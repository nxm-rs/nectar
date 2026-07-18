//! Typestate trust carrier for chunks.
//!
//! [`Chunk`] is the public chunk currency: whether its address is a verified
//! fact or an unverified claim is a sealed type parameter, so an unverified
//! chunk cannot flow where a verified one is required. Every source is parse
//! then verify; a store holding a [`TrustedSource`] capability is the single
//! gated exception ([`Chunk::assume_verified`]).

use std::marker::PhantomData;

use alloy_primitives::Address;
use bytes::Bytes;

use crate::cache::OnceCache;
use crate::error::Result;

use super::address::ChunkAddress;
use super::registry::{ChunkRegistry, StandardChunkSet};
use super::traits::ChunkOps;

mod sealed {
    pub trait Sealed {}
    impl Sealed for super::Verified {}
    impl Sealed for super::Unverified {}
}

/// Sealed trust state of a chunk's address: [`Verified`] or [`Unverified`].
pub trait TrustState: sealed::Sealed + Send + Sync + 'static {
    /// State name for diagnostics.
    const NAME: &'static str;
}

/// The address is a fact: it was certified by the member's full acceptance
/// rule, or vouched for by a [`TrustedSource`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Verified;

impl TrustState for Verified {
    const NAME: &'static str = "verified";
}

/// The address is a claim taken from context (a storage key, a wire field)
/// that nothing has certified yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Unverified;

impl TrustState for Unverified {
    const NAME: &'static str = "unverified";
}

/// Zero-sized capability vouching that a byte source only yields bytes that
/// were certified before they were stored.
///
/// Unforgeable in safe code: the only constructor is the `unsafe`
/// [`grant`](Self::grant). Mint one per trusted store and pass it to
/// [`Chunk::assume_verified`]; every other source is parse then verify.
#[derive(Debug)]
pub struct TrustedSource(());

impl TrustedSource {
    /// Mint the capability for one trusted read medium.
    ///
    /// # Safety
    ///
    /// This is a trust contract, not a memory-safety obligation: no
    /// undefined behaviour can result. The caller asserts that every byte
    /// the source yields was certified before it was stored and cannot have
    /// changed since; a false assertion puts lying addresses into
    /// consensus-critical state instead of tripping a verifier.
    #[must_use]
    pub const unsafe fn grant() -> Self {
        Self(())
    }
}

/// A chunk whose address carries its trust state in the type.
///
/// `S` is the sealed [`TrustState`]: for [`Unverified`] the address is a
/// claim, for [`Verified`] a fact. `R` is the [`ChunkRegistry`] the bytes
/// are decoded under. The only transitions are [`verify`](Self::verify),
/// which runs the member's full acceptance rule, and
/// [`assume_verified`](Self::assume_verified), gated on a [`TrustedSource`].
///
/// ```
/// use nectar_primitives::{Chunk, ChunkOps, ChunkRegistry, ContentChunk, StandardChunkSet, Unverified};
///
/// let content = ContentChunk::new(&b"currency"[..]).unwrap();
/// let claimed = *content.address();
/// let typed = StandardChunkSet::encode_typed(&content.into());
///
/// let verified = Chunk::<Unverified>::parse(claimed, &typed)
///     .unwrap()
///     .verify()
///     .unwrap();
/// assert_eq!(verified.address(), &claimed);
/// ```
pub struct Chunk<S: TrustState = Verified, R: ChunkRegistry = StandardChunkSet> {
    /// `S = Verified`: fact. `S = Unverified`: claim.
    address: ChunkAddress,
    /// The envelope decoded under `R`.
    inner: R::Envelope,
    /// Lazily recovered owner; only ever written in the verified state.
    owner: OnceCache<Option<Address>>,
    _state: PhantomData<S>,
}

/// Structural equality over the address and the decoded envelope (the owner
/// cache is ignored). A SOC address does not commit to the body, so address
/// equality alone is slot identity, not chunk equality.
impl<S: TrustState, R: ChunkRegistry> PartialEq for Chunk<S, R>
where
    R::Envelope: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.address == other.address && self.inner == other.inner
    }
}

impl<S: TrustState, R: ChunkRegistry> Eq for Chunk<S, R> where R::Envelope: Eq {}

impl<R: ChunkRegistry> Chunk<Unverified, R> {
    /// Structurally parse the registry's typed (store) encoding under a
    /// claimed address.
    ///
    /// The tag routes to a registry member and the payload is decoded;
    /// nothing certifies `claimed`. Errors on malformed input or an
    /// unsupported tag, never panics.
    pub fn parse(claimed: ChunkAddress, bytes: &[u8]) -> Result<Self> {
        Ok(Self {
            address: claimed,
            inner: R::parse_typed(bytes)?,
            owner: OnceCache::new(),
            _state: PhantomData,
        })
    }

    /// The address this chunk claims to live at; a claim until
    /// [`verify`](Self::verify).
    pub const fn claimed_address(&self) -> &ChunkAddress {
        &self.address
    }

    /// Certify the claimed address by the member's full acceptance rule.
    ///
    /// The rule recomputes everything from the parsed bytes with empty
    /// caches; nothing is compared against a stored derivation, so an
    /// internally consistent lie about the address cannot certify itself.
    pub fn verify(self) -> Result<Chunk<Verified, R>> {
        self.inner.verify(&self.address)?;
        Ok(Chunk::from_verified_parts(self.address, self.inner))
    }

    /// Take the claimed address on trust: the single gated skip of
    /// [`verify`](Self::verify), for bytes read back from a medium that only
    /// ever stored verified chunks.
    #[must_use]
    pub fn assume_verified(self, _source: &TrustedSource) -> Chunk<Verified, R> {
        Chunk::from_verified_parts(self.address, self.inner)
    }
}

impl<R: ChunkRegistry> Chunk<Verified, R> {
    /// Seed the address fact; the owner cache starts empty so recovery stays
    /// lazy.
    pub(crate) const fn from_verified_parts(address: ChunkAddress, inner: R::Envelope) -> Self {
        Self {
            address,
            inner,
            owner: OnceCache::new(),
            _state: PhantomData,
        }
    }

    /// Certify a locally built envelope at its own derived address.
    ///
    /// For upload paths where the value was constructed, not decoded. The
    /// address is key-sourced from the envelope's derivation, never
    /// caller-supplied, and the member's full acceptance rule still runs
    /// against it: derivation alone is not certification (a single-owner
    /// chunk with an unrecoverable signature derives a zero-owner address
    /// that must not certify).
    pub fn from_envelope(inner: R::Envelope) -> Result<Self> {
        let address = *inner.address();
        inner.verify(&address)?;
        Ok(Self::from_verified_parts(address, inner))
    }

    /// Decode and certify bare wire bytes (no type tag) in one step.
    ///
    /// Without a tag the address is the only member router, so parsing and
    /// certification are inseparable and the result is already verified.
    pub fn decode_wire(address: ChunkAddress, data: Bytes) -> Result<Self> {
        Ok(Self::from_verified_parts(
            address,
            R::decode_wire(&address, data)?,
        ))
    }

    /// The chunk's address: a certified fact, free to read.
    pub const fn address(&self) -> &ChunkAddress {
        &self.address
    }

    /// Encode into the registry's self-describing typed form, the store
    /// encoding. Only a verified chunk can produce it.
    pub fn typed_bytes(&self) -> Vec<u8> {
        R::encode_typed(&self.inner)
    }

    /// The owner the chunk's type binds, if it has one.
    ///
    /// Lazy and memoized: signature recovery runs on the first call only.
    /// `None` for ownerless types.
    pub fn owner(&self) -> Option<Address> {
        *self.owner.get_or_compute(|| self.inner.owner())
    }

    /// Borrow the decoded envelope.
    pub const fn envelope(&self) -> &R::Envelope {
        &self.inner
    }

    /// Consume into the decoded envelope.
    pub fn into_envelope(self) -> R::Envelope {
        self.inner
    }
}

impl<S: TrustState, R: ChunkRegistry> Clone for Chunk<S, R> {
    fn clone(&self) -> Self {
        Self {
            address: self.address,
            inner: self.inner.clone(),
            owner: self.owner.clone(),
            _state: PhantomData,
        }
    }
}

impl<S: TrustState, R: ChunkRegistry> std::fmt::Debug for Chunk<S, R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Chunk")
            .field("state", &S::NAME)
            .field("address", &self.address)
            .finish_non_exhaustive()
    }
}

/// Unifier over the trust states: identity for a verified chunk, a full
/// [`Chunk::verify`] for an unverified one. Monomorphizes to zero cost.
pub trait IntoVerified {
    /// Registry the chunk is decoded under.
    type Registry: ChunkRegistry;

    /// Certify into the verified state.
    fn into_verified(self) -> Result<Chunk<Verified, Self::Registry>>;
}

impl<R: ChunkRegistry> IntoVerified for Chunk<Verified, R> {
    type Registry = R;

    fn into_verified(self) -> Result<Self> {
        Ok(self)
    }
}

impl<R: ChunkRegistry> IntoVerified for Chunk<Unverified, R> {
    type Registry = R;

    fn into_verified(self) -> Result<Chunk<Verified, R>> {
        self.verify()
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::hex;
    use alloy_signer_local::PrivateKeySigner;

    use super::super::content::ContentChunk;
    use super::super::registry::ContentOnlyChunkSet;
    use super::super::single_owner::SingleOwnerChunk;
    use super::*;
    use crate::bmt::DEFAULT_BODY_SIZE;
    use crate::chunk::{ChunkError, SocId};
    use crate::error::PrimitivesError;

    type DefaultContentChunk = ContentChunk<DEFAULT_BODY_SIZE>;
    type DefaultSingleOwnerChunk = SingleOwnerChunk<DEFAULT_BODY_SIZE>;

    fn test_signer() -> PrivateKeySigner {
        // Fixed key so addresses are deterministic across runs.
        PrivateKeySigner::from_slice(&[0x42u8; 32]).unwrap()
    }

    /// Go-interop single-owner vector: id(32) || signature(65) || span(8) || "foo".
    fn soc_test_vector() -> Vec<u8> {
        hex!(
            "000000000000000000000000000000000000000000000000000000000000000\
            05acd384febc133b7b245e5ddc62d82d2cded9182d2716126cd8844509af65a05\
            3deb418208027f548e3e88343af6f84a8772fb3cebc0a1833a0ea7ec0c134831\
            1b0300000000000000666f6f"
        )
        .to_vec()
    }

    #[test]
    fn parse_then_verify_content_round_trips() {
        let content = DefaultContentChunk::new(&b"typestate currency"[..]).unwrap();
        let claimed = *content.address();
        let typed = StandardChunkSet::encode_typed(&content.into());

        let unverified = Chunk::<Unverified>::parse(claimed, &typed).unwrap();
        assert_eq!(unverified.claimed_address(), &claimed);

        let verified = unverified.verify().unwrap();
        assert_eq!(verified.address(), &claimed);
        assert_eq!(verified.typed_bytes(), typed);
        assert_eq!(verified.owner(), None, "a content chunk binds no owner");
    }

    #[test]
    fn verified_owner_is_recovered_and_memoized() {
        let signer = test_signer();
        let soc =
            DefaultSingleOwnerChunk::new(SocId::ZERO, b"owned payload".to_vec(), &signer).unwrap();
        let claimed = *soc.address();
        let verified = Chunk::<Verified>::from_envelope(soc.into()).unwrap();

        assert_eq!(verified.address(), &claimed);
        let owner = verified.owner();
        assert_eq!(owner, Some(signer.address()));
        // Second read serves the memoized value.
        assert_eq!(verified.owner(), owner);
        assert!(verified.envelope().is_single_owner());
    }

    /// Key regression: an internally consistent lie must be rejected. A
    /// single-owner chunk whose signature recovers to nobody still derives
    /// *some* address (the zero-owner one); parsing its bytes under exactly
    /// that claimed address must fail verify, where any path that trusted a
    /// stored or cached derivation would pass it.
    #[test]
    fn verify_rejects_internally_consistent_lie() {
        let mut wire = soc_test_vector();
        // Clobber the 65 signature bytes after the 32-byte id.
        for byte in wire.iter_mut().skip(32).take(65) {
            *byte = 0xff;
        }
        let lying = DefaultSingleOwnerChunk::try_from(wire.as_slice()).unwrap();
        let committed = *lying.address();
        let typed = StandardChunkSet::encode_typed(&lying.into());

        let unverified = Chunk::<Unverified>::parse(committed, &typed).unwrap();
        assert!(unverified.verify().is_err());
    }

    #[test]
    fn parse_is_structural_and_verify_certifies_the_claim() {
        let content = DefaultContentChunk::new(&b"honest bytes"[..]).unwrap();
        let typed = StandardChunkSet::encode_typed(&content.into());
        let lie: ChunkAddress = [0xFFu8; 32].into();

        // A lying claim parses fine: parse certifies nothing.
        let unverified = Chunk::<Unverified>::parse(lie, &typed).unwrap();
        assert_eq!(unverified.claimed_address(), &lie);

        // Verification is what rejects it.
        assert!(matches!(
            unverified.verify(),
            Err(PrimitivesError::Chunk(
                ChunkError::VerificationFailed { .. }
            ))
        ));
    }

    #[test]
    fn parse_malformed_input_errors_never_panics() {
        for bytes in [&[][..], &[0x00][..], &[0x00, 0x00][..]] {
            assert!(Chunk::<Unverified>::parse(ChunkAddress::default(), bytes).is_err());
        }

        // An unsupported tag is a distinct error carrying the tag.
        let unknown = [200u8, 0, 1, 2, 3, 4, 5, 6, 7, 8];
        assert!(matches!(
            Chunk::<Unverified>::parse(ChunkAddress::default(), &unknown),
            Err(PrimitivesError::Chunk(ChunkError::UnsupportedTag(_)))
        ));
    }

    /// The gated skip takes the claim on trust, even a false one: the
    /// capability holder vouches for the medium, nothing re-checks.
    #[test]
    fn assume_verified_skips_certification() {
        let content = DefaultContentChunk::new(&b"trusted medium"[..]).unwrap();
        let typed = StandardChunkSet::encode_typed(&content.into());
        let lie: ChunkAddress = [0xEEu8; 32].into();

        let source = unsafe { TrustedSource::grant() };
        let assumed = Chunk::<Unverified>::parse(lie, &typed)
            .unwrap()
            .assume_verified(&source);
        assert_eq!(assumed.address(), &lie);
    }

    #[test]
    fn decode_wire_certifies_in_one_step() {
        let signer = test_signer();
        let soc =
            DefaultSingleOwnerChunk::new(SocId::ZERO, b"wire form".to_vec(), &signer).unwrap();
        let address = *soc.address();
        let wire: Bytes = soc.into();

        let verified = Chunk::<Verified>::decode_wire(address, wire.clone()).unwrap();
        assert_eq!(verified.address(), &address);
        assert_eq!(verified.owner(), Some(signer.address()));

        let wrong: ChunkAddress = [0x11u8; 32].into();
        assert!(Chunk::<Verified>::decode_wire(wrong, wire).is_err());
    }

    #[test]
    fn into_verified_unifies_both_states() {
        fn certify<C: IntoVerified>(chunk: C) -> Result<Chunk<Verified, C::Registry>> {
            chunk.into_verified()
        }

        let content = DefaultContentChunk::new(&b"unifier"[..]).unwrap();
        let claimed = *content.address();
        let typed = StandardChunkSet::encode_typed(&content.into());

        // Unverified: runs the full acceptance rule.
        let via_unverified = certify(Chunk::<Unverified>::parse(claimed, &typed).unwrap()).unwrap();
        assert_eq!(via_unverified.address(), &claimed);

        // Verified: identity.
        let via_verified = certify(via_unverified).unwrap();
        assert_eq!(via_verified.address(), &claimed);

        // The unverified arm still rejects a lie.
        let lie: ChunkAddress = [0xAAu8; 32].into();
        assert!(certify(Chunk::<Unverified>::parse(lie, &typed).unwrap()).is_err());
    }

    #[test]
    fn content_only_registry_rejects_single_owner_tags() {
        let signer = test_signer();
        let soc =
            DefaultSingleOwnerChunk::new(SocId::ZERO, b"wrong network".to_vec(), &signer).unwrap();
        let claimed = *soc.address();
        let typed = StandardChunkSet::encode_typed(&soc.into());

        assert!(matches!(
            Chunk::<Unverified, ContentOnlyChunkSet>::parse(claimed, &typed),
            Err(PrimitivesError::Chunk(ChunkError::UnsupportedTag(_)))
        ));

        let content = DefaultContentChunk::new(&b"right network"[..]).unwrap();
        let claimed = *content.address();
        let typed = ContentOnlyChunkSet::encode_typed(&content);
        let verified = Chunk::<Unverified, ContentOnlyChunkSet>::parse(claimed, &typed)
            .unwrap()
            .verify()
            .unwrap();
        assert_eq!(verified.address(), &claimed);
    }

    #[test]
    fn from_envelope_derives_and_certifies() {
        let content = DefaultContentChunk::new(&b"local upload"[..]).unwrap();
        let derived = *content.address();

        let verified = Chunk::<Verified>::from_envelope(content.into()).unwrap();
        assert_eq!(verified.address(), &derived);
        assert_eq!(verified.into_envelope().address(), &derived);

        // Derivation is not certification: a single-owner envelope whose
        // signature recovers to nobody derives a zero-owner address but must
        // not certify at it.
        let mut wire = soc_test_vector();
        for byte in wire.iter_mut().skip(32).take(65) {
            *byte = 0xff;
        }
        let lying = DefaultSingleOwnerChunk::try_from(wire.as_slice()).unwrap();
        assert!(Chunk::<Verified>::from_envelope(lying.into()).is_err());
    }

    #[test]
    fn debug_names_the_trust_state() {
        let content = DefaultContentChunk::new(&b"debuggable"[..]).unwrap();
        let claimed = *content.address();
        let typed = StandardChunkSet::encode_typed(&content.into());

        let unverified = Chunk::<Unverified>::parse(claimed, &typed).unwrap();
        assert!(format!("{unverified:?}").contains("unverified"));
        assert!(format!("{:?}", unverified.verify().unwrap()).contains("verified"));
    }
}
