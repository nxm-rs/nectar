//! A chunk paired with the postage stamp that authorizes its storage.
//!
//! `verify` and `validate` commute, so either order reaches the same pair.

use alloc::vec::Vec;
use core::marker::PhantomData;

use alloy_primitives::Address;

use nectar_primitives::{
    AnyChunkSet, Chunk, ChunkAddress, DEFAULT_BODY_SIZE, SwarmSpec, TrustState, Unverified,
    Verified, bytes::Bytes, wire::Cursor,
};

use crate::{
    Batch, BatchId, Stamp, StampError, StampedAddress, Unvalidated, Validated, ValidationState,
};

/// A chunk together with its postage stamp, over trust and validation state.
///
/// [`new`](Self::new) exists only at [`Unvalidated`], so a validated pair
/// comes only from [`validate`](Self::validate) or
/// [`issued_by`](Self::issued_by):
///
/// ```compile_fail
/// use nectar_postage::{Stamp, StampedChunk, Validated};
/// use nectar_primitives::{AnyChunkSet, Chunk, DEFAULT_BODY_SIZE, Verified};
///
/// fn fabricate(
///     chunk: Chunk<Verified, AnyChunkSet<DEFAULT_BODY_SIZE>>,
///     stamp: Stamp,
/// ) -> StampedChunk<Verified, Validated> {
///     StampedChunk::new(chunk, stamp)
/// }
/// ```
///
/// Nor can a fuzzer draw one:
///
#[cfg_attr(feature = "arbitrary", doc = "```compile_fail")]
#[cfg_attr(not(feature = "arbitrary"), doc = "```ignore")]
/// use nectar_postage::StampedChunk;
///
/// fn draw<'a, T: arbitrary::Arbitrary<'a>>() {}
/// draw::<StampedChunk>();
/// ```
///
/// # Equality
///
/// Structural: the chunk half compares its address and decoded envelope, the
/// stamp half all stamp fields.
pub struct StampedChunk<
    T: TrustState = Verified,
    V: ValidationState = Validated,
    const BODY_SIZE: usize = DEFAULT_BODY_SIZE,
> {
    chunk: Chunk<T, AnyChunkSet<BODY_SIZE>>,
    stamp: Stamp,
    _validation: PhantomData<V>,
}

impl<T: TrustState, const BODY_SIZE: usize> StampedChunk<T, Unvalidated, BODY_SIZE> {
    /// Pair a chunk with the stamp that claims to pay for it.
    #[inline]
    #[must_use]
    pub const fn new(chunk: Chunk<T, AnyChunkSet<BODY_SIZE>>, stamp: Stamp) -> Self {
        Self {
            chunk,
            stamp,
            _validation: PhantomData,
        }
    }

    /// Certify the stamp against `batch` and the chunk's own address, which at
    /// [`Unverified`] is the claim the wire carried.
    ///
    /// # Errors
    ///
    /// Whatever [`StampedAddress::validate`] refuses the pairing with.
    pub fn validate<S: SwarmSpec>(
        self,
        batch: &Batch<S>,
    ) -> Result<StampedChunk<T, Validated, BODY_SIZE>, StampError> {
        let address = *self.chunk.address_in_state();
        let (_, stamp) = StampedAddress::new(address, self.stamp)
            .validate(batch)?
            .into_parts();
        Ok(StampedChunk {
            chunk: self.chunk,
            stamp,
            _validation: PhantomData,
        })
    }

    /// Certify a pair stamped by the batch owner's own key.
    ///
    /// `signer` is asserted, never re-derived: name an address that did not
    /// sign and the pair reaches [`Validated`] anyway.
    ///
    /// # Errors
    ///
    /// Whatever [`StampedAddress::issued_by`] refuses the pairing with.
    pub fn issued_by<S: SwarmSpec>(
        self,
        batch: &Batch<S>,
        signer: Address,
    ) -> Result<StampedChunk<T, Validated, BODY_SIZE>, StampError> {
        let address = *self.chunk.address_in_state();
        let (_, stamp) = StampedAddress::new(address, self.stamp)
            .issued_by(batch, signer)?
            .into_parts();
        Ok(StampedChunk {
            chunk: self.chunk,
            stamp,
            _validation: PhantomData,
        })
    }
}

impl<const BODY_SIZE: usize> StampedChunk<Unverified, Unvalidated, BODY_SIZE> {
    /// Decode a stamped chunk produced by
    /// [`to_typed_bytes`](Self::to_typed_bytes) without blessing either half.
    ///
    /// The first `STAMP_SIZE` bytes are the stamp ([`Stamp::from_bytes`]); the
    /// remainder is the type-tagged chunk, parsed under `address` as a claim.
    ///
    /// # Errors
    ///
    /// Returns an error (and never panics) when the input is shorter than a
    /// stamp, the stamp bytes are invalid, or the chunk payload cannot be
    /// parsed.
    pub fn parse(address: &ChunkAddress, bytes: &[u8]) -> Result<Self, StampError> {
        let mut cur = Cursor::new(bytes);
        let stamp = cur.take::<Stamp>()?;
        let chunk = Chunk::<Unverified, AnyChunkSet<BODY_SIZE>>::parse(*address, cur.finish())
            .map_err(|_| StampError::Chunk("failed to parse typed chunk"))?;
        Ok(Self::new(chunk, stamp))
    }
}

impl<const BODY_SIZE: usize> StampedChunk<Verified, Unvalidated, BODY_SIZE> {
    /// Rebuild a stamped chunk from the bare chunk wire bytes, its expected
    /// address, and a separately-carried stamp.
    ///
    /// `data` is the bare chunk wire bytes (no type tag), as carried in a
    /// `Delivery { data, stamp }` wire message. Bare wire bytes carry no tag,
    /// so the address routes the decode and certification is inseparable from
    /// it.
    ///
    /// # Errors
    ///
    /// Returns an error (and never panics) when `data` does not verify at
    /// `expected` as any registry member.
    pub fn reconstruct(
        expected: ChunkAddress,
        data: Bytes,
        stamp: Stamp,
    ) -> Result<Self, StampError> {
        let chunk = Chunk::<Verified, AnyChunkSet<BODY_SIZE>>::decode_wire(expected, data)
            .map_err(|_| StampError::Chunk("chunk bytes do not match expected address"))?;
        Ok(Self::new(chunk, stamp))
    }
}

impl<V: ValidationState, const BODY_SIZE: usize> StampedChunk<Unverified, V, BODY_SIZE> {
    /// Certify the claimed address by the chunk member's acceptance rule.
    ///
    /// # Errors
    ///
    /// Returns an error (and never panics) when the body does not hash to the
    /// claimed address.
    pub fn verify(self) -> Result<StampedChunk<Verified, V, BODY_SIZE>, StampError> {
        let chunk = self
            .chunk
            .verify()
            .map_err(|_| StampError::Chunk("chunk does not verify at the claimed address"))?;
        Ok(StampedChunk {
            chunk,
            stamp: self.stamp,
            _validation: PhantomData,
        })
    }
}

impl<T: TrustState, V: ValidationState, const BODY_SIZE: usize> StampedChunk<T, V, BODY_SIZE> {
    /// The chunk.
    #[inline]
    #[must_use]
    pub const fn chunk(&self) -> &Chunk<T, AnyChunkSet<BODY_SIZE>> {
        &self.chunk
    }

    /// The postage stamp.
    #[inline]
    #[must_use]
    pub const fn stamp(&self) -> &Stamp {
        &self.stamp
    }

    /// The stamp with the address it is bound to, without the body.
    ///
    /// `V` transfers: the only route to [`Validated`] is
    /// [`validate`](StampedChunk::validate), over this same address.
    #[inline]
    #[must_use]
    pub fn detach(&self) -> StampedAddress<V> {
        StampedAddress::from_parts(*self.chunk.address_in_state(), self.stamp.clone())
    }

    /// Split into the chunk and its stamp.
    #[inline]
    #[must_use]
    pub fn into_parts(self) -> (Chunk<T, AnyChunkSet<BODY_SIZE>>, Stamp) {
        (self.chunk, self.stamp)
    }

    /// Read the batch id from a [`to_typed_bytes`](Self::to_typed_bytes) value
    /// without a full decode.
    ///
    /// The stamp leads the encoding and the batch id is the stamp's first wire
    /// field, so a store can index by batch without decoding the chunk.
    ///
    /// # Errors
    ///
    /// Returns an error (and never panics) when `typed_bytes` is shorter than a
    /// batch id.
    pub fn batch_id(typed_bytes: &[u8]) -> Result<BatchId, StampError> {
        Ok(Cursor::new(typed_bytes).take::<BatchId>()?)
    }
}

impl<V: ValidationState, const BODY_SIZE: usize> StampedChunk<Verified, V, BODY_SIZE> {
    /// The chunk's certified address: a stored fact, free to read.
    #[inline]
    #[must_use]
    pub const fn address(&self) -> &ChunkAddress {
        self.chunk.address()
    }

    /// Encode to a self-describing byte string: the stamp followed by the
    /// type-tagged chunk.
    ///
    /// The layout is `[stamp: STAMP_SIZE][id: 1][version: 1][chunk wire
    /// bytes]`. Decode with [`parse`](StampedChunk::parse).
    #[must_use]
    pub fn to_typed_bytes(&self) -> Vec<u8> {
        let stamp = self.stamp.to_bytes();
        let chunk = self.chunk.typed_bytes();
        let mut out = Vec::with_capacity(stamp.len().saturating_add(chunk.len()));
        out.extend_from_slice(&stamp);
        out.extend_from_slice(&chunk);
        out
    }
}

impl<T: TrustState, V: ValidationState, const BODY_SIZE: usize> Clone
    for StampedChunk<T, V, BODY_SIZE>
{
    fn clone(&self) -> Self {
        Self {
            chunk: self.chunk.clone(),
            stamp: self.stamp.clone(),
            _validation: PhantomData,
        }
    }
}

impl<T: TrustState, V: ValidationState, const BODY_SIZE: usize> PartialEq
    for StampedChunk<T, V, BODY_SIZE>
{
    fn eq(&self, other: &Self) -> bool {
        self.chunk == other.chunk && self.stamp == other.stamp
    }
}

impl<T: TrustState, V: ValidationState, const BODY_SIZE: usize> Eq
    for StampedChunk<T, V, BODY_SIZE>
{
}

impl<T: TrustState, V: ValidationState, const BODY_SIZE: usize> core::fmt::Debug
    for StampedChunk<T, V, BODY_SIZE>
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("StampedChunk")
            .field("trust", &T::NAME)
            .field("validation", &V::NAME)
            .field("chunk", &self.chunk)
            .field("stamp", &self.stamp)
            .finish()
    }
}

#[cfg(any(test, feature = "arbitrary"))]
impl<'a, const BODY_SIZE: usize> arbitrary::Arbitrary<'a>
    for StampedChunk<Verified, Unvalidated, BODY_SIZE>
{
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        // The stamp is structurally valid but signs nothing; use
        // `crate::generators::signed_stamped_chunk` for one that validates.
        let envelope = nectar_primitives::generators::any_chunk::<BODY_SIZE>(u)?;
        let chunk =
            Chunk::from_envelope(envelope).map_err(|_| arbitrary::Error::IncorrectFormat)?;
        Ok(Self::new(chunk, Stamp::arbitrary(u)?))
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{B256, Signature};
    use alloy_signer_local::PrivateKeySigner;
    use arbitrary::Unstructured;
    use nectar_primitives::{
        AnyChunk, ChunkOps, ContentChunk, SingleOwnerChunk, SocId, bytes::Bytes,
    };

    use super::*;
    use crate::{STAMP_SIZE, generators};

    type Raw = StampedChunk<Unverified, Unvalidated>;
    type Signed = StampedChunk<Verified, Unvalidated>;
    type Sealed = StampedChunk<Verified, Validated>;

    fn test_stamp() -> Stamp {
        let sig = Signature::from_raw(&[1u8; 65]).expect("valid signature");
        Stamp::new(BatchId::new([0xaa; 32]), 3, 7, 42, sig)
    }

    fn content_chunk() -> ContentChunk<DEFAULT_BODY_SIZE> {
        ContentChunk::new(&b"hello swarm"[..]).expect("valid content chunk")
    }

    fn single_owner_chunk() -> SingleOwnerChunk<DEFAULT_BODY_SIZE> {
        let signer = PrivateKeySigner::from_bytes(&B256::repeat_byte(0x11)).expect("valid signer");
        SingleOwnerChunk::new(
            SocId::from(B256::repeat_byte(0x22)),
            &b"soc payload"[..],
            &signer,
        )
        .expect("valid soc")
    }

    fn verified(chunk: impl Into<AnyChunk<DEFAULT_BODY_SIZE>>) -> Chunk<Verified> {
        Chunk::from_envelope(chunk.into()).expect("locally built chunk certifies")
    }

    /// A pair whose stamp really signs the chunk's address, with its batch.
    fn signed(chunk: impl Into<AnyChunk<DEFAULT_BODY_SIZE>>) -> (Batch, Signed) {
        let chunk = verified(chunk);
        let mut u = Unstructured::new(&[7u8; 128]);
        let (batch, stamp) =
            generators::batch_and_stamp(&mut u, chunk.address()).expect("coherent stamp");
        (batch, StampedChunk::new(chunk, stamp))
    }

    #[test]
    fn into_parts_round_trips_the_fields() {
        let chunk = verified(content_chunk());
        let address = *chunk.address();
        let stamp = test_stamp();
        let stamped = Signed::new(chunk, stamp.clone());
        assert_eq!(stamped.address(), &address);
        let (got_chunk, got_stamp) = stamped.into_parts();
        assert_eq!(got_chunk.address(), &address);
        assert_eq!(got_stamp, stamp);
    }

    #[test]
    fn typed_content_round_trip() {
        let chunk = content_chunk();
        let address = *chunk.address();
        let stamp = test_stamp();
        let stamped = Signed::new(verified(chunk), stamp.clone());

        let bytes = stamped.to_typed_bytes();
        let decoded = Raw::parse(&address, &bytes).expect("decode");

        let stamp_of = decoded.stamp().clone();
        assert!(
            decoded
                .verify()
                .expect("verifies")
                .chunk()
                .envelope()
                .is_content()
        );
        assert_eq!(stamp_of, stamp);
    }

    /// The 113-byte stamp leads the encoding and the chunk follows unchanged.
    #[test]
    fn typed_encoding_is_the_stamp_then_the_chunk() {
        let chunk = verified(content_chunk());
        let typed = chunk.typed_bytes();
        let stamp = test_stamp();
        let bytes = Signed::new(chunk, stamp.clone()).to_typed_bytes();

        assert_eq!(bytes.len(), STAMP_SIZE + typed.len());
        assert_eq!(&bytes[..STAMP_SIZE], stamp.to_bytes().as_slice());
        assert_eq!(&bytes[STAMP_SIZE..], typed.as_slice());
    }

    #[test]
    fn typed_single_owner_round_trip() {
        let chunk = single_owner_chunk();
        let address = *chunk.address();
        let stamped = Signed::new(verified(chunk), test_stamp());

        let bytes = stamped.to_typed_bytes();
        let decoded = Raw::parse(&address, &bytes)
            .expect("decode")
            .verify()
            .expect("verifies");

        assert!(decoded.chunk().envelope().is_single_owner());
        assert_eq!(decoded, stamped);
    }

    #[test]
    fn the_two_transition_orders_agree() {
        let (batch, pair) = signed(content_chunk());
        let address = *pair.address();
        let bytes = pair.to_typed_bytes();

        let ingest: Sealed = Raw::parse(&address, &bytes)
            .expect("parse")
            .validate(&batch)
            .expect("stamp pays for the claimed address")
            .verify()
            .expect("body hashes to the claim");
        let producer: Sealed = Raw::parse(&address, &bytes)
            .expect("parse")
            .verify()
            .expect("body hashes to the claim")
            .validate(&batch)
            .expect("stamp pays for the certified address");

        assert_eq!(ingest, producer);
        assert_eq!(ingest.to_typed_bytes(), bytes);
    }

    #[test]
    fn issued_by_agrees_with_validate() {
        let (batch, pair) = signed(content_chunk());

        let issued: Sealed = pair.clone().issued_by(&batch, batch.owner()).expect("bound");
        let ingested: Sealed = pair.validate(&batch).expect("recovered");
        assert_eq!(issued, ingested);
    }

    #[test]
    fn issued_by_refuses_a_signer_that_is_not_the_owner() {
        let (batch, pair) = signed(content_chunk());
        let stranger = PrivateKeySigner::from_bytes(&B256::repeat_byte(0x33))
            .expect("valid signer")
            .address();

        assert!(matches!(
            pair.issued_by(&batch, stranger),
            Err(StampError::OwnerMismatch { .. })
        ));
    }

    #[test]
    fn a_verified_pair_validates_without_a_decode() {
        let (batch, pair) = signed(content_chunk());
        assert!(pair.validate(&batch).is_ok());
    }

    /// A lying claim survives parse and dies at verify.
    #[test]
    fn parse_certifies_nothing() {
        let stamped = Signed::new(verified(content_chunk()), test_stamp());
        let bytes = stamped.to_typed_bytes();
        let wrong: ChunkAddress = [0xFFu8; 32].into();

        let raw = Raw::parse(&wrong, &bytes).expect("parse takes the claim as given");
        assert_eq!(raw.detach().address(), &wrong);
        assert!(matches!(raw.verify(), Err(StampError::Chunk(_))));
    }

    /// The ingest order refuses an unpaid body before it is hashed.
    #[test]
    fn validate_refuses_an_unverified_pair_on_a_foreign_batch() {
        let (batch, pair) = signed(content_chunk());
        let address = *pair.address();
        let bytes = pair.to_typed_bytes();
        // Same geometry and owner, so only the id can refuse it.
        let mut id = [0u8; 32];
        id.copy_from_slice(batch.id().as_slice());
        id[0] ^= 0xFF;
        let elsewhere: Batch = Batch::new(
            BatchId::new(id),
            batch.value(),
            batch.start(),
            batch.owner(),
            batch.depth(),
            batch.bucket_depth(),
            batch.immutable(),
        );

        let raw = Raw::parse(&address, &bytes).expect("parse");
        assert!(matches!(
            raw.validate(&elsewhere),
            Err(StampError::BatchMismatch { .. })
        ));
    }

    #[test]
    fn validate_refuses_a_re_paired_stamp() {
        let (batch, pair) = signed(content_chunk());
        let (_, stamp) = pair.into_parts();

        let elsewhere = Signed::new(verified(single_owner_chunk()), stamp);
        assert!(elsewhere.validate(&batch).is_err());
    }

    /// The annotations are the assertion: `detach` carries `V` across.
    #[test]
    fn detach_projects_the_pair_in_either_state() {
        let (batch, pair) = signed(content_chunk());
        let address = *pair.address();
        let stamp = pair.stamp().clone();

        let raw: StampedAddress<Unvalidated> = pair.detach();
        assert_eq!(raw.address(), &address);
        assert_eq!(raw.stamp(), &stamp);

        let sealed: StampedAddress<Validated> = pair.validate(&batch).expect("validates").detach();
        assert_eq!(sealed.address(), &address);
        assert_eq!(sealed.stamp(), &stamp);
    }

    #[test]
    fn reconstruct_round_trips_from_wire() {
        let chunk = content_chunk();
        let address = *chunk.address();
        let data = Bytes::from(chunk);
        let stamp = test_stamp();

        let rebuilt = Signed::reconstruct(address, data.clone(), stamp.clone()).expect("rebuild");
        assert!(rebuilt.chunk().envelope().is_content());
        assert_eq!(*rebuilt.address(), address);
        assert_eq!(rebuilt.stamp(), &stamp);
        assert_eq!(rebuilt.into_parts().0.into_envelope().into_bytes(), data);
    }

    #[test]
    fn reconstruct_single_owner_from_wire() {
        let chunk = single_owner_chunk();
        let address = *chunk.address();
        let data = Bytes::from(chunk);
        let stamp = test_stamp();

        let rebuilt = Signed::reconstruct(address, data, stamp.clone()).expect("rebuild");
        assert!(rebuilt.chunk().envelope().is_single_owner());
        assert_eq!(*rebuilt.address(), address);
        assert_eq!(rebuilt.stamp(), &stamp);
    }

    #[test]
    fn equality_compares_address_and_stamp() {
        let stamp = test_stamp();
        let a = Signed::new(verified(content_chunk()), stamp.clone());
        let b = Signed::new(verified(content_chunk()), stamp.clone());
        assert_eq!(a, b);

        let sig = Signature::from_raw(&[1u8; 65]).expect("valid signature");
        let other_stamp = Stamp::new(BatchId::new([0xbb; 32]), 3, 7, 42, sig);
        let c = Signed::new(verified(content_chunk()), other_stamp);
        assert_ne!(a, c);

        let d = Signed::new(verified(single_owner_chunk()), stamp);
        assert_ne!(a, d);
    }

    #[test]
    fn debug_names_both_states() {
        let (batch, pair) = signed(content_chunk());
        let raw = format!("{pair:?}");
        assert!(raw.contains("verified") && raw.contains("unvalidated"));

        let sealed = format!("{:?}", pair.validate(&batch).expect("validates"));
        assert!(sealed.contains("validated"));
    }

    #[test]
    fn batch_id_matches_stamp_and_leading_bytes() {
        let stamp = test_stamp();
        let stamped = Signed::new(verified(content_chunk()), stamp.clone());
        let bytes = stamped.to_typed_bytes();

        let id = Sealed::batch_id(&bytes).expect("batch id");
        assert_eq!(id, stamp.batch());
        assert_eq!(id.as_slice(), &bytes[0..32]);
    }

    #[test]
    fn parse_empty_errors() {
        let address: ChunkAddress = [0u8; 32].into();
        assert!(Raw::parse(&address, &[]).is_err());
    }

    #[test]
    fn parse_shorter_than_stamp_errors() {
        let address: ChunkAddress = [0u8; 32].into();
        let short = [0u8; STAMP_SIZE - 1];
        let err = Raw::parse(&address, &short).expect_err("short input must error");
        assert!(matches!(err, StampError::Underrun { .. }));
    }

    #[test]
    fn reconstruct_rejects_wrong_address() {
        let chunk = content_chunk();
        let data = Bytes::from(chunk);
        let wrong: ChunkAddress = [0xFFu8; 32].into();
        let err =
            Signed::reconstruct(wrong, data, test_stamp()).expect_err("wrong address must error");
        assert!(matches!(err, StampError::Chunk(_)));
    }

    #[test]
    fn batch_id_short_errors() {
        let short = [0u8; 31];
        assert!(Sealed::batch_id(&short).is_err());
    }
}
