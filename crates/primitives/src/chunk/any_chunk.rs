//! Type-erased chunk type
//!
//! This module provides [`AnyChunk`], an enum that can hold any chunk type
//! for runtime polymorphism without requiring trait objects.

use bytes::Bytes;

use crate::bmt::DEFAULT_BODY_SIZE;
use crate::error::Result;

use super::address::ChunkAddress;
use super::chunk_type::ChunkType;
use super::content::ContentChunk;
use super::single_owner::SingleOwnerChunk;
use super::traits::{Chunk, ChunkHeader};
use super::type_id::ChunkTypeId;

/// Type-erased chunk for runtime polymorphism with configurable body size.
///
/// This enum provides dynamic dispatch for chunks without requiring object-safe traits.
/// Use this when you need to store heterogeneous chunk types in collections or pass
/// chunks through interfaces that can't be generic.
///
/// # Why an enum instead of `Box<dyn Chunk>`?
///
/// The [`Chunk`] trait has an associated type (`type Header`) which makes it not
/// object-safe. This enum provides the same functionality while maintaining type safety.
///
/// # Examples
///
/// ```
/// use nectar_primitives::{AnyChunk, Chunk, ContentChunk, ChunkTypeId};
///
/// // Create a content chunk
/// let content = ContentChunk::new(&b"hello world"[..]).unwrap();
/// let any: AnyChunk = content.clone().into();
///
/// // Access common properties
/// assert_eq!(any.type_id(), ChunkTypeId::CONTENT);
///
/// // Get the concrete type back
/// if let Some(recovered) = any.as_content() {
///     assert_eq!(recovered.address(), content.address());
/// }
/// ```
#[derive(Debug, Clone)]
pub enum AnyChunk<const BODY_SIZE: usize = DEFAULT_BODY_SIZE> {
    /// A content-addressed chunk (CAC).
    Content(ContentChunk<BODY_SIZE>),
    /// A single-owner chunk (SOC).
    SingleOwner(SingleOwnerChunk<BODY_SIZE>),
}

impl<const BODY_SIZE: usize> AnyChunk<BODY_SIZE> {
    /// Get the address of this chunk.
    pub fn address(&self) -> &ChunkAddress {
        match self {
            Self::Content(c) => c.address(),
            Self::SingleOwner(c) => c.address(),
        }
    }

    /// Get the raw data contained in this chunk.
    pub fn data(&self) -> &Bytes {
        match self {
            Self::Content(c) => c.data(),
            Self::SingleOwner(c) => c.data(),
        }
    }

    /// Get the type ID of this chunk.
    pub const fn type_id(&self) -> ChunkTypeId {
        match self {
            Self::Content(_) => ChunkTypeId::CONTENT,
            Self::SingleOwner(_) => ChunkTypeId::SINGLE_OWNER,
        }
    }

    /// Get the total size of this chunk in bytes.
    pub fn size(&self) -> usize {
        match self {
            Self::Content(c) => c.size(),
            Self::SingleOwner(c) => c.size(),
        }
    }

    /// Get the span (logical data length) of this chunk: the BMT span of its
    /// underlying body.
    pub fn span(&self) -> u64 {
        match self {
            Self::Content(c) => super::traits::BmtChunk::span(c),
            Self::SingleOwner(c) => super::traits::BmtChunk::span(c),
        }
    }

    /// Compute the anchor-keyed *transformed address* of this chunk.
    ///
    /// The transformed address is the redistribution sampler's per-round,
    /// per-node re-hash of a chunk. It is a prefixed BMT root keyed by the
    /// node's `anchor`, used to order reserve chunks deterministically while
    /// binding the ordering to the proving node. This reproduces bee's
    /// `storer.transformedAddress` (`pkg/storer/sample.go`) byte-for-byte.
    ///
    /// # Derivation
    ///
    /// - The anchor-keyed BMT root of the chunk body is computed by
    ///   [`BmtBody::transformed_root`](super::bmt_body::BmtBody::transformed_root) on the
    ///   chunk's borrowed body, which mixes the anchor into *every* node hash.
    ///   For a SOC the wrapped content body is the one re-hashed.
    /// - The root is then sealed into the transformed address by the chunk's
    ///   header predicate
    ///   ([`ChunkHeader::seal_transformed`](super::traits::ChunkHeader::seal_transformed)):
    ///   the root itself for a CAC, the plain (unprefixed, no anchor)
    ///   `keccak256(soc_address || inner)` for a SOC.
    ///
    /// # Endianness
    ///
    /// The span is serialised little-endian inside the BMT. Do not confuse this
    /// with the big-endian encodings used elsewhere on the redistribution wire
    /// (e.g. proof witness indices); the BMT span is always LE.
    ///
    /// # Borrowing
    ///
    /// Both paths dispatch through the carrier's borrowed body accessor
    /// ([`ChunkInner::body`](super::inner::ChunkInner::body)), so no chunk or
    /// body is cloned. For a SOC the wrapped body already *is* the content
    /// chunk's `span || payload`, so the inner root needs no `32 + 65`
    /// (id + signature) header slicing.
    pub fn transformed_address(&self, anchor: &[u8]) -> ChunkAddress {
        match self {
            Self::Content(c) => c
                .header()
                .seal_transformed(c.address(), c.body().transformed_root(anchor)),
            Self::SingleOwner(c) => c
                .header()
                .seal_transformed(c.address(), c.body().transformed_root(anchor)),
        }
    }

    /// Verify that this chunk's address matches an expected address.
    pub fn verify(&self, expected: &ChunkAddress) -> Result<()> {
        match self {
            Self::Content(c) => c.verify(expected),
            Self::SingleOwner(c) => c.verify(expected),
        }
    }

    /// Convert this chunk into its serialized bytes representation.
    pub fn into_bytes(self) -> Bytes {
        match self {
            Self::Content(c) => c.into(),
            Self::SingleOwner(c) => c.into(),
        }
    }

    /// Check if this chunk is of a specific type.
    pub fn is<T: ChunkType>(&self) -> bool {
        self.type_id() == T::TYPE_ID
    }

    /// Check if this is a content chunk.
    pub const fn is_content(&self) -> bool {
        matches!(self, Self::Content(_))
    }

    /// Check if this is a single-owner chunk.
    pub const fn is_single_owner(&self) -> bool {
        matches!(self, Self::SingleOwner(_))
    }

    /// Get a reference to the contained ContentChunk, if this is one.
    pub const fn as_content(&self) -> Option<&ContentChunk<BODY_SIZE>> {
        match self {
            Self::Content(c) => Some(c),
            _ => None,
        }
    }

    /// Get a reference to the contained SingleOwnerChunk, if this is one.
    pub const fn as_single_owner(&self) -> Option<&SingleOwnerChunk<BODY_SIZE>> {
        match self {
            Self::SingleOwner(c) => Some(c),
            _ => None,
        }
    }

    /// Convert into the contained ContentChunk, if this is one.
    pub fn into_content(self) -> Option<ContentChunk<BODY_SIZE>> {
        match self {
            Self::Content(c) => Some(c),
            _ => None,
        }
    }

    /// Convert into the contained SingleOwnerChunk, if this is one.
    pub fn into_single_owner(self) -> Option<SingleOwnerChunk<BODY_SIZE>> {
        match self {
            Self::SingleOwner(c) => Some(c),
            _ => None,
        }
    }

    /// Encode this chunk as a type-tagged, self-describing byte string.
    ///
    /// The layout is `[type_id: 1 byte][chunk wire bytes]`, where the chunk
    /// wire bytes are the same form produced by [`AnyChunk::into_bytes`] (the
    /// inverse of [`ContentChunk::try_from`]/[`SingleOwnerChunk::try_from`]).
    ///
    /// The chunk address is deliberately *not* embedded. It is supplied from
    /// context on decode (for example the redb key when reading from storage,
    /// or the address field of a wire message). This mirrors the existing
    /// reconstruction paths that take an expected address.
    ///
    /// Unlike the address-disambiguating reconstruction path, decoding the
    /// result of this method dispatches purely by `type_id`, so it never has
    /// to trial-parse both the content and single-owner shapes.
    ///
    /// # Examples
    ///
    /// ```
    /// use nectar_primitives::{AnyChunk, Chunk, ContentChunk};
    ///
    /// let content = ContentChunk::new(&b"hello world"[..]).unwrap();
    /// let address = *content.address();
    /// let any: AnyChunk = content.into();
    ///
    /// let encoded = any.to_typed_bytes();
    /// assert_eq!(encoded[0], 0); // CONTENT type id
    ///
    /// let decoded: AnyChunk = AnyChunk::from_typed_bytes(&address, &encoded).unwrap();
    /// assert_eq!(decoded.address(), any.address());
    /// ```
    #[allow(clippy::arithmetic_side_effects)] // 1 + a chunk wire length bounded by the chunk format is a capacity hint far below usize::MAX
    pub fn to_typed_bytes(&self) -> Vec<u8> {
        let tag = self.type_id().as_u8();
        // Clone is required because `into_bytes` consumes the chunk; chunk
        // payloads are reference-counted `Bytes`, so this is cheap.
        let wire = self.clone().into_bytes();
        let mut out = Vec::with_capacity(1 + wire.len());
        out.push(tag);
        out.extend_from_slice(&wire);
        out
    }

    /// Decode a type-tagged chunk produced by [`AnyChunk::to_typed_bytes`].
    ///
    /// The leading byte selects the chunk type and the remainder is the chunk
    /// wire payload. `address` is the expected chunk address (for example the
    /// redb storage key or the wire message address field); the decoded chunk
    /// is verified against it.
    ///
    /// Decoding dispatches by the type tag and therefore never trial-parses the
    /// other chunk shape. Only the standard content and single-owner types are
    /// recognised; any other tag is an error (custom chunk types are tracked
    /// separately as a future registration mechanism).
    ///
    /// # Errors
    ///
    /// Returns an error (and never panics) when:
    /// - the input is empty (no type tag),
    /// - the type tag is not a recognised standard chunk type,
    /// - the payload cannot be decoded as the tagged chunk type, or
    /// - a standard chunk's computed address does not match `address`.
    pub fn from_typed_bytes(address: &ChunkAddress, bytes: &[u8]) -> crate::error::Result<Self> {
        let (&tag, payload) = bytes.split_first().ok_or_else(|| {
            super::error::ChunkError::invalid_format("empty typed-chunk encoding: missing type tag")
        })?;

        let type_id = ChunkTypeId::from(tag);
        match type_id {
            ChunkTypeId::CONTENT => {
                let chunk = ContentChunk::try_from(Bytes::copy_from_slice(payload))?;
                chunk.verify(address)?;
                Ok(Self::Content(chunk))
            }
            ChunkTypeId::SINGLE_OWNER => {
                let chunk = SingleOwnerChunk::try_from(Bytes::copy_from_slice(payload))?;
                chunk.verify(address)?;
                Ok(Self::SingleOwner(chunk))
            }
            other => Err(super::error::ChunkError::unsupported_type(other).into()),
        }
    }

    /// Decode a chunk from its bare wire bytes (NO type tag) given the expected
    /// address, disambiguating content vs single-owner by which one hashes to
    /// `address`. This is the type-less wire form (e.g. a Delivery `data`
    /// field); prefer [`from_typed_bytes`](Self::from_typed_bytes) for
    /// self-describing storage. Only content and single-owner shapes are
    /// recognised; bytes that parse as neither (for the given address) error.
    ///
    /// Reconstructing an [`AnyChunk`] from raw bytes is ambiguous without the
    /// address: a [`ContentChunk`] parse almost always succeeds structurally (a
    /// span plus an arbitrary payload), so the expected address is the
    /// disambiguator. The chunk is whichever variant parses *and* hashes to
    /// `address`. Content is tried first, then single-owner; a lying address
    /// makes both attempts fail, so the address is self-validating against the
    /// bytes.
    ///
    /// # Errors
    ///
    /// Returns a verification error (and never panics) when neither the content
    /// nor the single-owner interpretation of `data` hashes to `address`.
    pub fn from_wire_bytes(address: &ChunkAddress, data: Bytes) -> crate::error::Result<Self> {
        if let Ok(content) = ContentChunk::try_from(data.clone())
            && content.address() == address
        {
            return Ok(Self::Content(content));
        }
        if let Ok(soc) = SingleOwnerChunk::try_from(data)
            && soc.address() == address
        {
            return Ok(Self::SingleOwner(soc));
        }
        Err(super::error::ChunkError::verification_failed(*address, *address).into())
    }
}

impl<const BODY_SIZE: usize> From<ContentChunk<BODY_SIZE>> for AnyChunk<BODY_SIZE> {
    fn from(chunk: ContentChunk<BODY_SIZE>) -> Self {
        Self::Content(chunk)
    }
}

impl<const BODY_SIZE: usize> From<SingleOwnerChunk<BODY_SIZE>> for AnyChunk<BODY_SIZE> {
    fn from(chunk: SingleOwnerChunk<BODY_SIZE>) -> Self {
        Self::SingleOwner(chunk)
    }
}

/// Structural equality: same variant, equal header and body.
impl<const BODY_SIZE: usize> PartialEq for AnyChunk<BODY_SIZE> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Content(a), Self::Content(b)) => a == b,
            (Self::SingleOwner(a), Self::SingleOwner(b)) => a == b,
            _ => false,
        }
    }
}

impl<const BODY_SIZE: usize> Eq for AnyChunk<BODY_SIZE> {}

#[cfg(any(test, feature = "arbitrary"))]
impl<'a, const BODY_SIZE: usize> arbitrary::Arbitrary<'a> for AnyChunk<BODY_SIZE> {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        // Raw tier: delegates to the variant impls, so the single-owner arm
        // may carry a signature that does not verify. Use
        // `crate::generators::any_chunk` for a valid-by-construction value.
        if u.arbitrary()? {
            Ok(ContentChunk::<BODY_SIZE>::arbitrary(u)?.into())
        } else {
            Ok(SingleOwnerChunk::<BODY_SIZE>::arbitrary(u)?.into())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::traits::{BmtChunk, Chunk};
    use super::*;

    type DefaultContentChunk = ContentChunk<DEFAULT_BODY_SIZE>;
    type DefaultSingleOwnerChunk = SingleOwnerChunk<DEFAULT_BODY_SIZE>;
    type DefaultAnyChunk = AnyChunk<DEFAULT_BODY_SIZE>;

    #[test]
    fn test_content_chunk_conversion() {
        let content = DefaultContentChunk::new(&b"hello world"[..]).unwrap();
        let address = *content.address();

        let any: DefaultAnyChunk = content.into();

        assert!(any.is_content());
        assert!(!any.is_single_owner());
        assert_eq!(any.type_id(), ChunkTypeId::CONTENT);
        assert_eq!(*any.address(), address);
    }

    #[test]
    fn test_as_content() {
        let content = DefaultContentChunk::new(&b"test data"[..]).unwrap();
        let expected_addr = *content.address();

        let any: DefaultAnyChunk = content.into();
        let recovered = any.as_content().unwrap();

        assert_eq!(*recovered.address(), expected_addr);
    }

    #[test]
    fn test_into_content() {
        let content = DefaultContentChunk::new(&b"test data"[..]).unwrap();
        let expected_addr = *content.address();

        let any: DefaultAnyChunk = content.into();
        let recovered = any.into_content().unwrap();

        assert_eq!(*recovered.address(), expected_addr);
    }

    #[test]
    fn test_is_methods() {
        let content: DefaultAnyChunk = DefaultContentChunk::new(&b"test"[..]).unwrap().into();

        assert!(content.is::<DefaultContentChunk>());
        assert!(!content.is::<DefaultSingleOwnerChunk>());
    }

    #[test]
    fn test_clone() {
        let content = DefaultContentChunk::new(&b"test"[..]).unwrap();
        let any: DefaultAnyChunk = content.into();
        let cloned = any.clone();

        assert_eq!(any.address(), cloned.address());
        assert_eq!(any.type_id(), cloned.type_id());
    }

    fn test_signer() -> alloy_signer_local::PrivateKeySigner {
        // Fixed key so addresses are deterministic across runs.
        let pk = [0x42u8; 32];
        alloy_signer_local::PrivateKeySigner::from_slice(&pk).unwrap()
    }

    fn sample_single_owner() -> DefaultSingleOwnerChunk {
        let id = crate::SocId::ZERO;
        DefaultSingleOwnerChunk::new(id, b"single owner payload".to_vec(), &test_signer()).unwrap()
    }

    #[test]
    fn test_typed_content_round_trip() {
        let content = DefaultContentChunk::new(&b"hello typed world"[..]).unwrap();
        let address = *content.address();
        let any: DefaultAnyChunk = content.into();

        let encoded = any.to_typed_bytes();
        assert_eq!(encoded[0], 0, "CONTENT tag must be 0");

        let decoded = DefaultAnyChunk::from_typed_bytes(&address, &encoded).unwrap();
        assert!(decoded.is_content());
        assert_eq!(decoded.type_id(), ChunkTypeId::CONTENT);
        assert_eq!(decoded.address(), any.address());
        assert_eq!(decoded.data(), any.data());
    }

    #[test]
    fn test_typed_single_owner_round_trip() {
        let soc = sample_single_owner();
        let address = *soc.address();
        let any: DefaultAnyChunk = soc.into();

        let encoded = any.to_typed_bytes();
        assert_eq!(encoded[0], 1, "SINGLE_OWNER tag must be 1");

        let decoded = DefaultAnyChunk::from_typed_bytes(&address, &encoded).unwrap();
        assert!(decoded.is_single_owner());
        assert_eq!(decoded.type_id(), ChunkTypeId::SINGLE_OWNER);
        assert_eq!(decoded.address(), any.address());
        assert_eq!(decoded.data(), any.data());
    }

    #[test]
    fn test_typed_custom_round_trip() {
        // The Custom variant has been removed; an unrecognised type tag must now
        // error rather than round-trip as an opaque blob.
        let type_id = ChunkTypeId::custom(200);
        let address: ChunkAddress = [0x11u8; 32].into();
        let data = Bytes::from_static(b"opaque custom chunk bytes");

        let encoded = {
            let mut out = Vec::with_capacity(1 + data.len());
            out.push(type_id.as_u8());
            out.extend_from_slice(&data);
            out
        };

        let result = DefaultAnyChunk::from_typed_bytes(&address, &encoded);
        assert!(
            result.is_err(),
            "unrecognised type tags must error now that Custom is removed",
        );
    }

    #[test]
    fn test_typed_dispatch_by_tag_not_trial_parse() {
        // A CONTENT-tagged payload must decode as Content, even though the old
        // address-disambiguating path would also attempt a SOC parse.
        let content = DefaultContentChunk::new(&b"dispatch sanity"[..]).unwrap();
        let address = *content.address();
        let any: DefaultAnyChunk = content.into();

        let encoded = any.to_typed_bytes();
        let decoded = DefaultAnyChunk::from_typed_bytes(&address, &encoded).unwrap();

        // Returned variant matches the tag exactly.
        assert!(decoded.is_content());
        assert!(!decoded.is_single_owner());
    }

    #[test]
    fn test_typed_decode_empty_input_errors() {
        let address: ChunkAddress = [0u8; 32].into();
        let result = DefaultAnyChunk::from_typed_bytes(&address, &[]);
        assert!(result.is_err(), "empty input must error, not panic");
    }

    #[test]
    fn test_typed_decode_address_mismatch_errors() {
        let content = DefaultContentChunk::new(&b"chunk A"[..]).unwrap();
        let encoded = DefaultAnyChunk::from(content).to_typed_bytes();

        // Decode with a deliberately wrong address.
        let wrong: ChunkAddress = [0xFFu8; 32].into();
        let result = DefaultAnyChunk::from_typed_bytes(&wrong, &encoded);
        assert!(result.is_err(), "address mismatch must error");
    }

    #[test]
    fn test_typed_decode_corrupt_content_payload_errors() {
        let content = DefaultContentChunk::new(&b"corruptible"[..]).unwrap();
        let address = *content.address();

        // CONTENT tag but a too-short payload that cannot form a valid body.
        let bad = vec![ChunkTypeId::CONTENT.as_u8(), 0x00];
        let result = DefaultAnyChunk::from_typed_bytes(&address, &bad);
        assert!(result.is_err(), "corrupt content payload must error");
    }

    #[test]
    fn test_from_wire_bytes_content_round_trip() {
        let content = DefaultContentChunk::new(&b"hello wire world"[..]).unwrap();
        let address = *content.address();
        let any: DefaultAnyChunk = content.into();
        // Bare wire bytes carry no type tag.
        let wire = any.clone().into_bytes();

        let decoded = DefaultAnyChunk::from_wire_bytes(&address, wire.clone()).unwrap();
        assert!(decoded.is_content());
        assert_eq!(decoded.address(), any.address());
        assert_eq!(decoded.into_bytes(), wire);
    }

    #[test]
    fn test_from_wire_bytes_single_owner_round_trip() {
        let soc = sample_single_owner();
        let address = *soc.address();
        let any: DefaultAnyChunk = soc.into();
        let wire = any.clone().into_bytes();

        let decoded = DefaultAnyChunk::from_wire_bytes(&address, wire.clone()).unwrap();
        assert!(decoded.is_single_owner());
        assert_eq!(decoded.address(), any.address());
        assert_eq!(decoded.into_bytes(), wire);
    }

    #[test]
    fn test_from_wire_bytes_address_mismatch_errors() {
        let content = DefaultContentChunk::new(&b"chunk A"[..]).unwrap();
        let wire = DefaultAnyChunk::from(content).into_bytes();

        let wrong: ChunkAddress = [0xFFu8; 32].into();
        let result = DefaultAnyChunk::from_wire_bytes(&wrong, wire);
        assert!(result.is_err(), "address mismatch must error, not panic");
    }

    #[test]
    fn test_from_wire_bytes_unknown_shape_errors() {
        // Bare wire bytes carry no type tag, so only the standard content and
        // single-owner shapes can ever be recovered. Opaque bytes that parse as
        // neither (for the given address) simply error.
        let opaque = Bytes::from_static(b"opaque custom-looking payload bytes");
        let addr: ChunkAddress = [0x11u8; 32].into();
        assert!(DefaultAnyChunk::from_wire_bytes(&addr, opaque).is_err());
    }

    // --- transformed address (redistribution sampler) bee parity -------------
    //
    // nectar owns the parity oracle for the anchor-keyed transformed address.
    // The vectors below are taken from bee so that any drift in the prefixed
    // BMT or the single-owner outer wrap is caught here, at the primitive.

    /// bee `TestSampleVectorCAC` (`pkg/storer/sample_test.go`): a 4096-byte CAC
    /// whose payload is the repeating pattern `i % 256`, transformed under the
    /// anchor `swarm-test-anchor-deterministic!`.
    #[test]
    fn transformed_address_reproduces_bee_cac_vector() {
        use alloy_primitives::hex;

        const ANCHOR: &[u8] = b"swarm-test-anchor-deterministic!";
        // Plain (unprefixed) BMT root: the chunk's own content address.
        const WANT_CHUNK_ADDR: &str =
            "902406053a7a2f3a17f16097e1d0b4b6a4abeae6b84968f5503ae621f9522e16";
        // Anchor-keyed transformed address.
        const WANT_TRANSFORMED: &str =
            "9dee91d1ed794460474ffc942996bd713176731db4581a3c6470fe9862905a60";

        let mut payload = vec![0u8; 4096];
        for (i, b) in payload.iter_mut().enumerate() {
            *b = (i % 256) as u8;
        }

        let content = DefaultContentChunk::new(payload).unwrap();

        // The chunk's plain BMT address is the unprefixed root.
        assert_eq!(
            hex::encode(content.address().as_bytes()),
            WANT_CHUNK_ADDR,
            "plain BMT address must match bee's published vector",
        );

        let any: DefaultAnyChunk = content.into();
        let tr = any.transformed_address(ANCHOR);
        assert_eq!(
            hex::encode(tr.as_bytes()),
            WANT_TRANSFORMED,
            "CAC transformed address must match bee byte-for-byte",
        );
    }

    /// A single-owner chunk vector from bee's `TestMakeInclusionProofsRegression`
    /// oracle (anchor1 = `0x64`). Exercises the SOC path: the wrapped content
    /// chunk is re-hashed under the anchor, then the SOC transformed address is
    /// the plain `keccak256(soc_address || inner)`.
    #[test]
    fn transformed_address_reproduces_bee_soc_vector() {
        use alloy_primitives::hex;

        // anchor1 from the oracle, a single byte 0x64 (== 100).
        const ANCHOR: &[u8] = &[0x64];
        const WANT_CHUNK_ADDR: &str =
            "71d5144d0525b82cd550aa9254245c6195fdac9ccbb625eb45a0bfe244cb131f";
        const WANT_TRANSFORMED: &str =
            "521f50f895dc1deea14448c09ba8d9c510c5db09cd84c7ed8413b66f15fbc110";
        // Full SOC wire bytes: id(32) || signature(65) || span(8) || payload.
        const SOC_WIRE: &str = "82d0ed66f956ed70445d02df922606e02c11a79014cc441f9cc678275d260703\
            15c7157590f599a81b38834786f79f57a6a6626bbe8bf48fbbd6db3e55ad41c0\
            277d7d310bbefbb5d81d74605a5950171229ebdd320249ef5f8fd6b8dfe2bc7d\
            1c1a00000000000000556e73746f707061626c65206461746121204368756e6b\
            202331";

        let wire = hex::decode(SOC_WIRE.replace([' ', '\n'], "")).unwrap();
        let soc = DefaultSingleOwnerChunk::try_from(wire.as_slice()).unwrap();

        // Sanity: this SOC's own address matches the oracle.
        assert_eq!(
            hex::encode(soc.address().as_bytes()),
            WANT_CHUNK_ADDR,
            "SOC address must match bee's oracle",
        );

        // `unwrap_cac` must expose the wrapped content body with no manual
        // 32 + 65 header slicing; its span/payload feed the inner BMT.
        let cac = soc.unwrap_cac();
        assert_eq!(cac.span(), soc.body().span());
        assert_eq!(cac.data(), soc.body().data());

        let any: DefaultAnyChunk = soc.into();
        let tr = any.transformed_address(ANCHOR);
        assert_eq!(
            hex::encode(tr.as_bytes()),
            WANT_TRANSFORMED,
            "SOC transformed address must match bee byte-for-byte",
        );
    }
}
