//! Type-erased chunk type
//!
//! This module provides [`AnyChunk`], an enum that can hold any chunk type
//! for runtime polymorphism without requiring trait objects.

use bytes::Bytes;

use crate::bmt::DEFAULT_BODY_SIZE;
use crate::error::Result;

use super::chunk_type::ChunkType;
use super::content::ContentChunk;
use super::single_owner::SingleOwnerChunk;
use super::traits::{Chunk, ChunkAddress};
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
    /// A custom chunk type (for extensibility).
    ///
    /// This variant allows storing chunks of types not known at compile time.
    /// The raw bytes are preserved for potential later processing.
    Custom {
        /// The chunk type identifier.
        type_id: ChunkTypeId,
        /// The chunk's address.
        address: ChunkAddress,
        /// The raw chunk data.
        data: Bytes,
    },
}

impl<const BODY_SIZE: usize> AnyChunk<BODY_SIZE> {
    /// Get the address of this chunk.
    pub fn address(&self) -> &ChunkAddress {
        match self {
            Self::Content(c) => c.address(),
            Self::SingleOwner(c) => c.address(),
            Self::Custom { address, .. } => address,
        }
    }

    /// Get the raw data contained in this chunk.
    pub fn data(&self) -> &Bytes {
        match self {
            Self::Content(c) => c.data(),
            Self::SingleOwner(c) => c.data(),
            Self::Custom { data, .. } => data,
        }
    }

    /// Get the type ID of this chunk.
    pub const fn type_id(&self) -> ChunkTypeId {
        match self {
            Self::Content(_) => ChunkTypeId::CONTENT,
            Self::SingleOwner(_) => ChunkTypeId::SINGLE_OWNER,
            Self::Custom { type_id, .. } => *type_id,
        }
    }

    /// Get the total size of this chunk in bytes.
    pub fn size(&self) -> usize {
        match self {
            Self::Content(c) => c.size(),
            Self::SingleOwner(c) => c.size(),
            Self::Custom { data, .. } => data.len(),
        }
    }

    /// Get the span (logical data length) of this chunk.
    ///
    /// For content chunks and single-owner chunks, this returns the BMT span.
    /// For custom chunks, the span is not available (returns 0).
    pub fn span(&self) -> u64 {
        match self {
            Self::Content(c) => super::traits::BmtChunk::span(c),
            Self::SingleOwner(c) => super::traits::BmtChunk::span(c),
            Self::Custom { .. } => 0, // Custom chunks don't have span info
        }
    }

    /// Verify that this chunk's address matches an expected address.
    pub fn verify(&self, expected: &ChunkAddress) -> Result<()> {
        match self {
            Self::Content(c) => c.verify(expected),
            Self::SingleOwner(c) => c.verify(expected),
            Self::Custom { address, .. } => {
                if address != expected {
                    return Err(
                        super::error::ChunkError::verification_failed(*expected, *address).into(),
                    );
                }
                Ok(())
            }
        }
    }

    /// Convert this chunk into its serialized bytes representation.
    pub fn into_bytes(self) -> Bytes {
        match self {
            Self::Content(c) => c.into(),
            Self::SingleOwner(c) => c.into(),
            Self::Custom { data, .. } => data,
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

    /// Check if this is a custom chunk type.
    pub const fn is_custom(&self) -> bool {
        matches!(self, Self::Custom { .. })
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
    /// to trial-parse both the content and single-owner shapes, and it
    /// round-trips the [`AnyChunk::Custom`] variant.
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
    /// redb storage key or the wire message address field); it is used to
    /// verify standard chunks and to populate the [`AnyChunk::Custom`] variant,
    /// whose address is not otherwise recoverable from the payload.
    ///
    /// Decoding dispatches by the type tag and therefore never trial-parses the
    /// other chunk shape.
    ///
    /// # Errors
    ///
    /// Returns an error (and never panics) when:
    /// - the input is empty (no type tag),
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
            _ => Ok(Self::Custom {
                type_id,
                address: *address,
                data: Bytes::copy_from_slice(payload),
            }),
        }
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

impl<const BODY_SIZE: usize> PartialEq for AnyChunk<BODY_SIZE> {
    fn eq(&self, other: &Self) -> bool {
        self.address() == other.address()
    }
}

impl<const BODY_SIZE: usize> Eq for AnyChunk<BODY_SIZE> {}

#[cfg(test)]
mod tests {
    use super::super::traits::Chunk;
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
        assert!(!any.is_custom());
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
        let id = alloy_primitives::B256::ZERO;
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
        let type_id = ChunkTypeId::custom(200);
        let address: ChunkAddress = [0x11u8; 32].into();
        let data = Bytes::from_static(b"opaque custom chunk bytes");

        let any: DefaultAnyChunk = AnyChunk::Custom {
            type_id,
            address,
            data: data.clone(),
        };

        let encoded = any.to_typed_bytes();
        assert_eq!(encoded[0], 200, "custom tag preserved");

        let decoded = DefaultAnyChunk::from_typed_bytes(&address, &encoded).unwrap();
        assert!(decoded.is_custom());
        assert_eq!(decoded.type_id(), type_id);
        assert_eq!(*decoded.address(), address);
        assert_eq!(decoded.data(), &data);
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
}
