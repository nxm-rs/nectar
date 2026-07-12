//! Core traits for chunk types and operations.
//!
//! [`ChunkHeader`] is the predicate a chunk type *is*: its address derivation
//! and self-certification rule. [`ChunkOps`] is the header-free behaviour
//! every chunk value offers; [`Chunk`] ties a carrier to its header type.

use alloy_primitives::B256;
use bytes::{Bytes, BytesMut};

use crate::error::PrimitivesError;
use crate::wire;

use super::address::ChunkAddress;
use super::error::ChunkError;
use super::type_id::ChunkTypeId;
use super::type_tag::ChunkVersion;

/// Address-derivation and self-certification predicate of one chunk type.
///
/// A chunk is `header || span || payload`: the header is everything that
/// precedes the BMT body on the wire, and everything that turns the body hash
/// into an address. Implementing this trait defines a chunk type; custom
/// swarms use ids 128-255 ([`ChunkTypeId::custom`]). The trait is deliberately
/// unsealed: auditability lives in each network's closed envelope enum, not
/// in sealing.
///
/// ```
/// use alloy_primitives::{B256, Keccak256};
/// use nectar_primitives::bytes::BytesMut;
/// use nectar_primitives::chunk::{ChunkAddress, ChunkError, ChunkHeader};
/// use nectar_primitives::{ChunkTypeId, ChunkVersion, wire};
///
/// /// Custom headerless type: address = keccak256(0xC0 || body_hash).
/// struct TaggedHeader;
///
/// impl ChunkHeader for TaggedHeader {
///     const TYPE_ID: ChunkTypeId = ChunkTypeId::custom(200);
///     const VERSION: ChunkVersion = ChunkVersion::new(0);
///     const NAME: &'static str = "tagged";
///     const SIZE: usize = 0;
///
///     fn commit(&self, body_hash: B256) -> ChunkAddress {
///         let mut hasher = Keccak256::new();
///         hasher.update([0xC0]);
///         hasher.update(body_hash);
///         ChunkAddress::from(hasher.finalize())
///     }
///
///     fn validate(&self, body_hash: B256, expected: &ChunkAddress) -> Result<(), ChunkError> {
///         let actual = self.commit(body_hash);
///         if actual == *expected {
///             Ok(())
///         } else {
///             Err(ChunkError::verification_failed(*expected, actual))
///         }
///     }
///
///     fn seal_transformed(&self, _address: &ChunkAddress, transformed_root: B256) -> ChunkAddress {
///         ChunkAddress::from(transformed_root)
///     }
///
///     fn encode(&self, _out: &mut BytesMut) {}
///
///     fn decode(_cursor: &mut wire::Cursor<'_>) -> Result<Self, ChunkError> {
///         Ok(Self)
///     }
/// }
/// ```
pub trait ChunkHeader: Sized + Send + Sync + 'static {
    /// Wire-level type id of this chunk type.
    const TYPE_ID: ChunkTypeId;

    /// Revision of this type id's acceptance rule.
    const VERSION: ChunkVersion;

    /// Human-readable type name.
    const NAME: &'static str;

    /// Exact wire width of the header in bytes; [`encode`](Self::encode)
    /// writes exactly this many.
    const SIZE: usize;

    /// Derive the chunk address this header commits to over `body_hash`.
    ///
    /// Total: inputs that cannot certify still commit to *some* address,
    /// which [`validate`](Self::validate) then rejects.
    fn commit(&self, body_hash: B256) -> ChunkAddress;

    /// Certify that this header and `body_hash` derive `expected`.
    ///
    /// Required, deliberately without a default: an address-compare-only
    /// implementation would accept single-owner chunks whose signatures do
    /// not recover, so every header must state its full acceptance rule.
    fn validate(&self, body_hash: B256, expected: &ChunkAddress) -> Result<(), ChunkError>;

    /// Seal the anchor-keyed `transformed_root` of the body into the chunk's
    /// transformed address (the redistribution sampler's re-hash).
    fn seal_transformed(&self, address: &ChunkAddress, transformed_root: B256) -> ChunkAddress;

    /// Append the wire header bytes, exactly [`SIZE`](Self::SIZE) of them,
    /// to `out`.
    fn encode(&self, out: &mut BytesMut);

    /// Read a header from the cursor, consuming exactly [`SIZE`](Self::SIZE)
    /// bytes.
    fn decode(cursor: &mut wire::Cursor<'_>) -> Result<Self, ChunkError>;
}

/// Header-free behaviour common to every chunk value.
///
/// No `Header` associated type, so the boundary enum
/// ([`AnyChunk`](super::any_chunk::AnyChunk)) implements it alongside the
/// concrete carriers and generic code accepts both through one bound.
pub trait ChunkOps: Send + Sync + 'static {
    /// Get the address of this chunk.
    fn address(&self) -> &ChunkAddress;

    /// Get the raw payload of this chunk (the BMT body without its span).
    fn data(&self) -> &Bytes;

    /// Get the total wire size of this chunk in bytes.
    fn size(&self) -> usize;

    /// Get the span (logical data length) of this chunk: the BMT span of its
    /// underlying body.
    fn span(&self) -> u64;

    /// Certify this chunk against an expected address.
    ///
    /// Required, deliberately without a default: verification must run the
    /// header's full acceptance rule ([`ChunkHeader::validate`]), never a bare
    /// compare against the chunk's own derived address.
    fn verify(&self, expected: &ChunkAddress) -> Result<(), PrimitivesError>;

    /// Compute the anchor-keyed *transformed address* of this chunk.
    ///
    /// The transformed address is the redistribution sampler's per-round,
    /// per-node re-hash of a chunk. It is a prefixed BMT root keyed by the
    /// node's `anchor`, used to order reserve chunks deterministically while
    /// binding the ordering to the proving node.
    ///
    /// # Derivation
    ///
    /// - The anchor-keyed BMT root of the chunk body is computed by
    ///   [`BmtBody::transformed_root`](super::bmt_body::BmtBody::transformed_root)
    ///   on the chunk's borrowed body, which mixes the anchor into *every* node
    ///   hash. For a SOC the wrapped content body is the one re-hashed.
    /// - The root is then sealed into the transformed address by the chunk's
    ///   header predicate ([`ChunkHeader::seal_transformed`]): the root itself
    ///   for a CAC, the plain (unprefixed, no anchor)
    ///   `keccak256(soc_address || inner)` for a SOC.
    ///
    /// # Endianness
    ///
    /// The span is serialised little-endian inside the BMT. Do not confuse this
    /// with the big-endian encodings used elsewhere on the redistribution wire
    /// (e.g. proof witness indices); the BMT span is always LE.
    fn transformed_address(&self, anchor: &[u8]) -> ChunkAddress;

    /// Convert this chunk into its bare wire bytes (`header || span ||
    /// payload`), the inverse of the carrier's `TryFrom<Bytes>` decode.
    fn into_bytes(self) -> Bytes
    where
        Self: Sized;
}

/// Carrier trait tying a chunk to its wire header type.
///
/// The behaviour surface lives on the [`ChunkOps`] supertrait; this trait adds
/// only the header association, so it is exactly the part the header-free
/// boundary enum cannot implement.
pub trait Chunk: ChunkOps {
    /// The header type for this chunk
    type Header: ChunkHeader;

    /// Get the header for this chunk
    fn header(&self) -> &Self::Header;
}
