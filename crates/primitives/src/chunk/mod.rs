//! Chunk types and operations
//!
//! This module provides implementations of various chunk types used in the storage system,
//! along with functionality for creating, parsing, and verifying chunks.
//!
//! # Chunk Type System
//!
//! The chunk system is built around a hierarchy of traits:
//!
//! - [`ChunkHeader`] - Address-derivation and self-certification predicate of
//!   a chunk type ([`CacHeader`], [`SocHeader`])
//! - [`ChunkInner`] - The single carrier: one header plus one BMT body;
//!   [`ContentChunk`] and [`SingleOwnerChunk`] are its aliases
//! - [`ChunkOps`] - Header-free behaviour shared by concrete chunks and
//!   [`AnyChunk`]
//! - [`HeaderedChunk`] - Ties a carrier to its header type
//! - [`ChunkType`] - Adds compile-time type identification
//! - [`ChunkRegistry`] - Compile-time registry of the chunk types a network
//!   accepts, keyed by its closed envelope type
//! - [`Chunk`] - The public chunk currency: a registry envelope under a
//!   sealed [`TrustState`], parse then verify from every source, with
//!   [`TrustedSource`] gating the single trusted-store skip
//!
//! # Type-Erased Chunks
//!
//! The [`AnyChunk`] enum provides runtime polymorphism for chunks without
//! requiring object-safe traits.
//!
//! # Extension
//!
//! Custom chunk types are a compile-time, per-network affair. Nothing here
//! needs opening to admit one: [`ChunkHeader`] is unsealed, [`ChunkInner`]
//! is generic over any header, [`ChunkRegistry`] is public, and ids 128-255
//! are reserved for custom types ([`ChunkTypeId::custom`]). The pattern:
//!
//! 1. Implement [`ChunkHeader`] for the new type's header under a custom id
//!    (the trait's own example shows a complete implementation).
//! 2. Own a closed envelope enum over the accepted standard members plus
//!    the custom type, implement [`ChunkOps`] on it by delegation, and
//!    expose it through a [`ChunkRegistry`] implementation.
//!
//! Everything generic over the registry or [`ChunkOps`] - the typestate
//! [`Chunk`], the [`store`](crate::store) traits - accepts the new registry
//! as-is. The envelope being downstream-owned and closed makes growing the
//! accepted set a deliberate, loud change: every match over it must be
//! extended, and nothing decodes a type the network did not choose.
//!
//! A runtime `type id -> decoder` registry, equivalently an open
//! `AnyChunk::Other` variant, is rejected rather than deferred: acceptance
//! is self-certifying per member predicate, so a chunk of a type unknown to
//! a node cannot be validated or admitted, and a runtime-unknown chunk can
//! never cross the trust boundary. Custom types are part of a network's
//! definition (a custom swarm, or a coordinated fork of an existing one),
//! not plugins on a running network.
//!
//! Two obligations transfer to the grown registry:
//!
//! - Domain separation on [`commit`](ChunkHeader::commit): no byte string
//!   may certify under two members at any address. A member certifies only
//!   at its self-derived address, so a custom `commit` must derive
//!   addresses distinct from every other member's over the same bytes; mix
//!   the type's own tag into the hash. The standard pair is swept by the
//!   `chunk_domain_separation` fuzz target; extend the sweep to the new
//!   member pairs.
//! - Tag uniqueness: force the compile-time guard with a
//!   `const _: () = MyRegistry::DISTINCT_TAGS;` item.
//!
//! A derive for the envelope delegation is deferred until a registry has a
//! third member: hand-written impls are small at two variants, and a derive
//! can be added later without API change.
//!
//! ```
//! use bytes::Bytes;
//! use nectar_primitives::chunk::{ChunkAddress, ChunkError, ChunkHeader};
//! use nectar_primitives::{
//!     CacHeader, Chunk, ChunkInner, ChunkOps, ChunkRegistry, ChunkTypeInfo, ChunkTypeTag,
//!     ContentChunk, Result, Unverified,
//! };
//! # use alloy_primitives::{Address, B256, Keccak256};
//! # use nectar_primitives::bytes::BytesMut;
//! # use nectar_primitives::{ChunkTypeId, ChunkVersion, wire};
//! #
//! # /// Headerless custom type: address = keccak256(type id || body hash).
//! # #[derive(Clone)]
//! # struct TaggedHeader;
//! #
//! # impl ChunkHeader for TaggedHeader {
//! #     const TYPE_ID: ChunkTypeId = ChunkTypeId::custom(200);
//! #     const VERSION: ChunkVersion = ChunkVersion::new(0);
//! #     const NAME: &'static str = "tagged";
//! #     const SIZE: usize = 0;
//! #
//! #     fn commit(&self, body_hash: B256) -> ChunkAddress {
//! #         let mut hasher = Keccak256::new();
//! #         hasher.update([Self::TYPE_ID.as_u8()]);
//! #         hasher.update(body_hash);
//! #         ChunkAddress::from(hasher.finalize())
//! #     }
//! #
//! #     fn validate(
//! #         &self,
//! #         body_hash: B256,
//! #         expected: &ChunkAddress,
//! #     ) -> core::result::Result<(), ChunkError> {
//! #         let actual = self.commit(body_hash);
//! #         if actual == *expected {
//! #             Ok(())
//! #         } else {
//! #             Err(ChunkError::verification_failed(*expected, actual))
//! #         }
//! #     }
//! #
//! #     fn seal_transformed(&self, _address: &ChunkAddress, root: B256) -> ChunkAddress {
//! #         ChunkAddress::from(root)
//! #     }
//! #
//! #     fn encode(&self, _out: &mut BytesMut) {}
//! #
//! #     fn decode(_cursor: &mut wire::Cursor<'_>) -> core::result::Result<Self, ChunkError> {
//! #         Ok(Self)
//! #     }
//! # }
//! // impl ChunkHeader for TaggedHeader { ... } as in the trait's example.
//! type TaggedChunk = ChunkInner<TaggedHeader>;
//!
//! /// Downstream-owned closed envelope: the accepted standard members plus
//! /// the custom type. Growing it is a deliberate change here, not upstream.
//! #[derive(Clone)]
//! enum ForkChunk {
//!     Content(ContentChunk),
//!     Tagged(TaggedChunk),
//! }
//!
//! impl ChunkOps for ForkChunk {
//!     fn address(&self) -> &ChunkAddress {
//!         match self {
//!             Self::Content(c) => c.address(),
//!             Self::Tagged(c) => c.address(),
//!         }
//!     }
//!
//!     fn verify(&self, expected: &ChunkAddress) -> Result<()> {
//!         match self {
//!             Self::Content(c) => c.verify(expected),
//!             Self::Tagged(c) => c.verify(expected),
//!         }
//!     }
//! #
//! #     fn data(&self) -> &Bytes {
//! #         match self {
//! #             Self::Content(c) => c.data(),
//! #             Self::Tagged(c) => c.data(),
//! #         }
//! #     }
//! #
//! #     fn size(&self) -> usize {
//! #         match self {
//! #             Self::Content(c) => c.size(),
//! #             Self::Tagged(c) => c.size(),
//! #         }
//! #     }
//! #
//! #     fn span(&self) -> u64 {
//! #         match self {
//! #             Self::Content(c) => c.span(),
//! #             Self::Tagged(c) => c.span(),
//! #         }
//! #     }
//! #
//! #     fn owner(&self) -> Option<Address> {
//! #         match self {
//! #             Self::Content(c) => c.owner(),
//! #             Self::Tagged(c) => c.owner(),
//! #         }
//! #     }
//! #
//! #     fn transformed_address(&self, anchor: &[u8]) -> ChunkAddress {
//! #         match self {
//! #             Self::Content(c) => c.transformed_address(anchor),
//! #             Self::Tagged(c) => c.transformed_address(anchor),
//! #         }
//! #     }
//! #
//! #     fn into_bytes(self) -> Bytes {
//! #         match self {
//! #             Self::Content(c) => c.into_bytes(),
//! #             Self::Tagged(c) => c.into_bytes(),
//! #         }
//! #     }
//!     // data, size, span, owner, transformed_address, and into_bytes
//!     // delegate the same way.
//! }
//!
//! const CAC_TAG: ChunkTypeTag = ChunkTypeTag::new(CacHeader::TYPE_ID, CacHeader::VERSION);
//! const TAGGED_TAG: ChunkTypeTag = ChunkTypeTag::new(TaggedHeader::TYPE_ID, TaggedHeader::VERSION);
//!
//! struct ForkChunkSet;
//!
//! impl ChunkRegistry for ForkChunkSet {
//!     type Envelope = ForkChunk;
//!
//!     const MEMBERS: &'static [ChunkTypeInfo] = &[
//!         ChunkTypeInfo::of::<CacHeader>(),
//!         ChunkTypeInfo::of::<TaggedHeader>(),
//!     ];
//!
//!     fn parse_typed(bytes: &[u8]) -> Result<ForkChunk> {
//!         let (tag, payload) = bytes.split_first_chunk::<2>().ok_or_else(|| {
//!             ChunkError::invalid_format("typed encoding shorter than the two-byte tag")
//!         })?;
//!         let payload = Bytes::copy_from_slice(payload);
//!         match ChunkTypeTag::from(*tag) {
//!             t if t == CAC_TAG => Ok(ForkChunk::Content(payload.try_into()?)),
//!             t if t == TAGGED_TAG => Ok(ForkChunk::Tagged(payload.try_into()?)),
//!             t => Err(ChunkError::unsupported_tag(t).into()),
//!         }
//!     }
//! #
//! #     fn decode_wire(address: &ChunkAddress, data: Bytes) -> Result<ForkChunk> {
//! #         if let Ok(content) = ContentChunk::try_from(data.clone()) {
//! #             if content.verify(address).is_ok() {
//! #                 return Ok(ForkChunk::Content(content));
//! #             }
//! #         }
//! #         let tagged = TaggedChunk::try_from(data)?;
//! #         tagged.verify(address)?;
//! #         Ok(ForkChunk::Tagged(tagged))
//! #     }
//! #
//! #     fn encode_typed(chunk: &ForkChunk) -> Vec<u8> {
//! #         let (tag, wire) = match chunk {
//! #             ForkChunk::Content(c) => (CAC_TAG, c.clone().into_bytes()),
//! #             ForkChunk::Tagged(c) => (TAGGED_TAG, c.clone().into_bytes()),
//! #         };
//! #         let mut out = tag.to_bytes().to_vec();
//! #         out.extend_from_slice(&wire);
//! #         out
//! #     }
//!     // decode_wire trial-parses members in declaration order with the
//!     // address as disambiguator; encode_typed prepends the member's
//!     // two-byte tag to its bare wire bytes.
//! }
//!
//! const _: () = ForkChunkSet::DISTINCT_TAGS;
//!
//! // The typestate currency accepts the grown registry unchanged.
//! let payload = b"custom swarm payload";
//! let mut wire_bytes = u64::try_from(payload.len()).unwrap().to_le_bytes().to_vec();
//! wire_bytes.extend_from_slice(payload);
//! let tagged = TaggedChunk::try_from(Bytes::from(wire_bytes)).unwrap();
//! let claimed = *tagged.address();
//!
//! let typed = ForkChunkSet::encode_typed(&ForkChunk::Tagged(tagged));
//! let verified = Chunk::<Unverified, ForkChunkSet>::parse(claimed, &typed)
//!     .unwrap()
//!     .verify()
//!     .unwrap();
//! assert_eq!(verified.address(), &claimed);
//! ```

mod address;
mod any_chunk;
mod bmt_body;
mod chunk_type;
mod content;
pub mod encryption;
pub(crate) mod error;
mod inner;
mod reference;
mod registry;
mod single_owner;
mod soc_id;
mod traits;
mod trust;
mod type_id;
mod type_tag;

#[cfg(target_arch = "wasm32")]
pub mod wasm;

// Re-export the address type, error type, and core traits
pub use address::ChunkAddress;
pub use error::ChunkError;
pub use inner::ChunkInner;
pub use traits::{ChunkHeader, ChunkOps, HeaderedChunk};

// Re-export the typestate trust carrier
pub use trust::{Chunk, IntoVerified, TrustState, TrustedSource, Unverified, Verified};

// Re-export the reference types
pub use reference::{ChunkRef, RefKind, Reference, WrongRefKind};

// Re-export the type system
pub use any_chunk::AnyChunk;
pub use chunk_type::ChunkType;
pub use registry::{
    AnyChunkSet, ChunkRegistry, ChunkTypeInfo, ContentOnlyChunkSet, StandardChunkSet,
};
pub use type_id::ChunkTypeId;
pub use type_tag::{ChunkTypeTag, ChunkVersion, TagWireError};

// Re-export the concrete chunk types and their headers
#[cfg(feature = "encryption")]
pub use content::EncryptedContentChunk;
pub use content::{CacHeader, ContentChunk};
#[cfg(feature = "encryption")]
pub use encryption::ChunkEncrypt;
pub use single_owner::{SingleOwnerChunk, SocHeader};
pub use soc_id::SocId;
