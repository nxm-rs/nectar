//! Feed error type.

use alloy_primitives::Address;
use nectar_primitives::PrimitivesError;
use nectar_primitives::chunk::ChunkAddress;
use nectar_primitives::store::{BoxedError, MaybeSend, MaybeSync};

/// Result alias for feed operations.
pub type Result<T, E = FeedError> = core::result::Result<T, E>;

/// Feed read or write failure.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum FeedError {
    /// The store returned a chunk for an address other than the queried slot.
    #[error("address mismatch: expected {expected}, got {actual}")]
    AddressMismatch {
        /// The derived update address.
        expected: ChunkAddress,
        /// The address of the returned chunk.
        actual: ChunkAddress,
    },
    /// The chunk at a feed slot is not a single-owner chunk.
    #[error("chunk at {0} is not a single-owner chunk")]
    NotSingleOwner(ChunkAddress),
    /// The signer is not the feed owner.
    #[error("owner mismatch: feed owner {expected}, signer {actual}")]
    OwnerMismatch {
        /// The feed owner.
        expected: Address,
        /// The address recovered from the signature.
        actual: Address,
    },
    /// The index space has no further position.
    #[error("feed index exhausted")]
    Exhausted,
    /// Chunk construction or certification failed.
    #[error(transparent)]
    Chunk(#[from] PrimitivesError),
    /// The chunk store failed; the backend error survives as the source.
    #[error("store operation failed")]
    Store(#[source] BoxedError),
}

impl FeedError {
    /// Wrap a store backend error.
    pub fn store<E>(source: E) -> Self
    where
        E: core::error::Error + MaybeSend + MaybeSync + 'static,
    {
        Self::Store(Box::new(source))
    }
}
