//! Error types for feed operations.

use alloc::sync::Arc;

use alloy_primitives::Address;
use nectar_primitives::PrimitivesError;
use nectar_primitives::chunk::ChunkAddress;

/// Result type alias for feed operations.
pub type Result<T> = core::result::Result<T, FeedError>;

/// Errors that can occur while reading from or writing to a feed.
///
/// [`strum::IntoStaticStr`] gives every variant a stable snake_case label for
/// metrics, with explicit overrides on the variants that wrap an upstream error
/// so the label does not leak field names.
#[derive(thiserror::Error, Debug, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum FeedError {
    /// No update chunk was found at the expected address.
    #[error("feed update not found at {address}")]
    NotFound {
        /// The address that was queried.
        address: ChunkAddress,
    },
    /// The retrieved chunk is owned by an address other than the feed owner.
    #[error("owner mismatch: expected {expected}, got {actual}")]
    OwnerMismatch {
        /// The expected feed owner.
        expected: Address,
        /// The owner recovered from the retrieved chunk.
        actual: Address,
    },
    /// The retrieved chunk is not a single-owner chunk.
    #[error("retrieved chunk is not a single-owner chunk")]
    NotSingleOwner,
    /// The index space for this feed scheme is exhausted.
    #[error("feed index exhausted")]
    IndexExhausted,
    /// An index could not be parsed or is out of range.
    #[error("invalid index: {0}")]
    InvalidIndex(&'static str),
    /// Error from primitives (chunk creation, signing, BMT, etc.).
    #[error(transparent)]
    #[strum(serialize = "primitives")]
    Primitives(#[from] PrimitivesError),
    /// Error from the typed chunk store during a get operation.
    #[error("store get error: {source}")]
    #[strum(serialize = "store_get")]
    StoreGet {
        /// Original store error, preserved for downcasting.
        source: Arc<dyn core::error::Error + Send + Sync>,
    },
    /// Error from the typed chunk store during a put operation.
    #[error("store put error: {source}")]
    #[strum(serialize = "store_put")]
    StorePut {
        /// Original store error, preserved for downcasting.
        source: Arc<dyn core::error::Error + Send + Sync>,
    },
}

/// These two constructors exist instead of `#[from]` because both store
/// variants ([`FeedError::StoreGet`] and [`FeedError::StorePut`]) wrap the same
/// erased source type (`Arc<dyn Error + Send + Sync>`). A single blanket
/// `#[from]` could not disambiguate which of the two variants to build, and the
/// constructors are not trivial renames: each performs the `Arc::new` erasure
/// from a concrete store error into the trait object.
impl FeedError {
    /// Wrap a chunk store get error.
    pub fn store_get<E: core::error::Error + Send + Sync + 'static>(source: E) -> Self {
        Self::StoreGet {
            source: Arc::new(source),
        }
    }

    /// Wrap a chunk store put error.
    pub fn store_put<E: core::error::Error + Send + Sync + 'static>(source: E) -> Self {
        Self::StorePut {
            source: Arc::new(source),
        }
    }
}
