//! Batch storage traits for persisting batch data.

use crate::{Batch, BatchId, PostageContext};

/// A trait for storing and retrieving batches.
///
/// Implementations may persist batches in memory, on disk, or retrieve
/// them from a remote source such as a blockchain node.
///
/// # Synchronous Design
///
/// The methods are synchronous. The known backends (in memory, redb) are
/// themselves synchronous, so there is no genuinely async work to drive here;
/// any async behaviour belongs at the true edges (a gRPC service, an FFI
/// boundary) where it is added by the edge, not by the store. Keeping the core
/// synchronous avoids colouring every caller with `async`, keeps the futures
/// `Send`-free on the wasm path, and makes this trait naturally object-safe (it
/// has an associated `Error` and no generic methods), a property the previous
/// async-in-trait shape did not have.
pub trait BatchStore {
    /// The error type returned by store operations.
    type Error: std::error::Error;

    /// Retrieves a batch by its ID.
    ///
    /// Returns `None` if the batch doesn't exist.
    fn get(&self, id: &BatchId) -> Result<Option<Batch>, Self::Error>;

    /// Stores or updates a batch.
    fn put(&self, batch: Batch) -> Result<(), Self::Error>;

    /// Removes a batch from the store.
    ///
    /// Returns `true` if the batch existed and was removed.
    fn remove(&self, id: &BatchId) -> Result<bool, Self::Error>;

    /// Checks if a batch exists in the store.
    fn contains(&self, id: &BatchId) -> Result<bool, Self::Error>;

    /// Returns the current postage context.
    fn context(&self) -> Result<PostageContext, Self::Error>;

    /// Updates the postage context.
    fn set_context(&self, state: PostageContext) -> Result<(), Self::Error>;

    /// Returns all batch IDs in the store.
    fn batch_ids(&self) -> Result<Vec<BatchId>, Self::Error>;

    /// Returns the number of batches in the store.
    fn count(&self) -> Result<usize, Self::Error>;
}

/// Extension methods for [`BatchStore`].
///
/// This is a plain synchronous extension trait; it carries no `Sync` bound,
/// because the methods are no longer `async` and therefore never need their
/// futures to be `Send` (which previously forced `Self: Sync`).
pub trait BatchStoreExt: BatchStore {
    /// Gets a batch and verifies it's usable.
    ///
    /// Returns an error if the batch doesn't exist, isn't usable yet,
    /// or has expired.
    fn get_usable(
        &self,
        id: &BatchId,
        confirmation_threshold: u64,
    ) -> Result<Batch, BatchStoreError<Self::Error>> {
        let batch = self
            .get(id)
            .map_err(BatchStoreError::Store)?
            .ok_or(BatchStoreError::NotFound(*id))?;

        let state = self.context().map_err(BatchStoreError::Store)?;

        if !batch.is_usable(state.block(), confirmation_threshold) {
            return Err(BatchStoreError::NotUsable {
                batch_id: *id,
                created: batch.start(),
                current: state.block(),
                threshold: confirmation_threshold,
            });
        }

        if batch.is_expired(state.total_amount()) {
            return Err(BatchStoreError::Expired {
                batch_id: *id,
                value: batch.value(),
                total_amount: state.total_amount(),
            });
        }

        Ok(batch)
    }
}

// Blanket implementation
impl<T: BatchStore> BatchStoreExt for T {}

/// Errors that can occur when working with a batch store.
#[derive(Debug)]
pub enum BatchStoreError<E> {
    /// The batch was not found in the store.
    NotFound(BatchId),
    /// The batch is not yet usable (needs more confirmations).
    NotUsable {
        /// The batch ID.
        batch_id: BatchId,
        /// Block when batch was created.
        created: u64,
        /// Current block number.
        current: u64,
        /// Required confirmations.
        threshold: u64,
    },
    /// The batch has expired.
    Expired {
        /// The batch ID.
        batch_id: BatchId,
        /// Current batch value.
        value: u128,
        /// Total amount consumed.
        total_amount: u128,
    },
    /// An error from the underlying store.
    Store(E),
}

impl<E: std::fmt::Display> std::fmt::Display for BatchStoreError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound(id) => write!(f, "batch not found: {}", id),
            Self::NotUsable {
                batch_id,
                created,
                current,
                threshold,
            } => write!(
                f,
                "batch {} not usable: created at block {}, current block {}, need {} confirmations",
                batch_id, created, current, threshold
            ),
            Self::Expired {
                batch_id,
                value,
                total_amount,
            } => write!(
                f,
                "batch {} expired: value {} <= total_amount {}",
                batch_id, value, total_amount
            ),
            Self::Store(e) => write!(f, "store error: {}", e),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for BatchStoreError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Store(e) => Some(e),
            _ => None,
        }
    }
}
