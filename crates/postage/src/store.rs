//! Batch storage traits for persisting batch data.

use crate::{Batch, BatchId, ChainState};

/// A trait for storing and retrieving batches.
///
/// Implementations may persist batches in memory, on disk, or retrieve
/// them from a remote source such as a blockchain node.
///
/// # Async Design
///
/// This trait uses async methods to support both local (fast) and
/// remote (potentially slow) storage backends without blocking.
pub trait BatchStore {
    /// The error type returned by store operations.
    type Error: std::error::Error;

    /// Retrieves a batch by its ID.
    ///
    /// Returns `None` if the batch doesn't exist.
    fn get(
        &self,
        id: &BatchId,
    ) -> impl std::future::Future<Output = Result<Option<Batch>, Self::Error>> + Send;

    /// Stores or updates a batch.
    fn put(
        &self,
        batch: Batch,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send;

    /// Removes a batch from the store.
    ///
    /// Returns `true` if the batch existed and was removed.
    fn remove(
        &self,
        id: &BatchId,
    ) -> impl std::future::Future<Output = Result<bool, Self::Error>> + Send;

    /// Checks if a batch exists in the store.
    fn contains(
        &self,
        id: &BatchId,
    ) -> impl std::future::Future<Output = Result<bool, Self::Error>> + Send;

    /// Returns the current chain state.
    fn chain_state(
        &self,
    ) -> impl std::future::Future<Output = Result<ChainState, Self::Error>> + Send;

    /// Updates the chain state.
    fn set_chain_state(
        &self,
        state: ChainState,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send;

    /// Returns all batch IDs in the store.
    fn batch_ids(
        &self,
    ) -> impl std::future::Future<Output = Result<Vec<BatchId>, Self::Error>> + Send;

    /// Returns the number of batches in the store.
    fn count(&self) -> impl std::future::Future<Output = Result<usize, Self::Error>> + Send;
}

/// Extension methods for [`BatchStore`].
pub trait BatchStoreExt: BatchStore + Sync {
    /// Gets a batch and verifies it's usable.
    ///
    /// Returns an error if the batch doesn't exist, isn't usable yet,
    /// or has expired.
    fn get_usable(
        &self,
        id: &BatchId,
        confirmation_threshold: u64,
    ) -> impl std::future::Future<Output = Result<Batch, BatchStoreError<Self::Error>>> + Send {
        async move {
            let batch = self
                .get(id)
                .await
                .map_err(BatchStoreError::Store)?
                .ok_or(BatchStoreError::NotFound(*id))?;

            let state = self.chain_state().await.map_err(BatchStoreError::Store)?;

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
}

// Blanket implementation
impl<T: BatchStore + Sync> BatchStoreExt for T {}

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
            BatchStoreError::NotFound(id) => write!(f, "batch not found: {}", id),
            BatchStoreError::NotUsable {
                batch_id,
                created,
                current,
                threshold,
            } => write!(
                f,
                "batch {} not usable: created at block {}, current block {}, need {} confirmations",
                batch_id, created, current, threshold
            ),
            BatchStoreError::Expired {
                batch_id,
                value,
                total_amount,
            } => write!(
                f,
                "batch {} expired: value {} <= total_amount {}",
                batch_id, value, total_amount
            ),
            BatchStoreError::Store(e) => write!(f, "store error: {}", e),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for BatchStoreError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            BatchStoreError::Store(e) => Some(e),
            _ => None,
        }
    }
}
