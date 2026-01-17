//! Batch factory traits for creating batches.

use crate::{Batch, BatchId, BatchParams};

/// The result of creating a batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateResult {
    /// The created batch.
    pub batch: Batch,
    /// The transaction hash (if created on-chain).
    pub tx_hash: Option<alloy_primitives::B256>,
}

/// A trait for creating postage batches.
///
/// Implementations may create batches on-chain (by sending transactions
/// to the postage stamp contract) or in-memory for testing.
pub trait BatchFactory {
    /// The error type returned by factory operations.
    type Error: std::error::Error;

    /// Creates a new batch with the given parameters.
    ///
    /// For on-chain implementations, this sends a transaction to the
    /// postage stamp contract and waits for confirmation.
    ///
    /// # Arguments
    ///
    /// * `params` - The batch parameters (owner, depth, bucket_depth, etc.)
    ///
    /// # Returns
    ///
    /// A `CreateResult` containing the created batch and optional transaction hash.
    fn create(
        &self,
        params: BatchParams,
    ) -> impl std::future::Future<Output = Result<CreateResult, Self::Error>> + Send;

    /// Tops up a batch with additional funds.
    ///
    /// # Arguments
    ///
    /// * `batch_id` - The ID of the batch to top up
    /// * `amount` - The amount to add to the batch
    ///
    /// # Returns
    ///
    /// The new normalized balance of the batch.
    fn top_up(
        &self,
        batch_id: BatchId,
        amount: u128,
    ) -> impl std::future::Future<Output = Result<u128, Self::Error>> + Send;

    /// Dilutes a batch by increasing its depth.
    ///
    /// This doubles the capacity of the batch for each depth increase,
    /// but halves the remaining TTL.
    ///
    /// # Arguments
    ///
    /// * `batch_id` - The ID of the batch to dilute
    /// * `new_depth` - The new depth (must be greater than current depth)
    fn dilute(
        &self,
        batch_id: BatchId,
        new_depth: u8,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send;
}

/// An in-memory batch factory for testing.
///
/// This implementation creates batches in memory without any blockchain
/// interaction. Useful for unit tests and local development.
#[derive(Debug)]
pub struct MemoryBatchFactory {
    /// Counter for generating unique batch IDs.
    next_id: std::sync::atomic::AtomicU64,
    /// The current block number (for start block).
    current_block: u64,
}

impl MemoryBatchFactory {
    /// Creates a new memory batch factory.
    pub fn new(current_block: u64) -> Self {
        Self {
            next_id: std::sync::atomic::AtomicU64::new(0),
            current_block,
        }
    }

    /// Sets the current block number.
    pub fn set_current_block(&mut self, block: u64) {
        self.current_block = block;
    }

    fn generate_batch_id(&self) -> BatchId {
        use alloy_primitives::B256;

        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let mut bytes = [0u8; 32];
        bytes[24..32].copy_from_slice(&id.to_be_bytes());
        B256::from(bytes)
    }
}

impl Default for MemoryBatchFactory {
    fn default() -> Self {
        Self::new(0)
    }
}

/// Error type for memory batch factory operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemoryBatchError {
    /// The batch was not found.
    NotFound(BatchId),
    /// The batch is immutable and cannot be diluted.
    Immutable(BatchId),
    /// Invalid depth for dilution.
    InvalidDepth {
        /// The batch ID.
        batch_id: BatchId,
        /// Current depth.
        current: u8,
        /// Requested depth.
        requested: u8,
    },
}

impl std::fmt::Display for MemoryBatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MemoryBatchError::NotFound(id) => write!(f, "batch not found: {}", id),
            MemoryBatchError::Immutable(id) => write!(f, "batch is immutable: {}", id),
            MemoryBatchError::InvalidDepth {
                batch_id,
                current,
                requested,
            } => write!(
                f,
                "invalid depth for batch {}: current {}, requested {}",
                batch_id, current, requested
            ),
        }
    }
}

impl std::error::Error for MemoryBatchError {}

impl BatchFactory for MemoryBatchFactory {
    type Error = std::convert::Infallible;

    async fn create(&self, params: BatchParams) -> Result<CreateResult, Self::Error> {
        let batch_id = self.generate_batch_id();

        let batch = Batch::new(
            batch_id,
            params.amount,
            self.current_block,
            params.owner,
            params.depth,
            params.bucket_depth,
            params.immutable,
        );

        Ok(CreateResult {
            batch,
            tx_hash: None,
        })
    }

    async fn top_up(&self, _batch_id: BatchId, _amount: u128) -> Result<u128, Self::Error> {
        // Memory factory doesn't track batches after creation
        // In a real implementation, this would update the batch in storage
        Ok(0)
    }

    async fn dilute(&self, _batch_id: BatchId, _new_depth: u8) -> Result<(), Self::Error> {
        // Memory factory doesn't track batches after creation
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::Address;

    #[tokio::test]
    async fn test_memory_factory_create() {
        let factory = MemoryBatchFactory::new(100);

        let params = BatchParams::new(Address::ZERO, 20, 16, 1000);
        let result = factory.create(params).await.unwrap();

        assert_eq!(result.batch.owner(), Address::ZERO);
        assert_eq!(result.batch.depth(), 20);
        assert_eq!(result.batch.bucket_depth(), 16);
        assert_eq!(result.batch.value(), 1000);
        assert_eq!(result.batch.start(), 100);
        assert!(result.tx_hash.is_none());
    }

    #[tokio::test]
    async fn test_memory_factory_unique_ids() {
        let factory = MemoryBatchFactory::new(0);

        let params = BatchParams::new(Address::ZERO, 20, 16, 1000);

        let r1 = factory.create(params.clone()).await.unwrap();
        let r2 = factory.create(params.clone()).await.unwrap();
        let r3 = factory.create(params).await.unwrap();

        assert_ne!(r1.batch.id(), r2.batch.id());
        assert_ne!(r2.batch.id(), r3.batch.id());
    }

    #[tokio::test]
    async fn test_memory_factory_immutable() {
        let factory = MemoryBatchFactory::new(0);

        let params = BatchParams::new(Address::ZERO, 20, 16, 1000).immutable(true);
        let result = factory.create(params).await.unwrap();

        assert!(result.batch.immutable());
    }
}
