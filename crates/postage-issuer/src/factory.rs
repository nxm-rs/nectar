//! Batch factory traits for creating batches.

use core::marker::PhantomData;

use nectar_postage::{Batch, BatchId, BatchParams};
use nectar_primitives::{Mainnet, SwarmSpec};

/// The result of creating a batch on the network `S`.
#[derive(Debug)]
pub struct CreateResultFor<S: SwarmSpec = Mainnet> {
    /// The created batch.
    pub batch: Batch<S>,
    /// The transaction hash (if created on-chain).
    pub tx_hash: Option<alloy_primitives::B256>,
}

/// The [`CreateResultFor`] of the mainnet spec.
pub type CreateResult = CreateResultFor<Mainnet>;

// The spec is a type-level tag, so the impls below carry no bound on `S` beyond
// `SwarmSpec`; deriving would demand `S: Clone` and `S: Eq` of a marker type
// that holds no data.

impl<S: SwarmSpec> Clone for CreateResultFor<S> {
    fn clone(&self) -> Self {
        Self {
            batch: self.batch.clone(),
            tx_hash: self.tx_hash,
        }
    }
}

impl<S: SwarmSpec> PartialEq for CreateResultFor<S> {
    fn eq(&self, other: &Self) -> bool {
        self.batch == other.batch && self.tx_hash == other.tx_hash
    }
}

impl<S: SwarmSpec> Eq for CreateResultFor<S> {}

/// A trait for creating postage batches.
///
/// Implementations may create batches on-chain (by sending transactions
/// to the postage stamp contract) or in-memory for testing.
pub trait BatchFactory {
    /// The error type returned by factory operations.
    type Error: std::error::Error;

    /// The network the created batches belong to.
    type Spec: SwarmSpec;

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
        params: BatchParams<Self::Spec>,
    ) -> impl std::future::Future<Output = Result<CreateResultFor<Self::Spec>, Self::Error>> + Send;

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
///
/// The network the batches are minted for is a type parameter;
/// [`MemoryBatchFactory`] is the mainnet factory.
#[derive(Debug)]
pub struct MemoryBatchFactoryFor<S: SwarmSpec = Mainnet> {
    /// Counter for generating unique batch IDs.
    next_id: std::sync::atomic::AtomicU64,
    /// The current block number (for start block).
    current_block: u64,
    /// The network the minted batches belong to.
    spec: PhantomData<fn() -> S>,
}

/// The [`MemoryBatchFactoryFor`] of the mainnet spec.
pub type MemoryBatchFactory = MemoryBatchFactoryFor<Mainnet>;

impl<S: SwarmSpec> MemoryBatchFactoryFor<S> {
    /// Creates a new memory batch factory.
    pub const fn new(current_block: u64) -> Self {
        Self {
            next_id: std::sync::atomic::AtomicU64::new(0),
            current_block,
            spec: PhantomData,
        }
    }

    /// Sets the current block number.
    pub const fn set_current_block(&mut self, block: u64) {
        self.current_block = block;
    }

    fn generate_batch_id(&self) -> BatchId {
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let mut bytes = [0u8; 32];
        bytes[24..32].copy_from_slice(&id.to_be_bytes());
        BatchId::new(bytes)
    }
}

impl<S: SwarmSpec> Default for MemoryBatchFactoryFor<S> {
    fn default() -> Self {
        Self::new(0)
    }
}

impl<S: SwarmSpec> BatchFactory for MemoryBatchFactoryFor<S> {
    type Error = std::convert::Infallible;
    type Spec = S;

    async fn create(&self, params: BatchParams<S>) -> Result<CreateResultFor<S>, Self::Error> {
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

        Ok(CreateResultFor {
            batch,
            tx_hash: None,
        })
    }

    async fn top_up(&self, _batch_id: BatchId, _amount: u128) -> Result<u128, Self::Error> {
        // Memory factory doesn't track batches after creation
        Ok(0)
    }

    async fn dilute(&self, _batch_id: BatchId, _new_depth: u8) -> Result<(), Self::Error> {
        // Memory factory doesn't track batches after creation
        Ok(())
    }
}

// Sanctioned tokio adapter tests: the test macro expands to `Runtime::block_on`.
#[cfg(test)]
#[allow(clippy::disallowed_methods)]
mod tests {
    use super::*;
    use alloy_primitives::Address;
    use nectar_postage::BucketDepth;

    #[tokio::test]
    async fn test_memory_factory_create() {
        let factory = MemoryBatchFactory::new(100);

        let params = BatchParams::new(Address::ZERO, 20, BucketDepth::new(16).unwrap(), 1000);
        let result = factory.create(params).await.unwrap();

        assert_eq!(result.batch.owner(), Address::ZERO);
        assert_eq!(result.batch.depth(), 20);
        assert_eq!(result.batch.bucket_depth().get(), 16);
        assert_eq!(result.batch.value(), 1000);
        assert_eq!(result.batch.start(), 100);
        assert!(result.tx_hash.is_none());
    }

    #[tokio::test]
    async fn test_memory_factory_unique_ids() {
        let factory = MemoryBatchFactory::new(0);

        let params = BatchParams::new(Address::ZERO, 20, BucketDepth::new(16).unwrap(), 1000);

        let r1 = factory.create(params.clone()).await.unwrap();
        let r2 = factory.create(params.clone()).await.unwrap();
        let r3 = factory.create(params).await.unwrap();

        assert_ne!(r1.batch.id(), r2.batch.id());
        assert_ne!(r2.batch.id(), r3.batch.id());
    }

    #[tokio::test]
    async fn test_memory_factory_immutable() {
        let factory = MemoryBatchFactory::new(0);

        let params = BatchParams::new(Address::ZERO, 20, BucketDepth::new(16).unwrap(), 1000)
            .immutable(true);
        let result = factory.create(params).await.unwrap();

        assert!(result.batch.immutable());
    }
}
