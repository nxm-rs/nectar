//! Fixtures shared by the crate's tests.

use alloy_primitives::{Address, B256};
use alloy_signer_local::PrivateKeySigner;

use nectar_postage::{Batch, BatchId, BucketDepth};

/// A deterministic key, one per `seed`.
pub(crate) fn key(seed: u8) -> PrivateKeySigner {
    PrivateKeySigner::from_bytes(&B256::repeat_byte(seed)).unwrap()
}

/// An immutable batch at depth 20 over 16-bit buckets: 16 slots per bucket.
pub(crate) fn batch_owned_by(owner: Address, id: BatchId) -> Batch {
    batch_at_depth(owner, id, 20)
}

pub(crate) fn batch_at_depth(owner: Address, id: BatchId, depth: u8) -> Batch {
    Batch::new(
        id,
        1_000,
        100,
        owner,
        depth,
        BucketDepth::new(16).unwrap(),
        true,
    )
}
