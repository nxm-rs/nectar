//! Shared helpers for the postage-usage integration tests.

#![allow(dead_code)]

use alloy_primitives::Address;
use nectar_postage_usage::{BatchId, BucketDepth};

/// The mainnet bucket depth these tests build tables at.
pub(crate) const BUCKET_DEPTH: u8 = 16;

/// [`BUCKET_DEPTH`] in the proof-carrying type.
pub(crate) fn bucket_depth() -> BucketDepth {
    BucketDepth::new(BUCKET_DEPTH).unwrap()
}

/// The batch these tests' tables belong to.
pub(crate) const fn batch_id() -> BatchId {
    BatchId::new([0x42; 32])
}

/// The owner these tests' tables are persisted by.
pub(crate) const fn owner() -> Address {
    Address::repeat_byte(0x11)
}
