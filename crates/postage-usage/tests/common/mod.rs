//! Shared helpers for the postage-usage integration tests.

#![allow(dead_code)]

use core::num::NonZeroU8;

use alloy_primitives::Address;
use nectar_postage_usage::{BatchId, BucketDepth, NetworkId, SwarmSpec};

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

/// A deployment whose bucket-depth floor is the format minimum, for the
/// geometries mainnet's floor of 16 forbids. The encoding does not depend on
/// the spec: only which geometries a network admits does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Shallow;

impl SwarmSpec for Shallow {
    const NETWORK_ID: NetworkId = NetworkId::TESTNET;
    const MIN_BUCKET_DEPTH: NonZeroU8 = NonZeroU8::new(1).unwrap();
}

/// A bucket depth [`Shallow`] accepts.
pub(crate) fn shallow(depth: u8) -> BucketDepth<Shallow> {
    BucketDepth::new(depth).unwrap()
}
