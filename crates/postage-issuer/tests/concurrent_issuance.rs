//! Lock-free issuance under contention.
//!
//! Fill allocation is dense: the slots a bucket hands out are always the prefix
//! `0..n`. Every test below asserts that prefix, which fails on a duplicate (two
//! threads on one slot) and on a hole (a slot lost by an allocator that rolls
//! back an overshoot).

#![allow(
    clippy::arithmetic_side_effects,
    clippy::panic,
    clippy::unwrap_used
)]

use std::sync::Mutex;
use std::thread;

use alloy_primitives::B256;
use nectar_postage_issuer::{
    BatchId, BucketDepth, MemoryIssuer, StampError, StampIssuer, calculate_bucket,
};
use nectar_primitives::ChunkAddress;
use proptest::prelude::*;

fn bucket_depth() -> BucketDepth {
    BucketDepth::new(16).unwrap()
}

/// An address in the bucket named by its leading 16 bits.
fn address_in(bucket: u16) -> ChunkAddress {
    let mut bytes = [0u8; 32];
    bytes[..2].copy_from_slice(&bucket.to_be_bytes());
    ChunkAddress::new(bytes)
}

/// Stamps one address `attempts` times per thread through shared handles,
/// returning the sorted slots handed out and the number of refusals.
fn contend(
    issuer: &MemoryIssuer,
    address: &ChunkAddress,
    threads: u64,
    attempts: u64,
) -> (Vec<u32>, u64) {
    let slots = Mutex::new(Vec::new());
    let refused = Mutex::new(0u64);

    thread::scope(|scope| {
        for _ in 0..threads {
            scope.spawn(|| {
                for timestamp in 0..attempts {
                    match issuer.prepare_stamp(address, timestamp) {
                        Ok(digest) => slots.lock().unwrap().push(digest.index.index()),
                        Err(StampError::BucketFull { .. }) => *refused.lock().unwrap() += 1,
                        Err(other) => panic!("unexpected issuance error: {other}"),
                    }
                }
            });
        }
    });

    let mut slots = slots.into_inner().unwrap();
    slots.sort_unstable();
    (slots, refused.into_inner().unwrap())
}

#[test]
fn one_bucket_under_contention_hands_out_each_slot_exactly_once() {
    // depth 24 over bucket depth 16 gives 256 slots; 8 threads overshoot it.
    let issuer: MemoryIssuer = MemoryIssuer::new(BatchId::ZERO, 24, bucket_depth());
    let address = address_in(0x9BCD);

    let (slots, refused) = contend(&issuer, &address, 8, 40);

    assert_eq!(slots, (0..256).collect::<Vec<_>>());
    assert_eq!(refused, 8 * 40 - 256);
    assert!(matches!(
        issuer.prepare_stamp(&address, 0),
        Err(StampError::BucketFull { capacity: 256, .. })
    ));
    assert_eq!(StampIssuer::stamps_issued(&issuer), Some(256));
}

#[test]
fn a_dilution_observed_mid_allocation_never_double_spends() {
    // Two slots to start with, grown to 256 while eight threads allocate, so
    // allocation and dilution genuinely interleave.
    let issuer: MemoryIssuer = MemoryIssuer::new(BatchId::ZERO, 17, bucket_depth());
    let address = address_in(0x0042);
    let bucket = calculate_bucket(&address, bucket_depth()).value();
    let slots = Mutex::new(Vec::new());

    thread::scope(|scope| {
        scope.spawn(|| {
            for depth in 18..=24u8 {
                issuer.dilute(depth).unwrap();
                thread::yield_now();
            }
        });
        for _ in 0..8 {
            scope.spawn(|| {
                for timestamp in 0..64u64 {
                    if let Ok(digest) = issuer.prepare_stamp(&address, timestamp) {
                        slots.lock().unwrap().push(digest.index.index());
                    }
                }
            });
        }
    });

    let mut slots = slots.into_inner().unwrap();
    slots.sort_unstable();
    let issued = u32::try_from(slots.len()).unwrap();
    assert_eq!(slots, (0..issued).collect::<Vec<_>>());
    assert_eq!(StampIssuer::bucket_utilization(&issuer, bucket), issued);
    assert_eq!(StampIssuer::stamps_issued(&issuer), Some(u64::from(issued)));
    assert_eq!(StampIssuer::batch_depth(&issuer), 24);
}

#[test]
fn a_depth_decrease_racing_allocation_never_shrinks_the_batch() {
    let issuer: MemoryIssuer = MemoryIssuer::new(BatchId::ZERO, 20, bucket_depth());
    let address = address_in(0x0001);

    thread::scope(|scope| {
        scope.spawn(|| {
            for _ in 0..64 {
                // A redelivered or reordered event carries a stale depth.
                assert!(issuer.dilute(17).is_err());
            }
        });
        scope.spawn(|| {
            // Depth 20 over bucket depth 16 leaves exactly 16 slots.
            for timestamp in 0..16u64 {
                issuer.prepare_stamp(&address, timestamp).unwrap();
            }
        });
    });

    assert_eq!(StampIssuer::batch_depth(&issuer), 20);
    assert_eq!(StampIssuer::bucket_capacity(&issuer), 16);
}

#[test]
fn the_whole_bucket_space_stamps_through_one_shared_handle() {
    let issuer: MemoryIssuer = MemoryIssuer::new(BatchId::ZERO, 24, bucket_depth());

    thread::scope(|scope| {
        for _ in 0..8 {
            scope.spawn(|| {
                for _ in 0..1000 {
                    let address = ChunkAddress::from(B256::random());
                    let mut handle = &issuer;
                    StampIssuer::prepare_stamp(&mut handle, &address, 0).unwrap();
                }
            });
        }
    });

    assert_eq!(StampIssuer::stamps_issued(&issuer), Some(8000));
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]

    /// Whatever the thread count and the bucket capacity, contention hands out
    /// each slot exactly once and refuses every attempt past the capacity.
    #[test]
    fn contention_conserves_the_slots_of_a_bucket(
        threads in 2u64..=8,
        slot_bits in 1u8..=6,
        overshoot in 1u64..=4,
    ) {
        let capacity = 1u64 << slot_bits;
        let issuer: MemoryIssuer =
            MemoryIssuer::new(BatchId::ZERO, 16 + slot_bits, bucket_depth());
        let address = address_in(0xABCD);
        // Enough attempts to fill the bucket and then some.
        let attempts = capacity.div_ceil(threads) + overshoot;

        let (slots, refused) = contend(&issuer, &address, threads, attempts);

        prop_assert_eq!(u64::try_from(slots.len()).unwrap(), capacity);
        prop_assert_eq!(&slots, &(0..u32::try_from(capacity).unwrap()).collect::<Vec<_>>());
        prop_assert_eq!(refused, threads * attempts - capacity);
        prop_assert_eq!(StampIssuer::stamps_issued(&issuer), Some(capacity));
    }
}
