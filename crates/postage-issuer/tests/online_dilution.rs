//! Dilution observed while a signing stream is in flight.
//!
//! A barrier pins the interleaving: exactly `HALF` addresses are admitted at
//! the narrow depth, the rest at the wide one.

#![allow(clippy::arithmetic_side_effects, clippy::panic, clippy::unwrap_used)]

use std::collections::HashSet;
use std::sync::{Arc, Barrier};
use std::thread;

use alloy_signer_local::PrivateKeySigner;
use nectar_postage_issuer::{
    Batch, BatchEvent, BatchEventHandler, BatchId, BucketDepth, IssuerRegistry, MemoryIssuer,
    SigningError, StampError, StampIssuer, StampPipeline, Window,
};
use nectar_primitives::ChunkAddress;

const BUCKETS: u32 = 8;
const NARROW_DEPTH: u8 = 17;
const WIDE_DEPTH: u8 = 21;
const BUCKET_DEPTH: u8 = 16;
const TOTAL: u32 = 256;
const HALF: u32 = TOTAL / 2;
const DILUTION_BLOCK: u64 = 4_200;
const CONFIRMATIONS: u64 = 12;
const NARROW_CAPACITY: u32 = 1 << (NARROW_DEPTH - BUCKET_DEPTH);
const NARROW_SLOTS: u32 = BUCKETS * NARROW_CAPACITY;

/// A window of exactly the narrow slot total. The first refill takes all of
/// them, so every later micro-batch is one address and none can straddle the
/// barrier and reserve a pre-barrier address at the wide depth.
fn window() -> Window {
    Window::new(u16::try_from(NARROW_SLOTS).unwrap()).unwrap()
}

/// An address whose leading bytes place it in bucket `i % BUCKETS`.
fn address_for(i: u32) -> ChunkAddress {
    let mut bytes = [0u8; 32];
    let bucket = u16::try_from(i % BUCKETS).unwrap();
    bytes[..2].copy_from_slice(&bucket.to_be_bytes());
    bytes[2..6].copy_from_slice(&i.to_be_bytes());
    ChunkAddress::new(bytes)
}

fn batch_at(owner: alloy_primitives::Address, depth: u8) -> Batch {
    Batch::new(
        BatchId::ZERO,
        1_000,
        0,
        owner,
        depth,
        BucketDepth::new(BUCKET_DEPTH).unwrap(),
        true,
    )
}

#[test]
fn a_dilution_mid_stream_widens_the_batch_without_minting_an_invalid_stamp() {
    let signer = PrivateKeySigner::from_slice(&[9u8; 32]).unwrap();
    let owner = signer.address();
    let issuer: Arc<MemoryIssuer> = Arc::new(MemoryIssuer::new(
        BatchId::ZERO,
        NARROW_DEPTH,
        BucketDepth::new(BUCKET_DEPTH).unwrap(),
    ));
    let mut registry = IssuerRegistry::new(CONFIRMATIONS);
    registry.register(issuer.clone());
    let at_half = Barrier::new(2);
    let resumed = Barrier::new(2);

    // Observations are asserted after the join: a panic between the barriers
    // would strand the stamping thread.
    let (results, unconfirmed, confirmed, gated) = thread::scope(|scope| {
        let stamping = scope.spawn(|| {
            let pipeline = StampPipeline::from_signer(signer.clone()).with_window(window());
            let addresses = (0..TOTAL).map(|i| {
                if i == HALF {
                    at_half.wait();
                    resumed.wait();
                }
                address_for(i)
            });
            pipeline.stamp(&*issuer, addresses).collect::<Vec<_>>()
        });

        // Half the stream is admitted, and its signatures are outstanding,
        // before the chain event lands.
        at_half.wait();
        let mut unconfirmed = Vec::new();
        for depth in NARROW_DEPTH + 1..=WIDE_DEPTH {
            registry
                .handle_event(BatchEvent::DepthIncrease {
                    batch_id: BatchId::ZERO,
                    new_depth: depth,
                    block: DILUTION_BLOCK,
                })
                .unwrap();
            unconfirmed.push(StampIssuer::batch_depth(&*issuer));
        }
        let gated = (
            StampIssuer::max_bucket_utilization(&*issuer),
            StampIssuer::stamps_issued(&*issuer),
        );
        registry.advance_to(DILUTION_BLOCK + CONFIRMATIONS).unwrap();
        let confirmed = StampIssuer::batch_depth(&*issuer);
        resumed.wait();

        (stamping.join().unwrap(), unconfirmed, confirmed, gated)
    });

    // Nothing is issuable into the widened range until the event confirms.
    assert!(unconfirmed.iter().all(|depth| *depth == NARROW_DEPTH));
    assert_eq!(confirmed, WIDE_DEPTH);
    assert_eq!(gated, (NARROW_CAPACITY, Some(u64::from(NARROW_SLOTS))));

    let batch = batch_at(owner, WIDE_DEPTH);
    let narrow = batch_at(owner, NARROW_DEPTH);
    let mut slots = HashSet::new();
    let mut issued = 0u32;
    let mut refused = 0u32;
    let mut narrow_valid = 0u32;

    assert_eq!(u32::try_from(results.len()).unwrap(), TOTAL);
    for outcome in results {
        match outcome.result {
            Ok(stamp) => {
                issued += 1;
                narrow_valid += u32::from(narrow.validate_index(&stamp.stamp_index()).is_ok());
                stamp.verify(&outcome.address, owner).unwrap();
                batch.validate_index(&stamp.stamp_index()).unwrap();
                batch
                    .validate_bucket(&stamp.stamp_index(), &outcome.address)
                    .unwrap();
                assert!(slots.insert(stamp.stamp_index().encode()), "slot reissued");
            }
            Err(SigningError::Stamp(StampError::BucketFull { .. })) => refused += 1,
            Err(other) => panic!("unexpected stamping error: {other}"),
        }
    }

    // The first half sees the narrow geometry, so each bucket fills at its old
    // capacity; the second half sees the wide one and every address lands.
    assert_eq!(issued, NARROW_SLOTS + HALF);
    assert_eq!(refused, TOTAL - issued);
    assert_eq!(u32::try_from(slots.len()).unwrap(), issued);
    // A peer still on the narrow geometry accepts exactly the pre-dilution
    // slots and no more.
    assert_eq!(narrow_valid, NARROW_SLOTS);
    assert_eq!(
        StampIssuer::stamps_issued(&*issuer),
        Some(u64::from(issued))
    );
}

#[test]
fn an_unsynchronized_dilution_never_reissues_a_slot() {
    let signer = PrivateKeySigner::from_slice(&[11u8; 32]).unwrap();
    let owner = signer.address();
    let issuer: Arc<MemoryIssuer> = Arc::new(MemoryIssuer::new(
        BatchId::ZERO,
        NARROW_DEPTH,
        BucketDepth::new(BUCKET_DEPTH).unwrap(),
    ));
    let mut registry = IssuerRegistry::new(0);
    registry.register(issuer.clone());

    let results = thread::scope(|scope| {
        let stamping = scope.spawn(|| {
            let pipeline = StampPipeline::from_signer(signer.clone()).with_window(window());
            pipeline
                .stamp(&*issuer, (0..TOTAL).map(address_for))
                .collect::<Vec<_>>()
        });

        for depth in NARROW_DEPTH + 1..=WIDE_DEPTH {
            registry
                .handle_event(BatchEvent::DepthIncrease {
                    batch_id: BatchId::ZERO,
                    new_depth: depth,
                    block: DILUTION_BLOCK,
                })
                .unwrap();
            thread::yield_now();
        }

        stamping.join().unwrap()
    });

    let batch = batch_at(owner, WIDE_DEPTH);
    let mut slots = HashSet::new();
    for outcome in results {
        if let Ok(stamp) = outcome.result {
            stamp.verify(&outcome.address, owner).unwrap();
            batch.validate_index(&stamp.stamp_index()).unwrap();
            assert!(slots.insert(stamp.stamp_index().encode()), "slot reissued");
        }
    }

    assert_eq!(
        StampIssuer::stamps_issued(&*issuer),
        Some(u64::try_from(slots.len()).unwrap())
    );
    assert_eq!(StampIssuer::batch_depth(&*issuer), WIDE_DEPTH);
}
