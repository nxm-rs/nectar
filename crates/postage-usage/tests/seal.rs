//! Tests for sealing a persist plan into signed chunks and stamps.

#![cfg(feature = "seal")]
// Bench, example, and integration-test code: unwraps, direct indexing,
// casts, and assertions are setup and illustration, not shipped surface.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::panic,
    clippy::panic_in_result_fn,
    clippy::as_conversions,
    clippy::missing_panics_doc
)]
use alloy_primitives::Address;
use alloy_signer_local::PrivateKeySigner;
use nectar_postage_usage::{
    BatchId, Mutability, PublishedSequence, SealError, Snapshot, UsageTable, seal_plan,
    usage_chunk_address,
};
use nectar_primitives::ChunkOps;

const BUCKET_DEPTH: u8 = 16;

fn seeded_snapshot(owner: Address, batch_id: BatchId) -> Snapshot {
    // Seed one stamp in bucket 123 via the inert constructor; the table itself
    // has no record path.
    let mut counts = vec![0u32; 1usize << BUCKET_DEPTH];
    counts[123] = 1;
    let table =
        UsageTable::from_counts(batch_id, 20, BUCKET_DEPTH, counts, Mutability::Immutable).unwrap();
    let mut snapshot = Snapshot::new(table);
    let _ = snapshot
        .revalidate(PublishedSequence::NONE)
        .unwrap()
        .plan_persist(&owner)
        .unwrap();
    snapshot
}

#[test]
fn sealed_chunks_and_stamps_verify() {
    let signer = PrivateKeySigner::random();
    let owner = signer.address();
    let batch_id = BatchId::new([0x42; 32]);

    let mut snapshot = seeded_snapshot(owner, batch_id);
    let plan = snapshot
        .revalidate(PublishedSequence::NONE)
        .unwrap()
        .plan_persist(&owner)
        .unwrap();
    // No persist has been sealed yet in this process, so the plan carries no
    // previous timestamp.
    assert_eq!(plan.previous_timestamp, None);

    let sealed = seal_plan(&mut snapshot, &plan, 1, &signer).unwrap();
    assert_eq!(sealed.len(), plan.chunks.len());
    // The seal advanced the in-process monotonicity floor.
    assert_eq!(snapshot.last_seal_timestamp(), Some(1));

    for (planned, sealed) in plan.chunks.iter().zip(sealed.iter()) {
        // The single-owner chunk sits at the predicted address and is owned
        // by the batch owner.
        assert_eq!(*sealed.chunk.address(), planned.address);
        assert_eq!(
            *sealed.chunk.address(),
            usage_chunk_address(&batch_id, &owner, planned.index)
        );
        assert_eq!(sealed.chunk.owner().unwrap(), owner);
        // The stamp binds the chunk address to the planned slot and verifies
        // against the batch owner.
        assert_eq!(sealed.stamp.stamp_index(), planned.stamp_index);
        sealed.stamp.verify(&planned.address, owner).unwrap();
    }

    // A different signer is rejected: the chunk address no longer matches. A
    // strictly newer timestamp keeps the monotonicity guard out of the way.
    let other = PrivateKeySigner::random();
    let next = snapshot
        .revalidate(PublishedSequence::NONE)
        .unwrap()
        .plan_persist(&owner)
        .unwrap();
    assert!(matches!(
        seal_plan(&mut snapshot, &next, 2, &other),
        Err(SealError::AddressMismatch)
    ));
    // The failed seal left the floor untouched so it can be retried.
    assert_eq!(snapshot.last_seal_timestamp(), Some(1));
}

/// A seal whose timestamp does not strictly exceed the previous seal's timestamp
/// is rejected: overwriting a metadata chunk in place needs a newer timestamp,
/// so the reserve would otherwise leave the stale version standing. This is the
/// in-process single-owner clock-skew guard for nectar issue #58.
#[test]
fn non_increasing_seal_timestamp_is_rejected() {
    let signer = PrivateKeySigner::random();
    let owner = signer.address();
    let batch_id = BatchId::new([0x42; 32]);

    let mut snapshot = seeded_snapshot(owner, batch_id);

    // First seal sets the floor at timestamp 100.
    let first = snapshot
        .revalidate(PublishedSequence::NONE)
        .unwrap()
        .plan_persist(&owner)
        .unwrap();
    assert_eq!(first.previous_timestamp, None);
    seal_plan(&mut snapshot, &first, 100, &signer).unwrap();
    assert_eq!(snapshot.last_seal_timestamp(), Some(100));

    // The next plan surfaces the floor it must beat.
    let second = snapshot
        .revalidate(PublishedSequence::NONE)
        .unwrap()
        .plan_persist(&owner)
        .unwrap();
    assert_eq!(second.previous_timestamp, Some(100));

    // An equal timestamp is rejected: it must strictly increase.
    assert!(matches!(
        seal_plan(&mut snapshot, &second, 100, &signer).unwrap_err(),
        SealError::NonIncreasingTimestamp {
            timestamp: 100,
            previous: 100,
        }
    ));
    // An older timestamp (clock skew) is rejected too.
    assert!(matches!(
        seal_plan(&mut snapshot, &second, 99, &signer).unwrap_err(),
        SealError::NonIncreasingTimestamp {
            timestamp: 99,
            previous: 100,
        }
    ));
    // The floor is unchanged by the rejected seals.
    assert_eq!(snapshot.last_seal_timestamp(), Some(100));

    // A strictly newer timestamp seals and advances the floor.
    seal_plan(&mut snapshot, &second, 101, &signer).unwrap();
    assert_eq!(snapshot.last_seal_timestamp(), Some(101));
}
