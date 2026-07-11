//! End-to-end tests for the snapshot format: persist planning, encoding,
//! decoding, dilution, and corruption rejection.

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
use alloy_primitives::{Address, B256};
use nectar_postage::calculate_bucket;
use nectar_postage_usage::{
    Batch, MAGIC, Mutability, PersistPlan, PublishedSequence, RootInfo, Snapshot, UsageError,
    UsageTable, usage_chunk_address, usage_chunk_id,
};

const BUCKET_DEPTH: u8 = 16;

const fn batch_id() -> B256 {
    B256::repeat_byte(0x42)
}

const fn owner() -> Address {
    Address::repeat_byte(0x11)
}

/// Deterministic pseudo-random counters with the given spread.
fn synthetic_counts(buckets: usize, base: u32, spread: u32) -> Vec<u32> {
    let mut state = 0x9e3779b97f4a7c15u64;
    (0..buckets)
        .map(|_| {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            // `state >> 33` keeps 31 bits, which always fit u32.
            #[allow(clippy::as_conversions)]
            let mixed = (state >> 33) as u32;
            base + mixed % (spread + 1)
        })
        .collect()
}

fn roundtrip(plan: &PersistPlan) -> Snapshot {
    let root = RootInfo::parse(&plan.chunks[0].payload).unwrap();
    let leaves: Vec<_> = plan.chunks[1..].iter().map(|c| &c.payload).collect();
    assert_eq!(usize::from(root.leaf_count()), leaves.len());
    root.assemble(&leaves).unwrap()
}

const fn batch(depth: u8, immutable: bool) -> Batch {
    Batch::new(
        batch_id(),
        1_000,
        0,
        owner(),
        depth,
        BUCKET_DEPTH,
        immutable,
    )
}

#[test]
fn snapshot_from_batch_matches_batch_polarity() {
    // An immutable batch yields a fill-watermark snapshot; a mutable batch
    // (immutable() == false) yields a wrapping ring. The geometry is read
    // straight from the batch.
    let immutable = Snapshot::from_batch(&batch(20, true)).unwrap();
    assert!(!immutable.table().is_mutable());
    assert_eq!(immutable.table().batch_id(), batch_id());
    assert_eq!(immutable.table().depth(), 20);
    assert_eq!(immutable.table().bucket_depth(), BUCKET_DEPTH);
    assert_eq!(immutable.sequence(), 0);

    let mutable = Snapshot::from_batch(&batch(22, false)).unwrap();
    assert!(mutable.table().is_mutable());
    assert_eq!(mutable.table().depth(), 22);
    assert_eq!(mutable.sequence(), 0);
}

#[test]
fn empty_table_persists_as_a_single_small_root() {
    let table = UsageTable::new(batch_id(), 20, BUCKET_DEPTH, Mutability::Immutable).unwrap();
    let mut snapshot = Snapshot::new(table);
    let plan = snapshot
        .revalidate(PublishedSequence::NONE)
        .unwrap()
        .plan_persist(&owner())
        .unwrap();

    assert_eq!(plan.sequence, 1);
    assert_eq!(plan.chunks.len(), 1);
    let root = &plan.chunks[0];
    assert!(root.newly_allocated);
    assert_eq!(root.id, usage_chunk_id(&batch_id(), 0));
    assert_eq!(root.address, usage_chunk_address(&batch_id(), &owner(), 0));
    // Header, one exception (the root's own bucket), one slot entry.
    assert_eq!(root.payload.len(), 66 + 8 + 4);

    // The snapshot accounts for its own stamp.
    assert_eq!(snapshot.table().total_issued(), 1);
    assert_eq!(roundtrip(&plan), snapshot);
}

#[test]
fn uniform_spread_uses_leaves_and_round_trips() {
    let buckets = 1usize << BUCKET_DEPTH;
    // Counts in 64..=127: base 64, deltas need 6 bits. A leaf holds
    // floor(32768 / 6) = 5461 buckets, so 65536 buckets need 13 leaves.
    let counts = synthetic_counts(buckets, 64, 63);
    let table =
        UsageTable::from_counts(batch_id(), 24, BUCKET_DEPTH, counts, Mutability::Immutable)
            .unwrap();
    let mut snapshot = Snapshot::new(table);
    let plan = snapshot
        .revalidate(PublishedSequence::NONE)
        .unwrap()
        .plan_persist(&owner())
        .unwrap();

    assert_eq!(plan.chunks.len(), 14, "root plus 13 leaves");
    assert!(plan.chunks.iter().all(|c| c.newly_allocated));
    assert!(plan.chunks[1..].iter().all(|c| c.payload.len() <= 4096));
    assert_eq!(roundtrip(&plan), snapshot);
}

#[test]
fn hot_bucket_becomes_an_exception_not_a_wider_table() {
    let buckets = 1usize << BUCKET_DEPTH;
    let mut counts = vec![0u32; buckets];
    counts[12345] = 200_000; // one hot bucket, everything else empty
    let table =
        UsageTable::from_counts(batch_id(), 34, BUCKET_DEPTH, counts, Mutability::Immutable)
            .unwrap();
    let mut snapshot = Snapshot::new(table);
    let plan = snapshot
        .revalidate(PublishedSequence::NONE)
        .unwrap()
        .plan_persist(&owner())
        .unwrap();

    // The outlier lands in the exception list, so width stays minimal and
    // everything fits in the root (width 0 needs no packed bits at all,
    // beyond exceptions for the snapshot chunk's own stamp).
    assert_eq!(plan.chunks.len(), 1);
    assert_eq!(roundtrip(&plan), snapshot);
}

#[test]
fn steady_state_persists_allocate_nothing_and_keep_slots() {
    let buckets = 1usize << BUCKET_DEPTH;
    let counts = synthetic_counts(buckets, 10, 15);
    let table =
        UsageTable::from_counts(batch_id(), 22, BUCKET_DEPTH, counts, Mutability::Immutable)
            .unwrap();
    let mut snapshot = Snapshot::new(table);

    let first = snapshot
        .revalidate(PublishedSequence::NONE)
        .unwrap()
        .plan_persist(&owner())
        .unwrap();
    let slots_after_first = snapshot.allocated_slots().to_vec();
    let second = snapshot
        .revalidate(PublishedSequence::NONE)
        .unwrap()
        .plan_persist(&owner())
        .unwrap();

    assert_eq!(second.sequence, first.sequence + 1);
    assert!(second.chunks.iter().all(|c| !c.newly_allocated));
    assert_eq!(snapshot.allocated_slots(), slots_after_first.as_slice());
    // Stamp indices are stable across persists: same slot, reused forever.
    for (a, b) in first.chunks.iter().zip(second.chunks.iter()) {
        assert_eq!(a.stamp_index, b.stamp_index);
        assert_eq!(a.address, b.address);
    }
    // Nothing changed between persists, so leaf payloads are identical.
    for (a, b) in first.chunks[1..].iter().zip(second.chunks[1..].iter()) {
        assert_eq!(a.payload, b.payload);
    }
    assert_eq!(roundtrip(&second), snapshot);
}

#[test]
fn snapshot_accounts_for_its_own_chunks() {
    let buckets = 1usize << BUCKET_DEPTH;
    let counts = synthetic_counts(buckets, 100, 90);
    let table =
        UsageTable::from_counts(batch_id(), 24, BUCKET_DEPTH, counts, Mutability::Immutable)
            .unwrap();
    let issued_before = table.total_issued();
    let mut snapshot = Snapshot::new(table);
    let plan = snapshot
        .revalidate(PublishedSequence::NONE)
        .unwrap()
        .plan_persist(&owner())
        .unwrap();

    // A plan holds at most root + leaves chunks; usize fits u64 on
    // supported targets.
    #[allow(clippy::as_conversions)]
    let allocated = plan.chunks.len() as u64;
    assert_eq!(snapshot.table().total_issued(), issued_before + allocated);
    for chunk in &plan.chunks {
        let bucket = calculate_bucket(&chunk.address, BUCKET_DEPTH);
        assert_eq!(chunk.stamp_index.bucket(), bucket);
        // The recorded counter covers the assigned slot.
        assert!(snapshot.table().count(bucket).unwrap() > chunk.stamp_index.index());
    }
    assert_eq!(roundtrip(&plan), snapshot);
}

#[test]
fn dilution_changes_no_leaf_bytes() {
    let buckets = 1usize << BUCKET_DEPTH;
    let counts = synthetic_counts(buckets, 5, 7);
    let table =
        UsageTable::from_counts(batch_id(), 20, BUCKET_DEPTH, counts, Mutability::Immutable)
            .unwrap();
    let mut snapshot = Snapshot::new(table);
    let before = snapshot
        .revalidate(PublishedSequence::NONE)
        .unwrap()
        .plan_persist(&owner())
        .unwrap();

    snapshot.dilute(24).unwrap();
    let after = snapshot
        .revalidate(PublishedSequence::NONE)
        .unwrap()
        .plan_persist(&owner())
        .unwrap();

    assert_eq!(before.chunks.len(), after.chunks.len());
    for (a, b) in before.chunks[1..].iter().zip(after.chunks[1..].iter()) {
        assert_eq!(a.payload, b.payload, "leaf bytes must survive dilution");
    }
    let recovered = roundtrip(&after);
    assert_eq!(recovered.table().depth(), 24);
    assert_eq!(recovered, snapshot);
}

#[test]
fn small_bucket_depth_inlines_in_the_root() {
    // 256 buckets at any width fit inline in the root.
    let counts = synthetic_counts(256, 1000, 4000);
    let table = UsageTable::from_counts(batch_id(), 21, 8, counts, Mutability::Immutable).unwrap();
    let mut snapshot = Snapshot::new(table);
    let plan = snapshot
        .revalidate(PublishedSequence::NONE)
        .unwrap()
        .plan_persist(&owner())
        .unwrap();
    assert_eq!(plan.chunks.len(), 1);
    assert_eq!(roundtrip(&plan), snapshot);
}

#[test]
fn full_capacity_counters_round_trip() {
    // Every bucket completely full at the smallest geometry.
    let buckets = 1usize << BUCKET_DEPTH;
    let mut counts = vec![2u32; buckets];
    // Leave room for the snapshot's own chunks.
    for c in counts.iter_mut().take(64) {
        *c = 0;
    }
    let table =
        UsageTable::from_counts(batch_id(), 17, BUCKET_DEPTH, counts, Mutability::Immutable)
            .unwrap();
    let mut snapshot = Snapshot::new(table);
    match snapshot
        .revalidate(PublishedSequence::NONE)
        .unwrap()
        .plan_persist(&owner())
    {
        Ok(plan) => {
            assert_eq!(roundtrip(&plan), snapshot);
        }
        Err(UsageError::BucketFull { .. }) => {
            // The root's bucket happened to be full: the documented failure
            // mode of persisting too late.
        }
        Err(err) => panic!("unexpected error: {err}"),
    }
}

#[test]
fn corruption_is_rejected() {
    let buckets = 1usize << BUCKET_DEPTH;
    let counts = synthetic_counts(buckets, 50, 40);
    let table =
        UsageTable::from_counts(batch_id(), 23, BUCKET_DEPTH, counts, Mutability::Immutable)
            .unwrap();
    let mut snapshot = Snapshot::new(table);
    let plan = snapshot
        .revalidate(PublishedSequence::NONE)
        .unwrap()
        .plan_persist(&owner())
        .unwrap();
    let root_payload = plan.chunks[0].payload.to_vec();
    let leaves: Vec<Vec<u8>> = plan.chunks[1..]
        .iter()
        .map(|c| c.payload.to_vec())
        .collect();

    // Bad magic.
    let mut bad = root_payload.clone();
    bad[0] ^= 0xff;
    assert_eq!(RootInfo::parse(&bad), Err(UsageError::BadMagic));
    assert_eq!(MAGIC, *b"SBU1");

    // Truncation.
    assert!(matches!(
        RootInfo::parse(&root_payload[..root_payload.len() - 1]),
        Err(UsageError::PayloadLength { .. })
    ));

    // Tampered issued total (bytes 48..56).
    let mut bad = root_payload.clone();
    bad[55] ^= 0x01;
    let root = RootInfo::parse(&bad).unwrap();
    assert!(matches!(
        root.assemble(&leaves),
        Err(UsageError::IssuedMismatch { .. })
    ));

    // Tampered leaf payload.
    let mut bad_leaves = leaves.clone();
    bad_leaves[0][0] ^= 0xff;
    let root = RootInfo::parse(&root_payload).unwrap();
    assert!(matches!(
        root.assemble(&bad_leaves),
        Err(UsageError::LeafDigestMismatch { index: 0 })
    ));

    // Wrong leaf count.
    let root = RootInfo::parse(&root_payload).unwrap();
    assert!(matches!(
        root.assemble(&leaves[1..]),
        Err(UsageError::LeafCount { .. })
    ));
}

#[test]
fn reserved_indices_match_planned_stamps_and_guard_reuse() {
    let buckets = 1usize << BUCKET_DEPTH;
    let counts = synthetic_counts(buckets, 10, 15);
    let table =
        UsageTable::from_counts(batch_id(), 22, BUCKET_DEPTH, counts, Mutability::Immutable)
            .unwrap();
    let mut snapshot = Snapshot::new(table);
    let plan = snapshot
        .revalidate(PublishedSequence::NONE)
        .unwrap()
        .plan_persist(&owner())
        .unwrap();

    let reserved = snapshot.reserved_stamp_indices(&owner());
    assert_eq!(reserved.len(), plan.chunks.len());
    for chunk in &plan.chunks {
        assert!(reserved.contains(&chunk.stamp_index));
        assert!(snapshot.is_reserved(&owner(), chunk.stamp_index));
        // Fresh issuance in the same bucket hands out the watermark, which
        // is above every reserved slot in that bucket.
        let bucket = chunk.stamp_index.bucket();
        let fresh = snapshot.table().count(bucket).unwrap();
        assert!(fresh > chunk.stamp_index.index());
        assert!(!snapshot.is_reserved(&owner(), nectar_postage::StampIndex::new(bucket, fresh)));
    }

    // The reserved list survives decode: it is derived from the root's
    // allocated slots section.
    let recovered = roundtrip(&plan);
    assert_eq!(recovered.reserved_stamp_indices(&owner()), reserved);
}

#[test]
fn mutable_round_trips_and_decodes_as_mutable() {
    let buckets = 1usize << BUCKET_DEPTH;
    let counts = synthetic_counts(buckets, 10, 15);
    let table =
        UsageTable::from_counts(batch_id(), 22, BUCKET_DEPTH, counts, Mutability::Mutable).unwrap();
    assert!(table.is_mutable());
    let mut snapshot = Snapshot::new(table);
    let plan = snapshot
        .revalidate(PublishedSequence::NONE)
        .unwrap()
        .plan_persist(&owner())
        .unwrap();

    // The flags byte marks the snapshot mutable.
    let root = RootInfo::parse(&plan.chunks[0].payload).unwrap();
    assert!(root.is_mutable());

    let leaves: Vec<_> = plan.chunks[1..].iter().map(|c| &c.payload).collect();
    let recovered = root.assemble(&leaves).unwrap();
    assert!(recovered.table().is_mutable());
    // A recovered mutable snapshot is inert (no reserved state on the table), and
    // its reserved slots are installed only when an issuer is obtained, so it
    // compares equal to its source by counters and geometry alone.
    assert_eq!(recovered, snapshot);
}

#[test]
fn mutable_dilution_changes_no_cursor_or_leaf_bytes() {
    let buckets = 1usize << BUCKET_DEPTH;
    let counts = synthetic_counts(buckets, 5, 7);
    let table =
        UsageTable::from_counts(batch_id(), 20, BUCKET_DEPTH, counts, Mutability::Mutable).unwrap();
    let mut snapshot = Snapshot::new(table);
    let before = snapshot
        .revalidate(PublishedSequence::NONE)
        .unwrap()
        .plan_persist(&owner())
        .unwrap();
    let cursors_before = snapshot.table().counts().to_vec();

    snapshot.dilute(24).unwrap();
    assert_eq!(
        snapshot.table().counts(),
        cursors_before.as_slice(),
        "dilution must not move any cursor"
    );

    let after = snapshot
        .revalidate(PublishedSequence::NONE)
        .unwrap()
        .plan_persist(&owner())
        .unwrap();
    assert_eq!(before.chunks.len(), after.chunks.len());
    for (a, b) in before.chunks[1..].iter().zip(after.chunks[1..].iter()) {
        assert_eq!(a.payload, b.payload, "leaf bytes must survive dilution");
    }
    let recovered = roundtrip(&after);
    assert_eq!(recovered.table().depth(), 24);
    assert_eq!(recovered, snapshot);
}

#[test]
fn merge_max_rejects_mutable() {
    let buckets = 1usize << BUCKET_DEPTH;
    let a = UsageTable::from_counts(
        batch_id(),
        20,
        BUCKET_DEPTH,
        synthetic_counts(buckets, 1, 2),
        Mutability::Mutable,
    )
    .unwrap();
    let b = UsageTable::from_counts(
        batch_id(),
        20,
        BUCKET_DEPTH,
        synthetic_counts(buckets, 1, 2),
        Mutability::Immutable,
    )
    .unwrap();
    // merge_max is exposed on the snapshot now; a mutable table is rejected.
    let mut snapshot = Snapshot::new(a);
    assert_eq!(snapshot.merge_max(&b), Err(UsageError::MutableMerge));
}

#[test]
fn recovered_mutable_issues_reserved_aware_through_the_handle() {
    // A small geometry where the root's bucket is a shallow ring, so a
    // reserved-blind ring cursor would re-emit the reserved slot quickly.
    let depth = 18; // capacity 4 per bucket
    let buckets = 1usize << BUCKET_DEPTH;
    // Fill the table close to full so cursors wrap fast.
    let counts = vec![3u32; buckets];
    let table =
        UsageTable::from_counts(batch_id(), depth, BUCKET_DEPTH, counts, Mutability::Mutable)
            .unwrap();
    let mut snapshot = Snapshot::new(table);
    let plan = snapshot
        .revalidate(PublishedSequence::NONE)
        .unwrap()
        .plan_persist(&owner())
        .unwrap();
    let reserved = snapshot.reserved_stamp_indices(&owner());

    // Recover the snapshot from the wire. It carries no reserved state; the
    // issuing handle installs it at construction, so the only counter-advance
    // path is reserved-aware from the very first write.
    let mut issuing = roundtrip(&plan);
    let root_index = reserved[0];

    // Find a content address that maps to the root's reserved bucket.
    let bucket = root_index.bucket();
    let content = address_in_bucket(bucket);
    assert_eq!(calculate_bucket(&content, BUCKET_DEPTH), bucket);
    for _ in 0..32 {
        let index = issuing.issuer(owner()).record_address(&content).unwrap();
        assert_ne!(
            index, root_index,
            "the reserved root slot must never be re-emitted"
        );
    }
    // The reserved slot value is intact in the snapshot view.
    assert!(issuing.is_reserved(&owner(), root_index));
}

#[test]
fn extract_then_rebuild_preserves_sequence_and_slots() {
    let buckets = 1usize << BUCKET_DEPTH;
    let counts = synthetic_counts(buckets, 10, 15);
    let table =
        UsageTable::from_counts(batch_id(), 22, BUCKET_DEPTH, counts, Mutability::Immutable)
            .unwrap();
    let mut snapshot = Snapshot::new(table);

    // Persist a few times so the sequence climbs above 0 and slots are allocated.
    snapshot
        .revalidate(PublishedSequence::NONE)
        .unwrap()
        .plan_persist(&owner())
        .unwrap();
    snapshot
        .revalidate(PublishedSequence::NONE)
        .unwrap()
        .plan_persist(&owner())
        .unwrap();
    let plan = snapshot
        .revalidate(PublishedSequence::NONE)
        .unwrap()
        .plan_persist(&owner())
        .unwrap();
    let sequence = snapshot.sequence();
    let slots = snapshot.allocated_slots().to_vec();
    assert_eq!(sequence, 3);
    assert!(!slots.is_empty());

    // Extracting and rebuilding through the opaque parts preserves the sequence
    // and the allocated slots: there is no safe route that resets them to a
    // fresh sequence-0 snapshot.
    let parts = snapshot.clone().into_parts();
    assert_eq!(parts.sequence(), sequence);
    assert_eq!(parts.allocated_slots(), slots.as_slice());
    let rebuilt = Snapshot::from_parts(parts).unwrap();
    assert_eq!(rebuilt.sequence(), sequence);
    assert_eq!(rebuilt.allocated_slots(), slots.as_slice());
    assert_eq!(rebuilt, snapshot);

    // The next persist from the rebuilt snapshot strictly advances the sequence
    // and keeps the metadata slots stable, exactly as it would from the original.
    let mut rebuilt = rebuilt;
    let next = rebuilt
        .revalidate(PublishedSequence::NONE)
        .unwrap()
        .plan_persist(&owner())
        .unwrap();
    assert_eq!(next.sequence, sequence + 1);
    assert_eq!(rebuilt.allocated_slots(), slots.as_slice());
    // Stamp indices for the metadata chunks are unchanged across the rebuild.
    for (a, b) in plan.chunks.iter().zip(next.chunks.iter()) {
        assert_eq!(a.stamp_index, b.stamp_index);
        assert_eq!(a.address, b.address);
    }
}

#[test]
fn recovered_snapshot_rebuilds_through_from_parts_without_regressing() {
    let buckets = 1usize << BUCKET_DEPTH;
    let counts = synthetic_counts(buckets, 64, 63);
    let table =
        UsageTable::from_counts(batch_id(), 24, BUCKET_DEPTH, counts, Mutability::Immutable)
            .unwrap();
    let mut snapshot = Snapshot::new(table);
    snapshot
        .revalidate(PublishedSequence::NONE)
        .unwrap()
        .plan_persist(&owner())
        .unwrap();
    let plan = snapshot
        .revalidate(PublishedSequence::NONE)
        .unwrap()
        .plan_persist(&owner())
        .unwrap();

    // Recover from the wire, then round-trip through the opaque parts. The
    // sequence and slots survive both hops.
    let recovered = roundtrip(&plan);
    assert_eq!(recovered.sequence(), snapshot.sequence());
    let rebuilt = Snapshot::from_parts(recovered.into_parts()).unwrap();
    assert_eq!(rebuilt.sequence(), snapshot.sequence());
    assert_eq!(rebuilt.allocated_slots(), snapshot.allocated_slots());
}

#[test]
fn table_view_exposes_counts_and_geometry_without_yielding_an_owned_table() {
    let buckets = 1usize << BUCKET_DEPTH;
    let mut counts = synthetic_counts(buckets, 64, 63);
    counts[7] = 200;
    let table = UsageTable::from_counts(
        batch_id(),
        24,
        BUCKET_DEPTH,
        counts.clone(),
        Mutability::Immutable,
    )
    .unwrap();
    let issued = table.total_issued();
    let mut snapshot = Snapshot::new(table);
    snapshot
        .revalidate(PublishedSequence::NONE)
        .unwrap()
        .plan_persist(&owner())
        .unwrap();

    // Recover from the wire so the assertions run against a view of a recovered
    // snapshot, exactly the case the clone-path closure protects.
    let plan = snapshot
        .revalidate(PublishedSequence::NONE)
        .unwrap()
        .plan_persist(&owner())
        .unwrap();
    let recovered = roundtrip(&plan);
    let view = recovered.table();

    // Test geometry: `buckets = 2^BUCKET_DEPTH` fits u32.
    #[allow(clippy::as_conversions)]
    let buckets_u32 = buckets as u32;

    // Geometry getters.
    assert_eq!(view.batch_id(), batch_id());
    assert_eq!(view.depth(), 24);
    assert_eq!(view.bucket_depth(), BUCKET_DEPTH);
    assert_eq!(view.bucket_count(), buckets_u32);
    assert_eq!(view.bucket_capacity(), 1u32 << (24 - BUCKET_DEPTH));
    assert_eq!(view.total_capacity(), 1u64 << 24);
    assert!(!view.is_mutable());

    // Counter getters. The snapshot's own chunks bumped a few buckets above the
    // synthetic counts, so total_issued only grew.
    assert!(view.total_issued() >= issued);
    assert_eq!(view.count(7).unwrap(), 200);
    assert_eq!(view.counts().len(), buckets);
    assert_eq!(view.max_count(), 200);
    assert_eq!(
        view.min_count(),
        recovered.table().counts().iter().copied().min().unwrap()
    );
    assert!(
        view.count(buckets_u32).is_err(),
        "out-of-range bucket errors"
    );
    assert!(view.has_capacity(7).unwrap());

    // The view is Copy, so a caller can take a cheap second window, but cloning it
    // yields another view, never an owned UsageTable that Snapshot::new accepts.
    let second = view;
    assert_eq!(second.depth(), view.depth());
}

/// Issuance since the last persist makes a snapshot dirty; persisting clears it.
/// `stamps_since_persist` counts the unpersisted stamps in immutable mode.
#[test]
fn is_dirty_and_stamps_since_persist_track_unpersisted_issuance() {
    let table = UsageTable::new(batch_id(), 20, BUCKET_DEPTH, Mutability::Immutable).unwrap();
    let mut snapshot = Snapshot::new(table);

    // A fresh snapshot that has issued nothing is clean.
    assert!(!snapshot.is_dirty());
    assert_eq!(snapshot.stamps_since_persist(), Some(0));

    // Issuing makes it dirty before any persist.
    let address = address_in_bucket(0x1234);
    snapshot.issuer(owner()).record_address(&address).unwrap();
    snapshot.issuer(owner()).record_address(&address).unwrap();
    assert!(snapshot.is_dirty());
    assert_eq!(snapshot.stamps_since_persist(), Some(2));

    // Persisting clears the dirty flag and resets the count, even though the
    // persist itself issues stamps for the snapshot's own chunks.
    snapshot
        .revalidate(PublishedSequence::NONE)
        .unwrap()
        .plan_persist(&owner())
        .unwrap();
    assert!(!snapshot.is_dirty());
    assert_eq!(snapshot.stamps_since_persist(), Some(0));

    // Further issuance makes it dirty again, counted from the persist baseline.
    snapshot.issuer(owner()).record_address(&address).unwrap();
    assert!(snapshot.is_dirty());
    assert_eq!(snapshot.stamps_since_persist(), Some(1));

    // Another persist clears it again.
    snapshot
        .revalidate(PublishedSequence::NONE)
        .unwrap()
        .plan_persist(&owner())
        .unwrap();
    assert!(!snapshot.is_dirty());
    assert_eq!(snapshot.stamps_since_persist(), Some(0));
}

/// A mutable snapshot has no well-defined unpersisted count (the ring sum is a
/// checksum), so `stamps_since_persist` is `None`, but `is_dirty` still flips on
/// ring churn and clears on persist.
#[test]
fn mutable_is_dirty_tracks_churn_but_count_is_none() {
    let table = UsageTable::new(batch_id(), 20, BUCKET_DEPTH, Mutability::Mutable).unwrap();
    let mut snapshot = Snapshot::new(table);
    assert!(snapshot.table().is_mutable());

    assert!(!snapshot.is_dirty());
    assert_eq!(snapshot.stamps_since_persist(), None);

    snapshot
        .issuer(owner())
        .record_address(&address_in_bucket(0x1234))
        .unwrap();
    assert!(snapshot.is_dirty());
    assert_eq!(snapshot.stamps_since_persist(), None);

    snapshot
        .revalidate(PublishedSequence::NONE)
        .unwrap()
        .plan_persist(&owner())
        .unwrap();
    assert!(!snapshot.is_dirty());
}

/// On a mutable ring whose cursor has reached the bucket bound, the next content
/// write wraps onto a previously-used slot; `will_wrap` predicts it and
/// `record_address_reporting_wrap` reports it. The signal fires at the wrap
/// boundary (the cursor sitting at capacity), once per ring cycle: a wrap resets
/// the cursor low, so the next overwrite is flagged again only once the cursor
/// climbs back to the bound. An immutable bucket never wraps.
#[test]
fn mutable_ring_wrap_is_signalled() {
    // Capacity 2 per bucket; fill the target bucket so the next write wraps.
    let buckets = 1usize << BUCKET_DEPTH;
    let counts = vec![0u32; buckets];
    let table =
        UsageTable::from_counts(batch_id(), 17, BUCKET_DEPTH, counts, Mutability::Mutable).unwrap();
    let mut snapshot = Snapshot::new(table);
    let capacity = snapshot.table().bucket_capacity();
    assert_eq!(capacity, 2);

    // Pick a content bucket clear of the snapshot's reserved slots.
    let content = address_in_bucket(0x1234);
    let bucket = calculate_bucket(&content, BUCKET_DEPTH);

    let mut issuer = snapshot.issuer(owner());

    // Fill the bucket to its bound: each fresh slot reports no wrap, and the
    // cursor only reaches the wrap boundary on the last fill.
    for slot in 0..capacity {
        assert!(
            !issuer.will_wrap(bucket).unwrap(),
            "fresh slot {slot} is not a wrap"
        );
        let (_, wrapped) = issuer.record_address_reporting_wrap(&content).unwrap();
        assert!(!wrapped, "filling fresh slot {slot} does not wrap");
    }

    // The cursor now sits at the bound: the next write wraps onto a used slot.
    assert!(issuer.will_wrap(bucket).unwrap());
    let (_, wrapped) = issuer.record_address_reporting_wrap(&content).unwrap();
    assert!(wrapped, "the ring wrapped onto a previously-used slot");

    // The wrap reset the cursor below the bound, so immediately overwriting is
    // not re-flagged until the cursor climbs back; a full further cycle returns
    // to the boundary and the wrap fires once more.
    assert!(!issuer.will_wrap(bucket).unwrap());
    for _ in 1..capacity {
        let (_, wrapped) = issuer.record_address_reporting_wrap(&content).unwrap();
        assert!(!wrapped);
    }
    assert!(
        issuer.will_wrap(bucket).unwrap(),
        "back at the bound after a full cycle"
    );
    assert!(issuer.record_address_reporting_wrap(&content).unwrap().1);
}

/// An immutable bucket never wraps: `will_wrap` is always false, and a full
/// bucket fails rather than overwriting, so no eviction is ever signalled.
#[test]
fn immutable_never_wraps() {
    let table = UsageTable::new(batch_id(), 17, BUCKET_DEPTH, Mutability::Immutable).unwrap();
    let mut snapshot = Snapshot::new(table);
    let content = address_in_bucket(0x1234);
    let bucket = calculate_bucket(&content, BUCKET_DEPTH);

    let mut issuer = snapshot.issuer(owner());
    assert!(!issuer.will_wrap(bucket).unwrap());
    // Capacity is 2; fill it.
    assert!(!issuer.record_address_reporting_wrap(&content).unwrap().1);
    assert!(!issuer.record_address_reporting_wrap(&content).unwrap().1);
    // Full now: will_wrap stays false (immutable does not overwrite) and the
    // next write fails instead.
    assert!(!issuer.will_wrap(bucket).unwrap());
    assert!(issuer.record_address(&content).is_err());
}

/// A steady-state persist (no new slot to allocate) changes only the sequence:
/// the counter table and the allocated slots are byte-identical before and
/// after, which is the observable signature of the no-clone fast path. A persist
/// that must allocate is exercised by the dilution and growth tests elsewhere.
#[test]
fn steady_state_persist_changes_only_the_sequence() {
    let buckets = 1usize << BUCKET_DEPTH;
    let counts = synthetic_counts(buckets, 10, 15);
    let table =
        UsageTable::from_counts(batch_id(), 22, BUCKET_DEPTH, counts, Mutability::Immutable)
            .unwrap();
    let mut snapshot = Snapshot::new(table);

    // First persist allocates the snapshot's own slots.
    snapshot
        .revalidate(PublishedSequence::NONE)
        .unwrap()
        .plan_persist(&owner())
        .unwrap();
    let counts_before = snapshot.table().counts().to_vec();
    let slots_before = snapshot.allocated_slots().to_vec();
    let issued_before = snapshot.table().total_issued();
    let sequence_before = snapshot.sequence();

    // The second persist is steady state: nothing to allocate, so the table is
    // untouched and only the sequence advances.
    snapshot
        .revalidate(PublishedSequence::NONE)
        .unwrap()
        .plan_persist(&owner())
        .unwrap();
    assert_eq!(snapshot.table().counts(), counts_before.as_slice());
    assert_eq!(snapshot.allocated_slots(), slots_before.as_slice());
    assert_eq!(snapshot.table().total_issued(), issued_before);
    assert_eq!(snapshot.sequence(), sequence_before + 1);
}

/// A persist that fails mid-allocation leaves the snapshot wholly unchanged: the
/// clone-on-allocation path preserves rollback. A bucket pinned at capacity on a
/// fresh table makes the very first (root) allocation fail.
#[test]
fn failed_allocation_rolls_back_the_snapshot() {
    // Capacity 1 per bucket; fill every bucket so the root's own allocation
    // immediately hits a full bucket.
    let buckets = 1usize << BUCKET_DEPTH;
    let counts = vec![1u32; buckets];
    let table =
        UsageTable::from_counts(batch_id(), 16, BUCKET_DEPTH, counts, Mutability::Immutable)
            .unwrap();
    let mut snapshot = Snapshot::new(table);
    let counts_before = snapshot.table().counts().to_vec();
    let issued_before = snapshot.table().total_issued();

    let err = snapshot
        .revalidate(PublishedSequence::NONE)
        .unwrap()
        .plan_persist(&owner())
        .unwrap_err();
    assert!(matches!(err, UsageError::BucketFull { .. }));

    // The snapshot is untouched: no slot allocated, no counter moved, sequence
    // still zero.
    assert_eq!(snapshot.sequence(), 0);
    assert!(snapshot.allocated_slots().is_empty());
    assert_eq!(snapshot.table().counts(), counts_before.as_slice());
    assert_eq!(snapshot.table().total_issued(), issued_before);
}

/// Returns a chunk address whose top `BUCKET_DEPTH` bits select `bucket`.
fn address_in_bucket(bucket: u32) -> nectar_primitives::SwarmAddress {
    let mut bytes = [0u8; 32];
    // bucket occupies the most-significant BUCKET_DEPTH (=16) bits: take the
    // low two big-endian bytes of the u32 (identical to the former `>> 8` /
    // truncating casts).
    let [_, _, hi, lo] = bucket.to_be_bytes();
    bytes[0] = hi;
    bytes[1] = lo;
    nectar_primitives::SwarmAddress::new(bytes)
}

/// A fresh table for a batch already published at a higher sequence is the
/// fresh-construction forgery (attack a): the public constructors must keep
/// minting fresh tables for genuinely new batches, so the floor is the only
/// thing that can reject a forged sequence-0 persist against a published batch.
#[test]
fn fresh_construction_forgery_is_rejected_by_the_floor() {
    let table = UsageTable::new(batch_id(), 20, BUCKET_DEPTH, Mutability::Immutable).unwrap();
    let mut snapshot = Snapshot::new(table);
    // The published root the consumer reads live already carries sequence 5, so
    // the floor is 5. A fresh snapshot would emit sequence 1, which does not
    // exceed it.
    let floor = PublishedSequence::new(5);
    assert_eq!(
        snapshot.revalidate(floor).unwrap_err(),
        UsageError::StaleSequence { next: 1, floor: 5 },
    );
}

/// Advances a snapshot to sequence `n` through the real persist path, then
/// recovers it from the wire so it stands in for a snapshot loaded from a stale
/// cache at sequence `n`.
fn recovered_at_sequence(n: u64) -> Snapshot {
    let table = UsageTable::new(batch_id(), 20, BUCKET_DEPTH, Mutability::Immutable).unwrap();
    let mut snapshot = Snapshot::new(table);
    let mut plan = None;
    for _ in 0..n {
        plan = Some(
            snapshot
                .revalidate(PublishedSequence::NONE)
                .unwrap()
                .plan_persist(&owner())
                .unwrap(),
        );
    }
    let plan = plan.expect("n must be at least 1");
    let recovered = roundtrip(&plan);
    assert_eq!(recovered.sequence(), n);
    recovered
}

/// A snapshot recovered from a stale cache at sequence R persists at R + 1; if
/// the live published sequence has moved past that, the floor rejects it
/// (attack b). A non-increasing sequence can never overwrite a newer published
/// snapshot.
#[test]
fn stale_recovered_persist_is_rejected_by_the_floor() {
    let mut recovered = recovered_at_sequence(4);
    // Cached sequence is 4, so the next persist would be 5. The live floor is 7,
    // so 5 does not exceed it and the persist is rejected.
    assert_eq!(
        recovered.revalidate(PublishedSequence::new(7)).unwrap_err(),
        UsageError::StaleSequence { next: 5, floor: 7 },
    );
    // Equality is also rejected: next must strictly exceed the floor.
    assert_eq!(
        recovered.revalidate(PublishedSequence::new(5)).unwrap_err(),
        UsageError::StaleSequence { next: 5, floor: 5 },
    );
}

/// A legitimate persist whose next sequence strictly exceeds the published floor
/// is admitted and plans as usual.
#[test]
fn persist_above_the_floor_succeeds() {
    let mut recovered = recovered_at_sequence(4);
    // Cached sequence 4, so next is 5; the live floor is 4, so 5 exceeds it.
    let mut validated = recovered.revalidate(PublishedSequence::new(4)).unwrap();
    assert_eq!(validated.floor(), 4);
    let plan = validated.plan_persist(&owner()).unwrap();
    assert_eq!(plan.sequence, 5);
    assert_eq!(recovered.sequence(), 5);
}

/// The `NONE` floor admits the very first persist of a genuinely new batch: next
/// is 1 and `1 > 0`.
#[test]
fn none_floor_admits_the_first_persist() {
    let table = UsageTable::new(batch_id(), 20, BUCKET_DEPTH, Mutability::Immutable).unwrap();
    let mut snapshot = Snapshot::new(table);
    let plan = snapshot
        .revalidate(PublishedSequence::NONE)
        .unwrap()
        .plan_persist(&owner())
        .unwrap();
    assert_eq!(plan.sequence, 1);
}

/// `PublishedSequence::from(&RootInfo)` reads the floor straight off a parsed
/// live root chunk, the only sanctioned source of the floor value.
#[test]
fn published_sequence_reads_from_a_parsed_root() {
    let table = UsageTable::new(batch_id(), 20, BUCKET_DEPTH, Mutability::Immutable).unwrap();
    let mut snapshot = Snapshot::new(table);
    // Publish twice so the live root carries sequence 2.
    snapshot
        .revalidate(PublishedSequence::NONE)
        .unwrap()
        .plan_persist(&owner())
        .unwrap();
    let plan = snapshot
        .revalidate(PublishedSequence::NONE)
        .unwrap()
        .plan_persist(&owner())
        .unwrap();
    let root = RootInfo::parse(&plan.chunks[0].payload).unwrap();
    assert_eq!(root.sequence(), 2);
    let floor = PublishedSequence::from(&root);
    assert_eq!(floor.get(), 2);
    // A would-be writer holding the same cached state (sequence 2) would plan
    // sequence 3, which clears the floor of 2.
    let mut next = snapshot;
    let plan = next
        .revalidate(floor)
        .unwrap()
        .plan_persist(&owner())
        .unwrap();
    assert_eq!(plan.sequence, 3);
}
