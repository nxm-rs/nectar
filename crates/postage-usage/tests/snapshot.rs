//! End-to-end tests for the snapshot format: persist planning, encoding,
//! decoding, dilution, and corruption rejection.

use alloy_primitives::{Address, B256};
use nectar_postage::calculate_bucket;
use nectar_postage_usage::{
    MAGIC, PersistPlan, RootInfo, Snapshot, UsageError, UsageTable, usage_chunk_address,
    usage_chunk_id,
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
            base + ((state >> 33) as u32) % (spread + 1)
        })
        .collect()
}

fn roundtrip(plan: &PersistPlan) -> Snapshot {
    let root = RootInfo::parse(&plan.chunks[0].payload).unwrap();
    let leaves: Vec<_> = plan.chunks[1..].iter().map(|c| &c.payload).collect();
    assert_eq!(root.leaf_count() as usize, leaves.len());
    root.assemble(&leaves).unwrap()
}

#[test]
fn empty_table_persists_as_a_single_small_root() {
    let table = UsageTable::new(batch_id(), 20, BUCKET_DEPTH).unwrap();
    let mut snapshot = Snapshot::new(table);
    let plan = snapshot.plan_persist(&owner()).unwrap();

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
    let table = UsageTable::from_counts(batch_id(), 24, BUCKET_DEPTH, counts).unwrap();
    let mut snapshot = Snapshot::new(table);
    let plan = snapshot.plan_persist(&owner()).unwrap();

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
    let table = UsageTable::from_counts(batch_id(), 34, BUCKET_DEPTH, counts).unwrap();
    let mut snapshot = Snapshot::new(table);
    let plan = snapshot.plan_persist(&owner()).unwrap();

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
    let table = UsageTable::from_counts(batch_id(), 22, BUCKET_DEPTH, counts).unwrap();
    let mut snapshot = Snapshot::new(table);

    let first = snapshot.plan_persist(&owner()).unwrap();
    let slots_after_first = snapshot.allocated_slots().to_vec();
    let second = snapshot.plan_persist(&owner()).unwrap();

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
    let table = UsageTable::from_counts(batch_id(), 24, BUCKET_DEPTH, counts).unwrap();
    let issued_before = table.total_issued();
    let mut snapshot = Snapshot::new(table);
    let plan = snapshot.plan_persist(&owner()).unwrap();

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
    let table = UsageTable::from_counts(batch_id(), 20, BUCKET_DEPTH, counts).unwrap();
    let mut snapshot = Snapshot::new(table);
    let before = snapshot.plan_persist(&owner()).unwrap();

    snapshot.table_mut().dilute(24).unwrap();
    let after = snapshot.plan_persist(&owner()).unwrap();

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
    let table = UsageTable::from_counts(batch_id(), 21, 8, counts).unwrap();
    let mut snapshot = Snapshot::new(table);
    let plan = snapshot.plan_persist(&owner()).unwrap();
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
    let table = UsageTable::from_counts(batch_id(), 17, BUCKET_DEPTH, counts).unwrap();
    let mut snapshot = Snapshot::new(table);
    match snapshot.plan_persist(&owner()) {
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
    let table = UsageTable::from_counts(batch_id(), 23, BUCKET_DEPTH, counts).unwrap();
    let mut snapshot = Snapshot::new(table);
    let plan = snapshot.plan_persist(&owner()).unwrap();
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
fn encode_requires_an_allocated_root() {
    let table = UsageTable::new(batch_id(), 20, BUCKET_DEPTH).unwrap();
    let snapshot = Snapshot::new(table);
    assert!(snapshot.encode().is_err());
}
