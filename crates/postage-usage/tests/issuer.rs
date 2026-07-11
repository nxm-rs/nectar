//! Tests for the `SnapshotIssuer` `StampIssuer` implementation, the sole
//! owner-aware issuance path bound to a `Snapshot`'s shared table.

#![cfg(feature = "issuer")]
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
use nectar_postage::{BatchId, StampIndex, calculate_bucket};
use nectar_postage_issuer::StampIssuer;
use nectar_postage_usage::{Mutability, PublishedSequence, Snapshot, SnapshotIssuer, UsageTable};
use nectar_primitives::SwarmAddress;

const BUCKET_DEPTH: u8 = 16;

const fn owner() -> Address {
    Address::repeat_byte(0x11)
}

/// Returns a content chunk address whose top 16 bits select `bucket`, with
/// `salt` mixed into lower bytes so distinct calls stay in the same bucket.
fn content_address(bucket: u32, salt: u8) -> SwarmAddress {
    let mut bytes = [0u8; 32];
    // Take the low two big-endian bytes of the u32 (identical to the former
    // `>> 8` / truncating casts).
    let [_, _, hi, lo] = bucket.to_be_bytes();
    bytes[0] = hi;
    bytes[1] = lo;
    bytes[31] = salt;
    SwarmAddress::new(bytes)
}

#[test]
fn snapshot_issuer_issues_sequential_indices() {
    let batch_id = BatchId::new([0x42; 32]);
    let table = UsageTable::new(batch_id, 18, BUCKET_DEPTH, Mutability::Immutable).unwrap();
    // A fresh, never-persisted snapshot reserves no slots, so issuance fills the
    // bucket from index zero just as a bare table once did.
    let snapshot = Snapshot::new(table);
    let mut issuer = SnapshotIssuer::new(snapshot, owner());

    let address = SwarmAddress::new([0xCB; 32]);
    let first = issuer.prepare_stamp(&address, 1).unwrap();
    let second = issuer.prepare_stamp(&address, 2).unwrap();

    assert_eq!(first.batch_id, batch_id);
    assert_eq!(first.index.bucket(), 0xCBCB);
    assert_eq!(first.index.index(), 0);
    assert_eq!(second.index.index(), 1);

    assert_eq!(StampIssuer::batch_id(&issuer), batch_id);
    assert_eq!(issuer.batch_depth(), 18);
    assert_eq!(StampIssuer::bucket_depth(&issuer), BUCKET_DEPTH);
    // An immutable snapshot tracks a true monotone count.
    assert_eq!(issuer.stamps_issued(), Some(2));
    assert_eq!(issuer.max_bucket_utilization(), 2);
    assert_eq!(issuer.bucket_utilization(0xCBCB), 2);
    assert!(issuer.bucket_has_capacity(0xCBCB));

    // Capacity is 4 at depth 18; exhaust the bucket.
    issuer.prepare_stamp(&address, 3).unwrap();
    issuer.prepare_stamp(&address, 4).unwrap();
    assert!(!issuer.bucket_has_capacity(0xCBCB));
    assert!(issuer.prepare_stamp(&address, 5).is_err());
}

#[test]
fn shared_table_immutable_never_collides_with_reserved_slots() {
    let batch_id = BatchId::new([0x42; 32]);
    let counts = vec![5u32; 1usize << BUCKET_DEPTH];
    let table =
        UsageTable::from_counts(batch_id, 24, BUCKET_DEPTH, counts, Mutability::Immutable).unwrap();
    let mut snapshot = Snapshot::new(table);

    // Persist once, recording the snapshot's own reserved slots.
    let plan = snapshot
        .revalidate(PublishedSequence::NONE)
        .unwrap()
        .plan_persist(&owner())
        .unwrap();
    let reserved = snapshot.reserved_stamp_indices(&owner());

    // Stamp content through the SAME table; collect every issued index.
    for chunk in &plan.chunks {
        let bucket = chunk.stamp_index.bucket();
        for salt in 0..20u8 {
            let addr = content_address(bucket, salt);
            let index = snapshot.issuer(owner()).record_address(&addr).unwrap();
            assert!(
                !reserved.contains(&index),
                "content stamp collided with a reserved snapshot slot"
            );
        }
    }
}

#[test]
fn shared_table_mutable_skips_reserved_across_wraps() {
    let batch_id = BatchId::new([0x42; 32]);
    // Capacity 4 per bucket; fill near-full so the ring wraps almost at once.
    let counts = vec![2u32; 1usize << BUCKET_DEPTH];
    let table =
        UsageTable::from_counts(batch_id, 18, BUCKET_DEPTH, counts, Mutability::Mutable).unwrap();
    assert!(table.is_mutable());
    let mut snapshot = Snapshot::new(table);

    let plan = snapshot
        .revalidate(PublishedSequence::NONE)
        .unwrap()
        .plan_persist(&owner())
        .unwrap();
    let reserved = snapshot.reserved_stamp_indices(&owner());

    // For each reserved bucket, churn the ring many times; the reserved slot
    // must never be re-emitted and must stay intact after multiple wraps.
    for chunk in &plan.chunks {
        let bucket = chunk.stamp_index.bucket();
        let reserved_here: Vec<StampIndex> = reserved
            .iter()
            .copied()
            .filter(|r| r.bucket() == bucket)
            .collect();
        for salt in 0..200u8 {
            let addr = content_address(bucket, salt);
            assert_eq!(calculate_bucket(&addr, BUCKET_DEPTH), bucket);
            let index = snapshot.issuer(owner()).record_address(&addr).unwrap();
            assert!(
                !reserved_here.contains(&index),
                "mutable content stamp re-emitted a reserved slot after wrap"
            );
        }
        // Reserved slots are still recognised as reserved after the churn.
        for r in &reserved_here {
            assert!(snapshot.is_reserved(&owner(), *r));
        }
    }
}

/// Regression guard for nectar issue #56: the owner-unaware `StampIssuer for
/// UsageTable` path is gone, so the only way to obtain a `StampIssuer` from this
/// crate is through a `Snapshot`, which is owner-aware. This test drives the
/// sole entry point through the exact scenario the deleted bare-table path would
/// have mishandled (a near-full mutable batch whose ring wraps onto the
/// snapshot's own slots) and asserts those reserved slots are never evicted. If
/// a bare-table issuance path were ever reintroduced, the eviction it allows
/// would have no owner context to skip the reserved set and this guarantee could
/// not hold.
#[test]
fn sole_issuance_path_cannot_evict_snapshot_slots() {
    let batch_id = BatchId::new([0x42; 32]);
    // A mutable bucket at capacity 4, pre-filled to 3 so the very next stamp
    // wraps the ring and would land on the reserved slot under a naive issuer.
    let counts = vec![3u32; 1usize << BUCKET_DEPTH];
    let table =
        UsageTable::from_counts(batch_id, 18, BUCKET_DEPTH, counts, Mutability::Mutable).unwrap();
    assert!(table.is_mutable());

    // `SnapshotIssuer` is the only `StampIssuer` this crate exposes. Persist
    // first so the snapshot reserves its own slots, then issue through the
    // issuer alone.
    let mut snapshot = Snapshot::new(table);
    let plan = snapshot
        .revalidate(PublishedSequence::NONE)
        .unwrap()
        .plan_persist(&owner())
        .unwrap();
    let reserved = snapshot.reserved_stamp_indices(&owner());
    assert!(
        !reserved.is_empty(),
        "persist must reserve at least the root"
    );

    let mut issuer = SnapshotIssuer::new(snapshot, owner());
    for chunk in &plan.chunks {
        let bucket = chunk.stamp_index.bucket();
        let reserved_here: Vec<StampIndex> = reserved
            .iter()
            .copied()
            .filter(|r| r.bucket() == bucket)
            .collect();
        // Churn well past several wraps of the ring.
        for salt in 0..120u8 {
            let addr = content_address(bucket, salt);
            let digest = issuer.prepare_stamp(&addr, u64::from(salt)).unwrap();
            assert!(
                !reserved_here.contains(&digest.index),
                "the sole issuance path evicted a reserved snapshot slot"
            );
        }
    }

    // The snapshot's own chunks survive: recover it and confirm every reserved
    // slot is still recognised.
    let snapshot = issuer.into_snapshot();
    for index in &reserved {
        assert!(snapshot.is_reserved(&owner(), *index));
    }
}

/// Both crates advance counters through the one shared table, so a snapshot's
/// immutable issuance and a standalone `MemoryIssuer` over the same geometry
/// assign byte-identical slots for the same addresses. If `record_bucket` still
/// hand-rolled its own watermark this parity would be a coincidence; with the
/// delegation it is structural.
#[test]
fn shared_counter_table_backs_both_crates_identically() {
    use nectar_postage_issuer::MemoryIssuer;

    let batch_id = BatchId::new([0x42; 32]);
    // A fresh, never-persisted snapshot reserves no slots, so its fill watermark
    // advances exactly like a bare MemoryIssuer.
    let table = UsageTable::new(batch_id, 20, BUCKET_DEPTH, Mutability::Immutable).unwrap();
    let mut snapshot = Snapshot::new(table);
    let mut memory = MemoryIssuer::new(batch_id, 20, BUCKET_DEPTH);

    for bucket in [0x0001u32, 0x0001, 0xCBE5, 0x0001, 0xCBE5] {
        for salt in 0..3u8 {
            let addr = content_address(bucket, salt);
            let from_snapshot = snapshot.issuer(owner()).record_address(&addr).unwrap();
            let from_memory = memory.prepare_stamp(&addr, 0).unwrap().index;
            assert_eq!(
                from_snapshot, from_memory,
                "snapshot and MemoryIssuer diverged; they no longer share one table"
            );
        }
    }

    // The shared counter sum is the lifetime count in immutable mode for both.
    assert_eq!(
        snapshot.table().total_issued(),
        memory.stamps_issued().unwrap()
    );
}

#[test]
fn snapshot_issuer_adapter_drives_a_batch_stamper_path() {
    use alloy_signer_local::PrivateKeySigner;
    use nectar_postage_issuer::BatchStamper;

    let signer = PrivateKeySigner::random();
    let owner = signer.address();
    let batch_id = BatchId::new([0x77; 32]);

    let table = UsageTable::new(batch_id, 20, BUCKET_DEPTH, Mutability::Mutable).unwrap();
    let mut snapshot = Snapshot::new(table);
    snapshot
        .revalidate(PublishedSequence::NONE)
        .unwrap()
        .plan_persist(&owner)
        .unwrap();
    let reserved = snapshot.reserved_stamp_indices(&owner);

    // The adapter owns the snapshot and drops into BatchStamper by value.
    let issuer = SnapshotIssuer::new(snapshot, owner);
    assert_eq!(StampIssuer::batch_id(&issuer), batch_id);
    let mut stamper = BatchStamper::new(issuer, signer);

    // Stamp content into the reserved bucket; never collides with reserved.
    let bucket = reserved[0].bucket();
    use nectar_postage_issuer::Stamper;
    for salt in 0..32u8 {
        let addr = content_address(bucket, salt);
        let stamp = stamper.stamp(&addr).unwrap();
        assert!(!reserved.contains(&stamp.stamp_index()));
    }
}
