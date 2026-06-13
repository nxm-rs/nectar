//! Tests for the `StampIssuer` implementations on `UsageTable` and the
//! `Snapshot`-bound shared-table path.

#![cfg(feature = "issuer")]

use alloy_primitives::{Address, B256};
use nectar_postage::{StampIndex, calculate_bucket};
use nectar_postage_issuer::StampIssuer;
use nectar_postage_usage::{Snapshot, SnapshotIssuer, UsageTable};
use nectar_primitives::SwarmAddress;

const BUCKET_DEPTH: u8 = 16;

const fn owner() -> Address {
    Address::repeat_byte(0x11)
}

/// Returns a content chunk address whose top 16 bits select `bucket`, with
/// `salt` mixed into lower bytes so distinct calls stay in the same bucket.
fn content_address(bucket: u32, salt: u8) -> SwarmAddress {
    let mut bytes = [0u8; 32];
    bytes[0] = (bucket >> 8) as u8;
    bytes[1] = bucket as u8;
    bytes[31] = salt;
    SwarmAddress::new(bytes)
}

#[test]
fn usage_table_issues_sequential_indices() {
    let batch_id = B256::repeat_byte(0x42);
    let mut table = UsageTable::new(batch_id, 18, 16).unwrap();

    let address = SwarmAddress::new([0xCB; 32]);
    let first = table.prepare_stamp(&address, 1).unwrap();
    let second = table.prepare_stamp(&address, 2).unwrap();

    assert_eq!(first.batch_id, batch_id);
    assert_eq!(first.index.bucket(), 0xCBCB);
    assert_eq!(first.index.index(), 0);
    assert_eq!(second.index.index(), 1);

    assert_eq!(StampIssuer::batch_id(&table), batch_id);
    assert_eq!(table.batch_depth(), 18);
    assert_eq!(StampIssuer::bucket_depth(&table), 16);
    assert_eq!(table.stamps_issued(), 2);
    assert_eq!(table.max_bucket_utilization(), 2);
    assert_eq!(table.bucket_utilization(0xCBCB), 2);
    assert!(table.bucket_has_capacity(0xCBCB));

    // Capacity is 4 at depth 18; exhaust the bucket.
    table.prepare_stamp(&address, 3).unwrap();
    table.prepare_stamp(&address, 4).unwrap();
    assert!(!table.bucket_has_capacity(0xCBCB));
    assert!(table.prepare_stamp(&address, 5).is_err());
}

#[test]
fn shared_table_immutable_never_collides_with_reserved_slots() {
    let batch_id = B256::repeat_byte(0x42);
    let counts = vec![5u32; 1usize << BUCKET_DEPTH];
    let table = UsageTable::from_counts(batch_id, 24, BUCKET_DEPTH, counts).unwrap();
    let mut snapshot = Snapshot::new(table);

    // Persist once, recording the snapshot's own reserved slots.
    let plan = snapshot.plan_persist(&owner()).unwrap();
    let reserved = snapshot.reserved_stamp_indices(&owner());

    // Stamp content through the SAME table; collect every issued index.
    for chunk in &plan.chunks {
        let bucket = chunk.stamp_index.bucket();
        for salt in 0..20u8 {
            let addr = content_address(bucket, salt);
            let index = snapshot.record_address(&owner(), &addr).unwrap();
            assert!(
                !reserved.contains(&index),
                "content stamp collided with a reserved snapshot slot"
            );
        }
    }
}

#[test]
fn shared_table_mutable_skips_reserved_across_wraps() {
    let batch_id = B256::repeat_byte(0x42);
    // Capacity 4 per bucket; fill near-full so the ring wraps almost at once.
    let counts = vec![2u32; 1usize << BUCKET_DEPTH];
    let table = UsageTable::from_counts_mutable(batch_id, 18, BUCKET_DEPTH, counts).unwrap();
    assert!(table.is_mutable());
    let mut snapshot = Snapshot::new(table);

    let plan = snapshot.plan_persist(&owner()).unwrap();
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
            let index = snapshot.record_address(&owner(), &addr).unwrap();
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

#[test]
fn snapshot_issuer_adapter_drives_a_batch_stamper_path() {
    use alloy_signer_local::PrivateKeySigner;
    use nectar_postage_issuer::BatchStamper;

    let signer = PrivateKeySigner::random();
    let owner = signer.address();
    let batch_id = B256::repeat_byte(0x77);

    let table = UsageTable::new_mutable(batch_id, 20, BUCKET_DEPTH).unwrap();
    let mut snapshot = Snapshot::new(table);
    snapshot.plan_persist(&owner).unwrap();
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
