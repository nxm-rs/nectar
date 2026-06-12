//! Tests for the `StampIssuer` implementation on `UsageTable`.

#![cfg(feature = "issuer")]

use alloy_primitives::B256;
use nectar_postage_issuer::StampIssuer;
use nectar_postage_usage::UsageTable;
use nectar_primitives::SwarmAddress;

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
