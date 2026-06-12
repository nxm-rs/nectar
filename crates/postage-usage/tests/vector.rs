//! Golden vector pinning the worked example in `README.md`.
//!
//! If this test fails, the wire format changed: either revert the change or
//! bump the format version in the magic and update the README example.

use alloy_primitives::{Address, B256, hex};
use nectar_postage::calculate_bucket;
use nectar_postage_usage::{RootInfo, Snapshot, UsageTable, usage_chunk_address, usage_chunk_id};

/// The full root payload from the README worked example: depth 12, bucket
/// depth 8, counts `3 + (b mod 4)` with bucket 200 full at 16, after one
/// persist by owner `0x11..11` of batch `0x42..42`.
const ROOT_PAYLOAD_HEX: &str = "5342553142424242424242424242424242424242424242424242424242424242424242420c0800020000000000000001000000000000048e00000003000100000001000000c800000010000000041b1b1b1b1b1b1b1b1b1b2b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1bdb1b1b1b1b1b1b1b1b1b1b1b1b1b";

const ROOT_ID_HEX: &str = "292b4137c7a5e52b62615fd3a0f9917fa09c5df5611976597fd4fa156791f6af";
const ROOT_ADDRESS_HEX: &str = "296daebd0b1cd7b78b83016fc9bc9cc62d378c2ff21fb934d7ee0a328145ac5d";

#[test]
fn readme_worked_example_vector() {
    let batch_id = B256::repeat_byte(0x42);
    let owner = Address::repeat_byte(0x11);
    let mut counts: Vec<u32> = (0..256u32).map(|b| 3 + (b & 3)).collect();
    counts[200] = 16;
    let table = UsageTable::from_counts(batch_id, 12, 8, counts).unwrap();
    let mut snapshot = Snapshot::new(table);
    let plan = snapshot.plan_persist(&owner).unwrap();

    // Deterministic addressing.
    assert_eq!(hex::encode(usage_chunk_id(&batch_id, 0)), ROOT_ID_HEX);
    assert_eq!(
        hex::encode(usage_chunk_address(&batch_id, &owner, 0)),
        ROOT_ADDRESS_HEX
    );

    // The root chunk's own stamp lands in bucket 41 at index 4, as
    // narrated in the README.
    assert_eq!(calculate_bucket(&plan.chunks[0].address, 8), 41);
    assert_eq!(snapshot.allocated_slots(), &[4]);
    assert_eq!(snapshot.table().total_issued(), 1166);

    // The exact serialized bytes.
    assert_eq!(plan.chunks.len(), 1);
    assert_eq!(hex::encode(&plan.chunks[0].payload), ROOT_PAYLOAD_HEX);

    // And the vector decodes back to the same snapshot.
    let root = RootInfo::parse(&hex::decode(ROOT_PAYLOAD_HEX).unwrap()).unwrap();
    let recovered = root.assemble(&[] as &[&[u8]]).unwrap();
    assert_eq!(recovered, snapshot);
}
