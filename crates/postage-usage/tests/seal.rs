//! Tests for sealing a persist plan into signed chunks and stamps.

#![cfg(feature = "seal")]

use alloy_primitives::B256;
use alloy_signer_local::PrivateKeySigner;
use nectar_postage_usage::{Snapshot, UsageTable, seal_plan, usage_chunk_address};
use nectar_primitives::Chunk;

#[test]
fn sealed_chunks_and_stamps_verify() {
    let signer = PrivateKeySigner::random();
    let owner = signer.address();
    let batch_id = B256::repeat_byte(0x42);

    let mut table = UsageTable::new(batch_id, 20, 16).unwrap();
    table.record(123).unwrap();
    let mut snapshot = Snapshot::new(table);
    let plan = snapshot.plan_persist(&owner).unwrap();

    let sealed = seal_plan(&plan, 1, &signer).unwrap();
    assert_eq!(sealed.len(), plan.chunks.len());

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

    // A different signer is rejected: the chunk address no longer matches.
    let other = PrivateKeySigner::random();
    assert!(seal_plan(&plan, 2, &other).is_err());
}
