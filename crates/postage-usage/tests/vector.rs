//! Golden vectors pinning the worked examples in `README.md`.
//!
//! If a test here fails, the wire format changed: either revert the change
//! or bump the format version in the magic and update the README examples.
//!
//! The multi-leaf vector pins only the root payload bytes: the root carries
//! the keccak digest of every leaf, so any change to leaf encoding fails the
//! root comparison and the assemble round-trip. Pinning the root pins the
//! entire snapshot.

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
use alloy_primitives::{Address, hex};
use nectar_postage::{BucketDepth, calculate_bucket};
use nectar_postage_usage::{
    BatchId, Mutability, PublishedSequence, RootInfo, RootInfoFor, Snapshot, SnapshotFor,
    UsageTable, UsageTableFor, usage_chunk_address, usage_chunk_id,
};

mod common;

// The README's small worked example runs at bucket depth 8, which mainnet's
// floor of 16 forbids, so those vectors are pinned for `Shallow`.
use common::{Shallow, shallow};

/// The full root payload from the README worked example: depth 12, bucket
/// depth 8, counts `3 + (b mod 4)` with bucket 200 full at 16, after one
/// persist by owner `0x11..11` of batch `0x42..42`.
const ROOT_PAYLOAD_HEX: &str = "5342553142424242424242424242424242424242424242424242424242424242424242420c0800020000000000000001000000000000048e00000003000100000001000000c800000010000000041b1b1b1b1b1b1b1b1b1b2b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1bdb1b1b1b1b1b1b1b1b1b1b1b1b1b";

const ROOT_ID_HEX: &str = "292b4137c7a5e52b62615fd3a0f9917fa09c5df5611976597fd4fa156791f6af";
const ROOT_ADDRESS_HEX: &str = "296daebd0b1cd7b78b83016fc9bc9cc62d378c2ff21fb934d7ee0a328145ac5d";

#[test]
fn readme_worked_example_vector() {
    let batch_id = BatchId::new([0x42; 32]);
    let owner = Address::repeat_byte(0x11);
    let mut counts: Vec<u32> = (0..256u32).map(|b| 3 + (b & 3)).collect();
    counts[200] = 16;
    let table = UsageTableFor::from_counts(batch_id, 12, shallow(8), counts, Mutability::Immutable)
        .unwrap();
    let mut snapshot = SnapshotFor::new(table);
    let plan = snapshot
        .revalidate(PublishedSequence::NONE)
        .unwrap()
        .plan_persist(&owner)
        .unwrap();

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
    let root = RootInfoFor::<Shallow>::parse(&hex::decode(ROOT_PAYLOAD_HEX).unwrap()).unwrap();
    let recovered = root.assemble::<&[u8]>(&[]).unwrap();
    assert_eq!(recovered, snapshot);
}

/// The root payload of the README large-batch example: depth 29, bucket
/// depth 16 (65536 buckets of 8192 slots), counts `100 + (b mod 50)` with
/// bucket 0x1234 at 5000 and bucket 0xCBE5 full at 8192, after one persist
/// by owner `0x11..11` of batch `0x42..42`. The encoder picks base 100 and
/// width 6, so the table spans 13 leaves and the snapshot self-allocates 14
/// slots (root plus leaves) before reaching its fixed point.
const LARGE_ROOT_PAYLOAD_HEX: &str = "5342553142424242424242424242424242424242424242424242424242424242424242421d100006000000000000000100000000007cb19900000064000e000d000200001234000013880000cbe500002000000000690000007d00000091000000880000007a000000760000006e00000079000000810000006c0000007d000000910000007a0000006b9c8de349a3c4b573d45db35c3585fcbe2e2c20d999ee2a5ead8c1600b5a5428a645b990e3f426220eda38496a262f6968b8a4d42f7becb602c51576f19112fe9fb1f9017218e72abf21947cb3290726dd5129c47e50562a22dfcaa64340dd76d5bb4cba42453e6cb20f8c1c0892bf2bf4873bd3b4850787f171952b662346708660c94f587fa4516af6eb9b083513f245d9bd9fc0559f48356021e51892201fb197b58a495d1305292904a61906b02e37e68101e3657b4e2ff9661d705a004c36ebbe152c039d8887c067cc8fa86636c62afc5f21cd8ca1afc4f546e77882de86c3e8248ce84ffd4c440f28c638007df05e9ea75a713a308e7ba48e6066903c88e6e5e60e48477ca6202b2333d5c06968f67baae3c03105bbb4a7a8491b04ce6f5c8fc3151cdfbc3136e8f0adac485df7ae4d866e1cce1cd1aa0d1cbbf33f19bf28b0420c19fcadaf197e1eaff36f8151ec107d59ad4e6d3a4cf1492a77828991d9c30151c90772df348891a627fa9a5046919dba774e2a388819e9e11aba2c68724aa66f7e98c8ec09b71685da6a49cf173a390ac1662a9e120712062d20fb8";

#[test]
fn readme_large_batch_multi_leaf_vector() {
    let batch_id = BatchId::new([0x42; 32]);
    let owner = Address::repeat_byte(0x11);
    let mut counts: Vec<u32> = (0..65536u32).map(|b| 100 + (b % 50)).collect();
    counts[0x1234] = 5000;
    counts[0xCBE5] = 8192;
    let table = UsageTable::from_counts(
        batch_id,
        29,
        BucketDepth::new(16).unwrap(),
        counts,
        Mutability::Immutable,
    )
    .unwrap();
    let mut snapshot = Snapshot::new(table);
    let plan = snapshot
        .revalidate(PublishedSequence::NONE)
        .unwrap()
        .plan_persist(&owner)
        .unwrap();

    // One root plus 13 leaves; the snapshot allocated a slot for each.
    assert_eq!(plan.chunks.len(), 14);
    assert_eq!(snapshot.allocated_slots().len(), 14);
    assert_eq!(snapshot.table().total_issued(), 8_171_929);

    // The root's own slot is the watermark of its bucket at allocation
    // time: bucket 0x296d held 100 + (0x296d mod 50) = 105 stamps.
    assert_eq!(calculate_bucket(&plan.chunks[0].address, 16), 0x296d);
    assert_eq!(snapshot.allocated_slots()[0], 105);

    // Width 6 packs floor(32768 / 6) = 5461 buckets per leaf: twelve full
    // 4096-byte leaves and a final 3-byte leaf holding the last 4 buckets.
    let leaf_lens: Vec<usize> = plan.chunks[1..].iter().map(|c| c.payload.len()).collect();
    assert_eq!(leaf_lens[..12], [4096; 12]);
    assert_eq!(leaf_lens[12], 3);

    // Leaf 0 opens with deltas 0,1,2,3,... at 6 bits MSB-first; the last
    // leaf holds deltas 32,33,34,35 in exactly 24 bits.
    assert_eq!(
        hex::encode(&plan.chunks[1].payload[..8]),
        "0010831051872092"
    );
    assert_eq!(hex::encode(&plan.chunks[13].payload), "8218a3");

    // The exact root bytes; the embedded digests pin every leaf payload.
    assert_eq!(hex::encode(&plan.chunks[0].payload), LARGE_ROOT_PAYLOAD_HEX);

    // Round-trip through parse and digest-verified assembly.
    let root = RootInfo::parse(&hex::decode(LARGE_ROOT_PAYLOAD_HEX).unwrap()).unwrap();
    assert_eq!(root.leaf_count(), 13);
    let leaves: Vec<_> = plan.chunks[1..].iter().map(|c| &c.payload).collect();
    let recovered = root.assemble(&leaves).unwrap();
    assert_eq!(recovered, snapshot);
}

/// A byte-exact mutable vector: identical geometry and cursors to the first
/// (immutable) worked example, but constructed mutable. Only the flags byte
/// differs, so this pins both the flag (0x01) and the mutable round-trip.
const MUTABLE_ROOT_PAYLOAD_HEX: &str = "5342553142424242424242424242424242424242424242424242424242424242424242420c0801020000000000000001000000000000048e00000003000100000001000000c800000010000000041b1b1b1b1b1b1b1b1b1b2b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1bdb1b1b1b1b1b1b1b1b1b1b1b1b1b";

#[test]
fn mutable_vector_flags_byte_and_round_trip() {
    let batch_id = BatchId::new([0x42; 32]);
    let owner = Address::repeat_byte(0x11);
    let mut counts: Vec<u32> = (0..256u32).map(|b| 3 + (b & 3)).collect();
    counts[200] = 16;
    let table =
        UsageTableFor::from_counts(batch_id, 12, shallow(8), counts, Mutability::Mutable).unwrap();
    let mut snapshot = SnapshotFor::new(table);
    let plan = snapshot
        .revalidate(PublishedSequence::NONE)
        .unwrap()
        .plan_persist(&owner)
        .unwrap();

    // Same self-allocation as the immutable vector: bucket 41, slot 4.
    assert_eq!(calculate_bucket(&plan.chunks[0].address, 8), 41);
    assert_eq!(snapshot.allocated_slots(), &[4]);

    // Exactly one byte differs from the immutable vector: the flags byte.
    assert_eq!(plan.chunks.len(), 1);
    let bytes = plan.chunks[0].payload.clone();
    assert_eq!(bytes[38], 0x01, "mutable flag must be set");
    assert_eq!(hex::encode(&bytes), MUTABLE_ROOT_PAYLOAD_HEX);

    // It decodes back as mutable to the same snapshot.
    let root =
        RootInfoFor::<Shallow>::parse(&hex::decode(MUTABLE_ROOT_PAYLOAD_HEX).unwrap()).unwrap();
    assert!(root.is_mutable());
    let recovered = root.assemble::<&[u8]>(&[]).unwrap();
    assert!(recovered.table().is_mutable());
    assert_eq!(recovered, snapshot);
}
