//! Structured round-trip fuzz of the SBU1 usage-snapshot codec.
//!
//! The `Arbitrary` impl for `Snapshot` (crates/postage-usage/src/snapshot.rs)
//! generates valid snapshots routed through the recovery-path validation, so
//! the oracle is stronger than "no panic": the full public persist pipeline
//! (`revalidate` -> `plan_persist`) must encode the snapshot into root+leaf
//! payloads that `RootInfo::parse` + `assemble` recover to an identical
//! snapshot. A persist may legitimately refuse to allocate a snapshot slot (a
//! full immutable bucket, an exhausted capacity-1 ring), so a `plan_persist`
//! error skips the input; every other failure is a codec bug.
//!
//! The same property is pinned on stable by
//! `arbitrary_snapshot_persist_parse_assemble_round_trip` in
//! `crates/postage-usage/src/codec.rs`.

#![no_main]

use alloy_primitives::Address;
use libfuzzer_sys::fuzz_target;
use nectar_postage_usage::{PublishedSequence, RootInfo, Snapshot};

fuzz_target!(|snapshot: Snapshot| {
    let mut snapshot = snapshot;
    let owner = Address::repeat_byte(0x11);

    // Arbitrary snapshots leave sequence headroom, so revalidation against
    // the NONE floor must succeed.
    let plan = match snapshot
        .revalidate(PublishedSequence::NONE)
        .expect("arbitrary snapshots must revalidate against the NONE floor")
        .plan_persist(&owner)
    {
        Ok(plan) => plan,
        // A full bucket can legitimately refuse a snapshot slot.
        Err(_) => return,
    };

    let root = RootInfo::parse(&plan.chunks[0].payload).expect("planned root must parse");
    let leaves: Vec<&[u8]> = plan.chunks[1..]
        .iter()
        .map(|c| c.payload.as_ref())
        .collect();
    let recovered = root
        .assemble(&leaves)
        .expect("planned leaves must assemble");
    assert_eq!(
        recovered, snapshot,
        "parse+assemble must recover the persisted snapshot"
    );
});
