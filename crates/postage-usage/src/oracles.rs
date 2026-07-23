//! Shared fuzz and test oracles for the SBU1 snapshot codec.
//!
//! One oracle per invariant: the fuzz target and the stable pins call the
//! same body, so the rungs cannot drift. Oracles return `Err` instead of
//! panicking; call sites assert.

use alloc::vec::Vec;

use alloy_primitives::Address;
use nectar_primitives::SwarmSpec;
use nectar_primitives::oracles::Violation;

use crate::{PublishedSequence, RootInfoFor, SnapshotFor};

/// One full persist round trip: revalidate at the `NONE` floor, plan a
/// persist for a fixed owner, parse the planned root, assemble the planned
/// leaves, and compare against the persisted snapshot. A plan may
/// legitimately refuse a snapshot slot (a full immutable bucket, an
/// exhausted capacity-1 ring); that skip is `Ok(false)`.
pub fn snapshot_persist_round_trip<S: SwarmSpec>(
    mut snapshot: SnapshotFor<S>,
) -> Result<bool, Violation> {
    let owner = Address::repeat_byte(0x11);

    let Ok(mut validated) = snapshot.revalidate(PublishedSequence::NONE) else {
        return Err(Violation::new(
            "snapshots with sequence headroom must revalidate against the NONE floor",
        ));
    };
    let Ok(plan) = validated.plan_persist(&owner) else {
        return Ok(false);
    };
    let Some((root_chunk, leaf_chunks)) = plan.chunks.split_first() else {
        return Err(Violation::new("a plan carries at least the root chunk"));
    };
    let Ok(root) = RootInfoFor::<S>::parse(&root_chunk.payload) else {
        return Err(Violation::new("planned root must parse"));
    };
    let leaves: Vec<&[u8]> = leaf_chunks.iter().map(|c| c.payload.as_ref()).collect();
    let Ok(recovered) = root.assemble(&leaves) else {
        return Err(Violation::new("planned leaves must assemble"));
    };
    if recovered != snapshot {
        return Err(Violation::new(
            "parse+assemble must recover the persisted snapshot",
        ));
    }
    Ok(true)
}
