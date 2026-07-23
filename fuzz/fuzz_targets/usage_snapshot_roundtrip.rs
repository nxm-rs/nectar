//! Structured round-trip fuzz of the SBU1 usage-snapshot codec.
//!
//! The `Arbitrary` impl for `Snapshot` (crates/postage-usage/src/snapshot.rs)
//! generates valid snapshots routed through the recovery-path validation, so
//! the oracle is stronger than "no panic": the shared
//! `nectar_postage_usage::oracles::snapshot_persist_round_trip` oracle
//! requires the full public persist pipeline (`revalidate` -> `plan_persist`)
//! to encode the snapshot into root+leaf payloads that `RootInfo::parse` +
//! `assemble` recover to an identical snapshot. A persist may legitimately
//! refuse to allocate a snapshot slot (a full immutable bucket, an exhausted
//! capacity-1 ring); the oracle reports that skip as `Ok(false)`, and every
//! other failure is a codec bug.
//!
//! The generator runs at `Shallow`, a spec whose collision-bucket floor is the
//! format minimum, so the inputs span the format's whole bucket-depth range
//! rather than the single geometry mainnet's floor of 16 admits.
//!
//! The same oracle is pinned on stable by the
//! `snapshot_persist_parse_assemble_round_trip` proptest in
//! `crates/postage-usage/src/codec.rs`.

#![no_main]

use core::num::NonZeroU8;

use libfuzzer_sys::fuzz_target;
use nectar_postage_usage::{NetworkId, SnapshotFor, SwarmSpec, oracles};

/// A deployment whose bucket-depth floor is the format minimum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Shallow;

impl SwarmSpec for Shallow {
    const NETWORK_ID: NetworkId = NetworkId::TESTNET;
    const MIN_BUCKET_DEPTH: NonZeroU8 = NonZeroU8::new(1).unwrap();
}

fuzz_target!(|snapshot: SnapshotFor<Shallow>| {
    let _ = oracles::snapshot_persist_round_trip(snapshot)
        .expect("persisted snapshots must parse and assemble back");
});
