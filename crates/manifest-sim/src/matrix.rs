//! The deterministic-section dispatcher: the capability matrix (built here,
//! now) plus a thin call into the four per-metric modules Units B-E own.
//!
//! The capability matrix is re-derived from the crates in tree, one row per
//! operation, and states the current 0.2 crate's abilities (a pruned,
//! ordered, resumable cursor), not the whitepaper's historical
//! classifications. The divergence is a rendered finding (red-team check 11).

use std::collections::BTreeMap;

use crate::arm::Capability;
use crate::arm_ldb::{CEILING_CLASS as LDB_CEILING_CLASS, CEILING_HOW as LDB_CEILING_HOW};
use crate::arm_mantaray::{
    BATCH_CLASS, BATCH_HOW, CEILING_CLASS, CEILING_HOW, FLOOR_CLASS, FLOOR_HOW, FULL_ITER_CLASS,
    FULL_ITER_HOW, INLINE_UNSUPPORTED, PREFIX_CLASS, PREFIX_HOW, RANGE_CLASS, RANGE_HOW,
    RECANON_UNSUPPORTED,
};
use crate::corpus::{Corpus, GenKey};
use crate::ordered_prefix::ordered_and_prefix;
use crate::results::{
    BuildProfileCell, CapabilityRow, GetHopsCell, OrderedOpCell, PrefixListingCell, StorageCell,
    WriteAmpCell,
};
use crate::storage_hops::storage_and_hops;
use crate::writeamp_build::writeamp_and_build;

/// The 1.0 arm label at the frozen `V1` parameters.
pub const LDB_V1: &str = "ldb-v1";
/// The 1.0 read-optimised arm label.
pub const LDB_V1READ: &str = "ldb-v1read";
/// The 0.2 arm label.
pub const MANTARAY: &str = "mantaray-0.2";

/// One capability-matrix row: the same capability for both 1.0 arms and the
/// 0.2 arm's own.
fn row(op: &str, ldb: Capability, mantaray: Capability) -> CapabilityRow {
    let mut per_arm = BTreeMap::new();
    per_arm.insert(LDB_V1.to_string(), ldb.clone());
    per_arm.insert(LDB_V1READ.to_string(), ldb);
    per_arm.insert(MANTARAY.to_string(), mantaray);
    CapabilityRow {
        op: op.to_string(),
        per_arm,
    }
}

/// The measured capability matrix, one row per operation, re-derived from the
/// in-tree crates (spec section 2, red-team check 11).
#[must_use]
pub fn capability_matrix() -> Vec<CapabilityRow> {
    vec![
        row("get", Capability::native(), Capability::native()),
        row(
            "floor",
            Capability::native(),
            Capability::emulated(FLOOR_HOW, FLOOR_CLASS),
        ),
        // Neither format has a dedicated ceiling primitive: 1.0 composes its
        // range cursor and 0.2 its after-bound seek, so both sides carry an
        // emulation label rather than the 1.0 side reading flat native.
        row(
            "ceiling",
            Capability::emulated(LDB_CEILING_HOW, LDB_CEILING_CLASS),
            Capability::emulated(CEILING_HOW, CEILING_CLASS),
        ),
        row(
            "range",
            Capability::native(),
            Capability::emulated(RANGE_HOW, RANGE_CLASS),
        ),
        row(
            "prefix_list",
            Capability::native(),
            Capability::emulated(PREFIX_HOW, PREFIX_CLASS),
        ),
        row(
            "full_iter",
            Capability::native(),
            Capability::emulated(FULL_ITER_HOW, FULL_ITER_CLASS),
        ),
        row(
            "batch_update",
            Capability::native(),
            Capability::emulated(BATCH_HOW, BATCH_CLASS),
        ),
        row(
            "inline_value",
            Capability::native(),
            Capability::unsupported(INLINE_UNSUPPORTED),
        ),
        row(
            "recanonicalise",
            Capability::native(),
            Capability::unsupported(RECANON_UNSUPPORTED),
        ),
    ]
}

/// The deterministic-section metric cells for one `(corpus, scale)`, gathered
/// from the four per-metric modules.
#[derive(Debug, Default)]
pub struct Deterministic {
    /// Whitepaper 2.1 storage cells.
    pub storage: Vec<StorageCell>,
    /// Whitepaper 2.2 get-hop cells.
    pub get_hops: Vec<GetHopsCell>,
    /// Whitepaper section 3 ordered-op cells.
    pub ordered_ops: Vec<OrderedOpCell>,
    /// Whitepaper 2.3 prefix-listing cells.
    pub prefix_listing: Vec<PrefixListingCell>,
    /// Whitepaper 6.2 write-amplification cells.
    pub write_amp: Vec<WriteAmpCell>,
    /// Whitepaper 6.1 build-profile cells.
    pub build_profile: Vec<BuildProfileCell>,
}

/// Drive the four deterministic metric modules over one `(corpus, scale)`.
#[must_use]
pub fn deterministic_metrics(
    corpus: Corpus,
    scale: u64,
    keys: &[GenKey],
    max_mantaray_scale: u64,
) -> Deterministic {
    let (storage, get_hops) = storage_and_hops(corpus, scale, keys, max_mantaray_scale);
    let (ordered_ops, prefix_listing) = ordered_and_prefix(corpus, scale, keys, max_mantaray_scale);
    let (write_amp, build_profile) = writeamp_and_build(corpus, scale, keys, max_mantaray_scale);
    Deterministic {
        storage,
        get_hops,
        ordered_ops,
        prefix_listing,
        write_amp,
        build_profile,
    }
}
