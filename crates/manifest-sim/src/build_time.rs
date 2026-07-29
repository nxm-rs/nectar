//! UNIT E: the build wall-time lane (the one sanctioned non-deterministic
//! axis).
//!
//! Per (arm, corpus, scale): one untimed warmup pass, then `build_samples`
//! timed passes through `Arm::timed_build`, each over a fresh plain store, so
//! the counting store's atomics never sit in the timed path. Report
//! `mean_ns`, `min_ns`, `keys_per_sec = n * 1e9 / mean_ns`, and the verbatim
//! cold-pass caveat on every [`BuildWallCell`]. Above `max_mantaray_scale` the
//! 0.2 arm is skipped; its gap is recorded by the caller as a null-with-reason
//! in the wall-time section.

use crate::corpus::{Corpus, GenKey};
use crate::results::BuildWallCell;

/// The cold-pass caveat, verbatim on every wall-time cell.
pub const BUILD_CAVEAT: &str = "Cold-pass wall-time on one host; small sample; non-portable and \
illustrative; fetch counts remain the primary currency (whitepaper section 7 item 1).";

/// Build wall-time cells for one `(corpus, scale)`.
///
/// - `corpus`: the corpus enum, for the cell's `corpus` key.
/// - `scale`: the scale, for the cell's `scale` key.
/// - `keys`: the shared, sorted key set both arms consume in the same order.
/// - `max_mantaray_scale`: above this the 0.2 arm is skipped by policy.
/// - `build_samples`: timed passes per (arm, corpus, scale).
#[allow(unused_variables)]
pub fn build_wall(
    corpus: Corpus,
    scale: u64,
    keys: &[GenKey],
    max_mantaray_scale: u64,
    build_samples: u32,
) -> Vec<BuildWallCell> {
    // UNIT E: implement
    Vec::new()
}
