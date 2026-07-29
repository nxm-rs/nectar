//! UNIT D: the write-amplification K sweep and the build profile.
//!
//! Build the shared `keys` once per arm, read the [`BuildReport`] and the
//! storage counters for the [`BuildProfileCell`] (frontier, nodes written and
//! embedded, `peak_live_store_bytes` from the counting store, total put
//! bytes). Then, for each K in the sweep, drive `Arm::batch_update` in
//! [`BatchMode::Batched`] and [`BatchMode::PerEdit`] against the unchanged root
//! for the [`WriteAmpCell`] (chunks written / K). Above `max_mantaray_scale`
//! every 0.2 field is a null-with-reason.
//!
//! [`BuildReport`]: crate::arm::BuildReport
//! [`BatchMode::Batched`]: crate::arm::BatchMode
//! [`BatchMode::PerEdit`]: crate::arm::BatchMode

use crate::corpus::{Corpus, GenKey};
use crate::results::{BuildProfileCell, WriteAmpCell};

/// Write-amplification and build-profile cells for one `(corpus, scale)`.
///
/// - `corpus`: the corpus enum, for the cell's `corpus` key.
/// - `scale`: the scale, for the cell's `scale` key.
/// - `keys`: the shared, sorted key set both arms consume in the same order.
/// - `max_mantaray_scale`: above this the 0.2 arm is skipped by policy and its
///   fields are null-with-reason.
///
/// The K sweep is `crate::perf::WRITE_AMP_KS`; edits come from
/// `crate::perf::sample_indices(n, k)` so K rows above n are skipped.
#[allow(unused_variables)]
pub fn writeamp_and_build(
    corpus: Corpus,
    scale: u64,
    keys: &[GenKey],
    max_mantaray_scale: u64,
) -> (Vec<WriteAmpCell>, Vec<BuildProfileCell>) {
    // UNIT D: implement
    (Vec::new(), Vec::new())
}
