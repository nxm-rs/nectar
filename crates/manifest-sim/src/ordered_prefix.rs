//! UNIT C: ordered-op (floor/ceiling/range) multipliers and prefix listing.
//!
//! Build the shared `keys` on all three arms, then probe 48 sampled keys (each
//! probed twice: the exact key and an absent neighbour) through `Arm::floor`,
//! `Arm::ceiling`, `Arm::range` and their `*_pessimal` twins for the
//! [`OrderedOpCell`] fair and pessimal columns and the 1.0 native absolutes.
//! For the [`PrefixListingCell`], drive `Arm::prefix_list` and
//! `Arm::prefix_list_pessimal` over the corpus prefix. The fair multiplier is
//! at most the pessimal multiplier by construction; every 0.2 field above
//! `max_mantaray_scale` is a null-with-reason.

use crate::corpus::{Corpus, GenKey};
use crate::results::{OrderedOpCell, PrefixListingCell};

/// Ordered-op and prefix-listing cells for one `(corpus, scale)`.
///
/// - `corpus`: the corpus enum, for the cell's `corpus` key.
/// - `scale`: the scale, for the cell's `scale` key.
/// - `keys`: the shared, sorted key set both arms consume in the same order.
/// - `max_mantaray_scale`: above this the 0.2 arm is skipped by policy and its
///   fields are null-with-reason.
#[allow(unused_variables)]
pub fn ordered_and_prefix(
    corpus: Corpus,
    scale: u64,
    keys: &[GenKey],
    max_mantaray_scale: u64,
) -> (Vec<OrderedOpCell>, Vec<PrefixListingCell>) {
    // UNIT C: implement
    (Vec::new(), Vec::new())
}
