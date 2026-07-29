//! UNIT B: storage, slot utilisation, embed fraction, and the get-hop
//! distribution with the RTT extension.
//!
//! Build the shared `keys` on all three arms over their own counting stores,
//! read the storage counters for the [`StorageCell`], then probe
//! `min(n, 4096)` evenly spaced keys through `Arm::get` for the [`GetHopsCell`]
//! histogram and its `hops * rtt` latency columns. Above `max_mantaray_scale`
//! every 0.2 field is a null-with-reason; the 0.2 column is absent from the
//! per-arm maps and named in `nulls`.

use crate::corpus::{Corpus, GenKey};
use crate::results::{GetHopsCell, StorageCell};

/// Storage and get-hop cells for one `(corpus, scale)`.
///
/// - `corpus`: the corpus enum, for the cell's `corpus` key.
/// - `scale`: the scale, for the cell's `scale` key.
/// - `keys`: the shared, sorted key set both arms consume in the same order.
/// - `max_mantaray_scale`: above this the 0.2 arm is skipped by policy and its
///   fields are null-with-reason.
#[allow(unused_variables)]
pub fn storage_and_hops(
    corpus: Corpus,
    scale: u64,
    keys: &[GenKey],
    max_mantaray_scale: u64,
) -> (Vec<StorageCell>, Vec<GetHopsCell>) {
    // UNIT B: implement
    (Vec::new(), Vec::new())
}
