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
//!
//! The timer lives inside `Arm::timed_build`: each arm pre-materialises its
//! entries, references and metadata, constructs a plain `MemoryStore`, and only
//! then starts the clock over the insert or put loop plus the build or commit.
//! Nothing in this module adds work to the timed region; it only repeats the
//! pass and reduces the samples.
//!
//! Wall-time is illustrative, never the currency. A capped or failing arm is
//! an absent cell plus a null-with-reason ([`cap_nulls`]), never a number.

use core::time::Duration;

use nectar_ldb::{V1, V1Read};

use crate::arm::{Arm, NullWithReason};
use crate::arm_ldb::LdbArm;
use crate::arm_mantaray::MantarayArm;
use crate::corpus::{Corpus, GenKey};
use crate::results::BuildWallCell;

/// The cold-pass caveat, verbatim on every wall-time cell.
pub const BUILD_CAVEAT: &str = "Cold-pass wall-time on one host; small sample; non-portable and \
illustrative; fetch counts remain the primary currency (whitepaper section 7 item 1).";

/// The 0.2 arm label, repeated here so the null names the same arm the cells
/// do.
const MANTARAY: &str = "mantaray-0.2";

/// Why the 0.2 arm has no wall-time cell above the cap (spec section 2).
#[must_use]
pub fn cap_reason(max_mantaray_scale: u64) -> String {
    format!(
        "mantaray 0.2 skipped by policy above {max_mantaray_scale}: the editor commit \
materialises the whole trie in RAM"
    )
}

/// The wall-time gaps for one `(corpus, scale)`: the 0.2 arm above its cap.
///
/// The cells and the nulls are separate returns because [`BuildWallCell`] holds
/// no null field; the caller extends
/// [`WallTimeSection::nulls`](crate::results::WallTimeSection::nulls) with this
/// and [`WallTimeSection::build_wall`](crate::results::WallTimeSection::build_wall)
/// with [`build_wall`]. An empty vector means every arm ran.
#[must_use]
pub fn cap_nulls(scale: u64, max_mantaray_scale: u64) -> Vec<NullWithReason> {
    if scale <= max_mantaray_scale {
        return Vec::new();
    }
    vec![NullWithReason {
        arm: MANTARAY.to_string(),
        field: "build_wall".to_string(),
        reason: cap_reason(max_mantaray_scale),
    }]
}

/// Build wall-time cells for one `(corpus, scale)`.
///
/// - `corpus`: the corpus enum, for the cell's `corpus` key.
/// - `scale`: the scale, for the cell's `scale` key.
/// - `keys`: the shared, sorted key set both arms consume in the same order.
/// - `max_mantaray_scale`: above this the 0.2 arm is skipped by policy.
/// - `build_samples`: timed passes per (arm, corpus, scale).
#[must_use]
pub fn build_wall(
    corpus: Corpus,
    scale: u64,
    keys: &[GenKey],
    max_mantaray_scale: u64,
    build_samples: u32,
) -> Vec<BuildWallCell> {
    if keys.is_empty() || build_samples == 0 {
        return Vec::new();
    }

    // Each arm owns its own counting store, exactly as the deterministic lanes
    // drive it; the timed region inside `timed_build` uses a fresh plain
    // `MemoryStore` instead, so no counter atomic is ever timed.
    let ldb_v1 = LdbArm::<V1>::new();
    let ldb_v1read = LdbArm::<V1Read>::new();
    let mantaray = MantarayArm::new();

    let mut arms: Vec<&dyn Arm> = vec![&ldb_v1, &ldb_v1read];
    if scale <= max_mantaray_scale {
        arms.push(&mantaray);
    }

    arms.into_iter()
        .filter_map(|arm| cell(arm, corpus, scale, keys, build_samples))
        .collect()
}

/// One arm's cell: one untimed warmup pass, then `samples` timed cold passes.
///
/// `None` when the arm cannot build the key set; a failure is an absent cell,
/// never a fabricated number.
fn cell(
    arm: &dyn Arm,
    corpus: Corpus,
    scale: u64,
    keys: &[GenKey],
    samples: u32,
) -> Option<BuildWallCell> {
    let mut timings: Vec<Duration> = Vec::with_capacity(samples as usize);
    // The warmup pass is discarded: it pays the allocator and page-fault cost
    // once so the timed passes measure the algorithm.
    for pass in 0..=samples {
        match arm.timed_build(keys) {
            Ok(elapsed) => {
                if pass > 0 {
                    timings.push(elapsed);
                }
            }
            Err(e) => {
                eprintln!(
                    "[manifest-perf] {} {} n={}: timed build failed: {e}",
                    arm.label(),
                    corpus.name(),
                    keys.len()
                );
                return None;
            }
        }
    }

    let total_ns: u128 = timings.iter().map(Duration::as_nanos).sum();
    let mean_ns = u64::try_from(total_ns / u128::from(samples)).unwrap_or(u64::MAX);
    let min_ns = timings
        .iter()
        .map(Duration::as_nanos)
        .min()
        .and_then(|ns| u64::try_from(ns).ok())
        .unwrap_or(mean_ns);
    // A zero mean would need a sub-nanosecond build of a non-empty key set;
    // report zero rather than an infinity if a coarse clock ever produces one.
    let keys_per_sec = if mean_ns == 0 {
        0.0
    } else {
        keys.len() as f64 * 1e9 / mean_ns as f64
    };

    Some(BuildWallCell {
        corpus: corpus.name().to_string(),
        scale,
        arm: arm.label().to_string(),
        samples,
        mean_ns,
        min_ns,
        keys_per_sec,
        caveat: BUILD_CAVEAT.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use nectar_ldb::V1;

    use super::{BUILD_CAVEAT, build_wall, cap_nulls, cap_reason};
    use crate::arm::Arm;
    use crate::arm_ldb::LdbArm;
    use crate::arm_mantaray::MantarayArm;
    use crate::corpus::{self, Corpus};

    const N: usize = 200;
    const SAMPLES: u32 = 2;

    /// Every acceptance gate the wall-time lane owns, on one small run: the
    /// caveat rides every cell, mean is at least min, the rate matches the
    /// mean, the sample count is honoured, and all three arms appear below the
    /// 0.2 cap.
    #[test]
    fn every_cell_carries_the_caveat_and_a_consistent_mean() {
        let keys = corpus::generate(Corpus::Kiwix, N);
        let cells = build_wall(Corpus::Kiwix, N as u64, &keys, N as u64, SAMPLES);

        let labels: Vec<&str> = cells.iter().map(|c| c.arm.as_str()).collect();
        assert_eq!(labels, ["ldb-v1", "ldb-v1read", "mantaray-0.2"]);
        assert!(
            cap_nulls(N as u64, N as u64).is_empty(),
            "no arm is capped at the cap itself"
        );

        for c in &cells {
            assert_eq!(c.caveat, BUILD_CAVEAT, "{}: caveat verbatim", c.arm);
            assert_eq!(c.corpus, "kiwix", "{}: corpus key", c.arm);
            assert_eq!(c.scale, N as u64, "{}: scale key", c.arm);
            assert_eq!(c.samples, SAMPLES, "{}: sample count", c.arm);
            assert!(
                c.mean_ns >= c.min_ns,
                "{}: mean {} below min {}",
                c.arm,
                c.mean_ns,
                c.min_ns
            );
            assert!(c.min_ns > 0, "{}: a build cannot take zero time", c.arm);
            let expect = N as f64 * 1e9 / c.mean_ns as f64;
            assert!(
                (c.keys_per_sec - expect).abs() < expect * 1e-9,
                "{}: keys_per_sec {} is not n * 1e9 / mean_ns {}",
                c.arm,
                c.keys_per_sec,
                expect
            );
        }
    }

    /// Above the cap the 0.2 arm has no cell at all and its gap is a
    /// null-with-reason: never a fabricated number.
    #[test]
    fn the_02_arm_above_the_cap_is_a_null_with_reason() {
        let keys = corpus::generate(Corpus::Kiwix, N);
        let cap = (N as u64) - 1;
        let cells = build_wall(Corpus::Kiwix, N as u64, &keys, cap, SAMPLES);

        let labels: Vec<&str> = cells.iter().map(|c| c.arm.as_str()).collect();
        assert_eq!(labels, ["ldb-v1", "ldb-v1read"]);

        let nulls = cap_nulls(N as u64, cap);
        assert_eq!(nulls.len(), 1, "one gap, for the 0.2 arm");
        assert_eq!(nulls[0].arm, "mantaray-0.2");
        assert_eq!(nulls[0].field, "build_wall");
        assert_eq!(nulls[0].reason, cap_reason(cap));
        assert!(
            nulls[0].reason.contains("whole trie in RAM"),
            "the reason states the policy: {}",
            nulls[0].reason
        );
    }

    /// The timed region is the algorithm only: the arm's counting store is
    /// untouched by a timed build (a plain `MemoryStore` carries it), and the
    /// reported duration is strictly inside the call, so corpus and entry
    /// materialisation cannot leak into it.
    #[test]
    fn the_counting_store_and_the_setup_stay_outside_the_timer() {
        let keys = corpus::generate(Corpus::Kiwix, N);

        let ldb = LdbArm::<V1>::new();
        let mantaray = MantarayArm::new();
        for arm in [&ldb as &dyn Arm, &mantaray] {
            let before = arm.counters();
            let outer = Instant::now();
            let timed = arm.timed_build(&keys).expect("timed build");
            let wall = outer.elapsed();
            let after = arm.counters();

            assert_eq!(
                (after.gets, after.puts, after.total_chunks),
                (before.gets, before.puts, before.total_chunks),
                "{}: the counting store sat in the timed path",
                arm.label()
            );
            assert!(
                timed < wall,
                "{}: the timed region {timed:?} is not inside the call {wall:?}",
                arm.label()
            );
            assert!(timed.as_nanos() > 0, "{}: nothing was timed", arm.label());
        }
    }

    /// Degenerate inputs produce no cells rather than a divide-by-zero or a
    /// fabricated rate.
    #[test]
    fn no_keys_or_no_samples_yields_no_cells() {
        let keys = corpus::generate(Corpus::Kiwix, N);
        assert!(build_wall(Corpus::Kiwix, 0, &[], 1_000, SAMPLES).is_empty());
        assert!(build_wall(Corpus::Kiwix, N as u64, &keys, 1_000, 0).is_empty());
    }
}
