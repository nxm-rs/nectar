//! Per-cell work-count measurement: drive a real getter over the counting
//! store and read probes, wasted probes, certified gets and rounds, the
//! rounds off a paused virtual clock.

use core::num::NonZeroUsize;
use std::error::Error;
use std::future::Future;

use nectar_feeds::{Getter, Sequence};

use crate::corpus::Corpus;
use crate::store::ProbeStore;

type Err = Box<dyn Error>;

/// Window widths swept per feed length.
pub const WIDTHS: [usize; 4] = [1, 8, 16, 64];

/// Feed lengths: the present-then-absent boundary positions, a superset of
/// the reference client's benchmark prefills {1, 100, 1000, 5000}.
pub const LENGTHS: [u64; 9] = [1, 10, 100, 500, 1_000, 5_000, 10_000, 100_000, 1_000_000];

/// Replay-work budget for the stepwise finder. Measuring a cell costs about
/// `n^2 / (2 * width)` frontier lookups because the replay recomputes from
/// the floor each round; cells over budget are reported as gaps, never
/// estimated.
pub const LINEAR_BUDGET: u64 = 100_000_000;

/// The two latest-update finders under measurement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FinderKind {
    /// Exponential-ladder-then-binary probing (`latest`).
    Probing,
    /// Stepwise scan (`latest_linear_from`).
    Stepwise,
}

/// One measured `(finder, n, width)` cell.
#[derive(Debug, Clone, Copy)]
pub struct Cell {
    pub n: u64,
    pub width: usize,
    /// Concurrent probe batches until the boundary committed: the
    /// latency-critical figure, one network round trip each.
    pub rounds: u64,
    /// Presence probes issued, speculation included: the chunks-required
    /// figure.
    pub total_probes: u64,
    /// Probes answered absent, all at or past the first free slot; the
    /// excess over the width-one figure is the speculation cost.
    pub wasted_probes: u64,
    /// Certified retrievals: the committed boundary update.
    pub verified_gets: u64,
    /// Committed update index.
    pub committed: Option<u64>,
    /// Reported next free slot.
    pub next: Option<u64>,
}

/// Whether a stepwise cell's replay work fits [`LINEAR_BUDGET`].
#[must_use]
pub const fn linear_feasible(n: u64, width: usize) -> bool {
    let work = n.saturating_mul(n) / (2 * width as u64);
    work <= LINEAR_BUDGET
}

/// Drive a future to completion on a fresh current-thread runtime with a
/// paused virtual clock, so elapsed `tokio::time` reads back the probe
/// rounds. The sole sanctioned runtime-blocking call site in the harness: a
/// paused clock needs a real timer driver, which `nectar_testing::run` does
/// not provide.
#[allow(clippy::disallowed_methods)]
pub(crate) fn block_on_paused<T>(f: impl Future<Output = T>) -> Result<T, Err> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()?;
    Ok(rt.block_on(f))
}

/// Measure one cell by running the real finder to its verdict.
pub fn measure(
    corpus: &Corpus,
    kind: FinderKind,
    n: u64,
    width: NonZeroUsize,
) -> Result<Cell, Err> {
    block_on_paused(async {
        let store = ProbeStore::new(corpus, n).await?;
        let getter = Getter::new(corpus.feed(), &store).with_window(width);
        let t0 = tokio::time::Instant::now();
        let latest = match kind {
            FinderKind::Probing => getter.latest().await?,
            FinderKind::Stepwise => getter.latest_linear_from(Sequence::ZERO).await?,
        };
        let rounds = t0.elapsed().as_millis() as u64;
        let counts = store.counts();
        Ok::<_, Err>(Cell {
            n,
            width: width.get(),
            rounds,
            total_probes: counts.probes,
            wasted_probes: counts.absent,
            verified_gets: counts.gets,
            committed: latest.update.map(|update| update.index().get()),
            next: latest.next.map(|seq| seq.get()),
        })
    })?
}

#[cfg(test)]
mod tests {
    use super::*;

    fn width(w: usize) -> NonZeroUsize {
        NonZeroUsize::new(w).unwrap()
    }

    fn cell(corpus: &Corpus, kind: FinderKind, n: u64, w: usize) -> Cell {
        measure(corpus, kind, n, width(w)).unwrap()
    }

    /// Consultations of the sequential exponential-then-binary scan over a
    /// present-below-`n` oracle: the independent width-one probe oracle.
    fn sequential_probe_count(n: u64) -> u64 {
        let present = |index: u64| index < n;
        let mut probes = 1;
        if !present(0) {
            return probes;
        }
        let mut lo = 0u64;
        let mut off = 1u64;
        let mut hi = loop {
            probes += 1;
            if present(off) {
                lo = off;
                off *= 2;
            } else {
                break off;
            }
        };
        while hi - lo > 1 {
            probes += 1;
            let mid = lo + (hi - lo) / 2;
            if present(mid) {
                lo = mid;
            } else {
                hi = mid;
            }
        }
        probes
    }

    /// Rounds never exceed probes and every verdict is the boundary, at every
    /// width, for both finders.
    #[test]
    fn rounds_bounded_by_probes_and_verdicts_hold() {
        let corpus = Corpus::new(1_000);
        for kind in [FinderKind::Probing, FinderKind::Stepwise] {
            for n in [1u64, 2, 3, 10, 100, 1_000] {
                for w in WIDTHS {
                    let cell = cell(&corpus, kind, n, w);
                    assert!(cell.rounds <= cell.total_probes, "{kind:?} n={n} w={w}");
                    assert!(cell.rounds >= 1, "{kind:?} n={n} w={w}");
                    assert_eq!(cell.committed, Some(n - 1), "{kind:?} n={n} w={w}");
                    assert_eq!(cell.next, Some(n), "{kind:?} n={n} w={w}");
                    assert_eq!(cell.verified_gets, 1, "{kind:?} n={n} w={w}");
                }
            }
        }
    }

    /// Width one is the sequential scan: probing matches the
    /// exponential-then-binary consultation count, the stepwise scan issues
    /// exactly `n + 1` probes, and every round answers one probe.
    #[test]
    fn width_one_matches_the_sequential_scan() {
        let corpus = Corpus::new(1_000);
        for n in [1u64, 2, 3, 4, 5, 10, 100, 127, 128, 129, 1_000] {
            let probing = cell(&corpus, FinderKind::Probing, n, 1);
            assert_eq!(probing.total_probes, sequential_probe_count(n), "n={n}");
            assert_eq!(probing.rounds, probing.total_probes, "n={n}");

            let stepwise = cell(&corpus, FinderKind::Stepwise, n, 1);
            assert_eq!(stepwise.total_probes, n + 1, "n={n}");
            assert_eq!(stepwise.rounds, n + 1, "n={n}");
            assert_eq!(stepwise.wasted_probes, 1, "n={n}");
        }
    }

    /// At a million updates the probing finder converges in a logarithmic
    /// round band once the window covers the binary tree's upper levels:
    /// width one is the full sequential scan, width 16 and 64 land in
    /// single-digit rounds.
    #[test]
    fn probing_converges_logarithmically_at_scale() {
        const N: u64 = 1_000_000;
        let corpus = Corpus::new(N);

        let sequential = cell(&corpus, FinderKind::Probing, N, 1);
        assert_eq!(sequential.total_probes, sequential_probe_count(N));
        assert_eq!(sequential.rounds, sequential.total_probes);

        for (w, band) in [(16usize, 4..=9), (64usize, 3..=6)] {
            let cell = cell(&corpus, FinderKind::Probing, N, w);
            assert!(
                band.contains(&cell.rounds),
                "w={w} rounds={} outside {band:?}",
                cell.rounds
            );
            assert!(cell.rounds <= cell.total_probes);
            assert_eq!(cell.committed, Some(N - 1));
        }
    }
}
