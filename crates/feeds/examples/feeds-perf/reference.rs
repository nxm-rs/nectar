//! Port of the reference client's concurrent finder, measured over the same
//! counting presence store: a fixed-width bounded exponential lookahead that
//! probes each interval at offsets `2^k - 1` for `k = 1..=LEVELS`.
//!
//! Results fold present-first in ascending level, one valid arrival order of
//! the original (presence answers beat its absence timeout). In the original
//! every probe is a full retrieval; presence probes stand in here, so a cell
//! carries no certified get.

use std::error::Error;

use futures_util::future::join_all;
use nectar_feeds::{Feed, Sequence};
use nectar_primitives::store::ChunkHas;

use crate::corpus::Corpus;
use crate::measure::{Cell, block_on_paused};
use crate::store::ProbeStore;

type Err = Box<dyn Error>;

/// Fixed lookahead concurrency: each batch spans `2^LEVELS` slots.
pub const LEVELS: usize = 8;

/// Latest found probe: slot index and its level within the interval.
#[derive(Debug, Clone, Copy)]
struct Found {
    index: u64,
    level: usize,
}

/// One batch interval `(base, base + 2^level)` with found/not-found level
/// tracking; `base` is guaranteed to hold an update.
#[derive(Debug, Clone, Copy)]
struct Interval {
    base: u64,
    level: usize,
    not_found: usize,
    found: Found,
}

impl Interval {
    const fn new(base: u64) -> Self {
        Self {
            base,
            level: LEVELS,
            not_found: LEVELS,
            found: Found {
                index: base,
                level: 0,
            },
        }
    }

    /// Narrow: base advances to the latest found index, the level ceiling
    /// inherits the found level.
    const fn next(&self) -> Self {
        Self {
            base: self.found.index,
            level: self.found.level,
            not_found: self.found.level,
            found: Found {
                index: self.found.index,
                level: 0,
            },
        }
    }

    /// Inconsistent-feed retry: narrow, but reset the level ceiling.
    const fn retry(&self) -> Self {
        let mut r = self.next();
        r.level = self.level;
        r.not_found = self.level;
        r
    }
}

/// One probe answer.
#[derive(Debug, Clone, Copy)]
struct Probe {
    index: u64,
    level: usize,
    present: bool,
}

/// What one folded batch decided.
#[derive(Debug, Clone, Copy)]
enum Verdict {
    Committed(u64),
    Narrow,
    Retry { min: usize },
}

/// Probe the interval concurrently at levels `min + 1..=interval.level`, one
/// virtual tick for the whole batch.
async fn batch(store: &ProbeStore<'_>, feed: &Feed, interval: &Interval, min: usize) -> Vec<Probe> {
    let indexed: Vec<(usize, u64)> = (min + 1..=interval.level)
        .map(|level| (level, interval.base + (1 << level) - 1))
        .collect();
    let addresses: Vec<_> = indexed
        .iter()
        .map(|(_, index)| feed.update_address(&Sequence::new(*index)))
        .collect();
    let answers = join_all(addresses.iter().map(|address| store.has(address))).await;
    indexed
        .into_iter()
        .zip(answers)
        .map(|((level, index), present)| Probe {
            index,
            level,
            present,
        })
        .collect()
}

/// Fold a batch present-first in ascending level; stale answers past a
/// decision are skipped by the level guards, so folding stops there.
fn collect(interval: &mut Interval, mut probes: Vec<Probe>) -> Result<Verdict, Err> {
    probes.sort_by_key(|p| (!p.present, p.level));
    for p in probes {
        if !p.present {
            if interval.not_found < p.level {
                continue;
            }
            interval.not_found = p.level - 1;
        } else {
            if interval.found.level > p.level {
                continue;
            }
            if interval.level == p.level && p.level < LEVELS {
                return Ok(Verdict::Committed(p.index));
            }
            interval.found = Found {
                index: p.index,
                level: p.level,
            };
        }
        if interval.found.level == interval.not_found {
            if interval.found.level == 0 {
                return Ok(Verdict::Committed(interval.found.index));
            }
            return Ok(Verdict::Narrow);
        }
        if interval.not_found < interval.found.level {
            return Ok(Verdict::Retry {
                min: interval.found.level,
            });
        }
    }
    Err("batch folded without a verdict".into())
}

/// Run the finder to its verdict: the committed index and next free slot.
async fn run(store: &ProbeStore<'_>, feed: &Feed) -> Result<(Option<u64>, Option<u64>), Err> {
    if !store.has(&feed.update_address(&Sequence::ZERO)).await {
        return Ok((None, Some(0)));
    }
    let mut interval = Interval::new(0);
    let mut min = 0;
    loop {
        let probes = batch(store, feed, &interval, min).await;
        match collect(&mut interval, probes)? {
            Verdict::Committed(index) => return Ok((Some(index), Some(index + 1))),
            Verdict::Narrow => {
                interval = interval.next();
                min = 0;
            }
            Verdict::Retry { min: m } => {
                interval = interval.retry();
                min = m;
            }
        }
    }
}

/// Measure one reference-finder cell; `width` reports the fixed concurrency.
pub fn measure(corpus: &Corpus, n: u64) -> Result<Cell, Err> {
    block_on_paused(async {
        let store = ProbeStore::new(corpus, n).await?;
        let feed = corpus.feed();
        let t0 = tokio::time::Instant::now();
        let (committed, next) = run(&store, &feed).await?;
        let rounds = t0.elapsed().as_millis() as u64;
        let counts = store.counts();
        Ok::<_, Err>(Cell {
            n,
            width: LEVELS,
            rounds,
            total_probes: counts.has_calls,
            wasted_probes: counts.absent,
            verified_gets: counts.gets,
            committed,
            next,
        })
    })?
}

#[cfg(test)]
mod tests {
    use super::*;

    /// All-found climb batches before the boundary block: the linear term.
    const fn climbs(n: u64) -> u64 {
        (n - 1) / ((1 << LEVELS) - 1)
    }

    /// Every verdict is the boundary, rounds are the initial probe plus the
    /// climb plus at most one bracket and `LEVELS` narrowing batches, and a
    /// batch never exceeds `LEVELS` probes: logarithmic within one
    /// `2^LEVELS` block, linear in `n / (2^LEVELS - 1)` past it.
    #[test]
    fn linear_climb_with_bounded_narrowing() {
        let corpus = Corpus::new(5_000);
        for n in [1u64, 2, 3, 10, 100, 255, 256, 257, 510, 1_000, 5_000] {
            let cell = measure(&corpus, n).unwrap();
            assert_eq!(cell.committed, Some(n - 1), "n={n}");
            assert_eq!(cell.next, Some(n), "n={n}");
            assert_eq!(cell.verified_gets, 0, "n={n}");
            let band = climbs(n) + 2..=climbs(n) + 2 + LEVELS as u64;
            assert!(
                band.contains(&cell.rounds),
                "n={n} rounds={} outside {band:?}",
                cell.rounds
            );
            assert!(cell.rounds <= cell.total_probes, "n={n}");
            assert!(cell.total_probes <= LEVELS as u64 * cell.rounds, "n={n}");
        }
    }

    /// At a million updates the climb dominates: rounds sit just past
    /// `n / 255`, three orders beyond the ladder's single digits.
    #[test]
    fn climb_is_linear_at_scale() {
        const N: u64 = 1_000_000;
        let corpus = Corpus::new(N);
        let cell = measure(&corpus, N).unwrap();
        assert_eq!(cell.committed, Some(N - 1));
        let band = climbs(N) + 2..=climbs(N) + 2 + LEVELS as u64;
        assert!(
            band.contains(&cell.rounds),
            "rounds={} outside {band:?}",
            cell.rounds
        );
    }
}
