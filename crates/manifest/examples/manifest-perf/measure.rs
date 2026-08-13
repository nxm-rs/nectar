//! Per-cell measurement: drive one arm over one corpus and read every figure
//! off its store counters.

use std::time::Duration;

use nectar_manifest::ManifestPath;

use crate::arm::{Arm, Err, Op, OpCost};
use crate::corpus::Corpus;
use crate::results::{BuildTimeCell, OpCell, StorageCell};

/// Point lookups and updates sampled per cell.
pub const OP_SAMPLES: usize = 32;

/// Seeks sampled per cell: one trie seek walks the whole manifest.
pub const SEEK_SAMPLES: usize = 8;

/// Evenly spaced sample of at most `count` indices in `0..n`.
fn sample(n: usize, count: usize) -> Vec<usize> {
    if n == 0 {
        return Vec::new();
    }
    if n <= count {
        return (0..n).collect();
    }
    let stride = n / count;
    (0..count).map(|slot| (slot * stride).min(n - 1)).collect()
}

/// The centred 10% window of the sorted key set.
fn window(keys: &[ManifestPath]) -> Option<(ManifestPath, ManifestPath)> {
    let n = keys.len();
    let lo = keys.get(n * 45 / 100)?;
    let hi = keys.get((n * 55 / 100).max(n * 45 / 100 + 1))?;
    Some((lo.clone(), hi.clone()))
}

fn fold(costs: &[OpCost]) -> (f64, u64, f64, u64) {
    let len = costs.len().max(1) as f64;
    let fetches: u64 = costs.iter().map(|cost| cost.fetches).sum();
    let puts: u64 = costs.iter().map(|cost| cost.puts).sum();
    let keys: u64 = costs.iter().map(|cost| cost.keys_returned).sum();
    let max = costs.iter().map(|cost| cost.fetches).max().unwrap_or(0);
    (fetches as f64 / len, max, puts as f64 / len, keys)
}

fn cell(corpus: Corpus, scale: u64, arm: &dyn Arm, op: Op, costs: &[OpCost]) -> OpCell {
    let (fetches_mean, fetches_max, puts_mean, keys_returned) = fold(costs);
    OpCell {
        corpus: corpus.name().to_string(),
        scale,
        arm: arm.label().to_string(),
        op: op.name().to_string(),
        capability: arm.capability(op),
        samples: costs.len() as u64,
        fetches_mean,
        fetches_max,
        puts_mean,
        keys_returned,
    }
}

/// Build one arm over one corpus and measure every verb against that root.
pub fn measure(
    corpus: Corpus,
    scale: u64,
    keys: &[ManifestPath],
    arm: &mut dyn Arm,
) -> Result<(StorageCell, Vec<OpCell>), Err> {
    let storage = arm.build(keys)?;
    if arm.keys()? != keys {
        return Err(format!("{}: built root does not hold the corpus", arm.label()).into());
    }
    let storage = StorageCell {
        corpus: corpus.name().to_string(),
        scale,
        arm: arm.label().to_string(),
        chunks: storage.chunks,
        puts: storage.puts,
        distinct_puts: storage.distinct_puts,
        put_bytes: storage.put_bytes,
        live_bytes: storage.live_bytes,
    };

    let mut points = Vec::new();
    for index in sample(keys.len(), OP_SAMPLES) {
        points.push(arm.get(&keys[index])?);
    }
    let mut seeks = Vec::new();
    let mut updates = Vec::new();
    for index in sample(keys.len(), SEEK_SAMPLES) {
        seeks.push(arm.floor(&keys[index])?);
        updates.push(arm.update(&keys[index])?);
    }
    let listing = vec![arm.dir(&ManifestPath::default())?];
    let scan = match window(keys) {
        Some((lo, hi)) => vec![arm.range(&lo, &hi)?],
        None => Vec::new(),
    };

    let ops = vec![
        cell(corpus, scale, arm, Op::Get, &points),
        cell(corpus, scale, arm, Op::Floor, &seeks),
        cell(corpus, scale, arm, Op::Dir, &listing),
        cell(corpus, scale, arm, Op::Range, &scan),
        cell(corpus, scale, arm, Op::Update, &updates),
    ];
    Ok((storage, ops))
}

/// One untimed warm-up pass, then `samples` timed cold builds.
pub fn build_time(
    corpus: Corpus,
    scale: u64,
    keys: &[ManifestPath],
    arm: &dyn Arm,
    samples: usize,
) -> Result<BuildTimeCell, Err> {
    arm.timed_build(keys)?;
    let mut timings = Vec::with_capacity(samples);
    for _ in 0..samples.max(1) {
        timings.push(arm.timed_build(keys)?);
    }
    let total: Duration = timings.iter().sum();
    let mean = total
        .checked_div(u32::try_from(timings.len()).unwrap_or(1))
        .unwrap_or_default();
    let min = timings.iter().copied().min().unwrap_or_default();
    let mean_ns = u64::try_from(mean.as_nanos()).unwrap_or(u64::MAX);
    Ok(BuildTimeCell {
        corpus: corpus.name().to_string(),
        scale,
        arm: arm.label().to_string(),
        samples: timings.len() as u64,
        mean_ns,
        min_ns: u64::try_from(min.as_nanos()).unwrap_or(u64::MAX),
        keys_per_second: if mean_ns == 0 {
            0.0
        } else {
            keys.len() as f64 * 1e9 / mean_ns as f64
        },
    })
}

#[cfg(test)]
mod tests {
    use super::{measure, sample, window};
    use crate::arm::{ldb_arm, mantaray_arm};
    use crate::corpus::{Corpus, generate};

    #[test]
    fn the_window_is_a_non_empty_slice_of_the_middle() {
        let keys = generate(Corpus::Site, 100);
        let (lo, hi) = window(&keys).unwrap();
        assert!(lo.as_bytes() < hi.as_bytes());
        assert_eq!(sample(100, 8).len(), 8);
        assert_eq!(sample(3, 8), vec![0, 1, 2]);
    }

    #[test]
    fn the_deterministic_lane_repeats_byte_for_byte() {
        let pass = || {
            let keys = generate(Corpus::Site, 200);
            let mut out = Vec::new();
            for arm in [
                &mut mantaray_arm() as &mut dyn crate::arm::Arm,
                &mut ldb_arm(),
            ] {
                out.push(measure(Corpus::Site, 200, &keys, arm).unwrap());
            }
            serde_json::to_string(&out).unwrap()
        };
        assert_eq!(pass(), pass());
    }
}
