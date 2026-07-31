//! UNIT B: storage, slot utilisation, embed fraction, and the get-hop
//! distribution with the RTT extension.
//!
//! Build the shared `keys` on all three arms over their own counting stores,
//! read the storage counters for the [`StorageCell`], then probe
//! `min(n, 4096)` evenly spaced keys through `Arm::get` for the [`GetHopsCell`]
//! histogram and its `hops * rtt` latency columns. Above `max_mantaray_scale`
//! every 0.2 field is a null-with-reason; the 0.2 column is absent from the
//! per-arm maps and named in `nulls`.
//!
//! Two apples-for-apples rules bind this module. Utilisation divides by the
//! same `DEFAULT_BODY_SIZE` on every arm, so the arms differ only in what they
//! pack into a chunk. Every hop figure is a counter delta that brackets exactly
//! one `Arm::get` call: the probe loop checks the per-probe deltas against the
//! whole-loop delta and fails the measurement rather than report a hop count it
//! cannot account for.

use std::collections::BTreeMap;

use nectar_ldb::{V1, V1Read};
use nectar_primitives::DEFAULT_BODY_SIZE;

use crate::arm::{Arm, Err, NullWithReason, build_checked};
use crate::arm_ldb::LdbArm;
use crate::arm_mantaray::MantarayArm;
use crate::corpus::{Corpus, GenKey};
use crate::matrix::{LDB_V1, LDB_V1READ, MANTARAY};
use crate::perf::sample_indices;
use crate::results::{GetHopsCell, HopStats, StorageCell};

/// The get-hop probe cap: the sample is `min(n, 4096)` evenly spaced keys.
const HOP_PROBES: usize = 4096;

/// RTT values (ms) for the illustrative hops-to-latency columns. The 100 ms
/// column is the whitepaper's mobile-RTT column, so this set is one wider than
/// the parallel-cursor set in `perf`.
pub const HOP_RTT_SET: [u32; 4] = [25, 50, 75, 100];

/// The latency model string that rides every hop block.
const HOP_MODEL: &str = "latency_ms = mean hops * rtt; sequential fetches, no pipelining and no \
caching; hop counts are the measured currency and the millisecond columns are illustrative.";

/// Why the 0.2 arm never reports an embed fraction.
const NO_EMBED: &str = "mantaray 0.2 has no node embedding: every entry is a 32/64-byte reference";

/// Why the 0.2 arm is absent above its scale cap.
fn cap_reason(max_mantaray_scale: u64) -> String {
    format!(
        "mantaray 0.2 skipped by policy above {max_mantaray_scale}: the editor commit materialises \
the whole trie in RAM"
    )
}

/// A recorded gap for one arm and field.
fn gap(arm: &str, field: &str, reason: &str) -> NullWithReason {
    NullWithReason {
        arm: arm.to_string(),
        field: field.to_string(),
        reason: reason.to_string(),
    }
}

/// `live_bytes / (total_chunks * chunk_body_size)`: the fraction of the chunk
/// slots the arm bought that carry payload. Both arms divide by the same
/// `DEFAULT_BODY_SIZE`.
fn utilisation(live_bytes: u64, total_chunks: u64) -> f64 {
    let capacity = total_chunks as f64 * DEFAULT_BODY_SIZE as f64;
    if capacity > 0.0 {
        live_bytes as f64 / capacity
    } else {
        0.0
    }
}

/// `a / b`, or `None` when either side is missing or the divisor is zero.
fn ratio(a: Option<f64>, b: Option<f64>) -> Option<f64> {
    match (a, b) {
        (Some(x), Some(y)) if y > 0.0 => Some(x / y),
        _ => None,
    }
}

/// One arm's measured storage and hop figures.
struct ArmMeasure {
    /// Distinct resident chunks after build.
    total_chunks: u64,
    /// `live_bytes / (total_chunks * DEFAULT_BODY_SIZE)`.
    slot_utilisation: f64,
    /// `nodes_embedded / (nodes_embedded + nodes_written)`; `None` on 0.2.
    embed_fraction: Option<f64>,
    /// The get-hop distribution over the probe set.
    stats: HopStats,
}

/// Build `keys` into the arm's own store, read the storage counters, then walk
/// the probe set through `Arm::get` and accumulate the hop distribution.
///
/// The probe loop is the integrity site: `fetches` on each outcome is the
/// counter delta the arm took around one `get`, and the sum of those deltas
/// must equal the whole-loop delta, so no fetch is double-counted and none is
/// lost between probes.
fn measure(arm: &mut dyn Arm, keys: &[GenKey], probes: &[usize]) -> Result<ArmMeasure, Err> {
    let report = build_checked(arm, keys)?;
    let counters = arm.counters();
    let total_chunks = counters.total_chunks;
    let embed_fraction = report.nodes_embedded.map(|embedded| {
        let total = embedded.saturating_add(report.nodes_written);
        if total == 0 {
            0.0
        } else {
            embedded as f64 / total as f64
        }
    });

    let before = arm.counters().gets;
    let mut histogram: BTreeMap<u64, u64> = BTreeMap::new();
    let mut sum = 0u64;
    let mut max = 0u64;
    for &i in probes {
        let key = keys
            .get(i)
            .ok_or_else(|| -> Err { "probe index past the key set".into() })?;
        let outcome = arm.get(&key.raw)?;
        let cost = outcome
            .cost
            .ok_or_else(|| -> Err { "a native get must carry a cost".into() })?;
        if cost.keys_returned != 1 {
            return Err(format!("{}: a corpus key read back absent", arm.label()).into());
        }
        let hops = cost.fetches;
        sum = sum.saturating_add(hops);
        max = max.max(hops);
        *histogram.entry(hops).or_insert(0) += 1;
    }
    let observed = arm.counters().gets.saturating_sub(before);
    if observed != sum {
        return Err(format!(
            "{}: hop deltas sum to {sum} but the store served {observed} fetches",
            arm.label()
        )
        .into());
    }

    let mean = if probes.is_empty() {
        0.0
    } else {
        sum as f64 / probes.len() as f64
    };
    let latency_ms_by_rtt = HOP_RTT_SET
        .iter()
        .map(|&rtt| (rtt.to_string(), mean * f64::from(rtt)))
        .collect();

    Ok(ArmMeasure {
        total_chunks,
        slot_utilisation: utilisation(counters.live_bytes, total_chunks),
        embed_fraction,
        stats: HopStats {
            mean,
            max,
            histogram,
            latency_ms_by_rtt,
            model: HOP_MODEL.to_string(),
        },
    })
}

/// Land one arm's measurement in both cells; an arm without an embed fraction
/// records the gap rather than a zero.
fn record(label: &str, m: ArmMeasure, storage: &mut StorageCell, hops: &mut GetHopsCell) {
    storage
        .total_chunks
        .insert(label.to_string(), m.total_chunks);
    storage
        .slot_utilisation
        .insert(label.to_string(), m.slot_utilisation);
    match m.embed_fraction {
        Some(f) => {
            storage.embed_fraction.insert(label.to_string(), f);
        }
        None => storage.nulls.push(gap(label, "embed_fraction", NO_EMBED)),
    }
    hops.per_arm.insert(label.to_string(), m.stats);
}

/// Record an absent arm across every field of both cells.
fn record_gap(label: &str, reason: &str, storage: &mut StorageCell, hops: &mut GetHopsCell) {
    for field in ["total_chunks", "slot_utilisation", "embed_fraction"] {
        storage.nulls.push(gap(label, field, reason));
    }
    hops.nulls.push(gap(label, "per_arm", reason));
}

/// Measure one arm and land it, or record the gap the failure leaves.
///
/// Returns the reason when the arm produced no numbers.
fn drive(
    label: &str,
    arm: &mut dyn Arm,
    keys: &[GenKey],
    probes: &[usize],
    storage: &mut StorageCell,
    hops: &mut GetHopsCell,
) -> Option<String> {
    match measure(arm, keys, probes) {
        Ok(m) => {
            record(label, m, storage, hops);
            None
        }
        Err(e) => {
            let reason = format!("{label} measurement failed: {e}");
            record_gap(label, &reason, storage, hops);
            Some(reason)
        }
    }
}

/// Storage and get-hop cells for one `(corpus, scale)`.
///
/// - `corpus`: the corpus enum, for the cell's `corpus` key.
/// - `scale`: the scale, for the cell's `scale` key.
/// - `keys`: the shared, sorted key set both arms consume in the same order.
/// - `max_mantaray_scale`: above this the 0.2 arm is skipped by policy and its
///   fields are null-with-reason.
#[must_use]
pub fn storage_and_hops(
    corpus: Corpus,
    scale: u64,
    keys: &[GenKey],
    max_mantaray_scale: u64,
) -> (Vec<StorageCell>, Vec<GetHopsCell>) {
    let probes = sample_indices(keys.len(), HOP_PROBES);

    let mut storage = StorageCell {
        corpus: corpus.name().to_string(),
        scale,
        total_chunks: BTreeMap::new(),
        slot_utilisation: BTreeMap::new(),
        embed_fraction: BTreeMap::new(),
        chunk_ratio_02_over_10: None,
        nulls: Vec::new(),
    };
    let mut hops = GetHopsCell {
        corpus: corpus.name().to_string(),
        scale,
        sample: probes.len() as u64,
        per_arm: BTreeMap::new(),
        mean_ratio_02_over_10: None,
        nulls: Vec::new(),
    };

    // The two 1.0 arms, each over its own counting store.
    let ldb_gap = drive(
        LDB_V1,
        &mut LdbArm::<V1>::new(),
        keys,
        &probes,
        &mut storage,
        &mut hops,
    );
    let _v1read_gap = drive(
        LDB_V1READ,
        &mut LdbArm::<V1Read>::new(),
        keys,
        &probes,
        &mut storage,
        &mut hops,
    );

    // The 0.2 arm, unless the scale cap skips it.
    let mantaray_gap = if scale > max_mantaray_scale {
        let reason = cap_reason(max_mantaray_scale);
        record_gap(MANTARAY, &reason, &mut storage, &mut hops);
        Some(reason)
    } else {
        drive(
            MANTARAY,
            &mut MantarayArm::new(),
            keys,
            &probes,
            &mut storage,
            &mut hops,
        )
    };

    // The comparison columns exist only when both sides measured.
    storage.chunk_ratio_02_over_10 = ratio(
        storage.total_chunks.get(MANTARAY).map(|&c| c as f64),
        storage.total_chunks.get(LDB_V1).map(|&c| c as f64),
    );
    hops.mean_ratio_02_over_10 = ratio(
        hops.per_arm.get(MANTARAY).map(|s| s.mean),
        hops.per_arm.get(LDB_V1).map(|s| s.mean),
    );
    if storage.chunk_ratio_02_over_10.is_none() {
        let reason = mantaray_gap.clone().or_else(|| ldb_gap.clone());
        storage.nulls.push(gap(
            MANTARAY,
            "chunk_ratio_02_over_10",
            &reason.unwrap_or_else(|| "no 1.0 chunk total to divide by".to_string()),
        ));
    }
    if hops.mean_ratio_02_over_10.is_none() {
        let reason = mantaray_gap.or(ldb_gap);
        hops.nulls.push(gap(
            MANTARAY,
            "mean_ratio_02_over_10",
            &reason.unwrap_or_else(|| "no 1.0 mean hop count to divide by".to_string()),
        ));
    }

    (vec![storage], vec![hops])
}

#[cfg(test)]
mod tests {
    use super::{HOP_RTT_SET, LDB_V1, LDB_V1READ, MANTARAY, storage_and_hops, utilisation};
    use crate::arm::Arm;
    use crate::arm_ldb::LdbArm;
    use crate::corpus::{self, Corpus};
    use crate::perf::sample_indices;
    use nectar_ldb::V1;
    use nectar_primitives::DEFAULT_BODY_SIZE;

    /// The unit-B acceptance gate at kiwix 1e4: the 0.2 slot utilisation lands
    /// near the whitepaper's flat ~5%, the 1.0 embed fraction is above 0.5,
    /// every histogram sums to the probe count and every RTT column is exactly
    /// `mean * rtt`.
    #[test]
    fn kiwix_1e4_storage_and_hops_meet_the_whitepaper_shape() {
        let keys = corpus::generate(Corpus::Kiwix, 10_000);
        let (storage, hops) = storage_and_hops(Corpus::Kiwix, 10_000, &keys, 100_000);

        let cell = &storage[0];
        let hop = &hops[0];
        assert_eq!(cell.corpus, "kiwix");
        assert_eq!(cell.scale, 10_000);
        assert!(cell.nulls.iter().all(|n| !n.reason.contains("failed")));
        assert!(hop.nulls.iter().all(|n| !n.reason.contains("failed")));

        // All three arms measured, over their own stores.
        for label in [LDB_V1, LDB_V1READ, MANTARAY] {
            assert!(cell.total_chunks.contains_key(label), "{label}: chunks");
            assert!(
                cell.slot_utilisation.contains_key(label),
                "{label}: utilisation"
            );
            assert!(hop.per_arm.contains_key(label), "{label}: hops");
        }

        // The 0.2 arm buys a whole chunk slot per small node: the whitepaper's
        // flat ~5%.
        let u02 = cell.slot_utilisation[MANTARAY];
        assert!(
            (0.03..0.08).contains(&u02),
            "0.2 slot utilisation {u02} is not near the whitepaper's flat ~5%"
        );
        // The 1.0 arm packs the chunk body it bought.
        let u10 = cell.slot_utilisation[LDB_V1];
        assert!(u10 > 0.25, "1.0 utilisation {u10} does not fill a slot");
        assert!(u10 > u02, "1.0 utilisation {u10} did not beat 0.2 {u02}");

        // Most 1.0 nodes are embedded in their parent, not written out.
        let embed = cell.embed_fraction[LDB_V1];
        assert!(
            embed > 0.5,
            "ldb-v1 embed fraction {embed} is not above 0.5"
        );
        // The 0.2 arm never fabricates an embed number.
        assert!(!cell.embed_fraction.contains_key(MANTARAY));
        assert!(
            cell.nulls
                .iter()
                .any(|n| n.arm == MANTARAY && n.field == "embed_fraction")
        );

        // The "fewer chunks" column divides the two measured totals.
        let ratio = cell
            .chunk_ratio_02_over_10
            .expect("both arms measured a chunk total");
        let expected = cell.total_chunks[MANTARAY] as f64 / cell.total_chunks[LDB_V1] as f64;
        assert!((ratio - expected).abs() < f64::EPSILON);
        assert!(ratio > 5.0, "0.2 did not buy far more chunks: {ratio}");

        for (label, stats) in &hop.per_arm {
            // The histogram is the raw CDF data: it accounts for every probe.
            let counted: u64 = stats.histogram.values().sum();
            assert_eq!(counted, hop.sample, "{label}: histogram sum");
            // Mean and max agree with the histogram.
            let weighted: u64 = stats.histogram.iter().map(|(h, c)| h * c).sum();
            let mean = weighted as f64 / hop.sample as f64;
            assert!((stats.mean - mean).abs() < 1e-12, "{label}: mean");
            assert_eq!(
                stats.max,
                *stats.histogram.keys().next_back().unwrap(),
                "{label}: max"
            );
            assert!(stats.max >= 1, "{label}: a get charged no fetch");
            // The RTT columns are exactly mean * rtt.
            assert_eq!(stats.latency_ms_by_rtt.len(), HOP_RTT_SET.len());
            for rtt in HOP_RTT_SET {
                let got = stats.latency_ms_by_rtt[&rtt.to_string()];
                assert_eq!(got, stats.mean * f64::from(rtt), "{label}: rtt {rtt}");
            }
        }

        // The "cut" column divides the two measured means.
        let cut = hop
            .mean_ratio_02_over_10
            .expect("both arms measured a mean");
        let expected = hop.per_arm[MANTARAY].mean / hop.per_arm[LDB_V1].mean;
        assert!((cut - expected).abs() < f64::EPSILON);
        assert!(cut > 1.0, "0.2 did not cost more hops than 1.0: {cut}");
    }

    /// Above the cap the 0.2 arm is a null-with-reason on every field and no
    /// comparison column is fabricated.
    #[test]
    fn above_the_cap_every_02_field_is_a_null_with_reason() {
        let keys = corpus::generate(Corpus::Kiwix, 500);
        let (storage, hops) = storage_and_hops(Corpus::Kiwix, 500, &keys, 100);

        let cell = &storage[0];
        let hop = &hops[0];
        assert!(!cell.total_chunks.contains_key(MANTARAY));
        assert!(!cell.slot_utilisation.contains_key(MANTARAY));
        assert!(!hop.per_arm.contains_key(MANTARAY));
        assert!(cell.chunk_ratio_02_over_10.is_none());
        assert!(hop.mean_ratio_02_over_10.is_none());
        for field in [
            "total_chunks",
            "slot_utilisation",
            "embed_fraction",
            "chunk_ratio_02_over_10",
        ] {
            let null = cell
                .nulls
                .iter()
                .find(|n| n.arm == MANTARAY && n.field == field)
                .unwrap_or_else(|| panic!("{field}: no null-with-reason"));
            assert!(null.reason.contains("skipped by policy"), "{field}: reason");
        }
        for field in ["per_arm", "mean_ratio_02_over_10"] {
            let null = hop
                .nulls
                .iter()
                .find(|n| n.arm == MANTARAY && n.field == field)
                .unwrap_or_else(|| panic!("{field}: no null-with-reason"));
            assert!(null.reason.contains("skipped by policy"), "{field}: reason");
        }
        // The 1.0 arms still measured.
        assert!(cell.total_chunks.contains_key(LDB_V1));
        assert!(hop.per_arm.contains_key(LDB_V1));
    }

    /// Each hop figure brackets exactly one `get`: the per-probe deltas sum to
    /// the whole-loop store delta, and utilisation divides by the one shared
    /// chunk body size.
    #[test]
    fn hop_deltas_bracket_one_get_and_utilisation_uses_one_body_size() {
        let keys = corpus::generate(Corpus::Kiwix, 1_000);
        let mut arm = LdbArm::<V1>::new();
        arm.build(&keys).unwrap();

        let probes = sample_indices(keys.len(), 64);
        let before = arm.counters().gets;
        let mut sum = 0u64;
        for &i in &probes {
            let outcome = arm.get(&keys[i].raw).unwrap();
            sum += outcome.cost.unwrap().fetches;
        }
        let observed = arm.counters().gets - before;
        assert_eq!(sum, observed, "per-probe deltas must tile the loop delta");

        let counters = arm.counters();
        assert_eq!(
            utilisation(counters.live_bytes, counters.total_chunks),
            counters.live_bytes as f64 / (counters.total_chunks as f64 * DEFAULT_BODY_SIZE as f64)
        );
        assert_eq!(utilisation(0, 0), 0.0);
    }
}
