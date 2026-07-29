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
//! Both 0.2 columns are measured, never assumed: the batched figure comes from
//! one multi-op `ManifestEditor` commit and the per-edit figure from K
//! open-put-commit cycles, so the whitepaper's flat per-edit line is a
//! prediction this module tests, not an input.
//!
//! Counter discipline: the frozen [`Arm`] seam owns each arm's
//! `CountingStore` and does not expose `reset_flow`, so a measurement is
//! bracketed instead of zeroed. Every `batch_update` is wrapped in a
//! `counters()` pair and the bracket delta must equal the cost the arm
//! reports; a disagreement would mean one measurement bled into the next and
//! is recorded as a null-with-reason rather than published. The bracket is
//! exact for the same reason `reset_flow` would be: the arms read the same
//! atomics, and nothing else touches the store between the two reads.
//!
//! [`BuildReport`]: crate::arm::BuildReport
//! [`BatchMode::Batched`]: crate::arm::BatchMode
//! [`BatchMode::PerEdit`]: crate::arm::BatchMode

use std::collections::BTreeMap;

use nectar_ldb::{V1, V1Read};

use crate::arm::{Arm, BatchMode, Capability, NullWithReason, build_checked};
use crate::arm_ldb::LdbArm;
use crate::arm_mantaray::MantarayArm;
use crate::corpus::{Corpus, GenKey};
use crate::matrix::MANTARAY;
use crate::perf::{WRITE_AMP_KS, sample_indices};
use crate::results::{ArmBuildProfile, BuildProfileCell, WriteAmpCell};

/// The batched write-amplification field name, used by the null records.
const FIELD_BATCHED: &str = "wa_batched";
/// The per-edit write-amplification field name, used by the null records.
const FIELD_PER_EDIT: &str = "wa_per_edit";
/// The build-profile field name, used by the null records.
const FIELD_PROFILE: &str = "per_arm";

/// The scale-cap policy reason (spec section 2).
fn cap_reason(max_mantaray_scale: u64) -> String {
    format!(
        "mantaray 0.2 skipped by policy above {max_mantaray_scale}: the editor commit \
         materialises the whole trie in RAM"
    )
}

/// One arm's null for `field`, with its reason.
fn gap(arm: &str, field: &str, reason: String) -> NullWithReason {
    NullWithReason {
        arm: arm.to_string(),
        field: field.to_string(),
        reason,
    }
}

/// The reason an outcome carries no cost: the capability's own words when the
/// op is unsupported, else the bare fact.
fn no_cost_reason(capability: &Capability) -> String {
    match capability {
        Capability::Unsupported { reason } => reason.clone(),
        _ => "the arm returned no cost for a supported operation".to_string(),
    }
}

/// Build one arm and record its build profile, or record why it has none.
///
/// Returns `true` when the arm is built and may join the sweep. The counters
/// are read straight after the build, so `peak_live_store_bytes` and
/// `total_put_bytes` are the build's own figures and no sweep write pollutes
/// them.
fn profile(
    arm: &mut dyn Arm,
    keys: &[GenKey],
    per_arm: &mut BTreeMap<String, ArmBuildProfile>,
    nulls: &mut Vec<NullWithReason>,
) -> bool {
    let label = arm.label();
    match build_checked(arm, keys) {
        Ok(report) => {
            let counters = arm.counters();
            per_arm.insert(
                label.to_string(),
                ArmBuildProfile {
                    frontier: report.frontier,
                    nodes_written: report.nodes_written,
                    nodes_embedded: report.nodes_embedded,
                    peak_live_store_bytes: counters.peak_live_bytes,
                    total_put_bytes: counters.put_bytes,
                },
            );
            true
        }
        Err(e) => {
            nulls.push(gap(label, FIELD_PROFILE, format!("build failed: {e}")));
            false
        }
    }
}

/// The edit budget of one K row: a row averages over `ceil(BUDGET / K)`
/// batches of K edits.
///
/// A K row is one batch as soon as K reaches the budget, which is the whole
/// sweep above K=10. Below it, one batch is one arbitrary key and its depth,
/// not the tree's: the K=1 row would read the depth of key 0 rather than the
/// mean single-update cost, and the sweep would compare rows drawn from
/// different key samples. Repeating the row over disjoint, equally spread
/// batches makes every row a mean over at least `BUDGET` edits.
const EDIT_BUDGET: usize = 64;

/// The batches of one K row: `ceil(BUDGET / K)` disjoint edit sets of K keys,
/// each spread across the whole key domain with the same stride.
///
/// Batch `r` is `sample_indices(n, k)` shifted by `r` sub-strides, so every
/// batch has the shape of the single `sample_indices` sample the spec names and
/// no two batches edit the same keys.
fn batches(keys: &[GenKey], k: usize) -> Vec<Vec<GenKey>> {
    let n = keys.len();
    if n == 0 || k == 0 {
        return Vec::new();
    }
    let reps = EDIT_BUDGET.div_ceil(k).max(1);
    let base = sample_indices(n, k);
    let stride = (n / k).max(1);
    let shift = (stride / reps).max(1);
    (0..reps)
        .map(|r| {
            base.iter()
                .filter_map(|i| keys.get((i + r * shift).min(n - 1)).cloned())
                .collect()
        })
        .collect()
}

/// Measure one `(arm, mode)` write amplification: chunks written over edits
/// applied, summed over the row's batches.
///
/// Every batch is bracketed by a `counters()` pair; the bracket delta must
/// equal the cost the arm reports, or the figure is dropped as a
/// null-with-reason instead of published.
fn measure(
    arm: &dyn Arm,
    batches: &[Vec<GenKey>],
    mode: BatchMode,
    field: &str,
    out: &mut BTreeMap<String, f64>,
    nulls: &mut Vec<NullWithReason>,
) {
    let label = arm.label();
    let mut puts = 0u64;
    let mut edits = 0usize;
    for batch in batches {
        if batch.is_empty() {
            continue;
        }
        let before = arm.counters();
        let outcome = match arm.batch_update(batch, mode) {
            Ok(outcome) => outcome,
            Err(e) => {
                nulls.push(gap(label, field, format!("batch update failed: {e}")));
                return;
            }
        };
        let after = arm.counters();
        let Some(cost) = outcome.cost else {
            nulls.push(gap(label, field, no_cost_reason(&outcome.capability)));
            return;
        };
        let delta = after.puts.saturating_sub(before.puts);
        if delta != cost.puts {
            nulls.push(gap(
                label,
                field,
                format!(
                    "counter bracket disagreed: the store charged {delta} puts and the arm \
                     reported {}",
                    cost.puts
                ),
            ));
            return;
        }
        puts += cost.puts;
        edits += batch.len();
    }
    if edits > 0 {
        out.insert(label.to_string(), puts as f64 / edits as f64);
    }
}

/// Write-amplification and build-profile cells for one `(corpus, scale)`.
///
/// - `corpus`: the corpus enum, for the cell's `corpus` key.
/// - `scale`: the scale, for the cell's `scale` key.
/// - `keys`: the shared, sorted key set both arms consume in the same order.
/// - `max_mantaray_scale`: above this the 0.2 arm is skipped by policy and its
///   fields are null-with-reason.
///
/// The K sweep is `crate::perf::WRITE_AMP_KS`; edits come from
/// `crate::perf::sample_indices(n, k)` so K rows above n are skipped. A row
/// repeats over disjoint equally spread batches until it has applied at least
/// [`EDIT_BUDGET`] edits, so the small-K rows are means and not one key's
/// depth.
#[must_use]
pub fn writeamp_and_build(
    corpus: Corpus,
    scale: u64,
    keys: &[GenKey],
    max_mantaray_scale: u64,
) -> (Vec<WriteAmpCell>, Vec<BuildProfileCell>) {
    sweep(corpus, scale, keys, max_mantaray_scale, &WRITE_AMP_KS)
}

/// The sweep body over an explicit K list.
///
/// The public entry point passes `crate::perf::WRITE_AMP_KS`. The tests pass a
/// shorter list at the same scale, because one per-edit row costs K
/// open-put-commit cycles on every arm and the top of the sweep is minutes of
/// unoptimised build time; the metric itself is unchanged, only the number of
/// K rows differs.
///
/// Visible to the crate so the determinism gate in `crate::render` can sweep
/// the same code over the same corpus and scale under a shorter K list.
pub(crate) fn sweep(
    corpus: Corpus,
    scale: u64,
    keys: &[GenKey],
    max_mantaray_scale: u64,
    ks: &[u64],
) -> (Vec<WriteAmpCell>, Vec<BuildProfileCell>) {
    let n = keys.len();
    if n == 0 {
        return (Vec::new(), Vec::new());
    }
    let name = corpus.name();
    let run_02 = scale <= max_mantaray_scale;

    // Each arm builds the same keys in the same order over its own counting
    // store; the profile is read before any sweep write.
    let mut ldb_v1 = LdbArm::<V1>::new();
    let mut ldb_v1read = LdbArm::<V1Read>::new();
    let mut mantaray = MantarayArm::new();
    let mut per_arm = BTreeMap::new();
    let mut build_nulls = Vec::new();

    let v1_ok = profile(&mut ldb_v1, keys, &mut per_arm, &mut build_nulls);
    let v1read_ok = profile(&mut ldb_v1read, keys, &mut per_arm, &mut build_nulls);
    let m_ok = run_02 && profile(&mut mantaray, keys, &mut per_arm, &mut build_nulls);
    if !run_02 {
        build_nulls.push(gap(MANTARAY, FIELD_PROFILE, cap_reason(max_mantaray_scale)));
    }

    let mut arms: Vec<&dyn Arm> = Vec::new();
    if v1_ok {
        arms.push(&ldb_v1);
    }
    if v1read_ok {
        arms.push(&ldb_v1read);
    }
    if m_ok {
        arms.push(&mantaray);
    }

    let build_profile = vec![BuildProfileCell {
        corpus: name.to_string(),
        scale,
        per_arm,
        nulls: build_nulls,
    }];

    // The K sweep. Every row edits the same tree: `batch_update` discards the
    // root each mode returns, so no row observes the previous row's writes.
    let mut write_amp = Vec::new();
    for &k in ks {
        let Ok(k_usize) = usize::try_from(k) else {
            continue;
        };
        if k_usize == 0 || k_usize > n {
            continue;
        }
        let row = batches(keys, k_usize);
        let mut wa_batched = BTreeMap::new();
        let mut wa_per_edit = BTreeMap::new();
        let mut nulls = Vec::new();
        for arm in &arms {
            measure(
                *arm,
                &row,
                BatchMode::Batched,
                FIELD_BATCHED,
                &mut wa_batched,
                &mut nulls,
            );
            measure(
                *arm,
                &row,
                BatchMode::PerEdit,
                FIELD_PER_EDIT,
                &mut wa_per_edit,
                &mut nulls,
            );
        }
        if !run_02 {
            let reason = cap_reason(max_mantaray_scale);
            nulls.push(gap(MANTARAY, FIELD_BATCHED, reason.clone()));
            nulls.push(gap(MANTARAY, FIELD_PER_EDIT, reason));
        }
        write_amp.push(WriteAmpCell {
            corpus: name.to_string(),
            scale,
            k,
            wa_batched,
            wa_per_edit,
            nulls,
        });
    }

    (write_amp, build_profile)
}

#[cfg(test)]
mod tests {
    use super::{
        FIELD_BATCHED, FIELD_PER_EDIT, FIELD_PROFILE, MANTARAY, batches, sweep, writeamp_and_build,
    };

    use nectar_ldb::V1;

    use crate::arm::{Arm, BatchMode, FrontierClass};
    use crate::arm_ldb::LdbArm;
    use crate::arm_mantaray::MantarayArm;
    use crate::corpus::{self, Corpus};
    use crate::matrix::{LDB_V1, LDB_V1READ};
    use crate::perf::{WRITE_AMP_KS, sample_indices};

    /// The K rows the gate sweeps at 1e4 through the published cells.
    ///
    /// A per-edit row costs K commits on every arm, so the top of
    /// `WRITE_AMP_KS` is minutes of unoptimised build time. The rows here carry
    /// the whole cell shape; the tail of the same curve is taken on the batched
    /// side by `the_batched_curve_keeps_falling_through_the_top_of_the_sweep`,
    /// where one K row is one commit.
    const GATE_KS: [u64; 3] = [1, 10, 100];

    /// The acceptance gate for the sweep: 1.0 batched write amplification falls
    /// monotonically in K, both 0.2 columns are measured from their own code
    /// paths, and the naive per-edit client never beats the batch on either
    /// format.
    #[test]
    fn batched_write_amp_falls_in_k_and_per_edit_never_wins() {
        let keys = corpus::generate(Corpus::Kiwix, 10_000);
        let (write_amp, build_profile) = sweep(Corpus::Kiwix, 10_000, &keys, 100_000, &GATE_KS);

        assert_eq!(write_amp.len(), GATE_KS.len(), "one row per K");
        for (row, k) in write_amp.iter().zip(GATE_KS) {
            assert_eq!(row.k, k);
            assert_eq!(row.corpus, "kiwix");
            assert_eq!(row.scale, 10_000);
            assert!(row.nulls.is_empty(), "unexpected gap: {:?}", row.nulls);
            for arm in [LDB_V1, LDB_V1READ, MANTARAY] {
                let batched = row.wa_batched.get(arm).copied().unwrap();
                let per_edit = row.wa_per_edit.get(arm).copied().unwrap();
                assert!(batched > 0.0, "{arm} K={k}: batched wrote nothing");
                assert!(per_edit > 0.0, "{arm} K={k}: per-edit wrote nothing");
                // Measured, not assumed: one multi-op commit is never worse
                // than K single-op commits.
                assert!(
                    per_edit >= batched,
                    "{arm} K={k}: per-edit {per_edit} below batched {batched}"
                );
            }
        }

        // 1.0 batched write amplification falls monotonically in K.
        for arm in [LDB_V1, LDB_V1READ] {
            let series: Vec<f64> = write_amp
                .iter()
                .filter_map(|row| row.wa_batched.get(arm).copied())
                .collect();
            assert_eq!(series.len(), GATE_KS.len());
            assert!(
                series.windows(2).all(|w| w[1] < w[0]),
                "{arm}: batched write amp not falling in K: {series:?}"
            );
        }

        // The 0.2 per-edit column is a real second measurement, not a copy of
        // the batched one: batching must amortise somewhere in the sweep.
        let amortised = write_amp.iter().any(|row| {
            let batched = row.wa_batched.get(MANTARAY).copied().unwrap_or(0.0);
            let per_edit = row.wa_per_edit.get(MANTARAY).copied().unwrap_or(0.0);
            per_edit > batched
        });
        assert!(amortised, "0.2 batched and per-edit never differ");

        // The build profile: both memory laws, both counted from the store.
        let cell = build_profile.first().unwrap();
        assert!(cell.nulls.is_empty(), "unexpected gap: {:?}", cell.nulls);
        for arm in [LDB_V1, LDB_V1READ] {
            let p = cell.per_arm.get(arm).unwrap();
            assert!(matches!(p.frontier, FrontierClass::Bounded { .. }));
            assert!(p.nodes_embedded.unwrap() > 0);
            assert!(p.nodes_written > 0);
            assert!(p.peak_live_store_bytes > 0);
            assert!(p.total_put_bytes >= p.peak_live_store_bytes);
        }
        let m = cell.per_arm.get(MANTARAY).unwrap();
        assert!(matches!(m.frontier, FrontierClass::WholeTrie { .. }));
        assert!(m.nodes_embedded.is_none(), "0.2 has no embedding");
        assert!(m.peak_live_store_bytes > 0);
    }

    /// The whole batched curve at kiwix 1e4, every K of `WRITE_AMP_KS`: it
    /// falls monotonically to the top of the sweep, and there 1.0 writes less
    /// than one chunk per edit while 0.2 stays above it.
    ///
    /// Only the batched side is swept here, so a K row is one commit and the
    /// K=10000 row is affordable. The figure is the published one: the same
    /// `batches` sample, the same chunks-written-over-edits ratio.
    #[test]
    fn the_batched_curve_keeps_falling_through_the_top_of_the_sweep() {
        let keys = corpus::generate(Corpus::Kiwix, 10_000);
        let mut ldb = LdbArm::<V1>::new();
        let mut mantaray = MantarayArm::new();
        ldb.build(&keys).unwrap();
        mantaray.build(&keys).unwrap();

        let mut tops = Vec::new();
        for arm in [&ldb as &dyn Arm, &mantaray] {
            let label = arm.label();
            let series: Vec<f64> = WRITE_AMP_KS
                .iter()
                .map(|&k| batched_wa(arm, &keys, k as usize))
                .collect();
            assert_eq!(series.len(), WRITE_AMP_KS.len());
            assert!(
                series.windows(2).all(|w| w[1] < w[0]),
                "{label}: batched write amp not falling in K: {series:?}"
            );
            tops.push(*series.last().unwrap());
        }

        // At the top of the sweep the 1.0 batch writes less than one chunk per
        // edit; the 0.2 whole-trie commit never gets there.
        let (ldb_top, m_top) = (tops[0], tops[1]);
        assert!(ldb_top < 1.0, "1.0 batched write amp at K=n is {ldb_top}");
        assert!(m_top > ldb_top, "0.2 batched write amp at K=n is {m_top}");
    }

    /// The published batched write amplification of one arm at one K: chunks
    /// written over edits applied, summed over the row's batches.
    fn batched_wa(arm: &dyn Arm, keys: &[crate::corpus::GenKey], k: usize) -> f64 {
        let row = batches(keys, k);
        let mut puts = 0u64;
        let mut edits = 0usize;
        for batch in &row {
            puts += arm
                .batch_update(batch, BatchMode::Batched)
                .unwrap()
                .cost
                .unwrap()
                .puts;
            edits += batch.len();
        }
        puts as f64 / edits as f64
    }

    /// The base tree never advances between sweep rows: every row edits the
    /// root the build produced.
    ///
    /// The frozen `Arm` seam exposes no root accessor, so the witness is
    /// behavioural and stronger than an equality on the reference. After the
    /// whole sweep, replaying the first measurement reproduces its cost to the
    /// fetch and adds no distinct chunk, and every probe's get cost is
    /// unchanged. A root that had advanced would rewrite the spine from a
    /// different tree and both would move.
    #[test]
    fn the_base_root_never_advances_between_sweep_rows() {
        let keys = corpus::generate(Corpus::Kiwix, 1_000);
        let n = keys.len();
        let mut ldb = LdbArm::<V1>::new();
        let mut mantaray = MantarayArm::new();
        ldb.build(&keys).unwrap();
        mantaray.build(&keys).unwrap();
        let probes: Vec<&[u8]> = sample_indices(n, 16)
            .into_iter()
            .map(|i| keys[i].raw.as_slice())
            .collect();

        for arm in [&ldb as &dyn Arm, &mantaray] {
            let label = arm.label();
            let first: Vec<crate::corpus::GenKey> = vec![keys[0].clone()];
            let before_gets: Vec<u64> = probes
                .iter()
                .map(|p| arm.get(p).unwrap().cost.unwrap().fetches)
                .collect();
            let baseline = arm
                .batch_update(&first, BatchMode::Batched)
                .unwrap()
                .cost
                .unwrap();

            // The rest of the sweep, both modes, against the same tree.
            for k in [10usize, 100] {
                let edits: Vec<crate::corpus::GenKey> = sample_indices(n, k)
                    .into_iter()
                    .map(|i| keys[i].clone())
                    .collect();
                let batched = arm
                    .batch_update(&edits, BatchMode::Batched)
                    .unwrap()
                    .cost
                    .unwrap();
                let per_edit = arm
                    .batch_update(&edits, BatchMode::PerEdit)
                    .unwrap()
                    .cost
                    .unwrap();
                assert!(
                    per_edit.puts >= batched.puts,
                    "{label} K={k}: per-edit {} below batched {}",
                    per_edit.puts,
                    batched.puts
                );
            }

            // Replay the first measurement: identical cost, and every chunk it
            // writes is already resident, so the tree it started from is the
            // tree the build produced.
            let chunks_before = arm.counters().total_chunks;
            let replay = arm
                .batch_update(&first, BatchMode::Batched)
                .unwrap()
                .cost
                .unwrap();
            let chunks_after = arm.counters().total_chunks;
            assert_eq!(replay.puts, baseline.puts, "{label}: replay puts moved");
            assert_eq!(
                replay.fetches, baseline.fetches,
                "{label}: replay fetches moved"
            );
            assert_eq!(
                chunks_after, chunks_before,
                "{label}: the replay wrote a new chunk address"
            );
            let after_gets: Vec<u64> = probes
                .iter()
                .map(|p| arm.get(p).unwrap().cost.unwrap().fetches)
                .collect();
            assert_eq!(before_gets, after_gets, "{label}: get cost moved");
        }
    }

    /// Above the cap every 0.2 field is a null-with-reason and no 0.2 number is
    /// published.
    #[test]
    fn above_the_cap_every_02_field_is_a_null_with_reason() {
        let keys = corpus::generate(Corpus::Kiwix, 500);
        let (write_amp, build_profile) =
            writeamp_and_build(Corpus::Kiwix, 1_000_000, &keys, 100_000);

        let cell = build_profile.first().unwrap();
        assert!(!cell.per_arm.contains_key(MANTARAY), "0.2 published a cell");
        assert!(cell.per_arm.contains_key(LDB_V1));
        let null = cell
            .nulls
            .iter()
            .find(|g| g.arm == MANTARAY && g.field == FIELD_PROFILE)
            .unwrap();
        assert!(null.reason.contains("skipped by policy above 100000"));

        // The public entry point sweeps every K of WRITE_AMP_KS at or below n.
        let expected: Vec<u64> = WRITE_AMP_KS.iter().copied().filter(|&k| k <= 500).collect();
        let seen: Vec<u64> = write_amp.iter().map(|row| row.k).collect();
        assert_eq!(seen, expected, "the K rows the public sweep emits");
        for row in &write_amp {
            assert!(!row.wa_batched.contains_key(MANTARAY));
            assert!(!row.wa_per_edit.contains_key(MANTARAY));
            for field in [FIELD_BATCHED, FIELD_PER_EDIT] {
                assert!(
                    row.nulls
                        .iter()
                        .any(|g| g.arm == MANTARAY && g.field == field),
                    "K={}: no null for {field}",
                    row.k
                );
            }
        }
    }
}
