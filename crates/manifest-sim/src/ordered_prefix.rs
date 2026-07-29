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
//!
//! Measurement conventions, because the frozen schema stores integral costs:
//!
//! - The `fair` column holds the AGGREGATE cost over the cell's `probes`
//!   probes. `OpCost` is integral, so a mean cannot be stored without loss; the
//!   per-probe mean is `fetches / probes` and the multipliers below are
//!   computed from that mean.
//! - The `pessimal` column holds ONE measurement per arm. A full walk's cost
//!   does not depend on the probe, so the pessimal figure is already a
//!   per-probe cost. The representative probe is the median of the probe set.
//! - The pessimal path of an ordered point op is the whole-manifest walk that a
//!   client without the primitive must run, so the 0.2 arm serves it with
//!   `Arm::full_iter`. The 1.0 arms have no degraded path, so their pessimal
//!   column repeats the native probe cost, matching the `*_pessimal` convention
//!   of the 1.0 arm.
//! - The multiplier denominators are the `ldb-v1` fair mean, so
//!   `fair_multiplier` and `pessimal_multiplier` share one baseline.
//! - `native_abs_mean` and `native_abs_max` are the `ldb-v1` absolute per-probe
//!   mean and maximum, the honest cost beside the ratio.
//! - The range sweep uses the sub-unit window fractions of `perf::RANGE_WS`. A
//!   full-domain window makes the fair drain and the pessimal walk the same
//!   walk, so no fair-below-pessimal reading exists there.
//! - A range probe drains O(window) keys, so the range windows use eight
//!   evenly spaced placements rather than the point-op probe set. Every cell
//!   states its own `probes` count.
//!
//! # A seek on a shallow tree can touch the whole tree
//!
//! On the uniform corpus at 1e3 the 1.0 floor and ceiling each charge 39
//! fetches, which is exactly that arm's resident chunk count. The figure reads
//! like a whole-tree walk and is not one. The uniform corpus is 48 random bytes
//! per key, so its trie is a wide, shallow node whose chunks all sit on or
//! beside the single descent, and the cursor's read-ahead window covers the
//! rest. A seek can never cost more than the tree holds, so a 39-chunk tree
//! caps the seek at 39.
//!
//! The law is settled by scale, not by one number: the same probe costs 7.0
//! fetches over 262 chunks at 1e4 and 21.8 fetches over 3910 chunks at 1e5, so
//! the touched fraction collapses from 100% to 2.7% to 0.6% while N grows a
//! hundredfold. A real whole-tree walk would hold that fraction at 1.0 and grow
//! with N. `the_ldb_seek_is_not_a_whole_tree_walk` is the gate that separates
//! the two.

use std::collections::BTreeMap;

use nectar_ldb::{V1, V1Read};

use crate::arm::{Arm, Capability, Err, NullWithReason, OpCost, OpOutcome, build_checked};
use crate::arm_ldb::LdbArm;
use crate::arm_mantaray::MantarayArm;
use crate::corpus::{Corpus, GenKey};
use crate::matrix::{LDB_V1, MANTARAY};
use crate::perf::{RANGE_WS, sample_indices};
use crate::results::{OrderedOpCell, PrefixListingCell};

/// Sampled keys per ordered point-op probe set; each is probed twice.
const PROBE_KEYS: usize = 48;
/// Evenly spaced window placements per range window fraction.
const RANGE_PROBES: usize = 8;
/// The 0.2 fields a scale cap nulls out.
const CAPPED_FIELDS: [&str; 4] = ["fair", "pessimal", "fair_multiplier", "pessimal_multiplier"];

/// Ordered-op and prefix-listing cells for one `(corpus, scale)`.
///
/// - `corpus`: the corpus enum, for the cell's `corpus` key.
/// - `scale`: the scale, for the cell's `scale` key.
/// - `keys`: the shared, sorted key set both arms consume in the same order.
/// - `max_mantaray_scale`: above this the 0.2 arm is skipped by policy and its
///   fields are null-with-reason.
///
/// A store error cannot happen over an in-memory store. If one does, the cells
/// are dropped rather than filled with a fabricated number.
#[must_use]
pub fn ordered_and_prefix(
    corpus: Corpus,
    scale: u64,
    keys: &[GenKey],
    max_mantaray_scale: u64,
) -> (Vec<OrderedOpCell>, Vec<PrefixListingCell>) {
    measure(corpus, scale, keys, max_mantaray_scale).unwrap_or_default()
}

/// The measured body of [`ordered_and_prefix`].
fn measure(
    corpus: Corpus,
    scale: u64,
    keys: &[GenKey],
    max_mantaray_scale: u64,
) -> Result<(Vec<OrderedOpCell>, Vec<PrefixListingCell>), Err> {
    let mut v1 = LdbArm::<V1>::new();
    build_checked(&mut v1, keys)?;
    let mut v1read = LdbArm::<V1Read>::new();
    build_checked(&mut v1read, keys)?;

    // The 0.2 arm runs up to the cap only: above it the editor commit
    // materialises the whole trie in RAM.
    let capped = scale > max_mantaray_scale;
    let mut mantaray = MantarayArm::new();
    if !capped {
        build_checked(&mut mantaray, keys)?;
    }
    let mut arms: Vec<&dyn Arm> = vec![&v1, &v1read];
    if !capped {
        arms.push(&mantaray);
    }
    let cap_reason = capped.then(|| cap_reason(max_mantaray_scale));
    let cap_null = cap_reason.as_deref();

    let probes = point_probes(keys);
    let mut ordered = Vec::new();
    for op in [PointOp::Floor, PointOp::Ceiling] {
        ordered.push(point_cell(corpus, scale, op, &arms, &probes, cap_null)?);
    }
    for window in RANGE_WS.iter().copied().filter(|w| *w < 1.0) {
        if let Some(cell) = range_cell(corpus, scale, window, keys, &arms, cap_null)? {
            ordered.push(cell);
        }
    }

    let listing = match prefix_cell(corpus, scale, keys, &arms, cap_null)? {
        Some(cell) => vec![cell],
        None => Vec::new(),
    };
    Ok((ordered, listing))
}

/// The policy reason the 0.2 arm carries above the scale cap.
fn cap_reason(max_mantaray_scale: u64) -> String {
    format!(
        "mantaray 0.2 skipped by policy above {max_mantaray_scale}: the editor commit \
         materialises the whole trie in RAM"
    )
}

// ---- ordered point ops ---------------------------------------------------

/// The two ordered point ops the section-3 table prices.
#[derive(Clone, Copy, Debug)]
enum PointOp {
    /// Greatest key `<= key`.
    Floor,
    /// Smallest key `>= key`.
    Ceiling,
}

impl PointOp {
    /// The cell's `op` key.
    const fn name(self) -> &'static str {
        match self {
            Self::Floor => "floor",
            Self::Ceiling => "ceiling",
        }
    }

    /// Run the op on one arm through the seam.
    fn run(self, arm: &dyn Arm, key: &[u8]) -> Result<OpOutcome, Err> {
        match self {
            Self::Floor => arm.floor(key),
            Self::Ceiling => arm.ceiling(key),
        }
    }
}

/// The point-op probe set: `PROBE_KEYS` evenly spaced keys, each followed by an
/// absent neighbour, so present and absent probes are equally represented.
fn point_probes(keys: &[GenKey]) -> Vec<Vec<u8>> {
    let mut out = Vec::with_capacity(PROBE_KEYS.saturating_mul(2));
    for i in sample_indices(keys.len(), PROBE_KEYS) {
        if let Some(k) = keys.get(i) {
            out.push(k.raw.clone());
            out.push(absent_neighbour(&k.raw));
        }
    }
    out
}

/// The key with its last byte incremented; an overflowing byte appends `0x00`.
fn absent_neighbour(raw: &[u8]) -> Vec<u8> {
    let mut out = raw.to_vec();
    match out.last_mut() {
        Some(b) if *b < u8::MAX => *b = b.saturating_add(1),
        _ => out.push(0),
    }
    out
}

/// One `(corpus, scale, op)` cell for floor or ceiling.
fn point_cell(
    corpus: Corpus,
    scale: u64,
    op: PointOp,
    arms: &[&dyn Arm],
    probes: &[Vec<u8>],
    cap_null: Option<&str>,
) -> Result<OrderedOpCell, Err> {
    let representative: &[u8] = probes.get(probes.len() / 2).map_or(&[], Vec::as_slice);
    let mut cols = Columns::default();
    for arm in arms {
        let mut outcomes = Vec::with_capacity(probes.len());
        for probe in probes {
            outcomes.push(op.run(*arm, probe)?);
        }
        cols.push_fair(arm.label(), &outcomes);
        // The pessimal path is the whole-manifest walk a client without the
        // primitive must run; the 1.0 arms have no degraded path.
        let pessimal = if arm.label() == MANTARAY {
            arm.full_iter()?
        } else {
            op.run(*arm, representative)?
        };
        cols.push_pessimal(arm.label(), pessimal);
    }
    Ok(cols.into_ordered_cell(
        corpus,
        scale,
        op.name(),
        None,
        probes.len() as u64,
        cap_null,
    ))
}

// ---- ordered range op ----------------------------------------------------

/// One `(corpus, scale, range, window)` cell, or `None` when the corpus is too
/// small to place a window.
fn range_cell(
    corpus: Corpus,
    scale: u64,
    window: f64,
    keys: &[GenKey],
    arms: &[&dyn Arm],
    cap_null: Option<&str>,
) -> Result<Option<OrderedOpCell>, Err> {
    let bounds = range_bounds(keys, window);
    let Some(representative) = bounds.get(bounds.len() / 2).copied() else {
        return Ok(None);
    };
    let mut cols = Columns::default();
    for arm in arms {
        let mut outcomes = Vec::with_capacity(bounds.len());
        for (lo, hi) in &bounds {
            outcomes.push(arm.range(lo, hi)?);
        }
        cols.push_fair(arm.label(), &outcomes);
        let pessimal = arm.range_pessimal(representative.0, representative.1)?;
        cols.push_pessimal(arm.label(), pessimal);
    }
    Ok(Some(cols.into_ordered_cell(
        corpus,
        scale,
        "range",
        Some(window),
        bounds.len() as u64,
        cap_null,
    )))
}

/// The `[lo, hi)` bounds of `RANGE_PROBES` evenly spaced windows of fractional
/// width `w`. Both bounds are corpus keys, so the 1.0 `range(lo, hi)` and the
/// 0.2 `get(lo)` plus `after(lo)` drain cover the same half-open window.
fn range_bounds(keys: &[GenKey], w: f64) -> Vec<(&[u8], &[u8])> {
    let n = keys.len();
    if n < 2 {
        return Vec::new();
    }
    let width = ((n as f64) * w).round().max(1.0) as usize;
    let width = width.min(n.saturating_sub(1));
    let placements = n.saturating_sub(width);
    sample_indices(placements, RANGE_PROBES)
        .into_iter()
        .filter_map(|lo| {
            let hi = lo.checked_add(width)?;
            Some((keys.get(lo)?.raw.as_slice(), keys.get(hi)?.raw.as_slice()))
        })
        .collect()
}

// ---- prefix listing ------------------------------------------------------

/// The whitepaper 2.3 listing cell for one `(corpus, scale)`.
fn prefix_cell(
    corpus: Corpus,
    scale: u64,
    keys: &[GenKey],
    arms: &[&dyn Arm],
    cap_null: Option<&str>,
) -> Result<Option<PrefixListingCell>, Err> {
    let Some(prefix) = listing_prefix(corpus, keys) else {
        return Ok(None);
    };
    let selected = keys.iter().filter(|k| k.raw.starts_with(&prefix)).count() as u64;
    let mut cols = Columns::default();
    for arm in arms {
        let fair = arm.prefix_list(&prefix)?;
        cols.push_fair(arm.label(), std::slice::from_ref(&fair));
        let pessimal = arm.prefix_list_pessimal(&prefix)?;
        cols.push_pessimal(arm.label(), pessimal);
    }
    let (fair_multiplier, pessimal_multiplier) = cols.multipliers();
    let mut nulls = cols.nulls;
    push_cap_nulls(&mut nulls, cap_null);
    push_multiplier_nulls(&mut nulls, fair_multiplier, pessimal_multiplier);
    Ok(Some(PrefixListingCell {
        corpus: corpus.name().to_string(),
        scale,
        prefix: String::from_utf8_lossy(&prefix).into_owned(),
        keys_returned: selected,
        fair: cols.fair,
        pessimal: cols.pessimal,
        fair_multiplier,
        pessimal_multiplier,
        nulls,
    }))
}

/// The listing prefix of a corpus: the directory span of the median key, or its
/// first byte for the flat uniform corpus. This mirrors the private
/// `perf::first_prefix`, which this unit may not export.
fn listing_prefix(corpus: Corpus, keys: &[GenKey]) -> Option<Vec<u8>> {
    let mid = keys.get(keys.len() / 2)?;
    let raw = &mid.raw;
    let p = match corpus {
        Corpus::Uniform => raw.iter().take(1).copied().collect(),
        _ => match raw.iter().rposition(|&b| b == b'/') {
            Some(pos) => raw.get(..=pos).map(<[u8]>::to_vec).unwrap_or_default(),
            None => raw.iter().take(1).copied().collect(),
        },
    };
    Some(p)
}

// ---- shared column accumulation ------------------------------------------

/// The fair and pessimal columns of one cell, plus the per-arm figures the
/// multipliers are computed from.
#[derive(Debug, Default)]
struct Columns {
    /// Aggregate fair outcome per arm.
    fair: BTreeMap<String, OpOutcome>,
    /// Single pessimal outcome per arm.
    pessimal: BTreeMap<String, OpOutcome>,
    /// Mean fair fetches per probe, per arm.
    fair_mean: BTreeMap<String, f64>,
    /// Pessimal fetches, per arm.
    pessimal_fetches: BTreeMap<String, f64>,
    /// The `ldb-v1` absolute per-probe mean.
    native_abs_mean: Option<f64>,
    /// The `ldb-v1` absolute per-probe maximum.
    native_abs_max: Option<u64>,
    /// Gaps: an unsupported op or a capped arm.
    nulls: Vec<NullWithReason>,
}

impl Columns {
    /// Fold one arm's probe series into the fair column.
    fn push_fair(&mut self, label: &str, outcomes: &[OpOutcome]) {
        let Some(first) = outcomes.first() else {
            return;
        };
        let capability = first.capability.clone();
        if outcomes.iter().any(|o| o.cost.is_none()) {
            let outcome = OpOutcome {
                capability,
                cost: None,
            };
            note_gap(&mut self.nulls, label, "fair", &outcome);
            self.fair.insert(label.to_string(), outcome);
            return;
        }
        let mut total = OpCost::default();
        let mut max_fetches = 0u64;
        for cost in outcomes.iter().filter_map(|o| o.cost) {
            total.fetches = total.fetches.saturating_add(cost.fetches);
            total.puts = total.puts.saturating_add(cost.puts);
            total.keys_returned = total.keys_returned.saturating_add(cost.keys_returned);
            max_fetches = max_fetches.max(cost.fetches);
        }
        let mean = (total.fetches as f64) / (outcomes.len() as f64);
        if label == LDB_V1 {
            self.native_abs_mean = Some(mean);
            self.native_abs_max = Some(max_fetches);
        }
        self.fair_mean.insert(label.to_string(), mean);
        self.fair.insert(
            label.to_string(),
            OpOutcome {
                capability,
                cost: Some(total),
            },
        );
    }

    /// Record one arm's single pessimal measurement.
    fn push_pessimal(&mut self, label: &str, outcome: OpOutcome) {
        match outcome.cost {
            Some(cost) => {
                self.pessimal_fetches
                    .insert(label.to_string(), cost.fetches as f64);
            }
            None => note_gap(&mut self.nulls, label, "pessimal", &outcome),
        }
        self.pessimal.insert(label.to_string(), outcome);
    }

    /// The fair and pessimal multipliers over the `ldb-v1` fair mean.
    fn multipliers(&self) -> (Option<f64>, Option<f64>) {
        let base = self.fair_mean.get(LDB_V1).copied();
        let fair = self
            .fair_mean
            .get(MANTARAY)
            .copied()
            .zip(base)
            .and_then(|(m, b)| ratio(m, b));
        let pessimal = self
            .pessimal_fetches
            .get(MANTARAY)
            .copied()
            .zip(base)
            .and_then(|(m, b)| ratio(m, b));
        (fair, pessimal)
    }

    /// Close the columns into an ordered-op cell.
    fn into_ordered_cell(
        self,
        corpus: Corpus,
        scale: u64,
        op: &str,
        window: Option<f64>,
        probes: u64,
        cap_null: Option<&str>,
    ) -> OrderedOpCell {
        let (fair_multiplier, pessimal_multiplier) = self.multipliers();
        let mut nulls = self.nulls;
        push_cap_nulls(&mut nulls, cap_null);
        push_multiplier_nulls(&mut nulls, fair_multiplier, pessimal_multiplier);
        OrderedOpCell {
            corpus: corpus.name().to_string(),
            scale,
            op: op.to_string(),
            window,
            probes,
            fair: self.fair,
            pessimal: self.pessimal,
            fair_multiplier,
            pessimal_multiplier,
            native_abs_mean: self.native_abs_mean,
            native_abs_max: self.native_abs_max,
            nulls,
        }
    }
}

/// Record a null-with-reason when an outcome carries no cost.
fn note_gap(nulls: &mut Vec<NullWithReason>, arm: &str, field: &str, outcome: &OpOutcome) {
    if outcome.cost.is_some() {
        return;
    }
    let reason = match &outcome.capability {
        Capability::Unsupported { reason } => reason.clone(),
        other => format!("no cost measured for {other:?}"),
    };
    nulls.push(NullWithReason {
        arm: arm.to_string(),
        field: field.to_string(),
        reason,
    });
}

/// Name every 0.2 field the scale cap leaves null.
fn push_cap_nulls(nulls: &mut Vec<NullWithReason>, cap_null: Option<&str>) {
    let Some(reason) = cap_null else {
        return;
    };
    for field in CAPPED_FIELDS {
        nulls.push(NullWithReason {
            arm: MANTARAY.to_string(),
            field: field.to_string(),
            reason: reason.to_string(),
        });
    }
}

/// Name a missing multiplier under its OWN field, carrying the reason its
/// numerator carries.
///
/// The renderer matches a footnote to the exact field it prints and never
/// borrows an unrelated gap the same arm recorded, so a multiplier cell only
/// gets a reason if one was filed against the multiplier itself.
fn push_multiplier_nulls(
    nulls: &mut Vec<NullWithReason>,
    fair_multiplier: Option<f64>,
    pessimal_multiplier: Option<f64>,
) {
    for (missing, source, field) in [
        (fair_multiplier.is_none(), "fair", "fair_multiplier"),
        (
            pessimal_multiplier.is_none(),
            "pessimal",
            "pessimal_multiplier",
        ),
    ] {
        if !missing || nulls.iter().any(|n| n.arm == MANTARAY && n.field == field) {
            continue;
        }
        let reason = nulls
            .iter()
            .find(|n| n.arm == MANTARAY && n.field == source)
            .map_or_else(
                || format!("no 0.2 {source} figure and no 1.0 baseline to divide it by"),
                |n| n.reason.clone(),
            );
        nulls.push(NullWithReason {
            arm: MANTARAY.to_string(),
            field: field.to_string(),
            reason,
        });
    }
}

/// `num / den`, or `None` when the denominator is zero.
fn ratio(num: f64, den: f64) -> Option<f64> {
    (den > 0.0).then_some(num / den)
}

#[cfg(test)]
mod tests {
    use super::{MANTARAY, ordered_and_prefix};
    use crate::corpus::{self, Corpus};
    use crate::matrix::{LDB_V1, LDB_V1READ};
    use crate::results::{OrderedOpCell, PrefixListingCell};

    /// The fair fetch total of one arm, or 0 when the arm is absent.
    fn fair_fetches(cell: &OrderedOpCell, arm: &str) -> u64 {
        cell.fair
            .get(arm)
            .and_then(|o| o.cost)
            .map_or(0, |c| c.fetches)
    }

    /// The fair key count of one arm, or `None` when the arm is absent.
    fn fair_keys(cell: &OrderedOpCell, arm: &str) -> Option<u64> {
        cell.fair
            .get(arm)
            .and_then(|o| o.cost)
            .map(|c| c.keys_returned)
    }

    /// The pessimal fetch count of one arm, or 0 when the arm is absent.
    fn pessimal_fetches(cell: &OrderedOpCell, arm: &str) -> u64 {
        cell.pessimal
            .get(arm)
            .and_then(|o| o.cost)
            .map_or(0, |c| c.fetches)
    }

    /// Every capped 0.2 field is named in the cell's nulls.
    fn assert_capped(nulls: &[crate::arm::NullWithReason]) {
        for field in super::CAPPED_FIELDS {
            assert!(
                nulls
                    .iter()
                    .any(|n| n.arm == MANTARAY && n.field == field && n.reason.contains("policy")),
                "no null-with-reason for the capped 0.2 {field}"
            );
        }
    }

    /// The 1.0 probe cost stays O(depth) at 1e4 and every 0.2 cell above the
    /// cap is a null-with-reason, never a number.
    #[test]
    fn ldb_seeks_stay_o_depth_and_the_capped_02_arm_is_null_with_reason() {
        let keys = corpus::generate(Corpus::Kiwix, 10_000);
        let (ordered, listing) = ordered_and_prefix(Corpus::Kiwix, 10_000, &keys, 1_000);
        assert!(!ordered.is_empty(), "no ordered-op cells");
        assert_eq!(listing.len(), 1, "one listing cell per (corpus, scale)");

        for cell in &ordered {
            if cell.op == "floor" || cell.op == "ceiling" {
                let max = cell.native_abs_max.unwrap_or(u64::MAX);
                assert!(
                    max < 64,
                    "{}: ldb-v1 max fetches {max} is not O(depth)",
                    cell.op
                );
                let mean = cell.native_abs_mean.unwrap_or(f64::MAX);
                assert!(mean < 64.0, "{}: ldb-v1 mean fetches {mean}", cell.op);
                let v1read = fair_fetches(cell, LDB_V1READ) / cell.probes.max(1);
                assert!(v1read < 64, "{}: ldb-v1read mean fetches {v1read}", cell.op);
            }
            assert!(!cell.fair.contains_key(MANTARAY), "capped 0.2 fair cell");
            assert!(
                !cell.pessimal.contains_key(MANTARAY),
                "capped 0.2 pessimal cell"
            );
            assert!(cell.fair_multiplier.is_none(), "capped 0.2 multiplier");
            assert!(cell.pessimal_multiplier.is_none(), "capped 0.2 multiplier");
            assert_capped(&cell.nulls);
        }
        for cell in &listing {
            assert!(!cell.fair.contains_key(MANTARAY), "capped 0.2 listing cell");
            assert!(cell.fair_multiplier.is_none(), "capped 0.2 listing ratio");
            assert_capped(&cell.nulls);
        }
    }

    /// The mean per-probe fetches of one arm on one cell.
    fn fair_mean(cell: &OrderedOpCell, arm: &str) -> f64 {
        fair_fetches(cell, arm) as f64 / (cell.probes.max(1) as f64)
    }

    /// The `(mean fetches, resident chunks)` of one 1.0 point op at one scale
    /// on the uniform corpus, measured over one build.
    fn uniform_seek(op: &str, scale: u64) -> (f64, u64) {
        use crate::arm::{Arm as _, build_checked};
        use crate::arm_ldb::LdbArm;
        use nectar_ldb::V1;

        let keys = corpus::generate(Corpus::Uniform, scale as usize);
        let mut arm = LdbArm::<V1>::new();
        build_checked(&mut arm, &keys).expect("build the uniform arm");
        let chunks = arm.counters().total_chunks;
        let (ordered, _) = ordered_and_prefix(Corpus::Uniform, scale, &keys, 0);
        let cell = ordered
            .iter()
            .find(|c| c.op == op)
            .expect("the point-op cell");
        (fair_mean(cell, LDB_V1), chunks)
    }

    /// A seek must not be a whole-tree walk wearing a seek's name.
    ///
    /// The uniform corpus at 1e3 is the trap: the 1.0 floor and ceiling each
    /// charge 39 fetches over a 39-chunk tree, so the arm reads the whole tree
    /// for one key. That is legitimate at that shape (see the module docs), and
    /// the way to tell it apart from a real O(N) walk is to change N: a seek's
    /// cost stays bounded while the tree grows, so the touched fraction must
    /// collapse. A whole-tree walk would hold the fraction at 1.0 and grow
    /// tenfold with the corpus.
    #[test]
    fn the_ldb_seek_is_not_a_whole_tree_walk() {
        for op in ["floor", "ceiling"] {
            let (small_fetches, small_chunks) = uniform_seek(op, 1_000);
            let (big_fetches, big_chunks) = uniform_seek(op, 10_000);
            assert!(
                big_chunks > small_chunks.saturating_mul(4),
                "{op}: the tree did not grow between the scales"
            );
            // The cost must not scale with the corpus: tenfold N, at most
            // double the fetches.
            assert!(
                big_fetches <= small_fetches * 2.0,
                "{op}: {small_fetches} fetches at 1e3 became {big_fetches} at 1e4, which tracks N"
            );
            // And the touched fraction must collapse, which a whole-tree walk
            // can never do.
            let small_fraction = small_fetches / (small_chunks.max(1) as f64);
            let big_fraction = big_fetches / (big_chunks.max(1) as f64);
            assert!(
                big_fraction < small_fraction / 4.0,
                "{op}: touched fraction {small_fraction} then {big_fraction}: the seek reads the \
                 whole tree at every scale"
            );
            assert!(
                big_fraction < 0.25,
                "{op}: one probe still touches {big_fraction} of a {big_chunks}-chunk tree"
            );
        }
    }

    /// Red-team check 6, the 1.0 side: the ceiling rides the range cursor, so
    /// its absolute is an upper bound on a dedicated seek. The bound must still
    /// behave like a seek across scales (near-flat, never O(N)), and every
    /// ceiling measurement must carry the label that says what it is.
    #[test]
    fn the_ldb_ceiling_is_labelled_and_stays_seek_grade_across_scales() {
        use crate::arm::Capability;

        let (small, _) = uniform_seek("ceiling", 1_000);
        let (big, _) = uniform_seek("ceiling", 10_000);
        assert!(
            big <= small * 2.0,
            "the ceiling bound grew with N: {small} then {big}"
        );

        let keys = corpus::generate(Corpus::Kiwix, 1_000);
        let (ordered, _) = ordered_and_prefix(Corpus::Kiwix, 1_000, &keys, 0);
        let cell = ordered
            .iter()
            .find(|c| c.op == "ceiling")
            .expect("the ceiling cell");
        for arm in [LDB_V1, LDB_V1READ] {
            let outcome = cell.fair.get(arm).expect("a 1.0 ceiling outcome");
            match &outcome.capability {
                Capability::Emulated { how, cost_class } => {
                    assert_eq!(how, crate::arm_ldb::CEILING_HOW);
                    assert!(
                        cost_class.contains("READ_AHEAD"),
                        "the ceiling class hides the read-ahead: {cost_class}"
                    );
                }
                other => panic!("{arm}: the ceiling is classed {other:?}, not as the bound it is"),
            }
        }
        // The floor is a real primitive and stays native, so the label is not
        // being sprayed over the whole 1.0 arm.
        let floor = ordered
            .iter()
            .find(|c| c.op == "floor")
            .expect("the floor cell");
        assert!(
            matches!(
                floor.fair.get(LDB_V1).map(|o| &o.capability),
                Some(Capability::Native)
            ),
            "the 1.0 floor lost its native class"
        );
    }

    /// With the 0.2 arm inside the cap: the fair emulation is cheaper than its
    /// pessimal at every window, the fair multiplier never exceeds the pessimal
    /// multiplier, and the two arms agree on the half-open range bounds.
    #[test]
    fn fair_paths_beat_pessimal_and_the_arms_agree_on_bounds() {
        let keys = corpus::generate(Corpus::Kiwix, 1_000);
        let (ordered, listing) = ordered_and_prefix(Corpus::Kiwix, 1_000, &keys, 100_000);
        let ranges: Vec<&OrderedOpCell> = ordered.iter().filter(|c| c.op == "range").collect();
        assert!(!ranges.is_empty(), "no range cells");

        for cell in &ordered {
            let probes = cell.probes.max(1);
            let fair_mean = fair_fetches(cell, MANTARAY) / probes;
            let pessimal = pessimal_fetches(cell, MANTARAY);
            assert!(pessimal > 0, "{}: 0.2 pessimal charged no fetch", cell.op);
            assert!(
                fair_mean < pessimal,
                "{} {:?}: 0.2 fair {fair_mean} did not beat pessimal {pessimal}",
                cell.op,
                cell.window
            );
            let fair_m = cell.fair_multiplier.unwrap_or(f64::MAX);
            let pessimal_m = cell.pessimal_multiplier.unwrap_or(0.0);
            assert!(
                fair_m <= pessimal_m,
                "{} {:?}: fair multiplier {fair_m} above pessimal {pessimal_m}",
                cell.op,
                cell.window
            );
            assert!(cell.nulls.is_empty(), "{}: unexpected null", cell.op);
        }

        // Inclusive and exclusive bounds match: the 1.0 native range and the
        // 0.2 get(lo) plus after(lo) drain return the same keys.
        for cell in &ranges {
            let ldb = fair_keys(cell, LDB_V1);
            let ldb_read = fair_keys(cell, LDB_V1READ);
            let mantaray = fair_keys(cell, MANTARAY);
            assert_eq!(
                ldb, mantaray,
                "window {:?}: bound mismatch between the arms",
                cell.window
            );
            assert_eq!(ldb, ldb_read, "window {:?}: 1.0 arms disagree", cell.window);
            assert!(
                mantaray.unwrap_or(0) > 0,
                "window {:?}: the drain returned nothing",
                cell.window
            );
        }

        // The listing keeps the same discipline and every arm returns exactly
        // the keys the prefix selects.
        let cell: &PrefixListingCell = listing.first().expect("one listing cell");
        for arm in [LDB_V1, LDB_V1READ, MANTARAY] {
            let returned = cell
                .fair
                .get(arm)
                .and_then(|o| o.cost)
                .map(|c| c.keys_returned);
            assert_eq!(
                returned,
                Some(cell.keys_returned),
                "{arm}: fair listing returned the wrong key count"
            );
        }
        let fair_m = cell.fair_multiplier.unwrap_or(f64::MAX);
        let pessimal_m = cell.pessimal_multiplier.unwrap_or(0.0);
        assert!(
            fair_m <= pessimal_m,
            "listing fair multiplier {fair_m} above pessimal {pessimal_m}"
        );
        let fair = cell.fair.get(MANTARAY).and_then(|o| o.cost);
        let pessimal = cell.pessimal.get(MANTARAY).and_then(|o| o.cost);
        assert!(
            fair.map_or(0, |c| c.fetches) < pessimal.map_or(0, |c| c.fetches),
            "the 0.2 pruned prefix walk did not beat the full walk"
        );
        assert!(cell.nulls.is_empty(), "unexpected listing null");
    }
}
