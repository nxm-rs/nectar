//! The markdown renderer: one [`Document`] in, GitHub-markdown tables out.
//!
//! The renderer reads a finished run back and prints it. It never measures,
//! never fills a gap, never averages across corpora and never extrapolates a
//! capped arm from a smaller scale. A missing figure prints as `--` and its
//! reason becomes a footnote, so a gap stays legible as a gap.
//!
//! # The two audiences
//!
//! The whitepaper is read by two people and the layout keeps them apart. The
//! `User experience` group answers what a reader of a manifest pays: the
//! capability matrix, storage, get hops, listing, ordered operations, cursor
//! rounds, pagination and subtree serve. The `Developer experience` group
//! answers what a writer of a manifest pays: the format choice, the build
//! frontier, write amplification and build wall-time. No table sits in both
//! groups, so neither audience reads the other's number as its own.
//!
//! # Native against emulated
//!
//! A number is not comparable to another number until the reader knows how each
//! was served. Every table whose cells come from a measured operation carries a
//! capability legend beneath it: an unmarked figure is a native primitive, and a
//! marked one names the emulation and its cost class. The legend is per table,
//! because a marker is only useful beside the cells it explains.
//!
//! Two labels matter most. The 1.0 ceiling is composed from the range cursor,
//! so its figure is an upper bound on a dedicated seek and says so. The 1.0
//! pessimal cells repeat the native cost, because 1.0 has no degraded path, so
//! they are labelled `native (no 1.0 fallback)` and never read as a measured
//! whole-manifest walk.
//!
//! Two further rules ride the layout. Every table is keyed by corpus, because a
//! figure from one corpus never speaks for another. Every multiplier prints
//! beside the 1.0 absolute cost it divides, because a widening ratio and a
//! growing floor are different findings.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use crate::arm::{Capability, FrontierClass, NullWithReason, OpOutcome};
use crate::arm_ldb::NO_FALLBACK;
use crate::arm_mantaray::{BATCH_CLASS, BATCH_HOW};
use crate::results::{
    BuildProfileCell, BuildWallCell, CapabilityRow, Document, GetHopsCell, OrderedOpCell,
    PaginateCell, ParallelCursorCell, PrefixListingCell, ReadProfileCell, StorageCell,
    SubtreeServeCell, WriteAmpCell,
};

/// The cell every gap prints as.
const NULL: &str = "--";

// ---- footnotes -----------------------------------------------------------

/// The footnote pool: one number per distinct reason, in first-seen order.
///
/// Reasons repeat heavily (one capped arm explains dozens of cells), so the
/// pool deduplicates by text. A gap without a recorded reason still prints
/// `--`; it just carries no marker, which is itself visible in review.
#[derive(Debug, Default)]
struct Footnotes {
    index: BTreeMap<String, usize>,
    order: Vec<String>,
}

impl Footnotes {
    /// The `--` cell for a gap, with a footnote marker when a reason exists.
    fn null(&mut self, reason: Option<&str>) -> String {
        match reason {
            Some(r) if !r.is_empty() => format!("{NULL}[^{}]", self.number(r)),
            _ => NULL.to_string(),
        }
    }

    /// The footnote number for `reason`, allocating on first sight.
    fn number(&mut self, reason: &str) -> usize {
        if let Some(&n) = self.index.get(reason) {
            return n;
        }
        let n = self.order.len().saturating_add(1);
        self.index.insert(reason.to_string(), n);
        self.order.push(reason.to_string());
        n
    }

    /// The footnote definition block, or an empty string when nothing is null.
    fn definitions(&self) -> String {
        if self.order.is_empty() {
            return String::new();
        }
        let mut out = String::from("\n## Notes on the null cells\n\n");
        for (i, reason) in self.order.iter().enumerate() {
            let _ = writeln!(out, "[^{}]: {reason}", i.saturating_add(1));
        }
        out
    }
}

/// The recorded reason for one arm's gap in one field.
///
/// The match is exact. An earlier version fell back to any gap the same arm
/// recorded, which let a storage footnote explain a hop cell; a wrong reason is
/// worse than none, so a field with no gap of its own prints a bare `--`.
fn reason_for<'a>(nulls: &'a [NullWithReason], arm: &str, field: &str) -> Option<&'a str> {
    nulls
        .iter()
        .find(|n| n.arm == arm && n.field == field)
        .map(|n| n.reason.as_str())
}

// ---- the capability legend -----------------------------------------------

/// The per-table legend of capability markers.
///
/// A native primitive is unmarked. Anything else takes a letter that resolves
/// under the table it appears in, so a reader never has to open the JSON to
/// learn which numbers the format served itself and which a client emulated.
#[derive(Debug, Default)]
struct Legend {
    entries: Vec<String>,
}

impl Legend {
    /// The marker for `text`, allocating a letter on first sight.
    fn marker(&mut self, text: &str) -> String {
        let idx = match self.entries.iter().position(|e| e == text) {
            Some(i) => i,
            None => {
                self.entries.push(text.to_string());
                self.entries.len().saturating_sub(1)
            }
        };
        format!(" [{}]", Self::letter(idx))
    }

    /// The `idx`-th marker letter, falling back to a number past `z`.
    fn letter(idx: usize) -> String {
        u32::try_from(idx)
            .ok()
            .filter(|i| *i < 26)
            .and_then(|i| char::from_u32(u32::from(b'a').saturating_add(i)))
            .map_or_else(|| idx.saturating_add(1).to_string(), String::from)
    }

    /// The marker a fair-column figure carries: none when the format served the
    /// operation itself.
    fn fair(&mut self, c: &Capability) -> String {
        match c {
            Capability::Native => String::new(),
            Capability::Emulated { how, cost_class } => {
                self.marker(&format!("emulated: {how} ({cost_class})"))
            }
            Capability::Unsupported { .. } => String::new(),
        }
    }

    /// The marker a pessimal-column figure carries.
    ///
    /// A pessimal column prices the degraded path a client without the
    /// primitive must run. A native cell there is not a measured whole-manifest
    /// walk: it is the native cost repeated, because the arm has no degraded
    /// path at all. The marker says exactly that.
    fn pessimal(&mut self, c: &Capability) -> String {
        match c {
            Capability::Native => self.marker(NO_FALLBACK),
            other => self.fair(other),
        }
    }

    /// The legend block, or an empty string when every figure was native.
    fn block(&self) -> String {
        if self.entries.is_empty() {
            return String::new();
        }
        let mut out = String::from(
            "Capability legend (an unmarked figure is a native primitive \
of the format):\n\n",
        );
        for (i, text) in self.entries.iter().enumerate() {
            let _ = writeln!(out, "- `[{}]` {text}", Self::letter(i));
        }
        out.push('\n');
        out
    }
}

// ---- formatting ----------------------------------------------------------

/// Two decimal places: ratios, multipliers and fractions.
fn f2(v: f64) -> String {
    format!("{v:.2}")
}

/// Three decimal places: slot utilisation and write amplification.
fn f3(v: f64) -> String {
    format!("{v:.3}")
}

/// A thousands-separated integer, so a six-figure fetch count stays readable.
fn int(v: u64) -> String {
    let s = v.to_string();
    let mut out = String::with_capacity(s.len().saturating_add(s.len() / 3));
    let bytes = s.as_bytes();
    for (i, b) in bytes.iter().enumerate() {
        if i > 0 && (bytes.len().saturating_sub(i)) % 3 == 0 {
            out.push(',');
        }
        out.push(char::from(*b));
    }
    out
}

/// A window fraction as a percentage, or the em-free dash for a point op.
fn window(w: Option<f64>) -> String {
    w.map_or_else(|| NULL.to_string(), |w| format!("{:.3}%", w * 100.0))
}

/// One markdown table row.
fn row(cells: &[String]) -> String {
    format!("| {} |\n", cells.join(" | "))
}

/// A markdown table header plus its alignment rule.
fn header(cells: &[&str]) -> String {
    let mut out = row(&cells.iter().map(|c| (*c).to_string()).collect::<Vec<_>>());
    let _ = writeln!(
        out,
        "|{}|",
        cells
            .iter()
            .map(|_| " --- ".to_string())
            .collect::<Vec<_>>()
            .join("|")
    );
    out
}

/// Render one capability as its own table cell.
fn capability_cell(c: &Capability, fx: &mut Footnotes) -> String {
    match c {
        Capability::Native => "native".to_string(),
        Capability::Emulated { how, cost_class } => format!("emulated: {how} ({cost_class})"),
        // No primitive and no honest emulation is a gap, printed as one.
        Capability::Unsupported { reason } => fx.null(Some(reason)),
    }
}

/// The fetch count of an outcome with its capability marker, or a null carrying
/// the outcome's own reason.
fn outcome_fetches(
    o: Option<&OpOutcome>,
    fx: &mut Footnotes,
    lg: &mut Legend,
    pessimal: bool,
    fallback: Option<&str>,
) -> String {
    match o {
        Some(OpOutcome {
            capability,
            cost: Some(cost),
        }) => {
            let mark = if pessimal {
                lg.pessimal(capability)
            } else {
                lg.fair(capability)
            };
            format!("{}{mark}", int(cost.fetches))
        }
        Some(OpOutcome {
            capability: Capability::Unsupported { reason },
            ..
        }) => fx.null(Some(reason)),
        Some(_) | None => fx.null(fallback),
    }
}

/// The per-probe mean fetches of an aggregated outcome over `probes` probes.
fn outcome_mean(
    o: Option<&OpOutcome>,
    probes: u64,
    fx: &mut Footnotes,
    lg: &mut Legend,
    fallback: Option<&str>,
) -> String {
    match o {
        Some(OpOutcome {
            capability,
            cost: Some(cost),
        }) if probes > 0 => {
            let mark = lg.fair(capability);
            format!("{}{mark}", f2(cost.fetches as f64 / probes as f64))
        }
        other => outcome_fetches(other, fx, lg, false, fallback),
    }
}

/// A group of cells partitioned by corpus, in first-seen order, so no table
/// ever mixes two corpora.
fn by_corpus<'a, T>(cells: &'a [T], key: impl Fn(&'a T) -> &'a str) -> Vec<(&'a str, Vec<&'a T>)> {
    let mut out: Vec<(&str, Vec<&T>)> = Vec::new();
    for c in cells {
        let k = key(c);
        match out.iter_mut().find(|(name, _)| *name == k) {
            Some((_, group)) => group.push(c),
            None => out.push((k, vec![c])),
        }
    }
    out
}

/// The RTT column keys present across a group, ascending by value.
fn rtt_keys<'a, T>(cells: &'a [&'a T], keys: impl Fn(&'a T) -> Vec<&'a String>) -> Vec<String> {
    let mut set: BTreeSet<(u64, String)> = BTreeSet::new();
    for c in cells {
        for k in keys(c) {
            set.insert((k.parse::<u64>().unwrap_or(u64::MAX), k.clone()));
        }
    }
    set.into_iter().map(|(_, k)| k).collect()
}

// ---- the document --------------------------------------------------------

/// Render a whole result document as GitHub markdown.
#[must_use]
pub fn render(doc: &Document) -> String {
    let arms: Vec<&str> = doc.meta.arms.iter().map(|a| a.label.as_str()).collect();
    let mut fx = Footnotes::default();
    let mut out = String::new();

    out.push_str("# Two-arm manifest benchmark: mantaray 0.2 against mantaray 1.0\n\n");
    out.push_str(&provenance(doc));

    // The end user's side: what reading a manifest costs.
    out.push_str("## User experience\n\n");
    out.push_str(
        "What a client pays to read a manifest: which operations exist, how many chunks the tree \
holds, how many hops a lookup takes, and what a listing, an ordered seek, a cursor round and a \
page cost.\n\n",
    );
    out.push_str(&capability_matrix(
        &doc.deterministic.capability_matrix,
        &arms,
        &mut fx,
    ));
    out.push_str(&storage(&doc.deterministic.storage, &arms, &mut fx));
    out.push_str(&get_hops(
        &doc.deterministic.get_hops,
        &arms,
        &doc.meta.rtt_ms_set,
        &mut fx,
    ));
    out.push_str(&prefix_listing(
        &doc.deterministic.prefix_listing,
        &arms,
        &mut fx,
    ));
    out.push_str(&ordered_ops(&doc.deterministic.ordered_ops, &arms, &mut fx));
    out.push_str(&parallel_cursor(&doc.deterministic.parallel_cursor));
    out.push_str(&paginate(&doc.deterministic.paginate, &mut fx));
    out.push_str(&subtree_serve(&doc.deterministic.subtree_serve, &mut fx));

    // The builder's side: what writing a manifest costs.
    out.push_str("## Developer experience\n\n");
    out.push_str(
        "What a builder pays to write a manifest: which format parameters to pick, how much memory \
one build holds live, how many chunks an update rewrites, and how long a cold build runs.\n\n",
    );
    out.push_str(&read_profile(&doc.deterministic.v1read));
    out.push_str(&build_profile(
        &doc.deterministic.build_profile,
        &arms,
        &mut fx,
    ));
    out.push_str(&write_amp(&doc.deterministic.write_amp, &arms, &mut fx));
    out.push_str(&build_wall(
        &doc.wall_time.build_wall,
        &doc.wall_time.nulls,
        &mut fx,
    ));

    out.push_str(&fx.definitions());
    out
}

/// The provenance block: what was run, on what, under which policy.
fn provenance(doc: &Document) -> String {
    let m = &doc.meta;
    let mut out = String::new();
    let _ = writeln!(
        out,
        "Harness version {} at `{}` on `{}`, generated {}.",
        m.harness_version, m.git_commit, m.git_branch, m.generated
    );
    let _ = writeln!(
        out,
        "\nMaster seed {}, chunk body {} bytes, scales {}.",
        m.seed_master,
        m.chunk_body_size,
        m.scales
            .iter()
            .map(|s| int(*s))
            .collect::<Vec<_>>()
            .join(", ")
    );
    let _ = writeln!(
        out,
        "\nThe 0.2 arm runs up to {}; above that cap every 0.2 cell is a null with a reason and \
no multiplier exists.",
        int(m.max_mantaray_scale)
    );
    out.push_str("\nArms:\n\n");
    for a in &m.arms {
        let _ = writeln!(out, "- `{}`: {} {}", a.label, a.package, a.version);
    }
    out.push_str("\nCaveats:\n\n");
    for c in &m.caveats {
        let _ = writeln!(out, "- {c}");
    }
    out.push('\n');
    out
}

// ---- the capability matrix ------------------------------------------------

/// Where the in-tree crates depart from the whitepaper's classifications.
///
/// The harness reports the crate in front of it, so every departure is stated
/// where the matrix is read. Three rows moved since the whitepaper measured
/// them, and all three move in 0.2's favour, so leaving them unstated would
/// overstate the 1.0 case.
pub(crate) const DIVERGENCE_NOTE: &str = "Re-derived from the crates in tree, not from the \
whitepaper's historical classifications. Three rows diverge, and all three move in 0.2's favour, \
so the table below states each was-and-is pair rather than absorbing the correction silently.";

/// One whitepaper row the in-tree 0.2 crate has moved past.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Divergence {
    /// Which whitepaper row moved.
    pub row: &'static str,
    /// How the whitepaper-era crate classified it.
    pub was: &'static str,
    /// How the crate in tree measures today.
    pub is: &'static str,
}

/// Every divergence the harness corrects, in whitepaper row order.
pub(crate) const DIVERGENCES: [Divergence; 3] = [
    Divergence {
        row: "row 7, ceiling",
        was: "unsupported, so a client scanned the manifest: O(N)",
        is: "an after-bound pruned seek through the public cursor: O(depth + window)",
    },
    Divergence {
        row: "row 10, ordered iteration",
        was: "unordered, and it held O(N) entries in memory",
        is: "a streaming trie walk in documented path order: O(depth + window) memory",
    },
    Divergence {
        row: "6.2, batch update",
        was: "one flat per-edit line, because batching was not credited",
        is: "one multi-op editor commit amortises its writes: O(touched spine)",
    },
];

/// The measured capability matrix, one row per operation.
fn capability_matrix(rows: &[CapabilityRow], arms: &[&str], fx: &mut Footnotes) -> String {
    if rows.is_empty() {
        return String::new();
    }
    let mut out = String::from("### Capability matrix\n\n");
    let mut head = vec!["Operation"];
    head.extend_from_slice(arms);
    out.push_str(&header(&head));
    for r in rows {
        let mut cells = vec![format!("`{}`", r.op)];
        for arm in arms {
            cells.push(match r.per_arm.get(*arm) {
                Some(c) => capability_cell(c, fx),
                None => fx.null(None),
            });
        }
        out.push_str(&row(&cells));
    }
    out.push('\n');
    out.push_str(&divergences());
    out
}

/// The was-and-is table of every whitepaper row the in-tree crate has moved.
fn divergences() -> String {
    let mut out = String::from("#### Divergences from the whitepaper-era 0.2 crate\n\n");
    out.push_str(DIVERGENCE_NOTE);
    out.push_str("\n\n");
    out.push_str(&header(&["Whitepaper row", "Was", "Is"]));
    for d in DIVERGENCES {
        out.push_str(&row(&[
            d.row.to_string(),
            d.was.to_string(),
            d.is.to_string(),
        ]));
    }
    out.push('\n');
    out
}

// ---- 2.1 storage ----------------------------------------------------------

/// Whitepaper 2.1: resident chunks, slot utilisation and embed fraction, plus
/// the separate chunk-ratio table.
fn storage(cells: &[StorageCell], arms: &[&str], fx: &mut Footnotes) -> String {
    if cells.is_empty() {
        return String::new();
    }
    let mut out = String::from("### 2.1 Storage, slot utilisation and embedding\n\n");
    for (corpus, group) in by_corpus(cells, |c| c.corpus.as_str()) {
        let _ = writeln!(out, "#### Corpus `{corpus}`\n");
        out.push_str(&header(&[
            "Scale",
            "Arm",
            "Resident chunks",
            "Slot utilisation",
            "Embed fraction",
        ]));
        for c in &group {
            for arm in arms {
                out.push_str(&row(&[
                    int(c.scale),
                    format!("`{arm}`"),
                    c.total_chunks.get(*arm).map_or_else(
                        || fx.null(reason_for(&c.nulls, arm, "total_chunks")),
                        |v| int(*v),
                    ),
                    c.slot_utilisation.get(*arm).map_or_else(
                        || fx.null(reason_for(&c.nulls, arm, "slot_utilisation")),
                        |v| f3(*v),
                    ),
                    c.embed_fraction.get(*arm).map_or_else(
                        || fx.null(reason_for(&c.nulls, arm, "embed_fraction")),
                        |v| f3(*v),
                    ),
                ]));
            }
        }
        out.push_str("\nChunks written by 0.2 for every chunk written by 1.0:\n\n");
        out.push_str(&header(&["Scale", "0.2 chunks / 1.0 chunks"]));
        for c in &group {
            out.push_str(&row(&[
                int(c.scale),
                c.chunk_ratio_02_over_10.map_or_else(
                    || {
                        fx.null(reason_for(
                            &c.nulls,
                            "mantaray-0.2",
                            "chunk_ratio_02_over_10",
                        ))
                    },
                    f2,
                ),
            ]));
        }
        out.push_str(
            "\nBuilding is native on both arms, so no figure above is an emulation. The 0.2 build \
is one multi-op editor commit; the 1.0 build is the format's own builder.\n\n",
        );
    }
    out
}

// ---- 2.2 get hops ---------------------------------------------------------

/// Whitepaper 2.2: the get-hop distribution and the modelled RTT columns.
fn get_hops(cells: &[GetHopsCell], arms: &[&str], rtts: &[u32], fx: &mut Footnotes) -> String {
    if cells.is_empty() {
        return String::new();
    }
    let mut out = String::from("### 2.2 Get hops and modelled latency\n\n");
    for (corpus, group) in by_corpus(cells, |c| c.corpus.as_str()) {
        let _ = writeln!(out, "#### Corpus `{corpus}`\n");
        let rtt_heads: Vec<String> = rtts.iter().map(|r| format!("{r} ms")).collect();
        let mut head = vec![
            "Scale".to_string(),
            "Arm".to_string(),
            "Probes".to_string(),
            "Mean hops".to_string(),
            "Max hops".to_string(),
        ];
        head.extend(rtt_heads);
        out.push_str(&header(
            &head.iter().map(String::as_str).collect::<Vec<_>>(),
        ));
        for c in &group {
            for arm in arms {
                let mut cells = vec![int(c.scale), format!("`{arm}`"), int(c.sample)];
                match c.per_arm.get(*arm) {
                    Some(s) => {
                        cells.push(f2(s.mean));
                        cells.push(int(s.max));
                        for r in rtts {
                            let key = r.to_string();
                            cells.push(
                                s.latency_ms_by_rtt
                                    .get(&key)
                                    .map_or_else(|| fx.null(None), |v| f2(*v)),
                            );
                        }
                    }
                    None => {
                        let reason = reason_for(&c.nulls, arm, "per_arm");
                        for _ in 0..rtts.len().saturating_add(2) {
                            cells.push(fx.null(reason));
                        }
                    }
                }
                out.push_str(&row(&cells));
            }
        }
        out.push_str("\nHops charged by 0.2 for every hop charged by 1.0:\n\n");
        out.push_str(&header(&["Scale", "0.2 mean / 1.0 mean"]));
        for c in &group {
            out.push_str(&row(&[
                int(c.scale),
                c.mean_ratio_02_over_10.map_or_else(
                    || {
                        fx.null(reason_for(
                            &c.nulls,
                            "mantaray-0.2",
                            "mean_ratio_02_over_10",
                        ))
                    },
                    f2,
                ),
            ]));
        }
        out.push_str(
            "\nHops are the measured currency. Every millisecond column is that measured mean \
times a stated RTT under a sequential model with no pipelining and no caching; it is \
illustrative, not a timing. `get` is a native primitive on both arms, so no figure here is an \
emulation.\n\n",
        );
    }
    out
}

// ---- 2.3 prefix listing ---------------------------------------------------

/// Whitepaper 2.3: the fair listing walk beside the pessimal whole-manifest
/// walk, and both multipliers over the 1.0 native cost.
fn prefix_listing(cells: &[PrefixListingCell], arms: &[&str], fx: &mut Footnotes) -> String {
    if cells.is_empty() {
        return String::new();
    }
    let mut out = String::from("### 2.3 Prefix listing: fair and pessimal\n\n");
    for (corpus, group) in by_corpus(cells, |c| c.corpus.as_str()) {
        let mut lg = Legend::default();
        let _ = writeln!(out, "#### Corpus `{corpus}`\n");
        out.push_str(&header(&[
            "Scale",
            "Prefix",
            "Keys",
            "Arm",
            "Fair fetches",
            "Pessimal fetches",
        ]));
        for c in &group {
            for arm in arms {
                out.push_str(&row(&[
                    int(c.scale),
                    format!("`{}`", c.prefix),
                    int(c.keys_returned),
                    format!("`{arm}`"),
                    outcome_fetches(
                        c.fair.get(*arm),
                        fx,
                        &mut lg,
                        false,
                        reason_for(&c.nulls, arm, "fair"),
                    ),
                    outcome_fetches(
                        c.pessimal.get(*arm),
                        fx,
                        &mut lg,
                        true,
                        reason_for(&c.nulls, arm, "pessimal"),
                    ),
                ]));
            }
        }
        out.push_str("\nMultipliers over the 1.0 fair cost:\n\n");
        out.push_str(&header(&["Scale", "Fair 0.2 / 1.0", "Pessimal 0.2 / 1.0"]));
        for c in &group {
            out.push_str(&row(&[
                int(c.scale),
                c.fair_multiplier.map_or_else(
                    || fx.null(reason_for(&c.nulls, "mantaray-0.2", "fair_multiplier")),
                    f2,
                ),
                c.pessimal_multiplier.map_or_else(
                    || fx.null(reason_for(&c.nulls, "mantaray-0.2", "pessimal_multiplier")),
                    f2,
                ),
            ]));
        }
        out.push('\n');
        out.push_str(
            "The fair column is each arm's best public-API path: a prefix scan on 1.0, a pruned \
prefix walk on 0.2. The pessimal column is the whole-manifest walk a client without the pruned \
path must run. 1.0 has no degraded path at all, so no whole-manifest walk was measured on the 1.0 \
arms: their pessimal cells repeat the fair measurement and carry the marker that says so.\n\n",
        );
        out.push_str(&lg.block());
    }
    out
}

// ---- section 3 ordered ops ------------------------------------------------

/// The reading order of the section-3 ops, so a sorted table reads floor, then
/// ceiling, then the range windows.
fn op_rank(op: &str) -> u8 {
    match op {
        "floor" => 0,
        "ceiling" => 1,
        "range" => 2,
        _ => 3,
    }
}

/// Sort a corpus group so each op's N-series reads ascending.
///
/// The multiplier table is where "the ratio widens with N" is read, and that
/// reading needs the scales of one op adjacent and ascending. The measured
/// table above it stays in run order, which is scale-major, so a reader can
/// still take one scale as a block.
fn by_op_then_scale<'a>(group: &[&'a OrderedOpCell]) -> Vec<&'a OrderedOpCell> {
    let mut rows: Vec<&OrderedOpCell> = group.to_vec();
    rows.sort_by(|a, b| {
        op_rank(&a.op).cmp(&op_rank(&b.op)).then_with(|| {
            a.window
                .unwrap_or(0.0)
                .partial_cmp(&b.window.unwrap_or(0.0))
                .unwrap_or(core::cmp::Ordering::Equal)
                .then_with(|| a.scale.cmp(&b.scale))
        })
    });
    rows
}

/// Whitepaper section 3: floor, ceiling and range, per scale.
///
/// The multiplier table prints the 1.0 absolute mean and max beside the ratio,
/// because a ratio that widens with N and a 1.0 floor that grows with depth are
/// two separate findings and neither stands in for the other.
fn ordered_ops(cells: &[OrderedOpCell], arms: &[&str], fx: &mut Footnotes) -> String {
    if cells.is_empty() {
        return String::new();
    }
    let mut out = String::from("### 3 Ordered operations: floor, ceiling and range\n\n");
    for (corpus, group) in by_corpus(cells, |c| c.corpus.as_str()) {
        let mut lg = Legend::default();
        let _ = writeln!(out, "#### Corpus `{corpus}`\n");
        out.push_str(&header(&[
            "Scale",
            "Op",
            "Window",
            "Probes",
            "Arm",
            "Fair fetches / probe",
            "Pessimal fetches",
        ]));
        for c in &group {
            for arm in arms {
                out.push_str(&row(&[
                    int(c.scale),
                    format!("`{}`", c.op),
                    window(c.window),
                    int(c.probes),
                    format!("`{arm}`"),
                    outcome_mean(
                        c.fair.get(*arm),
                        c.probes,
                        fx,
                        &mut lg,
                        reason_for(&c.nulls, arm, "fair"),
                    ),
                    outcome_fetches(
                        c.pessimal.get(*arm),
                        fx,
                        &mut lg,
                        true,
                        reason_for(&c.nulls, arm, "pessimal"),
                    ),
                ]));
            }
        }
        out.push_str(
            "\nMultipliers beside the 1.0 absolute cost they divide, each op's scales \
ascending:\n\n",
        );
        out.push_str(&header(&[
            "Scale",
            "Op",
            "Window",
            "Fair 0.2 / 1.0",
            "Pessimal 0.2 / 1.0",
            "1.0 mean fetches",
            "1.0 max fetches",
        ]));
        for c in by_op_then_scale(&group) {
            out.push_str(&row(&[
                int(c.scale),
                format!("`{}`", c.op),
                window(c.window),
                c.fair_multiplier.map_or_else(
                    || fx.null(reason_for(&c.nulls, "mantaray-0.2", "fair_multiplier")),
                    f2,
                ),
                c.pessimal_multiplier.map_or_else(
                    || fx.null(reason_for(&c.nulls, "mantaray-0.2", "pessimal_multiplier")),
                    f2,
                ),
                c.native_abs_mean.map_or_else(|| fx.null(None), f2),
                c.native_abs_max.map_or_else(|| fx.null(None), int),
            ]));
        }
        out.push('\n');
        out.push_str(
            "Fair fetches are per-probe means over the cell's probes. The pessimal path is \
measured once per cell, because a whole-manifest walk costs the same whichever probe asked for \
it. Read every multiplier with N beside it: it widens because the 0.2 side grows, while the 1.0 \
absolute grows only with depth. 1.0 has no degraded path at all, so no whole-manifest walk was \
measured on the 1.0 arms: their pessimal cells repeat the fair measurement of the same op.\n\n",
        );
        out.push_str(
            "The 1.0 ceiling absolute is an upper bound, not a dedicated-seek cost. 1.0 has no \
dedicated ceiling primitive, so the arm rides `range(key, MAX)` and takes one item; that cursor \
fills a read-ahead window of speculative child fetches on its first poll, so the figure counts \
nodes a dedicated seek would never fetch. The number is kept and labelled, not corrected.\n\n",
        );
        out.push_str(&lg.block());
    }
    out
}

// ---- 4.1 the parallel cursor ---------------------------------------------

/// Whitepaper 4.1: the bounded-concurrency cursor's fetch rounds, and the
/// latency model those rounds drive.
///
/// Rounds are measured off the real cursor under a paused virtual clock, never
/// derived from the fetch count, so the speedup column is a measurement and not
/// an assumption about how a client would pipeline.
fn parallel_cursor(cells: &[ParallelCursorCell]) -> String {
    if cells.is_empty() {
        return String::new();
    }
    let mut out = String::from("### 4.1 Cursor fetch rounds under bounded concurrency\n\n");
    for (corpus, group) in by_corpus(cells, |c| c.corpus.as_str()) {
        let _ = writeln!(out, "#### Corpus `{corpus}`\n");
        out.push_str(&header(&[
            "Scale",
            "Op",
            "Window",
            "Keys",
            "Fetches",
            "Rounds",
            "Read-ahead",
        ]));
        for c in &group {
            out.push_str(&row(&[
                int(c.scale),
                format!("`{}`", c.op),
                window(c.window),
                int(c.keys_returned),
                int(c.fetch_count),
                int(c.rounds),
                int(u64::from(c.read_ahead)),
            ]));
        }
        let rtts = rtt_keys(&group, |c| c.by_rtt_ms.keys().collect());
        if !rtts.is_empty() {
            out.push_str("\nModelled latency per round trip:\n\n");
            out.push_str(&header(&[
                "Scale",
                "Op",
                "Window",
                "RTT ms",
                "Serial ms",
                "Concurrent ms",
                "Speedup",
            ]));
            for c in &group {
                for rtt in &rtts {
                    let Some(l) = c.by_rtt_ms.get(rtt) else {
                        continue;
                    };
                    out.push_str(&row(&[
                        int(c.scale),
                        format!("`{}`", c.op),
                        window(c.window),
                        rtt.clone(),
                        f2(l.serial_ms),
                        f2(l.concurrent_ms),
                        l.speedup.map_or_else(|| NULL.to_string(), f2),
                    ]));
                }
            }
        }
        out.push('\n');
        if let Some(model) = group.first().map(|c| c.model.as_str()) {
            let _ = writeln!(out, "> {model}\n");
        }
    }
    out
}

// ---- 4.3 pagination -------------------------------------------------------

/// Whitepaper 4.3: rank-directed pagination against the skip baseline and the
/// 0.2 resume-token walk to the same offset.
fn paginate(cells: &[PaginateCell], fx: &mut Footnotes) -> String {
    if cells.is_empty() {
        return String::new();
    }
    let mut out = String::from("### 4.3 Pagination to an offset\n\n");
    for (corpus, group) in by_corpus(cells, |c| c.corpus.as_str()) {
        let mut lg = Legend::default();
        let _ = writeln!(out, "#### Corpus `{corpus}`\n");
        out.push_str(&header(&[
            "Scale",
            "Offset",
            "Limit",
            "Keys",
            "1.0 paginate fetches",
            "1.0 skip-baseline fetches",
            "Skip / paginate",
            "0.2 resume-walk fetches",
        ]));
        for c in &group {
            let resume = match (c.v02_resume_fetch_count, c.v02_resume_capability.as_ref()) {
                (Some(n), Some(cap)) => format!("{}{}", int(n), lg.fair(cap)),
                (Some(n), None) => int(n),
                (None, _) => fx.null(c.v02_resume_null_reason.as_deref()),
            };
            out.push_str(&row(&[
                int(c.scale),
                int(c.offset),
                int(u64::from(c.limit)),
                int(c.keys_returned),
                int(c.paginate_fetch_count),
                int(c.skip_baseline_fetch_count),
                c.skip_over_paginate.map_or_else(|| fx.null(None), f2),
                resume,
            ]));
        }
        out.push('\n');
        out.push_str(
            "1.0 seeks the page by rank in O(depth). The skip baseline and the 0.2 resume-token \
walk both pay O(offset): 0.2 has no rank-directed seek, so a client carries the last path of each \
page into the next.\n\n",
        );
        out.push_str(&lg.block());
    }
    out
}

// ---- subtree serve --------------------------------------------------------

/// The subtree-reference handoff against the full cursor walk for the same
/// folder listing: what a gateway saves by serving a subtree reference.
fn subtree_serve(cells: &[SubtreeServeCell], fx: &mut Footnotes) -> String {
    if cells.is_empty() {
        return String::new();
    }
    let mut out = String::from("### Subtree serve: one reference against a full walk\n\n");
    for (corpus, group) in by_corpus(cells, |c| c.corpus.as_str()) {
        let _ = writeln!(out, "#### Corpus `{corpus}`\n");
        out.push_str(&header(&[
            "Scale",
            "Prefix",
            "Keys",
            "Handoff found",
            "Handoff fetches",
            "Cursor-walk fetches",
            "Walk / handoff",
        ]));
        for c in &group {
            out.push_str(&row(&[
                int(c.scale),
                format!("`{}`", c.prefix),
                int(c.keys_returned),
                if c.handoff_found { "yes" } else { "no" }.to_string(),
                int(c.handoff_fetch_count),
                int(c.cursor_walk_fetch_count),
                c.walk_over_handoff.map_or_else(
                    || {
                        fx.null((!c.handoff_found).then_some(
                            "no single chunk covers exactly this prefix, so there \
is no reference to hand off",
                        ))
                    },
                    f2,
                ),
            ]));
        }
        out.push('\n');
        out.push_str(
            "The handoff resolves the covering subtree reference in O(depth) and fetches nothing \
below the boundary; the walk drains the same listing from the database root. This is a 1.0-only \
lane: 0.2 has no subtree reference to hand off.\n\n",
        );
    }
    out
}

// ---- 4.2 V1Read against V1 ------------------------------------------------

/// Whitepaper 4.2: what the read-optimised format parameters buy and what they
/// cost, which is a build-side choice and so sits with the builder.
fn read_profile(cells: &[ReadProfileCell]) -> String {
    if cells.is_empty() {
        return String::new();
    }
    let mut out = String::from("### 4.2 V1Read against V1\n\n");
    for (corpus, group) in by_corpus(cells, |c| c.corpus.as_str()) {
        let _ = writeln!(out, "#### Corpus `{corpus}`\n");
        out.push_str(&header(&[
            "Scale",
            "Format",
            "Version byte",
            "Inline max",
            "Mean get depth",
            "Max get depth",
            "Chunks rewritten per single update",
        ]));
        for c in &group {
            for (name, side) in [("V1", &c.v1), ("V1Read", &c.v1read)] {
                out.push_str(&row(&[
                    int(c.scale),
                    format!("`{name}`"),
                    int(u64::from(side.version_byte)),
                    int(u64::from(side.inline_max)),
                    f2(side.tree_depth_mean),
                    int(side.tree_depth_max),
                    f2(side.single_update_chunks_mean),
                ]));
            }
        }
        out.push_str("\nFetches to drain each range window:\n\n");
        out.push_str(&header(&[
            "Scale",
            "Window",
            "V1 fetches",
            "V1Read fetches",
            "V1Read / V1",
        ]));
        for c in &group {
            for (w, v1) in &c.v1.range_fetch_by_window {
                out.push_str(&row(&[
                    int(c.scale),
                    w.clone(),
                    int(*v1),
                    c.v1read
                        .range_fetch_by_window
                        .get(w)
                        .map_or_else(|| NULL.to_string(), |v| int(*v)),
                    c.fetch_ratio_by_window
                        .get(w)
                        .map_or_else(|| NULL.to_string(), |v| f2(*v)),
                ]));
            }
        }
        out.push_str("\nThe read win beside the write cost it is paid for with:\n\n");
        out.push_str(&header(&[
            "Scale",
            "Depth ratio",
            "Single-update chunk delta",
            "Single-update ratio",
        ]));
        for c in &group {
            out.push_str(&row(&[
                int(c.scale),
                c.depth_ratio.map_or_else(|| NULL.to_string(), f2),
                f2(c.single_update_wa_delta),
                c.single_update_wa_ratio
                    .map_or_else(|| NULL.to_string(), f2),
            ]));
        }
        out.push('\n');
        out.push_str(
            "A ratio below 1.0 is the read win; the chunk delta is what one update pays for it. \
Both formats serve every operation natively, so no figure here is an emulation.\n\n",
        );
    }
    out
}

// ---- 6.1 build profile ----------------------------------------------------

/// Whitepaper 6.1: the build memory law, the node counts and the peak live
/// store bytes. The peak is the store's live bytes, never process RSS.
fn build_profile(cells: &[BuildProfileCell], arms: &[&str], fx: &mut Footnotes) -> String {
    if cells.is_empty() {
        return String::new();
    }
    let mut out = String::from("### 6.1 Build frontier and profile\n\n");
    for (corpus, group) in by_corpus(cells, |c| c.corpus.as_str()) {
        let _ = writeln!(out, "#### Corpus `{corpus}`\n");
        out.push_str(&header(&[
            "Scale",
            "Arm",
            "Frontier",
            "Frontier nodes",
            "Nodes written",
            "Nodes embedded",
            "Peak live store bytes",
            "Total put bytes",
        ]));
        for c in &group {
            for arm in arms {
                let mut cells = vec![int(c.scale), format!("`{arm}`")];
                match c.per_arm.get(*arm) {
                    Some(p) => {
                        let (class, count) = match p.frontier {
                            FrontierClass::Bounded { peak_open_nodes } => {
                                ("bounded", peak_open_nodes)
                            }
                            FrontierClass::WholeTrie { resident_nodes } => {
                                ("whole_trie", resident_nodes)
                            }
                        };
                        cells.push(format!("`{class}`"));
                        cells.push(int(count));
                        cells.push(int(p.nodes_written));
                        cells.push(p.nodes_embedded.map_or_else(
                            || fx.null(reason_for(&c.nulls, arm, "nodes_embedded")),
                            int,
                        ));
                        cells.push(int(p.peak_live_store_bytes));
                        cells.push(int(p.total_put_bytes));
                    }
                    None => {
                        let reason = reason_for(&c.nulls, arm, "per_arm");
                        for _ in 0..6 {
                            cells.push(fx.null(reason));
                        }
                    }
                }
                out.push_str(&row(&cells));
            }
        }
        out.push('\n');
        out.push_str(
            "`bounded` is an O(depth) frontier of simultaneously open nodes; `whole_trie` is an \
O(N) commit that materialises every node at once. Peak bytes are the store's live bytes, never \
process RSS.\n\n",
        );
    }
    out
}

// ---- 6.2 write amplification ----------------------------------------------

/// Whitepaper 6.2: write amplification against K.
///
/// K is the row axis and every swept K shares one table, so the K=1 row can
/// never be read outside the curve that explains it. A single-update figure in
/// isolation is the hazard this layout removes.
///
/// A sweep that holds one row carries no curve, so it publishes no figure. The
/// layout alone is not the guard: a corpus and scale small enough to drop every
/// K above the first would otherwise print the single-update number by itself,
/// which is the reading this section exists to prevent.
fn write_amp(cells: &[WriteAmpCell], arms: &[&str], fx: &mut Footnotes) -> String {
    if cells.is_empty() {
        return String::new();
    }
    let mut out = String::from("### 6.2 Write amplification against K\n\n");
    for (corpus, group) in by_corpus(cells, |c| c.corpus.as_str()) {
        for (scale, rows) in by_scale(&group) {
            let _ = writeln!(out, "#### Corpus `{corpus}`, scale {}\n", int(scale));
            if rows.len() < 2 {
                let ks = rows.iter().map(|c| int(c.k)).collect::<Vec<_>>().join(", ");
                let reason = format!(
                    "the K sweep holds one row at this scale (K = {ks}), so the figure is \
withheld: a write amplification read apart from its K curve is the hazard this table removes"
                );
                let _ = writeln!(out, "{}\n", fx.null(Some(reason.as_str())));
                continue;
            }
            let mut head = vec!["K".to_string()];
            for arm in arms {
                head.push(format!("`{arm}` batched"));
                head.push(format!("`{arm}` per edit"));
            }
            out.push_str(&header(
                &head.iter().map(String::as_str).collect::<Vec<_>>(),
            ));
            for c in rows {
                let mut line = vec![int(c.k)];
                for arm in arms {
                    line.push(c.wa_batched.get(*arm).map_or_else(
                        || fx.null(reason_for(&c.nulls, arm, "wa_batched")),
                        |v| f3(*v),
                    ));
                    line.push(c.wa_per_edit.get(*arm).map_or_else(
                        || fx.null(reason_for(&c.nulls, arm, "wa_per_edit")),
                        |v| f3(*v),
                    ));
                }
                out.push_str(&row(&line));
            }
            out.push('\n');
            out.push_str(
                "Chunks written per edit. `batched` is one changeset on 1.0 and one multi-op \
commit on 0.2; `per edit` is one commit or apply for every edit. The K=1 row is a point on this \
curve and is never printed apart from it.\n\n",
            );
            let _ = writeln!(
                out,
                "Capability legend: the 1.0 columns are native `Changeset` applies. The 0.2 \
columns are an emulation: {BATCH_HOW} ({BATCH_CLASS}).\n"
            );
        }
    }
    out
}

/// Group write-amp rows by scale, in first-seen order.
fn by_scale<'a>(cells: &[&'a WriteAmpCell]) -> Vec<(u64, Vec<&'a WriteAmpCell>)> {
    let mut out: Vec<(u64, Vec<&WriteAmpCell>)> = Vec::new();
    for c in cells {
        match out.iter_mut().find(|(s, _)| *s == c.scale) {
            Some((_, group)) => group.push(c),
            None => out.push((c.scale, vec![c])),
        }
    }
    out
}

// ---- the wall-time section ------------------------------------------------

/// The build wall-time table, under the verbatim cold-pass caveat.
///
/// The caveat prints once per corpus table, not once per document, so a reader
/// who lands on one corpus meets the reason nanoseconds are not the currency
/// before meeting the nanoseconds.
fn build_wall(cells: &[BuildWallCell], nulls: &[NullWithReason], fx: &mut Footnotes) -> String {
    if cells.is_empty() && nulls.is_empty() {
        return String::new();
    }
    let mut out = String::from("### Build wall-time (non-deterministic)\n\n");
    if !cells.is_empty() {
        for (corpus, group) in by_corpus(cells, |c| c.corpus.as_str()) {
            let _ = writeln!(out, "#### Corpus `{corpus}`\n");
            if let Some(caveat) = group.first().map(|c| c.caveat.as_str()) {
                let _ = writeln!(out, "> {caveat}\n");
            }
            out.push_str(&header(&[
                "Scale", "Arm", "Samples", "Mean ns", "Min ns", "Keys / s",
            ]));
            for c in &group {
                out.push_str(&row(&[
                    int(c.scale),
                    format!("`{}`", c.arm),
                    int(u64::from(c.samples)),
                    int(c.mean_ns),
                    int(c.min_ns),
                    f2(c.keys_per_sec),
                ]));
            }
            out.push('\n');
        }
    }
    if !nulls.is_empty() {
        out.push_str("Arms with no wall-time cell:\n\n");
        out.push_str(&header(&["Arm", "Field", "Wall-time"]));
        // One statement per distinct gap: the same policy repeated for every
        // corpus is one finding, and printing it four times says nothing more.
        let mut seen: Vec<(&str, &str, &str)> = Vec::new();
        for n in nulls {
            let key = (n.arm.as_str(), n.field.as_str(), n.reason.as_str());
            if seen.contains(&key) {
                continue;
            }
            seen.push(key);
            let cell = fx.null(Some(&n.reason));
            out.push_str(&row(&[
                format!("`{}`", n.arm),
                format!("`{}`", n.field),
                cell,
            ]));
        }
        out.push('\n');
    }
    out
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{Footnotes, Legend, NULL, f3, int, render};
    use crate::arm::{Capability, NullWithReason, OpCost, OpOutcome};
    use crate::results::{
        ArmMeta, Document, Meta, PaginateCell, PrefixListingCell, WriteAmpCell, generated_iso,
    };

    /// The body of one markdown section, from its heading to the next heading
    /// of the same or a higher level.
    fn section(md: &str, heading: &str) -> String {
        let level = heading.chars().take_while(|c| *c == '#').count();
        let mut out = String::new();
        let mut inside = false;
        for line in md.lines() {
            let hashes = line.chars().take_while(|c| *c == '#').count();
            if line.starts_with(heading) && line.trim_end() == heading.trim_end() {
                inside = true;
                continue;
            }
            if inside && hashes > 0 && hashes <= level {
                break;
            }
            if inside {
                out.push_str(line);
                out.push('\n');
            }
        }
        out
    }

    /// A document with one gap in every shape the renderer must print as `--`.
    fn doc() -> Document {
        let mut d = Document {
            meta: Meta {
                generated: generated_iso(Some(0)),
                git_branch: "bench/manifest-sim".to_string(),
                git_commit: "deadbeef".to_string(),
                harness_version: "5".to_string(),
                arms: ["mantaray-0.2", "ldb-v1"]
                    .into_iter()
                    .map(|l| ArmMeta {
                        label: l.to_string(),
                        package: "p".to_string(),
                        version: "0".to_string(),
                    })
                    .collect(),
                seed_master: "0x0".to_string(),
                rtt_ms_set: vec![25, 100],
                read_ahead: 8,
                scales: vec![1_000],
                max_mantaray_scale: 100,
                build_samples: 1,
                write_amp_ks: vec![1, 10],
                corpora: vec!["kiwix".to_string()],
                range_windows: vec![0.01],
                paginate_offsets: vec![0],
                paginate_limit: 20,
                chunk_body_size: 4_096,
                caveats: vec!["fetches are the currency".to_string()],
            },
            deterministic: crate::results::DeterministicSection::default(),
            wall_time: crate::results::WallTimeSection::default(),
        };

        let capped = "mantaray 0.2 skipped by policy above 100".to_string();
        let mut fair = BTreeMap::new();
        fair.insert(
            "ldb-v1".to_string(),
            OpOutcome {
                capability: Capability::native(),
                cost: Some(OpCost {
                    fetches: 7,
                    puts: 0,
                    keys_returned: 3,
                }),
            },
        );
        let mut pessimal = BTreeMap::new();
        pessimal.insert(
            "ldb-v1".to_string(),
            OpOutcome {
                capability: Capability::native(),
                cost: Some(OpCost {
                    fetches: 7,
                    puts: 0,
                    keys_returned: 3,
                }),
            },
        );
        d.deterministic.prefix_listing.push(PrefixListingCell {
            corpus: "kiwix".to_string(),
            scale: 1_000,
            prefix: "a/".to_string(),
            keys_returned: 3,
            fair,
            pessimal,
            fair_multiplier: None,
            pessimal_multiplier: None,
            nulls: vec![
                NullWithReason {
                    arm: "mantaray-0.2".to_string(),
                    field: "fair".to_string(),
                    reason: capped.clone(),
                },
                NullWithReason {
                    arm: "mantaray-0.2".to_string(),
                    field: "fair_multiplier".to_string(),
                    reason: capped.clone(),
                },
            ],
        });
        for k in [1u64, 10] {
            let mut batched = BTreeMap::new();
            batched.insert("ldb-v1".to_string(), 1.5);
            d.deterministic.write_amp.push(WriteAmpCell {
                corpus: "kiwix".to_string(),
                scale: 1_000,
                k,
                wa_batched: batched,
                wa_per_edit: BTreeMap::new(),
                nulls: vec![NullWithReason {
                    arm: "mantaray-0.2".to_string(),
                    field: "wa_batched".to_string(),
                    reason: capped.clone(),
                }],
            });
        }
        d.deterministic.paginate.push(PaginateCell {
            corpus: "kiwix".to_string(),
            scale: 1_000,
            offset: 0,
            limit: 20,
            keys_returned: 20,
            paginate_fetch_count: 4,
            skip_baseline_fetch_count: 4,
            skip_over_paginate: Some(1.0),
            v02_resume_fetch_count: None,
            v02_resume_capability: None,
            v02_resume_null_reason: Some(capped),
        });
        d
    }

    /// A gap prints as `--` with a footnote marker, and every marker resolves
    /// to a definition carrying the recorded reason: the renderer never drops a
    /// null reason and never fills a gap with a number.
    #[test]
    fn nulls_render_as_a_dash_with_a_footnoted_reason() {
        let md = render(&doc());
        assert!(md.contains("--[^1]"), "no footnoted null in:\n{md}");
        assert!(
            md.contains("[^1]: mantaray 0.2 skipped by policy above 100"),
            "the reason is not defined in:\n{md}"
        );
        // One reason, one footnote, however many cells cite it.
        assert!(!md.contains("[^2]:"), "the reason pool did not deduplicate");
        // The capped arm is never given a zero, an average or a borrowed
        // figure: every one of its cells is the dash.
        let capped_cells: Vec<&str> = md
            .lines()
            .filter(|l| l.contains("`mantaray-0.2`") && l.starts_with('|'))
            .collect();
        assert!(!capped_cells.is_empty(), "no 0.2 rows in:\n{md}");
        for line in capped_cells {
            assert!(
                !line.contains("0.00") && !line.contains("0.000"),
                "a 0.2 gap was filled with a zero: {line}"
            );
        }
        assert!(
            md.contains("| 0.2 resume-walk fetches |"),
            "the 4.3 resume column is missing"
        );
    }

    /// A footnote is only ever the reason recorded against the exact field
    /// printed. Borrowing another gap the same arm filed would attach a
    /// confident explanation to the wrong cell.
    #[test]
    fn a_footnote_never_borrows_an_unrelated_gap_the_same_arm_filed() {
        let mut d = doc();
        let cell = d
            .deterministic
            .prefix_listing
            .first_mut()
            .expect("the listing cell");
        // A gap filed against a different field of the same arm.
        cell.nulls = vec![NullWithReason {
            arm: "mantaray-0.2".to_string(),
            field: "slot_utilisation".to_string(),
            reason: "an unrelated storage gap".to_string(),
        }];
        let md = render(&d);
        assert!(
            !md.contains("an unrelated storage gap"),
            "a listing cell borrowed a storage reason:\n{md}"
        );
        assert!(md.contains(NULL), "the gap stopped printing as a dash");
    }

    /// The two audiences are separated and no table sits in both. A reader of
    /// the user-experience group must not meet a build-side figure there.
    #[test]
    fn the_document_splits_the_user_and_developer_audiences() {
        use crate::arm::FrontierClass;
        use crate::results::{ArmBuildProfile, BuildProfileCell, BuildWallCell};

        let mut d = doc();
        d.deterministic.capability_matrix = crate::matrix::capability_matrix();
        let mut per_arm = BTreeMap::new();
        per_arm.insert(
            "ldb-v1".to_string(),
            ArmBuildProfile {
                frontier: FrontierClass::Bounded { peak_open_nodes: 4 },
                nodes_written: 39,
                nodes_embedded: Some(11),
                peak_live_store_bytes: 1_024,
                total_put_bytes: 2_048,
            },
        );
        d.deterministic.build_profile.push(BuildProfileCell {
            corpus: "kiwix".to_string(),
            scale: 1_000,
            per_arm,
            nulls: vec![NullWithReason {
                arm: "mantaray-0.2".to_string(),
                field: "per_arm".to_string(),
                reason: "mantaray 0.2 skipped by policy above 100".to_string(),
            }],
        });
        d.wall_time.build_wall.push(BuildWallCell {
            corpus: "kiwix".to_string(),
            scale: 1_000,
            arm: "ldb-v1".to_string(),
            samples: 1,
            mean_ns: 12_345,
            min_ns: 12_345,
            keys_per_sec: 81_000.0,
            caveat: crate::build_time::BUILD_CAVEAT.to_string(),
        });
        let md = render(&d);
        let ux = section(&md, "## User experience");
        let dx = section(&md, "## Developer experience");
        assert!(!ux.is_empty(), "no user-experience group in:\n{md}");
        assert!(!dx.is_empty(), "no developer-experience group in:\n{md}");
        assert!(
            md.find("## User experience") < md.find("## Developer experience"),
            "the audiences are out of order"
        );
        for heading in ["### Capability matrix", "### 2.3", "### 4.3"] {
            assert!(ux.contains(heading), "{heading} is not a user-side table");
            assert!(!dx.contains(heading), "{heading} leaked into the DX group");
        }
        for heading in ["### 6.1", "### 6.2", "### Build wall-time"] {
            assert!(
                dx.contains(heading),
                "{heading} is not a builder-side table"
            );
            assert!(!ux.contains(heading), "{heading} leaked into the UX group");
        }
    }

    /// The 6.2 section of a rendered document.
    fn sweep_section(md: &str) -> String {
        section(md, "### 6.2 Write amplification against K")
    }

    /// The K=1 write-amplification row never stands alone: it is a row of the
    /// same table as the rest of the sweep, and write amplification is printed
    /// in that one section and nowhere else (whitepaper hazard 1).
    #[test]
    fn the_k1_row_stays_inside_the_sweep_table() {
        let md = render(&doc());
        let table = sweep_section(&md);
        let ks: Vec<&str> = table
            .lines()
            .filter(|l| l.starts_with("| 1 |") || l.starts_with("| 10 |"))
            .collect();
        assert_eq!(ks.len(), 2, "K=1 and K=10 must share one table: {table}");

        // Grep the whole document: only 6.2 carries a write-amplification
        // column, so no single-update figure can be met away from its curve.
        let elsewhere: Vec<&str> = md
            .split("\n### ")
            .filter(|s| !s.starts_with("6.2") && s.contains("` batched"))
            .collect();
        assert!(
            elsewhere.is_empty(),
            "write amplification printed outside 6.2: {elsewhere:?}"
        );
    }

    /// A sweep that collapsed to one row publishes no figure at all: the
    /// layout alone cannot keep K=1 inside a curve that has no other point,
    /// so the renderer withholds the number and states why.
    #[test]
    fn a_one_row_sweep_publishes_no_write_amp_figure() {
        let mut d = doc();
        d.deterministic.write_amp.truncate(1);
        let value = *d.deterministic.write_amp[0]
            .wa_batched
            .get("ldb-v1")
            .expect("the K=1 row carries a 1.0 figure");
        let md = render(&d);
        let table = sweep_section(&md);
        assert!(
            !table.contains(&f3(value)),
            "a lone K=1 figure was published: {table}"
        );
        assert!(
            !table.lines().any(|l| l.starts_with("| 1 |")),
            "a lone K=1 row was printed: {table}"
        );
        assert!(table.contains(NULL), "the withheld figure prints no dash");
        assert!(
            md.contains("apart from its K curve"),
            "the reason is not footnoted: {md}"
        );
    }

    /// Red-team check 11: the rendered matrix states the current crates, and
    /// every divergence from the whitepaper is named where the matrix is read,
    /// as an explicit was-and-is pair.
    #[test]
    fn the_capability_matrix_states_the_in_tree_crate_and_its_divergence() {
        let mut d = doc();
        d.deterministic.capability_matrix = crate::matrix::capability_matrix();
        let md = render(&d);
        let section = section(&md, "### Capability matrix");

        // The in-tree classifications, not the whitepaper's historical ones.
        for how in [
            crate::arm_mantaray::CEILING_HOW,
            crate::arm_mantaray::FULL_ITER_HOW,
            crate::arm_mantaray::PREFIX_HOW,
            crate::arm_mantaray::RANGE_HOW,
            crate::arm_mantaray::BATCH_HOW,
        ] {
            assert!(section.contains(how), "the matrix does not print {how}");
        }
        // No table row is left at its whitepaper class. The was-and-is table
        // names the old classifications to contrast them, so only the matrix
        // rows are checked.
        for line in section.lines().filter(|l| l.starts_with("| `")) {
            assert!(
                !line.contains("unordered") && !line.contains("unsupported"),
                "a matrix row still carries a whitepaper-era class: {line}"
            );
        }
        // The divergence is stated in the rendered notes as was-and-is pairs.
        assert!(section.contains(super::DIVERGENCE_NOTE));
        for d in super::DIVERGENCES {
            assert!(section.contains(d.row), "the notes drop {}", d.row);
            assert!(section.contains(d.was), "the notes drop what {} was", d.row);
            assert!(section.contains(d.is), "the notes drop what {} is", d.row);
        }
        assert!(
            section.contains("| Whitepaper row | Was | Is |"),
            "the divergences are prose, not a was-and-is table"
        );
    }

    /// The 1.0 ceiling rides the range cursor, so the matrix says so where the
    /// number is read: the read-ahead bound is named, not implied.
    #[test]
    fn the_ldb_ceiling_carries_its_read_ahead_bound() {
        let mut d = doc();
        d.deterministic.capability_matrix = crate::matrix::capability_matrix();
        let md = render(&d);
        let section = section(&md, "### Capability matrix");
        let line = section
            .lines()
            .find(|l| l.starts_with("| `ceiling`"))
            .unwrap_or_default();
        assert!(
            line.contains(crate::arm_ldb::CEILING_HOW),
            "the 1.0 ceiling reads as a plain native seek: {line}"
        );
        assert!(
            line.contains("READ_AHEAD"),
            "the ceiling row hides the read-ahead bound: {line}"
        );
    }

    /// Grouping is by corpus and nothing crosses corpora or averages them.
    #[test]
    fn every_table_is_keyed_by_corpus() {
        let md = render(&doc());
        assert!(md.contains("#### Corpus `kiwix`"));
        assert!(!md.to_lowercase().contains("all corpora"));
        assert!(!md.to_lowercase().contains("average across"));
    }

    /// The footnote pool numbers reasons in first-seen order and reuses a
    /// number for a repeated reason.
    #[test]
    fn the_footnote_pool_deduplicates_by_reason() {
        let mut fx = Footnotes::default();
        assert_eq!(fx.null(Some("a")), "--[^1]");
        assert_eq!(fx.null(Some("b")), "--[^2]");
        assert_eq!(fx.null(Some("a")), "--[^1]");
        assert_eq!(fx.null(None), "--");
        let defs = fx.definitions();
        assert!(defs.contains("[^1]: a"));
        assert!(defs.contains("[^2]: b"));
    }

    /// A native figure is unmarked, an emulation takes a letter that resolves
    /// in the legend, and a native cell in a pessimal column is labelled as the
    /// repeat it is rather than as a measured whole walk.
    #[test]
    fn the_legend_marks_emulations_and_the_absent_fallback() {
        let mut lg = Legend::default();
        assert_eq!(lg.fair(&Capability::native()), "");
        let m = lg.fair(&Capability::emulated("a walk", "O(N)"));
        assert_eq!(m, " [a]");
        // The same emulation reuses its letter; a different one takes the next.
        assert_eq!(lg.fair(&Capability::emulated("a walk", "O(N)")), " [a]");
        assert_eq!(lg.fair(&Capability::emulated("a seek", "O(1)")), " [b]");
        assert_eq!(lg.pessimal(&Capability::native()), " [c]");
        let block = lg.block();
        assert!(block.contains("`[a]` emulated: a walk (O(N))"));
        assert!(block.contains("`[b]` emulated: a seek (O(1))"));
        assert!(block.contains(crate::arm_ldb::NO_FALLBACK));
        assert!(block.contains("unmarked figure is a native primitive"));
    }

    /// Every emulated figure in a rendered table carries its label, and a 1.0
    /// pessimal cell never reads as a measured whole-manifest walk.
    #[test]
    fn rendered_tables_label_every_emulated_and_repeated_figure() {
        let mut d = doc();
        let cell = d
            .deterministic
            .prefix_listing
            .first_mut()
            .expect("the listing cell");
        cell.fair.insert(
            "mantaray-0.2".to_string(),
            OpOutcome {
                capability: Capability::emulated(
                    crate::arm_mantaray::PREFIX_HOW,
                    crate::arm_mantaray::PREFIX_CLASS,
                ),
                cost: Some(OpCost {
                    fetches: 91,
                    puts: 0,
                    keys_returned: 3,
                }),
            },
        );
        cell.nulls.clear();
        d.deterministic.paginate[0].v02_resume_fetch_count = Some(42);
        d.deterministic.paginate[0].v02_resume_capability = Some(Capability::emulated(
            crate::arm_mantaray::RESUME_HOW,
            crate::arm_mantaray::RESUME_CLASS,
        ));
        d.deterministic.paginate[0].v02_resume_null_reason = None;
        let md = render(&d);

        let listing = section(&md, "### 2.3 Prefix listing: fair and pessimal");
        let emulated = listing
            .lines()
            .find(|l| l.contains("`mantaray-0.2`"))
            .unwrap_or_default();
        assert!(
            emulated.contains("91 ["),
            "the 0.2 listing figure carries no marker: {emulated}"
        );
        assert!(
            listing.contains(crate::arm_mantaray::PREFIX_HOW),
            "the legend does not resolve the 0.2 marker:\n{listing}"
        );
        // The 1.0 pessimal cell is the native cost repeated, and says so, in
        // the marker and in the prose the whole-walk sentence sits in.
        assert!(
            listing.contains(crate::arm_ldb::NO_FALLBACK),
            "a 1.0 pessimal cell reads as a measured whole walk:\n{listing}"
        );
        assert!(
            listing.contains("no whole-manifest walk was measured on the 1.0 arms"),
            "the whole-walk prose still speaks for the 1.0 arms:\n{listing}"
        );

        // The 4.3 resume column carries its emulation label too.
        let pages = section(&md, "### 4.3 Pagination to an offset");
        assert!(
            pages.contains(crate::arm_mantaray::RESUME_HOW),
            "the resume walk lost its emulation label:\n{pages}"
        );
        assert!(
            pages.lines().any(|l| l.contains("42 [")),
            "the resume figure carries no marker:\n{pages}"
        );
    }

    /// Thousands separators land on the right boundaries.
    #[test]
    fn integers_group_in_threes() {
        assert_eq!(int(0), "0");
        assert_eq!(int(999), "999");
        assert_eq!(int(1_000), "1,000");
        assert_eq!(int(1_000_000), "1,000,000");
    }

    /// The scale both determinism gates measure at.
    const DET_SCALE: u64 = 1_000;

    /// The read lanes of the deterministic section at one `(corpus, scale)`:
    /// the capability matrix, storage, hops, ordered ops, prefix listing and
    /// the v4 cells.
    fn read_lanes(
        corpus: crate::corpus::Corpus,
        scale: u64,
    ) -> crate::results::DeterministicSection {
        use crate::{matrix, perf};
        let keys = crate::corpus::generate(corpus, scale as usize);
        let mut det = crate::results::DeterministicSection {
            capability_matrix: matrix::capability_matrix(),
            ..crate::results::DeterministicSection::default()
        };
        det.parallel_cursor
            .extend(perf::parallel_cursor_cells(corpus, scale, &keys).expect("parallel cursor"));
        det.v1read
            .push(perf::read_profile_cell(corpus, scale, &keys).expect("read profile"));
        det.paginate
            .extend(perf::paginate_cells(corpus, scale, &keys, scale).expect("paginate"));
        det.subtree_serve
            .extend(perf::subtree_serve_cell(corpus, scale, &keys).expect("subtree serve"));
        let (storage, get_hops) =
            crate::storage_hops::storage_and_hops(corpus, scale, &keys, scale);
        let (ordered_ops, prefix_listing) =
            crate::ordered_prefix::ordered_and_prefix(corpus, scale, &keys, scale);
        det.storage = storage;
        det.get_hops = get_hops;
        det.ordered_ops = ordered_ops;
        det.prefix_listing = prefix_listing;
        det
    }

    /// The K list the write-lane determinism gate sweeps.
    ///
    /// The lane runs the same code for every K, so reproducibility is settled
    /// by the shape of the sweep and not by its top row. The top row costs K
    /// open-put-commit cycles on every arm, which is minutes of unoptimised
    /// build time twice over; Unit D's own tests price the full curve.
    const DET_KS: [u64; 3] = [1, 10, 100];

    /// The write lanes of the deterministic section: the write-amplification
    /// sweep and the build profile. They are split from the read lanes so one
    /// K sweep does not push a single test near the hang gate.
    fn write_lanes(
        corpus: crate::corpus::Corpus,
        scale: u64,
    ) -> crate::results::DeterministicSection {
        let keys = crate::corpus::generate(corpus, scale as usize);
        let (write_amp, build_profile) =
            crate::writeamp_build::sweep(corpus, scale, &keys, scale, &DET_KS);
        crate::results::DeterministicSection {
            write_amp,
            build_profile,
            ..crate::results::DeterministicSection::default()
        }
    }

    /// Serialize a section twice and assert byte-identity, a non-trivial size,
    /// and that nothing the wall-time lane produces is inside it.
    ///
    /// The last assertion is the section boundary: wall-clock varies between
    /// runs by design, so a nanosecond inside the deterministic section would
    /// break reproducibility silently.
    fn assert_reproducible(lane: &str, make: impl Fn() -> crate::results::DeterministicSection) {
        let once = serde_json::to_string(&make()).expect("serialize first run");
        let twice = serde_json::to_string(&make()).expect("serialize second run");
        assert_eq!(once, twice, "{lane} is not reproducible");
        assert!(once.len() > 1_000, "{lane} is suspiciously small");
        for field in ["mean_ns", "min_ns", "keys_per_sec"] {
            assert!(
                !once.contains(field),
                "{lane}: the wall-time field {field} leaked in"
            );
        }
        assert!(
            !once.contains(crate::build_time::BUILD_CAVEAT),
            "{lane}: the wall-time caveat leaked in"
        );
    }

    /// The determinism the checked-in figures rest on: the read lanes of the
    /// deterministic section at 1e3 serialize byte-identically across two runs.
    #[test]
    fn the_deterministic_section_is_byte_identical_across_runs() {
        assert_reproducible("the deterministic read lanes", || {
            read_lanes(crate::corpus::Corpus::Kiwix, DET_SCALE)
        });
    }

    /// The same gate over the write lanes, which the read-lane test omits only
    /// for runtime: together the two cover every field of the section.
    #[test]
    fn the_write_lanes_are_byte_identical_across_runs() {
        assert_reproducible("the deterministic write lanes", || {
            write_lanes(crate::corpus::Corpus::Kiwix, DET_SCALE)
        });
    }

    /// Every measured lane of the deterministic section reaches the markdown.
    ///
    /// The parallel cursor, the V1Read profile and the subtree handoff were
    /// measured and serialized but never rendered, so a reader of the tables
    /// could not see them at all. A populated lane with no table is a dropped
    /// finding, not a formatting choice.
    #[test]
    fn every_measured_lane_reaches_the_markdown() {
        let det = read_lanes(crate::corpus::Corpus::Kiwix, DET_SCALE);
        assert!(!det.parallel_cursor.is_empty(), "no cursor cells measured");
        assert!(!det.v1read.is_empty(), "no read-profile cells measured");
        assert!(!det.subtree_serve.is_empty(), "no subtree cells measured");
        let mut d = doc();
        d.deterministic = det;
        let md = render(&d);
        for heading in [
            "### 4.1 Cursor fetch rounds under bounded concurrency",
            "### 4.2 V1Read against V1",
            "### 4.3 Pagination to an offset",
            "### Subtree serve: one reference against a full walk",
        ] {
            assert!(md.contains(heading), "the {heading} lane was dropped");
            let rows = section(&md, heading)
                .lines()
                .filter(|l| l.starts_with("| "))
                .count();
            assert!(rows >= 2, "{heading} rendered a heading with no rows");
        }
    }

    /// The section-3 multiplier table reads each op's N-series ascending, so
    /// "the ratio widens with N" can be read off the rows rather than
    /// reconstructed from a scale-major block.
    #[test]
    fn the_multiplier_rows_read_ascending_in_n_per_op() {
        use crate::results::OrderedOpCell;
        let cell = |scale: u64, op: &str| OrderedOpCell {
            corpus: "kiwix".to_string(),
            scale,
            op: op.to_string(),
            window: None,
            probes: 2,
            fair: BTreeMap::new(),
            pessimal: BTreeMap::new(),
            fair_multiplier: Some(scale as f64),
            pessimal_multiplier: None,
            native_abs_mean: None,
            native_abs_max: None,
            nulls: Vec::new(),
        };
        let mut d = doc();
        // Run order is scale-major: 1e3 floor, 1e3 ceiling, 1e4 floor, ...
        d.deterministic.ordered_ops = vec![
            cell(1_000, "floor"),
            cell(1_000, "ceiling"),
            cell(10_000, "floor"),
            cell(10_000, "ceiling"),
        ];
        let md = render(&d);
        let ordered = section(&md, "### 3 Ordered operations: floor, ceiling and range");
        let mult: Vec<&str> = ordered
            .split("Multipliers beside")
            .nth(1)
            .unwrap_or_default()
            .lines()
            .filter(|l| l.starts_with("| 1"))
            .collect();
        assert_eq!(mult.len(), 4, "the multiplier table lost rows: {ordered}");
        let ops: Vec<&str> = mult
            .iter()
            .map(|l| {
                if l.contains("`floor`") {
                    "floor"
                } else {
                    "ceiling"
                }
            })
            .collect();
        assert_eq!(
            ops,
            ["floor", "floor", "ceiling", "ceiling"],
            "the ops are still interleaved by scale: {mult:?}"
        );
        assert!(
            mult.first().unwrap_or(&"").starts_with("| 1,000 |")
                && mult.get(1).unwrap_or(&"").starts_with("| 10,000 |"),
            "the N-series does not ascend: {mult:?}"
        );
    }

    /// The document split holds: a wall-time cell lives in its own section and
    /// nothing it carries appears in the serialized deterministic section.
    #[test]
    fn the_wall_time_section_stays_out_of_the_deterministic_section() {
        use crate::results::BuildWallCell;
        let mut d = doc();
        d.wall_time.build_wall.push(BuildWallCell {
            corpus: "kiwix".to_string(),
            scale: 1_000,
            arm: "ldb-v1".to_string(),
            samples: 1,
            mean_ns: 12_345,
            min_ns: 12_345,
            keys_per_sec: 81_000.0,
            caveat: crate::build_time::BUILD_CAVEAT.to_string(),
        });
        let det = serde_json::to_string(&d.deterministic).expect("serialize deterministic");
        for marker in ["mean_ns", "min_ns", "keys_per_sec", "12345"] {
            assert!(!det.contains(marker), "{marker} leaked into the section");
        }
        let wall = serde_json::to_string(&d.wall_time).expect("serialize wall time");
        assert!(wall.contains("mean_ns"), "the wall-time cell went missing");
    }

    /// The cold-pass caveat prints inside every corpus table, so a reader who
    /// lands on one corpus meets it before the nanoseconds.
    #[test]
    fn the_wall_time_caveat_prints_once_per_corpus_table() {
        use crate::results::BuildWallCell;
        let mut d = doc();
        for corpus in ["kiwix", "uniform"] {
            d.wall_time.build_wall.push(BuildWallCell {
                corpus: corpus.to_string(),
                scale: 1_000,
                arm: "ldb-v1".to_string(),
                samples: 1,
                mean_ns: 12_345,
                min_ns: 12_345,
                keys_per_sec: 81_000.0,
                caveat: crate::build_time::BUILD_CAVEAT.to_string(),
            });
        }
        let md = render(&d);
        let printed = md.matches(crate::build_time::BUILD_CAVEAT).count();
        assert_eq!(
            printed, 2,
            "the caveat printed {printed} times, want one per corpus:\n{md}"
        );
    }
}
