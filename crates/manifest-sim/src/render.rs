//! The markdown renderer: one [`Document`] in, GitHub-markdown tables out.
//!
//! The renderer reads a finished run back and prints it. It never measures,
//! never fills a gap, never averages across corpora and never extrapolates a
//! capped arm from a smaller scale. A missing figure prints as `--` and its
//! reason becomes a footnote, so a gap stays legible as a gap.
//!
//! The table shapes mirror the whitepaper: the capability matrix, 2.1 storage,
//! 2.2 get hops with the RTT columns, 2.3 listing fair and pessimal, section 3
//! ordered multipliers, 6.1 the build frontier, 6.2 write amplification against
//! K, 4.3 pagination with the 0.2 resume column, and the build wall-time table
//! under its caveat.
//!
//! Two rules ride the layout. Every table is keyed by corpus, because a figure
//! from one corpus never speaks for another. Every multiplier prints beside the
//! 1.0 absolute cost it divides, because a widening ratio and a growing floor
//! are different findings.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use crate::arm::{Capability, FrontierClass, NullWithReason, OpOutcome};
use crate::results::{
    BuildProfileCell, BuildWallCell, CapabilityRow, Document, GetHopsCell, OrderedOpCell,
    PaginateCell, PrefixListingCell, StorageCell, WriteAmpCell,
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

/// The recorded reason for one arm's gap in one field, preferring an exact
/// field match and falling back to any gap the same arm recorded.
fn reason_for<'a>(nulls: &'a [NullWithReason], arm: &str, field: &str) -> Option<&'a str> {
    nulls
        .iter()
        .find(|n| n.arm == arm && n.field == field)
        .or_else(|| nulls.iter().find(|n| n.arm == arm))
        .map(|n| n.reason.as_str())
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

/// The fetch count of an outcome, or a null carrying the outcome's own reason.
fn outcome_fetches(o: Option<&OpOutcome>, fx: &mut Footnotes, fallback: Option<&str>) -> String {
    match o {
        Some(OpOutcome {
            cost: Some(cost), ..
        }) => int(cost.fetches),
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
    fallback: Option<&str>,
) -> String {
    match o {
        Some(OpOutcome {
            cost: Some(cost), ..
        }) if probes > 0 => f2(cost.fetches as f64 / probes as f64),
        other => outcome_fetches(other, fx, fallback),
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

// ---- the document --------------------------------------------------------

/// Render a whole result document as GitHub markdown.
#[must_use]
pub fn render(doc: &Document) -> String {
    let arms: Vec<&str> = doc.meta.arms.iter().map(|a| a.label.as_str()).collect();
    let mut fx = Footnotes::default();
    let mut out = String::new();

    out.push_str("# Two-arm manifest benchmark: mantaray 0.2 against mantaray 1.0\n\n");
    out.push_str(&provenance(doc));
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
    out.push_str(&build_profile(
        &doc.deterministic.build_profile,
        &arms,
        &mut fx,
    ));
    out.push_str(&write_amp(&doc.deterministic.write_amp, &arms, &mut fx));
    out.push_str(&paginate(&doc.deterministic.paginate, &mut fx));
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

/// The measured capability matrix, one row per operation.
fn capability_matrix(rows: &[CapabilityRow], arms: &[&str], fx: &mut Footnotes) -> String {
    if rows.is_empty() {
        return String::new();
    }
    let mut out = String::from("## Capability matrix\n\n");
    out.push_str(
        "Re-derived from the crates in tree, not from the whitepaper's historical \
classifications: the current 0.2 cursor is pruned, ordered and resumable.\n\n",
    );
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
    out
}

// ---- 2.1 storage ----------------------------------------------------------

/// Whitepaper 2.1: resident chunks, slot utilisation and embed fraction, plus
/// the separate chunk-ratio table.
fn storage(cells: &[StorageCell], arms: &[&str], fx: &mut Footnotes) -> String {
    if cells.is_empty() {
        return String::new();
    }
    let mut out = String::from("## 2.1 Storage, slot utilisation and embedding\n\n");
    for (corpus, group) in by_corpus(cells, |c| c.corpus.as_str()) {
        let _ = writeln!(out, "### Corpus `{corpus}`\n");
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
        out.push('\n');
    }
    out
}

// ---- 2.2 get hops ---------------------------------------------------------

/// Whitepaper 2.2: the get-hop distribution and the modelled RTT columns.
fn get_hops(cells: &[GetHopsCell], arms: &[&str], rtts: &[u32], fx: &mut Footnotes) -> String {
    if cells.is_empty() {
        return String::new();
    }
    let mut out = String::from("## 2.2 Get hops and modelled latency\n\n");
    out.push_str(
        "Hops are the measured currency. Every millisecond column is that measured mean times a \
stated RTT under a sequential model with no pipelining and no caching; it is illustrative, not a \
timing.\n\n",
    );
    for (corpus, group) in by_corpus(cells, |c| c.corpus.as_str()) {
        let _ = writeln!(out, "### Corpus `{corpus}`\n");
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
        out.push('\n');
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
    let mut out = String::from("## 2.3 Prefix listing: fair and pessimal\n\n");
    out.push_str(
        "The fair column is each arm's best public-API path: a prefix scan on 1.0, a pruned \
prefix walk on 0.2. The pessimal column is the whole-manifest walk a client without the pruned \
path must run.\n\n",
    );
    for (corpus, group) in by_corpus(cells, |c| c.corpus.as_str()) {
        let _ = writeln!(out, "### Corpus `{corpus}`\n");
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
                    outcome_fetches(c.fair.get(*arm), fx, reason_for(&c.nulls, arm, "fair")),
                    outcome_fetches(
                        c.pessimal.get(*arm),
                        fx,
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
                c.fair_multiplier
                    .map_or_else(|| fx.null(reason_for(&c.nulls, "mantaray-0.2", "fair")), f2),
                c.pessimal_multiplier.map_or_else(
                    || fx.null(reason_for(&c.nulls, "mantaray-0.2", "pessimal")),
                    f2,
                ),
            ]));
        }
        out.push('\n');
    }
    out
}

// ---- section 3 ordered ops ------------------------------------------------

/// Whitepaper section 3: floor, ceiling and range, per scale.
///
/// The multiplier table prints the 1.0 absolute mean and max beside the ratio,
/// because a ratio that widens with N and a 1.0 floor that grows with depth are
/// two separate findings and neither stands in for the other.
fn ordered_ops(cells: &[OrderedOpCell], arms: &[&str], fx: &mut Footnotes) -> String {
    if cells.is_empty() {
        return String::new();
    }
    let mut out = String::from("## 3 Ordered operations: floor, ceiling and range\n\n");
    out.push_str(
        "Fair and pessimal fetches are per-probe means. The pessimal path is measured once per \
cell, because a whole-manifest walk costs the same whichever probe asked for it.\n\n",
    );
    for (corpus, group) in by_corpus(cells, |c| c.corpus.as_str()) {
        let _ = writeln!(out, "### Corpus `{corpus}`\n");
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
                        reason_for(&c.nulls, arm, "fair"),
                    ),
                    outcome_fetches(
                        c.pessimal.get(*arm),
                        fx,
                        reason_for(&c.nulls, arm, "pessimal"),
                    ),
                ]));
            }
        }
        out.push_str("\nMultipliers beside the 1.0 absolute cost they divide:\n\n");
        out.push_str(&header(&[
            "Scale",
            "Op",
            "Window",
            "Fair 0.2 / 1.0",
            "Pessimal 0.2 / 1.0",
            "1.0 mean fetches",
            "1.0 max fetches",
        ]));
        for c in &group {
            out.push_str(&row(&[
                int(c.scale),
                format!("`{}`", c.op),
                window(c.window),
                c.fair_multiplier
                    .map_or_else(|| fx.null(reason_for(&c.nulls, "mantaray-0.2", "fair")), f2),
                c.pessimal_multiplier.map_or_else(
                    || fx.null(reason_for(&c.nulls, "mantaray-0.2", "pessimal")),
                    f2,
                ),
                c.native_abs_mean.map_or_else(|| fx.null(None), f2),
                c.native_abs_max.map_or_else(|| fx.null(None), int),
            ]));
        }
        out.push('\n');
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
    let mut out = String::from("## 6.1 Build frontier and profile\n\n");
    out.push_str(
        "`bounded` is an O(depth) frontier of simultaneously open nodes; `whole_trie` is an O(N) \
commit that materialises every node at once. Peak bytes are the store's live bytes, never process \
RSS.\n\n",
    );
    for (corpus, group) in by_corpus(cells, |c| c.corpus.as_str()) {
        let _ = writeln!(out, "### Corpus `{corpus}`\n");
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
    }
    out
}

// ---- 6.2 write amplification ----------------------------------------------

/// Whitepaper 6.2: write amplification against K.
///
/// K is the row axis and every swept K shares one table, so the K=1 row can
/// never be read outside the curve that explains it. A single-update figure in
/// isolation is the hazard this layout removes.
fn write_amp(cells: &[WriteAmpCell], arms: &[&str], fx: &mut Footnotes) -> String {
    if cells.is_empty() {
        return String::new();
    }
    let mut out = String::from("## 6.2 Write amplification against K\n\n");
    out.push_str(
        "Chunks written per edit. `batched` is one changeset on 1.0 and one multi-op commit on \
0.2; `per edit` is one commit or apply for every edit. The K=1 row is a point on this curve and \
is never printed apart from it.\n\n",
    );
    for (corpus, group) in by_corpus(cells, |c| c.corpus.as_str()) {
        for (scale, rows) in by_scale(&group) {
            let _ = writeln!(out, "### Corpus `{corpus}`, scale {}\n", int(scale));
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

// ---- 4.3 pagination -------------------------------------------------------

/// Whitepaper 4.3: rank-directed pagination against the skip baseline and the
/// 0.2 resume-token walk to the same offset.
fn paginate(cells: &[PaginateCell], fx: &mut Footnotes) -> String {
    if cells.is_empty() {
        return String::new();
    }
    let mut out = String::from("## 4.3 Pagination to an offset\n\n");
    out.push_str(
        "1.0 seeks the page by rank in O(depth). The skip baseline and the 0.2 resume-token walk \
both pay O(offset): 0.2 has no rank-directed seek, so a client carries the last path of each page \
into the next.\n\n",
    );
    for (corpus, group) in by_corpus(cells, |c| c.corpus.as_str()) {
        let _ = writeln!(out, "### Corpus `{corpus}`\n");
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
            out.push_str(&row(&[
                int(c.scale),
                int(c.offset),
                int(u64::from(c.limit)),
                int(c.keys_returned),
                int(c.paginate_fetch_count),
                int(c.skip_baseline_fetch_count),
                c.skip_over_paginate.map_or_else(|| fx.null(None), f2),
                c.v02_resume_fetch_count
                    .map_or_else(|| fx.null(c.v02_resume_null_reason.as_deref()), int),
            ]));
        }
        out.push('\n');
    }
    out
}

// ---- the wall-time section ------------------------------------------------

/// The build wall-time table, under the verbatim cold-pass caveat.
///
/// The caveat prints above the table, once, so no reader meets a nanosecond
/// before meeting the reason it is not the currency.
fn build_wall(cells: &[BuildWallCell], nulls: &[NullWithReason], fx: &mut Footnotes) -> String {
    if cells.is_empty() && nulls.is_empty() {
        return String::new();
    }
    let mut out = String::from("## Build wall-time (non-deterministic)\n\n");
    let caveat = cells
        .first()
        .map_or(String::new(), |c| format!("> {}\n\n", c.caveat));
    out.push_str(&caveat);
    if !cells.is_empty() {
        for (corpus, group) in by_corpus(cells, |c| c.corpus.as_str()) {
            let _ = writeln!(out, "### Corpus `{corpus}`\n");
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

    use super::{Footnotes, int, render};
    use crate::arm::{Capability, NullWithReason, OpCost, OpOutcome};
    use crate::results::{
        ArmMeta, Document, Meta, PaginateCell, PrefixListingCell, WriteAmpCell, generated_iso,
    };

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
        d.deterministic.prefix_listing.push(PrefixListingCell {
            corpus: "kiwix".to_string(),
            scale: 1_000,
            prefix: "a/".to_string(),
            keys_returned: 3,
            fair,
            pessimal: BTreeMap::new(),
            fair_multiplier: None,
            pessimal_multiplier: None,
            nulls: vec![NullWithReason {
                arm: "mantaray-0.2".to_string(),
                field: "fair".to_string(),
                reason: capped.clone(),
            }],
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

    /// The K=1 write-amplification row never stands alone: it is a row of the
    /// same table as the rest of the sweep (whitepaper hazard 1).
    #[test]
    fn the_k1_row_stays_inside_the_sweep_table() {
        let md = render(&doc());
        let table = md
            .split("## 6.2")
            .nth(1)
            .and_then(|s| s.split("\n## ").next())
            .unwrap_or_default();
        let ks: Vec<&str> = table
            .lines()
            .filter(|l| l.starts_with("| 1 |") || l.starts_with("| 10 |"))
            .collect();
        assert_eq!(ks.len(), 2, "K=1 and K=10 must share one table: {table}");
    }

    /// Grouping is by corpus and nothing crosses corpora or averages them.
    #[test]
    fn every_table_is_keyed_by_corpus() {
        let md = render(&doc());
        assert!(md.contains("### Corpus `kiwix`"));
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

    /// Thousands separators land on the right boundaries.
    #[test]
    fn integers_group_in_threes() {
        assert_eq!(int(0), "0");
        assert_eq!(int(999), "999");
        assert_eq!(int(1_000), "1,000");
        assert_eq!(int(1_000_000), "1,000,000");
    }
}
