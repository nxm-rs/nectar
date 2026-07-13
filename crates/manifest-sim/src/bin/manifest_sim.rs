//! Drive the harness across every `(format, corpus, scale)` and write one JSON
//! result document. Every number is measured; unruns are `ran:false` + reason.

use std::collections::BTreeMap;
use std::error::Error;
use std::path::{Path, PathBuf};
use std::process::Command;

use nectar_manifest_sim::corpus::{self, Corpus};
use nectar_manifest_sim::criterion_fold;
use nectar_manifest_sim::measure::{self, Cfg};
use nectar_manifest_sim::results::{
    CapabilityRow, Cell, CorpusBlock, Document, Fallback, FormatBlock, Headline, Headlines, LogLog,
    Meta,
};

const DEFAULT_OUT: &str = "/home/mfw78/.claude/jobs/ba6b8d73/tmp/manifest-sim-results-v2.json";
const F10: &str = "mantaray_1_0";
const F02: &str = "mantaray_0_2";

struct Args {
    out: PathBuf,
    criterion_base: PathBuf,
    scales: Vec<u64>,
    max_02_scale: u64,
}

fn parse_args() -> Args {
    let mut out = PathBuf::from(DEFAULT_OUT);
    let mut criterion_base = PathBuf::from("target");
    let mut scales = vec![1_000u64, 10_000, 100_000, 1_000_000];
    let mut max_02_scale = 100_000u64;
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        match a.as_str() {
            "--out" => {
                if let Some(v) = it.next() {
                    out = PathBuf::from(v);
                }
            }
            "--criterion-base" => {
                if let Some(v) = it.next() {
                    criterion_base = PathBuf::from(v);
                }
            }
            "--scales" => {
                if let Some(v) = it.next() {
                    scales = v.split(',').filter_map(|s| s.trim().parse().ok()).collect();
                }
            }
            "--max-02-scale" => {
                if let Some(v) = it.next()
                    && let Ok(n) = v.parse()
                {
                    max_02_scale = n;
                }
            }
            _ => {}
        }
    }
    Args {
        out,
        criterion_base,
        scales,
        max_02_scale,
    }
}

fn git(args: &[&str]) -> String {
    Command::new("git")
        .args(args)
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_default()
}

fn now_iso() -> String {
    // Wall-clock via `date -u`; harness metadata only.
    Command::new("date")
        .args(["-u", "+%Y-%m-%dT%H:%M:%SZ"])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_default()
}

fn cfg() -> Cfg {
    Cfg {
        sample_keys: 10_000,
        update_sample: 48,
        batch_ops: 10_000,
        rtt_ms: 25,
    }
}

fn fold_into(cell: &mut Cell, base: &Path, format: &str, corpus_name: &str, scale: u64) {
    if let Some(b) = &mut cell.build
        && let Some(e) = criterion_fold::load(base, "build", format, corpus_name, scale)
    {
        b.criterion_ns_per_op = Some(e.mean_ns);
        b.criterion_stddev_ns = Some(e.stddev_ns);
    }
    if let Some(g) = &mut cell.get
        && let Some(e) = criterion_fold::load(base, "get", format, corpus_name, scale)
    {
        g.criterion_ns_per_op = Some(e.mean_ns);
    }
    if let Some(u) = &mut cell.update
        && let Some(s) = &mut u.single
        && let Some(e) = criterion_fold::load(base, "apply", format, corpus_name, scale)
    {
        s.criterion_ns_per_op = Some(e.mean_ns);
    }
}

fn skipped_cell(reason: &str, n: u64, encoding: &str) -> Cell {
    Cell {
        ran: false,
        reason: Some(reason.to_string()),
        n_keys: Some(n),
        key_encoding: Some(encoding.to_string()),
        ..Cell::default()
    }
}

const NOT_RUN: &str = "0.2 not run at this scale";

/// Fill 1.0-only op fallback costs + multipliers from the matching 0.2 cell.
fn cross_fill(c10: &mut BTreeMap<String, CorpusBlock>, c02: &BTreeMap<String, CorpusBlock>) {
    for (corpus, block) in c10.iter_mut() {
        for (scale, cell) in block.scales.iter_mut() {
            let peer = c02
                .get(corpus)
                .and_then(|b| b.scales.get(scale))
                .filter(|c| c.ran);
            let walk = peer.and_then(|c| c.full_entries_walk_fetches);
            let peer_walk_from = peer
                .and_then(|c| c.listing.as_ref())
                .and_then(|l| l.fallback_02_walk_from.as_ref())
                .map(|k| k.fetches);

            // floor
            if let Some(f) = cell.floor.as_mut() {
                let native = f.hops_mean.unwrap_or(0.0);
                f.fallback_02 = Some(match walk {
                    Some(fetches) => Fallback {
                        method: "entries() full walk + client scan".to_string(),
                        fetches: Some(fetches),
                        multiplier: (native > 0.0).then(|| fetches as f64 / native),
                        reason_if_null: None,
                    },
                    None => Fallback {
                        method: "entries() full walk + client scan".to_string(),
                        fetches: None,
                        multiplier: None,
                        reason_if_null: Some(NOT_RUN.to_string()),
                    },
                });
            }
            // ceiling
            if let Some(cl) = cell.ceiling.as_mut() {
                let native = cl.native_seek_hops_mean.unwrap_or(0.0);
                cl.fallback_02 = Some(match walk {
                    Some(fetches) => Fallback {
                        method: "entries() full walk + client scan".to_string(),
                        fetches: Some(fetches),
                        multiplier: (native > 0.0).then(|| fetches as f64 / native),
                        reason_if_null: None,
                    },
                    None => Fallback {
                        method: "entries() full walk + client scan".to_string(),
                        fetches: None,
                        multiplier: None,
                        reason_if_null: Some(NOT_RUN.to_string()),
                    },
                });
            }
            // range windows
            if let Some(r) = cell.range.as_mut() {
                for win in r.windows.iter_mut() {
                    match (walk, win.fetch_count) {
                        (Some(w), Some(native)) if native > 0 => {
                            win.fallback_02_fetches = Some(w);
                            win.multiplier = Some(w as f64 / native as f64);
                        }
                        (None, _) => win.reason_if_null = Some(NOT_RUN.to_string()),
                        _ => {}
                    }
                }
            }
            // listing fair multiplier
            if let Some(l) = cell.listing.as_mut()
                && let (Some(wf), Some(native)) = (peer_walk_from, l.fetch_count)
                && native > 0
            {
                l.multiplier_fair = Some(wf as f64 / native as f64);
            }
        }
    }
}

/// Least-squares fit of log(build wall_ns) vs log(N) per corpus, stored on every
/// scale's cell. A single cold pass, so it is indicative not a precise
/// asymptote (see caveats); r2 and n_points are reported for that reason.
fn fit_loglog(corpora: &mut BTreeMap<String, CorpusBlock>) {
    for block in corpora.values_mut() {
        let mut pts: Vec<(f64, f64)> = Vec::new();
        for cell in block.scales.values() {
            if let (Some(n), Some(b)) = (cell.n_keys, cell.build.as_ref())
                && let Some(ns) = b.wall_ns
                && n > 0
                && ns > 0
            {
                pts.push(((n as f64).ln(), (ns as f64).ln()));
            }
        }
        if pts.len() < 2 {
            continue;
        }
        let fit = least_squares(&pts);
        for cell in block.scales.values_mut() {
            if let Some(b) = cell.build.as_mut() {
                b.cpu_loglog = Some(LogLog {
                    slope: fit.0,
                    intercept: fit.1,
                    r2: fit.2,
                    n_points: pts.len() as u32,
                    basis: "build wall_ns, single cold pass".to_string(),
                });
            }
        }
    }
}

/// (slope, intercept, r2) of ys on xs.
fn least_squares(pts: &[(f64, f64)]) -> (f64, f64, f64) {
    let n = pts.len() as f64;
    let mx = pts.iter().map(|p| p.0).sum::<f64>() / n;
    let my = pts.iter().map(|p| p.1).sum::<f64>() / n;
    let mut sxx = 0.0;
    let mut sxy = 0.0;
    let mut syy = 0.0;
    for &(x, y) in pts {
        sxx += (x - mx) * (x - mx);
        sxy += (x - mx) * (y - my);
        syy += (y - my) * (y - my);
    }
    let slope = if sxx == 0.0 { 0.0 } else { sxy / sxx };
    let intercept = my - slope * mx;
    let r2 = if sxx == 0.0 || syy == 0.0 {
        0.0
    } else {
        (sxy * sxy) / (sxx * syy)
    };
    (slope, intercept, r2)
}

/// Cross-cell headline multipliers, derived from executed cells only.
fn build_headlines(
    c10: &BTreeMap<String, CorpusBlock>,
    c02: &BTreeMap<String, CorpusBlock>,
) -> Headlines {
    let mut entries: Vec<Headline> = Vec::new();
    for (corpus, block) in c10 {
        for (scale, cell) in &block.scales {
            let scale_n: u64 = scale.parse().unwrap_or(0);
            // floor multiplier (native hops vs 0.2 full-walk fetches)
            if let Some(f) = &cell.floor
                && let Some(fb) = &f.fallback_02
            {
                entries.push(Headline {
                    metric: "floor_multiplier".to_string(),
                    corpus: corpus.clone(),
                    scale: scale_n,
                    native: f.hops_mean.unwrap_or(0.0),
                    native_unit: "hops".to_string(),
                    fallback: fb.fetches.map(|x| x as f64),
                    fallback_unit: "fetches".to_string(),
                    multiplier: fb.multiplier,
                    reason_if_null: fb.reason_if_null.clone(),
                });
            }
            // range @ 10% window
            if let Some(r) = &cell.range
                && let Some(w) = r.windows.iter().find(|w| (w.w - 0.10).abs() < 1e-9)
            {
                entries.push(Headline {
                    metric: "range_multiplier_w0.10".to_string(),
                    corpus: corpus.clone(),
                    scale: scale_n,
                    native: w.fetch_count.unwrap_or(0) as f64,
                    native_unit: "fetches".to_string(),
                    fallback: w.fallback_02_fetches.map(|x| x as f64),
                    fallback_unit: "fetches".to_string(),
                    multiplier: w.multiplier,
                    reason_if_null: w.reason_if_null.clone(),
                });
            }
            // listing fair multiplier
            if let Some(l) = &cell.listing {
                entries.push(Headline {
                    metric: "listing_fair_multiplier".to_string(),
                    corpus: corpus.clone(),
                    scale: scale_n,
                    native: l.fetch_count.unwrap_or(0) as f64,
                    native_unit: "fetches".to_string(),
                    fallback: l.fallback_02_walk_from.as_ref().map(|k| k.fetches as f64),
                    fallback_unit: "fetches (walk_from)".to_string(),
                    multiplier: l.multiplier_fair,
                    reason_if_null: (l.multiplier_fair.is_none()).then(|| NOT_RUN.to_string()),
                });
            }
            // build-scale ceiling: 1.0 frontier vs 0.2 (absent above cap)
            if let Some(b) = &cell.build {
                let peer_ran = c02
                    .get(corpus)
                    .and_then(|bl| bl.scales.get(scale))
                    .is_some_and(|c| c.ran);
                let peer_frontier = c02
                    .get(corpus)
                    .and_then(|bl| bl.scales.get(scale))
                    .and_then(|c| c.build.as_ref())
                    .and_then(|bb| bb.builder_frontier_nodes);
                entries.push(Headline {
                    metric: "build_frontier_nodes".to_string(),
                    corpus: corpus.clone(),
                    scale: scale_n,
                    native: b.builder_frontier_nodes.unwrap_or(0) as f64,
                    native_unit: "open nodes (O(depth))".to_string(),
                    fallback: peer_frontier.map(|x| x as f64),
                    fallback_unit: "resident nodes (O(N))".to_string(),
                    multiplier: match (b.builder_frontier_nodes, peer_frontier) {
                        (Some(nat), Some(pf)) if nat > 0 => Some(pf as f64 / nat as f64),
                        _ => None,
                    },
                    reason_if_null: (!peer_ran).then(|| NOT_RUN.to_string()),
                });
            }
        }
    }
    Headlines {
        notes: "Every multiplier is native-vs-0.2-fallback at a scale where BOTH ran; where 0.2 did not run the fallback is null with reason. Units are store fetches (hardware-independent) or hop counts, never wall-clock.".to_string(),
        entries,
    }
}

/// One capability-matrix row as fixed data: see [`CapabilityRow`] for fields.
type Row = (
    u32,
    &'static str,
    bool,
    Option<&'static str>,
    &'static str,
    bool,
    Option<&'static str>,
    &'static str,
    Option<&'static str>,
    Option<&'static str>,
    &'static str,
);

fn cap(r: Row) -> CapabilityRow {
    CapabilityRow {
        n: r.0,
        op: r.1.to_string(),
        v1_supported: r.2,
        v1_api: r.3.map(str::to_string),
        v1_class: r.4.to_string(),
        v02_supported: r.5,
        v02_api: r.6.map(str::to_string),
        v02_class: r.7.to_string(),
        v02_fallback: r.8.map(str::to_string),
        v02_asymptote: r.9.map(str::to_string),
        notes: r.10.to_string(),
    }
}

/// The exhaustive op x format capability matrix (fixed API facts verified on
/// the source tree at build time).
fn capability_matrix() -> Vec<CapabilityRow> {
    const ROWS: [Row; 20] = [
        (
            1,
            "build-from-scratch",
            true,
            Some("Builder::insert+build"),
            "native",
            true,
            Some("Manifest::new+add*+save"),
            "native",
            None,
            Some("O(N) RAM"),
            "1.0 frontier O(depth) (peak_open_nodes ~6..11); 0.2 holds whole trie -> O(N) RAM, ran:false at 1e6.",
        ),
        (
            2,
            "build_files",
            true,
            Some("build_files(store, iter)"),
            "native",
            false,
            None,
            "fallback",
            Some("hand-rolled BMT loop + add-loop + save"),
            Some("O(N)"),
            "1.0 one-call ingest; 0.2 dev writes the loop.",
        ),
        (
            3,
            "get",
            true,
            Some("Reader::get"),
            "seek",
            true,
            Some("Manifest::get"),
            "seek",
            None,
            Some("O(depth)"),
            "Both O(depth) hop-counted descent.",
        ),
        (
            4,
            "has/contains",
            true,
            Some("get(...).is_some()"),
            "seek",
            true,
            Some("get(...).is_some()"),
            "seek",
            None,
            Some("O(depth)"),
            "Neither ships a dedicated has; equal.",
        ),
        (
            5,
            "folder-exists",
            true,
            Some("prefix(p).next().is_some()"),
            "seek",
            true,
            Some("Manifest::has_prefix"),
            "seek",
            None,
            Some("O(depth)"),
            "0.2 has the only named prefix-existence probe; cost equal.",
        ),
        (
            6,
            "floor",
            true,
            Some("Reader::floor"),
            "seek",
            false,
            None,
            "unsupported",
            Some("entries() full walk + client scan"),
            Some("O(N)"),
            "HARD 0.2 gap; multiplier measured per cell (floor.fallback_02).",
        ),
        (
            7,
            "ceiling",
            true,
            Some("range(key, MAX).next()"),
            "seek",
            false,
            None,
            "unsupported",
            Some("entries() full walk + client scan"),
            Some("O(N)"),
            "Soft gap in both (neither names it); 1.0 emulates by seek O(depth), 0.2 pays O(N).",
        ),
        (
            8,
            "range",
            true,
            Some("Reader::range / Cursor"),
            "seek",
            false,
            None,
            "unsupported",
            Some("entries() full walk + filter"),
            Some("O(N)"),
            "HARD 0.2 gap; selectivity sweep in range.windows.",
        ),
        (
            9,
            "prefix-scan / listing",
            true,
            Some("prefix / folder::list"),
            "native",
            true,
            Some("walk_from(prefix) or entries()"),
            "partial",
            Some("walk_from = O(subtree); entries = O(N); no ordered seek"),
            Some("O(subtree)"),
            "1.0 embeds children; fair multiplier in listing.multiplier_fair.",
        ),
        (
            10,
            "ordered iter (full)",
            true,
            Some("Cursor::iter"),
            "native",
            false,
            Some("entries() (DFS, unguaranteed)"),
            "partial",
            Some("materialise all N then maybe sort"),
            Some("O(N) mem"),
            "1.0 first key after O(depth); 0.2 after loading everything; entries_concurrent unordered.",
        ),
        (
            11,
            "serve / website-view",
            true,
            Some("Reader::serve + website()"),
            "native",
            false,
            Some("index_document/error_document"),
            "partial",
            Some("client re-implements exact->index->error via repeated get"),
            Some("O(depth) per probe"),
            "Site metadata in both; resolver is 1.0-only.",
        ),
        (
            12,
            "single-key insert",
            true,
            Some("apply(Changeset{put})"),
            "native",
            true,
            Some("add+save"),
            "native",
            None,
            None,
            "1.0 CoW new root; 0.2 reseals touched spine.",
        ),
        (
            13,
            "single-key update",
            true,
            Some("apply(Changeset{put})"),
            "native",
            true,
            Some("add(overwrite)+save"),
            "native",
            None,
            None,
            "1.0 rewrites more spine per isolated edit; batch reverses it.",
        ),
        (
            14,
            "single-key remove",
            true,
            Some("apply(Changeset{remove})"),
            "native",
            true,
            Some("remove+save"),
            "native",
            None,
            None,
            "Plus collapse cost (update.subtree_delete).",
        ),
        (
            15,
            "batch apply(changeset)",
            true,
            Some("apply(store, root, Changeset)"),
            "native",
            false,
            Some("K add/remove + one save"),
            "fallback",
            Some("O(K*depth) in-RAM mutations + reseal; no declarative delta"),
            Some("O(K*depth)"),
            "1.0 batch write-amp far lower; the amortisation story (update.batch_sweep).",
        ),
        (
            16,
            "metadata read/write",
            true,
            Some("Metadata<KeyId/CustomKey>"),
            "native",
            true,
            Some("add_with_metadata (BTreeMap)"),
            "native",
            None,
            None,
            "0.2 arbitrary map -> dedup hazard; 1.0 typed TLV canonical.",
        ),
        (
            17,
            "inline values",
            true,
            Some("Entry::Inline (<=128B)"),
            "native",
            false,
            None,
            "unsupported",
            Some("store a content chunk + 1 extra fetch/read"),
            Some("O(1) extra"),
            "HARD 1.0-only; not exercised here (synthetic ref32 values).",
        ),
        (
            18,
            "referenced values (ref32)",
            true,
            Some("Entry::Ref32"),
            "native",
            true,
            Some("ChunkRef"),
            "native",
            None,
            None,
            "Both; the exercised value model.",
        ),
        (
            19,
            "encrypted refs (ref64)",
            true,
            Some("Entry::Ref64 + EncryptedNode"),
            "native",
            true,
            Some("EncryptedManifest / new_encrypted"),
            "native",
            None,
            None,
            "Both; harness exercises ref32 only.",
        ),
        (
            20,
            "recanonicalise / history-independence",
            true,
            Some("recanonicalize(); apply==rebuild (I6)"),
            "native",
            false,
            None,
            "unsupported",
            Some("cannot verify; obfuscation-key randomisation defeats dedup"),
            None,
            "1.0 proves dedup-stability (dedup_ratio_second_build); 0.2 cannot.",
        ),
    ];
    ROWS.into_iter().map(cap).collect()
}

fn caveats() -> Vec<String> {
    vec![
        "hops->wall-clock latency is a stated model (hops x {25,50,75}ms, sequential, no pipelining/caching); FETCH COUNT is the hardware-independent truth, wall-ms illustrative only (derived_from_hops:true).".to_string(),
        "Single-key update looks cheaper on 0.2 in isolation but 1.0 wins the amortised/batch path and is history-independent; never read single-update without the batch_sweep beside it.".to_string(),
        "floor native hops are O(depth) but with a large N-growing constant (rightmost-spine fallback descent), far higher than a point get; shown as their own series.".to_string(),
        "Listing multiplier_fair uses walk_from on identical prefixes (apples-to-apples); fallback_02_full_entries is the pessimal whole-map walk kept for reference.".to_string(),
        "peak_rss_bytes is cumulative process VmHWM (monotone, dirty across cells); use peak_live_store_bytes for per-cell memory.".to_string(),
        "0.2 at N=1e6 is ran:false (O(N)-RAM full-trie build outside budget); every 1e6 multiplier is null with reason. The absence IS the build-scale-ceiling finding.".to_string(),
        "build wall_ns is a single cold pass (includes RSS sampling); criterion_ns_per_op is a warm multi-iter mean present only for some cells; cpu_loglog is fit over wall_ns and is indicative, not a precise asymptote.".to_string(),
        "Corpus dependence is large; never quote a headline number without its corpus (uniform is the zero-prefix-sharing control).".to_string(),
        "Value/metadata model is synthetic (ref32 = sha256(b\"val\"||key)); value-layer figures describe manifest structure, not real payload entropy; inline study not exercised.".to_string(),
        "0.2 entries() DFS order is not a guaranteed ordered-iter API; any order is incidental and unseekable (ordered_guaranteed:false).".to_string(),
        "cpu_loglog fits use few points (2-4 scales); treat slopes as indicative with the reported r2 and n_points.".to_string(),
    ]
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = parse_args();
    let c = cfg();

    let mut formats: BTreeMap<String, FormatBlock> = BTreeMap::new();

    // --- mantaray 1.0 ---
    let mut corpora10: BTreeMap<String, CorpusBlock> = BTreeMap::new();
    for corpus in Corpus::all() {
        let mut scales_map: BTreeMap<String, Cell> = BTreeMap::new();
        for &scale in &args.scales {
            let n = scale as usize;
            eprintln!("[1.0] {} n={}", corpus.name(), n);
            let keys = corpus::generate(corpus, n);
            let mut cell = measure::measure_10(corpus, &keys, c)?;
            fold_into(&mut cell, &args.criterion_base, F10, corpus.name(), scale);
            scales_map.insert(scale.to_string(), cell);
        }
        corpora10.insert(
            corpus.name().to_string(),
            CorpusBlock { scales: scales_map },
        );
    }

    // --- mantaray 0.2 ---
    let mut corpora02: BTreeMap<String, CorpusBlock> = BTreeMap::new();
    for corpus in Corpus::all() {
        let mut scales_map: BTreeMap<String, Cell> = BTreeMap::new();
        for &scale in &args.scales {
            let n = scale as usize;
            if scale > args.max_02_scale {
                scales_map.insert(
                    scale.to_string(),
                    skipped_cell(
                        "0.2 build not run: O(N)-RAM full-trie build outside harness wall-clock budget at this scale",
                        scale,
                        corpus.key_encoding(),
                    ),
                );
                continue;
            }
            eprintln!("[0.2] {} n={}", corpus.name(), n);
            let keys = corpus::generate(corpus, n);
            let mut cell = measure::measure_02(corpus, &keys, c)?;
            fold_into(&mut cell, &args.criterion_base, F02, corpus.name(), scale);
            scales_map.insert(scale.to_string(), cell);
        }
        corpora02.insert(
            corpus.name().to_string(),
            CorpusBlock { scales: scales_map },
        );
    }
    // Cross-fill 1.0-only op fallback costs + multipliers from the matching 0.2
    // cell (both must have run), and fit CPU log-log slopes per (format,corpus).
    cross_fill(&mut corpora10, &corpora02);
    fit_loglog(&mut corpora10);
    fit_loglog(&mut corpora02);
    let headlines = build_headlines(&corpora10, &corpora02);

    formats.insert(
        F10.to_string(),
        FormatBlock {
            crate_name: "nectar-manifest".to_string(),
            registry: "StandardChunkSet".to_string(),
            corpora: corpora10,
        },
    );
    formats.insert(
        F02.to_string(),
        FormatBlock {
            crate_name: "nectar-mantaray".to_string(),
            registry: "AnyChunkSet<4096>".to_string(),
            corpora: corpora02,
        },
    );

    let meta = Meta {
        generated: now_iso(),
        git_branch: git(&["rev-parse", "--abbrev-ref", "HEAD"]),
        git_commit: git(&["rev-parse", "HEAD"]),
        harness_version: "2".to_string(),
        seed_master: format!("0x{:016x}", corpus::MASTER_SEED),
        rtt_ms: c.rtt_ms,
        rtt_ms_set: vec![25, 50, 75],
        batch_k_sweep: vec![1, 10, 100, 1_000, 10_000],
        range_windows: vec![0.001, 0.01, 0.10, 1.0],
        value_read_corpus: "not_exercised (synthetic ref32 values)".to_string(),
        chunk_body_size: 4096,
        criterion_iters_note: "means from target/criterion/*/new/estimates.json".to_string(),
        sample_keys: c.sample_keys as u32,
        update_sample: c.update_sample as u32,
        batch_ops: c.batch_ops as u32,
        caveats: caveats(),
    };

    let doc = Document {
        meta,
        capability_matrix: capability_matrix(),
        headlines,
        formats,
    };
    let json = serde_json::to_string_pretty(&doc)?;
    if let Some(parent) = args.out.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&args.out, json.as_bytes())?;
    eprintln!("wrote {} ({} bytes)", args.out.display(), json.len());
    Ok(())
}
