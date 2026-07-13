//! Drive the harness across every `(format, corpus, scale)` and write one JSON
//! result document. Every number is measured; unruns are `ran:false` + reason.

use std::collections::BTreeMap;
use std::error::Error;
use std::path::{Path, PathBuf};
use std::process::Command;

use nectar_manifest_sim::corpus::{self, Corpus};
use nectar_manifest_sim::criterion_fold;
use nectar_manifest_sim::measure::{self, Cfg};
use nectar_manifest_sim::results::{Cell, CorpusBlock, Document, FormatBlock, Meta};

const DEFAULT_OUT: &str = "/home/mfw78/.claude/jobs/ba6b8d73/tmp/manifest-sim-results.json";
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
    formats.insert(
        F10.to_string(),
        FormatBlock {
            crate_name: "nectar-manifest".to_string(),
            registry: "StandardChunkSet".to_string(),
            corpora: corpora10,
        },
    );

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
        harness_version: "1".to_string(),
        seed_master: format!("0x{:016x}", corpus::MASTER_SEED),
        rtt_ms: c.rtt_ms,
        chunk_body_size: 4096,
        criterion_iters_note: "means from target/criterion/*/new/estimates.json".to_string(),
        sample_keys: c.sample_keys as u32,
        update_sample: c.update_sample as u32,
        batch_ops: c.batch_ops as u32,
    };

    let doc = Document { meta, formats };
    let json = serde_json::to_string_pretty(&doc)?;
    if let Some(parent) = args.out.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&args.out, json.as_bytes())?;
    eprintln!("wrote {} ({} bytes)", args.out.display(), json.len());
    Ok(())
}
