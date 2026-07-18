//! Drive the v3 range-query measurements across every
//! `(corpus, scale)` and write one JSON result document. Every number is
//! measured; the only modelled figure is `rounds * rtt`, and `rounds` itself is
//! read off the real bounded-concurrency cursor under a paused virtual clock.

use std::error::Error;
use std::path::PathBuf;
use std::process::Command;

use nectar_manifest::V1;
use nectar_manifest_sim::corpus::{self, Corpus};
use nectar_manifest_sim::perf_v3;
use nectar_manifest_sim::results_v3::{DocumentV3, MetaV3};
use nectar_primitives::DEFAULT_BODY_SIZE;

use nectar_manifest::Format;

const DEFAULT_OUT: &str = "/home/mfw78/.claude/jobs/ba6b8d73/tmp/manifest-perf-v3-results.json";

struct Args {
    out: PathBuf,
    scales: Vec<u64>,
}

fn parse_args() -> Args {
    let mut out = PathBuf::from(DEFAULT_OUT);
    let mut scales = vec![1_000u64, 10_000, 100_000, 1_000_000];
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        match a.as_str() {
            "--out" => {
                if let Some(v) = it.next() {
                    out = PathBuf::from(v);
                }
            }
            "--scales" => {
                if let Some(v) = it.next() {
                    scales = v.split(',').filter_map(|s| s.trim().parse().ok()).collect();
                }
            }
            _ => {}
        }
    }
    Args { out, scales }
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
    Command::new("date")
        .args(["-u", "+%Y-%m-%dT%H:%M:%SZ"])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_default()
}

fn caveats() -> Vec<String> {
    vec![
        "Every fetch/round/byte figure is measured. The only modelled numbers are the cursor \
latency columns: serial = fetch_count * rtt and concurrent = rounds * rtt, with rounds read off \
the real bounded-concurrency cursor under a paused virtual clock (one RTT per node fetch). Fetch \
counts are hardware-independent; wall-ms are illustrative."
            .to_string(),
        "parallel_cursor rounds count the sequential fetch rounds the READ_AHEAD=16 read-ahead \
window actually takes over the covering frontier, respecting the parent-before-child dependency; \
the seek prologue is serial (one round per depth) and the drain is where concurrency pays."
            .to_string(),
        "V1Read trades single-update write-amplification (single_update_wa_delta, the honest cost) \
for fewer referenced hops per range/listing window; read both sides together."
            .to_string(),
        "paginate is the rank-directed page: its fetch count is ~O(depth) and flat in \
offset, against the iter().skip(offset) baseline whose fetches grow with offset."
            .to_string(),
    ]
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = parse_args();

    let mut parallel_cursor = Vec::new();
    let mut v1read = Vec::new();
    let mut paginate = Vec::new();

    for corpus in Corpus::all() {
        for &scale in &args.scales {
            let n = scale as usize;
            eprintln!("[v3] {} n={}", corpus.name(), n);
            let keys = corpus::generate(corpus, n);
            parallel_cursor.extend(perf_v3::parallel_cursor_cells(corpus, scale, &keys)?);
            v1read.push(perf_v3::read_profile_cell(corpus, scale, &keys)?);
            paginate.extend(perf_v3::paginate_cells(corpus, scale, &keys)?);
        }
    }

    let meta = MetaV3 {
        generated: now_iso(),
        git_branch: git(&["rev-parse", "--abbrev-ref", "HEAD"]),
        git_commit: git(&["rev-parse", "HEAD"]),
        harness_version: "3".to_string(),
        seed_master: format!("0x{:016x}", corpus::MASTER_SEED),
        rtt_ms_set: perf_v3::RTT_SET.to_vec(),
        read_ahead: V1::READ_AHEAD as u32,
        scales: args.scales.clone(),
        corpora: Corpus::all().iter().map(|c| c.name().to_string()).collect(),
        range_windows: perf_v3::RANGE_WS.to_vec(),
        paginate_offsets: perf_v3::PAGE_OFFSETS.to_vec(),
        paginate_limit: perf_v3::PAGE_LIMIT as u32,
        chunk_body_size: DEFAULT_BODY_SIZE as u32,
        caveats: caveats(),
    };

    let doc = DocumentV3 {
        meta,
        parallel_cursor,
        v1read,
        paginate,
    };
    let json = serde_json::to_string_pretty(&doc)?;
    if let Some(parent) = args.out.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&args.out, json.as_bytes())?;
    eprintln!("wrote {} ({} bytes)", args.out.display(), json.len());
    Ok(())
}
