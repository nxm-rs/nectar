//! The two-arm manifest harness binary, in two modes.
//!
//! `manifest-perf run` drives every `(corpus, scale)` over both arms and writes
//! one JSON result document, split into a bit-reproducible deterministic
//! section and a non-deterministic build wall-time section. `manifest-perf
//! render --in <json> --out <md>` reads that document back and prints the
//! markdown tables; it measures nothing and fills nothing.
//!
//! Run flags: `--out`, `--scales`, `--max-mantaray-scale`, `--build-samples`.
//! Render flags: `--in`, `--out`. With no mode word the binary runs, so the
//! bare invocation keeps working.

use std::error::Error;
use std::path::PathBuf;
use std::process::Command;

use nectar_ldb::Format;
use nectar_ldb::V1;
use nectar_manifest_sim::corpus::{self, Corpus};
use nectar_manifest_sim::results::{
    self, ArmMeta, DeterministicSection, Document, Meta, WallTimeSection,
};
use nectar_manifest_sim::{build_time, matrix, perf, render};
use nectar_primitives::DEFAULT_BODY_SIZE;

const DEFAULT_OUT: &str = "manifest-perf-results.json";
/// The default markdown target of the render mode.
const DEFAULT_RENDER_OUT: &str = "manifest-perf-tables.md";
/// The 0.2 arm scale cap: above it the editor commit materialises the whole
/// trie in RAM, so every 0.2 cell is a null-with-reason.
const DEFAULT_MAX_MANTARAY_SCALE: u64 = 100_000;
/// Timed build passes per (arm, corpus, scale).
const DEFAULT_BUILD_SAMPLES: u32 = 3;

struct Args {
    out: PathBuf,
    scales: Vec<u64>,
    max_mantaray_scale: u64,
    build_samples: u32,
}

/// What the invocation asked for.
enum Mode {
    /// Measure and write the JSON document.
    Run(Args),
    /// Read a document back and write the markdown tables.
    Render { input: PathBuf, out: PathBuf },
}

fn parse_args() -> Mode {
    let mut out: Option<PathBuf> = None;
    let mut input: Option<PathBuf> = None;
    let mut scales = vec![1_000u64, 10_000, 100_000, 1_000_000];
    let mut max_mantaray_scale = DEFAULT_MAX_MANTARAY_SCALE;
    let mut build_samples = DEFAULT_BUILD_SAMPLES;
    // No mode word means `run`, so the bare invocation keeps working.
    let mut render_mode = false;
    let mut it = std::env::args().skip(1).peekable();
    if let Some(first) = it.peek()
        && matches!(first.as_str(), "run" | "render")
    {
        render_mode = first == "render";
        let _ = it.next();
    }
    while let Some(a) = it.next() {
        match a.as_str() {
            "--out" => {
                if let Some(v) = it.next() {
                    out = Some(PathBuf::from(v));
                }
            }
            "--in" => {
                if let Some(v) = it.next() {
                    input = Some(PathBuf::from(v));
                }
            }
            "--scales" => {
                if let Some(v) = it.next() {
                    scales = v.split(',').filter_map(|s| s.trim().parse().ok()).collect();
                }
            }
            "--max-mantaray-scale" => {
                if let Some(v) = it.next()
                    && let Ok(n) = v.trim().parse()
                {
                    max_mantaray_scale = n;
                }
            }
            "--build-samples" => {
                if let Some(v) = it.next()
                    && let Ok(n) = v.trim().parse()
                {
                    build_samples = n;
                }
            }
            _ => {}
        }
    }
    if render_mode {
        return Mode::Render {
            input: input.unwrap_or_else(|| PathBuf::from(DEFAULT_OUT)),
            out: out.unwrap_or_else(|| PathBuf::from(DEFAULT_RENDER_OUT)),
        };
    }
    Mode::Run(Args {
        out: out.unwrap_or_else(|| PathBuf::from(DEFAULT_OUT)),
        scales,
        max_mantaray_scale,
        build_samples,
    })
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

fn arms_meta() -> Vec<ArmMeta> {
    let version = env!("CARGO_PKG_VERSION").to_string();
    let arm = |label: &str, package: &str| ArmMeta {
        label: label.to_string(),
        package: package.to_string(),
        version: version.clone(),
    };
    vec![
        arm("mantaray-0.2", "nectar-mantaray"),
        arm("ldb-v1", "nectar-ldb"),
        arm("ldb-v1read", "nectar-ldb"),
    ]
}

fn caveats() -> Vec<String> {
    vec![
        "Store fetches, puts and bytes are the currency; wall-clock is illustrative. A capability \
gap is a null-with-reason, never an estimate. The build wall-time section is the one sanctioned \
addition of real timing and is fenced off from the deterministic currency."
            .to_string(),
        "The value model is synthetic ref32 on both arms: entries are 32-byte references, so no \
value-read cell is charted and inline values stay a 1.0-only capability row."
            .to_string(),
        "The capability matrix reports the crates in front of it: the current 0.2 cursor is \
pruned, ordered and resumable, and one multi-op editor commit amortises its writes. These are \
honest improvements over the whitepaper-era classifications for ordered iteration, ceiling and \
batch update."
            .to_string(),
    ]
}

fn main() -> Result<(), Box<dyn Error>> {
    match parse_args() {
        Mode::Run(args) => run(args),
        Mode::Render { input, out } => render_mode(&input, &out),
    }
}

/// Read a finished document back and write its markdown tables.
fn render_mode(input: &std::path::Path, out: &std::path::Path) -> Result<(), Box<dyn Error>> {
    let json = std::fs::read_to_string(input)?;
    let doc: Document = serde_json::from_str(&json)?;
    let md = render::render(&doc);
    if let Some(parent) = out.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(out, md.as_bytes())?;
    eprintln!("wrote {} ({} bytes)", out.display(), md.len());
    Ok(())
}

fn run(args: Args) -> Result<(), Box<dyn Error>> {
    let mut det = DeterministicSection {
        capability_matrix: matrix::capability_matrix(),
        ..DeterministicSection::default()
    };
    let mut wall = WallTimeSection::default();

    for corpus in Corpus::all() {
        for &scale in &args.scales {
            let n = scale as usize;
            eprintln!("[manifest-perf] {} n={}", corpus.name(), n);
            let keys = corpus::generate(corpus, n);

            // v4 metric cells (reused unchanged).
            det.parallel_cursor
                .extend(perf::parallel_cursor_cells(corpus, scale, &keys)?);
            det.v1read
                .push(perf::read_profile_cell(corpus, scale, &keys)?);
            det.paginate.extend(perf::paginate_cells(
                corpus,
                scale,
                &keys,
                args.max_mantaray_scale,
            )?);
            det.subtree_serve
                .extend(perf::subtree_serve_cell(corpus, scale, &keys)?);

            // Two-arm deterministic metric modules (Units B-D).
            let metrics =
                matrix::deterministic_metrics(corpus, scale, &keys, args.max_mantaray_scale);
            det.storage.extend(metrics.storage);
            det.get_hops.extend(metrics.get_hops);
            det.ordered_ops.extend(metrics.ordered_ops);
            det.prefix_listing.extend(metrics.prefix_listing);
            det.write_amp.extend(metrics.write_amp);
            det.build_profile.extend(metrics.build_profile);

            // Build wall-time lane (Unit E). A capped arm has no cell at all,
            // so its gap rides the section's null list instead.
            wall.build_wall.extend(build_time::build_wall(
                corpus,
                scale,
                &keys,
                args.max_mantaray_scale,
                args.build_samples,
            ));
            wall.nulls
                .extend(build_time::cap_nulls(scale, args.max_mantaray_scale));
        }
    }

    let meta = Meta {
        generated: results::generated_iso(
            std::env::var("SOURCE_DATE_EPOCH")
                .ok()
                .and_then(|v| v.parse().ok()),
        ),
        git_branch: git(&["rev-parse", "--abbrev-ref", "HEAD"]),
        git_commit: git(&["rev-parse", "HEAD"]),
        harness_version: "5".to_string(),
        arms: arms_meta(),
        seed_master: format!("0x{:016x}", corpus::MASTER_SEED),
        rtt_ms_set: vec![25, 50, 75, 100],
        read_ahead: V1::READ_AHEAD as u32,
        scales: args.scales.clone(),
        max_mantaray_scale: args.max_mantaray_scale,
        build_samples: args.build_samples,
        write_amp_ks: perf::WRITE_AMP_KS.to_vec(),
        corpora: Corpus::all().iter().map(|c| c.name().to_string()).collect(),
        range_windows: perf::RANGE_WS.to_vec(),
        paginate_offsets: perf::PAGE_OFFSETS.to_vec(),
        paginate_limit: perf::PAGE_LIMIT as u32,
        chunk_body_size: DEFAULT_BODY_SIZE as u32,
        caveats: caveats(),
    };

    let doc = Document {
        meta,
        deterministic: det,
        wall_time: wall,
    };
    let json = serde_json::to_string_pretty(&doc)?;
    if let Some(parent) = args.out.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&args.out, json.as_bytes())?;
    eprintln!("wrote {} ({} bytes)", args.out.display(), json.len());
    Ok(())
}
