//! Drive the two-arm manifest comparison across every `(corpus, scale)` and
//! write one JSON result document: the trie and the key-value database
//! measured through the same seam calls over their own counting stores.
//!
//! Run: `cargo run -p nectar-manifest --example manifest-perf`
#![allow(
    missing_docs,
    unreachable_pub,
    clippy::arithmetic_side_effects,
    clippy::as_conversions,
    clippy::indexing_slicing,
    clippy::unwrap_used
)]

mod arm;
mod corpus;
mod measure;
mod results;

use std::error::Error;
use std::path::PathBuf;

use nectar_testing::bench::RunMeta;

use crate::arm::{Arm, ldb_arm, mantaray_arm};
use crate::corpus::{Corpus, MASTER_SEED};
use crate::measure::{OP_SAMPLES, SEEK_SAMPLES};
use crate::results::{Deterministic, Document, Meta};

const DEFAULT_OUT: &str = "manifest-perf-results.json";

/// Schema and harness version; bump it when a measured field changes meaning.
const HARNESS_VERSION: &str = "1";

struct Args {
    out: PathBuf,
    scales: Vec<u64>,
    build_samples: usize,
}

fn parse_args() -> Args {
    let mut out = PathBuf::from(DEFAULT_OUT);
    let mut scales = vec![1_000u64, 10_000];
    let mut build_samples = 1usize;
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
            "--build-samples" => {
                if let Some(v) = it.next() {
                    build_samples = v.trim().parse().unwrap_or(1);
                }
            }
            _ => {}
        }
    }
    Args {
        out,
        scales,
        build_samples,
    }
}

fn caveats() -> Vec<String> {
    vec![
        "Every figure in the deterministic section is a store-counter delta around one real seam \
call: fetches are get() calls and puts are chunk writes. Nothing there is wall time, and two \
runs of one commit produce it byte for byte."
            .to_string(),
        "Both arms run the same seam calls over their own counting store, so a column is never \
shared state. The corpus is identical by construction and the arms are checked to hold the same \
key set after a build."
            .to_string(),
        "A capability of emulated names a verb the format leaves to the seam default: the trie \
has no ordered seek, so its floor and its bounded range are filters over a full walk, and their \
cost tracks the corpus rather than its depth. Read the label with the cost."
            .to_string(),
        "wall_time is a separate section on purpose. It is a cold-pass build over a plain store \
with no counter in the timed path, it varies by machine and by run, and no figure in it divides \
a deterministic figure."
            .to_string(),
        "The update row rebinds one key against the built root and never advances it, so every \
sample starts from the same tree and the mean is the single-update write amplification."
            .to_string(),
    ]
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = parse_args();
    let mut storage = Vec::new();
    let mut ops = Vec::new();
    let mut wall_time = Vec::new();

    for corpus in Corpus::all() {
        for &scale in &args.scales {
            let keys = corpus::generate(corpus, usize::try_from(scale)?);
            for arm in [&mut mantaray_arm() as &mut dyn Arm, &mut ldb_arm()] {
                eprintln!(
                    "[manifest-perf] {} n={scale} {}",
                    corpus.name(),
                    arm.label()
                );
                let (cell, measured) = measure::measure(corpus, scale, &keys, arm)?;
                storage.push(cell);
                ops.extend(measured);
                wall_time.push(measure::build_time(
                    corpus,
                    scale,
                    &keys,
                    arm,
                    args.build_samples,
                )?);
            }
        }
    }

    let doc = Document {
        meta: Meta {
            run: RunMeta::current(HARNESS_VERSION),
            seed_master: format!("0x{MASTER_SEED:016x}"),
            scales: args.scales.clone(),
            corpora: Corpus::all().iter().map(|c| c.name().to_string()).collect(),
            arms: vec!["mantaray".to_string(), "ldb".to_string()],
            op_samples: OP_SAMPLES as u64,
            build_samples: args.build_samples as u64,
            caveats: caveats(),
        },
        deterministic: Deterministic { storage, ops },
        wall_time,
    };
    let json = serde_json::to_string_pretty(&doc)?;
    if let Some(parent) = args.out.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&args.out, json.as_bytes())?;
    eprintln!(
        "wrote {} ({} bytes, seeks sampled {SEEK_SAMPLES} per cell)",
        args.out.display(),
        json.len()
    );
    Ok(())
}
