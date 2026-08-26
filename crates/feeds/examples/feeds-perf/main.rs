//! Drive the finder lookup-cost measurements across every
//! `(finder, length, width)` and write one JSON result document. Every
//! number is a measured work count; rounds are read off the real windowed
//! finder under a paused virtual clock, one tick per retrieval.
//!
//! Run: `cargo run -p nectar-feeds --example feeds-perf`
#![allow(
    unreachable_pub,
    clippy::arithmetic_side_effects,
    clippy::as_conversions,
    clippy::expect_used,
    clippy::unwrap_used
)]

mod corpus;
mod measure;
mod reference;
mod results;
mod store;

use core::error::Error;
use core::num::NonZeroUsize;
use std::path::PathBuf;

use nectar_testing::bench::RunMeta;

use crate::corpus::{Corpus, TOPIC_LABEL};
use crate::measure::{FinderKind, LENGTHS, LINEAR_BUDGET, WIDTHS};
use crate::results::{Document, FinderCell, Meta};

const DEFAULT_OUT: &str = "feeds-perf-results.json";

/// Schema and harness version; bump it when a measured field changes meaning.
const HARNESS_VERSION: &str = "2";

struct Args {
    out: PathBuf,
    lengths: Vec<u64>,
}

fn parse_args() -> Args {
    let mut out = PathBuf::from(DEFAULT_OUT);
    let mut lengths = LENGTHS.to_vec();
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        match a.as_str() {
            "--out" => {
                if let Some(v) = it.next() {
                    out = PathBuf::from(v);
                }
            }
            "--lengths" => {
                if let Some(v) = it.next() {
                    lengths = v.split(',').filter_map(|s| s.trim().parse().ok()).collect();
                }
            }
            _ => {}
        }
    }
    Args { out, lengths }
}

fn caveats() -> Vec<String> {
    vec![
        "Every figure is a measured work count from driving the real reader over a counting \
probe store; nothing is wall time. rounds is elapsed virtual time under a paused clock with \
one tick per retrieval, probe and certified commit alike, so the probes of one concurrent \
batch collapse into one round, and one round is one network round trip."
            .to_string(),
        "total_probes counts presence probes, speculation included: the chunks required for the \
lookup. wasted_probes counts probes answered absent (at or past the first free slot); the \
sequential scan itself needs bracketing misses, so a width's speculation overhead is its excess \
over the width-one figure."
            .to_string(),
        "verified_gets is the certified retrieval of the committed boundary update; presence \
answers are unverified, per the finder's documented absence caveat."
            .to_string(),
        "Stepwise cells whose replay work n^2/(2*width) exceeds linear_budget are null with a \
reason: the stepwise replay recomputes its frontier from the floor each round, so measuring \
those cells is quadratic even though the finder's own probe and round counts are linear."
            .to_string(),
        "Wire indexing is shared with the reference client (8-byte big-endian index, \
keccak(topic || index) id, keccak(id || owner) address); the comparison is lookup strategy only."
            .to_string(),
        "The reference series is a faithful port of the reference client's concurrent finder \
(fixed eight-way lookahead at offsets 2^k - 1 per interval) driven over the same probe store. \
In the original every probe is a full retrieval with absence inferred from a timeout; the port \
pays the same full-retrieval cost per probe and only its absence detection is free, a \
classified not-found against the timeout. verified_gets is zero because the port never \
certifies."
            .to_string(),
    ]
}

fn reference_comparison() -> Vec<String> {
    vec![
        "The reference client's concurrent finder probes the interval (base, base + 2^levels) at \
offsets 2^k - 1 for k = 1..levels, with levels = 8 concurrent lookaheads per batch, recursing \
into the subinterval of the highest update found; each probe is a full retrieval whose absence \
is inferred from a per-probe timeout. The reference series measures a faithful port of it over \
the same counting store."
            .to_string(),
        "Rounds: below 2^levels = 256 updates both strategies converge in a logarithmic number \
of batches, so they are broadly equal for small feeds. Past 256 the reference interval base \
advances by at most 2^levels - 1 slots per batch, so its measured batch count grows linearly in \
n (about n / 255: see the reference cells from n = 1000 up), while the ladder here doubles per \
rung and stays logarithmic at every scale: see the single-digit measured rounds at n = 10^6, \
width >= 16."
            .to_string(),
        "Probes: the reference issues its full fan-out of up to levels retrievals per batch, \
about levels * n / (2^levels - 1) in total for large n (see the reference cells). Width one \
here is exactly the sequential exponential-then-binary scan, 2 * ceil(log2 n) + 1 probes for \
n >= 2; wider windows add speculation bounded by the window per round, visible in \
wasted_probes."
            .to_string(),
        "Where the reference is better or equal: on a feed a few updates long a wide window here \
speculates far up the ladder and wastes more probes than the reference's fixed eight lookaheads \
(see wasted_probes at n = 1, width = 64), a probe-count loss bounded by one window; rounds are \
never worse. Both now pay the full-retrieval cost per probe; the difference is absence \
detection, a classified not-found against a timeout, which the counts do not credit."
            .to_string(),
        "The per-length verdicts and the measured comparison table live in results/COMPARISON.md \
next to this document."
            .to_string(),
    ]
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = parse_args();
    let max = args.lengths.iter().copied().max().unwrap_or(0);
    eprintln!("[feeds-perf] building corpus for {max} slots");
    let corpus = Corpus::new(max);

    let mut latest = Vec::new();
    let mut linear = Vec::new();
    let mut reference_cells = Vec::new();
    for &n in &args.lengths {
        eprintln!("[feeds-perf] reference n={n}");
        reference_cells.push(FinderCell::measured(reference::measure(&corpus, n)?));
        for w in WIDTHS {
            let width = NonZeroUsize::new(w).ok_or("zero width")?;
            eprintln!("[feeds-perf] latest n={n} w={w}");
            latest.push(FinderCell::measured(measure::measure(
                &corpus,
                FinderKind::Probing,
                n,
                width,
            )?));
            if measure::linear_feasible(n, w) {
                eprintln!("[feeds-perf] linear n={n} w={w}");
                linear.push(FinderCell::measured(measure::measure(
                    &corpus,
                    FinderKind::Stepwise,
                    n,
                    width,
                )?));
            } else {
                linear.push(FinderCell::gap(
                    n,
                    w,
                    format!(
                        "stepwise replay work n^2/(2*width) exceeds the {LINEAR_BUDGET} budget; \
the replay recomputes its frontier from the floor each round"
                    ),
                ));
            }
        }
    }

    let meta = Meta {
        run: RunMeta::current(HARNESS_VERSION),
        topic_label: TOPIC_LABEL.to_string(),
        owner: corpus.feed().owner().to_string(),
        widths: WIDTHS.to_vec(),
        lengths: args.lengths.clone(),
        linear_budget: LINEAR_BUDGET,
        caveats: caveats(),
    };

    let doc = Document {
        meta,
        latest,
        linear,
        reference: reference_cells,
        reference_comparison: reference_comparison(),
    };
    let json = serde_json::to_string_pretty(&doc)?;
    if let Some(parent) = args.out.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&args.out, json.as_bytes())?;
    eprintln!("wrote {} ({} bytes)", args.out.display(), json.len());
    Ok(())
}
