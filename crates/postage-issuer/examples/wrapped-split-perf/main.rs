//! Wrapped-split throughput: a plain split against both stamping decorators,
//! swept over signer latency and put-window width.
//!
//! Run: `cargo run --release -p nectar-postage-issuer --example wrapped-split-perf`.
//! A debug build measures its own hashing, not the pipeline.
//!
//! Flags: `--bytes`, `--latencies` (ms), `--put-windows`, `--sign-window`.
#![allow(
    unreachable_pub,
    clippy::arithmetic_side_effects,
    clippy::as_conversions,
    clippy::indexing_slicing,
    clippy::unwrap_used
)]

mod arms;
mod report;

use std::time::Duration;

use nectar_testing::bench::RunMeta;

use crate::arms::{Outcome, corpus, plain, staged, staged_refusal, stamped, stamped_refusal};
use crate::report::{Row, render};

/// Schema and harness version; bump it when a measured field changes meaning.
const HARNESS_VERSION: &str = "1";

struct Args {
    bytes: usize,
    latencies: Vec<Duration>,
    put_windows: Vec<u16>,
    sign_window: u16,
}

fn parse_args() -> Args {
    let mut args = Args {
        bytes: 1024 * 1024,
        latencies: vec![0, 10, 50]
            .into_iter()
            .map(Duration::from_millis)
            .collect(),
        put_windows: vec![16, 64, 256],
        sign_window: 256,
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        match flag.as_str() {
            "--bytes" => {
                if let Some(value) = it.next() {
                    args.bytes = value.trim().parse().unwrap_or(args.bytes);
                }
            }
            "--latencies" => {
                if let Some(value) = it.next() {
                    args.latencies = value
                        .split(',')
                        .filter_map(|ms| ms.trim().parse().ok())
                        .map(Duration::from_millis)
                        .collect();
                }
            }
            // Zero is not a window, so a zero here would panic the arms.
            "--put-windows" => {
                if let Some(value) = it.next() {
                    args.put_windows = value
                        .split(',')
                        .filter_map(|slots| slots.trim().parse().ok())
                        .filter(|&slots| slots > 0)
                        .collect();
                }
            }
            "--sign-window" => {
                if let Some(value) = it.next() {
                    args.sign_window = value
                        .trim()
                        .parse()
                        .ok()
                        .filter(|&slots| slots > 0)
                        .unwrap_or(args.sign_window);
                }
            }
            _ => {}
        }
    }
    args
}

const fn row(
    arm: &'static str,
    latency: Duration,
    puts: u16,
    bytes: usize,
    outcome: Outcome,
) -> Row {
    Row {
        arm,
        latency,
        put_window: puts,
        bytes,
        outcome,
        of_plain: 1.0,
    }
}

fn main() {
    let args = parse_args();
    let data = corpus(args.bytes);
    let mut rows = Vec::new();
    let mut plain_rate = 0.0f64;

    for &puts in &args.put_windows {
        let baseline = plain(&data, puts);
        let rate = baseline.bytes_per_second(args.bytes);
        plain_rate = plain_rate.max(rate);
        rows.push(row("plain", Duration::ZERO, puts, args.bytes, baseline));

        for &latency in &args.latencies {
            for (arm, outcome) in [
                ("stamped", stamped(&data, puts, latency)),
                ("staged", staged(&data, puts, latency, args.sign_window)),
            ] {
                let mut cell = row(arm, latency, puts, args.bytes, outcome);
                cell.of_plain = if rate > 0.0 {
                    outcome.bytes_per_second(args.bytes) / rate
                } else {
                    0.0
                };
                rows.push(cell);
            }
        }
    }

    println!(
        "{}",
        render(
            &RunMeta::current(HARNESS_VERSION),
            args.bytes,
            args.sign_window,
            &rows,
            plain_rate,
            (stamped_refusal(), staged_refusal()),
        )
    );
}
