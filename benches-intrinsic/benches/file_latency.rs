//! Latency-shaped join: wall time under a simulated per-get RTT, plus the
//! observed fetch concurrency of each pipeline's discipline.
//!
//! RTTs are real short sleeps on a shared timer thread (see the delay
//! module). Fetch depth is swept explicitly on both sides so matched-depth
//! cells separate pipeline design from the shipped default window (streaming
//! defaults to 16, legacy to 8). The axes are pruned to keep the run
//! bounded: the 5 ms RTT stays on the 1 MiB payload only, and 32 MiB runs
//! at the lowest RTT; the worst cell is roughly half a second per
//! iteration. The RTT floor sits at 0.5 ms, above condvar wake granularity.

#![allow(missing_docs)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::panic,
    clippy::as_conversions
)]

use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use nectar_benches_intrinsic::corpus::{SEED, payload};
use nectar_benches_intrinsic::delay::DelayStore;
use nectar_benches_intrinsic::file_api::{FileLegacy, FilePipeline, FileStreaming};
use nectar_benches_intrinsic::results::Suite;
use nectar_benches_intrinsic::store::CountingStore;

const DEPTHS: [u16; 2] = [8, 16];

/// `(payload, RTTs)` cells; the long-RTT axis stays on the small payload.
const CELLS: [(usize, &[u64]); 2] = [(1 << 20, &[500, 1_000, 5_000]), (32 << 20, &[500])];

/// Shared JSONL sink; created (truncating) once per process.
fn suite() -> &'static Mutex<Suite> {
    static SUITE: OnceLock<Mutex<Suite>> = OnceLock::new();
    SUITE.get_or_init(|| Mutex::new(Suite::create("file-latency")))
}

fn latency_suite<P: FilePipeline>(c: &mut Criterion) {
    for &(len, rtts) in &CELLS {
        let data = payload(len, SEED);
        let base = CountingStore::new();
        let root = P::split(&base, &data);

        let mut group = c.benchmark_group("latency-join");
        group.sample_size(10);
        group.warm_up_time(Duration::from_millis(300));
        // Honest budgets: 10 samples of the worst cell fit inside the
        // stated window instead of silently overrunning it.
        group.measurement_time(Duration::from_secs(if len > (1 << 20) { 6 } else { 2 }));
        group.throughput(Throughput::Bytes(len as u64));
        for &rtt_us in rtts {
            let store = DelayStore::new(base.clone(), Duration::from_micros(rtt_us));
            for &depth in &DEPTHS {
                group.bench_function(
                    BenchmarkId::new(P::NAME, format!("{len}/rtt-{rtt_us}us/d{depth}")),
                    |b| b.iter(|| black_box(P::join_depth(&store, &root, depth))),
                );
                // One counted run outside the timing loop: correctness plus
                // the pipeline's observed fetch concurrency at this depth.
                store.gauge().reset();
                assert_eq!(P::join_depth(&store, &root, depth), data, "join mismatch");
                let in_flight = store.gauge().max_in_flight();
                suite().lock().unwrap().emit(
                    P::NAME,
                    &format!("latency-join/{len}/rtt-{rtt_us}us/d{depth}"),
                    "max_in_flight",
                    in_flight as f64,
                    "gets",
                );
                eprintln!(
                    "latency-join/{}/{len}/rtt-{rtt_us}us/d{depth} max_in_flight={in_flight}",
                    P::NAME,
                );
            }
        }
        group.finish();
    }
}

criterion_group!(benches, latency_suite::<FileStreaming>, latency_suite::<FileLegacy>);
criterion_main!(benches);
