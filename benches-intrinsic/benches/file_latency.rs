//! Latency-shaped join: wall time under a simulated per-get RTT, plus the
//! observed fetch concurrency of each pipeline's discipline.
//!
//! RTTs are real short sleeps on a shared timer thread (see the delay
//! module), kept small so runs stay bounded.

#![allow(missing_docs)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::panic,
    clippy::as_conversions
)]

use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use nectar_benches_intrinsic::corpus::{SEED, payload};
use nectar_benches_intrinsic::delay::DelayStore;
use nectar_benches_intrinsic::file_api::{FileLegacy, FilePipeline, FileStreaming};
use nectar_benches_intrinsic::store::CountingStore;

const PAYLOADS: [usize; 2] = [1 << 20, 32 << 20];
const RTTS_US: [u64; 3] = [200, 1_000, 5_000];

fn latency_suite<P: FilePipeline>(c: &mut Criterion) {
    for &len in &PAYLOADS {
        let data = payload(len, SEED);
        let base = CountingStore::new();
        let root = P::split(&base, &data);

        let mut group = c.benchmark_group("latency-join");
        group.sample_size(10);
        group.warm_up_time(Duration::from_millis(300));
        group.measurement_time(Duration::from_secs(2));
        group.throughput(Throughput::Bytes(len as u64));
        for &rtt_us in &RTTS_US {
            let store = DelayStore::new(base.clone(), Duration::from_micros(rtt_us));
            group.bench_function(
                BenchmarkId::new(P::NAME, format!("{len}/rtt-{rtt_us}us")),
                |b| b.iter(|| black_box(P::join(&store, &root))),
            );
            // One counted run outside the timing loop: correctness plus the
            // pipeline's observed fetch concurrency at this RTT.
            store.gauge().reset();
            assert_eq!(P::join(&store, &root), data, "join mismatch");
            eprintln!(
                "latency-join/{}/{len}/rtt-{rtt_us}us max_in_flight={}",
                P::NAME,
                store.gauge().max_in_flight()
            );
        }
        group.finish();
    }
}

criterion_group!(benches, latency_suite::<FileStreaming>, latency_suite::<FileLegacy>);
criterion_main!(benches);
