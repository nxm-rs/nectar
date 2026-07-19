//! File timing suite: streaming pipeline vs legacy splitter and joiner.

#![allow(missing_docs)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::panic,
    clippy::as_conversions
)]

use criterion::{
    BatchSize, BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main,
};
use nectar_benches_intrinsic::corpus::{SEED, payload};
use nectar_benches_intrinsic::file_api::{FileLegacy, FilePipeline, FileStreaming};
use nectar_benches_intrinsic::store::CountingStore;

const PAYLOADS: [usize; 3] = [4 << 10, 1 << 20, 32 << 20];

/// Samples per cell: generous where iterations are cheap, minimal for the
/// 32 MiB cells so the suite stays bounded.
const fn samples(len: usize) -> usize {
    if len >= 32 << 20 { 10 } else { 50 }
}

/// Both pipelines must chunk every plain payload to the same root, or the
/// join cells would compare walks over different trees.
fn split_parity(_c: &mut Criterion) {
    for &len in &PAYLOADS {
        let data = payload(len, SEED);
        let streaming = FileStreaming::split(&CountingStore::new(), &data);
        let legacy = FileLegacy::split(&CountingStore::new(), &data);
        assert_eq!(streaming, legacy, "split roots diverged at {len}");
    }
}

fn file_suite<P: FilePipeline>(c: &mut Criterion) {
    for &len in &PAYLOADS {
        let data = payload(len, SEED);

        {
            let mut group = c.benchmark_group("split");
            group.sample_size(samples(len));
            group.throughput(Throughput::Bytes(len as u64));
            group.bench_function(BenchmarkId::new(P::NAME, len), |b| {
                b.iter_batched(
                    CountingStore::new,
                    |store| black_box(P::split(&store, &data)),
                    BatchSize::LargeInput,
                );
            });
            group.finish();
        }

        let store = CountingStore::new();
        let root = P::split(&store, &data);

        {
            let mut group = c.benchmark_group("join");
            group.sample_size(samples(len));
            group.throughput(Throughput::Bytes(len as u64));
            group.bench_function(BenchmarkId::new(P::NAME, len), |b| {
                b.iter(|| black_box(P::join(&store, &root)));
            });
            group.finish();
        }

        assert_eq!(P::join(&store, &root), data, "join mismatch");
    }
}

criterion_group!(benches, split_parity, file_suite::<FileStreaming>, file_suite::<FileLegacy>);
criterion_main!(benches);
