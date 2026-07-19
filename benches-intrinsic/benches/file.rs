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

fn file_suite<P: FilePipeline>(c: &mut Criterion) {
    for &len in &PAYLOADS {
        let data = payload(len, SEED);

        {
            let mut group = c.benchmark_group("split");
            group.sample_size(10);
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
            group.sample_size(10);
            group.throughput(Throughput::Bytes(len as u64));
            group.bench_function(BenchmarkId::new(P::NAME, len), |b| {
                b.iter(|| black_box(P::join(&store, &root)));
            });
            group.finish();
        }

        assert_eq!(P::join(&store, &root), data, "join mismatch");
    }
}

criterion_group!(benches, file_suite::<FileStreaming>, file_suite::<FileLegacy>);
criterion_main!(benches);
