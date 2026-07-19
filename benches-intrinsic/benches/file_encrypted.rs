//! Encrypted split and join over the memory store, both pipelines.
//!
//! Keys are random on both sides (neither legacy nor streaming injects a
//! deterministic source); payloads stay seeded.

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

const PAYLOADS: [usize; 2] = [1 << 20, 32 << 20];

/// Samples per cell, scaled down only where iterations run long.
const fn samples(len: usize) -> usize {
    if len >= 32 << 20 { 10 } else { 30 }
}

fn encrypted_suite<P: FilePipeline>(c: &mut Criterion) {
    for &len in &PAYLOADS {
        let data = payload(len, SEED);

        {
            let mut group = c.benchmark_group("split-encrypted");
            group.sample_size(samples(len));
            group.throughput(Throughput::Bytes(len as u64));
            group.bench_function(BenchmarkId::new(P::NAME, len), |b| {
                b.iter_batched(
                    CountingStore::new,
                    |store| black_box(P::split_encrypted(&store, &data)),
                    BatchSize::LargeInput,
                );
            });
            group.finish();
        }

        let store = CountingStore::new();
        let root = P::split_encrypted(&store, &data);

        {
            let mut group = c.benchmark_group("join-encrypted");
            group.sample_size(samples(len));
            group.throughput(Throughput::Bytes(len as u64));
            group.bench_function(BenchmarkId::new(P::NAME, len), |b| {
                b.iter(|| black_box(P::join_encrypted(&store, &root)));
            });
            group.finish();
        }

        assert_eq!(P::join_encrypted(&store, &root), data, "join mismatch");
    }
}

criterion_group!(benches, encrypted_suite::<FileStreaming>, encrypted_suite::<FileLegacy>);
criterion_main!(benches);
