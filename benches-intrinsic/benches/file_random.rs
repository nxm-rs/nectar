//! Random access over a 32 MiB file, both pipelines, plus the streaming
//! ordered-drain (collect) versus completion-order (download) comparison.

#![allow(missing_docs)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::panic,
    clippy::as_conversions
)]

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use nectar_benches_intrinsic::corpus::{self, SEED, payload};
use nectar_benches_intrinsic::file_api::{
    FileLegacy, FilePipeline, FileStreaming, streaming_download_unordered,
};
use nectar_benches_intrinsic::store::CountingStore;

const FILE_LEN: usize = 32 << 20;
const READS: usize = 256;
const READ_LEN: usize = 16 << 10;

fn random_access_suite<P: FilePipeline>(c: &mut Criterion) {
    let data = payload(FILE_LEN, SEED);
    let store = CountingStore::new();
    let root = P::split(&store, &data);
    let ranges = corpus::read_ranges(FILE_LEN, READS, READ_LEN, SEED);

    let out = P::read_ranges(&store, &root, &ranges);
    let mut expected = Vec::with_capacity(READS * READ_LEN);
    for &(offset, len) in &ranges {
        let start = usize::try_from(offset).unwrap();
        expected.extend_from_slice(&data[start..start + len]);
    }
    assert_eq!(out, expected, "range read mismatch");

    let mut group = c.benchmark_group("random-access");
    group.sample_size(10);
    group.throughput(Throughput::Bytes((READS * READ_LEN) as u64));
    group.bench_function(
        BenchmarkId::new(P::NAME, format!("{FILE_LEN}/{READS}x{READ_LEN}")),
        |b| b.iter(|| black_box(P::read_ranges(&store, &root, &ranges))),
    );
    group.finish();
}

fn drain_suite(c: &mut Criterion) {
    let data = payload(FILE_LEN, SEED);
    let store = CountingStore::new();
    let root = FileStreaming::split(&store, &data);
    assert_eq!(streaming_download_unordered(&store, &root), data, "download mismatch");

    let mut group = c.benchmark_group("drain");
    group.sample_size(10);
    group.throughput(Throughput::Bytes(FILE_LEN as u64));
    group.bench_function(BenchmarkId::new("collect-ordered", FILE_LEN), |b| {
        b.iter(|| black_box(FileStreaming::join(&store, &root)));
    });
    group.bench_function(BenchmarkId::new("download-unordered", FILE_LEN), |b| {
        b.iter(|| black_box(streaming_download_unordered(&store, &root)));
    });
    group.finish();
}

criterion_group!(
    benches,
    random_access_suite::<FileStreaming>,
    random_access_suite::<FileLegacy>,
    drain_suite
);
criterion_main!(benches);
