#![allow(missing_docs)]
//! Benchmarks for file splitting and joining operations.
//!
//! Measures throughput (bytes/sec) for splitting files into chunks and
//! joining them back, covering plain and encrypted modes, sequential
//! and parallel variants.

use std::collections::HashMap;
use std::io::Write;

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use rand::{RngCore, rng};

use nectar_primitives::chunk::{Chunk, ChunkAddress, ContentChunk};
use nectar_primitives::file::{Joiner, ParallelSplitter, Splitter, split};
use nectar_primitives::store::{MemorySink, VecSink};
use nectar_primitives::DEFAULT_BODY_SIZE;

/// File sizes to benchmark, covering typical use cases.
const SIZES: &[(u64, &str)] = &[
    (4 * 1024, "4KB"),                        // Single chunk
    (32 * 1024, "32KB"),                      // Few chunks
    (256 * 1024, "256KB"),                    // Document size
    (1024 * 1024, "1MB"),                     // Small file
    (4 * 1024 * 1024, "4MB"),                // Medium file
    (16 * 1024 * 1024, "16MB"),              // Large file
    (64 * 1024 * 1024, "64MB"),              // Very large file
    (128 * DEFAULT_BODY_SIZE as u64, "128c"), // Exactly 128 chunks (tree boundary)
    (129 * DEFAULT_BODY_SIZE as u64, "129c"), // Just past tree boundary
];

/// Smaller set for comparison benchmarks.
const COMPARISON_SIZES: &[(u64, &str)] = &[
    (4 * 1024, "4KB"),
    (256 * 1024, "256KB"),
    (1024 * 1024, "1MB"),
    (4 * 1024 * 1024, "4MB"),
    (16 * 1024 * 1024, "16MB"),
];

fn random_data(size: u64) -> Vec<u8> {
    let mut data = vec![0u8; size as usize];
    rng().fill_bytes(&mut data);
    data
}

fn split_to_store(data: &[u8]) -> (ChunkAddress, HashMap<ChunkAddress, ContentChunk>) {
    let (root, chunks) = split::<DEFAULT_BODY_SIZE>(data).unwrap();
    let store: HashMap<ChunkAddress, ContentChunk> =
        chunks.into_iter().map(|c| (*c.address(), c)).collect();
    (root, store)
}

// ---------------------------------------------------------------------------
// Splitting benchmarks
// ---------------------------------------------------------------------------

fn bench_sequential_splitter(c: &mut Criterion) {
    let mut group = c.benchmark_group("file_split_sequential");

    for &(size, name) in SIZES {
        let data = random_data(size);

        group.throughput(Throughput::Bytes(size));
        group.bench_with_input(BenchmarkId::from_parameter(name), &data, |b, data| {
            b.iter(|| {
                let sink = VecSink::<DEFAULT_BODY_SIZE>::new();
                let mut splitter = Splitter::new(sink, data.len() as u64);
                splitter.write_all(data).unwrap();
                black_box(splitter.finish().unwrap())
            });
        });
    }

    group.finish();
}

fn bench_parallel_splitter(c: &mut Criterion) {
    let mut group = c.benchmark_group("file_split_parallel");

    for &(size, name) in SIZES {
        let data = random_data(size);

        group.throughput(Throughput::Bytes(size));
        group.bench_with_input(BenchmarkId::from_parameter(name), &data, |b, data| {
            b.iter(|| {
                let sink = VecSink::<DEFAULT_BODY_SIZE>::new();
                let splitter = ParallelSplitter::new(sink);
                let root = splitter.split(data).unwrap();
                black_box((root, splitter.into_sink()))
            });
        });
    }

    group.finish();
}

fn bench_splitter_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("file_split_comparison");

    for &(size, name) in COMPARISON_SIZES {
        let data = random_data(size);

        group.throughput(Throughput::Bytes(size));

        group.bench_with_input(
            BenchmarkId::new("sequential", name),
            &data,
            |b, data| {
                b.iter(|| {
                    let sink = VecSink::<DEFAULT_BODY_SIZE>::new();
                    let mut splitter = Splitter::new(sink, data.len() as u64);
                    splitter.write_all(data).unwrap();
                    black_box(splitter.finish().unwrap())
                });
            },
        );

        group.bench_with_input(BenchmarkId::new("parallel", name), &data, |b, data| {
            b.iter(|| {
                let sink = VecSink::<DEFAULT_BODY_SIZE>::new();
                let splitter = ParallelSplitter::new(sink);
                let root = splitter.split(data).unwrap();
                black_box((root, splitter.into_sink()))
            });
        });
    }

    group.finish();
}

fn bench_incremental_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("file_split_incremental");

    let size = 4 * 1024 * 1024;
    let data = random_data(size);

    group.throughput(Throughput::Bytes(size));

    group.bench_function("single_write", |b| {
        b.iter(|| {
            let sink = VecSink::<DEFAULT_BODY_SIZE>::new();
            let mut splitter = Splitter::new(sink, data.len() as u64);
            splitter.write_all(&data).unwrap();
            black_box(splitter.finish().unwrap())
        });
    });

    group.bench_function("4kb_chunks", |b| {
        b.iter(|| {
            let sink = VecSink::<DEFAULT_BODY_SIZE>::new();
            let mut splitter = Splitter::new(sink, data.len() as u64);
            for chunk in data.chunks(4096) {
                splitter.write_all(chunk).unwrap();
            }
            black_box(splitter.finish().unwrap())
        });
    });

    group.bench_function("64kb_chunks", |b| {
        b.iter(|| {
            let sink = VecSink::<DEFAULT_BODY_SIZE>::new();
            let mut splitter = Splitter::new(sink, data.len() as u64);
            for chunk in data.chunks(65536) {
                splitter.write_all(chunk).unwrap();
            }
            black_box(splitter.finish().unwrap())
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Joiner benchmarks
// ---------------------------------------------------------------------------

fn bench_joiner(c: &mut Criterion) {
    let mut group = c.benchmark_group("file_join");

    for &(size, name) in SIZES {
        let data = random_data(size);
        let (root, store) = split_to_store(&data);

        group.throughput(Throughput::Bytes(size));
        group.bench_with_input(BenchmarkId::from_parameter(name), &root, |b, root| {
            b.iter(|| {
                let joiner = Joiner::new(store.clone(), *root).unwrap();
                black_box(joiner.read_all().unwrap())
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Round-trip benchmarks (split + join)
// ---------------------------------------------------------------------------

fn bench_roundtrip(c: &mut Criterion) {
    let mut group = c.benchmark_group("file_roundtrip");

    let roundtrip_sizes: &[(u64, &str)] = &[
        (4 * 1024, "4KB"),
        (256 * 1024, "256KB"),
        (1024 * 1024, "1MB"),
        (4 * 1024 * 1024, "4MB"),
    ];

    for &(size, name) in roundtrip_sizes {
        let data = random_data(size);

        group.throughput(Throughput::Bytes(size));

        group.bench_with_input(
            BenchmarkId::new("sequential_split", name),
            &data,
            |b, data| {
                b.iter(|| {
                    let (root, store) = split_to_store(data);
                    let joiner = Joiner::new(store, root).unwrap();
                    black_box(joiner.read_all().unwrap())
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("parallel_split", name),
            &data,
            |b, data| {
                b.iter(|| {
                    let sink = MemorySink::<DEFAULT_BODY_SIZE>::new();
                    let splitter = ParallelSplitter::new(sink);
                    let root = splitter.split(data).unwrap();
                    let store = splitter.into_sink();
                    let joiner = Joiner::new(store, root).unwrap();
                    black_box(joiner.read_all().unwrap())
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_sequential_splitter,
    bench_parallel_splitter,
    bench_splitter_comparison,
    bench_incremental_writes,
    bench_joiner,
    bench_roundtrip,
);
criterion_main!(benches);
