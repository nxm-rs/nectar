#![allow(missing_docs)]
//! Benchmarks for file splitting and joining operations.
//!
//! Measures throughput (bytes/sec) for splitting files into chunks and
//! joining them back, covering plain and encrypted modes, streaming
//! and direct variants.

use std::collections::HashMap;
use std::io::Write;

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use rand::{RngCore, rng};

use nectar_primitives::chunk::{AnyChunk, ChunkAddress};
use nectar_primitives::file::{SyncJoiner, SyncParallelSplitter, SyncSplitter, sync_split};
use nectar_primitives::store::MemoryStore;
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

fn split_to_store(data: &[u8]) -> (ChunkAddress, HashMap<ChunkAddress, AnyChunk>) {
    let (root, store) = sync_split::<DEFAULT_BODY_SIZE>(data).unwrap();
    (root, store.into_chunks())
}

/// Benchmark the streaming Write-based splitter (buffers then splits in parallel).
fn bench_streaming_splitter(c: &mut Criterion) {
    let mut group = c.benchmark_group("file_split_streaming");

    for &(size, name) in SIZES {
        let data = random_data(size);

        group.throughput(Throughput::Bytes(size));
        group.bench_with_input(BenchmarkId::from_parameter(name), &data, |b, data| {
            b.iter(|| {
                let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
                let mut splitter = SyncSplitter::new(store, data.len() as u64);
                splitter.write_all(data).unwrap();
                black_box(splitter.finish().unwrap())
            });
        });
    }

    group.finish();
}

/// Benchmark the direct parallel splitter (random-access source).
fn bench_parallel_splitter(c: &mut Criterion) {
    let mut group = c.benchmark_group("file_split_parallel");

    for &(size, name) in SIZES {
        let data = random_data(size);

        group.throughput(Throughput::Bytes(size));
        group.bench_with_input(BenchmarkId::from_parameter(name), &data, |b, data| {
            b.iter(|| {
                let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
                let splitter = SyncParallelSplitter::new(store);
                let root = splitter.split(data).unwrap();
                black_box((root, splitter.into_store()))
            });
        });
    }

    group.finish();
}

/// Compare streaming vs direct split to measure buffering overhead.
fn bench_splitter_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("file_split_comparison");

    for &(size, name) in COMPARISON_SIZES {
        let data = random_data(size);

        group.throughput(Throughput::Bytes(size));

        group.bench_with_input(
            BenchmarkId::new("streaming", name),
            &data,
            |b, data| {
                b.iter(|| {
                    let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
                    let mut splitter = SyncSplitter::new(store, data.len() as u64);
                    splitter.write_all(data).unwrap();
                    black_box(splitter.finish().unwrap())
                });
            },
        );

        group.bench_with_input(BenchmarkId::new("direct", name), &data, |b, data| {
            b.iter(|| {
                let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
                let splitter = SyncParallelSplitter::new(store);
                let root = splitter.split(data).unwrap();
                black_box((root, splitter.into_store()))
            });
        });
    }

    group.finish();
}

/// Benchmark incremental Write calls to measure buffering overhead.
fn bench_incremental_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("file_split_incremental");

    let size = 4 * 1024 * 1024;
    let data = random_data(size);

    group.throughput(Throughput::Bytes(size));

    group.bench_function("single_write", |b| {
        b.iter(|| {
            let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
            let mut splitter = SyncSplitter::new(store, data.len() as u64);
            splitter.write_all(&data).unwrap();
            black_box(splitter.finish().unwrap())
        });
    });

    group.bench_function("4kb_chunks", |b| {
        b.iter(|| {
            let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
            let mut splitter = SyncSplitter::new(store, data.len() as u64);
            for chunk in data.chunks(4096) {
                splitter.write_all(chunk).unwrap();
            }
            black_box(splitter.finish().unwrap())
        });
    });

    group.bench_function("64kb_chunks", |b| {
        b.iter(|| {
            let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
            let mut splitter = SyncSplitter::new(store, data.len() as u64);
            for chunk in data.chunks(65536) {
                splitter.write_all(chunk).unwrap();
            }
            black_box(splitter.finish().unwrap())
        });
    });

    group.finish();
}

fn bench_joiner(c: &mut Criterion) {
    let mut group = c.benchmark_group("file_join");

    for &(size, name) in SIZES {
        let data = random_data(size);
        let (root, store) = split_to_store(&data);

        group.throughput(Throughput::Bytes(size));
        group.bench_with_input(BenchmarkId::from_parameter(name), &root, |b, root| {
            b.iter(|| {
                let joiner = SyncJoiner::new(store.clone(), *root).unwrap();
                black_box(joiner.read_all().unwrap())
            });
        });
    }

    group.finish();
}

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
            BenchmarkId::new("streaming_split", name),
            &data,
            |b, data| {
                b.iter(|| {
                    let (root, store) = split_to_store(data);
                    let joiner = SyncJoiner::new(store, root).unwrap();
                    black_box(joiner.read_all().unwrap())
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("direct_split", name),
            &data,
            |b, data| {
                b.iter(|| {
                    let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
                    let splitter = SyncParallelSplitter::new(store);
                    let root = splitter.split(data).unwrap();
                    let store = splitter.into_store();
                    let joiner = SyncJoiner::new(store, root).unwrap();
                    black_box(joiner.read_all().unwrap())
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_streaming_splitter,
    bench_parallel_splitter,
    bench_splitter_comparison,
    bench_incremental_writes,
    bench_joiner,
    bench_roundtrip,
);
criterion_main!(benches);
