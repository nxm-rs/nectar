#![allow(missing_docs)]
//! Benchmarks for file splitting operations.
//!
//! Measures throughput (bytes/sec) for splitting files into chunks,
//! which helps estimate upload processing time for large files.

use std::io::Write;

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use rand::{RngCore, rng};

use nectar_primitives::file::{ParallelSplitter, Splitter};
use nectar_primitives::store::VecSink;
use nectar_primitives::DEFAULT_BODY_SIZE;

/// File sizes to benchmark, covering typical use cases.
const SIZES: &[(u64, &str)] = &[
    (4 * 1024, "4KB"),                       // Single chunk
    (32 * 1024, "32KB"),                     // Few chunks
    (256 * 1024, "256KB"),                   // Document size
    (1024 * 1024, "1MB"),                    // Small file
    (4 * 1024 * 1024, "4MB"),                // Medium file
    (16 * 1024 * 1024, "16MB"),              // Large file
    (64 * 1024 * 1024, "64MB"),              // Very large file
    (128 * DEFAULT_BODY_SIZE as u64, "128c"), // Exactly 128 chunks (tree boundary)
    (129 * DEFAULT_BODY_SIZE as u64, "129c"), // Just past tree boundary
];

fn bench_sequential_splitter(c: &mut Criterion) {
    let mut group = c.benchmark_group("file_split_sequential");

    for &(size, name) in SIZES {
        // Generate random data
        let mut data = vec![0u8; size as usize];
        rng().fill_bytes(&mut data);

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
        // Generate random data
        let mut data = vec![0u8; size as usize];
        rng().fill_bytes(&mut data);

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

    // Compare at sizes where parallelism matters
    let comparison_sizes: &[(u64, &str)] = &[
        (1024 * 1024, "1MB"),
        (4 * 1024 * 1024, "4MB"),
        (16 * 1024 * 1024, "16MB"),
    ];

    for &(size, name) in comparison_sizes {
        let mut data = vec![0u8; size as usize];
        rng().fill_bytes(&mut data);

        group.throughput(Throughput::Bytes(size));

        // Sequential
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

        // Parallel
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

    // Test with 4MB file
    let size = 4 * 1024 * 1024;
    let mut data = vec![0u8; size];
    rng().fill_bytes(&mut data);

    group.throughput(Throughput::Bytes(size as u64));

    // Write all at once
    group.bench_function("single_write", |b| {
        b.iter(|| {
            let sink = VecSink::<DEFAULT_BODY_SIZE>::new();
            let mut splitter = Splitter::new(sink, data.len() as u64);
            splitter.write_all(&data).unwrap();
            black_box(splitter.finish().unwrap())
        });
    });

    // Write in 4KB chunks (typical buffer size)
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

    // Write in 64KB chunks
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

criterion_group!(
    benches,
    bench_sequential_splitter,
    bench_parallel_splitter,
    bench_splitter_comparison,
    bench_incremental_writes,
);
criterion_main!(benches);
