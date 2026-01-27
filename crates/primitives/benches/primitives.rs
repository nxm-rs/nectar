#![allow(missing_docs)]
use alloy_primitives::keccak256;
use bytes::BytesMut;
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use digest::Digest;
use nectar_primitives::bmt::Hasher;
use rand::prelude::*;

pub fn primitives(c: &mut Criterion) {
    let mut group = c.benchmark_group("primitives");
    let mut rng = rand::rng();

    // Create 4096-byte random data for benchmarking
    let random_data: Vec<u8> = (0..4096).map(|_| rng.random::<u8>()).collect();

    // Benchmark baseline Keccak256 performance on 4096 bytes
    group.bench_function("hash_baseline_4096", |b| {
        b.iter(|| {
            black_box(keccak256(&random_data));
        })
    });

    // Benchmark the BMT implementation (non-concurrent) on 4096 bytes
    group.bench_function("bmt_hash_4096", |b| {
        b.iter(|| {
            let mut hasher = Hasher::new();
            hasher.set_span(4096);
            hasher.update(&random_data);
            black_box(hasher.sum());
        })
    });

    // Benchmark the BMT implementation with different data sizes for reference
    let sizes = [128, 512, 1024, 2048, 4096];
    for size in sizes {
        let data = vec![0x42; size];
        group.bench_with_input(BenchmarkId::new("bmt_by_size", size), &size, |b, &size| {
            b.iter(|| {
                let mut hasher = Hasher::new();
                hasher.set_span(size as u64);
                hasher.update(&data);
                black_box(hasher.finalize());
            });
        });
    }

    // Create 4096-byte deterministic data for consistent benchmarking
    let fixed_data = vec![0x42; 4096];

    // Benchmark with new hasher for each iteration (4096 bytes)
    group.bench_function("bmt_new_hasher_4096", |b| {
        b.iter(|| {
            let mut hasher = Hasher::new();
            hasher.set_span(4096);
            hasher.update(&fixed_data);
            black_box(hasher.finalize());
        });
    });

    // Benchmark with reused hasher (4096 bytes)
    group.bench_function("bmt_reused_hasher_4096", |b| {
        let mut hasher = Hasher::new();
        b.iter(|| {
            hasher.set_span(4096);
            hasher.update(&fixed_data);
            // finalize_reset() computes hash AND resets internal state
            black_box(hasher.finalize_reset());
        });
    });

    // Benchmark batch hashing with different operation counts - all using 4096 bytes
    let batch_sizes = [10, 100, 500];

    for &batch_size in &batch_sizes {
        // Benchmark with multiple hashes per iteration using new hasher each time
        group.bench_with_input(
            BenchmarkId::new("bmt_batch_new_hasher", batch_size),
            &batch_size,
            |b, &size| {
                b.iter(|| {
                    for _ in 0..size {
                        let mut hasher = Hasher::new();
                        hasher.set_span(4096);
                        hasher.update(&fixed_data);
                        black_box(hasher.finalize());
                    }
                });
            },
        );

        // Benchmark with multiple hashes per iteration using a reused hasher
        group.bench_with_input(
            BenchmarkId::new("bmt_batch_reused_hasher", batch_size),
            &batch_size,
            |b, &size| {
                let mut hasher = Hasher::new();
                b.iter(|| {
                    for _ in 0..size {
                        hasher.set_span(4096);
                        hasher.update(&fixed_data);
                        black_box(hasher.finalize_reset());
                    }
                });
            },
        );

        // Benchmark with multiple hashes per iteration simulating streaming writes
        group.bench_with_input(
            BenchmarkId::new("bmt_batch_streaming", batch_size),
            &batch_size,
            |b, &size| {
                let mut hasher = Hasher::new();
                // Simulate streaming by writing in chunks of 1024 bytes
                let chunks = [
                    &fixed_data[0..1024],
                    &fixed_data[1024..2048],
                    &fixed_data[2048..3072],
                    &fixed_data[3072..4096],
                ];

                b.iter(|| {
                    for _ in 0..size {
                        hasher.set_span(4096);
                        for chunk in &chunks {
                            hasher.update(chunk);
                        }
                        black_box(hasher.finalize_reset());
                    }
                });
            },
        );
    }

    // Benchmark Write trait implementation efficiency
    group.bench_function("bmt_write_trait_4096", |b| {
        use std::io::Write;
        let mut hasher = Hasher::new();

        b.iter(|| {
            hasher.set_span(4096);
            // Write the 4096 bytes in a single operation
            hasher.write_all(&fixed_data).unwrap();
            black_box(hasher.finalize_reset());
        });
    });

    // Benchmark Write trait with streaming writes
    group.bench_function("bmt_write_streaming_4096", |b| {
        use std::io::Write;
        let mut hasher = Hasher::new();
        let chunks = [
            &fixed_data[0..1024],
            &fixed_data[1024..2048],
            &fixed_data[2048..3072],
            &fixed_data[3072..4096],
        ];

        b.iter(|| {
            hasher.set_span(4096);
            for chunk in &chunks {
                hasher.write_all(chunk).unwrap();
            }
            black_box(hasher.finalize_reset());
        });
    });

    // Benchmark with zero buffer allocation (direct buffer filling)
    group.bench_function("bmt_zero_alloc_4096", |b| {
        // Pre-allocate a buffer to the exact size needed
        let mut buffer = BytesMut::with_capacity(4096);
        buffer.resize(4096, 0x42);
        let data = buffer.freeze();

        let mut hasher = Hasher::new();
        b.iter(|| {
            hasher.set_span(4096);
            hasher.update(data.as_ref());
            black_box(hasher.finalize_reset());
        });
    });

    group.finish();
}

criterion_group!(benches, primitives);
criterion_main!(benches);
