#![allow(missing_docs)]
use alloy_primitives::B256;
use alloy_signer_local::PrivateKeySigner;
use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rand::{RngCore, rng};

use nectar_primitives::bmt::Prover;
use nectar_primitives::{
    DEFAULT_BODY_SIZE, DefaultContentChunk, DefaultHasher, DefaultSingleOwnerChunk,
};

fn bench_bmt_hash(c: &mut Criterion) {
    let mut group = c.benchmark_group("bmt_hash");

    // Test power-of-2 aligned sizes (BMT rounds up to these internally)
    for size in [64, 128, 256, 512, 1024, 2048, 4096].iter() {
        // Generate random data
        let mut data = vec![0u8; *size];
        rng().fill_bytes(&mut data);

        group.bench_with_input(BenchmarkId::from_parameter(size), &data, |b, data| {
            b.iter(|| {
                let mut hasher = DefaultHasher::new();
                hasher.set_span(data.len() as u64);
                hasher.update(data);
                hasher.sum()
            });
        });
    }

    group.finish();
}

fn bench_content_chunk_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("content_chunk");

    // Test different data sizes
    for size in [128, 1024, 4096].iter() {
        // Generate random data
        let mut data = vec![0u8; *size];
        rng().fill_bytes(&mut data);

        group.bench_with_input(BenchmarkId::from_parameter(size), &data, |b, data| {
            b.iter(|| DefaultContentChunk::new(data.clone()).unwrap());
        });
    }

    group.finish();
}

fn bench_bmt_proof(c: &mut Criterion) {
    let mut group = c.benchmark_group("bmt_proof");

    // Generate full-size data
    let mut data = vec![0u8; DEFAULT_BODY_SIZE];
    rng().fill_bytes(&mut data);

    // Create hasher and calculate root hash
    let mut hasher = DefaultHasher::new();
    hasher.set_span(data.len() as u64);
    hasher.update(&data);
    let root_hash = hasher.sum();

    // Generate a proof for segment 0
    let proof = hasher.generate_proof(&data, 0).unwrap();

    // Benchmark proof generation
    group.bench_function("generate", |b| {
        b.iter(|| hasher.generate_proof(&data, 0).unwrap());
    });

    // Benchmark proof verification
    group.bench_function("verify", |b| {
        b.iter(|| DefaultHasher::verify_proof(&proof, root_hash.as_slice()).unwrap());
    });

    group.finish();
}

fn bench_large_update(c: &mut Criterion) {
    let mut group = c.benchmark_group("bmt_update");

    // Generate full-size data
    let mut data = vec![0u8; DEFAULT_BODY_SIZE];
    rng().fill_bytes(&mut data);

    // Benchmark single large update
    group.bench_function("single_large", |b| {
        b.iter(|| {
            let mut hasher = DefaultHasher::new();
            hasher.set_span(data.len() as u64);
            hasher.update(&data);
            hasher.sum()
        });
    });

    // Benchmark multiple small updates
    group.bench_function("multiple_small", |b| {
        b.iter(|| {
            let mut hasher = DefaultHasher::new();
            hasher.set_span(data.len() as u64);

            // Split data into 32 chunks
            let chunk_size = data.len() / 32;
            for i in 0..32 {
                let start = i * chunk_size;
                let end = if i == 31 {
                    data.len()
                } else {
                    (i + 1) * chunk_size
                };
                hasher.update(&data[start..end]);
            }

            hasher.sum()
        });
    });

    group.finish();
}

fn bench_single_owner_chunk_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_owner_chunk");

    // Create a random signer for signing chunks
    let signer = PrivateKeySigner::random();

    // Test different data sizes
    for size in [128, 1024, 4096].iter() {
        // Generate random data and ID
        let mut data = vec![0u8; *size];
        rng().fill_bytes(&mut data);
        let id = B256::random();

        group.bench_with_input(BenchmarkId::from_parameter(size), &data, |b, data| {
            b.iter(|| DefaultSingleOwnerChunk::new(id, data.clone(), &signer).unwrap());
        });
    }

    group.finish();
}

fn bench_chunk_deserialization(c: &mut Criterion) {
    let mut group = c.benchmark_group("chunk_deserialization");

    // Create test chunks for deserialization
    let signer = PrivateKeySigner::random();

    for size in [128, 1024, 4096].iter() {
        let mut data = vec![0u8; *size];
        rng().fill_bytes(&mut data);

        // Create content chunk and serialize
        let content_chunk = DefaultContentChunk::new(data.clone()).unwrap();
        let content_bytes: Bytes = content_chunk.into();

        group.bench_with_input(
            BenchmarkId::new("content_chunk", size),
            &content_bytes,
            |b, bytes| {
                b.iter(|| DefaultContentChunk::try_from(bytes.clone()).unwrap());
            },
        );

        // Create single-owner chunk and serialize
        let id = B256::random();
        let soc = DefaultSingleOwnerChunk::new(id, data.clone(), &signer).unwrap();
        let soc_bytes: Bytes = soc.into();

        group.bench_with_input(
            BenchmarkId::new("single_owner_chunk", size),
            &soc_bytes,
            |b, bytes| {
                b.iter(|| DefaultSingleOwnerChunk::try_from(bytes.clone()).unwrap());
            },
        );
    }

    group.finish();
}

fn bench_bmt_zero_tree_optimization(c: &mut Criterion) {
    // Test 1: Partial data - real zero tree optimization
    // This tests when we have small amounts of data and the rest of the 4096-byte
    // buffer is zeros, triggering the ZERO_HASHES rollup optimization
    {
        let mut group = c.benchmark_group("bmt_partial_data");

        // Small data sizes within a 4096-byte buffer
        let sizes = [32, 64, 128, 256, 512, 1024, 2048];

        for &size in &sizes {
            let mut data = vec![0u8; size];
            rng().fill_bytes(&mut data);

            group.bench_with_input(BenchmarkId::from_parameter(size), &data, |b, data| {
                b.iter(|| {
                    let mut hasher = DefaultHasher::new();
                    hasher.set_span(data.len() as u64);
                    hasher.update(data);
                    hasher.sum()
                });
            });
        }

        group.finish();
    }

    // Test 2: All-zero data - tests instant lookup optimization
    // When data is entirely zeros, the result should be a pre-computed lookup
    {
        let mut group = c.benchmark_group("bmt_all_zeros");

        let sizes = [32, 64, 128, 256, 512, 1024, 2048, 4096];

        for &size in &sizes {
            let data = vec![0u8; size]; // All zeros

            group.bench_with_input(BenchmarkId::from_parameter(size), &data, |b, data| {
                b.iter(|| {
                    let mut hasher = DefaultHasher::new();
                    hasher.set_span(data.len() as u64);
                    hasher.update(data);
                    hasher.sum()
                });
            });
        }

        group.finish();
    }

    // Test 3: Compare partial vs full data at same sizes
    // Shows the benefit of zero tree optimization vs hashing full random data
    {
        let mut group = c.benchmark_group("bmt_zero_tree_benefit");

        // Compare: 100 bytes of real data vs 4096 bytes of real data
        // Power-of-2 aligned sizes for meaningful comparison
        let partial_sizes = [128, 512, 1024, 2048];

        for &size in &partial_sizes {
            let mut partial_data = vec![0u8; size];
            rng().fill_bytes(&mut partial_data);

            let mut full_data = vec![0u8; DEFAULT_BODY_SIZE];
            rng().fill_bytes(&mut full_data);

            // Benchmark partial data (benefits from zero tree)
            group.bench_with_input(
                BenchmarkId::new("partial", size),
                &partial_data,
                |b, data| {
                    b.iter(|| {
                        let mut hasher = DefaultHasher::new();
                        hasher.set_span(data.len() as u64);
                        hasher.update(data);
                        hasher.sum()
                    });
                },
            );

            // Benchmark full 4096 bytes for comparison
            group.bench_with_input(
                BenchmarkId::new("full_4096", size),
                &full_data,
                |b, data| {
                    b.iter(|| {
                        let mut hasher = DefaultHasher::new();
                        hasher.set_span(data.len() as u64);
                        hasher.update(data);
                        hasher.sum()
                    });
                },
            );
        }

        group.finish();
    }
}

criterion_group!(
    benches,
    bench_bmt_hash,
    bench_content_chunk_creation,
    bench_single_owner_chunk_creation,
    bench_chunk_deserialization,
    bench_bmt_proof,
    bench_large_update,
    bench_bmt_zero_tree_optimization
);
criterion_main!(benches);
