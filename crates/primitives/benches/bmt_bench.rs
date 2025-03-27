#![allow(missing_docs)]
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rand::{RngCore, rng};

use nectar_primitives::bmt::{Hasher, MAX_DATA_LENGTH, Prover};
use nectar_primitives::chunk::ContentChunk;

fn bench_bmt_hash(c: &mut Criterion) {
    let mut group = c.benchmark_group("bmt_hash");

    // Test different data sizes
    for size in [100, 1000, 4096, 4096 * 4].iter() {
        // Generate random data
        let mut data = vec![0u8; *size];
        rng().fill_bytes(&mut data);

        group.bench_with_input(BenchmarkId::from_parameter(size), &data, |b, data| {
            b.iter(|| {
                let mut hasher = Hasher::new();
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
    for size in [100, 1000, 4000].iter() {
        // Generate random data
        let mut data = vec![0u8; *size];
        rng().fill_bytes(&mut data);

        group.bench_with_input(BenchmarkId::from_parameter(size), &data, |b, data| {
            b.iter(|| ContentChunk::new(data.clone()).unwrap());
        });
    }

    group.finish();
}

fn bench_bmt_proof(c: &mut Criterion) {
    let mut group = c.benchmark_group("bmt_proof");

    // Generate full-size data
    let mut data = vec![0u8; MAX_DATA_LENGTH];
    rng().fill_bytes(&mut data);

    // Create hasher and calculate root hash
    let mut hasher = Hasher::new();
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
        b.iter(|| Hasher::verify_proof(&proof, root_hash.as_slice()).unwrap());
    });

    group.finish();
}

fn bench_large_update(c: &mut Criterion) {
    let mut group = c.benchmark_group("bmt_update");

    // Generate full-size data
    let mut data = vec![0u8; MAX_DATA_LENGTH];
    rng().fill_bytes(&mut data);

    // Benchmark single large update
    group.bench_function("single_large", |b| {
        b.iter(|| {
            let mut hasher = Hasher::new();
            hasher.set_span(data.len() as u64);
            hasher.update(&data);
            hasher.sum()
        });
    });

    // Benchmark multiple small updates
    group.bench_function("multiple_small", |b| {
        b.iter(|| {
            let mut hasher = Hasher::new();
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

criterion_group!(
    benches,
    bench_bmt_hash,
    bench_content_chunk_creation,
    bench_bmt_proof,
    bench_large_update
);
criterion_main!(benches);
