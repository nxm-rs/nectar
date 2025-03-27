#![allow(missing_docs)]
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use nectar_primitives::{
    MAX_CHUNK_SIZE,
    bmt::{Hasher, Prover},
};
use rand::Rng;
use std::time::Duration;

pub fn proofs(c: &mut Criterion) {
    let mut group = c.benchmark_group("proofs");

    // Configure the benchmark group
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(50);

    // Create test data
    let data: Vec<u8> = (0..MAX_CHUNK_SIZE).map(|_| rand::random::<u8>()).collect();
    let mut hasher = Hasher::new();
    hasher.set_span(data.len() as u64);
    hasher.update(&data);
    let root_hash = hasher.sum();

    // Sample indexes to benchmark proof generation
    let indexes = [0, 32, 64, 127];

    // Benchmark proof generation for different segment indexes
    for &index in &indexes {
        group.bench_with_input(
            BenchmarkId::new("generate_proof", index),
            &index,
            |b, &idx| {
                b.iter(|| {
                    black_box(
                        hasher
                            .generate_proof(&data, idx)
                            .expect("Failed to generate proof"),
                    );
                });
            },
        );
    }

    // Generate proofs for verification benchmarks
    let proofs: Vec<_> = indexes
        .iter()
        .map(|&idx| {
            hasher
                .generate_proof(&data, idx)
                .expect("Failed to generate proof")
        })
        .collect();

    // Benchmark proof verification for different segment indexes
    for (i, proof) in proofs.iter().enumerate() {
        let index = indexes[i];
        group.bench_with_input(BenchmarkId::new("verify_proof", index), &index, |b, _| {
            b.iter(|| {
                let result = Hasher::verify_proof(proof, root_hash.as_slice())
                    .expect("Failed to verify proof");
                assert!(result, "Verification failed");
            });
        });
    }

    // Benchmark proof generation and verification together
    group.bench_function("full_proof_cycle", |b| {
        b.iter(|| {
            let index = rand::rng().random_range(0..128);
            let proof = hasher
                .generate_proof(&data, index)
                .expect("Failed to generate proof");
            let is_valid =
                Hasher::verify_proof(&proof, root_hash.as_slice()).expect("Failed to verify proof");
            assert!(is_valid, "Verification failed");
        });
    });

    // Benchmark partial data handling
    let partial_sizes = [512, 1024, 2048, 3072, 4096];
    for &size in &partial_sizes {
        let partial_data = &data[..size];
        group.bench_with_input(
            BenchmarkId::new("partial_data_proofs", size),
            &size,
            |b, _| {
                let mut h = Hasher::new();
                h.set_span(size as u64);
                h.update(partial_data);
                let partial_root = h.sum();

                b.iter(|| {
                    let idx = rand::rng().random_range(0..size / 32);
                    let proof = h
                        .generate_proof(partial_data, idx)
                        .expect("Failed to generate proof");
                    let is_valid = Hasher::verify_proof(&proof, partial_root.as_slice())
                        .expect("Failed to verify proof");
                    assert!(is_valid, "Verification failed");
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, proofs);
criterion_main!(benches);
