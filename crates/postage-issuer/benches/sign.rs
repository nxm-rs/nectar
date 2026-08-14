//! Benchmarks for postage stamp signing operations.
//!
//! This benchmark file focuses on stamp issuing and signing operations,
//! suitable for CLI tools (like dipper) that create stamps.
#![allow(missing_docs, clippy::unwrap_used)]

use alloy_primitives::{B256, Signature, U256};
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use nectar_postage_issuer::{
    BatchId, BatchStamper, BucketDepth, MemoryIssuer, ShardedIssuer, StampPipeline, Stamper,
};
use nectar_primitives::ChunkAddress;
use rand::RngExt;
use std::hint::black_box;

/// Generate a random ChunkAddress for benchmarking.
fn random_address() -> ChunkAddress {
    let mut rng = rand::rng();
    let mut bytes = [0u8; 32];
    rng.fill(&mut bytes);
    ChunkAddress::new(bytes)
}

// Mock Signer (for measuring non-crypto overhead)

/// A mock signer for benchmarking that creates deterministic signatures.
struct MockSigner;

impl SignerSync for MockSigner {
    fn sign_hash_sync(&self, _hash: &B256) -> Result<Signature, alloy_signer::Error> {
        Ok(Signature::new(U256::from(1), U256::from(2), false))
    }

    fn sign_message_sync(&self, _message: &[u8]) -> Result<Signature, alloy_signer::Error> {
        Ok(Signature::new(U256::from(1), U256::from(2), false))
    }

    fn chain_id_sync(&self) -> Option<u64> {
        None
    }
}

fn bench_stamper_mock(c: &mut Criterion) {
    let mut group = c.benchmark_group("stamper_mock");

    group.bench_function("single", |b| {
        b.iter(|| {
            let issuer: MemoryIssuer =
                MemoryIssuer::new(BatchId::ZERO, 24, BucketDepth::new(16).unwrap());
            let mut stamper = BatchStamper::new(issuer, MockSigner);
            let address = random_address();
            black_box(stamper.stamp(black_box(&address)))
        })
    });

    let addresses: Vec<ChunkAddress> = (0..1000).map(|_| random_address()).collect();
    group.throughput(Throughput::Elements(1000));

    group.bench_function("throughput_1000", |b| {
        b.iter(|| {
            let issuer: MemoryIssuer =
                MemoryIssuer::new(BatchId::ZERO, 32, BucketDepth::new(16).unwrap());
            let mut stamper = BatchStamper::new(issuer, MockSigner);
            for addr in &addresses {
                black_box(stamper.stamp(addr).unwrap());
            }
        })
    });

    group.finish();
}

// Sequential ECDSA Signing Benchmarks

fn bench_ecdsa_sign_sequential(c: &mut Criterion) {
    let signer = PrivateKeySigner::random();
    let addresses: Vec<ChunkAddress> = (0..100).map(|_| random_address()).collect();

    let mut group = c.benchmark_group("ecdsa_sign_sequential");

    group.bench_function("single", |b| {
        b.iter(|| {
            let issuer: MemoryIssuer =
                MemoryIssuer::new(BatchId::ZERO, 24, BucketDepth::new(16).unwrap());
            let mut stamper = BatchStamper::new(issuer, &signer);
            let address = random_address();
            black_box(stamper.stamp(black_box(&address)))
        })
    });

    group.throughput(Throughput::Elements(100));

    group.bench_function("throughput_100", |b| {
        b.iter(|| {
            let issuer: MemoryIssuer =
                MemoryIssuer::new(BatchId::ZERO, 32, BucketDepth::new(16).unwrap());
            let mut stamper = BatchStamper::new(issuer, &signer);
            for addr in &addresses {
                black_box(stamper.stamp(addr).unwrap());
            }
        })
    });

    group.finish();
}

// Parallel ECDSA Signing Benchmarks

fn bench_ecdsa_sign_parallel(c: &mut Criterion) {
    let pipeline = StampPipeline::from_signer(PrivateKeySigner::random());
    let addresses_100: Vec<ChunkAddress> = (0..100).map(|_| random_address()).collect();
    let addresses_1000: Vec<ChunkAddress> = (0..1000).map(|_| random_address()).collect();

    let mut group = c.benchmark_group("ecdsa_sign_parallel");

    group.throughput(Throughput::Elements(100));
    group.bench_function("throughput_100", |b| {
        b.iter(|| {
            let issuer: ShardedIssuer =
                ShardedIssuer::new(BatchId::ZERO, 32, BucketDepth::new(16).unwrap());
            let mut handle = &issuer;
            let results: Vec<_> = pipeline
                .stamp(&mut handle, addresses_100.iter().copied())
                .collect();
            black_box(results)
        })
    });

    group.throughput(Throughput::Elements(1000));
    group.bench_function("throughput_1000", |b| {
        b.iter(|| {
            let issuer: ShardedIssuer =
                ShardedIssuer::new(BatchId::ZERO, 32, BucketDepth::new(16).unwrap());
            let mut handle = &issuer;
            let results: Vec<_> = pipeline
                .stamp(&mut handle, addresses_1000.iter().copied())
                .collect();
            black_box(results)
        })
    });

    group.finish();
}

// Comparison: Sequential vs Parallel Signing

fn bench_sign_comparison(c: &mut Criterion) {
    let signer = PrivateKeySigner::random();
    let pipeline = StampPipeline::from_signer(signer.clone());
    let addresses: Vec<ChunkAddress> = (0..1000).map(|_| random_address()).collect();

    let mut group = c.benchmark_group("sign_1000_comparison");
    group.throughput(Throughput::Elements(1000));

    // Sequential
    group.bench_function("sequential", |b| {
        b.iter(|| {
            let issuer: MemoryIssuer =
                MemoryIssuer::new(BatchId::ZERO, 32, BucketDepth::new(16).unwrap());
            let mut stamper = BatchStamper::new(issuer, &signer);
            for addr in &addresses {
                black_box(stamper.stamp(addr).unwrap());
            }
        })
    });

    // Streaming pipeline over the sharded issuer
    group.bench_function("pipeline", |b| {
        b.iter(|| {
            let issuer: ShardedIssuer =
                ShardedIssuer::new(BatchId::ZERO, 32, BucketDepth::new(16).unwrap());
            let mut handle = &issuer;
            let results: Vec<_> = pipeline
                .stamp(&mut handle, addresses.iter().copied())
                .collect();
            black_box(results)
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_stamper_mock,
    bench_ecdsa_sign_sequential,
    bench_ecdsa_sign_parallel,
    bench_sign_comparison,
);

criterion_main!(benches);
