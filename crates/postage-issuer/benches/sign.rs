//! Benchmarks for postage stamp signing operations.
//!
//! This benchmark file focuses on stamp issuing and signing operations,
//! suitable for CLI tools (like dipper) that create stamps.

use alloy_primitives::{B256, Signature, U256};
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use nectar_postage_issuer::{
    BatchStamper, MemoryIssuer, ShardedIssuer, SigningError, Stamper, sign_stamps_parallel,
};
use nectar_primitives::SwarmAddress;
use rand::Rng;

/// Generate a random SwarmAddress for benchmarking.
fn random_address() -> SwarmAddress {
    let mut rng = rand::rng();
    let mut bytes = [0u8; 32];
    rng.fill(&mut bytes);
    SwarmAddress::new(bytes)
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
            let issuer = MemoryIssuer::new(B256::ZERO, 24, 16);
            let mut stamper = BatchStamper::new(issuer, MockSigner);
            let address = random_address();
            black_box(stamper.stamp(black_box(&address)))
        })
    });

    let addresses: Vec<SwarmAddress> = (0..1000).map(|_| random_address()).collect();
    group.throughput(Throughput::Elements(1000));

    group.bench_function("throughput_1000", |b| {
        b.iter(|| {
            let issuer = MemoryIssuer::new(B256::ZERO, 32, 16);
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
    let addresses: Vec<SwarmAddress> = (0..100).map(|_| random_address()).collect();

    let mut group = c.benchmark_group("ecdsa_sign_sequential");

    group.bench_function("single", |b| {
        b.iter(|| {
            let issuer = MemoryIssuer::new(B256::ZERO, 24, 16);
            let mut stamper = BatchStamper::new(issuer, &signer);
            let address = random_address();
            black_box(stamper.stamp(black_box(&address)))
        })
    });

    group.throughput(Throughput::Elements(100));

    group.bench_function("throughput_100", |b| {
        b.iter(|| {
            let issuer = MemoryIssuer::new(B256::ZERO, 32, 16);
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
    let signer = PrivateKeySigner::random();
    let addresses_100: Vec<SwarmAddress> = (0..100).map(|_| random_address()).collect();
    let addresses_1000: Vec<SwarmAddress> = (0..1000).map(|_| random_address()).collect();

    // Use sign_message_sync for EIP-191 compatibility with Go/bee
    let sign_fn = |prehash: &B256| -> Result<Signature, SigningError> {
        Ok(signer
            .sign_message_sync(prehash.as_slice())
            .map_err(alloy_signer::Error::other)?)
    };

    let mut group = c.benchmark_group("ecdsa_sign_parallel");

    group.throughput(Throughput::Elements(100));
    group.bench_function("throughput_100", |b| {
        b.iter(|| {
            let issuer = ShardedIssuer::new(B256::ZERO, 32, 16);
            black_box(sign_stamps_parallel(&issuer, &sign_fn, &addresses_100))
        })
    });

    group.throughput(Throughput::Elements(1000));
    group.bench_function("throughput_1000", |b| {
        b.iter(|| {
            let issuer = ShardedIssuer::new(B256::ZERO, 32, 16);
            black_box(sign_stamps_parallel(&issuer, &sign_fn, &addresses_1000))
        })
    });

    group.finish();
}

// Comparison: Sequential vs Parallel Signing

fn bench_sign_comparison(c: &mut Criterion) {
    let signer = PrivateKeySigner::random();
    let addresses: Vec<SwarmAddress> = (0..1000).map(|_| random_address()).collect();

    // Use sign_message_sync for EIP-191 compatibility with Go/bee
    let sign_fn = |prehash: &B256| -> Result<Signature, SigningError> {
        Ok(signer
            .sign_message_sync(prehash.as_slice())
            .map_err(alloy_signer::Error::other)?)
    };

    let mut group = c.benchmark_group("sign_1000_comparison");
    group.throughput(Throughput::Elements(1000));

    // Sequential
    group.bench_function("sequential", |b| {
        b.iter(|| {
            let issuer = MemoryIssuer::new(B256::ZERO, 32, 16);
            let mut stamper = BatchStamper::new(issuer, &signer);
            for addr in &addresses {
                black_box(stamper.stamp(addr).unwrap());
            }
        })
    });

    // Parallel
    group.bench_function("parallel", |b| {
        b.iter(|| {
            let issuer = ShardedIssuer::new(B256::ZERO, 32, 16);
            black_box(sign_stamps_parallel(&issuer, &sign_fn, &addresses))
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
