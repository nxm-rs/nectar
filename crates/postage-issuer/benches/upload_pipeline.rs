#![allow(missing_docs)]
//! End-to-end upload pipeline benchmarks.
//!
//! Measures the complete upload processing flow:
//! 1. Reading file data
//! 2. Splitting into BMT chunks
//! 3. Stamping each chunk with postage
//!
//! This provides realistic throughput estimates for upload operations.

use std::io::Write;

use alloy_primitives::{B256, Signature, U256};
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use rand::{Rng, rng};

use nectar_postage_issuer::{
    BatchStamper, MemoryIssuer, ShardedIssuer, SigningError, Stamper, sign_stamps_parallel,
};
use nectar_primitives::DEFAULT_BODY_SIZE;
use nectar_primitives::file::{SyncParallelSplitter, SyncSplitter};
use nectar_primitives::store::MemoryStore;

/// File sizes to benchmark, representing realistic upload scenarios.
const SIZES: &[(u64, &str)] = &[
    (64 * 1024, "64KB"),        // Small document
    (256 * 1024, "256KB"),      // Medium document
    (1024 * 1024, "1MB"),       // Typical file
    (4 * 1024 * 1024, "4MB"),   // Large file
    (16 * 1024 * 1024, "16MB"), // Very large file
];

/// Mock signer for isolating non-crypto overhead.
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

/// Full pipeline: split + stamp (sequential, mock signer).
///
/// Measures the raw pipeline overhead without crypto costs.
fn bench_pipeline_mock_sequential(c: &mut Criterion) {
    let mut group = c.benchmark_group("upload_pipeline_mock_seq");

    for &(size, name) in SIZES {
        let mut data = vec![0u8; size as usize];
        rng().fill_bytes(&mut data);

        group.throughput(Throughput::Bytes(size));
        group.bench_with_input(BenchmarkId::from_parameter(name), &data, |b, data| {
            b.iter(|| {
                // Split file into chunks
                let mut splitter = SyncSplitter::<DEFAULT_BODY_SIZE>::new(data.len() as u64);
                splitter.write_all(data).unwrap();
                let (root, chunks) = splitter.finish().unwrap();
                let store = MemoryStore::from_chunks(chunks);

                // Stamp each chunk
                let issuer = MemoryIssuer::new(B256::ZERO, 32, 16);
                let mut stamper = BatchStamper::new(issuer, MockSigner);

                let chunks = store.into_chunks();
                let stamps: Vec<_> = chunks
                    .keys()
                    .map(|addr| stamper.stamp(addr).unwrap())
                    .collect();

                black_box((root, stamps))
            });
        });
    }

    group.finish();
}

/// Full pipeline: parallel split + sequential stamp (mock signer).
fn bench_pipeline_mock_parallel_split(c: &mut Criterion) {
    let mut group = c.benchmark_group("upload_pipeline_mock_par_split");

    for &(size, name) in SIZES {
        let mut data = vec![0u8; size as usize];
        rng().fill_bytes(&mut data);

        group.throughput(Throughput::Bytes(size));
        group.bench_with_input(BenchmarkId::from_parameter(name), &data, |b, data| {
            b.iter(|| {
                // Parallel split
                let (root, chunks) =
                    SyncParallelSplitter::<DEFAULT_BODY_SIZE>::split_to_vec(data).unwrap();
                let store = MemoryStore::from_chunks(chunks);

                // Stamp each chunk (sequential)
                let issuer = MemoryIssuer::new(B256::ZERO, 32, 16);
                let mut stamper = BatchStamper::new(issuer, MockSigner);

                let chunks = store.into_chunks();
                let stamps: Vec<_> = chunks
                    .keys()
                    .map(|addr| stamper.stamp(addr).unwrap())
                    .collect();

                black_box((root, stamps))
            });
        });
    }

    group.finish();
}

/// Full pipeline with real ECDSA signing (sequential).
///
/// This represents the realistic upload throughput.
fn bench_pipeline_ecdsa_sequential(c: &mut Criterion) {
    let signer = PrivateKeySigner::random();

    let mut group = c.benchmark_group("upload_pipeline_ecdsa_seq");

    for &(size, name) in SIZES {
        let mut data = vec![0u8; size as usize];
        rng().fill_bytes(&mut data);

        group.throughput(Throughput::Bytes(size));
        group.bench_with_input(BenchmarkId::from_parameter(name), &data, |b, data| {
            b.iter(|| {
                // Split file into chunks
                let mut splitter = SyncSplitter::<DEFAULT_BODY_SIZE>::new(data.len() as u64);
                splitter.write_all(data).unwrap();
                let (root, chunks) = splitter.finish().unwrap();
                let store = MemoryStore::from_chunks(chunks);

                // Stamp each chunk with real signatures
                let issuer = MemoryIssuer::new(B256::ZERO, 32, 16);
                let mut stamper = BatchStamper::new(issuer, &signer);

                let chunks = store.into_chunks();
                let stamps: Vec<_> = chunks
                    .keys()
                    .map(|addr| stamper.stamp(addr).unwrap())
                    .collect();

                black_box((root, stamps))
            });
        });
    }

    group.finish();
}

/// Full pipeline with parallel splitting and parallel signing.
///
/// Maximum throughput configuration.
fn bench_pipeline_fully_parallel(c: &mut Criterion) {
    let signer = PrivateKeySigner::random();

    let sign_fn = |prehash: &B256| -> Result<Signature, SigningError> {
        Ok(signer
            .sign_message_sync(prehash.as_slice())
            .map_err(alloy_signer::Error::other)?)
    };

    let mut group = c.benchmark_group("upload_pipeline_fully_parallel");

    for &(size, name) in SIZES {
        let mut data = vec![0u8; size as usize];
        rng().fill_bytes(&mut data);

        group.throughput(Throughput::Bytes(size));
        group.bench_with_input(BenchmarkId::from_parameter(name), &data, |b, data| {
            b.iter(|| {
                // Parallel split
                let (root, chunks) =
                    SyncParallelSplitter::<DEFAULT_BODY_SIZE>::split_to_vec(data).unwrap();
                let store = MemoryStore::from_chunks(chunks);

                // Collect addresses for parallel signing
                let chunks = store.into_chunks();
                let addresses: Vec<_> = chunks.keys().copied().collect();

                // Parallel stamp signing
                let issuer = ShardedIssuer::new(B256::ZERO, 32, 16);
                let stamps = sign_stamps_parallel(&issuer, &sign_fn, &addresses);

                black_box((root, stamps))
            });
        });
    }

    group.finish();
}

/// Comparison at 4MB: sequential vs parallel pipeline.
fn bench_pipeline_comparison(c: &mut Criterion) {
    let signer = PrivateKeySigner::random();
    let size = 4 * 1024 * 1024u64;

    let mut data = vec![0u8; size as usize];
    rng().fill_bytes(&mut data);

    let sign_fn = |prehash: &B256| -> Result<Signature, SigningError> {
        Ok(signer
            .sign_message_sync(prehash.as_slice())
            .map_err(alloy_signer::Error::other)?)
    };

    let mut group = c.benchmark_group("upload_pipeline_4mb_comparison");
    group.throughput(Throughput::Bytes(size));

    // Fully sequential
    group.bench_function("sequential", |b| {
        b.iter(|| {
            let mut splitter = SyncSplitter::<DEFAULT_BODY_SIZE>::new(data.len() as u64);
            splitter.write_all(&data).unwrap();
            let (root, chunks) = splitter.finish().unwrap();
            let store = MemoryStore::from_chunks(chunks);

            let issuer = MemoryIssuer::new(B256::ZERO, 32, 16);
            let mut stamper = BatchStamper::new(issuer, &signer);

            let chunks = store.into_chunks();
            let stamps: Vec<_> = chunks
                .keys()
                .map(|addr| stamper.stamp(addr).unwrap())
                .collect();

            black_box((root, stamps))
        });
    });

    // Parallel split only
    group.bench_function("parallel_split", |b| {
        b.iter(|| {
            let (root, chunks) =
                SyncParallelSplitter::<DEFAULT_BODY_SIZE>::split_to_vec(&data).unwrap();
            let store = MemoryStore::from_chunks(chunks);

            let issuer = MemoryIssuer::new(B256::ZERO, 32, 16);
            let mut stamper = BatchStamper::new(issuer, &signer);

            let chunks = store.into_chunks();
            let stamps: Vec<_> = chunks
                .keys()
                .map(|addr| stamper.stamp(addr).unwrap())
                .collect();

            black_box((root, stamps))
        });
    });

    // Fully parallel
    group.bench_function("fully_parallel", |b| {
        b.iter(|| {
            let (root, chunks) =
                SyncParallelSplitter::<DEFAULT_BODY_SIZE>::split_to_vec(&data).unwrap();
            let store = MemoryStore::from_chunks(chunks);

            let chunks = store.into_chunks();
            let addresses: Vec<_> = chunks.keys().copied().collect();

            let issuer = ShardedIssuer::new(B256::ZERO, 32, 16);
            let stamps = sign_stamps_parallel(&issuer, &sign_fn, &addresses);

            black_box((root, stamps))
        });
    });

    group.finish();
}

/// Breakdown: measure each stage separately for 4MB.
fn bench_pipeline_stages(c: &mut Criterion) {
    let signer = PrivateKeySigner::random();
    let size = 4 * 1024 * 1024u64;

    let mut data = vec![0u8; size as usize];
    rng().fill_bytes(&mut data);

    let mut group = c.benchmark_group("upload_pipeline_stages_4mb");
    group.throughput(Throughput::Bytes(size));

    // Stage 1: Split only (sequential)
    group.bench_function("1_split_sequential", |b| {
        b.iter(|| {
            let mut splitter = SyncSplitter::<DEFAULT_BODY_SIZE>::new(data.len() as u64);
            splitter.write_all(&data).unwrap();
            black_box(splitter.finish().unwrap())
        });
    });

    // Stage 1: Split only (parallel)
    group.bench_function("1_split_parallel", |b| {
        b.iter(|| {
            black_box(SyncParallelSplitter::<DEFAULT_BODY_SIZE>::split_to_vec(&data).unwrap())
        });
    });

    // Pre-split for stamp benchmarks
    let mut splitter = SyncSplitter::<DEFAULT_BODY_SIZE>::new(data.len() as u64);
    splitter.write_all(&data).unwrap();
    let (_, chunks) = splitter.finish().unwrap();
    let store = MemoryStore::from_chunks(chunks);
    let chunks = store.into_chunks();
    let addresses: Vec<_> = chunks.keys().copied().collect();
    let num_chunks = addresses.len();

    group.throughput(Throughput::Elements(num_chunks as u64));

    // Stage 2: Stamp only (sequential)
    group.bench_function("2_stamp_sequential", |b| {
        b.iter(|| {
            let issuer = MemoryIssuer::new(B256::ZERO, 32, 16);
            let mut stamper = BatchStamper::new(issuer, &signer);
            let stamps: Vec<_> = addresses
                .iter()
                .map(|addr| stamper.stamp(addr).unwrap())
                .collect();
            black_box(stamps)
        });
    });

    // Stage 2: Stamp only (parallel)
    let sign_fn = |prehash: &B256| -> Result<Signature, SigningError> {
        Ok(signer
            .sign_message_sync(prehash.as_slice())
            .map_err(alloy_signer::Error::other)?)
    };

    group.bench_function("2_stamp_parallel", |b| {
        b.iter(|| {
            let issuer = ShardedIssuer::new(B256::ZERO, 32, 16);
            black_box(sign_stamps_parallel(&issuer, &sign_fn, &addresses))
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_pipeline_mock_sequential,
    bench_pipeline_mock_parallel_split,
    bench_pipeline_ecdsa_sequential,
    bench_pipeline_fully_parallel,
    bench_pipeline_comparison,
    bench_pipeline_stages,
);
criterion_main!(benches);
