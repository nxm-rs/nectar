//! End-to-end upload pipeline benchmarks.
//!
//! Measures the complete upload processing flow:
//! 1. Reading file data
//! 2. Splitting into BMT chunks
//! 3. Stamping each chunk with postage
//!
//! This provides realistic throughput estimates for upload operations.
#![allow(
    missing_docs,
    clippy::arithmetic_side_effects,
    clippy::as_conversions,
    clippy::indexing_slicing,
    clippy::unwrap_used
)]

use std::sync::{Arc, Mutex};

use alloy_primitives::{B256, Signature, U256};
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use nectar_testing::run;
use rand::{Rng, rng};
use std::hint::black_box;

use nectar_file::{File, HashWindow, Policy, PutWindow, ReadAt, ReadAtSource};
use nectar_postage_issuer::{
    BatchId, BatchStamper, BucketDepth, MemoryIssuer, StampPipeline, Stamper,
};
use nectar_primitives::DEFAULT_BODY_SIZE;
use nectar_primitives::chunk::{AnyChunkSet, Chunk, ChunkAddress, Verified};
use nectar_primitives::store::{BoxedError, ChunkPut};

type SealedChunk = Chunk<Verified, AnyChunkSet<DEFAULT_BODY_SIZE>>;

/// Collecting put target: appends every sealed chunk to a shared list.
#[derive(Clone, Default)]
struct Collect(Arc<Mutex<Vec<SealedChunk>>>);

impl ChunkPut<Chunk<Verified, AnyChunkSet<DEFAULT_BODY_SIZE>>> for Collect {
    type Error = std::convert::Infallible;

    async fn put(&self, chunk: SealedChunk) -> Result<(), Self::Error> {
        self.0.lock().unwrap().push(chunk);
        Ok(())
    }
}

/// Zero-copy shared buffer source for the batch ingest.
#[derive(Clone)]
struct SharedBuf(Arc<Vec<u8>>);

impl ReadAt for SharedBuf {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize, BoxedError> {
        let start = (offset as usize).min(self.0.len());
        let take = buf.len().min(self.0.len() - start);
        buf[..take].copy_from_slice(&self.0[start..start + take]);
        Ok(take)
    }

    fn size(&self) -> Option<u64> {
        Some(self.0.len() as u64)
    }
}

/// Sequential streaming split of the whole buffer.
fn split_sequential(data: &[u8]) -> (ChunkAddress, Vec<SealedChunk>) {
    let sink = Collect::default();
    let root =
        run(File::<Collect, DEFAULT_BODY_SIZE>::new(sink.clone(), Policy::DEFAULT).save(data))
            .unwrap();
    let chunks = std::mem::take(&mut *sink.0.lock().unwrap());
    (root, chunks)
}

/// Pooled batch ingest of the whole shared buffer.
fn split_parallel(source: &SharedBuf) -> (ChunkAddress, Vec<SealedChunk>) {
    let sink = Collect::default();
    let policy = Policy::DEFAULT
        .with_put_window(PutWindow::DEFAULT)
        .with_hash_window(HashWindow::DEFAULT);
    let root = run(
        File::<Collect, DEFAULT_BODY_SIZE>::new(sink.clone(), policy)
            .save(ReadAtSource::new(source.clone())),
    )
    .unwrap();
    let chunks = std::mem::take(&mut *sink.0.lock().unwrap());
    (root, chunks)
}

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
                let (root, chunks) = split_sequential(data);

                // Stamp each chunk
                let issuer: MemoryIssuer =
                    MemoryIssuer::new(BatchId::ZERO, 32, BucketDepth::new(16).unwrap());
                let mut stamper = BatchStamper::new(issuer, MockSigner);

                let stamps: Vec<_> = chunks
                    .iter()
                    .map(|chunk| stamper.stamp(chunk.address()).unwrap())
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
        let source = SharedBuf(Arc::new(data.clone()));
        group.bench_with_input(BenchmarkId::from_parameter(name), &source, |b, source| {
            b.iter(|| {
                // Parallel split
                let (root, chunks) = split_parallel(source);

                // Stamp each chunk (sequential)
                let issuer: MemoryIssuer =
                    MemoryIssuer::new(BatchId::ZERO, 32, BucketDepth::new(16).unwrap());
                let mut stamper = BatchStamper::new(issuer, MockSigner);

                let stamps: Vec<_> = chunks
                    .iter()
                    .map(|chunk| stamper.stamp(chunk.address()).unwrap())
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
                let (root, chunks) = split_sequential(data);

                // Stamp each chunk with real signatures
                let issuer: MemoryIssuer =
                    MemoryIssuer::new(BatchId::ZERO, 32, BucketDepth::new(16).unwrap());
                let mut stamper = BatchStamper::new(issuer, &signer);

                let stamps: Vec<_> = chunks
                    .iter()
                    .map(|chunk| stamper.stamp(chunk.address()).unwrap())
                    .collect();

                black_box((root, stamps))
            });
        });
    }

    group.finish();
}

/// Full pipeline with parallel splitting and streamed parallel signing.
///
/// Maximum throughput configuration.
fn bench_pipeline_fully_parallel(c: &mut Criterion) {
    let pipeline = StampPipeline::from_signer(PrivateKeySigner::random());

    let mut group = c.benchmark_group("upload_pipeline_fully_parallel");

    for &(size, name) in SIZES {
        let mut data = vec![0u8; size as usize];
        rng().fill_bytes(&mut data);

        group.throughput(Throughput::Bytes(size));
        let source = SharedBuf(Arc::new(data.clone()));
        group.bench_with_input(BenchmarkId::from_parameter(name), &source, |b, source| {
            b.iter(|| {
                // Parallel split
                let (root, chunks) = split_parallel(source);

                // Stream the addresses through the stamp pipeline
                let issuer: MemoryIssuer =
                    MemoryIssuer::new(BatchId::ZERO, 32, BucketDepth::new(16).unwrap());
                let handle = &issuer;
                let stamps: Vec<_> = pipeline
                    .stamp(&handle, chunks.iter().map(|c| *c.address()))
                    .collect();

                black_box((root, stamps))
            });
        });
    }

    group.finish();
}

/// Comparison at 4MB: sequential vs parallel pipeline.
fn bench_pipeline_comparison(c: &mut Criterion) {
    let signer = PrivateKeySigner::random();
    let pipeline = StampPipeline::from_signer(signer.clone());
    let size = 4 * 1024 * 1024u64;

    let mut data = vec![0u8; size as usize];
    rng().fill_bytes(&mut data);

    let mut group = c.benchmark_group("upload_pipeline_4mb_comparison");
    group.throughput(Throughput::Bytes(size));

    let source = SharedBuf(Arc::new(data.clone()));

    // Fully sequential
    group.bench_function("sequential", |b| {
        b.iter(|| {
            let (root, chunks) = split_sequential(&data);

            let issuer: MemoryIssuer =
                MemoryIssuer::new(BatchId::ZERO, 32, BucketDepth::new(16).unwrap());
            let mut stamper = BatchStamper::new(issuer, &signer);

            let stamps: Vec<_> = chunks
                .iter()
                .map(|chunk| stamper.stamp(chunk.address()).unwrap())
                .collect();

            black_box((root, stamps))
        });
    });

    // Parallel split only
    group.bench_function("parallel_split", |b| {
        b.iter(|| {
            let (root, chunks) = split_parallel(&source);

            let issuer: MemoryIssuer =
                MemoryIssuer::new(BatchId::ZERO, 32, BucketDepth::new(16).unwrap());
            let mut stamper = BatchStamper::new(issuer, &signer);

            let stamps: Vec<_> = chunks
                .iter()
                .map(|chunk| stamper.stamp(chunk.address()).unwrap())
                .collect();

            black_box((root, stamps))
        });
    });

    // Fully parallel
    group.bench_function("fully_parallel", |b| {
        b.iter(|| {
            let (root, chunks) = split_parallel(&source);

            let issuer: MemoryIssuer =
                MemoryIssuer::new(BatchId::ZERO, 32, BucketDepth::new(16).unwrap());
            let handle = &issuer;
            let stamps: Vec<_> = pipeline
                .stamp(&handle, chunks.iter().map(|c| *c.address()))
                .collect();

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

    let source = SharedBuf(Arc::new(data.clone()));

    // Stage 1: Split only (sequential)
    group.bench_function("1_split_sequential", |b| {
        b.iter(|| black_box(split_sequential(&data)));
    });

    // Stage 1: Split only (parallel)
    group.bench_function("1_split_parallel", |b| {
        b.iter(|| black_box(split_parallel(&source)));
    });

    // Pre-split for stamp benchmarks
    let (_, chunks) = split_sequential(&data);
    let addresses: Vec<_> = chunks.iter().map(|c| *c.address()).collect();
    let num_chunks = addresses.len();

    group.throughput(Throughput::Elements(num_chunks as u64));

    // Stage 2: Stamp only (sequential)
    group.bench_function("2_stamp_sequential", |b| {
        b.iter(|| {
            let issuer: MemoryIssuer =
                MemoryIssuer::new(BatchId::ZERO, 32, BucketDepth::new(16).unwrap());
            let mut stamper = BatchStamper::new(issuer, &signer);
            let stamps: Vec<_> = addresses
                .iter()
                .map(|addr| stamper.stamp(addr).unwrap())
                .collect();
            black_box(stamps)
        });
    });

    // Stage 2: Stamp only (streamed parallel)
    let pipeline = StampPipeline::from_signer(signer.clone());

    group.bench_function("2_stamp_parallel", |b| {
        b.iter(|| {
            let issuer: MemoryIssuer =
                MemoryIssuer::new(BatchId::ZERO, 32, BucketDepth::new(16).unwrap());
            let handle = &issuer;
            let stamps: Vec<_> = pipeline.stamp(&handle, addresses.iter().copied()).collect();
            black_box(stamps)
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
