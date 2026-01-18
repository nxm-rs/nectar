//! Benchmarks for postage stamp operations.

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use nectar_postage::{
    parallel::{sign_stamps_parallel, verify_stamps_parallel, ShardedIssuer},
    streaming::{SignRequest, StreamVerifyError, VerifyRequest},
    Batch, BatchStamper, MemoryIssuer, Stamp, StampBytes, StampDigest, StampError,
    StampIndex, StampSigner, Stamper,
};
use nectar_primitives::SwarmAddress;
use alloy_primitives::{Address, B256, U256};
use alloy_signer::{Signature, SignerSync};
use alloy_signer_local::PrivateKeySigner;
use rand::Rng;
use std::sync::Arc;

/// Generate a random stamp for benchmarking.
fn random_stamp() -> Stamp {
    let mut rng = rand::rng();
    let mut batch_bytes = [0u8; 32];
    rng.fill(&mut batch_bytes);
    let batch = B256::from(batch_bytes);

    let bucket: u32 = rng.random();
    let index: u32 = rng.random();
    let timestamp: u64 = rng.random();

    let mut sig = [0u8; 65];
    rng.fill(&mut sig);

    Stamp::new(batch, bucket, index, timestamp, sig)
}

/// Generate a random SwarmAddress for benchmarking.
fn random_address() -> SwarmAddress {
    let mut rng = rand::rng();
    let mut bytes = [0u8; 32];
    rng.fill(&mut bytes);
    SwarmAddress::new(bytes)
}

// =============================================================================
// Stamp Serialization Benchmarks
// =============================================================================

fn bench_stamp_roundtrip(c: &mut Criterion) {
    let stamp = random_stamp();

    c.bench_function("stamp_roundtrip", |b| {
        b.iter(|| {
            let encoded = stamp.to_bytes();
            black_box(Stamp::from_bytes(black_box(&encoded)))
        })
    });
}

fn bench_stamp_throughput(c: &mut Criterion) {
    let stamps: Vec<Stamp> = (0..1000).map(|_| random_stamp()).collect();
    let bytes: Vec<StampBytes> = stamps.iter().map(|s| s.to_bytes()).collect();

    let mut group = c.benchmark_group("stamp_serde_throughput");
    group.throughput(Throughput::Elements(1000));

    group.bench_function("encode_1000", |b| {
        b.iter(|| {
            for stamp in &stamps {
                black_box(stamp.to_bytes());
            }
        })
    });

    group.bench_function("decode_1000", |b| {
        b.iter(|| {
            for b_arr in &bytes {
                black_box(Stamp::from_bytes(b_arr));
            }
        })
    });

    group.finish();
}

// =============================================================================
// StampIndex Benchmarks
// =============================================================================

fn bench_stamp_index_roundtrip(c: &mut Criterion) {
    let mut rng = rand::rng();
    let idx = StampIndex::new(rng.random(), rng.random());

    c.bench_function("stamp_index_roundtrip", |b| {
        b.iter(|| {
            let encoded = idx.encode();
            black_box(StampIndex::decode(black_box(encoded)))
        })
    });
}

// =============================================================================
// Validation Benchmarks
// =============================================================================

fn bench_validate_index(c: &mut Criterion) {
    let batch = Batch::new(B256::ZERO, 0, 0, Address::ZERO, 20, 16, false);
    let valid_index = StampIndex::new(1000, 10);
    let invalid_bucket = StampIndex::new(70000, 0);

    let mut group = c.benchmark_group("validate_index");

    group.bench_function("valid", |b| {
        b.iter(|| black_box(batch.validate_index(black_box(&valid_index))))
    });

    group.bench_function("invalid", |b| {
        b.iter(|| black_box(batch.validate_index(black_box(&invalid_bucket))))
    });

    group.finish();
}

// =============================================================================
// Digest/Prehash Benchmarks
// =============================================================================

fn bench_stamp_digest_prehash(c: &mut Criterion) {
    let address = random_address();
    let mut rng = rand::rng();
    let mut batch_bytes = [0u8; 32];
    rng.fill(&mut batch_bytes);
    let batch_id = B256::from(batch_bytes);
    let index = StampIndex::new(1000, 5);
    let timestamp = 1234567890u64;
    let digest = StampDigest::new(address, batch_id, index, timestamp);

    c.bench_function("stamp_digest_prehash", |b| {
        b.iter(|| black_box(digest.to_prehash()))
    });
}

// =============================================================================
// Mock Signer (for measuring non-crypto overhead)
// =============================================================================

/// A mock signer for benchmarking that creates deterministic signatures.
struct MockSigner;

impl StampSigner for MockSigner {
    fn sign_message(&self, _prehash: &B256) -> Result<Signature, alloy_signer::Error> {
        Ok(Signature::new(U256::from(1), U256::from(2), false))
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

// =============================================================================
// Real ECDSA Signer
// =============================================================================

/// Wrapper around PrivateKeySigner that implements StampSigner.
/// Uses EIP-191 message signing for compatibility with Go/bee.
struct EcdsaSigner(PrivateKeySigner);

impl EcdsaSigner {
    fn random() -> Self {
        Self(PrivateKeySigner::random())
    }
}

impl StampSigner for EcdsaSigner {
    fn sign_message(&self, prehash: &B256) -> Result<Signature, alloy_signer::Error> {
        // Use sign_message_sync for EIP-191 compatibility with Go/bee
        self.0.sign_message_sync(prehash.as_slice())
    }
}

impl StampSigner for &EcdsaSigner {
    fn sign_message(&self, prehash: &B256) -> Result<Signature, alloy_signer::Error> {
        // Use sign_message_sync for EIP-191 compatibility with Go/bee
        self.0.sign_message_sync(prehash.as_slice())
    }
}

// =============================================================================
// Sequential ECDSA Benchmarks
// =============================================================================

fn bench_ecdsa_sign_sequential(c: &mut Criterion) {
    let signer = EcdsaSigner::random();
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

/// Helper to recover address from a stamp signature.
/// Uses EIP-191 message recovery for interoperability.
fn recover_stamp_signer(
    stamp: &Stamp,
    chunk_address: &SwarmAddress,
) -> Result<Address, alloy_primitives::SignatureError> {
    let digest = StampDigest::new(
        *chunk_address,
        stamp.batch(),
        stamp.stamp_index(),
        stamp.timestamp(),
    );
    let prehash = digest.to_prehash();

    // Signature::from_raw automatically handles v-value normalization
    // (both 0/1 and 27/28 formats)
    let sig = Signature::from_raw(stamp.signature())?;

    // Use recover_address_from_msg for EIP-191 compatibility
    sig.recover_address_from_msg(prehash.as_slice())
}

fn bench_ecdsa_verify_sequential(c: &mut Criterion) {
    let signer = EcdsaSigner::random();
    let expected_address = signer.0.address();
    let issuer = MemoryIssuer::new(B256::ZERO, 32, 16);
    let mut stamper = BatchStamper::new(issuer, &signer);

    let single_addr = random_address();
    let single_stamp = stamper.stamp(&single_addr).unwrap();

    let test_data: Vec<(SwarmAddress, Stamp)> = (0..100)
        .map(|_| {
            let addr = random_address();
            let stamp = stamper.stamp(&addr).unwrap();
            (addr, stamp)
        })
        .collect();

    let mut group = c.benchmark_group("ecdsa_verify_sequential");

    group.bench_function("single", |b| {
        b.iter(|| {
            let recovered =
                recover_stamp_signer(black_box(&single_stamp), black_box(&single_addr)).unwrap();
            black_box(recovered == expected_address)
        })
    });

    group.throughput(Throughput::Elements(100));

    group.bench_function("throughput_100", |b| {
        b.iter(|| {
            for (addr, stamp) in &test_data {
                let recovered = recover_stamp_signer(stamp, addr).unwrap();
                black_box(recovered == expected_address);
            }
        })
    });

    group.finish();
}

// =============================================================================
// Parallel ECDSA Benchmarks
// =============================================================================

fn bench_ecdsa_sign_parallel(c: &mut Criterion) {
    let signer = PrivateKeySigner::random();
    let addresses_100: Vec<SwarmAddress> = (0..100).map(|_| random_address()).collect();
    let addresses_1000: Vec<SwarmAddress> = (0..1000).map(|_| random_address()).collect();

    // Use sign_message_sync for EIP-191 compatibility with Go/bee
    let sign_fn = |prehash: &B256| -> Result<Signature, StampError> {
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

fn bench_ecdsa_verify_parallel(c: &mut Criterion) {
    let signer = PrivateKeySigner::random();
    let _expected_address = signer.address();

    // Use sign_message_sync for EIP-191 compatibility with Go/bee
    let sign_fn = |prehash: &B256| -> Result<Signature, StampError> {
        Ok(signer
            .sign_message_sync(prehash.as_slice())
            .map_err(alloy_signer::Error::other)?)
    };

    // Pre-generate 100 stamps for verification
    let issuer_100 = ShardedIssuer::new(B256::ZERO, 32, 16);
    let addresses_100: Vec<SwarmAddress> = (0..100).map(|_| random_address()).collect();
    let results_100 = sign_stamps_parallel(&issuer_100, &sign_fn, &addresses_100);
    let stamps_100: Vec<Stamp> = results_100
        .iter()
        .map(|r| r.result.as_ref().unwrap().clone())
        .collect();

    // Pre-generate 1000 stamps for verification
    let issuer_1000 = ShardedIssuer::new(B256::ZERO, 32, 16);
    let addresses_1000: Vec<SwarmAddress> = (0..1000).map(|_| random_address()).collect();
    let results_1000 = sign_stamps_parallel(&issuer_1000, &sign_fn, &addresses_1000);
    let stamps_1000: Vec<Stamp> = results_1000
        .iter()
        .map(|r| r.result.as_ref().unwrap().clone())
        .collect();

    let mut group = c.benchmark_group("ecdsa_verify_parallel");

    // 100 stamps
    let verify_input_100: Vec<_> = stamps_100
        .iter()
        .zip(addresses_100.iter())
        .map(|(stamp, addr)| (stamp, addr))
        .collect();

    group.throughput(Throughput::Elements(100));
    group.bench_function("throughput_100", |b| {
        b.iter(|| black_box(verify_stamps_parallel(&verify_input_100)))
    });

    // 1000 stamps
    let verify_input_1000: Vec<_> = stamps_1000
        .iter()
        .zip(addresses_1000.iter())
        .map(|(stamp, addr)| (stamp, addr))
        .collect();

    group.throughput(Throughput::Elements(1000));
    group.bench_function("throughput_1000", |b| {
        b.iter(|| black_box(verify_stamps_parallel(&verify_input_1000)))
    });

    group.finish();
}

// =============================================================================
// Comparison: Sequential vs Parallel
// =============================================================================

fn bench_comparison(c: &mut Criterion) {
    let signer = PrivateKeySigner::random();
    let addresses: Vec<SwarmAddress> = (0..1000).map(|_| random_address()).collect();

    // Use sign_message_sync for EIP-191 compatibility with Go/bee
    let sign_fn = |prehash: &B256| -> Result<Signature, StampError> {
        Ok(signer
            .sign_message_sync(prehash.as_slice())
            .map_err(alloy_signer::Error::other)?)
    };

    // Wrapper to implement StampSigner with EIP-191 compatibility
    struct SignerWrapper<'a>(&'a PrivateKeySigner);

    impl StampSigner for SignerWrapper<'_> {
        fn sign_message(&self, prehash: &B256) -> Result<Signature, alloy_signer::Error> {
            // Use sign_message_sync for EIP-191 compatibility with Go/bee
            self.0.sign_message_sync(prehash.as_slice())
        }
    }

    let mut group = c.benchmark_group("sign_1000_comparison");
    group.throughput(Throughput::Elements(1000));

    // Sequential
    group.bench_function("sequential", |b| {
        b.iter(|| {
            let issuer = MemoryIssuer::new(B256::ZERO, 32, 16);
            let mut stamper = BatchStamper::new(issuer, SignerWrapper(&signer));
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

    // Verification comparison
    let issuer = ShardedIssuer::new(B256::ZERO, 32, 16);
    let results = sign_stamps_parallel(&issuer, &sign_fn, &addresses);
    let stamps: Vec<Stamp> = results
        .iter()
        .map(|r| r.result.as_ref().unwrap().clone())
        .collect();

    let expected_address = signer.address();

    let mut group = c.benchmark_group("verify_1000_comparison");
    group.throughput(Throughput::Elements(1000));

    // Sequential verification
    group.bench_function("sequential", |b| {
        b.iter(|| {
            for (addr, stamp) in addresses.iter().zip(stamps.iter()) {
                let recovered = recover_stamp_signer(stamp, addr).unwrap();
                black_box(recovered == expected_address);
            }
        })
    });

    // Parallel verification
    let verify_input: Vec<_> = stamps
        .iter()
        .zip(addresses.iter())
        .map(|(stamp, addr)| (stamp, addr))
        .collect();

    group.bench_function("parallel", |b| {
        b.iter(|| black_box(verify_stamps_parallel(&verify_input)))
    });

    group.finish();
}

// =============================================================================
// Streaming Benchmarks
// =============================================================================

/// Helper to run async streaming sign benchmark (hybrid: tokio + rayon)
async fn run_streaming_sign(
    signer: &Arc<PrivateKeySigner>,
    addresses: &[SwarmAddress],
    batch_size: usize,
) -> Vec<Result<Stamp, StampError>> {
    use nectar_postage::streaming::streaming_signer;

    let signer_clone = Arc::clone(signer);
    let sign_fn = Arc::new(move |prehash: &B256| -> Result<Signature, alloy_signer::Error> {
        signer_clone.sign_message_sync(prehash.as_slice())
    });
    let issuer = Arc::new(ShardedIssuer::new(B256::ZERO, 32, 16));

    // channel_size controls backpressure, batch_size controls rayon parallelism granularity
    let tx = streaming_signer(issuer, sign_fn, 100, batch_size);

    // Send all requests and collect response receivers
    let mut receivers = Vec::with_capacity(addresses.len());
    for addr in addresses.iter() {
        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
        tx.send(SignRequest {
            address: *addr,
            response: resp_tx,
        })
        .await
        .unwrap();
        receivers.push(resp_rx);
    }
    drop(tx);

    // Collect all responses
    let mut results = Vec::with_capacity(addresses.len());
    for rx in receivers {
        results.push(rx.await.unwrap());
    }
    results
}

/// Helper to run async streaming verify benchmark (hybrid: tokio + rayon)
async fn run_streaming_verify(
    stamps: &[(Stamp, SwarmAddress)],
    batch_size: usize,
) -> Vec<Result<Address, StreamVerifyError>> {
    use nectar_postage::streaming::streaming_verifier;

    // channel_size controls backpressure, batch_size controls rayon parallelism granularity
    let tx = streaming_verifier(100, batch_size);

    // Send all requests and collect response receivers
    let mut receivers = Vec::with_capacity(stamps.len());
    for (stamp, addr) in stamps.iter() {
        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
        tx.send(VerifyRequest {
            stamp: stamp.clone(),
            address: *addr,
            response: resp_tx,
        })
        .await
        .unwrap();
        receivers.push(resp_rx);
    }
    drop(tx);

    // Collect all responses
    let mut results = Vec::with_capacity(stamps.len());
    for rx in receivers {
        results.push(rx.await.unwrap());
    }
    results
}

fn bench_streaming_sign(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let signer = Arc::new(PrivateKeySigner::random());
    let addresses_1000: Vec<SwarmAddress> = (0..1000).map(|_| random_address()).collect();

    let mut group = c.benchmark_group("ecdsa_sign_streaming_hybrid");
    group.throughput(Throughput::Elements(1000));

    // 1000 stamps with batch_size=64 (rayon processes 64 at a time)
    group.bench_function("throughput_1000_batch64", |b| {
        b.iter(|| {
            rt.block_on(run_streaming_sign(&signer, &addresses_1000, 64))
        })
    });

    // 1000 stamps with batch_size=256
    group.bench_function("throughput_1000_batch256", |b| {
        b.iter(|| {
            rt.block_on(run_streaming_sign(&signer, &addresses_1000, 256))
        })
    });

    // 1000 stamps with batch_size=1000 (process all at once)
    group.bench_function("throughput_1000_batch1000", |b| {
        b.iter(|| {
            rt.block_on(run_streaming_sign(&signer, &addresses_1000, 1000))
        })
    });

    group.finish();
}

fn bench_streaming_verify(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let signer = PrivateKeySigner::random();

    // Use sign_message_sync for EIP-191 compatibility with Go/bee
    let sign_fn = |prehash: &B256| -> Result<Signature, StampError> {
        Ok(signer
            .sign_message_sync(prehash.as_slice())
            .map_err(alloy_signer::Error::other)?)
    };

    // Pre-generate stamps for verification
    let addresses_1000: Vec<SwarmAddress> = (0..1000).map(|_| random_address()).collect();
    let issuer_1000 = ShardedIssuer::new(B256::ZERO, 32, 16);
    let results_1000 = sign_stamps_parallel(&issuer_1000, &sign_fn, &addresses_1000);
    let stamps_with_addrs: Vec<(Stamp, SwarmAddress)> = results_1000
        .iter()
        .zip(addresses_1000.iter())
        .map(|(r, addr)| (r.result.as_ref().unwrap().clone(), *addr))
        .collect();

    let mut group = c.benchmark_group("ecdsa_verify_streaming_hybrid");
    group.throughput(Throughput::Elements(1000));

    // 1000 stamps with batch_size=64
    group.bench_function("throughput_1000_batch64", |b| {
        b.iter(|| {
            rt.block_on(run_streaming_verify(&stamps_with_addrs, 64))
        })
    });

    // 1000 stamps with batch_size=256
    group.bench_function("throughput_1000_batch256", |b| {
        b.iter(|| {
            rt.block_on(run_streaming_verify(&stamps_with_addrs, 256))
        })
    });

    // 1000 stamps with batch_size=1000
    group.bench_function("throughput_1000_batch1000", |b| {
        b.iter(|| {
            rt.block_on(run_streaming_verify(&stamps_with_addrs, 1000))
        })
    });

    group.finish();
}

/// Comparison benchmark: parallel (rayon-only) vs streaming (tokio+rayon hybrid)
fn bench_parallel_vs_streaming(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let signer = Arc::new(PrivateKeySigner::random());
    let addresses: Vec<SwarmAddress> = (0..1000).map(|_| random_address()).collect();

    // Use sign_message_sync for EIP-191 compatibility with Go/bee
    let signer_for_parallel = Arc::clone(&signer);
    let sign_fn = move |prehash: &B256| -> Result<Signature, StampError> {
        Ok(signer_for_parallel
            .sign_message_sync(prehash.as_slice())
            .map_err(alloy_signer::Error::other)?)
    };

    let mut group = c.benchmark_group("sign_1000_parallel_vs_streaming");
    group.throughput(Throughput::Elements(1000));

    // Parallel (rayon-only, batch collect)
    group.bench_function("parallel_rayon", |b| {
        b.iter(|| {
            let issuer = ShardedIssuer::new(B256::ZERO, 32, 16);
            black_box(sign_stamps_parallel(&issuer, &sign_fn, &addresses))
        })
    });

    // Streaming hybrid (tokio+rayon) with batch_size=256
    group.bench_function("streaming_hybrid_batch256", |b| {
        b.iter(|| {
            rt.block_on(run_streaming_sign(&signer, &addresses, 256))
        })
    });

    group.finish();

    // Verification comparison
    let issuer = ShardedIssuer::new(B256::ZERO, 32, 16);
    let results = sign_stamps_parallel(&issuer, &sign_fn, &addresses);
    let stamps_with_addrs: Vec<(Stamp, SwarmAddress)> = results
        .iter()
        .zip(addresses.iter())
        .map(|(r, addr)| (r.result.as_ref().unwrap().clone(), *addr))
        .collect();

    // Also prepare for parallel verification
    let verify_input: Vec<_> = stamps_with_addrs
        .iter()
        .map(|(stamp, addr)| (stamp, addr))
        .collect();

    let mut group = c.benchmark_group("verify_1000_parallel_vs_streaming");
    group.throughput(Throughput::Elements(1000));

    // Parallel (rayon-only)
    group.bench_function("parallel_rayon", |b| {
        b.iter(|| black_box(verify_stamps_parallel(&verify_input)))
    });

    // Streaming hybrid (tokio+rayon) with batch_size=256
    group.bench_function("streaming_hybrid_batch256", |b| {
        b.iter(|| {
            rt.block_on(run_streaming_verify(&stamps_with_addrs, 256))
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_stamp_roundtrip,
    bench_stamp_throughput,
    bench_stamp_index_roundtrip,
    bench_validate_index,
    bench_stamp_digest_prehash,
    bench_stamper_mock,
    bench_ecdsa_sign_sequential,
    bench_ecdsa_verify_sequential,
    bench_ecdsa_sign_parallel,
    bench_ecdsa_verify_parallel,
    bench_comparison,
    bench_streaming_sign,
    bench_streaming_verify,
    bench_parallel_vs_streaming,
);

criterion_main!(benches);
