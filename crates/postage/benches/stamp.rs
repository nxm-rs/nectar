//! Benchmarks for postage stamp operations.

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use nectar_postage::{
    parallel::{sign_stamps_parallel, verify_stamps_parallel, ShardedIssuer, StampWithAddress},
    Batch, BatchStamper, BatchValidation, SignerError, Stamp, StampBytes, StampDigest, StampError,
    StampIndex, StampSigner, Stamper,
};
use nectar_primitives::SwarmAddress;
use alloy_primitives::{Address, B256, U256};
use alloy_signer::{Signature, SignerSync};
use alloy_signer_local::PrivateKeySigner;
use rand::Rng;

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
    type Error = SignerError;

    fn sign_message(&self, _prehash: &B256) -> Result<Signature, SignerError> {
        Ok(Signature::new(U256::from(1), U256::from(2), false))
    }
}

fn bench_stamper_mock(c: &mut Criterion) {
    let mut group = c.benchmark_group("stamper_mock");

    group.bench_function("single", |b| {
        b.iter(|| {
            let batch = Batch::new(B256::ZERO, 0, 0, Address::ZERO, 24, 16, false);
            let mut stamper = BatchStamper::new(batch, MockSigner);
            let address = random_address();
            black_box(stamper.stamp(black_box(&address)))
        })
    });

    let addresses: Vec<SwarmAddress> = (0..1000).map(|_| random_address()).collect();
    group.throughput(Throughput::Elements(1000));

    group.bench_function("throughput_1000", |b| {
        b.iter(|| {
            let batch = Batch::new(B256::ZERO, 0, 0, Address::ZERO, 32, 16, false);
            let mut stamper = BatchStamper::new(batch, MockSigner);
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
    type Error = SignerError;

    fn sign_message(&self, prehash: &B256) -> Result<Signature, SignerError> {
        // Use sign_message_sync for EIP-191 compatibility with Go/bee
        self.0.sign_message_sync(prehash.as_slice()).map_err(|_| SignerError)
    }
}

impl StampSigner for &EcdsaSigner {
    type Error = SignerError;

    fn sign_message(&self, prehash: &B256) -> Result<Signature, SignerError> {
        // Use sign_message_sync for EIP-191 compatibility with Go/bee
        self.0.sign_message_sync(prehash.as_slice()).map_err(|_| SignerError)
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
            let batch = Batch::new(B256::ZERO, 0, 0, Address::ZERO, 24, 16, false);
            let mut stamper = BatchStamper::new(batch, &signer);
            let address = random_address();
            black_box(stamper.stamp(black_box(&address)))
        })
    });

    group.throughput(Throughput::Elements(100));

    group.bench_function("throughput_100", |b| {
        b.iter(|| {
            let batch = Batch::new(B256::ZERO, 0, 0, Address::ZERO, 32, 16, false);
            let mut stamper = BatchStamper::new(batch, &signer);
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
    let batch = Batch::new(B256::ZERO, 0, 0, expected_address, 32, 16, false);
    let mut stamper = BatchStamper::new(batch, &signer);

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
        signer
            .sign_message_sync(prehash.as_slice())
            .map_err(|_| StampError::SigningFailed("signing failed"))
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
    let expected_address = signer.address();

    // Use sign_message_sync for EIP-191 compatibility with Go/bee
    let sign_fn = |prehash: &B256| -> Result<Signature, StampError> {
        signer
            .sign_message_sync(prehash.as_slice())
            .map_err(|_| StampError::SigningFailed("signing failed"))
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
        .map(|(stamp, addr)| StampWithAddress {
            stamp,
            address: addr,
        })
        .collect();

    group.throughput(Throughput::Elements(100));
    group.bench_function("throughput_100", |b| {
        b.iter(|| black_box(verify_stamps_parallel(&verify_input_100)))
    });

    // 1000 stamps
    let verify_input_1000: Vec<_> = stamps_1000
        .iter()
        .zip(addresses_1000.iter())
        .map(|(stamp, addr)| StampWithAddress {
            stamp,
            address: addr,
        })
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
        signer
            .sign_message_sync(prehash.as_slice())
            .map_err(|_| StampError::SigningFailed("signing failed"))
    };

    // Wrapper to implement StampSigner with EIP-191 compatibility
    struct SignerWrapper<'a>(&'a PrivateKeySigner);

    impl StampSigner for SignerWrapper<'_> {
        type Error = SignerError;

        fn sign_message(&self, prehash: &B256) -> Result<Signature, SignerError> {
            // Use sign_message_sync for EIP-191 compatibility with Go/bee
            self.0.sign_message_sync(prehash.as_slice()).map_err(|_| SignerError)
        }
    }

    let mut group = c.benchmark_group("sign_1000_comparison");
    group.throughput(Throughput::Elements(1000));

    // Sequential
    group.bench_function("sequential", |b| {
        b.iter(|| {
            let batch = Batch::new(B256::ZERO, 0, 0, Address::ZERO, 32, 16, false);
            let mut stamper = BatchStamper::new(batch, SignerWrapper(&signer));
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
        .map(|(stamp, addr)| StampWithAddress {
            stamp,
            address: addr,
        })
        .collect();

    group.bench_function("parallel", |b| {
        b.iter(|| black_box(verify_stamps_parallel(&verify_input)))
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
);

criterion_main!(benches);
