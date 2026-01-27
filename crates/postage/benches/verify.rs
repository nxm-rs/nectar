//! Benchmarks for postage stamp verification operations.
//!
//! This benchmark file focuses on verification-only operations, suitable for
//! node/vertex use cases that primarily verify stamps rather than create them.

use alloy_primitives::{Address, B256, Signature};
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use nectar_postage::{
    Batch, Stamp, StampBytes, StampDigest, StampIndex,
    parallel::{verify_stamps_parallel, verify_stamps_parallel_with_pubkey},
};
use nectar_primitives::SwarmAddress;
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

    // Use test signature for benchmarking serialization
    let sig = Signature::test_signature();

    Stamp::new(batch, bucket, index, timestamp, sig)
}

/// Generate a random SwarmAddress for benchmarking.
fn random_address() -> SwarmAddress {
    let mut rng = rand::rng();
    let mut bytes = [0u8; 32];
    rng.fill(&mut bytes);
    SwarmAddress::new(bytes)
}

/// Creates a valid stamp signed by the given signer.
fn create_signed_stamp(
    signer: &PrivateKeySigner,
    chunk_address: &SwarmAddress,
    batch_id: B256,
) -> Stamp {
    let index = StampIndex::new(0, 0);
    let timestamp = 12345u64;
    let digest = StampDigest::new(*chunk_address, batch_id, index, timestamp);
    let prehash = digest.to_prehash();

    // sign_message_sync returns alloy_primitives::Signature directly
    let sig = signer.sign_message_sync(prehash.as_slice()).unwrap();
    Stamp::with_index(batch_id, index, timestamp, sig)
}

// Stamp Serialization Benchmarks

fn bench_stamp_roundtrip(c: &mut Criterion) {
    let stamp = random_stamp();

    c.bench_function("stamp_roundtrip", |b| {
        b.iter(|| {
            let encoded = stamp.to_bytes();
            black_box(Stamp::from_bytes(black_box(&encoded)).unwrap())
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
                black_box(Stamp::from_bytes(b_arr).unwrap());
            }
        })
    });

    group.finish();
}

// StampIndex Benchmarks

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

// Validation Benchmarks

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

// Digest/Prehash Benchmarks

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

// Sequential ECDSA Verification Benchmarks

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

    // stamp.signature() now returns &Signature directly
    stamp
        .signature()
        .recover_address_from_msg(prehash.as_slice())
}

fn bench_ecdsa_verify_sequential(c: &mut Criterion) {
    let signer = PrivateKeySigner::random();
    let expected_address = signer.address();
    let batch_id = B256::ZERO;

    let single_addr = random_address();
    let single_stamp = create_signed_stamp(&signer, &single_addr, batch_id);

    let test_data: Vec<(SwarmAddress, Stamp)> = (0..100)
        .map(|_| {
            let addr = random_address();
            let stamp = create_signed_stamp(&signer, &addr, batch_id);
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

// Cached Public Key Verification Benchmarks

fn bench_ecdsa_verify_with_pubkey(c: &mut Criterion) {
    let signer = PrivateKeySigner::random();
    let batch_id = B256::ZERO;

    // Create stamps
    let addresses: Vec<SwarmAddress> = (0..100).map(|_| random_address()).collect();
    let stamps: Vec<Stamp> = addresses
        .iter()
        .map(|addr| create_signed_stamp(&signer, addr, batch_id))
        .collect();

    // Recover pubkey from first stamp (simulating cached pubkey)
    let pubkey = stamps[0].recover_pubkey(&addresses[0]).unwrap();

    let mut group = c.benchmark_group("ecdsa_verify_with_pubkey");

    // Single stamp with cached pubkey
    group.bench_function("single_cached", |b| {
        b.iter(|| {
            black_box(stamps[0].verify_with_pubkey(black_box(&addresses[0]), black_box(&pubkey)))
        })
    });

    // Throughput with cached pubkey
    group.throughput(Throughput::Elements(100));
    group.bench_function("throughput_100_cached", |b| {
        b.iter(|| {
            for (stamp, addr) in stamps.iter().zip(addresses.iter()) {
                black_box(stamp.verify_with_pubkey(addr, &pubkey).unwrap());
            }
        })
    });

    group.finish();
}

// Parallel ECDSA Verification Benchmarks

fn bench_ecdsa_verify_parallel(c: &mut Criterion) {
    let signer = PrivateKeySigner::random();
    let batch_id = B256::ZERO;

    // Pre-generate 100 stamps for verification
    let addresses_100: Vec<SwarmAddress> = (0..100).map(|_| random_address()).collect();
    let stamps_100: Vec<Stamp> = addresses_100
        .iter()
        .map(|addr| create_signed_stamp(&signer, addr, batch_id))
        .collect();

    // Pre-generate 1000 stamps for verification
    let addresses_1000: Vec<SwarmAddress> = (0..1000).map(|_| random_address()).collect();
    let stamps_1000: Vec<Stamp> = addresses_1000
        .iter()
        .map(|addr| create_signed_stamp(&signer, addr, batch_id))
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

// Parallel Verification with Cached Pubkey

fn bench_ecdsa_verify_parallel_with_pubkey(c: &mut Criterion) {
    let signer = PrivateKeySigner::random();
    let batch_id = B256::ZERO;

    // Pre-generate 100 stamps for verification
    let addresses_100: Vec<SwarmAddress> = (0..100).map(|_| random_address()).collect();
    let stamps_100: Vec<Stamp> = addresses_100
        .iter()
        .map(|addr| create_signed_stamp(&signer, addr, batch_id))
        .collect();

    // Pre-generate 1000 stamps for verification
    let addresses_1000: Vec<SwarmAddress> = (0..1000).map(|_| random_address()).collect();
    let stamps_1000: Vec<Stamp> = addresses_1000
        .iter()
        .map(|addr| create_signed_stamp(&signer, addr, batch_id))
        .collect();

    // Recover pubkey from first stamp (simulating cached pubkey)
    let pubkey = stamps_100[0].recover_pubkey(&addresses_100[0]).unwrap();

    let mut group = c.benchmark_group("ecdsa_verify_parallel_cached");

    // 100 stamps with cached pubkey
    let verify_input_100: Vec<_> = stamps_100
        .iter()
        .zip(addresses_100.iter())
        .map(|(stamp, addr)| (stamp, addr))
        .collect();

    group.throughput(Throughput::Elements(100));
    group.bench_function("throughput_100", |b| {
        b.iter(|| {
            black_box(verify_stamps_parallel_with_pubkey(
                &verify_input_100,
                &pubkey,
            ))
        })
    });

    // 1000 stamps with cached pubkey
    let verify_input_1000: Vec<_> = stamps_1000
        .iter()
        .zip(addresses_1000.iter())
        .map(|(stamp, addr)| (stamp, addr))
        .collect();

    group.throughput(Throughput::Elements(1000));
    group.bench_function("throughput_1000", |b| {
        b.iter(|| {
            black_box(verify_stamps_parallel_with_pubkey(
                &verify_input_1000,
                &pubkey,
            ))
        })
    });

    group.finish();
}

// Comparison: Recovery vs Cached Pubkey Verification

fn bench_verify_comparison(c: &mut Criterion) {
    let signer = PrivateKeySigner::random();
    let batch_id = B256::ZERO;

    // Pre-generate 1000 stamps
    let addresses: Vec<SwarmAddress> = (0..1000).map(|_| random_address()).collect();
    let stamps: Vec<Stamp> = addresses
        .iter()
        .map(|addr| create_signed_stamp(&signer, addr, batch_id))
        .collect();

    // Recover pubkey from first stamp
    let pubkey = stamps[0].recover_pubkey(&addresses[0]).unwrap();
    let expected_address = signer.address();

    let mut group = c.benchmark_group("verify_1000_comparison");
    group.throughput(Throughput::Elements(1000));

    // Sequential with full recovery
    group.bench_function("sequential_recovery", |b| {
        b.iter(|| {
            for (addr, stamp) in addresses.iter().zip(stamps.iter()) {
                let recovered = recover_stamp_signer(stamp, addr).unwrap();
                black_box(recovered == expected_address);
            }
        })
    });

    // Sequential with cached pubkey (~10x faster)
    group.bench_function("sequential_cached", |b| {
        b.iter(|| {
            for (addr, stamp) in addresses.iter().zip(stamps.iter()) {
                black_box(stamp.verify_with_pubkey(addr, &pubkey).unwrap());
            }
        })
    });

    // Parallel with full recovery
    let verify_input: Vec<_> = stamps
        .iter()
        .zip(addresses.iter())
        .map(|(stamp, addr)| (stamp, addr))
        .collect();

    group.bench_function("parallel_recovery", |b| {
        b.iter(|| black_box(verify_stamps_parallel(&verify_input)))
    });

    // Parallel with cached pubkey (~10x faster)
    group.bench_function("parallel_cached", |b| {
        b.iter(|| black_box(verify_stamps_parallel_with_pubkey(&verify_input, &pubkey)))
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
    bench_ecdsa_verify_sequential,
    bench_ecdsa_verify_with_pubkey,
    bench_ecdsa_verify_parallel,
    bench_ecdsa_verify_parallel_with_pubkey,
    bench_verify_comparison,
);

criterion_main!(benches);
