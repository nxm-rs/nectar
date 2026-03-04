#![allow(missing_docs)]
use std::collections::HashMap;
use std::io::Write;

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use rand::{RngCore, rng};

use nectar_primitives::chunk::encryption::{
    self, ChunkEncrypt, EncryptionKey, transcrypt, transcrypt_in_place,
};
use nectar_primitives::chunk::{AnyChunk, ChunkAddress};
use nectar_primitives::file::{
    EncryptedSyncJoiner, EncryptedSyncParallelSplitter, EncryptedSyncSplitter, SyncJoiner,
    SyncParallelSplitter, SyncSplitter, sync_split, sync_split_encrypted,
};
use nectar_primitives::store::MemoryStore;
use nectar_primitives::{ContentChunk, DEFAULT_BODY_SIZE};

fn bench_transcrypt(c: &mut Criterion) {
    let mut group = c.benchmark_group("transcrypt");
    let key = EncryptionKey::generate();

    for &size in &[32, 256, 1024, 4096] {
        let mut input = vec![0u8; size];
        rng().fill_bytes(&mut input);
        let mut output = vec![0u8; size];

        group.bench_with_input(BenchmarkId::from_parameter(size), &input, |b, input| {
            b.iter(|| {
                transcrypt(&key, 0, input, &mut output).unwrap();
                black_box(&output);
            });
        });
    }

    group.finish();
}

fn bench_transcrypt_in_place(c: &mut Criterion) {
    let mut group = c.benchmark_group("transcrypt_in_place");
    let key = EncryptionKey::generate();

    for &size in &[32, 256, 1024, 4096] {
        let mut data = vec![0u8; size];
        rng().fill_bytes(&mut data);
        // Keep a copy to reset between iterations
        let original = data.clone();

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| {
                data.copy_from_slice(&original);
                transcrypt_in_place(&key, 0, &mut data);
                black_box(&data);
            });
        });
    }

    group.finish();
}

fn bench_encrypt_chunk(c: &mut Criterion) {
    let mut group = c.benchmark_group("encrypt_chunk");

    for &data_size in &[0, 100, 1024, 4096] {
        let mut data = vec![0u8; data_size];
        if data_size > 0 {
            rng().fill_bytes(&mut data);
        }
        let chunk = ContentChunk::<DEFAULT_BODY_SIZE>::new(data).unwrap();
        let key = EncryptionKey::generate();

        group.bench_with_input(
            BenchmarkId::new("data_bytes", data_size),
            &chunk,
            |b, chunk| {
                b.iter(|| {
                    black_box(chunk.encrypt_with(&key).unwrap());
                });
            },
        );
    }

    group.finish();
}

fn bench_decrypt_chunk(c: &mut Criterion) {
    let mut group = c.benchmark_group("decrypt_chunk");

    for &data_size in &[0, 100, 1024, 4096] {
        let mut data = vec![0u8; data_size];
        if data_size > 0 {
            rng().fill_bytes(&mut data);
        }
        let chunk = ContentChunk::<DEFAULT_BODY_SIZE>::new(data).unwrap();
        let encrypted = chunk.encrypt().unwrap();

        group.bench_with_input(
            BenchmarkId::new("data_bytes", data_size),
            &encrypted,
            |b, encrypted| {
                b.iter(|| {
                    black_box(encrypted.decrypt().unwrap());
                });
            },
        );
    }

    group.finish();
}

/// File sizes for encrypted benchmarks.
const SIZES: &[(u64, &str)] = &[
    (4 * 1024, "4KB"),
    (32 * 1024, "32KB"),
    (256 * 1024, "256KB"),
    (1024 * 1024, "1MB"),
    (4 * 1024 * 1024, "4MB"),
    (16 * 1024 * 1024, "16MB"),
];

const COMPARISON_SIZES: &[(u64, &str)] = &[
    (4 * 1024, "4KB"),
    (256 * 1024, "256KB"),
    (1024 * 1024, "1MB"),
    (4 * 1024 * 1024, "4MB"),
];

fn random_data(size: u64) -> Vec<u8> {
    let mut data = vec![0u8; size as usize];
    rng().fill_bytes(&mut data);
    data
}

fn split_to_store(data: &[u8]) -> (ChunkAddress, HashMap<ChunkAddress, AnyChunk>) {
    let (root, store) = sync_split::<DEFAULT_BODY_SIZE>(data).unwrap();
    (root, store.into_chunks())
}

fn encrypted_split_to_store(
    data: &[u8],
) -> (
    encryption::EncryptedChunkRef,
    HashMap<ChunkAddress, AnyChunk>,
) {
    let (root_ref, store) = sync_split_encrypted::<DEFAULT_BODY_SIZE>(data).unwrap();
    (root_ref, store.into_chunks())
}

fn bench_encrypted_streaming_splitter(c: &mut Criterion) {
    let mut group = c.benchmark_group("encrypted_split_streaming");

    for &(size, name) in SIZES {
        let data = random_data(size);

        group.throughput(Throughput::Bytes(size));
        group.bench_with_input(BenchmarkId::from_parameter(name), &data, |b, data| {
            b.iter(|| {
                let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
                let mut splitter = EncryptedSyncSplitter::new(store, data.len() as u64);
                splitter.write_all(data).unwrap();
                black_box(splitter.finish().unwrap())
            });
        });
    }

    group.finish();
}

fn bench_encrypted_parallel_splitter(c: &mut Criterion) {
    let mut group = c.benchmark_group("encrypted_split_parallel");

    for &(size, name) in SIZES {
        let data = random_data(size);

        group.throughput(Throughput::Bytes(size));
        group.bench_with_input(BenchmarkId::from_parameter(name), &data, |b, data| {
            b.iter(|| {
                let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
                let splitter = EncryptedSyncParallelSplitter::new(store);
                let root_ref = splitter.split(data).unwrap();
                black_box((root_ref, splitter.into_store()))
            });
        });
    }

    group.finish();
}

fn bench_encrypted_joiner(c: &mut Criterion) {
    let mut group = c.benchmark_group("encrypted_join");

    for &(size, name) in SIZES {
        let data = random_data(size);
        let (root_ref, store) = encrypted_split_to_store(&data);

        group.throughput(Throughput::Bytes(size));
        group.bench_with_input(
            BenchmarkId::from_parameter(name),
            &root_ref,
            |b, root_ref| {
                b.iter(|| {
                    let joiner = EncryptedSyncJoiner::new(store.clone(), root_ref.clone()).unwrap();
                    black_box(joiner.read_all().unwrap())
                });
            },
        );
    }

    group.finish();
}

/// Compare plain vs encrypted at the same sizes.
fn bench_plain_vs_encrypted_split(c: &mut Criterion) {
    let mut group = c.benchmark_group("split_plain_vs_encrypted");

    for &(size, name) in COMPARISON_SIZES {
        let data = random_data(size);

        group.throughput(Throughput::Bytes(size));

        group.bench_with_input(
            BenchmarkId::new("plain_streaming", name),
            &data,
            |b, data| {
                b.iter(|| {
                    let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
                    let mut splitter = SyncSplitter::new(store, data.len() as u64);
                    splitter.write_all(data).unwrap();
                    black_box(splitter.finish().unwrap())
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("encrypted_streaming", name),
            &data,
            |b, data| {
                b.iter(|| {
                    let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
                    let mut splitter = EncryptedSyncSplitter::new(store, data.len() as u64);
                    splitter.write_all(data).unwrap();
                    black_box(splitter.finish().unwrap())
                });
            },
        );

        group.bench_with_input(BenchmarkId::new("plain_direct", name), &data, |b, data| {
            b.iter(|| {
                let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
                let splitter = SyncParallelSplitter::new(store);
                let root = splitter.split(data).unwrap();
                black_box((root, splitter.into_store()))
            });
        });

        group.bench_with_input(
            BenchmarkId::new("encrypted_direct", name),
            &data,
            |b, data| {
                b.iter(|| {
                    let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
                    let splitter = EncryptedSyncParallelSplitter::new(store);
                    let root_ref = splitter.split(data).unwrap();
                    black_box((root_ref, splitter.into_store()))
                });
            },
        );
    }

    group.finish();
}

/// Compare plain vs encrypted join.
fn bench_plain_vs_encrypted_join(c: &mut Criterion) {
    let mut group = c.benchmark_group("join_plain_vs_encrypted");

    for &(size, name) in COMPARISON_SIZES {
        let data = random_data(size);
        let (root, store) = split_to_store(&data);
        let (enc_root_ref, enc_store) = encrypted_split_to_store(&data);

        group.throughput(Throughput::Bytes(size));

        group.bench_with_input(BenchmarkId::new("plain", name), &root, |b, root| {
            b.iter(|| {
                let joiner = SyncJoiner::new(store.clone(), *root).unwrap();
                black_box(joiner.read_all().unwrap())
            });
        });

        group.bench_with_input(
            BenchmarkId::new("encrypted", name),
            &enc_root_ref,
            |b, root_ref| {
                b.iter(|| {
                    let joiner =
                        EncryptedSyncJoiner::new(enc_store.clone(), root_ref.clone()).unwrap();
                    black_box(joiner.read_all().unwrap())
                });
            },
        );
    }

    group.finish();
}

/// Encrypted round-trip (split + join).
fn bench_encrypted_roundtrip(c: &mut Criterion) {
    let mut group = c.benchmark_group("encrypted_roundtrip");

    let roundtrip_sizes: &[(u64, &str)] = &[
        (4 * 1024, "4KB"),
        (256 * 1024, "256KB"),
        (1024 * 1024, "1MB"),
        (4 * 1024 * 1024, "4MB"),
    ];

    for &(size, name) in roundtrip_sizes {
        let data = random_data(size);

        group.throughput(Throughput::Bytes(size));

        group.bench_with_input(BenchmarkId::new("streaming", name), &data, |b, data| {
            b.iter(|| {
                let (root_ref, store) = encrypted_split_to_store(data);
                let joiner = EncryptedSyncJoiner::new(store, root_ref).unwrap();
                black_box(joiner.read_all().unwrap())
            });
        });

        group.bench_with_input(BenchmarkId::new("direct", name), &data, |b, data| {
            b.iter(|| {
                let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
                let splitter = EncryptedSyncParallelSplitter::new(store);
                let root_ref = splitter.split(data).unwrap();
                let store = splitter.into_store();
                let joiner = EncryptedSyncJoiner::new(store, root_ref).unwrap();
                black_box(joiner.read_all().unwrap())
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_transcrypt,
    bench_transcrypt_in_place,
    bench_encrypt_chunk,
    bench_decrypt_chunk,
    bench_encrypted_streaming_splitter,
    bench_encrypted_parallel_splitter,
    bench_encrypted_joiner,
    bench_plain_vs_encrypted_split,
    bench_plain_vs_encrypted_join,
    bench_encrypted_roundtrip,
);
criterion_main!(benches);
