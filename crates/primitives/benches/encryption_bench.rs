#![allow(missing_docs)]
// Bench and example code: unwraps, direct indexing, casts, and assertions
// are setup and illustration, not shipped surface.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::panic,
    clippy::panic_in_result_fn,
    clippy::as_conversions,
    clippy::missing_panics_doc
)]
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use rand::{Rng, rng};

use nectar_primitives::chunk::encryption::{
    ChunkEncrypt, EncryptionKey, transcrypt, transcrypt_in_place,
};
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

criterion_group!(
    benches,
    bench_transcrypt,
    bench_transcrypt_in_place,
    bench_encrypt_chunk,
    bench_decrypt_chunk,
);
criterion_main!(benches);
