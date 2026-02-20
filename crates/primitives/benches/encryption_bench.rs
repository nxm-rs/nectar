#![allow(missing_docs)]
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use rand::{RngCore, rng};

use nectar_primitives::chunk::encryption::{self, EncryptionKey, transcrypt, transcrypt_in_place};
use nectar_primitives::{DEFAULT_BODY_SIZE, bmt::SPAN_SIZE};

fn bench_transcrypt(c: &mut Criterion) {
    let mut group = c.benchmark_group("transcrypt");
    let key = EncryptionKey::generate();

    for &size in &[32, 256, 1024, 4096] {
        let mut input = vec![0u8; size];
        rng().fill_bytes(&mut input);
        let mut output = vec![0u8; size];

        group.bench_with_input(BenchmarkId::from_parameter(size), &input, |b, input| {
            b.iter(|| {
                transcrypt(&key, 0, input, &mut output);
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
        let mut chunk_data = vec![0u8; SPAN_SIZE + data_size];
        chunk_data[..SPAN_SIZE].copy_from_slice(&(data_size as u64).to_le_bytes());
        if data_size > 0 {
            rng().fill_bytes(&mut chunk_data[SPAN_SIZE..]);
        }

        group.bench_with_input(
            BenchmarkId::new("data_bytes", data_size),
            &chunk_data,
            |b, chunk_data| {
                b.iter(|| {
                    black_box(encryption::encrypt_chunk::<DEFAULT_BODY_SIZE>(chunk_data).unwrap());
                });
            },
        );
    }

    group.finish();
}

fn bench_decrypt_chunk(c: &mut Criterion) {
    let mut group = c.benchmark_group("decrypt_chunk");

    for &data_size in &[0, 100, 1024, 4096] {
        let mut chunk_data = vec![0u8; SPAN_SIZE + data_size];
        chunk_data[..SPAN_SIZE].copy_from_slice(&(data_size as u64).to_le_bytes());
        if data_size > 0 {
            rng().fill_bytes(&mut chunk_data[SPAN_SIZE..]);
        }

        let (key, encrypted) =
            encryption::encrypt_chunk::<DEFAULT_BODY_SIZE>(&chunk_data).unwrap();

        group.bench_with_input(
            BenchmarkId::new("data_bytes", data_size),
            &encrypted,
            |b, encrypted| {
                b.iter(|| {
                    black_box(
                        encryption::decrypt_chunk_data::<DEFAULT_BODY_SIZE>(
                            encrypted, &key, data_size,
                        )
                        .unwrap(),
                    );
                });
            },
        );
    }

    group.finish();
}

fn bench_decrypt_chunk_into(c: &mut Criterion) {
    let mut group = c.benchmark_group("decrypt_chunk_into");

    for &data_size in &[0, 100, 1024, 4096] {
        let mut chunk_data = vec![0u8; SPAN_SIZE + data_size];
        chunk_data[..SPAN_SIZE].copy_from_slice(&(data_size as u64).to_le_bytes());
        if data_size > 0 {
            rng().fill_bytes(&mut chunk_data[SPAN_SIZE..]);
        }

        let (key, encrypted) =
            encryption::encrypt_chunk::<DEFAULT_BODY_SIZE>(&chunk_data).unwrap();

        // Pre-allocate the output buffer (measured separately from decrypt)
        let mut output = vec![0u8; SPAN_SIZE + data_size];

        group.bench_with_input(
            BenchmarkId::new("data_bytes", data_size),
            &encrypted,
            |b, encrypted| {
                b.iter(|| {
                    encryption::decrypt_chunk_into::<DEFAULT_BODY_SIZE>(
                        encrypted, &key, data_size, &mut output,
                    )
                    .unwrap();
                    black_box(&output);
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
    bench_decrypt_chunk_into,
);
criterion_main!(benches);
