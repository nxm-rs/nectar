#![allow(missing_docs)]
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use rand::{RngCore, rng};

use nectar_primitives::encryption::{self, EncryptionKey, transcrypt};
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

criterion_group!(
    benches,
    bench_transcrypt,
    bench_encrypt_chunk,
    bench_decrypt_chunk
);
criterion_main!(benches);
