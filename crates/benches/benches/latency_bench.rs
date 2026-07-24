//! Latency and throughput benches over an injectable-latency store.
//!
//! The store charges a fixed delay (0, 10 or 100 ms) per get and put,
//! modelling a remote chunk path. Sweeping the fetch and put windows around
//! the sixteen-slot default witnesses the two-budget admission overlap:
//! drain time tracks `latency * batches`, not `latency * chunks`. The deep
//! group narrows the body so intermediate levels dominate, exercising the
//! derived branch budget alongside the leaf window.

// The latency model is wall clock under a real tokio runtime, so the runtime's
// own `block_on` is the entry point here.
#![allow(clippy::disallowed_methods)]

use core::time::Duration;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use criterion::measurement::WallTime;
use criterion::{
    BatchSize, BenchmarkGroup, BenchmarkId, Criterion, SamplingMode, Throughput, black_box,
    criterion_group, criterion_main,
};
use nectar_file::{File, Plain, PutWindow, Split, Window};
use nectar_primitives::DEFAULT_BODY_SIZE;
use nectar_primitives::chunk::{AnyChunkSet, Chunk, ChunkAddress, Verified};
use nectar_primitives::store::{ChunkGet, ChunkPut, ChunkStoreError};
use tokio::runtime::Runtime;

/// Narrow body for the deep group: fan-out 8, so a hundred leaves already
/// build three intermediate levels.
const DEEP_BODY: usize = 256;

/// Window sweep around the sixteen-slot default, serial baseline included.
const WINDOWS: &[u16] = &[1, 4, 16, 64];

/// Shared in-memory chunk store charging a fixed latency per operation.
#[derive(Clone)]
struct LatencyStore<const B: usize> {
    chunks: Arc<Mutex<HashMap<ChunkAddress, Chunk<Verified, AnyChunkSet<B>>>>>,
    latency: Duration,
}

impl<const B: usize> LatencyStore<B> {
    fn new(latency: Duration) -> Self {
        Self {
            chunks: Arc::new(Mutex::new(HashMap::new())),
            latency,
        }
    }

    /// A handle over the same chunks at a different latency, so reads run
    /// against a tree seeded at zero cost.
    fn with_latency(&self, latency: Duration) -> Self {
        Self {
            chunks: Arc::clone(&self.chunks),
            latency,
        }
    }

    /// Charge one operation's latency; concurrent charges overlap.
    async fn charge(&self) {
        if !self.latency.is_zero() {
            tokio::time::sleep(self.latency).await;
        }
    }
}

impl<const B: usize> ChunkGet<AnyChunkSet<B>> for LatencyStore<B> {
    type Trust = Verified;
    type Error = ChunkStoreError;

    async fn get(
        &self,
        address: &ChunkAddress,
    ) -> Result<Chunk<Verified, AnyChunkSet<B>>, ChunkStoreError> {
        self.charge().await;
        self.chunks
            .lock()
            .unwrap()
            .get(address)
            .cloned()
            .ok_or_else(|| ChunkStoreError::not_found(address))
    }
}

impl<const B: usize> ChunkPut<AnyChunkSet<B>> for LatencyStore<B> {
    type Error = ChunkStoreError;

    async fn put(&self, chunk: Chunk<Verified, AnyChunkSet<B>>) -> Result<(), ChunkStoreError> {
        self.charge().await;
        self.chunks.lock().unwrap().insert(*chunk.address(), chunk);
        Ok(())
    }
}

/// Position-varying fill so leaf chunks differ across the file.
fn fill(len: usize) -> Vec<u8> {
    (0..len as u64)
        .map(|i| (i.wrapping_mul(2_654_435_761) >> 11) as u8)
        .collect()
}

/// Current-thread runtime with the timer driver the latency charges need.
fn runtime() -> Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap()
}

/// Stream `data` through a plain split into `store`, returning the root.
async fn split_into<const B: usize>(
    store: LatencyStore<B>,
    data: &[u8],
    window: PutWindow,
) -> ChunkAddress {
    Split::<LatencyStore<B>, Plain, B>::collect_with(store, window, data)
        .await
        .unwrap()
}

/// Split `data` into a fresh zero-latency store, returning the root.
fn seed<const B: usize>(rt: &Runtime, data: &[u8]) -> (ChunkAddress, LatencyStore<B>) {
    let store = LatencyStore::new(Duration::ZERO);
    let root = rt.block_on(split_into(store.clone(), data, PutWindow::DEFAULT));
    (root, store)
}

/// Slow-group tuning: flat sampling with few samples, so the serial
/// baseline's multi-second iterations stay runnable.
fn tune(group: &mut BenchmarkGroup<'_, WallTime>, latency: Duration) {
    if !latency.is_zero() {
        group.sampling_mode(SamplingMode::Flat);
        group.sample_size(10);
        group.warm_up_time(Duration::from_secs(1));
        group.measurement_time(Duration::from_secs(5));
    }
}

/// Time an ordered drain of `leaves` full bodies at each fetch window; each
/// iteration opens the file cold, so the root fetch is charged too.
fn read_group<const B: usize>(c: &mut Criterion, name: &str, latency: Duration, leaves: usize) {
    let rt = runtime();
    let len = leaves * B;
    let data = fill(len);
    let (root, seeded) = seed::<B>(&rt, &data);
    let store = seeded.with_latency(latency);
    let mut group = c.benchmark_group(name);
    group.throughput(Throughput::Bytes(len as u64));
    tune(&mut group, latency);
    for &window in WINDOWS {
        group.bench_with_input(BenchmarkId::from_parameter(window), &window, |b, &w| {
            b.iter(|| {
                rt.block_on(async {
                    let file: File<LatencyStore<B>, Plain, B> =
                        File::open(store.clone(), root).await.unwrap();
                    let mut reader = file.read().window(Window::new(w).unwrap()).build();
                    let mut total = 0usize;
                    while let Some(segment) = reader.next_segment().await {
                        total += segment.unwrap().len();
                    }
                    assert_eq!(total, len);
                    black_box(total)
                })
            });
        });
    }
    group.finish();
}

/// Time a streamed split of `leaves` full bodies at each put window, into a
/// fresh store per iteration.
fn split_group<const B: usize>(c: &mut Criterion, name: &str, latency: Duration, leaves: usize) {
    let rt = runtime();
    let len = leaves * B;
    let data = fill(len);
    let mut group = c.benchmark_group(name);
    group.throughput(Throughput::Bytes(len as u64));
    tune(&mut group, latency);
    for &window in WINDOWS {
        group.bench_with_input(BenchmarkId::from_parameter(window), &window, |b, &w| {
            b.iter_batched(
                || LatencyStore::<B>::new(latency),
                |store| {
                    rt.block_on(async {
                        black_box(split_into(store, &data, PutWindow::new(w).unwrap()).await)
                    })
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn read_0ms(c: &mut Criterion) {
    read_group::<DEFAULT_BODY_SIZE>(c, "read_drain_0ms", Duration::ZERO, 256);
}

fn read_10ms(c: &mut Criterion) {
    read_group::<DEFAULT_BODY_SIZE>(c, "read_drain_10ms", Duration::from_millis(10), 64);
}

fn read_100ms(c: &mut Criterion) {
    read_group::<DEFAULT_BODY_SIZE>(c, "read_drain_100ms", Duration::from_millis(100), 16);
}

fn read_deep_10ms(c: &mut Criterion) {
    read_group::<DEEP_BODY>(c, "read_drain_deep_10ms", Duration::from_millis(10), 128);
}

fn split_0ms(c: &mut Criterion) {
    split_group::<DEFAULT_BODY_SIZE>(c, "split_stream_0ms", Duration::ZERO, 256);
}

fn split_10ms(c: &mut Criterion) {
    split_group::<DEFAULT_BODY_SIZE>(c, "split_stream_10ms", Duration::from_millis(10), 64);
}

fn split_100ms(c: &mut Criterion) {
    split_group::<DEFAULT_BODY_SIZE>(c, "split_stream_100ms", Duration::from_millis(100), 16);
}

criterion_group!(
    benches,
    read_0ms,
    read_10ms,
    read_100ms,
    read_deep_10ms,
    split_0ms,
    split_10ms,
    split_100ms
);
criterion_main!(benches);
