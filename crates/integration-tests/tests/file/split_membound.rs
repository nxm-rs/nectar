//! Peak working-set witness for the split engine, through the public API.
//!
//! The split seals a chunk, threads its 32-byte reference up the spine, and
//! moves the body into the put it dispatches; nothing retains the body past
//! dispatch. So the engine's peak working set is a payload-independent
//! constant, `O(put window + spine depth)` bodies plus the spine of
//! references, never `O(payload)`.
//!
//! The allocation witness records peak live bytes while a drop-store
//! discards each body on put, so the peak is the pure engine working set.
//! The payload scales 16x across three sizes that mirror 8/32/128 MiB at
//! the default body; the peak stays flat.
// Integration-test code: unwraps, direct indexing, casts, and assertions are
// setup and illustration, not shipped surface.
use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use std::sync::{Arc, Mutex};

use nectar_file::{File, Policy, PutWindow, Source};
use nectar_primitives::chunk::{AnyChunkSet, Chunk, Verified};
use nectar_primitives::store::{ChunkPut, ChunkStoreError};
use nectar_testing::{measure_allocations, run};

/// Tiny body: fan-out 8, so a few thousand leaves build a deep tree at a
/// modest byte count.
const B: usize = 256;
/// Put window held for the witness.
const WINDOW: u16 = 8;

/// A put future that parks once before completing, so up to `window` bodies
/// occupy the in-flight set at a time.
#[derive(Default)]
struct YieldOnce {
    polled: bool,
}

impl Future for YieldOnce {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.polled {
            Poll::Ready(())
        } else {
            self.polled = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

/// Drops bodies on put, keeping only the last chunk so the root survives;
/// peak live is then the pure engine working set.
#[derive(Clone)]
struct DropStore {
    root: Arc<Mutex<Option<Chunk<Verified, AnyChunkSet<B>>>>>,
}

impl DropStore {
    fn new() -> Self {
        Self {
            root: Arc::new(Mutex::new(None)),
        }
    }
}

impl ChunkPut<AnyChunkSet<B>> for DropStore {
    type Error = ChunkStoreError;

    async fn put(&self, chunk: Chunk<Verified, AnyChunkSet<B>>) -> Result<(), ChunkStoreError> {
        YieldOnce::default().await;
        *self.root.lock().unwrap() = Some(chunk);
        Ok(())
    }
}

/// Deterministic byte source generated on the fly, so the input never lives
/// as one payload-sized allocation.
struct Splitmix {
    produced: usize,
    total: usize,
}

impl Source for Splitmix {
    type Error = core::convert::Infallible;

    fn poll_fill(
        &mut self,
        _cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, Self::Error>> {
        let take = buf.len().min(self.total - self.produced);
        for (j, slot) in buf[..take].iter_mut().enumerate() {
            // splitmix64 of the absolute byte index: aperiodic, so every body
            // is unique and nothing dedups.
            let i = (self.produced + j) as u64;
            let mut z = i.wrapping_add(0x9E37_79B9_7F4A_7C15);
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^= z >> 31;
            *slot = z as u8;
        }
        self.produced += take;
        Poll::Ready(Ok(take))
    }
}

/// Stream `total` deterministic bytes through a plain save and return the
/// peak live bytes (`bytes_max`) the write added over the witness baseline.
fn split_peak(total: usize) -> u64 {
    let file = File::<DropStore, B>::new(
        DropStore::new(),
        Policy::DEFAULT.with_put_window(PutWindow::new(WINDOW).unwrap()),
    );
    let ((), info) = measure_allocations(|| {
        run(async {
            file.save(Splitmix {
                produced: 0,
                total,
            })
            .await
            .unwrap();
        })
    });
    info.bytes_max
}

#[test]
fn split_working_set_is_flat_in_payload() {
    // Leaf counts 2048 / 8192 / 32768 mirror the tree of 8 / 32 / 128 MiB at
    // the default body; the payload scales 16x.
    let sizes = [2048 * B, 8192 * B, 32768 * B];
    let peaks: Vec<u64> = sizes.iter().map(|&size| split_peak(size)).collect();
    for (size, peak) in sizes.iter().zip(&peaks) {
        println!("{:>4} KiB payload | engine peak {peak} bytes", size / 1024);
    }

    let min = *peaks.iter().min().unwrap();
    let max = *peaks.iter().max().unwrap();

    // Flat: a 16x payload adds only one spine level, so the peak barely
    // moves; O(payload) retention would grow it 16x.
    assert!(
        max < 3 * min,
        "engine peak grew from {min} to {max} bytes as the payload scaled 16x"
    );

    // A payload-independent working set: the peak stays a small fraction of
    // the smallest payload, so no body-proportional buffer is retained.
    let smallest = sizes[0] as u64;
    assert!(
        max < smallest / 16,
        "engine peak {max} bytes is not small against the {smallest}-byte payload"
    );
}
