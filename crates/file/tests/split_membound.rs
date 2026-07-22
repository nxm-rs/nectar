//! Peak working-set witness for the split engine, through the public API.
//!
//! The split seals a chunk, threads its 32-byte reference up the spine, and
//! moves the body into the put it dispatches; nothing retains the body past
//! dispatch. So the engine's peak working set is a payload-independent
//! constant, `O(put window + spine depth)` bodies plus the spine of
//! references, never `O(payload)`.
//!
//! A counting global allocator records peak live bytes while a drop-store
//! discards each body on put, so the peak is the pure engine working set. The
//! payload scales 16x across three sizes that mirror 8/32/128 MiB at the
//! default body; the peak stays flat.
// Integration-test code: unwraps, direct indexing, casts, and assertions are
// setup and illustration, not shipped surface.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::panic,
    clippy::as_conversions,
    clippy::missing_panics_doc
)]

use core::future::{Future, poll_fn};
use core::pin::Pin;
use core::sync::atomic::{AtomicUsize, Ordering};
use core::task::{Context, Poll};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::{Arc, Mutex};

use nectar_file::{Plain, PutWindow, Split};
use nectar_primitives::chunk::{AnyChunkSet, Chunk, Verified};
use nectar_primitives::store::{ChunkPut, ChunkStoreError};
use nectar_testing::run;

/// Tiny body: fan-out 8, so a few thousand leaves build a deep tree at a
/// modest byte count.
const B: usize = 256;
/// Put window held for the witness.
const WINDOW: u16 = 8;

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

/// System allocator wrapper tracking live and peak bytes.
struct Counting;

// SAFETY: delegates to `System` unchanged; the counters are side effects.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            let live = LIVE.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            PEAK.fetch_max(live, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// Reset the peak to the current live bytes, so a following measurement nets
/// out the baseline (the reused input block and harness state).
fn reset_peak() {
    PEAK.store(LIVE.load(Ordering::Relaxed), Ordering::Relaxed);
}

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

/// Stream `total` deterministic bytes through a plain split from a small
/// reused block, so the input never lives as one payload-sized allocation,
/// and return the peak live-byte delta the split added over the baseline.
fn split_peak_delta(total: usize) -> usize {
    const BLOCK: usize = 4096;
    let store = DropStore::new();
    let mut split: Split<DropStore, Plain, B> = Split::new(store, PutWindow::new(WINDOW).unwrap());
    let mut block = vec![0u8; BLOCK];
    let baseline = LIVE.load(Ordering::Relaxed);
    reset_peak();
    run(async {
        let mut produced = 0usize;
        while produced < total {
            let take = BLOCK.min(total - produced);
            for (j, slot) in block[..take].iter_mut().enumerate() {
                // splitmix64 of the absolute byte index: aperiodic, so every
                // body is unique and nothing dedups.
                let i = (produced + j) as u64;
                let mut z = i.wrapping_add(0x9E37_79B9_7F4A_7C15);
                z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
                z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
                z ^= z >> 31;
                *slot = z as u8;
            }
            let mut buf = &block[..take];
            while !buf.is_empty() {
                let n = poll_fn(|cx| split.poll_write(cx, buf)).await.unwrap();
                buf = &buf[n..];
            }
            produced += take;
        }
        poll_fn(|cx| split.poll_finish(cx)).await.unwrap();
    });
    PEAK.load(Ordering::Relaxed).saturating_sub(baseline)
}

#[test]
fn split_working_set_is_flat_in_payload() {
    // Leaf counts 2048 / 8192 / 32768 mirror the tree of 8 / 32 / 128 MiB at
    // the default body; the payload scales 16x.
    let sizes = [2048 * B, 8192 * B, 32768 * B];
    let peaks: Vec<usize> = sizes.iter().map(|&size| split_peak_delta(size)).collect();
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
    let smallest = sizes[0];
    assert!(
        max < smallest / 16,
        "engine peak {max} bytes is not small against the {smallest}-byte payload"
    );
}
