//! Allocation probe over the real plain read path.
//!
//! Splits a file through the plain splitter into a `MemoryStore`, then reads
//! it back through `File::open` under a counting allocator. Plain bodies pass
//! through undecoded, so no body-sized allocation lands per fetched node, and
//! the walk holds outstanding fetches in a reusable in-flight set, so the
//! per-fetch allocation count stays below the boxed-future-plus-task-node
//! pair that a `FuturesUnordered` charged: one boxed store future per fetch
//! plus a chunk-independent remainder, never two.
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

use core::future::poll_fn;
use core::sync::atomic::{AtomicU64, Ordering};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Arc;

use futures::executor::block_on;
use nectar_file::{File, Plain, PutWindow, Split};
use nectar_primitives::chunk::{AnyChunkSet, ChunkAddress};
use nectar_primitives::store::MemoryStore;

/// Body size of the default profile.
const BODY: usize = nectar_primitives::DEFAULT_BODY_SIZE;

/// Allocator calls observed so far.
static CALLS: AtomicU64 = AtomicU64::new(0);
/// Allocator calls of at least one body size.
static BODY_CALLS: AtomicU64 = AtomicU64::new(0);

fn note(size: usize) {
    CALLS.fetch_add(1, Ordering::Relaxed);
    if size >= BODY {
        BODY_CALLS.fetch_add(1, Ordering::Relaxed);
    }
}

/// System allocator wrapper counting calls by size class.
struct Counting;

// SAFETY: delegates to `System` unchanged; the counters are side effects.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        note(layout.size());
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        note(new_size);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

type Store = Arc<MemoryStore<AnyChunkSet<BODY>>>;

/// Distinct byte per position so every chunk address is unique.
fn fill(len: usize) -> Vec<u8> {
    (0..len as u64)
        .map(|i| ((i.wrapping_mul(2_654_435_761) >> 11) & 0xff) as u8)
        .collect()
}

/// Stream `data` through a plain split into a fresh memory store.
fn split_plain(data: &[u8]) -> (ChunkAddress, Store) {
    let store: Store = Arc::new(MemoryStore::new());
    let mut split: Split<Store, Plain, BODY> = Split::new(Arc::clone(&store), PutWindow::DEFAULT);
    let root = block_on(async {
        let mut buf = data;
        while !buf.is_empty() {
            let n = poll_fn(|cx| split.poll_write(cx, buf)).await.unwrap();
            buf = &buf[n..];
        }
        poll_fn(|cx| split.poll_finish(cx)).await.unwrap()
    });
    (root, store)
}

/// Full read of `leaves` body-sized leaves; returns the calls, body-sized
/// calls, and fetches the open-plus-drain made.
fn probe(leaves: usize) -> (u64, u64, u64) {
    let data = fill(leaves * BODY);
    let (root, store) = split_plain(&data);

    let calls = CALLS.load(Ordering::Relaxed);
    let body_calls = BODY_CALLS.load(Ordering::Relaxed);
    let (read, fetches) = block_on(async {
        let file: File<Store, Plain, BODY> = File::open(store, root).await.unwrap();
        let mut reader = file.read().build();
        let mut read = 0usize;
        while let Some(segment) = reader.next_segment().await {
            read += segment.unwrap().len();
        }
        (read, reader.stats().fetches)
    });
    let calls = CALLS.load(Ordering::Relaxed) - calls;
    let body_calls = BODY_CALLS.load(Ordering::Relaxed) - body_calls;

    assert_eq!(read, data.len(), "plaintext short at {leaves} leaves");
    (calls, body_calls, fetches)
}

#[test]
fn plain_read_allocations_stay_below_two_per_fetch() {
    // Plain fan-out 128: 128 leaves sit under one root; 512 leaves add four
    // intermediates, so the fetch count scales while the peaks do not.
    let (small_calls, small_body, small_fetches) = probe(128);
    let (large_calls, large_body, large_fetches) = probe(512);
    println!("128 leaves: {small_calls} calls, {small_body} body-sized, {small_fetches} fetches");
    println!("512 leaves: {large_calls} calls, {large_body} body-sized, {large_fetches} fetches");

    // Plain bodies pass through undecoded, so the only body-sized allocations
    // are the reader's own staging, independent of the chunk count; one body
    // per fetch would be 129 and 517.
    assert!(
        small_body <= 16,
        "128-leaf read made {small_body} body-sized allocations"
    );
    assert!(
        large_body <= 16,
        "512-leaf read made {large_body} body-sized allocations"
    );

    // The reusable in-flight set holds one boxed store future per fetch and
    // no per-push task node, so total allocations stay below two per fetch; a
    // `FuturesUnordered` charged two (box plus `Arc<Task>`).
    assert!(
        large_calls * 2 < large_fetches * 3,
        "512-leaf read made {large_calls} allocations over {large_fetches} fetches, at or above 1.5 per fetch"
    );
    assert!(
        small_calls * 2 < small_fetches * 3,
        "128-leaf read made {small_calls} allocations over {small_fetches} fetches, at or above 1.5 per fetch"
    );
}
