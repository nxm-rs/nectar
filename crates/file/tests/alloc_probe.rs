//! Allocation probe over the real encrypted read path.
//!
//! Splits a file through the encrypted splitter into a `MemoryStore`, then
//! reads it back through `File::open_encrypted` and `collect` under a
//! counting allocator. Fetched bodies are shared offset sub-views, so the
//! walk's staging buffer is the only place plaintext can land: body-sized
//! allocator calls during the read must stay a chunk-count-independent
//! constant plus the output buffer, never one per chunk.
#![cfg(feature = "encryption")]
// Integration-test code: unwraps, direct indexing, casts, and assertions
// are setup and illustration, not shipped surface.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::panic,
    clippy::as_conversions,
    clippy::missing_panics_doc
)]

use core::sync::atomic::{AtomicU64, Ordering};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Arc;

use futures::executor::block_on;
use nectar_file::{Encrypted, File, RandomKeys, Split};
use nectar_primitives::chunk::AnyChunkSet;
use nectar_primitives::chunk::encryption::EncryptedChunkRef;
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

/// Stream `data` through an encrypted split into a fresh memory store.
fn split_encrypted(data: &[u8]) -> (EncryptedChunkRef, Store) {
    let store: Store = Arc::new(MemoryStore::new());
    let root =
        block_on(Split::<Store, Encrypted<RandomKeys>, BODY>::collect(Arc::clone(&store), data))
            .unwrap();
    (root, store)
}

/// Full read of `leaves` body-sized leaves; returns (calls, body-sized
/// calls) the open-plus-collect made.
fn probe(leaves: usize) -> (u64, u64) {
    let data = fill(leaves * BODY);
    let (root, store) = split_encrypted(&data);

    let calls = CALLS.load(Ordering::Relaxed);
    let body_calls = BODY_CALLS.load(Ordering::Relaxed);
    let out = block_on(async {
        let file: File<Store, Encrypted, BODY> = File::open_encrypted(store, root).await.unwrap();
        file.collect(u64::MAX).await.unwrap()
    });
    let calls = CALLS.load(Ordering::Relaxed) - calls;
    let body_calls = BODY_CALLS.load(Ordering::Relaxed) - body_calls;

    assert_eq!(out, data, "plaintext diverged at {leaves} leaves");
    (calls, body_calls)
}

#[test]
fn encrypted_read_body_allocations_stay_constant() {
    // Encrypted fan-out 64: 32 leaves sit under one root; 128 leaves add
    // two intermediates.
    let (small_calls, small_body) = probe(32);
    let (large_calls, large_body) = probe(128);
    println!("32 leaves (33 chunks): {small_calls} calls, {small_body} body-sized");
    println!("128 leaves (131 chunks): {large_calls} calls, {large_body} body-sized");

    // Output buffer plus a bounded staging remainder, independent of the
    // chunk count; one per chunk would be 33 and 131.
    assert!(
        small_body <= 16,
        "32-leaf read made {small_body} body-sized allocations"
    );
    assert!(
        large_body <= 16,
        "128-leaf read made {large_body} body-sized allocations"
    );
}
