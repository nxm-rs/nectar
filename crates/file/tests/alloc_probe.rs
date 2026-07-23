//! Allocation probe over the real encrypted read path.
//!
//! Splits a file through the encrypted splitter into a `MemoryStore`, then
//! reads it back through `File::open_encrypted` and `collect` under the
//! allocation witness. Fetched bodies are shared offset sub-views, so the
//! walk's staging buffer is the only place plaintext can land: `collect`
//! reserves the output exactly, so allocated bytes beyond the output must
//! grow by less than a quarter body per added chunk, never by a per-chunk
//! plaintext staging buffer.
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

use std::sync::Arc;

use nectar_file::{Encrypted, File, RandomKeys};
use nectar_primitives::chunk::AnyChunkSet;
use nectar_primitives::store::MemoryStore;
use nectar_testing::{AllocationInfo, measure_allocations, run, split_into};

/// Body size of the default profile.
const BODY: usize = nectar_primitives::DEFAULT_BODY_SIZE;

type Store = Arc<MemoryStore<AnyChunkSet<BODY>>>;

/// Distinct byte per position so every chunk address is unique.
fn fill(len: usize) -> Vec<u8> {
    (0..len as u64)
        .map(|i| ((i.wrapping_mul(2_654_435_761) >> 11) & 0xff) as u8)
        .collect()
}

/// Full read of `leaves` body-sized leaves; returns the witness stats of the
/// open-plus-collect.
fn probe(leaves: usize) -> AllocationInfo {
    let data = fill(leaves * BODY);
    let (root, store) = split_into::<Encrypted<RandomKeys>, BODY>(&data);
    let store: Store = Arc::new(store);

    let (out, info) = measure_allocations(|| {
        run(async {
            let file: File<Store, Encrypted, BODY> =
                File::open_encrypted(store, root).await.unwrap();
            file.collect(u64::MAX).await.unwrap()
        })
    });

    assert_eq!(out, data, "plaintext diverged at {leaves} leaves");
    info
}

#[test]
fn encrypted_read_body_allocations_stay_constant() {
    // Encrypted fan-out 64: 32 leaves sit under one root (33 chunks); 128
    // leaves add two intermediates (131 chunks).
    const SMALL: usize = 32;
    const LARGE: usize = 128;
    const CHUNK_DELTA: u64 = 131 - 33;
    let small = probe(SMALL);
    let large = probe(LARGE);
    println!(
        "32 leaves (33 chunks): {} allocations, {} bytes",
        small.count_total, small.bytes_total
    );
    println!(
        "128 leaves (131 chunks): {} allocations, {} bytes",
        large.count_total, large.bytes_total
    );

    // `collect` reserves the output exactly once at the payload size, so the
    // bytes beyond the output are the walk's own traffic. Bounded staging
    // holds that remainder's growth to a few hundred bytes per added chunk;
    // a plaintext staging buffer even every fourth chunk would breach the
    // quarter-body slope.
    let small_extra = small.bytes_total.saturating_sub((SMALL * BODY) as u64);
    let large_extra = large.bytes_total.saturating_sub((LARGE * BODY) as u64);
    let extra_delta = large_extra.saturating_sub(small_extra);
    assert!(
        extra_delta < CHUNK_DELTA * (BODY as u64 / 4),
        "read traffic beyond the output grew {extra_delta} bytes over {CHUNK_DELTA} added chunks, at or above a quarter body per chunk"
    );
}
