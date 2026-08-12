//! Allocation probe over the real plain read path.
//!
//! Splits a file through the plain save into a `MemoryStore`, then reads it
//! back through `File::open` under the allocation witness. Plain bodies
//! pass through undecoded, so the marginal bytes per added fetch stay below
//! a quarter body, and each outstanding fetch costs one boxed store future
//! plus its task node in the in-flight set, so the marginal allocation per
//! added fetch holds at two: the staging remainder is chunk-count
//! independent and adds no third.
// Integration-test code: unwraps, direct indexing, casts, and assertions are
// setup and illustration, not shipped surface.
use std::sync::Arc;

use nectar_file::{File, Policy};
use nectar_primitives::chunk::{AnyChunkSet, ChunkAddress};
use nectar_primitives::store::{ContentGet, MemoryStore};
use nectar_testing::{AllocationInfo, measure_allocations, run};

/// Body size of the default profile.
const BODY: usize = nectar_primitives::DEFAULT_BODY_SIZE;

type Store = Arc<MemoryStore<AnyChunkSet<BODY>>>;

/// Distinct byte per position so every chunk address is unique.
fn fill(len: usize) -> Vec<u8> {
    (0..len as u64)
        .map(|i| ((i.wrapping_mul(2_654_435_761) >> 11) & 0xff) as u8)
        .collect()
}

/// Stream `data` through a plain save into a fresh memory store.
fn split_plain(data: &[u8]) -> (ChunkAddress, Store) {
    let store: Store = Arc::new(MemoryStore::new());
    let root =
        run(File::<Store, BODY>::new(Arc::clone(&store), Policy::DEFAULT).save(data)).unwrap();
    (root, store)
}

/// Full read of `leaves` body-sized leaves; returns the witness stats and
/// the fetches the open-plus-drain made.
fn probe(leaves: usize) -> (AllocationInfo, u64) {
    let data = fill(leaves * BODY);
    let (root, store) = split_plain(&data);

    let file: File<ContentGet<Store>, BODY> = File::new(ContentGet::new(store), Policy::DEFAULT);
    let ((read, fetches), info) = measure_allocations(|| {
        run(async {
            let mut reader = file.open(root.into()).await.unwrap();
            let mut read = 0usize;
            while let Some(segment) = reader.next_segment().await {
                read += segment.unwrap().len();
            }
            (read, reader.stats().fetches)
        })
    });

    assert_eq!(read, data.len(), "plaintext short at {leaves} leaves");
    (info, fetches)
}

#[test]
fn plain_read_allocations_hold_at_two_per_added_fetch() {
    // Plain fan-out 128: 128 leaves sit under one root; 512 leaves add four
    // intermediates, so the fetch count scales while the staging does not.
    let (small, small_fetches) = probe(128);
    let (large, large_fetches) = probe(512);
    println!(
        "128 leaves: {} allocations, {} bytes, {small_fetches} fetches",
        small.count_total, small.bytes_total
    );
    println!(
        "512 leaves: {} allocations, {} bytes, {large_fetches} fetches",
        large.count_total, large.bytes_total
    );

    // Plain bodies pass through undecoded and nothing collects the payload,
    // so the reader's chunk-count-independent staging holds the marginal
    // bytes to a few hundred per added fetch; a body-sized allocation even
    // every fourth fetched node would breach the quarter-body slope.
    let byte_delta = large.bytes_total.saturating_sub(small.bytes_total);
    let fetch_delta = large_fetches - small_fetches;
    assert!(
        byte_delta < fetch_delta * (BODY as u64 / 4),
        "read traffic grew {byte_delta} bytes over {fetch_delta} added fetches, at or above a quarter body per fetch"
    );

    // Each added fetch costs one boxed store future plus its task node in the
    // in-flight set, and the staging remainder is chunk-count independent, so
    // the marginal allocation per added fetch holds below three.
    let count_delta = large.count_total.saturating_sub(small.count_total);
    assert!(
        count_delta < fetch_delta * 3,
        "plain read made {count_delta} allocations over {fetch_delta} added fetches, at or above three per fetch"
    );
}
