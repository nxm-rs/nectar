//! Allocation probe over the real plain read path.
//!
//! Splits a file through the plain splitter into a `MemoryStore`, then reads
//! it back through `File::open` under the allocation witness. Plain bodies
//! pass through undecoded, so the marginal bytes per added fetch stay below
//! a quarter body, and the walk holds outstanding fetches in a reusable
//! in-flight set, so the per-fetch allocation count stays below the
//! boxed-future-plus-task-node pair that a `FuturesUnordered` charged: one
//! boxed store future per fetch plus a chunk-independent remainder, never
//! two.
// Integration-test code: unwraps, direct indexing, casts, and assertions are
// setup and illustration, not shipped surface.
use core::future::poll_fn;
use std::sync::Arc;

use nectar_file::{File, Plain, PutWindow, Split};
use nectar_primitives::chunk::{AnyChunkSet, ChunkAddress};
use nectar_primitives::store::MemoryStore;
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

/// Stream `data` through a plain split into a fresh memory store.
fn split_plain(data: &[u8]) -> (ChunkAddress, Store) {
    let store: Store = Arc::new(MemoryStore::new());
    let mut split: Split<Store, Plain, BODY> = Split::new(Arc::clone(&store), PutWindow::DEFAULT);
    let root = run(async {
        let mut buf = data;
        while !buf.is_empty() {
            let n = poll_fn(|cx| split.poll_write(cx, buf)).await.unwrap();
            buf = &buf[n..];
        }
        poll_fn(|cx| split.poll_finish(cx)).await.unwrap()
    });
    (root, store)
}

/// Full read of `leaves` body-sized leaves; returns the witness stats and
/// the fetches the open-plus-drain made.
fn probe(leaves: usize) -> (AllocationInfo, u64) {
    let data = fill(leaves * BODY);
    let (root, store) = split_plain(&data);

    let ((read, fetches), info) = measure_allocations(|| {
        run(async {
            let file: File<Store, Plain, BODY> = File::open(store, root).await.unwrap();
            let mut reader = file.read().build();
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
fn plain_read_allocations_stay_below_two_per_fetch() {
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

    // The reusable in-flight set holds one boxed store future per fetch and
    // no per-push task node, so total allocations stay below two per fetch; a
    // `FuturesUnordered` charged two (box plus `Arc<Task>`).
    assert!(
        large.count_total * 2 < large_fetches * 3,
        "512-leaf read made {} allocations over {large_fetches} fetches, at or above 1.5 per fetch",
        large.count_total
    );
    assert!(
        small.count_total * 2 < small_fetches * 3,
        "128-leaf read made {} allocations over {small_fetches} fetches, at or above 1.5 per fetch",
        small.count_total
    );
}
