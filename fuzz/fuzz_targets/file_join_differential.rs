//! Fuzz the streaming read facade for split-then-read consistency.
//!
//! One store is filled by the streaming split over fuzzed bytes; the oracle
//! is byte equality between the whole-file collect, a buffered reader
//! drain, and the source bytes. A small body size keeps multi-level trees
//! reachable from short fuzz inputs.

#![no_main]

use core::future::poll_fn;
use std::sync::Arc;

use futures::executor::block_on;
use libfuzzer_sys::fuzz_target;
use nectar_file::{File, Plain, PutWindow, Split};
use nectar_primitives::chunk::{AnyChunkSet, ChunkAddress};
use nectar_primitives::store::MemoryStore;

/// Tiny body size: fan-out 8, so a few KiB already builds a deep tree.
const BODY: usize = 256;
/// Source-length cap; four tree levels at the tiny body size.
const MAX_LEN: usize = 32 * 1024;

/// Tile `seed` to `copies` repetitions, capped at [`MAX_LEN`] bytes.
fn tile(seed: &[u8], copies: u16) -> Vec<u8> {
    if seed.is_empty() {
        return Vec::new();
    }
    let len = seed
        .len()
        .saturating_mul(usize::from(copies.max(1)))
        .min(MAX_LEN);
    seed.iter().copied().cycle().take(len).collect()
}

/// Whole-buffer streaming split into a shared store.
fn split(data: &[u8]) -> (ChunkAddress, Arc<MemoryStore<AnyChunkSet<BODY>>>) {
    let store = Arc::new(MemoryStore::new());
    let mut split: Split<Arc<MemoryStore<AnyChunkSet<BODY>>>, Plain, BODY> =
        Split::new(Arc::clone(&store), PutWindow::DEFAULT);
    let root = block_on(async {
        let mut buf = data;
        while !buf.is_empty() {
            let n = poll_fn(|cx| split.poll_write(cx, buf))
                .await
                .expect("memory puts never fail");
            buf = &buf[n..];
        }
        poll_fn(|cx| split.poll_finish(cx))
            .await
            .expect("finish over a memory store succeeds")
    });
    (root, store)
}

fuzz_target!(|input: (Vec<u8>, u16)| {
    let (seed, copies) = input;
    let data = tile(&seed, copies);

    let (root, store) = split(&data);

    let collected = block_on(async {
        let file = File::<_, Plain, BODY>::open(Arc::clone(&store), root)
            .await
            .expect("open must succeed over a complete store");
        file.collect(u64::MAX)
            .await
            .expect("collect must succeed over a complete store")
    });
    assert_eq!(collected, data, "collect diverged from the source bytes");

    // The buffered reader drains the same bytes through its own path.
    let drained = block_on(async {
        let file = File::<_, Plain, BODY>::open(store, root)
            .await
            .expect("reopen must succeed over a complete store");
        let mut reader = file.read().build();
        let mut out = Vec::new();
        let mut buf = [0u8; 97];
        loop {
            let n = reader
                .read(&mut buf)
                .await
                .expect("read must succeed over a complete store");
            if n == 0 {
                break;
            }
            out.extend_from_slice(&buf[..n]);
        }
        out
    });
    assert_eq!(drained, data, "reader drain diverged from the collect");
});
