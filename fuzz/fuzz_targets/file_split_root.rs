//! Fuzz the split engine's root idempotence over write segmentation.
//!
//! The same bytes are streamed through two independent splits under fuzzed
//! segmentations and put windows. The oracle is one root: both runs and
//! every repeated finish must agree, and the written store must read back
//! to the source bytes.

#![no_main]

use core::task::{Context, Poll, Waker};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use libfuzzer_sys::fuzz_target;
use nectar_file::sync::drive;
use nectar_file::{File, Plain, PutWindow, Split};
use nectar_primitives::chunk::{AnyChunkSet, Chunk, ChunkAddress, Verified};
use nectar_primitives::store::{ChunkGet, ChunkPut, ChunkStoreError};

/// Tiny body size: fan-out 8, so a few KiB already builds a deep tree.
const BODY: usize = 256;
/// Source-length cap; four tree levels at the tiny body size.
const MAX_LEN: usize = 32 * 1024;
/// Poll budget per drive; a ready store must finish well within it.
const SPIN_BOUND: u32 = 1 << 20;

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

/// Shared ready store: clones alias one map, so the engine's per-put clones
/// and the read-back handle see the same chunks.
#[derive(Default)]
struct SharedStore {
    chunks: Arc<Mutex<HashMap<ChunkAddress, Chunk<Verified, AnyChunkSet<BODY>>>>>,
}

impl Clone for SharedStore {
    fn clone(&self) -> Self {
        Self {
            chunks: Arc::clone(&self.chunks),
        }
    }
}

impl ChunkPut<AnyChunkSet<BODY>> for SharedStore {
    type Error = ChunkStoreError;

    async fn put(&self, chunk: Chunk<Verified, AnyChunkSet<BODY>>) -> Result<(), Self::Error> {
        self.chunks
            .lock()
            .expect("store mutex is never poisoned")
            .insert(*chunk.address(), chunk);
        Ok(())
    }
}

impl ChunkGet<AnyChunkSet<BODY>> for SharedStore {
    type Trust = Verified;
    type Error = ChunkStoreError;

    async fn get(
        &self,
        address: &ChunkAddress,
    ) -> Result<Chunk<Verified, AnyChunkSet<BODY>>, Self::Error> {
        self.chunks
            .lock()
            .expect("store mutex is never poisoned")
            .get(address)
            .cloned()
            .ok_or_else(|| ChunkStoreError::not_found(address))
    }
}

/// Poll to completion under a no-op waker; a ready store re-polls to
/// progress, so exhausting the budget is a stall finding.
fn drive_poll<T>(mut poll: impl FnMut(&mut Context<'_>) -> Poll<T>) -> T {
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..SPIN_BOUND {
        if let Poll::Ready(value) = poll(&mut cx) {
            return value;
        }
    }
    panic!("split stalled under a ready store");
}

/// Stream `data` through a fresh split in fuzzed write segments, returning
/// the root and the written store.
fn stream_split(data: &[u8], window: u16, steps: &[u16]) -> (ChunkAddress, SharedStore) {
    let store = SharedStore::default();
    let window = PutWindow::new((window % 16) + 1).expect("bounded slots are nonzero");
    let mut split: Split<_, Plain, BODY> = Split::new(store.clone(), window);
    let mut rest = data;
    let mut index = 0usize;
    while !rest.is_empty() {
        let step =
            usize::from(steps.get(index % steps.len().max(1)).copied().unwrap_or(97)) % 719 + 1;
        index += 1;
        let (mut piece, tail) = rest.split_at(step.min(rest.len()));
        rest = tail;
        while !piece.is_empty() {
            let n = drive_poll(|cx| split.poll_write(cx, piece))
                .expect("write must succeed over a ready store");
            assert!(n > 0, "write made no progress on a non-empty buffer");
            piece = &piece[n..];
        }
    }
    let root = drive_poll(|cx| split.poll_finish(cx)).expect("finish must succeed");
    let again = drive_poll(|cx| split.poll_finish(cx)).expect("finish must stay fused");
    assert_eq!(again, root, "a repeated finish delivered a different root");
    (root, store)
}

fuzz_target!(|input: (Vec<u8>, u16, Vec<u16>, Vec<u16>, u16, u16)| {
    let (seed, copies, steps_a, steps_b, win_a, win_b) = input;
    let data = tile(&seed, copies);

    let (root_a, store) = stream_split(&data, win_a, &steps_a);
    let (root_b, _) = stream_split(&data, win_b, &steps_b);
    assert_eq!(root_a, root_b, "root diverged across write segmentations");

    let read_back = drive(async move {
        let file = File::<_, Plain, BODY>::open(store, root_a)
            .await
            .expect("open must succeed over the written store");
        file.collect(u64::MAX)
            .await
            .expect("collect must succeed over the written store")
    })
    .expect("a ready store never pends");
    assert_eq!(
        read_back, data,
        "written tree did not read back to the source"
    );
});
