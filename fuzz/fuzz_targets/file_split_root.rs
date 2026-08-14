//! Fuzz the split engine's root idempotence over write segmentation.
//!
//! The same bytes are streamed through two independent splits under fuzzed
//! segmentations and put windows. The oracle is one root: both runs and
//! every repeated finish must agree, and the written store must read back
//! to the source bytes.

#![no_main]

use core::task::{Context, Poll};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use libfuzzer_sys::fuzz_target;
use nectar_file::sync::drive;
use nectar_file::{File, Policy, PutWindow, Source};
use nectar_fuzz::tile;
use nectar_primitives::chunk::{AnyChunkSet, Chunk, ChunkAddress, Verified};
use nectar_primitives::store::{ChunkGet, ChunkPut, ChunkStoreError, ContentGet};

/// Tiny body size: fan-out 8, so a few KiB already builds a deep tree.
const BODY: usize = 256;

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

impl ChunkPut<Chunk<Verified, AnyChunkSet<BODY>>> for SharedStore {
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

/// Byte source handing out fuzzed-size pieces, so one save meets an
/// arbitrary pull segmentation.
struct Segmented<'a> {
    data: &'a [u8],
    steps: &'a [u16],
    index: usize,
}

impl Source for Segmented<'_> {
    type Error = core::convert::Infallible;

    fn poll_fill(
        &mut self,
        _cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, Self::Error>> {
        let step = usize::from(
            self.steps
                .get(self.index % self.steps.len().max(1))
                .copied()
                .unwrap_or(97),
        ) % 719
            + 1;
        self.index += 1;
        let take = step.min(buf.len()).min(self.data.len());
        buf[..take].copy_from_slice(&self.data[..take]);
        self.data = &self.data[take..];
        Poll::Ready(Ok(take))
    }
}

/// Stream `data` through a fresh save in fuzzed pull segments, returning
/// the root and the written store.
fn stream_split(data: &[u8], window: u16, steps: &[u16]) -> (ChunkAddress, SharedStore) {
    let store = SharedStore::default();
    let window = PutWindow::new((window % 16) + 1).expect("bounded slots are nonzero");
    let file = File::<_, BODY>::new(store.clone(), Policy::DEFAULT.with_put_window(window));
    // A ready store must settle every put inline, so one poll finishes the
    // save; a `Pending` here is a stall finding, not a slow store.
    let root = drive(file.save(Segmented {
        data,
        steps,
        index: 0,
    }))
    .expect("a ready store must never pend")
    .expect("save must succeed over a ready store");
    (root, store)
}

fuzz_target!(|input: (Vec<u8>, u16, Vec<u16>, Vec<u16>, u16, u16)| {
    let (seed, copies, steps_a, steps_b, win_a, win_b) = input;
    let data = tile(&seed, copies);

    let (root_a, store) = stream_split(&data, win_a, &steps_a);
    let (root_b, _) = stream_split(&data, win_b, &steps_b);
    assert_eq!(root_a, root_b, "root diverged across write segmentations");

    let read_back = drive(async move {
        let file = File::<_, BODY>::new(ContentGet::new(store), Policy::DEFAULT);
        file.collect(root_a.into(), u64::MAX)
            .await
            .expect("collect must succeed over the written store")
    })
    .expect("a ready store never pends");
    assert_eq!(
        read_back, data,
        "written tree did not read back to the source"
    );
});
