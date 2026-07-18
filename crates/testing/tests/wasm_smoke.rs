//! Node smoke test proving the wasm relaxation of the store traits: a `!Send`
//! store round-trips a file and the windowed reader honours its read-ahead
//! bound. Native targets compile this file to nothing.
#![cfg(target_arch = "wasm32")]

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;

use futures::StreamExt;
use nectar_primitives::{
    AnyChunkSet, Chunk, ChunkAddress, ChunkGet, ChunkGetExt, ChunkPut, ChunkPutExt,
    ChunkStoreError, DEFAULT_BODY_SIZE, Verified,
};
use wasm_bindgen_test::wasm_bindgen_test;

type SealedChunk = Chunk<Verified, AnyChunkSet<DEFAULT_BODY_SIZE>>;

/// `Rc`/`RefCell` make this store neither `Send` nor `Sync`, so it satisfies
/// the chunk traits only on wasm32.
#[derive(Clone, Default)]
struct RcStore {
    chunks: Rc<RefCell<HashMap<ChunkAddress, SealedChunk>>>,
    in_flight: Rc<Cell<usize>>,
    peak: Rc<Cell<usize>>,
}

impl ChunkPut<AnyChunkSet<DEFAULT_BODY_SIZE>> for RcStore {
    type Error = std::convert::Infallible;

    async fn put(&self, chunk: SealedChunk) -> Result<(), Self::Error> {
        self.chunks.borrow_mut().insert(*chunk.address(), chunk);
        Ok(())
    }
}

impl ChunkGet<AnyChunkSet<DEFAULT_BODY_SIZE>> for RcStore {
    type Trust = Verified;
    type Error = ChunkStoreError;

    /// Yields once mid-fetch so overlapping gets register in `peak`.
    async fn get(&self, address: &ChunkAddress) -> Result<SealedChunk, Self::Error> {
        self.in_flight.set(self.in_flight.get() + 1);
        self.peak.set(self.peak.get().max(self.in_flight.get()));
        nectar_testing::yield_now().await;
        self.in_flight.set(self.in_flight.get() - 1);
        self.chunks
            .borrow()
            .get(address)
            .cloned()
            .ok_or_else(|| ChunkStoreError::not_found(address))
    }
}

/// Five leaves: a two-level tree, so the download has read-ahead to exercise.
fn sample_data() -> Vec<u8> {
    (0..DEFAULT_BODY_SIZE * 4 + 123)
        .map(|i| u8::try_from(i % 251).unwrap())
        .collect()
}

#[wasm_bindgen_test]
async fn non_send_store_round_trips_a_file() {
    let store = RcStore::default();
    let data = sample_data();
    let root = store.write_file(data.clone()).await.unwrap();
    let out = store.clone().read_file(root).await.unwrap();
    assert_eq!(out, data);
}

#[wasm_bindgen_test]
async fn windowed_read_ahead_stays_within_bound() {
    const WINDOW: usize = 2;
    let store = RcStore::default();
    let data = sample_data();
    let root = store.write_file(data.clone()).await.unwrap();

    let mut reader = store
        .clone()
        .joiner(root)
        .await
        .unwrap()
        .into_windowed_reader(WINDOW);
    store.in_flight.set(0);
    store.peak.set(0);

    let mut out = Vec::new();
    let stream = reader.stream();
    futures::pin_mut!(stream);
    while let Some(run) = stream.next().await {
        out.extend_from_slice(&run.unwrap());
    }

    assert_eq!(out, data);
    let peak = store.peak.get();
    assert!(peak <= WINDOW, "read-ahead exceeded the window: {peak}");
    assert!(peak > 1, "leaf fetches never overlapped: {peak}");
    assert_eq!(store.in_flight.get(), 0);
}
