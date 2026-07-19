//! Counting wrapper over the shared in-memory chunk store.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use nectar_primitives::chunk::{Chunk, ChunkAddress, ChunkOps, StandardChunkSet, Verified};
use nectar_primitives::store::{ChunkGet, ChunkHas, ChunkPut, ChunkStoreError};
use nectar_primitives::MemoryStore;

/// Operation counters, shared across shallow clones.
#[derive(Debug, Default)]
pub struct Counters {
    gets: AtomicU64,
    puts: AtomicU64,
    puts_new: AtomicU64,
    put_bytes: AtomicU64,
}

/// A [`MemoryStore`] that counts gets, puts, first-time puts and put bytes.
///
/// `Clone` is shallow: clones share the chunk map and the counters, as the
/// cursor surfaces require. [`deep_fork`](Self::deep_fork) copies the chunks
/// into a fresh store with zeroed counters, for scenarios that mutate state.
#[derive(Clone, Debug)]
pub struct CountingStore {
    inner: Arc<MemoryStore<StandardChunkSet>>,
    counters: Arc<Counters>,
}

impl Default for CountingStore {
    fn default() -> Self {
        Self::new()
    }
}

impl CountingStore {
    /// An empty store with zeroed counters.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(MemoryStore::new()),
            counters: Arc::new(Counters::default()),
        }
    }

    /// A deep copy of the chunks under fresh counters.
    #[must_use]
    pub fn deep_fork(&self) -> Self {
        Self {
            inner: Arc::new((*self.inner).clone()),
            counters: Arc::new(Counters::default()),
        }
    }

    /// Zero every counter.
    pub fn reset_counters(&self) {
        self.counters.gets.store(0, Ordering::Relaxed);
        self.counters.puts.store(0, Ordering::Relaxed);
        self.counters.puts_new.store(0, Ordering::Relaxed);
        self.counters.put_bytes.store(0, Ordering::Relaxed);
    }

    /// Chunks fetched since the last reset.
    #[must_use]
    pub fn gets(&self) -> u64 {
        self.counters.gets.load(Ordering::Relaxed)
    }

    /// Chunks written since the last reset, repeats included.
    #[must_use]
    pub fn puts(&self) -> u64 {
        self.counters.puts.load(Ordering::Relaxed)
    }

    /// Chunks written whose address was not yet stored.
    #[must_use]
    pub fn puts_new(&self) -> u64 {
        self.counters.puts_new.load(Ordering::Relaxed)
    }

    /// Payload bytes written since the last reset.
    #[must_use]
    pub fn put_bytes(&self) -> u64 {
        self.counters.put_bytes.load(Ordering::Relaxed)
    }

    /// Chunks currently stored.
    #[must_use]
    pub fn chunk_count(&self) -> usize {
        self.inner.len()
    }
}

impl ChunkGet<StandardChunkSet> for CountingStore {
    type Trust = Verified;
    type Error = ChunkStoreError;

    async fn get(
        &self,
        address: &ChunkAddress,
    ) -> Result<Chunk<Verified, StandardChunkSet>, Self::Error> {
        self.counters.gets.fetch_add(1, Ordering::Relaxed);
        <MemoryStore<StandardChunkSet> as ChunkGet<StandardChunkSet>>::get(&self.inner, address)
            .await
    }
}

impl ChunkPut<StandardChunkSet> for CountingStore {
    type Error = core::convert::Infallible;

    async fn put(&self, chunk: Chunk<Verified, StandardChunkSet>) -> Result<(), Self::Error> {
        self.counters.puts.fetch_add(1, Ordering::Relaxed);
        if self.inner.get(chunk.address()).is_none() {
            self.counters.puts_new.fetch_add(1, Ordering::Relaxed);
        }
        let bytes = u64::try_from(chunk.envelope().data().len()).unwrap_or(u64::MAX);
        self.counters.put_bytes.fetch_add(bytes, Ordering::Relaxed);
        <MemoryStore<StandardChunkSet> as ChunkPut<StandardChunkSet>>::put(&self.inner, chunk).await
    }
}

impl ChunkHas for CountingStore {
    async fn has(&self, address: &ChunkAddress) -> bool {
        <MemoryStore<StandardChunkSet> as ChunkHas>::has(&self.inner, address).await
    }
}
