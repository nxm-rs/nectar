//! Instrumented in-memory chunk store for the pinned 0.2 baseline.
//!
//! Mirrors [`crate::store::CountingStore`] over the registry-pinned legacy
//! crate's store traits, so both formats report the same [`Counters`] shape
//! from 4096-byte content-chunk bodies.

use std::sync::atomic::{AtomicU64, Ordering::SeqCst};

use primitives_old::chunk::{AnyChunk, ChunkAddress};
use primitives_old::store::{ChunkGet, ChunkHas, ChunkPut, MemoryStore};

use crate::store::Counters;

const BODY: usize = 4096;

/// In-memory chunk store for the legacy manifest that counts gets, puts and
/// byte residency.
#[derive(Debug, Default)]
pub struct OldCountingStore {
    inner: MemoryStore<BODY>,
    gets: AtomicU64,
    puts: AtomicU64,
    put_bytes: AtomicU64,
    live_bytes: AtomicU64,
    peak_live_bytes: AtomicU64,
    distinct_puts: AtomicU64,
}

impl OldCountingStore {
    /// An empty counting store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Read every counter and the resident chunk count.
    #[must_use]
    pub fn snapshot(&self) -> Counters {
        Counters {
            gets: self.gets.load(SeqCst),
            puts: self.puts.load(SeqCst),
            put_bytes: self.put_bytes.load(SeqCst),
            live_bytes: self.live_bytes.load(SeqCst),
            peak_live_bytes: self.peak_live_bytes.load(SeqCst),
            distinct_puts: self.distinct_puts.load(SeqCst),
            total_chunks: self.inner.len() as u64,
        }
    }

    /// Distinct resident addresses.
    #[must_use]
    pub fn total_chunks(&self) -> usize {
        self.inner.len()
    }

    /// Gets observed so far.
    #[must_use]
    pub fn gets(&self) -> u64 {
        self.gets.load(SeqCst)
    }

    /// Puts observed so far.
    #[must_use]
    pub fn puts(&self) -> u64 {
        self.puts.load(SeqCst)
    }
}

impl ChunkPut<BODY> for OldCountingStore {
    type Error = std::convert::Infallible;

    async fn put(&self, chunk: AnyChunk<BODY>) -> Result<(), Self::Error> {
        let n = chunk.data().len() as u64;
        let is_new = !ChunkHas::has(&self.inner, chunk.address()).await;
        self.puts.fetch_add(1, SeqCst);
        self.put_bytes.fetch_add(n, SeqCst);
        if is_new {
            self.distinct_puts.fetch_add(1, SeqCst);
            let lb = self.live_bytes.fetch_add(n, SeqCst) + n;
            self.peak_live_bytes.fetch_max(lb, SeqCst);
        }
        ChunkPut::put(&self.inner, chunk).await
    }
}

impl ChunkGet<BODY> for OldCountingStore {
    type Error = <MemoryStore<BODY> as ChunkGet<BODY>>::Error;

    async fn get(&self, address: &ChunkAddress) -> Result<AnyChunk<BODY>, Self::Error> {
        self.gets.fetch_add(1, SeqCst);
        ChunkGet::get(&self.inner, address).await
    }
}

impl ChunkHas<BODY> for OldCountingStore {
    async fn has(&self, address: &ChunkAddress) -> bool {
        ChunkHas::has(&self.inner, address).await
    }
}
