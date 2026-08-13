//! The simulator's own RTT model over an already-populated store.

use std::sync::atomic::{AtomicU64, Ordering::SeqCst};
use std::time::Duration;

use nectar_primitives::chunk::{Chunk, ChunkAddress, ChunkRegistry, StandardChunkSet, Verified};
use nectar_primitives::store::{ChunkGet, ChunkHas, MemoryStore};

/// A read-only store that models one network round trip per node fetch: every
/// `get` awaits `rtt`, then serves the chunk from an already-populated backing
/// store, counting the fetch.
///
/// Driven under a paused virtual clock, the ordered cursor's bounded-concurrency
/// read-ahead makes independent fetches share a deadline, so they fire in one
/// clock advance: the elapsed virtual time is exactly `rounds * rtt`, read off
/// the real cursor rather than derived. The fetch count is unchanged from a
/// serial walk; only the wall-clock differs.
#[derive(Debug)]
pub struct LatencyStore<'a, R: ChunkRegistry = StandardChunkSet> {
    inner: &'a MemoryStore<R>,
    rtt: Duration,
    gets: AtomicU64,
}

impl<'a, R: ChunkRegistry> LatencyStore<'a, R> {
    /// Wrap a populated store, charging `rtt` of virtual latency per fetch.
    #[must_use]
    pub const fn new(inner: &'a MemoryStore<R>, rtt: Duration) -> Self {
        Self {
            inner,
            rtt,
            gets: AtomicU64::new(0),
        }
    }

    /// Fetches served so far.
    #[must_use]
    pub fn gets(&self) -> u64 {
        self.gets.load(SeqCst)
    }
}

impl<R: ChunkRegistry> ChunkGet<R> for LatencyStore<'_, R> {
    type Trust = Verified;
    type Error = <MemoryStore<R> as ChunkGet<R>>::Error;

    async fn get(&self, address: &ChunkAddress) -> Result<Chunk<Verified, R>, Self::Error> {
        self.gets.fetch_add(1, SeqCst);
        tokio::time::sleep(self.rtt).await;
        ChunkGet::get(self.inner, address).await
    }
}

impl<R: ChunkRegistry> ChunkHas for LatencyStore<'_, R> {
    async fn has(&self, address: &ChunkAddress) -> bool {
        ChunkHas::has(self.inner, address).await
    }
}
