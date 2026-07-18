//! Shared instrumented in-memory chunk store.
//!
//! One wrapper type satisfies both formats: it is generic over the chunk
//! registry, so `CountingStore<StandardChunkSet>` backs mantaray 1.0 and
//! `CountingStore<AnyChunkSet<4096>>` backs mantaray 0.2. Both hold 4096-byte
//! content-chunk bodies, so every byte and chunk metric is directly
//! comparable across formats.
//!
//! Counters are atomic so the store stays `Sync` (satisfies `MaybeSync`) and
//! a caller snapshots gets/puts around a single operation to read off hops and
//! chunks-rewritten by difference.

use std::sync::atomic::{AtomicU64, Ordering::SeqCst};
use std::time::Duration;

use nectar_primitives::ChunkOps;
use nectar_primitives::chunk::{Chunk, ChunkAddress, ChunkRegistry, StandardChunkSet, Verified};
use nectar_primitives::store::{ChunkGet, ChunkHas, ChunkPut, MemoryStore};

/// A point-in-time read of every counter plus the resident chunk count.
#[derive(Clone, Copy, Debug, Default)]
pub struct Counters {
    /// Total `get()` calls; the delta around one operation is that operation's
    /// hop count.
    pub gets: u64,
    /// Total `put()` calls, counting rewrites of an already-resident address.
    pub puts: u64,
    /// Sum of payload bytes over every `put()`.
    pub put_bytes: u64,
    /// Payload bytes currently resident (grows only on a not-yet-present
    /// address).
    pub live_bytes: u64,
    /// Peak of `live_bytes` observed so far.
    pub peak_live_bytes: u64,
    /// Puts of an address that was not already resident.
    pub distinct_puts: u64,
    /// Distinct resident addresses (`inner.len()`).
    pub total_chunks: u64,
}

/// In-memory chunk store that counts gets, puts and byte residency.
#[derive(Debug)]
pub struct CountingStore<R: ChunkRegistry = StandardChunkSet> {
    inner: MemoryStore<R>,
    gets: AtomicU64,
    puts: AtomicU64,
    put_bytes: AtomicU64,
    live_bytes: AtomicU64,
    peak_live_bytes: AtomicU64,
    distinct_puts: AtomicU64,
}

impl<R: ChunkRegistry> Default for CountingStore<R> {
    fn default() -> Self {
        Self::new()
    }
}

impl<R: ChunkRegistry> CountingStore<R> {
    /// An empty counting store.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: MemoryStore::new(),
            gets: AtomicU64::new(0),
            puts: AtomicU64::new(0),
            put_bytes: AtomicU64::new(0),
            live_bytes: AtomicU64::new(0),
            peak_live_bytes: AtomicU64::new(0),
            distinct_puts: AtomicU64::new(0),
        }
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

    /// Zero the flow counters (gets/puts/put_bytes) while keeping stored chunks
    /// and residency figures.
    pub fn reset_flow(&self) {
        self.gets.store(0, SeqCst);
        self.puts.store(0, SeqCst);
        self.put_bytes.store(0, SeqCst);
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

impl<R: ChunkRegistry> ChunkPut<R> for CountingStore<R> {
    type Error = std::convert::Infallible;

    async fn put(&self, chunk: Chunk<Verified, R>) -> Result<(), Self::Error> {
        let n = chunk.envelope().data().len() as u64;
        let addr = *chunk.address();
        let is_new = !ChunkHas::has(&self.inner, &addr).await;
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

impl<R: ChunkRegistry> ChunkGet<R> for CountingStore<R> {
    type Trust = Verified;
    type Error = <MemoryStore<R> as ChunkGet<R>>::Error;

    async fn get(&self, address: &ChunkAddress) -> Result<Chunk<Verified, R>, Self::Error> {
        self.gets.fetch_add(1, SeqCst);
        ChunkGet::get(&self.inner, address).await
    }
}

impl<R: ChunkRegistry> ChunkHas for CountingStore<R> {
    async fn has(&self, address: &ChunkAddress) -> bool {
        ChunkHas::has(&self.inner, address).await
    }
}

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
