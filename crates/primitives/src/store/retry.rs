//! `RetryingChunkGet`: a wasm-safe [`ChunkGet`] decorator that absorbs
//! transient retrieval failures with bounded exponential backoff.
//!
//! A joiner propagates the first `get` error, which aborts a whole-file
//! reconstruction. On a live network a single `get` often fails transiently:
//! too few candidate storers momentarily, or a candidate refusing under load.
//! Retrying with capped exponential backoff turns those transient misses into
//! eventual hits so a large download survives per-chunk flakiness.
//!
//! The decorator takes no timer dependency of its own: the sleep is injected
//! through [`Sleeper`], so each consumer supplies its platform delay (tokio on
//! native, a browser timer on wasm) and nectar stays timer-agnostic.

use std::fmt;
use std::future::Future;
use std::time::Duration;

use super::maybe_send::{MaybeSend, MaybeSync};
use super::typed::{ChunkGet, ChunkHas, ChunkPut};
use crate::chunk::{Chunk, ChunkAddress, ChunkRegistry, Verified};

/// Injected async delay so the decorator owns its timer: nectar takes no new
/// timer dependency and each consumer supplies its platform sleep.
pub trait Sleeper: MaybeSend + MaybeSync {
    /// Complete after at least `dur` has elapsed.
    fn sleep(&self, dur: Duration) -> impl Future<Output = ()> + MaybeSend;
}

/// Retry budget and backoff shape for [`RetryingChunkGet`].
#[derive(Clone, Copy, Debug)]
pub struct RetryConfig {
    /// Total `get` attempts (initial try plus retries) before the error
    /// propagates. Counts the first try, so `1` disables retrying.
    pub max_attempts: u32,
    /// Backoff before the first retry; doubles each subsequent retry up to
    /// [`Self::backoff_cap`].
    pub base_backoff: Duration,
    /// Upper bound on a single backoff wait, so late retries stay responsive.
    pub backoff_cap: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 8,
            base_backoff: Duration::from_millis(150),
            backoff_cap: Duration::from_secs(8),
        }
    }
}

impl RetryConfig {
    /// Backoff for the retry that follows attempt `attempt` (1-based): base
    /// doubled `attempt - 1` times, capped, plus up to 50% jitter keyed on the
    /// address so chunks failing together spread their retries apart.
    fn backoff_for(&self, attempt: u32, address: &ChunkAddress) -> Duration {
        let shift = attempt.saturating_sub(1).min(16);
        let scaled = self.base_backoff.saturating_mul(1u32 << shift);
        let capped = scaled.min(self.backoff_cap);
        let jitter = capped
            .mul_f64(0.5 * jitter_unit(address))
            .min(self.backoff_cap);
        capped.saturating_add(jitter)
    }
}

/// A wasm-safe pseudo-random value in `[0, 1)` used only to decorrelate retries
/// of chunks that failed together. Mixes the `web-time` wall clock (browser
/// clock on wasm, `std::time` on native) with the address so distinct chunks
/// jitter apart even within one clock tick; needs no `rand` dependency and is
/// never security-sensitive.
fn jitter_unit(address: &ChunkAddress) -> f64 {
    use web_time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.subsec_nanos());
    let addr_mix = address
        .as_bytes()
        .iter()
        .take(4)
        .fold(0u32, |acc, &b| (acc << 8) | u32::from(b));
    f64::from(nanos ^ addr_mix) / (f64::from(u32::MAX) + 1.0)
}

/// [`ChunkGet`] decorator that retries transient `get` failures with capped
/// exponential backoff and jitter, sleeping through an injected [`Sleeper`].
///
/// Retries on any error since the inner error type is opaque here; a genuinely
/// unretrievable chunk still fails, but only after the attempt budget is spent.
/// `put` and `has` delegate to the inner store untouched. `Clone` is cheap when
/// `G` and `S` are.
#[derive(Clone)]
pub struct RetryingChunkGet<G, S> {
    inner: G,
    sleeper: S,
    config: RetryConfig,
}

impl<G, S> fmt::Debug for RetryingChunkGet<G, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetryingChunkGet")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl<G, S> RetryingChunkGet<G, S> {
    /// Wrap `inner`, sleeping through `sleeper`, using `config`.
    pub const fn new(inner: G, sleeper: S, config: RetryConfig) -> Self {
        Self {
            inner,
            sleeper,
            config,
        }
    }

    /// Wrap `inner` with [`RetryConfig::default`].
    pub fn with_default(inner: G, sleeper: S) -> Self {
        Self::new(inner, sleeper, RetryConfig::default())
    }
}

impl<R: ChunkRegistry, G: ChunkGet<R>, S: Sleeper> ChunkGet<R> for RetryingChunkGet<G, S> {
    /// Retrying changes nothing about the medium: the inner trust level
    /// passes through.
    type Trust = G::Trust;
    type Error = G::Error;

    #[allow(clippy::arithmetic_side_effects)] // attempt only increments while < max_attempts (u32), so + 1 cannot overflow
    async fn get(&self, address: &ChunkAddress) -> Result<Chunk<G::Trust, R>, Self::Error> {
        let mut attempt = 1;
        loop {
            match self.inner.get(address).await {
                Ok(chunk) => return Ok(chunk),
                Err(e) => {
                    if attempt >= self.config.max_attempts {
                        return Err(e);
                    }
                    self.sleeper
                        .sleep(self.config.backoff_for(attempt, address))
                        .await;
                    attempt += 1;
                }
            }
        }
    }
}

impl<R: ChunkRegistry, G: ChunkPut<R>, S: MaybeSend + MaybeSync> ChunkPut<R>
    for RetryingChunkGet<G, S>
{
    type Error = G::Error;

    async fn put(&self, chunk: Chunk<Verified, R>) -> Result<(), Self::Error> {
        self.inner.put(chunk).await
    }
}

impl<G: ChunkHas, S: MaybeSend + MaybeSync> ChunkHas for RetryingChunkGet<G, S> {
    async fn has(&self, address: &ChunkAddress) -> bool {
        self.inner.has(address).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU32, Ordering};

    use futures::executor::block_on;

    use crate::DefaultContentChunk;
    use crate::chunk::StandardChunkSet;

    /// A [`Sleeper`] that returns immediately, so tests never wait real time.
    struct NoSleep;

    impl Sleeper for NoSleep {
        async fn sleep(&self, _dur: Duration) {}
    }

    #[derive(Debug, thiserror::Error)]
    #[error("transient")]
    struct Transient;

    /// A store that fails its first `remaining_failures` gets then succeeds,
    /// counting every `get`, `put`, and `has` call.
    struct FlakyStore {
        chunk: Chunk,
        remaining_failures: Mutex<u32>,
        get_calls: AtomicU32,
        put_calls: AtomicU32,
        has_calls: AtomicU32,
    }

    impl FlakyStore {
        fn new(remaining_failures: u32) -> Self {
            let content = DefaultContentChunk::new("retry probe").expect("build content chunk");
            let chunk = Chunk::from_envelope(content.into()).expect("seal content chunk");
            Self {
                chunk,
                remaining_failures: Mutex::new(remaining_failures),
                get_calls: AtomicU32::new(0),
                put_calls: AtomicU32::new(0),
                has_calls: AtomicU32::new(0),
            }
        }
    }

    impl ChunkGet<StandardChunkSet> for FlakyStore {
        type Trust = Verified;
        type Error = Transient;

        async fn get(&self, _address: &ChunkAddress) -> Result<Chunk, Self::Error> {
            self.get_calls.fetch_add(1, Ordering::SeqCst);
            let mut left = self.remaining_failures.lock().expect("lock");
            if *left > 0 {
                *left -= 1;
                return Err(Transient);
            }
            Ok(self.chunk.clone())
        }
    }

    impl ChunkPut<StandardChunkSet> for FlakyStore {
        type Error = Transient;

        async fn put(&self, _chunk: Chunk) -> Result<(), Self::Error> {
            self.put_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    impl ChunkHas for FlakyStore {
        async fn has(&self, _address: &ChunkAddress) -> bool {
            self.has_calls.fetch_add(1, Ordering::SeqCst);
            true
        }
    }

    #[test]
    fn recovers_when_failures_below_budget() {
        // 7 failures, then success on the 8th (== max_attempts) get.
        let store = RetryingChunkGet::with_default(FlakyStore::new(7), NoSleep);
        let address = *store.inner.chunk.address();

        let got = block_on(store.get(&address)).expect("recovered within budget");
        assert_eq!(got.address(), &address);
        assert_eq!(store.inner.get_calls.load(Ordering::SeqCst), 8);
    }

    #[test]
    fn propagates_after_exactly_max_attempts() {
        // Always fails: expect exactly max_attempts gets, then the error.
        let store = RetryingChunkGet::with_default(FlakyStore::new(u32::MAX), NoSleep);
        let address = *store.inner.chunk.address();

        let err = block_on(store.get(&address));
        assert!(err.is_err(), "budget exhausted, error must propagate");
        assert_eq!(
            store.inner.get_calls.load(Ordering::SeqCst),
            RetryConfig::default().max_attempts
        );
    }

    #[test]
    fn put_and_has_are_not_retried() {
        let store = RetryingChunkGet::with_default(FlakyStore::new(u32::MAX), NoSleep);
        let address = *store.inner.chunk.address();

        assert!(block_on(store.has(&address)));
        block_on(store.put(store.inner.chunk.clone())).expect("put delegates");
        assert_eq!(store.inner.has_calls.load(Ordering::SeqCst), 1);
        assert_eq!(store.inner.put_calls.load(Ordering::SeqCst), 1);
    }
}
