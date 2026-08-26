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
use std::time::Duration;

use super::typed::{ChunkGet, ChunkPut, PutUnit};
use crate::chunk::{Chunk, ChunkAddress, ChunkRegistry};
use crate::error::StoreError;
use crate::marker::{MaybeSend, MaybeSync};
use nectar_tasks::Sleeper;

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
/// Retries while the failure is classified transient; a definite miss or a
/// terminal failure propagates at once, so spending the attempt budget no
/// longer turns a miss into eight retries. `put` delegates to the inner store
/// untouched. `Clone` is cheap when `G` and `S` are.
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
                Err(error) if attempt >= self.config.max_attempts || !error.is_transient() => {
                    return Err(error);
                }
                Err(_) => {
                    self.sleeper
                        .sleep(self.config.backoff_for(attempt, address))
                        .await;
                    attempt += 1;
                }
            }
        }
    }
}

impl<U: PutUnit, G: ChunkPut<U>, S: MaybeSend + MaybeSync> ChunkPut<U> for RetryingChunkGet<G, S> {
    type Error = G::Error;

    async fn put(&self, unit: U) -> Result<(), Self::Error> {
        self.inner.put(unit).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU32, Ordering};

    use nectar_testing::run;

    use crate::DefaultContentChunk;
    use crate::chunk::{StandardChunkSet, Verified};
    use crate::error::{ChunkStoreError, StoreError};

    /// A [`Sleeper`] that returns immediately, so tests never wait real time.
    struct NoSleep;

    impl Sleeper for NoSleep {
        async fn sleep(&self, _dur: Duration) {}
    }

    #[derive(Debug, thiserror::Error)]
    #[error("transient")]
    struct Transient;

    impl StoreError for Transient {
        fn is_definitely_absent(&self) -> bool {
            false
        }

        fn is_transient(&self) -> bool {
            true
        }
    }

    /// A store that fails its first `remaining_failures` gets then succeeds,
    /// counting every `get` and `put` call.
    struct FlakyStore {
        chunk: Chunk,
        remaining_failures: Mutex<u32>,
        get_calls: AtomicU32,
        put_calls: AtomicU32,
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

    impl ChunkPut<Chunk> for FlakyStore {
        type Error = Transient;

        async fn put(&self, _chunk: Chunk) -> Result<(), Self::Error> {
            self.put_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    /// A store that answers every get with the medium's own absence.
    struct MissStore;

    impl ChunkGet<StandardChunkSet> for MissStore {
        type Trust = Verified;
        type Error = ChunkStoreError;

        async fn get(&self, address: &ChunkAddress) -> Result<Chunk, Self::Error> {
            Err(ChunkStoreError::not_found(address))
        }
    }

    #[test]
    fn recovers_when_failures_below_budget() {
        // 7 failures, then success on the 8th (== max_attempts) get.
        let store = RetryingChunkGet::with_default(FlakyStore::new(7), NoSleep);
        let address = *store.inner.chunk.address();

        let got = run(store.get(&address)).expect("recovered within budget");
        assert_eq!(got.address(), &address);
        assert_eq!(store.inner.get_calls.load(Ordering::SeqCst), 8);
    }

    #[test]
    fn propagates_after_exactly_max_attempts() {
        // Always fails: expect exactly max_attempts gets, then the error.
        let store = RetryingChunkGet::with_default(FlakyStore::new(u32::MAX), NoSleep);
        let address = *store.inner.chunk.address();

        let err = run(store.get(&address));
        assert!(err.is_err(), "budget exhausted, error must propagate");
        assert_eq!(
            store.inner.get_calls.load(Ordering::SeqCst),
            RetryConfig::default().max_attempts
        );
    }

    #[test]
    fn put_is_not_retried() {
        let store = RetryingChunkGet::with_default(FlakyStore::new(u32::MAX), NoSleep);

        run(store.put(store.inner.chunk.clone())).expect("put delegates");
        assert_eq!(store.inner.put_calls.load(Ordering::SeqCst), 1);
    }

    /// A definite miss is not a failure to retry: it propagates on the first
    /// get instead of spending the attempt budget.
    #[test]
    fn definite_miss_propagates_without_retrying() {
        let store = RetryingChunkGet::with_default(MissStore, NoSleep);
        let address = ChunkAddress::default();

        let error = run(store.get(&address)).expect_err("the miss propagates");
        assert!(error.is_definitely_absent());
    }

    /// A failure that is neither transient nor a miss is terminal: it
    /// propagates on the first get instead of spending the attempt budget.
    #[test]
    fn terminal_failure_propagates_without_retrying() {
        #[derive(Debug, thiserror::Error)]
        #[error("terminal")]
        struct Terminal;

        impl StoreError for Terminal {
            fn is_definitely_absent(&self) -> bool {
                false
            }

            fn is_transient(&self) -> bool {
                false
            }
        }

        struct TerminalStore;

        impl ChunkGet<StandardChunkSet> for TerminalStore {
            type Trust = Verified;
            type Error = Terminal;

            async fn get(&self, _address: &ChunkAddress) -> Result<Chunk, Self::Error> {
                Err(Terminal)
            }
        }

        let store = RetryingChunkGet::with_default(TerminalStore, NoSleep);
        let error = run(store.get(&ChunkAddress::default())).expect_err("terminal propagates");
        assert!(!error.is_definitely_absent());
        assert!(!error.is_transient());
    }
}
