//! Latency-shaped store: every get waits one simulated round trip.
//!
//! Latency model: real short sleeps against a shared timer thread. A get's
//! deadline is its registration instant plus the configured RTT, so
//! concurrent gets genuinely overlap in wall time while a serial fetch
//! discipline pays one full RTT per chunk. Condvar timeout granularity is
//! tens of microseconds on Linux, so keep timed RTTs at 0.5 ms or above
//! and sub-10 ms so runs stay bounded. The single timer queue means lock
//! traffic grows with in-flight depth; that biases against the deeper
//! window, never for it.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use nectar_primitives::chunk::{Chunk, ChunkAddress, StandardChunkSet, Verified};
use nectar_primitives::store::{ChunkGet, ChunkHas, ChunkPut, ChunkStoreError};

use crate::store::CountingStore;

/// Fetch-concurrency gauge: current and high-water in-flight gets.
#[derive(Debug, Default)]
pub struct FetchGauge {
    in_flight: AtomicU64,
    high_water: AtomicU64,
}

impl FetchGauge {
    fn enter(&self) -> InFlight<'_> {
        let now = self.in_flight.fetch_add(1, Ordering::Relaxed) + 1;
        self.high_water.fetch_max(now, Ordering::Relaxed);
        InFlight(self)
    }

    /// Highest concurrent get count since the last reset.
    #[must_use]
    pub fn max_in_flight(&self) -> u64 {
        self.high_water.load(Ordering::Relaxed)
    }

    /// Drop the high-water mark back to the current in-flight count.
    pub fn reset(&self) {
        self.high_water
            .store(self.in_flight.load(Ordering::Relaxed), Ordering::Relaxed);
    }
}

/// Guard pairing every gauge entry with an exit, cancelled gets included.
struct InFlight<'a>(&'a FetchGauge);

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        self.0.in_flight.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Pending deadlines and their wakers, serviced by the timer thread.
#[derive(Default)]
struct TimerQueue {
    pending: Mutex<Vec<(Instant, Waker)>>,
    tick: Condvar,
}

/// The process-wide timer thread, started on first use.
fn timer() -> &'static TimerQueue {
    static TIMER: OnceLock<&'static TimerQueue> = OnceLock::new();
    TIMER.get_or_init(|| {
        let queue: &'static TimerQueue = Box::leak(Box::default());
        std::thread::Builder::new()
            .name("bench-rtt-timer".into())
            .spawn(move || drive(queue))
            .unwrap();
        queue
    })
}

fn drive(queue: &'static TimerQueue) -> ! {
    let mut due = Vec::new();
    let mut pending = queue.pending.lock().unwrap();
    loop {
        let now = Instant::now();
        let mut next: Option<Instant> = None;
        let mut index = 0;
        while index < pending.len() {
            if pending[index].0 <= now {
                due.push(pending.swap_remove(index).1);
            } else {
                let deadline = pending[index].0;
                next = Some(next.map_or(deadline, |soonest| soonest.min(deadline)));
                index += 1;
            }
        }
        if !due.is_empty() {
            // Wake outside the lock so woken tasks can re-register freely.
            drop(pending);
            for waker in due.drain(..) {
                waker.wake();
            }
            pending = queue.pending.lock().unwrap();
            continue;
        }
        pending = match next {
            Some(deadline) => {
                let wait = deadline.saturating_duration_since(now);
                queue.tick.wait_timeout(pending, wait).unwrap().0
            }
            None => queue.tick.wait(pending).unwrap(),
        };
    }
}

/// One RTT's wait; registration happens on poll.
struct Sleep {
    deadline: Instant,
}

impl Future for Sleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if Instant::now() >= self.deadline {
            return Poll::Ready(());
        }
        // Re-registration on spurious polls only leaves stale entries that
        // wake an already-ready future; harmless and bounded by the window.
        let queue = timer();
        queue
            .pending
            .lock()
            .unwrap()
            .push((self.deadline, cx.waker().clone()));
        queue.tick.notify_one();
        Poll::Pending
    }
}

/// A [`CountingStore`] whose gets each wait one RTT before resolving; puts
/// and existence checks stay immediate.
///
/// `Clone` is shallow: clones share the chunks, counters and gauge.
#[derive(Clone, Debug)]
pub struct DelayStore {
    inner: CountingStore,
    rtt: Duration,
    gauge: Arc<FetchGauge>,
}

impl DelayStore {
    /// Wrap `inner`, delaying every get by `rtt`.
    #[must_use]
    pub fn new(inner: CountingStore, rtt: Duration) -> Self {
        Self {
            inner,
            rtt,
            gauge: Arc::new(FetchGauge::default()),
        }
    }

    /// The fetch-concurrency gauge.
    #[must_use]
    pub fn gauge(&self) -> &FetchGauge {
        &self.gauge
    }
}

impl ChunkGet<StandardChunkSet> for DelayStore {
    type Trust = Verified;
    type Error = ChunkStoreError;

    async fn get(
        &self,
        address: &ChunkAddress,
    ) -> Result<Chunk<Verified, StandardChunkSet>, Self::Error> {
        let guard = self.gauge.enter();
        Sleep {
            deadline: Instant::now() + self.rtt,
        }
        .await;
        let out =
            <CountingStore as ChunkGet<StandardChunkSet>>::get(&self.inner, address).await;
        drop(guard);
        out
    }
}

impl ChunkPut<StandardChunkSet> for DelayStore {
    type Error = core::convert::Infallible;

    async fn put(&self, chunk: Chunk<Verified, StandardChunkSet>) -> Result<(), Self::Error> {
        <CountingStore as ChunkPut<StandardChunkSet>>::put(&self.inner, chunk).await
    }
}

impl ChunkHas for DelayStore {
    async fn has(&self, address: &ChunkAddress) -> bool {
        <CountingStore as ChunkHas>::has(&self.inner, address).await
    }
}
