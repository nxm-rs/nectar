//! Deterministic async test harness: the sanctioned executor entry point,
//! budgeted cooperative yields, a manual-poll driver, and a gate for
//! stepwise backpressure probes. Panics are the point: a deadlock becomes a
//! failure instead of a hang, hence the opt-out from the workspace lints.
//! Also hosts [`SeedReplay`], the walker behind every stable seed-corpus
//! replay test.
//!
//! # Generator and test layering
//!
//! The workspace convention for `Arbitrary`, proptest, seed-replay and fuzz
//! code:
//!
//! 1. `Arbitrary` impls sit top-level under
//!    `#[cfg(any(test, feature = "arbitrary"))]`, in a `mod arbitrary_impls`
//!    where a file has several.
//! 2. Two tiers: raw `Arbitrary` impls may yield structured-but-invalid
//!    values and exist only where such values are representable and consumed
//!    (decode-target food); valid-by-construction constructors
//!    (`arbitrary_signed`, the `generators` modules) feed round-trips. Pure
//!    wire decoders eat raw fuzzer bytes and need no structured raw tier.
//! 3. One generator per type: fuzz targets compose the crate generators,
//!    never local duplicates. Target-specific input grammars with no
//!    crate-side equivalent stay local to the target. Types whose upstream
//!    crate already ships an `Arbitrary` impl behind a feature compose that
//!    impl; never hand-write one for an upstream type.
//! 4. Proptest strategies bridge from the `Arbitrary` layer via
//!    `proptest-arbitrary-interop` (`arb::<T>()`) for the raw tier;
//!    regime-targeted hand strategies are the sanctioned exception and must
//!    state which threshold they reach.
//! 5. Pin split: seed-replay tests pin fuzz findings through the shared
//!    oracle entry points; `proptest!` plus committed `proptest-regressions`
//!    files pin properties. Every crate with fuzz targets carries both,
//!    derived from the same `Arbitrary` layer.
//! 6. Feature hygiene: every `arbitrary` feature lists `std` and is
//!    unreachable from default, `std` or any shipped feature. Test matrices
//!    use per-crate feature lanes, never workspace-wide `--all-features`.
//! 7. Derive hygiene: crate-side impls are hand-written (the two-tier
//!    pattern is semantic, not derivable); only the fuzz workspace pulls the
//!    `arbitrary` derive, for its target-local input grammars.
//! Behind `fixtures`: the shared split fixtures and spec doubles.

mod seeds;

pub use seeds::SeedReplay;
#[cfg(feature = "fixtures")]
mod fixtures;
#[cfg(feature = "fixtures")]
pub use fixtures::*;

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll, Waker};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::Wake;

/// Drives a future to completion on the calling thread.
///
/// The sole sanctioned `block_on` call site in the workspace.
#[allow(clippy::disallowed_methods)]
pub fn run<F: Future>(f: F) -> F::Output {
    futures_executor::block_on(f)
}

/// Yields once so sibling futures in the same combinator get re-polled.
pub async fn yield_now() {
    struct YieldNow(bool);
    impl Future for YieldNow {
        type Output = ();
        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            if self.0 {
                Poll::Ready(())
            } else {
                self.0 = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
    YieldNow(false).await;
}

/// Yields up to `budget` times waiting for `cond`; panics on exhaustion so a
/// would-be hang becomes a failure.
pub async fn yield_until(budget: usize, cond: impl Fn() -> bool) {
    for _ in 0..budget {
        if cond() {
            return;
        }
        yield_now().await;
    }
    assert!(
        cond(),
        "yield_until: condition still false after {budget} yields"
    );
}

struct CountWaker(AtomicUsize);

impl Wake for CountWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

/// Manual-poll driver: single-steps a future so invariants can be asserted
/// between polls.
pub struct Drive<F> {
    fut: Pin<Box<F>>,
    counter: Arc<CountWaker>,
    waker: Waker,
}

impl<F> core::fmt::Debug for Drive<F> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Drive")
            .field("wakes", &self.counter.0.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl<F: Future> Drive<F> {
    /// Wraps `f` for manual polling.
    pub fn new(f: F) -> Self {
        let counter = Arc::new(CountWaker(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&counter));
        Self {
            fut: Box::pin(f),
            counter,
            waker,
        }
    }

    /// Polls the future once.
    pub fn poll(&mut self) -> Poll<F::Output> {
        self.fut
            .as_mut()
            .poll(&mut Context::from_waker(&self.waker))
    }

    /// Wakes recorded so far. Diagnostic only: exact counts couple tests to
    /// combinator internals, so never assert equality on this.
    pub fn wakes(&self) -> usize {
        self.counter.0.load(Ordering::Relaxed)
    }
}

fn lock(m: &Mutex<State>) -> MutexGuard<'_, State> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

#[derive(Default)]
struct State {
    next_id: u64,
    queue: VecDeque<(u64, Option<Waker>)>,
    granted: Vec<u64>,
    peak: usize,
}

/// FIFO gate for probe stores: [`enter`](Self::enter) parks until a matching
/// [`release`](Self::release), so backpressure is exercised at each step
/// rather than only on the initial burst. Accounting is drop-aware: a parked
/// waiter that is cancelled leaves the queue.
///
/// ```
/// use nectar_testing::{Drive, GateStore};
///
/// let gate = GateStore::new();
/// let mut d = Drive::new({
///     let g = gate.clone();
///     async move {
///         g.enter().await;
///         g.enter().await;
///     }
/// });
/// assert!(d.poll().is_pending());
/// assert_eq!(gate.waiting(), 1);
/// gate.release(1);
/// assert!(d.poll().is_pending()); // the second enter parks: stepwise, not burst
/// assert_eq!(gate.waiting(), 1);
/// gate.release(1);
/// assert!(d.poll().is_ready());
/// ```
#[derive(Clone, Default)]
pub struct GateStore {
    state: Arc<Mutex<State>>,
}

impl core::fmt::Debug for GateStore {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("GateStore").finish_non_exhaustive()
    }
}

impl GateStore {
    /// New gate with no waiters.
    pub fn new() -> Self {
        Self::default()
    }

    /// Parks until granted by a later [`release`](Self::release).
    pub fn enter(&self) -> Enter {
        Enter {
            gate: Arc::clone(&self.state),
            state: EnterState::Idle,
        }
    }

    /// Grants the `n` oldest parked waiters and wakes them. No stored permits:
    /// any `n` beyond the parked count is dropped, so releasing before a waiter
    /// parks is a no-op.
    pub fn release(&self, n: usize) {
        let mut wakers = Vec::new();
        {
            let mut s = lock(&self.state);
            for _ in 0..n {
                if let Some((id, waker)) = s.queue.pop_front() {
                    s.granted.push(id);
                    wakers.extend(waker);
                }
            }
        }
        for w in wakers {
            w.wake();
        }
    }

    /// Currently parked waiters.
    pub fn waiting(&self) -> usize {
        lock(&self.state).queue.len()
    }

    /// High-water mark of concurrently parked waiters.
    pub fn peak(&self) -> usize {
        lock(&self.state).peak
    }
}

enum EnterState {
    Idle,
    Parked(u64),
    Done,
}

/// Future returned by [`GateStore::enter`].
pub struct Enter {
    gate: Arc<Mutex<State>>,
    state: EnterState,
}

impl core::fmt::Debug for Enter {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Enter").finish_non_exhaustive()
    }
}

impl Future for Enter {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        let mut s = lock(&this.gate);
        match this.state {
            EnterState::Idle => {
                let id = s.next_id;
                s.next_id += 1;
                s.queue.push_back((id, Some(cx.waker().clone())));
                s.peak = s.peak.max(s.queue.len());
                this.state = EnterState::Parked(id);
                Poll::Pending
            }
            EnterState::Parked(id) => {
                if let Some(pos) = s.granted.iter().position(|&g| g == id) {
                    s.granted.swap_remove(pos);
                    this.state = EnterState::Done;
                    Poll::Ready(())
                } else {
                    if let Some(slot) = s.queue.iter_mut().find(|(q, _)| *q == id) {
                        slot.1 = Some(cx.waker().clone());
                    }
                    Poll::Pending
                }
            }
            EnterState::Done => Poll::Ready(()),
        }
    }
}

impl Drop for Enter {
    fn drop(&mut self) {
        if let EnterState::Parked(id) = self.state {
            let mut s = lock(&self.gate);
            s.queue.retain(|(q, _)| *q != id);
            if let Some(pos) = s.granted.iter().position(|&g| g == id) {
                s.granted.swap_remove(pos);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::cell::Cell;

    #[test]
    fn yield_now_parks_once_then_completes() {
        let mut d = Drive::new(yield_now());
        assert!(d.poll().is_pending());
        assert!(d.wakes() >= 1);
        assert!(d.poll().is_ready());
    }

    #[test]
    fn yield_until_returns_once_condition_holds() {
        let polls = Cell::new(0_usize);
        run(yield_until(10, || {
            polls.set(polls.get() + 1);
            polls.get() >= 3
        }));
        assert_eq!(polls.get(), 3);
    }

    #[test]
    #[should_panic(expected = "yield_until")]
    fn yield_until_panics_on_exhaustion() {
        run(yield_until(3, || false));
    }

    #[test]
    fn gate_grants_fifo_and_cancellation_unparks() {
        let gate = GateStore::new();
        let mut a = Drive::new(gate.enter());
        let mut b = Drive::new(gate.enter());
        assert!(a.poll().is_pending());
        assert!(b.poll().is_pending());
        assert_eq!((gate.waiting(), gate.peak()), (2, 2));
        gate.release(1);
        assert!(a.poll().is_ready());
        assert!(b.poll().is_pending());
        drop(b);
        assert_eq!(gate.waiting(), 0);
        gate.release(1);
        let mut c = Drive::new(gate.enter());
        assert!(c.poll().is_pending());
    }
}
