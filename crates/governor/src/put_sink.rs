//! The shared bounded put window: a [`FuturesUnordered`] set capped by
//! [`Admission`], generic over the put future.

use core::fmt;
use core::future::{Future, poll_fn};
use core::pin::Pin;
use core::task::{Context, Poll, Waker};

use futures_util::stream::{FuturesUnordered, Stream};

use crate::admission::Admission;
use crate::window::Window;

/// Bounded set of in-flight puts, order-free within `window` slots.
///
/// Every put is admitted head-served, so the whole window is usable and
/// completions surface in any order. Opening polls run on a noop waker, so a
/// synchronously ready put settles inline and never occupies a slot; a parked
/// put is driven only by [`poll_step`](Self::poll_step) and the wrappers over
/// it under the caller's waker.
pub struct PutSink<F> {
    in_flight: FuturesUnordered<F>,
    admission: Admission,
}

impl<F: Future> PutSink<F> {
    /// A put window `window` slots wide.
    pub fn new(window: Window) -> Self {
        Self {
            in_flight: FuturesUnordered::new(),
            admission: Admission::new(window),
        }
    }

    /// Puts currently in flight; the caller's peak-occupancy stat.
    pub fn len(&self) -> usize {
        self.in_flight.len()
    }

    /// Whether no put is in flight.
    pub fn is_empty(&self) -> bool {
        self.in_flight.is_empty()
    }

    /// The window admitted against.
    pub const fn window(&self) -> Window {
        self.admission.window()
    }

    /// Whether the window admits another put now.
    pub fn admits(&self) -> bool {
        self.admission.admits(self.in_flight.len(), true)
    }

    /// Poll one settled put out of the window. `Ready(None)` once empty;
    /// `Pending` leaves the window untouched.
    pub fn poll_step(&mut self, cx: &mut Context<'_>) -> Poll<Option<F::Output>> {
        Pin::new(&mut self.in_flight).poll_next(cx)
    }

    /// Secure a slot. `Ready(None)` when a slot is free to [`push`](Self::push)
    /// into; `Ready(Some(completion))` after settling one to make room;
    /// `Pending` when full and none ready, consuming nothing.
    pub fn poll_admit(&mut self, cx: &mut Context<'_>) -> Poll<Option<F::Output>> {
        if self.admits() {
            return Poll::Ready(None);
        }
        // The window is full, so the set is non-empty and never yields `None`.
        self.poll_step(cx)
    }

    /// Await one completion; `None` when the window is empty.
    // reinvention: sanctioned put-window drain; nectar_governor::PutSink is the home of this vocabulary.
    pub async fn settle_one(&mut self) -> Option<F::Output> {
        poll_fn(|cx| self.poll_step(cx)).await
    }

    /// Secure a slot, parking until one is free. Returns the completion that
    /// freed it, or `None` when a slot was already free.
    pub async fn admit(&mut self) -> Option<F::Output> {
        poll_fn(|cx| self.poll_admit(cx)).await
    }

    /// Fold every outstanding completion, stopping on the first `fold` error.
    pub async fn settle<E>(
        &mut self,
        mut fold: impl FnMut(F::Output) -> Result<(), E>,
    ) -> Result<(), E> {
        while let Some(completion) = self.settle_one().await {
            fold(completion)?;
        }
        Ok(())
    }

    /// Fold the completions ready now without parking, stopping on the first
    /// `fold` error: newly admitted puts start under the caller's waker before
    /// more work enters.
    // reinvention: sanctioned put-window drain; nectar_governor::PutSink is the home of this vocabulary.
    pub async fn sweep<E>(
        &mut self,
        mut fold: impl FnMut(F::Output) -> Result<(), E>,
    ) -> Result<(), E> {
        poll_fn(move |cx| {
            loop {
                match self.poll_step(cx) {
                    Poll::Ready(Some(completion)) => {
                        if let Err(error) = fold(completion) {
                            return Poll::Ready(Err(error));
                        }
                    }
                    Poll::Ready(None) | Poll::Pending => return Poll::Ready(Ok(())),
                }
            }
        })
        .await
    }
}

impl<F: Future + Unpin> PutSink<F> {
    /// Admit `put`, driving its opening poll on a noop waker: a ready put
    /// settles inline as `Some(completion)` and never occupies a slot, a
    /// pending one parks in the window as `None`.
    ///
    /// Secure a slot with [`admit`](Self::admit) first; this does not bound
    /// the window.
    pub fn push(&mut self, put: F) -> Option<F::Output> {
        let mut put = put;
        match Pin::new(&mut put).poll(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(completion) => Some(completion),
            Poll::Pending => {
                self.in_flight.push(put);
                None
            }
        }
    }
}

impl<F> fmt::Debug for PutSink<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PutSink")
            .field("in_flight", &self.in_flight.len())
            .field("window", &self.admission.window())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use core::sync::atomic::{AtomicUsize, Ordering};

    use std::boxed::Box;
    use std::sync::Arc;
    use std::vec;
    use std::vec::Vec;

    use super::*;

    type BoxedPut = Pin<Box<dyn Future<Output = usize>>>;

    /// A completion that stays pending for `left` polls, waking itself each
    /// time, before yielding `id`: a latency injector for genuine overlap.
    struct Delay {
        left: u32,
        id: usize,
    }

    impl Future for Delay {
        type Output = usize;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<usize> {
            if self.left == 0 {
                return Poll::Ready(self.id);
            }
            self.left -= 1;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }

    const fn delayed(id: usize, left: u32) -> Delay {
        Delay { left, id }
    }

    fn window(slots: u16) -> Window {
        Window::new(slots).unwrap()
    }

    #[test]
    fn synchronous_put_settles_inline_without_a_slot() {
        let mut sink: PutSink<Delay> = PutSink::new(window(4));
        assert_eq!(sink.push(delayed(7, 0)), Some(7));
        assert_eq!(sink.len(), 0);
        assert!(sink.is_empty());
    }

    #[test]
    fn an_erased_boxed_put_rides_the_same_window() {
        let mut sink: PutSink<BoxedPut> = PutSink::new(window(4));
        assert_eq!(sink.push(Box::pin(delayed(7, 0))), Some(7));
        assert!(sink.push(Box::pin(delayed(9, 1))).is_none());
        assert_eq!(sink.len(), 1);
    }

    #[test]
    fn a_non_unpin_put_drains() {
        nectar_testing::run(async {
            let mut sink = PutSink::new(window(4));
            // An async block is `!Unpin`; only `push` carries that bound.
            sink.in_flight.push(async { 7usize });
            assert_eq!(sink.settle_one().await, Some(7));
        });
    }

    #[test]
    fn peak_stays_within_the_window_and_overlaps() {
        nectar_testing::run(async {
            let mut sink: PutSink<Delay> = PutSink::new(window(4));
            let mut peak = 0;
            let mut done = Vec::new();
            for id in 0..20 {
                if let Some(completion) = sink.admit().await {
                    done.push(completion);
                }
                assert!(sink.admits());
                if let Some(completion) = sink.push(delayed(id, 3)) {
                    done.push(completion);
                }
                peak = peak.max(sink.len());
            }
            sink.settle(|completion| {
                done.push(completion);
                Ok::<(), ()>(())
            })
            .await
            .unwrap();

            // Bounded: the window is never exceeded.
            assert!(peak <= 4, "peak {peak} exceeded the window");
            // Genuine overlap: more than one put ran concurrently.
            assert!(peak > 1, "peak {peak} shows no overlap");
            // Byte-safety: every put settled exactly once.
            done.sort_unstable();
            assert_eq!(done, (0..20).collect::<Vec<_>>());
        });
    }

    #[test]
    fn completions_are_order_free() {
        nectar_testing::run(async {
            let mut sink: PutSink<Delay> = PutSink::new(window(8));
            // Later ids finish first: submission order is not completion order.
            for id in 0..6 {
                let latency = u32::try_from(6 - id).unwrap();
                assert!(sink.push(delayed(id, latency)).is_none());
            }
            let mut order = Vec::new();
            sink.settle(|completion| {
                order.push(completion);
                Ok::<(), ()>(())
            })
            .await
            .unwrap();

            assert_ne!(order, (0..6).collect::<Vec<_>>());
            order.sort_unstable();
            assert_eq!(order, (0..6).collect::<Vec<_>>());
        });
    }

    #[test]
    fn settle_stops_on_the_first_fold_error() {
        nectar_testing::run(async {
            let mut sink: PutSink<Delay> = PutSink::new(window(4));
            for id in 0..4 {
                assert!(sink.push(delayed(id, 1)).is_none());
            }
            let mut seen = 0;
            let outcome = sink
                .settle(|_| {
                    seen += 1;
                    Err::<(), usize>(seen)
                })
                .await;
            assert_eq!(outcome, Err(1));
            assert_eq!(seen, 1);
        });
    }

    #[test]
    fn sweep_folds_ready_puts_without_parking() {
        nectar_testing::run(async {
            let mut sink: PutSink<Delay> = PutSink::new(window(4));
            // One ready-after-a-poll and one long-lived put.
            assert!(sink.push(delayed(1, 1)).is_none());
            assert!(sink.push(delayed(2, 1000)).is_none());
            let mut swept = Vec::new();
            // The first sweep may see nothing ready yet; keep sweeping until
            // the short put lands, proving sweep never parks on the long one.
            while swept.is_empty() {
                sink.sweep(|completion| {
                    swept.push(completion);
                    Ok::<(), ()>(())
                })
                .await
                .unwrap();
            }
            assert_eq!(swept, vec![1]);
            assert_eq!(sink.len(), 1);
        });
    }

    #[test]
    fn dropping_mid_flight_aborts_every_put() {
        let alive = Arc::new(AtomicUsize::new(0));
        let mut sink: PutSink<BoxedPut> = PutSink::new(window(8));
        for id in 0..3 {
            let alive = Arc::clone(&alive);
            alive.fetch_add(1, Ordering::SeqCst);
            sink.push(Box::pin(async move {
                let _guard = Guard(alive);
                core::future::pending::<()>().await;
                id
            }));
        }
        assert_eq!(sink.len(), 3);
        assert_eq!(alive.load(Ordering::SeqCst), 3);

        drop(sink);
        // Every parked put was dropped, none ran to completion.
        assert_eq!(alive.load(Ordering::SeqCst), 0);

        struct Guard(Arc<AtomicUsize>);
        impl Drop for Guard {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::SeqCst);
            }
        }
    }
}
