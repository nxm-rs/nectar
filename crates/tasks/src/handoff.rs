//! Oneshot pool-to-poll handoff: one job's result crosses from a worker
//! closure back into a poll future.

use alloc::sync::Arc;
use core::fmt;
use core::task::{Context, Poll, Waker};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Mutex, MutexGuard, PoisonError};

/// One job's completion cell: at most one value, plus the latest waker.
struct Slot<T> {
    state: Mutex<State<T>>,
}

struct State<T> {
    value: Option<T>,
    /// Sender gone; no further value can arrive.
    closed: bool,
    waker: Option<Waker>,
}

fn lock<T>(slot: &Slot<T>) -> MutexGuard<'_, State<T>> {
    slot.state.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Creates a connected [`HandoffSender`] and [`Handoff`] for one value.
pub fn handoff<T>() -> (HandoffSender<T>, Handoff<T>) {
    let slot = Arc::new(Slot {
        state: Mutex::new(State {
            value: None,
            closed: false,
            waker: None,
        }),
    });
    (
        HandoffSender {
            slot: Arc::clone(&slot),
        },
        Handoff { slot },
    )
}

/// Sending half: delivers at most one value. Dropping it without a value
/// closes the handoff, so the receiver reads a dropped job.
pub struct HandoffSender<T> {
    slot: Arc<Slot<T>>,
}

impl<T> HandoffSender<T> {
    /// Delivers `value`; the drop that follows wakes the receiver.
    pub fn complete(self, value: T) {
        lock(&self.slot).value = Some(value);
    }

    /// Runs `job` and delivers its value; a caught panic reads as a dropped
    /// job. The job's interior state across a caught panic is the
    /// submitter's contract.
    pub fn run<F: FnOnce() -> T>(self, job: F) {
        if let Ok(value) = catch_unwind(AssertUnwindSafe(job)) {
            self.complete(value);
        }
    }
}

impl<T> Drop for HandoffSender<T> {
    fn drop(&mut self) {
        let waker = {
            let mut state = lock(&self.slot);
            state.closed = true;
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

impl<T> fmt::Debug for HandoffSender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HandoffSender").finish_non_exhaustive()
    }
}

/// Receiving half of one submitted job; polled by the collecting future.
///
/// The waker registration is overwritten on every poll: the first poll may
/// carry a noop waker, and only the latest waker is entitled to the wakeup.
pub struct Handoff<T> {
    slot: Arc<Slot<T>>,
}

impl<T> Handoff<T> {
    /// Ready with the value, or `None` when the job finished without one:
    /// the job panicked, or the pool dropped it.
    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        let mut state = lock(&self.slot);
        if let Some(value) = state.value.take() {
            return Poll::Ready(Some(value));
        }
        if state.closed {
            return Poll::Ready(None);
        }
        state.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl<T> fmt::Debug for Handoff<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Handoff").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use core::time::Duration;
    use std::sync::mpsc;
    use std::task::Wake;
    use std::thread;
    use std::time::Instant;

    use crate::unpark_waker;

    /// Drives `handoff` off wakes alone, panicking once `budget` is spent so
    /// a lost wake surfaces as a fast diagnostic rather than a hang.
    fn recv_before<T>(mut handoff: Handoff<T>, budget: Duration) -> Option<T> {
        let waker = unpark_waker();
        let mut cx = Context::from_waker(&waker);
        let start = Instant::now();
        loop {
            assert!(
                start.elapsed() < budget,
                "lost wake: handoff still pending after {budget:?}"
            );
            if let Poll::Ready(value) = handoff.poll_recv(&mut cx) {
                return value;
            }
            thread::park_timeout(budget.saturating_sub(start.elapsed()));
        }
    }

    /// A sender dropped unsent must always wake the receiver, whatever the
    /// interleaving against the poll that registered the waker.
    #[test]
    fn a_sender_dropped_without_a_value_always_wakes_the_receiver() {
        for _ in 0..1_000 {
            let (tx, rx) = handoff::<u32>();
            let sender = thread::spawn(move || drop(tx));
            assert_eq!(recv_before(rx, Duration::from_secs(10)), None);
            sender.join().unwrap();
        }
    }

    /// A value delivered before the drop arrives, never read as a drop.
    #[test]
    fn a_sender_carrying_a_value_delivers_it() {
        for _ in 0..1_000 {
            let (tx, rx) = handoff::<u32>();
            let sender = thread::spawn(move || tx.complete(7));
            assert_eq!(recv_before(rx, Duration::from_secs(10)), Some(7));
            sender.join().unwrap();
        }
    }

    /// The runner catches the panic: the receiver reads a drop, the worker
    /// thread survives.
    #[test]
    fn a_panicking_job_reads_as_a_drop() {
        let (tx, rx) = handoff::<u32>();
        let worker = thread::spawn(move || tx.run(|| panic!("job panicked")));
        assert_eq!(recv_before(rx, Duration::from_secs(10)), None);
        worker.join().unwrap();
    }

    /// Signals each wake over a channel.
    struct SignalWaker(mpsc::Sender<()>);

    impl Wake for SignalWaker {
        fn wake(self: Arc<Self>) {
            let _ = self.0.send(());
        }
    }

    /// The completion wakes the latest registered waker, not the first.
    #[test]
    fn completion_wakes_the_latest_registration_not_first() {
        let (tx, mut rx) = handoff::<u32>();
        assert!(
            rx.poll_recv(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );

        let (wake_tx, wake_rx) = mpsc::channel();
        let waker = Waker::from(Arc::new(SignalWaker(wake_tx)));
        assert!(rx.poll_recv(&mut Context::from_waker(&waker)).is_pending());

        tx.complete(9);
        wake_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("completion must wake the latest registered waker");
        assert_eq!(
            rx.poll_recv(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(Some(9))
        );
    }
}
