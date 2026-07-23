//! Bounded handoff from the thread pool back to the polling future.

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};

use std::panic::{AssertUnwindSafe, catch_unwind};

use futures_channel::oneshot;

/// Receiving half of one submitted job; polled by the split engine.
pub(super) struct Handoff<T> {
    rx: oneshot::Receiver<T>,
}

impl<T> Handoff<T> {
    /// Ready with the reply, or `None` when the job finished without one:
    /// the job panicked, or the pool dropped it.
    pub(super) fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        Pin::new(&mut self.rx).poll(cx).map(Result::ok)
    }
}

/// Queue `job` on the pool, returning the handoff its reply arrives on.
///
/// Submission only enqueues, so neither building nor polling the caller's
/// future ever blocks on the pool. A panicking job is caught here and drops
/// its sender unsent, so the receiver sees a dropped job instead of a
/// process abort.
pub(super) fn submit<T, F>(job: F) -> Handoff<T>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    let (tx, rx) = oneshot::channel();
    rayon::spawn(move || {
        if let Ok(value) = catch_unwind(AssertUnwindSafe(job)) {
            drop(tx.send(value));
        }
    });
    Handoff { rx }
}

#[cfg(test)]
mod tests {
    use super::*;

    use core::task::Waker;
    use core::time::Duration;
    use std::sync::Arc;
    use std::task::Wake;
    use std::thread::{self, Thread};
    use std::time::Instant;

    /// Waker that unparks the thread which registered it.
    struct Unpark(Thread);

    impl Wake for Unpark {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }

    /// Drives `handoff` off wakes alone, panicking once `budget` is spent so a
    /// lost wake surfaces as a fast diagnostic rather than a hang.
    fn recv_before<T>(mut handoff: Handoff<T>, budget: Duration) -> Option<T> {
        let waker = Waker::from(Arc::new(Unpark(thread::current())));
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
            let (tx, rx) = oneshot::channel::<u32>();
            let sender = thread::spawn(move || drop(tx));
            assert_eq!(recv_before(Handoff { rx }, Duration::from_secs(10)), None);
            sender.join().unwrap();
        }
    }

    /// A value sent before the drop is delivered, never read as a drop.
    #[test]
    fn a_sender_carrying_a_value_delivers_it() {
        for _ in 0..1_000 {
            let (tx, rx) = oneshot::channel::<u32>();
            let sender = thread::spawn(move || tx.send(7).unwrap());
            assert_eq!(
                recv_before(Handoff { rx }, Duration::from_secs(10)),
                Some(7)
            );
            sender.join().unwrap();
        }
    }

    /// The pool path end to end: a panicking job reads as a drop.
    #[test]
    fn a_panicking_job_reads_as_a_drop() {
        let handoff = submit(|| panic!("job panicked"));
        assert_eq!(recv_before::<u32>(handoff, Duration::from_secs(10)), None);
    }
}
