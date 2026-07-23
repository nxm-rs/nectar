//! Bounded handoff from the thread pool back to the polling future.

use core::sync::atomic::{AtomicBool, Ordering};
use core::task::{Context, Poll};

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex, PoisonError};

use futures_util::task::AtomicWaker;

/// Reply slot shared between one pool job and its receiver.
struct Slot<T> {
    value: Mutex<Option<T>>,
    waker: AtomicWaker,
    /// Raised by the reply half before it wakes, so a receiver that reads it
    /// unset is guaranteed a later wake.
    done: AtomicBool,
}

impl<T> Slot<T> {
    /// The slot contents, riding out lock poisoning: the slot holds plain
    /// data, so a poisoned lock leaves it intact.
    fn value(&self) -> std::sync::MutexGuard<'_, Option<T>> {
        self.value.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Receiving half of one submitted job; polled by the split engine.
pub(super) struct Handoff<T> {
    slot: Arc<Slot<T>>,
}

impl<T> Handoff<T> {
    /// Ready with the reply, or `None` when the job finished without one:
    /// the job panicked, or the pool dropped it.
    ///
    /// The waker is registered before the slot is checked, so a reply
    /// landing between the two is never missed.
    pub(super) fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        self.slot.waker.register(cx.waker());
        if let Some(value) = self.slot.value().take() {
            return Poll::Ready(Some(value));
        }
        if self.slot.done.load(Ordering::Acquire) {
            // The reply half is gone, but it may have written its value
            // between the take above and this load; re-check so that
            // ordering never turns a delivered reply into a spurious drop.
            return Poll::Ready(self.slot.value().take());
        }
        Poll::Pending
    }
}

/// Sending half held by the pool job; waking on drop covers both delivery
/// and a job that finished without replying.
struct Reply<T> {
    slot: Arc<Slot<T>>,
}

impl<T> Drop for Reply<T> {
    fn drop(&mut self) {
        self.slot.done.store(true, Ordering::Release);
        self.slot.waker.wake();
    }
}

/// Both halves of one fresh slot.
fn pair<T>() -> (Handoff<T>, Reply<T>) {
    let slot = Arc::new(Slot {
        value: Mutex::new(None),
        waker: AtomicWaker::new(),
        done: AtomicBool::new(false),
    });
    let reply = Reply {
        slot: Arc::clone(&slot),
    };
    (Handoff { slot }, reply)
}

/// Queue `job` on the pool, returning the handoff its reply arrives on.
///
/// Submission only enqueues, so neither building nor polling the caller's
/// future ever blocks on the pool. A panicking job is caught here and leaves
/// its slot empty, so the receiver sees a dropped job instead of a process
/// abort.
pub(super) fn submit<T, F>(job: F) -> Handoff<T>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    let (handoff, reply) = pair();
    rayon::spawn(move || {
        if let Ok(value) = catch_unwind(AssertUnwindSafe(job)) {
            *reply.slot.value() = Some(value);
        }
        drop(reply);
    });
    handoff
}

#[cfg(test)]
mod tests {
    use super::*;

    use core::task::Waker;
    use core::time::Duration;
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

    /// Waking after the completion flag is raised is what keeps a receiver that
    /// parks mid-drop from sleeping for good, so hammer that interleaving.
    #[test]
    fn a_reply_dropped_without_a_value_always_wakes_the_receiver() {
        for _ in 0..1_000 {
            let (handoff, reply) = pair::<u32>();
            let sender = thread::spawn(move || drop(reply));
            assert_eq!(recv_before(handoff, Duration::from_secs(10)), None);
            sender.join().unwrap();
        }
    }

    /// A value stored before the drop is delivered, never read as a drop.
    #[test]
    fn a_reply_carrying_a_value_delivers_it() {
        for _ in 0..1_000 {
            let (handoff, reply) = pair::<u32>();
            let sender = thread::spawn(move || {
                *reply.slot.value() = Some(7);
                drop(reply);
            });
            assert_eq!(recv_before(handoff, Duration::from_secs(10)), Some(7));
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
