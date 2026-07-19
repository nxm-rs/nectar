//! Bounded handoff from the thread pool back to the polling future.

use core::task::{Context, Poll};

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex, PoisonError};

use futures_util::task::AtomicWaker;

/// Reply slot shared between one pool job and its receiver.
struct Slot<T> {
    value: Mutex<Option<T>>,
    waker: AtomicWaker,
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
        if Arc::strong_count(&self.slot) == 1 {
            // The reply half is gone, but it may have written its value
            // before dropping; re-check so that ordering never turns a
            // delivered reply into a spurious drop.
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
        self.slot.waker.wake();
    }
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
    let slot = Arc::new(Slot {
        value: Mutex::new(None),
        waker: AtomicWaker::new(),
    });
    let reply = Reply {
        slot: Arc::clone(&slot),
    };
    rayon::spawn(move || {
        if let Ok(value) = catch_unwind(AssertUnwindSafe(job)) {
            *reply.slot.value() = Some(value);
        }
        drop(reply);
    });
    Handoff { slot }
}
