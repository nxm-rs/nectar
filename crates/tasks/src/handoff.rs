//! Bounded handoff from a spawned job back to the polling future.

use core::fmt;
use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};

#[cfg(feature = "rayon")]
use std::panic::{AssertUnwindSafe, catch_unwind};

use alloc::boxed::Box;

use futures_channel::oneshot::{self, Canceled};
use futures_util::FutureExt;
use futures_util::future::Map;
use nectar_marker::MaybeSend;

use crate::{Spawn, TaskHandle};

/// The oneshot resolves to the reply, or to `None` when the sender dropped
/// unsent: a caught panic, an aborted task, or a dropped job.
type Recovered<T> = Map<oneshot::Receiver<T>, fn(Result<T, Canceled>) -> Option<T>>;

/// Receiving half of one submitted job; polled by the caller's future.
///
/// A [`submit_on`] handoff owns the job's abort handle, so dropping it before
/// the reply arrives aborts the task.
pub struct Handoff<T> {
    inner: Recovered<T>,
    _guard: Option<TaskHandle>,
}

impl<T> fmt::Debug for Handoff<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Handoff").finish_non_exhaustive()
    }
}

impl<T> Handoff<T> {
    /// Wrap `rx` in the reply-or-`None` map, holding `guard` until drop.
    fn over(rx: oneshot::Receiver<T>, guard: Option<TaskHandle>) -> Self {
        let recover: fn(Result<T, Canceled>) -> Option<T> = Result::ok;
        Self {
            inner: rx.map(recover),
            _guard: guard,
        }
    }

    /// Ready with the reply, or `None` when the job finished without one.
    ///
    /// A thin delegate for poll-native callers; equivalent to polling the
    /// handoff as a [`Future`].
    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        Pin::new(&mut self.inner).poll(cx)
    }
}

impl<T> Future for Handoff<T> {
    type Output = Option<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        self.get_mut().poll_recv(cx)
    }
}

/// Queue `job` on the pool, returning the handoff its reply arrives on.
///
/// Submission only enqueues, so neither building nor polling the caller's
/// future ever blocks on the pool. A panicking job is caught here and drops
/// its sender unsent, so the receiver sees a dropped job instead of a
/// process abort.
#[cfg(feature = "rayon")]
pub fn submit<T, F>(job: F) -> Handoff<T>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    let (tx, rx) = oneshot::channel();
    // reinvention: the pool submit that owns this pairing; the reply rides the oneshot.
    rayon::spawn(move || {
        // reinvention: panic boundary; an unwinding worker drops the reply unsent instead of aborting.
        if let Ok(value) = catch_unwind(AssertUnwindSafe(job)) {
            drop(tx.send(value));
        }
    });
    Handoff::over(rx, None)
}

/// Spawn `job` on `spawner`, returning the handoff its output arrives on.
///
/// The `Spawn`-generic sibling of [`submit`]: the returned handoff holds the
/// task's abort handle, so dropping it before the job resolves aborts the
/// task, and that drop then reads as `None`.
pub fn submit_on<S, F, T>(spawner: &S, job: F) -> Handoff<T>
where
    S: Spawn,
    F: Future<Output = T> + MaybeSend + 'static,
    T: MaybeSend + 'static,
{
    let (tx, rx) = oneshot::channel();
    let handle = spawner.spawn(Box::pin(async move {
        drop(tx.send(job.await));
    }));
    Handoff::over(rx, Some(handle))
}

#[cfg(test)]
mod tests {
    use super::*;

    use alloc::sync::Arc;
    use core::sync::atomic::{AtomicU32, Ordering};
    use core::time::Duration;
    use std::thread;
    use std::time::Instant;

    /// Drives `handoff` off wakes alone, panicking once `budget` is spent so a
    /// lost wake surfaces as a fast diagnostic rather than a hang.
    fn recv_before<T>(mut handoff: Handoff<T>, budget: Duration) -> Option<T> {
        let waker = crate::unpark_current();
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
            // reinvention: test driver; the manual-poll loop parks to yield the thread.
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
            assert_eq!(
                recv_before(Handoff::over(rx, None), Duration::from_secs(10)),
                None
            );
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
                recv_before(Handoff::over(rx, None), Duration::from_secs(10)),
                Some(7)
            );
            sender.join().unwrap();
        }
    }

    /// The pool path end to end: a panicking job reads as a drop.
    #[cfg(feature = "rayon")]
    #[test]
    fn a_panicking_job_reads_as_a_drop() {
        let handoff = submit(|| panic!("job panicked"));
        assert_eq!(recv_before::<u32>(handoff, Duration::from_secs(10)), None);
    }

    /// A handoff resolves through its `Future` impl as it does through
    /// [`Handoff::poll_recv`].
    #[test]
    fn drives_as_a_future() {
        let (tx, rx) = oneshot::channel::<u32>();
        tx.send(9).unwrap();
        let mut handoff = Handoff::over(rx, None);
        let waker = crate::unpark_current();
        let mut cx = Context::from_waker(&waker);
        assert_eq!(Pin::new(&mut handoff).poll(&mut cx), Poll::Ready(Some(9)));
    }

    /// Spawner that never runs the task and records its abort.
    struct RecordingSpawner {
        aborts: Arc<AtomicU32>,
    }

    impl Spawn for RecordingSpawner {
        fn spawn(&self, task: crate::BoxFuture<'static, ()>) -> TaskHandle {
            drop(task);
            let aborts = Arc::clone(&self.aborts);
            TaskHandle::new(move || {
                aborts.fetch_add(1, Ordering::Relaxed);
            })
        }
    }

    /// Spawner that polls the task once; a ready future completes inline.
    struct InlineSpawner;

    impl Spawn for InlineSpawner {
        fn spawn(&self, mut task: crate::BoxFuture<'static, ()>) -> TaskHandle {
            let waker = crate::unpark_current();
            let mut cx = Context::from_waker(&waker);
            let _ = task.as_mut().poll(&mut cx);
            TaskHandle::new(|| {})
        }
    }

    /// `submit_on` delivers the spawned job's output on its handoff.
    #[test]
    fn submit_on_delivers_the_job_output() {
        let handoff = submit_on(&InlineSpawner, async { 4_u32 });
        assert_eq!(recv_before(handoff, Duration::from_secs(10)), Some(4));
    }

    /// Dropping the handoff aborts the task through the captured handle.
    #[test]
    fn dropping_a_submit_on_handoff_aborts_the_task() {
        let aborts = Arc::new(AtomicU32::new(0));
        let spawner = RecordingSpawner {
            aborts: Arc::clone(&aborts),
        };
        let handoff = submit_on(&spawner, async { 1_u32 });
        assert_eq!(aborts.load(Ordering::Relaxed), 0);
        drop(handoff);
        assert_eq!(aborts.load(Ordering::Relaxed), 1);
    }
}
