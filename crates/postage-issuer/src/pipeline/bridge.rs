//! Blocking-bridge machinery: the sink spawner the
//! [`Stamped`](super::Stamped) iterator drives the poll-native sink with,
//! parked between completions by the shared unpark waker.

use core::task::Context;

use nectar_tasks::{BoxFuture, Spawn, TaskHandle};
use std::thread;

/// Waker for the calling thread; a completion unparks it.
pub(super) use nectar_tasks::unpark_current;

/// Runs one sign job to completion; a pending poll parks the running thread
/// until the job wakes it, so a job must not await work the pool schedules.
fn drive(mut task: BoxFuture<'static, ()>) {
    let waker = unpark_current();
    let mut cx = Context::from_waker(&waker);
    while task.as_mut().poll(&mut cx).is_pending() {
        thread::park();
    }
}

/// The blocking iterator's spawner: signs on the rayon pool.
#[cfg(feature = "parallel")]
pub(super) struct BlockingSpawn;

#[cfg(feature = "parallel")]
impl Spawn for BlockingSpawn {
    fn spawn(&self, task: BoxFuture<'static, ()>) -> TaskHandle {
        rayon::spawn(move || drive(task));
        TaskHandle::new(|| {})
    }
}

#[cfg(not(feature = "parallel"))]
use alloc::collections::VecDeque;
#[cfg(not(feature = "parallel"))]
use alloc::sync::Arc;
#[cfg(not(feature = "parallel"))]
use std::sync::{Mutex, PoisonError};

/// Queued sign jobs the bridge runs inline, one per pending poll, so `next`
/// blocks for at most one signer round-trip.
#[cfg(not(feature = "parallel"))]
#[derive(Clone, Default)]
pub(super) struct Jobs(Arc<Mutex<VecDeque<BoxFuture<'static, ()>>>>);

#[cfg(not(feature = "parallel"))]
impl Jobs {
    /// Runs one queued job; reports whether one was queued.
    pub(super) fn run_one(&self) -> bool {
        let job = self
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop_front();
        job.map(drive).is_some()
    }
}

/// The blocking iterator's spawner: queues each sign job on [`Jobs`] for
/// the bridge to run inline.
#[cfg(not(feature = "parallel"))]
#[derive(Default)]
pub(super) struct BlockingSpawn(Jobs);

#[cfg(not(feature = "parallel"))]
impl BlockingSpawn {
    /// The queue this spawner feeds.
    pub(super) fn jobs(&self) -> Jobs {
        self.0.clone()
    }
}

#[cfg(not(feature = "parallel"))]
impl Spawn for BlockingSpawn {
    fn spawn(&self, task: BoxFuture<'static, ()>) -> TaskHandle {
        self.0
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push_back(task);
        TaskHandle::new(|| {})
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::sync::Arc;
    use core::future::poll_fn;
    use core::sync::atomic::{AtomicUsize, Ordering};
    use core::task::Poll;
    use std::sync::{Mutex, PoisonError};
    use std::thread;
    use std::time::{Duration, Instant};

    /// The first poll registers a wake and parks the job; the second reports
    /// ready once the job is woken, so a pended job is retried rather than
    /// dropped.
    #[test]
    fn a_pended_job_is_polled_to_completion() {
        let wakers = Arc::new(Mutex::new(None));
        let polls = Arc::new(AtomicUsize::new(0));
        let job = {
            let wakers = Arc::clone(&wakers);
            let polls = Arc::clone(&polls);
            Box::pin(poll_fn(move |cx| {
                if polls.fetch_add(1, Ordering::SeqCst) == 0 {
                    *wakers.lock().unwrap_or_else(PoisonError::into_inner) =
                        Some(cx.waker().clone());
                    Poll::Pending
                } else {
                    Poll::Ready(())
                }
            }))
        };
        let driver = thread::spawn(move || drive(job));

        let deadline = Instant::now() + Duration::from_secs(10);
        let mut woken = None;
        while woken.is_none() && Instant::now() < deadline {
            woken = wakers
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone();
            thread::yield_now();
        }
        assert!(woken.is_some(), "the job never registered a wake");
        woken.unwrap().wake();

        driver.join().unwrap();
        assert_eq!(polls.load(Ordering::SeqCst), 2, "the job ran past its pend");
    }
}
