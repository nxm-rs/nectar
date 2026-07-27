//! Blocking-bridge machinery: the sink spawner the
//! [`Stamped`](super::Stamped) iterator drives the poll-native sink with,
//! parked between completions by the shared unpark waker.

use core::task::{Context, Waker};

use nectar_tasks::{BoxFuture, Spawn, TaskHandle};

/// Waker for the calling thread; a completion unparks it.
pub(super) use nectar_tasks::unpark_current;

/// Runs one sign job to completion.
fn drive(mut task: BoxFuture<'static, ()>) {
    let mut cx = Context::from_waker(Waker::noop());
    let poll = task.as_mut().poll(&mut cx);
    // Sign jobs are single-poll futures.
    debug_assert!(poll.is_ready(), "sign job pended");
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
