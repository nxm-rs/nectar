//! Bounded set of in-flight futures, polled without a per-future task node.

use alloc::boxed::Box;
use alloc::vec::Vec;
use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};

/// Boxed future held in the set: `Send` on multi-threaded targets, unbounded
/// on wasm32 and under the `unsync` feature.
#[cfg(multi_thread)]
pub(crate) type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;
/// Boxed future held in the set: `Send` on multi-threaded targets, unbounded
/// on wasm32 and under the `unsync` feature.
#[cfg(not(multi_thread))]
pub(crate) type BoxFuture<T> = Pin<Box<dyn Future<Output = T>>>;

/// A fixed-membership set of outstanding futures.
///
/// Concurrency is capped by the caller's window, so the slot vector grows to
/// the peak once and is then reused: a completed future vacates its slot and
/// the next admission refills it, so the set holds no per-future task node.
/// One boxed future per admission remains, because a store future borrows its
/// store and is not nameable as a slab element.
///
/// [`poll`](Self::poll) scans the live slots and returns one completion per
/// call, so a caller drives it in a loop; before it reports `Pending` every
/// outstanding future has been polled with the current context, so no wakeup
/// is lost.
pub(crate) struct InFlight<T> {
    slots: Vec<Option<BoxFuture<T>>>,
    live: usize,
}

impl<T> InFlight<T> {
    /// An empty set.
    pub(crate) const fn new() -> Self {
        Self {
            slots: Vec::new(),
            live: 0,
        }
    }

    /// Futures currently outstanding.
    pub(crate) const fn len(&self) -> usize {
        self.live
    }

    /// Whether no future is outstanding.
    pub(crate) const fn is_empty(&self) -> bool {
        self.live == 0
    }

    /// Admit one future, reusing a vacated slot before growing the vector.
    pub(crate) fn push(&mut self, future: BoxFuture<T>) {
        self.live = self.live.saturating_add(1);
        for slot in &mut self.slots {
            if slot.is_none() {
                *slot = Some(future);
                return;
            }
        }
        self.slots.push(Some(future));
    }

    /// Poll for one completion: `Ready(Some(_))` retires the first ready
    /// future, `Ready(None)` reports the set empty, and `Pending` means every
    /// outstanding future is pending with the current context registered.
    pub(crate) fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        if self.live == 0 {
            return Poll::Ready(None);
        }
        for slot in &mut self.slots {
            let Some(future) = slot else { continue };
            if let Poll::Ready(output) = future.as_mut().poll(cx) {
                *slot = None;
                self.live = self.live.saturating_sub(1);
                return Poll::Ready(Some(output));
            }
        }
        Poll::Pending
    }
}
