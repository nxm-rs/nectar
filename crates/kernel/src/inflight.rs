//! Bounded set of in-flight futures, polled without a per-future task node.

use core::fmt;
use core::task::{Context, Poll};

use alloc::vec::Vec;

use crate::future::BoxFuture;

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
///
/// The routing contract is payload-in-future: a future carries its own
/// context back in its output, so completions need no slot identity.
pub struct InFlight<T> {
    slots: Vec<Option<BoxFuture<T>>>,
    live: usize,
}

impl<T> InFlight<T> {
    /// An empty set.
    pub const fn new() -> Self {
        Self {
            slots: Vec::new(),
            live: 0,
        }
    }

    /// Futures currently outstanding.
    pub const fn len(&self) -> usize {
        self.live
    }

    /// Whether no future is outstanding.
    pub const fn is_empty(&self) -> bool {
        self.live == 0
    }

    /// Admit one future, reusing a vacated slot before growing the vector.
    pub fn push(&mut self, future: BoxFuture<T>) {
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
    pub fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
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

impl<T> Default for InFlight<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> fmt::Debug for InFlight<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InFlight")
            .field("live", &self.live)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::boxed::Box;
    use core::future::{pending, ready};
    use core::task::Waker;

    #[test]
    fn poll_contract_holds() {
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        let mut set: InFlight<u32> = InFlight::new();
        assert!(set.is_empty());
        assert_eq!(set.poll(&mut cx), Poll::Ready(None));

        set.push(Box::pin(ready(1)));
        set.push(Box::pin(pending()));
        set.push(Box::pin(ready(2)));
        assert_eq!(set.len(), 3);

        assert_eq!(set.poll(&mut cx), Poll::Ready(Some(1)));
        assert_eq!(set.poll(&mut cx), Poll::Ready(Some(2)));
        assert_eq!(set.poll(&mut cx), Poll::Pending);
        assert_eq!(set.len(), 1);

        // A vacated slot is reused before the vector grows.
        set.push(Box::pin(ready(3)));
        assert_eq!(set.slots.len(), 3);
        assert_eq!(set.poll(&mut cx), Poll::Ready(Some(3)));
    }
}
