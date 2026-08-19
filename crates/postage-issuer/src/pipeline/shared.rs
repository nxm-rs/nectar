//! The clone-shared state cell behind the put decorators: a mutex under
//! threads, a cell wherever the Send/Sync bounds relax. Hosted no-std builds
//! without `unsync` have neither, so the surface is absent there.

use alloc::vec::Vec;
use core::task::Waker;

#[cfg(multi_thread)]
use std::sync::{Mutex, PoisonError};

#[cfg(multi_thread)]
pub(super) type Shared<T> = alloc::sync::Arc<Mutex<T>>;
#[cfg(not(multi_thread))]
pub(super) type Shared<T> = alloc::rc::Rc<core::cell::RefCell<T>>;

#[cfg(multi_thread)]
pub(super) fn new_shared<T>(state: T) -> Shared<T> {
    alloc::sync::Arc::new(Mutex::new(state))
}

#[cfg(not(multi_thread))]
pub(super) fn new_shared<T>(state: T) -> Shared<T> {
    alloc::rc::Rc::new(core::cell::RefCell::new(state))
}

/// Runs `f` under the state lock; never held across an await.
#[cfg(multi_thread)]
pub(super) fn with_state<T, R>(shared: &Shared<T>, f: impl FnOnce(&mut T) -> R) -> R {
    f(&mut shared.lock().unwrap_or_else(PoisonError::into_inner))
}

/// Runs `f` under the state cell; never held across an await.
#[cfg(not(multi_thread))]
pub(super) fn with_state<T, R>(shared: &Shared<T>, f: impl FnOnce(&mut T) -> R) -> R {
    f(&mut shared.borrow_mut())
}

/// Wakes outside the lock, so a woken poller never blocks on the waker.
pub(super) fn wake_all(wakers: Vec<Waker>) {
    for waker in wakers {
        waker.wake();
    }
}

#[cfg(feature = "std")]
pub(super) fn park(wakers: &mut Vec<Waker>, waker: &Waker) {
    if !wakers.iter().any(|parked| parked.will_wake(waker)) {
        wakers.push(waker.clone());
    }
}

/// State whose pollers share one registration in the machinery below it.
#[cfg(feature = "std")]
pub(super) trait Parked {
    fn parked(&mut self) -> &mut Vec<Waker>;
}

/// Hands the parked wake on when a poller leaves, settled or cancelled: it may
/// hold the sole live registration, whose wake dies with it.
#[cfg(feature = "std")]
pub(super) struct Unpark<'a, T: Parked>(&'a Shared<T>);

#[cfg(feature = "std")]
impl<'a, T: Parked> Unpark<'a, T> {
    pub(super) const fn new(shared: &'a Shared<T>) -> Self {
        Self(shared)
    }
}

#[cfg(feature = "std")]
impl<T: Parked> Drop for Unpark<'_, T> {
    fn drop(&mut self) {
        wake_all(with_state(self.0, |state| core::mem::take(state.parked())));
    }
}
