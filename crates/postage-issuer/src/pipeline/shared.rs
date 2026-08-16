//! The clone-shared state cell behind the put decorators: a mutex under
//! threads, a cell wherever the Send/Sync bounds relax.

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

/// Registers `waker` unless an equivalent one is already parked.
pub(super) fn park(wakers: &mut Vec<Waker>, waker: &Waker) {
    if !wakers.iter().any(|parked| parked.will_wake(waker)) {
        wakers.push(waker.clone());
    }
}
