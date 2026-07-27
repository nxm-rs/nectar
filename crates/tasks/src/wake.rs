//! Thread-unpark waker for blocking bridges.

use alloc::sync::Arc;
use core::task::Waker;
use std::task::Wake;
use std::thread::{self, Thread};

/// Waker that unparks the thread which registered it.
struct Unpark(Thread);

impl Wake for Unpark {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

/// Waker for the calling thread; a wake unparks it.
#[must_use]
pub fn unpark_current() -> Waker {
    Waker::from(Arc::new(Unpark(thread::current())))
}
