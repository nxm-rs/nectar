//! Current-thread sync driver: park the calling thread between wakes.

use alloc::sync::Arc;
use core::future::Future;
use core::pin::pin;
use core::task::{Context, Poll, Waker};
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
pub fn unpark_waker() -> Waker {
    Waker::from(Arc::new(Unpark(thread::current())))
}

/// Drives `future` to completion on the calling thread, parking between
/// wakes.
pub fn block_on<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    let waker = unpark_waker();
    let mut cx = Context::from_waker(&waker);
    loop {
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(output) => return output,
            Poll::Pending => thread::park(),
        }
    }
}

#[cfg(test)]
mod tests {
    use core::future::poll_fn;
    use core::time::Duration;
    use std::thread;

    use super::block_on;
    use crate::handoff;

    #[test]
    fn a_ready_future_returns_without_parking() {
        assert_eq!(block_on(async { 7 }), 7);
    }

    #[test]
    fn a_cross_thread_wake_unparks_the_driver() {
        let (tx, mut rx) = handoff::<u32>();
        let worker = thread::spawn(move || {
            thread::sleep(Duration::from_millis(10));
            tx.complete(9);
        });
        assert_eq!(block_on(poll_fn(|cx| rx.poll_recv(cx))), Some(9));
        worker.join().unwrap();
    }
}
