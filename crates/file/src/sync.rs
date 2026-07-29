//! Ready-only driver for single-threaded guests.
//!
//! [`drive`] polls a future exactly once with a no-op waker: over a
//! synchronous store every await resolves inside that poll, and a
//! [`Pending`] return is terminal because nothing in the guest can wake the
//! future again.

use core::future::Future;
use core::pin::pin;
use core::task::{Context, Poll, Waker};

/// The driven future returned `Poll::Pending`; a Ready-only guest holds no
/// waker that could resume it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("future pended under the ready-only driver")]
pub struct Pending;

/// Run `future` to completion in a single poll.
///
/// Never re-polls after a `Pending` (no wake can arrive), so the driver
/// cannot livelock.
///
/// # Examples
///
/// A whole-file read over a synchronous store completes in the single poll:
///
/// ```
/// use nectar_file::sync::drive;
/// use nectar_file::{File, Policy};
/// use nectar_primitives::chunk::AnyChunkSet;
/// use nectar_primitives::store::{ContentGet, MemoryStore};
/// use std::sync::Arc;
///
/// let data = b"guest payload".to_vec();
/// let store = Arc::new(MemoryStore::<AnyChunkSet<4096>>::new());
/// let root = drive(File::<_, 4096>::new(Arc::clone(&store), Policy::DEFAULT).save(&data[..]))
///     .unwrap()
///     .unwrap();
/// let file = File::<_, 4096>::new(ContentGet::new(store), Policy::DEFAULT);
/// let bytes = drive(file.collect(root.into(), u64::MAX)).unwrap().unwrap();
/// assert_eq!(bytes, data);
/// ```
pub fn drive<F: Future>(future: F) -> Result<F::Output, Pending> {
    let future = pin!(future);
    let mut cx = Context::from_waker(Waker::noop());
    match future.poll(&mut cx) {
        Poll::Ready(output) => Ok(output),
        Poll::Pending => Err(Pending),
    }
}

#[cfg(test)]
mod tests {
    use std::vec::Vec;

    use nectar_testing::split_fixture;

    use super::*;
    use crate::handle::{File, Policy};

    #[test]
    fn ready_future_completes_in_one_poll() {
        assert_eq!(drive(async { 7u8 }), Ok(7));
    }

    #[test]
    fn pending_future_is_a_typed_error() {
        assert_eq!(drive(core::future::pending::<()>()), Err(Pending));
    }

    #[test]
    fn whole_file_read_is_ready_only_over_a_synchronous_store() {
        const TINY: usize = 256;
        let data: Vec<u8> = (0..(9 * TINY + 21) as u64)
            .map(|i| (i % 251) as u8)
            .collect();
        let (root, store) = split_fixture::<TINY>(&data);
        let file = File::<_, TINY>::new(store, Policy::DEFAULT);
        let bytes = drive(file.collect(root.into(), u64::MAX)).unwrap().unwrap();
        assert_eq!(bytes, data);
    }
}
