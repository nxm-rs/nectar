//! Dependency spike for the streaming file pipeline's no_std base.
//!
//! Exercises `futures-util` (alloc only) and `bytes` (default features off)
//! so the bare-metal and wasm CI lanes prove the dependency base compiles,
//! and the host test lane proves it behaves under manual polling.

#![cfg_attr(not(test), warn(unused_crate_dependencies))]
#![no_std]
#![cfg_attr(docsrs, feature(doc_cfg))]
// Test code may freely unwrap/index/panic; the runtime-safety restriction
// lints target production code paths.
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::get_unwrap,
        clippy::indexing_slicing,
        clippy::string_slice,
        clippy::arithmetic_side_effects,
        clippy::panic,
        clippy::unreachable,
        clippy::panic_in_result_fn
    )
)]

extern crate alloc;

use alloc::vec::Vec;
use core::{
    num::NonZeroUsize,
    pin::pin,
    task::{Context, Poll, Waker},
};

use bytes::{BufMut, Bytes, BytesMut};
use futures_util::stream::{self, Stream, StreamExt};

/// Reassembles frames into one contiguous buffer, in iteration order.
pub fn reassemble<'a, I>(frames: I) -> Bytes
where
    I: IntoIterator<Item = &'a [u8]>,
{
    let mut buf = BytesMut::new();
    for frame in frames {
        buf.put_slice(frame);
    }
    buf.freeze()
}

/// Drains an item source through an alloc-backed stream combinator by
/// polling with a no-op waker; no runtime is involved.
pub fn chunked<I>(items: I, width: NonZeroUsize) -> Vec<Vec<I::Item>>
where
    I: IntoIterator,
{
    let mut cx = Context::from_waker(Waker::noop());
    let mut chunks = pin!(stream::iter(items).chunks(width.get()));
    let mut out = Vec::new();
    while let Poll::Ready(Some(chunk)) = chunks.as_mut().poll_next(&mut cx) {
        out.push(chunk);
    }
    out
}

#[cfg(test)]
mod tests {
    use alloc::vec;

    use super::*;

    #[test]
    fn reassemble_concatenates_in_order() {
        let joined = reassemble([b"swarm".as_slice(), b" ", b"chunks"]);
        assert_eq!(joined.as_ref(), b"swarm chunks");
    }

    #[test]
    fn reassemble_of_nothing_is_empty() {
        assert!(reassemble([]).is_empty());
    }

    #[test]
    fn chunked_preserves_order_with_ragged_tail() {
        let width = NonZeroUsize::new(2).unwrap();
        assert_eq!(chunked(1..=5, width), vec![vec![1, 2], vec![3, 4], vec![5]]);
    }
}
