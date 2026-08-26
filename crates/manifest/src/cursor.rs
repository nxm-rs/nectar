//! Seam-owned walk machinery: a format's raw byte-key cursor lifted into the
//! ordered path walk.

use alloc::vec::Vec;
use core::cmp::Ordering;
use core::ops::Bound;
use core::pin::Pin;
use core::task::{Context, Poll};

use futures_util::Stream;
use nectar_marker::{MaybeSend, MaybeSync};
use nectar_primitives::chunk::{ChunkRef, Reference};

use crate::path::ManifestPath;
use crate::view::{ManifestCursor, MapEntry};

#[cfg(test)]
mod tests;

/// One raw walk step: the key bytes and the entry bound at them.
pub type RawItem<R> = (Vec<u8>, MapEntry<R>);

/// A format's raw ordered walk, as a [`Stream`] of `(key bytes, entry)` in
/// byte order, reserved keys included; [`PathCursor`] applies the shared key
/// law on top.
pub trait RawCursor<R: Reference = ChunkRef>:
    MaybeSend + Unpin + Stream<Item = Result<RawItem<R>, Self::Error>>
{
    /// Error type a walk fails with.
    type Error: core::error::Error + MaybeSend + MaybeSync + 'static;
}

/// The seam's [`ManifestCursor`] over any [`RawCursor`]: skips reserved keys,
/// filters to bounds, and yields owned paths. A format with a native seek
/// hands [`new`](Self::new) a pre-bounded raw walk; one without hands
/// [`bounded`](Self::bounded) a full walk.
#[derive(Debug)]
pub struct PathCursor<C, R: Reference = ChunkRef> {
    raw: C,
    bounds: (Bound<Vec<u8>>, Bound<Vec<u8>>),
    reference: core::marker::PhantomData<R>,
}

impl<C, R: Reference> PathCursor<C, R> {
    /// A walk over every key `raw` yields.
    pub const fn new(raw: C) -> Self
    where
        C: RawCursor<R>,
    {
        Self {
            raw,
            bounds: (Bound::Unbounded, Bound::Unbounded),
            reference: core::marker::PhantomData,
        }
    }

    /// A walk filtered to `bounds`; a key past the upper bound ends it, so
    /// the raw walk is never drained past the range.
    pub fn bounded(raw: C, bounds: (Bound<ManifestPath>, Bound<ManifestPath>)) -> Self
    where
        C: RawCursor<R>,
    {
        Self {
            raw,
            bounds: (
                bounds.0.map(ManifestPath::into_bytes),
                bounds.1.map(ManifestPath::into_bytes),
            ),
            reference: core::marker::PhantomData,
        }
    }
}

impl<R, C> Stream for PathCursor<C, R>
where
    R: Reference + Unpin,
    C: RawCursor<R>,
{
    type Item = Result<(ManifestPath, MapEntry<R>), C::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            match Pin::new(&mut this.raw).poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Ready(Some(Err(error))) => return Poll::Ready(Some(Err(error))),
                Poll::Ready(Some(Ok((key, entry)))) => {
                    if outside(&this.bounds.1, &key, Ordering::Greater) {
                        return Poll::Ready(None);
                    }
                    if outside(&this.bounds.0, &key, Ordering::Less)
                        || ManifestPath::is_reserved_bytes(&key)
                    {
                        continue;
                    }
                    return Poll::Ready(Some(Ok((ManifestPath::new(key), entry))));
                }
            }
        }
    }
}

impl<C, R> ManifestCursor<R> for PathCursor<C, R>
where
    C: RawCursor<R>,
    R: Reference + Unpin,
{
    type Error = C::Error;
}

/// Whether `key` falls outside `edge` on the side `out` names.
fn outside(edge: &Bound<Vec<u8>>, key: &[u8], out: Ordering) -> bool {
    match edge {
        Bound::Unbounded => false,
        Bound::Included(bound) => key.cmp(bound.as_slice()) == out,
        Bound::Excluded(bound) => key.cmp(bound.as_slice()) != out.reverse(),
    }
}
