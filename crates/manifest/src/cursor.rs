//! Seam-owned walk machinery: a format's raw byte-key cursor lifted into the
//! ordered path walk.

use alloc::vec::Vec;
use core::cmp::Ordering;
use core::future::Future;
use core::ops::Bound;

use nectar_marker::{MaybeSend, MaybeSync};
use nectar_primitives::chunk::{ChunkRef, Reference};

use crate::path::ManifestPath;
use crate::view::{ManifestCursor, MapEntry};

#[cfg(test)]
mod tests;

/// One raw walk step: the key bytes and the entry bound at them.
pub type RawItem<R> = (Vec<u8>, MapEntry<R>);

/// A format's raw ordered walk: byte keys with their entries, reserved keys
/// included; [`PathCursor`] applies the shared key law on top.
pub trait RawCursor<R: Reference = ChunkRef>: MaybeSend {
    /// Error type a walk fails with.
    type Error: core::error::Error + MaybeSend + MaybeSync + 'static;

    /// The next `(key bytes, entry)` in byte order.
    fn next(&mut self)
    -> impl Future<Output = Result<Option<RawItem<R>>, Self::Error>> + MaybeSend;
}

/// The seam's [`ManifestCursor`] over any [`RawCursor`]: skips reserved keys,
/// filters to bounds, and yields owned paths. A format with a native seek
/// hands [`new`](Self::new) a pre-bounded raw walk; one without hands
/// [`bounded`](Self::bounded) a full walk.
#[derive(Debug)]
pub struct PathCursor<C> {
    raw: C,
    bounds: (Bound<Vec<u8>>, Bound<Vec<u8>>),
}

impl<C> PathCursor<C> {
    /// A walk over every key `raw` yields.
    pub const fn new(raw: C) -> Self {
        Self {
            raw,
            bounds: (Bound::Unbounded, Bound::Unbounded),
        }
    }

    /// A walk filtered to `bounds`; a key past the upper bound ends it, so
    /// the raw walk is never drained past the range.
    pub fn bounded(raw: C, bounds: (Bound<ManifestPath>, Bound<ManifestPath>)) -> Self {
        Self {
            raw,
            bounds: (
                bounds.0.map(ManifestPath::into_bytes),
                bounds.1.map(ManifestPath::into_bytes),
            ),
        }
    }
}

impl<C, R> ManifestCursor<R> for PathCursor<C>
where
    C: RawCursor<R>,
    R: Reference,
{
    type Error = C::Error;

    fn next(
        &mut self,
    ) -> impl Future<Output = Result<Option<(ManifestPath, MapEntry<R>)>, Self::Error>> + MaybeSend
    {
        let (start, end) = (&self.bounds.0, &self.bounds.1);
        let raw = &mut self.raw;
        async move {
            while let Some((key, entry)) = raw.next().await? {
                if outside(end, &key, Ordering::Greater) {
                    return Ok(None);
                }
                if outside(start, &key, Ordering::Less) || ManifestPath::is_reserved_bytes(&key) {
                    continue;
                }
                return Ok(Some((ManifestPath::new(key), entry)));
            }
            Ok(None)
        }
    }
}

/// Whether `key` falls outside `edge` on the side `out` names.
fn outside(edge: &Bound<Vec<u8>>, key: &[u8], out: Ordering) -> bool {
    match edge {
        Bound::Unbounded => false,
        Bound::Included(bound) => key.cmp(bound.as_slice()) == out,
        Bound::Excluded(bound) => key.cmp(bound.as_slice()) != out.reverse(),
    }
}
