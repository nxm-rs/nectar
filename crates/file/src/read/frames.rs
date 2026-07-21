//! Completion-order frames drain over one walk.

use core::fmt;
use core::ops::Range;
use core::pin::Pin;
use core::task::{Context, Poll};

use futures_util::stream::Stream;
use nectar_primitives::DEFAULT_BODY_SIZE;
use nectar_primitives::chunk::AnyChunkSet;
use nectar_primitives::store::TrustedGet;

use crate::walk::{Frame, Walk, WalkError, WalkMode, WalkStats};

/// Offset-tagged frames of one clipped range in completion order: the
/// frames tile the range exactly once, in no particular order.
pub struct FileFrames<S, M, const B: usize = DEFAULT_BODY_SIZE>
where
    S: TrustedGet<AnyChunkSet<B>>,
    M: WalkMode,
{
    walk: Walk<S, M, B>,
}

impl<S, M, const B: usize> FileFrames<S, M, B>
where
    S: TrustedGet<AnyChunkSet<B>> + Clone + 'static,
    M: WalkMode,
{
    pub(super) const fn new(walk: Walk<S, M, B>) -> Self {
        Self { walk }
    }

    /// Clipped absolute byte range the frames tile.
    pub const fn range(&self) -> Range<u64> {
        self.walk.range()
    }

    /// Occupancy witnesses of the underlying walk.
    pub const fn stats(&self) -> WalkStats {
        self.walk.stats()
    }
}

/// Movable regardless of the store or context types: the drain owns plain
/// state and boxed futures, never a self-reference.
impl<S, M, const B: usize> Unpin for FileFrames<S, M, B>
where
    S: TrustedGet<AnyChunkSet<B>>,
    M: WalkMode,
{
}

impl<S, M, const B: usize> Stream for FileFrames<S, M, B>
where
    S: TrustedGet<AnyChunkSet<B>> + Clone + 'static,
    M: WalkMode,
{
    type Item = Result<Frame, WalkError<S::Error>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().walk.poll_next_any(cx)
    }
}

impl<S, M, const B: usize> fmt::Debug for FileFrames<S, M, B>
where
    S: TrustedGet<AnyChunkSet<B>>,
    M: WalkMode,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileFrames")
            .field("walk", &self.walk)
            .finish_non_exhaustive()
    }
}
