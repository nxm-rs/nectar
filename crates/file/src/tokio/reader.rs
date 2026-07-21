//! Shim reader: the caller's polls drain the walk directly.

use core::fmt;
use core::pin::Pin;
use core::task::{Context, Poll};
use std::io;
use std::io::SeekFrom;

use nectar_primitives::DEFAULT_BODY_SIZE;
use nectar_primitives::chunk::AnyChunkSet;
use nectar_primitives::store::TrustedGet;
use tokio::io::{AsyncRead, AsyncSeek, ReadBuf};

use super::resolve;
use crate::read::FileReader;
use crate::walk::WalkMode;

/// [`AsyncRead`] plus [`AsyncSeek`] over one [`FileReader`].
///
/// Every poll drains the reader's walk in place, so the fetch window stays
/// in flight across polls and no future is created per call. Positions are
/// zero-based within the clipped range; a seek outside it is
/// `InvalidInput`, never a clamp.
pub struct TokioReader<S, M, const B: usize = DEFAULT_BODY_SIZE>
where
    S: TrustedGet<AnyChunkSet<B>>,
    M: WalkMode,
{
    inner: FileReader<S, M, B>,
}

impl<S, M, const B: usize> TokioReader<S, M, B>
where
    S: TrustedGet<AnyChunkSet<B>> + Clone + 'static,
    M: WalkMode,
{
    /// Current position within the clipped range.
    pub const fn position(&self) -> u64 {
        self.inner.position()
    }

    /// Bytes the clipped range covers.
    pub const fn effective_len(&self) -> u64 {
        self.inner.effective_len()
    }
}

impl<S, M, const B: usize> From<FileReader<S, M, B>> for TokioReader<S, M, B>
where
    S: TrustedGet<AnyChunkSet<B>>,
    M: WalkMode,
{
    fn from(inner: FileReader<S, M, B>) -> Self {
        Self { inner }
    }
}

impl<S, M, const B: usize> From<TokioReader<S, M, B>> for FileReader<S, M, B>
where
    S: TrustedGet<AnyChunkSet<B>>,
    M: WalkMode,
{
    fn from(reader: TokioReader<S, M, B>) -> Self {
        reader.inner
    }
}

/// Movable regardless of the store or context types: the shim owns plain
/// state and boxed futures, never a self-reference.
impl<S, M, const B: usize> Unpin for TokioReader<S, M, B>
where
    S: TrustedGet<AnyChunkSet<B>>,
    M: WalkMode,
{
}

impl<S, M, const B: usize> AsyncRead for TokioReader<S, M, B>
where
    S: TrustedGet<AnyChunkSet<B>> + Clone + 'static,
    S::Error: std::error::Error + Send + Sync + 'static,
    M: WalkMode,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        match this.inner.poll_read(cx, buf.initialize_unfilled()) {
            Poll::Ready(Ok(filled)) => {
                buf.advance(filled);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(error)) => Poll::Ready(Err(io::Error::other(error))),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<S, M, const B: usize> AsyncSeek for TokioReader<S, M, B>
where
    S: TrustedGet<AnyChunkSet<B>> + Clone + 'static,
    S::Error: std::error::Error + Send + Sync + 'static,
    M: WalkMode,
{
    /// Seeks resolve synchronously here; completion only reports the
    /// position.
    fn start_seek(self: Pin<&mut Self>, position: SeekFrom) -> io::Result<()> {
        let this = self.get_mut();
        let target = resolve(position, this.inner.position(), this.inner.effective_len())
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        this.inner
            .seek(target)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))
    }

    fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        Poll::Ready(Ok(self.get_mut().inner.position()))
    }
}

impl<S, M, const B: usize> fmt::Debug for TokioReader<S, M, B>
where
    S: TrustedGet<AnyChunkSet<B>>,
    M: WalkMode,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TokioReader")
            .field("inner", &self.inner)
            .finish()
    }
}
