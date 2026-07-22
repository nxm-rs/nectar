//! Shim writer: the caller's polls feed the split directly.

use core::fmt;
use core::pin::Pin;
use core::task::{Context, Poll};
use std::io;

use nectar_primitives::DEFAULT_BODY_SIZE;
use nectar_primitives::chunk::AnyChunkSet;
use nectar_primitives::store::ChunkPut;
use tokio::io::AsyncWrite;

use crate::split::{Split, SplitMode, SplitStats};

/// [`AsyncWrite`] over one [`Split`].
///
/// Every poll feeds the split's ascent in place, so the put window stays in
/// flight across polls and no future is created per call. `poll_shutdown`
/// drives the finish to the root, which [`into_inner`](Self::into_inner)
/// then hands back.
///
/// Streaming an upload from any [`AsyncRead`](::tokio::io::AsyncRead)
/// source:
///
/// ```
/// use nectar_file::{Plain, PutWindow, Split, TokioWriter};
/// use nectar_primitives::chunk::AnyChunkSet;
/// use nectar_primitives::store::MemoryStore;
/// use tokio::io::AsyncWriteExt;
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// let data: Vec<u8> = (0u32..100_000)
///     .map(|i| u8::try_from(i % 251).unwrap())
///     .collect();
/// let store = MemoryStore::<AnyChunkSet<4096>>::new();
/// let split = Split::<_, Plain, 4096>::new(store, PutWindow::DEFAULT);
/// let mut writer = TokioWriter::from(split);
/// let mut source = &data[..];
/// tokio::io::copy(&mut source, &mut writer).await.unwrap();
/// writer.shutdown().await.unwrap();
/// let root = writer.into_inner().unwrap();
/// # // Root equality with an independent whole-buffer split of the same bytes.
/// # let expected = Split::<_, Plain, 4096>::collect(
/// #     MemoryStore::<AnyChunkSet<4096>>::new(),
/// #     &data,
/// # )
/// # .await
/// # .unwrap();
/// # assert_eq!(root, expected);
/// # }
/// ```
pub struct TokioWriter<S, M, const B: usize = DEFAULT_BODY_SIZE>
where
    S: ChunkPut<AnyChunkSet<B>>,
    M: SplitMode,
{
    inner: Split<S, M, B>,
    root: Option<M::Root>,
}

impl<S, M, const B: usize> TokioWriter<S, M, B>
where
    S: ChunkPut<AnyChunkSet<B>> + Clone + 'static,
    M: SplitMode,
{
    /// Occupancy witnesses accumulated so far.
    pub const fn stats(&self) -> SplitStats {
        self.inner.stats()
    }

    /// Whether the split has delivered its root or failed.
    pub const fn is_finished(&self) -> bool {
        self.inner.is_finished()
    }

    /// The delivered root; `None` until `poll_shutdown` has completed.
    pub fn into_inner(self) -> Option<M::Root> {
        self.root
    }
}

impl<S, M, const B: usize> From<Split<S, M, B>> for TokioWriter<S, M, B>
where
    S: ChunkPut<AnyChunkSet<B>>,
    M: SplitMode,
{
    fn from(inner: Split<S, M, B>) -> Self {
        Self { inner, root: None }
    }
}

impl<S, M, const B: usize> From<TokioWriter<S, M, B>> for Split<S, M, B>
where
    S: ChunkPut<AnyChunkSet<B>>,
    M: SplitMode,
{
    fn from(writer: TokioWriter<S, M, B>) -> Self {
        writer.inner
    }
}

/// Movable regardless of the store or context types: the shim owns plain
/// state and boxed futures, never a self-reference.
impl<S, M, const B: usize> Unpin for TokioWriter<S, M, B>
where
    S: ChunkPut<AnyChunkSet<B>>,
    M: SplitMode,
{
}

impl<S, M, const B: usize> AsyncWrite for TokioWriter<S, M, B>
where
    S: ChunkPut<AnyChunkSet<B>> + Clone + 'static,
    S::Error: std::error::Error + Send + Sync + 'static,
    M: SplitMode,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.get_mut()
            .inner
            .poll_write(cx, buf)
            .map_err(io::Error::other)
    }

    /// The shim buffers nothing: sealed chunks stream to the store as write
    /// and shutdown polls drive them, and a partial leaf seals only at the
    /// finish.
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    /// Drives the split's finish; the fused root lands in the shim for
    /// [`into_inner`](Self::into_inner).
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        match this.inner.poll_finish(cx) {
            Poll::Ready(Ok(root)) => {
                this.root = Some(root);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(error)) => Poll::Ready(Err(io::Error::other(error))),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<S, M, const B: usize> fmt::Debug for TokioWriter<S, M, B>
where
    S: ChunkPut<AnyChunkSet<B>>,
    M: SplitMode,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TokioWriter")
            .field("inner", &self.inner)
            .field("root", &self.root)
            .finish()
    }
}
