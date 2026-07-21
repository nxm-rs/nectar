//! Spawned pool driver: a runtime task advances the walk between reads.

use core::fmt;
use core::future::poll_fn;
use core::pin::Pin;
use core::task::{Context, Poll};
use std::io;
use std::io::SeekFrom;

use bytes::Bytes;
use nectar_primitives::DEFAULT_BODY_SIZE;
use nectar_primitives::chunk::{AnyChunkSet, ChunkAddress};
use nectar_primitives::store::TrustedGet;
use tokio::io::{AsyncRead, AsyncSeek, ReadBuf};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::resolve;
use crate::config::Window;
use crate::num::u64_from_usize;
use crate::read::{FileReader, SeekPastEnd};
use crate::walk::{Frame, Walk, WalkError, WalkMode};

/// Frame channel one driver task fills.
type Frames<E> = mpsc::Receiver<Result<Frame, WalkError<E>>>;

/// [`AsyncRead`] plus [`AsyncSeek`] reader whose walk a spawned runtime
/// task drives, so the fetch window keeps filling between reads.
///
/// Read-ahead is bounded by the walk window plus one frame channel of the
/// same depth. A seek away from the position aborts the driver and spawns
/// a fresh walk; dropping the reader aborts it. Positions are zero-based
/// within the clipped range; a seek outside it is `InvalidInput`, never a
/// clamp.
pub struct SpawnedReader<S, M, const B: usize = DEFAULT_BODY_SIZE>
where
    S: TrustedGet<AnyChunkSet<B>>,
    M: WalkMode,
{
    store: S,
    root: ChunkAddress,
    context: M::Context,
    span: u64,
    window: Window,
    /// Absolute first byte of the clipped range.
    start: u64,
    /// Absolute end of the clipped range.
    end: u64,
    /// Absolute offset of the next undelivered byte.
    position: u64,
    /// Unconsumed tail of the last delivered frame.
    current: Bytes,
    frames: Frames<S::Error>,
    driver: JoinHandle<()>,
}

impl<S, M, const B: usize> SpawnedReader<S, M, B>
where
    S: TrustedGet<AnyChunkSet<B>> + Clone + Send + 'static,
    S::Error: Send,
    M: WalkMode,
{
    /// Move `reader` onto the current runtime, retaining its walk and any
    /// prefetched frames. Must be called within a runtime.
    pub fn spawn(reader: FileReader<S, M, B>) -> Self {
        let parts = reader.into_parts();
        let (frames, driver) = drive(parts.walk, parts.window);
        Self {
            store: parts.store,
            root: parts.root,
            context: parts.context,
            span: parts.span,
            window: parts.window,
            start: parts.start,
            end: parts.end,
            position: parts.position,
            current: parts.current,
            frames,
            driver,
        }
    }

    /// Current position within the clipped range.
    pub const fn position(&self) -> u64 {
        self.position.saturating_sub(self.start)
    }

    /// Bytes the clipped range covers.
    pub const fn effective_len(&self) -> u64 {
        self.end.saturating_sub(self.start)
    }

    /// Move to `pos` within the clipped range; a move away from the current
    /// position abandons the driver with its prefetched frames and spawns a
    /// fresh walk.
    fn reseek(&mut self, pos: u64) -> Result<(), SeekPastEnd> {
        let effective_len = self.effective_len();
        if pos > effective_len {
            return Err(SeekPastEnd {
                requested: pos,
                effective_len,
            });
        }
        let target = self.start.saturating_add(pos);
        if target == self.position {
            return Ok(());
        }
        self.driver.abort();
        self.current = Bytes::new();
        let walk: Walk<S, M, B> = Walk::new(
            self.store.clone(),
            self.root,
            self.context.clone(),
            self.span,
            target..self.end,
            self.window,
        );
        let (frames, driver) = drive(walk, self.window);
        self.frames = frames;
        self.driver = driver;
        self.position = target;
        Ok(())
    }
}

/// Drive one walk into a bounded frame channel until it finishes, fails or
/// loses its receiver.
fn drive<S, M, const B: usize>(
    mut walk: Walk<S, M, B>,
    window: Window,
) -> (Frames<S::Error>, JoinHandle<()>)
where
    S: TrustedGet<AnyChunkSet<B>> + Clone + Send + 'static,
    S::Error: Send,
    M: WalkMode,
{
    let (sender, receiver) = mpsc::channel(usize::from(window.get()));
    let driver = tokio::spawn(async move {
        loop {
            let Some(item) = poll_fn(|cx| walk.poll_next_ordered(cx)).await else {
                break;
            };
            let terminal = item.is_err();
            if sender.send(item).await.is_err() || terminal {
                break;
            }
        }
    });
    (receiver, driver)
}

impl<S, M, const B: usize> Drop for SpawnedReader<S, M, B>
where
    S: TrustedGet<AnyChunkSet<B>>,
    M: WalkMode,
{
    fn drop(&mut self) {
        self.driver.abort();
    }
}

/// Movable regardless of the store or context types: the reader owns plain
/// state and channel handles, never a self-reference.
impl<S, M, const B: usize> Unpin for SpawnedReader<S, M, B>
where
    S: TrustedGet<AnyChunkSet<B>>,
    M: WalkMode,
{
}

impl<S, M, const B: usize> AsyncRead for SpawnedReader<S, M, B>
where
    S: TrustedGet<AnyChunkSet<B>> + Clone + Send + 'static,
    S::Error: std::error::Error + Send + Sync + 'static,
    M: WalkMode,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.current.is_empty() {
            match this.frames.poll_recv(cx) {
                Poll::Ready(Some(Ok(frame))) => this.current = frame.data,
                Poll::Ready(Some(Err(error))) => {
                    return Poll::Ready(Err(io::Error::other(error)));
                }
                Poll::Ready(None) => return Poll::Ready(Ok(())),
                Poll::Pending => return Poll::Pending,
            }
        }
        let unfilled = buf.initialize_unfilled();
        let take = this.current.len().min(unfilled.len());
        let (head, _) = unfilled.split_at_mut(take);
        head.copy_from_slice(this.current.split_to(take).as_ref());
        buf.advance(take);
        this.position = this.position.saturating_add(u64_from_usize(take));
        Poll::Ready(Ok(()))
    }
}

impl<S, M, const B: usize> AsyncSeek for SpawnedReader<S, M, B>
where
    S: TrustedGet<AnyChunkSet<B>> + Clone + Send + 'static,
    S::Error: std::error::Error + Send + Sync + 'static,
    M: WalkMode,
{
    /// Seeks resolve synchronously here (respawning the driver when they
    /// move); completion only reports the position.
    fn start_seek(self: Pin<&mut Self>, position: SeekFrom) -> io::Result<()> {
        let this = self.get_mut();
        let target = resolve(position, this.position(), this.effective_len())
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        this.reseek(target)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))
    }

    fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        Poll::Ready(Ok(self.get_mut().position()))
    }
}

impl<S, M, const B: usize> fmt::Debug for SpawnedReader<S, M, B>
where
    S: TrustedGet<AnyChunkSet<B>>,
    M: WalkMode,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpawnedReader")
            .field("start", &self.start)
            .field("end", &self.end)
            .field("position", &self.position)
            .field("buffered", &self.current.len())
            .finish_non_exhaustive()
    }
}
