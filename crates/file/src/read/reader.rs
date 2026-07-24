//! Ordered reader and stream over one walk.

use alloc::vec::Vec;
use core::fmt;
use core::future::poll_fn;
use core::mem;
use core::ops::Range;
use core::pin::Pin;
use core::task::{Context, Poll};
use core::time::Duration;

use bytes::Bytes;
use futures_util::stream::{Stream, StreamExt};
use nectar_primitives::DEFAULT_BODY_SIZE;
use nectar_primitives::chunk::{AnyChunkSet, ChunkAddress};
use nectar_primitives::store::TrustedGet;

use super::body_size;
use super::error::{CollectError, SeekPastEnd};
use super::frames::FileFrames;
use crate::config::Window;
use crate::num::u64_from_usize;
use crate::walk::{Walk, WalkError, WalkMode, WalkStats};

/// Builder of one ordered read; construction is infallible.
///
/// Ranges use clip semantics: the built reader covers the intersection of
/// the requested range and the file, and the clipped length is readable as
/// [`FileReader::effective_len`].
pub struct ReadBuilder<S, M: WalkMode, const B: usize = DEFAULT_BODY_SIZE> {
    store: S,
    root: ChunkAddress,
    context: M::Context,
    span: u64,
    window: Window,
    range: Range<u64>,
}

impl<S, M: WalkMode, const B: usize> ReadBuilder<S, M, B> {
    pub(super) const fn new(
        store: S,
        root: ChunkAddress,
        context: M::Context,
        span: u64,
        window: Window,
        range: Range<u64>,
    ) -> Self {
        Self {
            store,
            root,
            context,
            span,
            window,
            range,
        }
    }

    /// Fetch window the read drains against.
    #[must_use]
    pub const fn window(mut self, window: Window) -> Self {
        self.window = window;
        self
    }

    /// Fetch window sized to sustain `bytes_per_second` at `mean_latency`
    /// per leaf fetch; see [`Window::for_throughput`].
    #[must_use]
    pub const fn throughput(self, bytes_per_second: u64, mean_latency: Duration) -> Self {
        self.window(Window::for_throughput(
            bytes_per_second,
            mean_latency,
            body_size::<B>(),
        ))
    }

    /// Absolute byte range to read, clipped to the file.
    #[must_use]
    pub const fn range(mut self, range: Range<u64>) -> Self {
        self.range = range;
        self
    }
}

impl<S, M, const B: usize> ReadBuilder<S, M, B>
where
    S: TrustedGet<AnyChunkSet<B>> + Clone + 'static,
    M: WalkMode,
{
    /// Build the ordered, seekable reader.
    pub fn build(self) -> FileReader<S, M, B> {
        let walk = Walk::new(
            self.store.clone(),
            self.root,
            self.context.clone(),
            self.span,
            self.range,
            self.window,
        );
        let clipped = walk.range();
        FileReader {
            store: self.store,
            root: self.root,
            context: self.context,
            span: self.span,
            window: self.window,
            start: clipped.start,
            end: clipped.end,
            position: clipped.start,
            current: Bytes::new(),
            walk,
        }
    }

    /// Build the ordered stream directly.
    pub fn stream(self) -> FileStream<S, M, B> {
        self.build().into_stream()
    }

    /// Build the completion-order frames drain.
    pub fn frames(self) -> FileFrames<S, M, B> {
        FileFrames::new(Walk::new(
            self.store,
            self.root,
            self.context,
            self.span,
            self.range,
            self.window,
        ))
    }

    /// Assemble the clipped range in memory, at most `max` bytes.
    ///
    /// The buffer is reserved up front with `try_reserve_exact`, so an
    /// oversized range fails typed before any fetch; the bound saturates at
    /// the address width. Frames land at their range-relative offsets in
    /// completion order, tiling the buffer exactly once with no reorder
    /// buffering.
    pub async fn collect(self, max: u64) -> Result<Vec<u8>, CollectError<S::Error>> {
        let mut frames = self.frames();
        let clipped = frames.range();
        let len = clipped.end.saturating_sub(clipped.start);
        let bound = max.min(u64_from_usize(usize::MAX));
        let capacity = match usize::try_from(len) {
            Ok(capacity) if len <= bound => capacity,
            _ => return Err(CollectError::TooLarge { len, max: bound }),
        };
        let mut out = Vec::new();
        out.try_reserve_exact(capacity)?;
        out.resize(capacity, 0);
        while let Some(frame) = frames.next().await {
            let frame = frame?;
            let offset = frame.offset.saturating_sub(clipped.start);
            let start = usize::try_from(offset).unwrap_or(usize::MAX);
            for (slot, byte) in out.iter_mut().skip(start).zip(frame.data.as_ref()) {
                *slot = *byte;
            }
        }
        Ok(out)
    }
}

impl<S, M: WalkMode, const B: usize> fmt::Debug for ReadBuilder<S, M, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadBuilder")
            .field("root", &self.root)
            .field("span", &self.span)
            .field("window", &self.window)
            .field("range", &self.range)
            .finish_non_exhaustive()
    }
}

/// Ordered, seekable reader over one clipped range.
///
/// Positions are zero-based offsets within the clipped range. Reads are
/// cancel-safe: all progress lives in the reader, and the position advances
/// only when a call returns.
pub struct FileReader<S, M, const B: usize = DEFAULT_BODY_SIZE>
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
    walk: Walk<S, M, B>,
}

impl<S, M, const B: usize> FileReader<S, M, B>
where
    S: TrustedGet<AnyChunkSet<B>> + Clone + 'static,
    M: WalkMode,
{
    /// Current position within the clipped range.
    pub const fn position(&self) -> u64 {
        self.position.saturating_sub(self.start)
    }

    /// Bytes the clipped range covers.
    pub const fn effective_len(&self) -> u64 {
        self.end.saturating_sub(self.start)
    }

    /// Occupancy witnesses of the underlying walk.
    pub const fn stats(&self) -> WalkStats {
        self.walk.stats()
    }

    /// Move to `pos` within the clipped range; synchronous and typed, never
    /// clamps. Seeking to the effective length is legal and reads as
    /// end-of-range. A seek away from the current position abandons the
    /// walk's prefetched frames.
    pub fn seek(&mut self, pos: u64) -> Result<(), SeekPastEnd> {
        let effective_len = self.effective_len();
        if pos > effective_len {
            return Err(SeekPastEnd {
                requested: pos,
                effective_len,
            });
        }
        let target = self.start.saturating_add(pos);
        if target != self.position {
            self.current = Bytes::new();
            self.walk = Walk::new(
                self.store.clone(),
                self.root,
                self.context.clone(),
                self.span,
                target..self.end,
                self.window,
            );
            self.position = target;
        }
        Ok(())
    }

    /// Poll twin of [`read`](Self::read): copy the next in-order bytes into
    /// `buf`, delivering the count; zero means end of range (or an empty
    /// `buf`).
    ///
    /// The walk's fetch window stays in flight across polls, and no future
    /// is created per call.
    pub fn poll_read(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, WalkError<S::Error>>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        if self.current.is_empty() {
            match self.walk.poll_next_ordered(cx) {
                Poll::Ready(Some(Ok(frame))) => self.current = frame.data,
                Poll::Ready(Some(Err(error))) => return Poll::Ready(Err(error)),
                Poll::Ready(None) => return Poll::Ready(Ok(0)),
                Poll::Pending => return Poll::Pending,
            }
        }
        let take = self.current.len().min(buf.len());
        let (head, _) = buf.split_at_mut(take);
        head.copy_from_slice(self.current.split_to(take).as_ref());
        self.position = self.position.saturating_add(u64_from_usize(take));
        Poll::Ready(Ok(take))
    }

    /// Copy the next in-order bytes into `buf`, returning the count; zero
    /// means end of range (or an empty `buf`).
    pub async fn read(&mut self, buf: &mut [u8]) -> Result<usize, WalkError<S::Error>> {
        poll_fn(|cx| self.poll_read(cx, buf)).await
    }

    /// The next in-order run of bytes without copying; `None` at end of
    /// range.
    pub async fn next_segment(&mut self) -> Option<Result<Bytes, WalkError<S::Error>>> {
        if !self.current.is_empty() {
            let rest = mem::take(&mut self.current);
            self.position = self.position.saturating_add(u64_from_usize(rest.len()));
            return Some(Ok(rest));
        }
        match poll_fn(|cx| self.walk.poll_next_ordered(cx)).await? {
            Ok(frame) => {
                self.position = self
                    .position
                    .saturating_add(u64_from_usize(frame.data.len()));
                Some(Ok(frame.data))
            }
            Err(error) => Some(Err(error)),
        }
    }

    /// Continue as a stream from the current position, delivering any
    /// partially consumed frame first.
    pub fn into_stream(self) -> FileStream<S, M, B> {
        FileStream {
            lead: self.current,
            walk: self.walk,
        }
    }

    /// Decompose into the io adapters' state: the live walk and lead bytes
    /// plus the rebuild recipe a seek re-walks from.
    #[cfg(all(
        feature = "tokio",
        not(any(target_arch = "wasm32", feature = "unsync"))
    ))]
    pub(crate) fn into_parts(self) -> ReaderParts<S, M, B> {
        ReaderParts {
            store: self.store,
            root: self.root,
            context: self.context,
            span: self.span,
            window: self.window,
            start: self.start,
            end: self.end,
            position: self.position,
            current: self.current,
            walk: self.walk,
        }
    }
}

/// Decomposed reader state for the io adapters; fields mirror
/// [`FileReader`].
#[cfg(all(
    feature = "tokio",
    not(any(target_arch = "wasm32", feature = "unsync"))
))]
pub(crate) struct ReaderParts<S, M, const B: usize>
where
    S: TrustedGet<AnyChunkSet<B>>,
    M: WalkMode,
{
    pub(crate) store: S,
    pub(crate) root: ChunkAddress,
    pub(crate) context: M::Context,
    pub(crate) span: u64,
    pub(crate) window: Window,
    pub(crate) start: u64,
    pub(crate) end: u64,
    pub(crate) position: u64,
    pub(crate) current: Bytes,
    pub(crate) walk: Walk<S, M, B>,
}

impl<S, M, const B: usize> fmt::Debug for FileReader<S, M, B>
where
    S: TrustedGet<AnyChunkSet<B>>,
    M: WalkMode,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileReader")
            .field("start", &self.start)
            .field("end", &self.end)
            .field("position", &self.position)
            .field("buffered", &self.current.len())
            .finish_non_exhaustive()
    }
}

/// Ordered stream of byte runs over one clipped range; consecutive items
/// tile the range gaplessly.
///
/// Runtime-free: no spawns, threads or timers, so the stream drains under
/// any single-threaded executor, wasm32 included.
///
/// ```
/// use futures::StreamExt;
/// use nectar_file::File;
///
/// # nectar_testing::run(async {
/// let data: Vec<u8> = (0u32..20_000)
///     .map(|i| u8::try_from(i % 251).unwrap())
///     .collect();
/// # let store = std::sync::Arc::new(nectar_primitives::store::MemoryStore::new());
/// # let root = nectar_file::Split::<_, nectar_file::Plain, 4096>::collect(
/// #     std::sync::Arc::clone(&store),
/// #     &data,
/// # )
/// # .await
/// # .unwrap();
/// let file = File::open(store, root).await.unwrap();
/// let mut stream = file.read().range(4_096..12_288).stream();
/// let mut out = Vec::new();
/// while let Some(run) = stream.next().await {
///     out.extend_from_slice(&run.unwrap());
/// }
/// assert_eq!(out, &data[4_096..12_288]);
/// # });
/// ```
pub struct FileStream<S, M, const B: usize = DEFAULT_BODY_SIZE>
where
    S: TrustedGet<AnyChunkSet<B>>,
    M: WalkMode,
{
    /// Partially consumed frame handed over by a reader, delivered first.
    lead: Bytes,
    walk: Walk<S, M, B>,
}

/// Movable regardless of the store or context types: the stream owns plain
/// state and boxed futures, never a self-reference.
impl<S, M, const B: usize> Unpin for FileStream<S, M, B>
where
    S: TrustedGet<AnyChunkSet<B>>,
    M: WalkMode,
{
}

impl<S, M, const B: usize> Stream for FileStream<S, M, B>
where
    S: TrustedGet<AnyChunkSet<B>> + Clone + 'static,
    M: WalkMode,
{
    type Item = Result<Bytes, WalkError<S::Error>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if !this.lead.is_empty() {
            return Poll::Ready(Some(Ok(mem::take(&mut this.lead))));
        }
        this.walk
            .poll_next_ordered(cx)
            .map(|next| next.map(|frame| frame.map(|frame| frame.data)))
    }
}

impl<S, M, const B: usize> fmt::Debug for FileStream<S, M, B>
where
    S: TrustedGet<AnyChunkSet<B>>,
    M: WalkMode,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileStream")
            .field("lead", &self.lead.len())
            .field("walk", &self.walk)
            .finish_non_exhaustive()
    }
}
