//! The one file handle over a chunk store.
//!
//! [`File`] pairs a store with an admission [`Policy`] and exposes the two
//! verbs the pipeline has: `load` drains a chunk tree into a positional
//! [`DataSink`], `save` drains a [`Source`] into a fresh tree. The reference
//! width is dispatched at runtime off the [`EntryRef`], so one handle reads
//! both plain and encrypted trees; the write side picks its grammar by verb.
//!
//! Ranges use clip semantics: out-of-file bounds shrink the read instead of
//! failing. Only [`Reader::seek`] is typed-strict; it never clamps.
//!
//! ```
//! use nectar_file::{File, MemSink, Policy};
//! use nectar_primitives::chunk::AnyChunkSet;
//! use nectar_primitives::store::{ContentGet, MemoryStore};
//! use std::sync::Arc;
//!
//! # nectar_testing::run(async {
//! let data: Vec<u8> = (0u32..40_000).map(|i| u8::try_from(i % 251).unwrap()).collect();
//! let store = Arc::new(MemoryStore::<AnyChunkSet<4096>>::new());
//!
//! let writer = File::<_, 4096>::new(Arc::clone(&store), Policy::DEFAULT);
//! let root = writer.save(&data[..]).await.unwrap();
//!
//! let reader = File::<_, 4096>::new(ContentGet::new(Arc::clone(&store)), Policy::DEFAULT);
//! let mut sink = MemSink::new();
//! let written = reader.load(root.into(), &mut sink).await.unwrap();
//! assert_eq!(written, 40_000);
//! assert_eq!(sink.as_ref(), &data[..]);
//! # });
//! ```

use alloc::vec::Vec;
use core::fmt;
use core::ops::Range;
use core::pin::Pin;
use core::task::{Context, Poll};
use core::time::Duration;

use bytes::Bytes;
use futures_util::stream::Stream;
use nectar_primitives::chunk::{AnyChunkSet, ChunkAddress, ContentOnlyChunkSet};
use nectar_primitives::store::{ChunkPut, TrustedGet};
use nectar_primitives::{DEFAULT_BODY_SIZE, EntryRef};

use crate::config::{HashWindow, PutWindow, Window};
use crate::geometry::Mode;
use crate::read::{
    AnyOpened, CollectError, DownloadBuilder, FileReader, FileStream, LoadError, OpenError, Opened,
    ProgressFn, ReadBuilder, SeekPastEnd,
};
use crate::sink::DataSink;
use crate::source::Source;
use crate::split::{SaveError, SplitMode, SplitStats, save_source};
use crate::walk::{Encrypted, Plain, WalkError, WalkMode, WalkStats};

/// Closed-loop window seed: the throughput hint an adaptive controller is
/// built from, plus the cap it must never pass.
#[cfg(feature = "std")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Adaptive {
    bytes_per_second: u64,
    mean_latency: Duration,
    max: Window,
}

/// Admission budgets every read and write of one handle runs under.
///
/// The hash window is only honoured by a `rayon` build; other builds seal
/// leaves inline and ignore it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Policy {
    window: Window,
    put: PutWindow,
    hash: Option<HashWindow>,
    #[cfg(feature = "std")]
    adaptive: Option<Adaptive>,
}

impl Policy {
    /// The default budgets: the default fetch window and put window, leaves
    /// sealed inline.
    pub const DEFAULT: Self = Self {
        window: Window::DEFAULT,
        put: PutWindow::DEFAULT,
        hash: None,
        #[cfg(feature = "std")]
        adaptive: None,
    };

    /// Fetch window a read drains against.
    pub const fn window(self) -> Window {
        self.window
    }

    /// Put window a write holds sealed chunks in flight against.
    pub const fn put_window(self) -> PutWindow {
        self.put
    }

    /// Leaf-seal window a pooled write fans out against; `None` seals
    /// inline.
    pub const fn hash_window(self) -> Option<HashWindow> {
        self.hash
    }

    /// Replace the fetch window.
    #[must_use]
    pub const fn with_window(mut self, window: Window) -> Self {
        self.window = window;
        self
    }

    /// Fetch window sized to sustain `bytes_per_second` at `mean_latency`
    /// per leaf fetch; see [`Window::for_throughput`].
    #[must_use]
    pub const fn with_throughput<const B: usize>(
        self,
        bytes_per_second: u64,
        mean_latency: Duration,
    ) -> Self {
        self.with_window(Window::for_throughput(
            bytes_per_second,
            mean_latency,
            crate::read::body_size::<B>(),
        ))
    }

    /// Closed-loop window: seed from the throughput hint and let an
    /// [`AdaptiveWindow`](crate::AdaptiveWindow) controller retune the cap
    /// against realized latency, never past `max`.
    #[cfg(feature = "std")]
    #[cfg_attr(docsrs, doc(cfg(feature = "std")))]
    #[must_use]
    pub const fn with_adaptive_throughput(
        mut self,
        bytes_per_second: u64,
        mean_latency: Duration,
        max: Window,
    ) -> Self {
        self.adaptive = Some(Adaptive {
            bytes_per_second,
            mean_latency,
            max,
        });
        self
    }

    /// Replace the put window.
    #[must_use]
    pub const fn with_put_window(mut self, window: PutWindow) -> Self {
        self.put = window;
        self
    }

    /// Fan leaf sealing onto the thread pool under `window` seals in flight.
    /// Honoured only by a `rayon` build.
    #[must_use]
    pub const fn with_hash_window(mut self, window: HashWindow) -> Self {
        self.hash = Some(window);
        self
    }
}

impl Default for Policy {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// One store plus the budgets its reads and writes run under.
///
/// The handle is stateless past its store and policy, so a clone is as cheap
/// as the store's own clone and every call is independent.
pub struct File<S, const B: usize = DEFAULT_BODY_SIZE> {
    store: S,
    policy: Policy,
}

impl<S, const B: usize> File<S, B> {
    /// Bind `store` to `policy`.
    pub const fn new(store: S, policy: Policy) -> Self {
        Self { store, policy }
    }

    /// The budgets this handle runs under.
    pub const fn policy(&self) -> Policy {
        self.policy
    }

    /// Borrow the backing store.
    pub const fn store(&self) -> &S {
        &self.store
    }

    /// Consume into the backing store.
    pub fn into_store(self) -> S {
        self.store
    }
}

impl<S: Clone, const B: usize> Clone for File<S, B> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            policy: self.policy,
        }
    }
}

impl<S, const B: usize> fmt::Debug for File<S, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("File")
            .field("policy", &self.policy)
            .finish_non_exhaustive()
    }
}

impl<S, const B: usize> File<S, B>
where
    S: TrustedGet<ContentOnlyChunkSet<B>> + Clone + 'static,
{
    /// Open `root` for an ordered, seekable read of the whole file.
    pub async fn open(&self, root: EntryRef) -> Result<Reader<S, B>, OpenError<S::Error>> {
        self.open_range(root, 0..u64::MAX).await
    }

    /// Open `root` for an ordered, seekable read of `range`, clipped to the
    /// file.
    pub async fn open_range(
        &self,
        root: EntryRef,
        range: Range<u64>,
    ) -> Result<Reader<S, B>, OpenError<S::Error>> {
        let inner = match self.opened(root).await? {
            AnyOpened::Plain(file) => {
                let span = file.len();
                let root = *file.root();
                ReaderInner::Plain {
                    root,
                    span,
                    reader: self.reads(&file, range).build(),
                }
            }
            AnyOpened::Encrypted(file) => {
                let span = file.len();
                let root = *file.root();
                ReaderInner::Encrypted {
                    root,
                    span,
                    reader: self.reads(&file, range).build(),
                }
            }
        };
        Ok(Reader { inner })
    }

    /// Drain the whole file at `root` into `sink`, returning the bytes
    /// written.
    ///
    /// Frames land in completion order, each written once at its
    /// range-relative offset, which is what makes unordered retrieval
    /// possible; any error is terminal for this run. A load is restartable,
    /// not resumable: run it again in full and the sink's idempotent
    /// overwrites make the re-run safe.
    pub async fn load<K: DataSink>(
        &self,
        root: EntryRef,
        sink: &mut K,
    ) -> Result<u64, LoadError<S::Error, K::Error>> {
        self.load_range(root, 0..u64::MAX, sink).await
    }

    /// Drain `range` of the file at `root` into `sink`; sink offsets are
    /// relative to the clipped range start.
    pub async fn load_range<K: DataSink>(
        &self,
        root: EntryRef,
        range: Range<u64>,
        sink: &mut K,
    ) -> Result<u64, LoadError<S::Error, K::Error>> {
        self.load_with(root, range, None, sink).await
    }

    /// [`load_range`](Self::load_range) reporting progress after each frame
    /// lands in the sink.
    pub async fn load_with_progress<K: DataSink>(
        &self,
        root: EntryRef,
        range: Range<u64>,
        progress: ProgressFn,
        sink: &mut K,
    ) -> Result<u64, LoadError<S::Error, K::Error>> {
        self.load_with(root, range, Some(progress), sink).await
    }

    async fn load_with<K: DataSink>(
        &self,
        root: EntryRef,
        range: Range<u64>,
        progress: Option<ProgressFn>,
        sink: &mut K,
    ) -> Result<u64, LoadError<S::Error, K::Error>> {
        match self.opened(root).await? {
            AnyOpened::Plain(file) => self.downloads(&file, range, progress).run(sink).await,
            AnyOpened::Encrypted(file) => self.downloads(&file, range, progress).run(sink).await,
        }
    }

    /// Assemble the whole file at `root` in memory, at most `max` bytes.
    ///
    /// The buffer is reserved up front, so an oversized file fails typed
    /// before any body fetch.
    pub async fn collect(
        &self,
        root: EntryRef,
        max: u64,
    ) -> Result<Vec<u8>, CollectError<S::Error>> {
        self.collect_range(root, 0..u64::MAX, max).await
    }

    /// Assemble `range` of the file at `root` in memory, at most `max`
    /// bytes.
    pub async fn collect_range(
        &self,
        root: EntryRef,
        range: Range<u64>,
        max: u64,
    ) -> Result<Vec<u8>, CollectError<S::Error>> {
        match self.opened(root).await? {
            AnyOpened::Plain(file) => self.reads(&file, range).collect(max).await,
            AnyOpened::Encrypted(file) => self.reads(&file, range).collect(max).await,
        }
    }

    /// Total length of the file at `root`, read off its root chunk.
    pub async fn size(&self, root: EntryRef) -> Result<u64, OpenError<S::Error>> {
        Ok(self.opened(root).await?.len())
    }

    /// Open the root chunk, dispatching the grammar on the reference width.
    async fn opened(&self, root: EntryRef) -> Result<AnyOpened<S, B>, OpenError<S::Error>> {
        AnyOpened::open(self.store.clone(), root).await
    }

    /// One read builder wired to this handle's policy.
    fn reads<M: WalkMode>(&self, file: &Opened<S, M, B>, range: Range<u64>) -> ReadBuilder<S, M, B> {
        let builder = file.read().window(self.policy.window).range(range);
        #[cfg(feature = "std")]
        if let Some(adaptive) = self.policy.adaptive {
            return builder.adaptive_throughput(
                adaptive.bytes_per_second,
                adaptive.mean_latency,
                adaptive.max,
            );
        }
        builder
    }

    /// One download builder wired to this handle's policy.
    fn downloads<M: WalkMode>(
        &self,
        file: &Opened<S, M, B>,
        range: Range<u64>,
        progress: Option<ProgressFn>,
    ) -> DownloadBuilder<S, M, B> {
        let builder = file.download().window(self.policy.window).range(range);
        #[cfg(feature = "std")]
        let builder = match self.policy.adaptive {
            Some(adaptive) => builder.adaptive_throughput(
                adaptive.bytes_per_second,
                adaptive.mean_latency,
                adaptive.max,
            ),
            None => builder,
        };
        match progress {
            Some(progress) => builder.progress(progress),
            None => builder,
        }
    }
}

impl<S, const B: usize> File<S, B>
where
    S: ChunkPut<AnyChunkSet<B>>,
{
    /// Split `src` into a plain chunk tree, returning its root address.
    ///
    /// The put window bounds the chunks in flight against the store; the
    /// retained memory is that window plus the spine height.
    pub async fn save<Src: Source>(
        &self,
        src: Src,
    ) -> Result<ChunkAddress, SaveError<S::Error, Src::Error>> {
        self.save_as::<Plain, Src>(src).await
    }

    /// Split `src` into an encrypted chunk tree, each chunk sealed under a
    /// fresh random key; the returned reference carries the root's key.
    #[cfg(feature = "encryption")]
    #[cfg_attr(docsrs, doc(cfg(feature = "encryption")))]
    pub async fn save_encrypted<Src: Source>(
        &self,
        src: Src,
    ) -> Result<
        nectar_primitives::chunk::encryption::EncryptedChunkRef,
        SaveError<S::Error, Src::Error>,
    > {
        self.save_as::<Encrypted<crate::split::RandomKeys>, Src>(src)
            .await
    }

    /// Split `src` under an explicit reference grammar.
    pub async fn save_as<M, Src>(&self, src: Src) -> Result<M::Root, SaveError<S::Error, Src::Error>>
    where
        M: SplitMode + Default,
        Src: Source,
    {
        self.save_as_with_stats::<M, Src>(src)
            .await
            .map(|(root, _)| root)
    }

    /// [`save_as`](Self::save_as) also handing back the write's occupancy
    /// witnesses.
    pub async fn save_as_with_stats<M, Src>(
        &self,
        src: Src,
    ) -> Result<(M::Root, SplitStats), SaveError<S::Error, Src::Error>>
    where
        M: SplitMode + Default,
        Src: Source,
    {
        self.save_with_mode(M::default(), src).await
    }

    /// Split `src` under an explicitly constructed grammar, such as an
    /// [`Encrypted`] mode over a caller-owned
    /// [`KeySource`](crate::KeySource); the only entry point for a grammar
    /// that is not default-constructible.
    pub async fn save_with_mode<M, Src>(
        &self,
        mode: M,
        src: Src,
    ) -> Result<(M::Root, SplitStats), SaveError<S::Error, Src::Error>>
    where
        M: SplitMode,
        Src: Source,
    {
        save_source::<S, M, Src, B>(&self.store, self.policy, mode, src).await
    }
}

/// The grammar-specific half of an open read.
enum ReaderInner<S, const B: usize>
where
    S: TrustedGet<ContentOnlyChunkSet<B>>,
{
    Plain {
        root: ChunkAddress,
        span: u64,
        reader: FileReader<S, Plain, B>,
    },
    Encrypted {
        root: ChunkAddress,
        span: u64,
        reader: FileReader<S, Encrypted, B>,
    },
}

/// Ordered, seekable read over one clipped range of one file.
///
/// Positions are zero-based offsets within the clipped range. Reads are
/// cancel-safe: all progress lives in the reader, and the position advances
/// only when a call returns.
pub struct Reader<S, const B: usize = DEFAULT_BODY_SIZE>
where
    S: TrustedGet<ContentOnlyChunkSet<B>>,
{
    inner: ReaderInner<S, B>,
}

/// Run `body` against whichever grammar the reader opened.
macro_rules! dispatch {
    ($self:expr, $reader:ident => $body:expr) => {
        match &$self.inner {
            ReaderInner::Plain { reader: $reader, .. } => $body,
            ReaderInner::Encrypted { reader: $reader, .. } => $body,
        }
    };
    (mut $self:expr, $reader:ident => $body:expr) => {
        match &mut $self.inner {
            ReaderInner::Plain { reader: $reader, .. } => $body,
            ReaderInner::Encrypted { reader: $reader, .. } => $body,
        }
    };
}

impl<S, const B: usize> Reader<S, B>
where
    S: TrustedGet<ContentOnlyChunkSet<B>> + Clone + 'static,
{
    /// Total file length in bytes, whatever the read's range.
    pub const fn len(&self) -> u64 {
        match &self.inner {
            ReaderInner::Plain { span, .. } | ReaderInner::Encrypted { span, .. } => *span,
        }
    }

    /// Whether the file carries no bytes.
    pub const fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Address of the root chunk.
    pub const fn root(&self) -> &ChunkAddress {
        match &self.inner {
            ReaderInner::Plain { root, .. } | ReaderInner::Encrypted { root, .. } => root,
        }
    }

    /// Reference layout of the opened tree.
    pub const fn mode(&self) -> Mode {
        match &self.inner {
            ReaderInner::Plain { .. } => Mode::Plain,
            ReaderInner::Encrypted { .. } => Mode::Encrypted,
        }
    }

    /// Current position within the clipped range.
    pub const fn position(&self) -> u64 {
        dispatch!(self, reader => reader.position())
    }

    /// Bytes the clipped range covers.
    pub const fn effective_len(&self) -> u64 {
        dispatch!(self, reader => reader.effective_len())
    }

    /// Occupancy witnesses of the underlying walk.
    pub const fn stats(&self) -> WalkStats {
        dispatch!(self, reader => reader.stats())
    }

    /// Move to `pos` within the clipped range; synchronous and typed, never
    /// clamps. A seek away from the current position abandons the walk's
    /// prefetched frames.
    pub fn seek(&mut self, pos: u64) -> Result<(), SeekPastEnd> {
        dispatch!(mut self, reader => reader.seek(pos))
    }

    /// Poll twin of [`read`](Self::read): copy the next in-order bytes into
    /// `buf`, delivering the count; zero means end of range.
    ///
    /// The walk's fetch window stays in flight across polls, and no future
    /// is created per call.
    pub fn poll_read(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, WalkError<S::Error>>> {
        dispatch!(mut self, reader => reader.poll_read(cx, buf))
    }

    /// Copy the next in-order bytes into `buf`, returning the count; zero
    /// means end of range.
    pub async fn read(&mut self, buf: &mut [u8]) -> Result<usize, WalkError<S::Error>> {
        core::future::poll_fn(|cx| self.poll_read(cx, buf)).await
    }

    /// The next in-order run of bytes without copying; `None` at end of
    /// range.
    pub async fn next_segment(&mut self) -> Option<Result<Bytes, WalkError<S::Error>>> {
        match &mut self.inner {
            ReaderInner::Plain { reader, .. } => reader.next_segment().await,
            ReaderInner::Encrypted { reader, .. } => reader.next_segment().await,
        }
    }

    /// Continue as a stream from the current position, delivering any
    /// partially consumed frame first.
    pub fn into_segments(self) -> Segments<S, B> {
        let inner = match self.inner {
            ReaderInner::Plain { reader, .. } => SegmentsInner::Plain(reader.into_stream()),
            ReaderInner::Encrypted { reader, .. } => SegmentsInner::Encrypted(reader.into_stream()),
        };
        Segments { inner }
    }
}

impl<S, const B: usize> fmt::Debug for Reader<S, B>
where
    S: TrustedGet<ContentOnlyChunkSet<B>>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.inner {
            ReaderInner::Plain { root, span, .. } | ReaderInner::Encrypted { root, span, .. } => f
                .debug_struct("Reader")
                .field("root", root)
                .field("span", span)
                .finish_non_exhaustive(),
        }
    }
}

/// The grammar-specific half of a segment stream.
enum SegmentsInner<S, const B: usize>
where
    S: TrustedGet<ContentOnlyChunkSet<B>>,
{
    Plain(FileStream<S, Plain, B>),
    Encrypted(FileStream<S, Encrypted, B>),
}

/// Ordered stream of byte runs over one clipped range; consecutive items
/// tile the range gaplessly.
///
/// Runtime-free: no spawns, threads or timers, so the stream drains under
/// any single-threaded executor, wasm32 included.
///
/// ```
/// use futures_util::StreamExt;
/// use nectar_file::{File, Policy};
/// use nectar_primitives::chunk::AnyChunkSet;
/// use nectar_primitives::store::{ContentGet, MemoryStore};
/// use std::sync::Arc;
///
/// # nectar_testing::run(async {
/// let data: Vec<u8> = (0u32..20_000).map(|i| u8::try_from(i % 251).unwrap()).collect();
/// let store = Arc::new(MemoryStore::<AnyChunkSet<4096>>::new());
/// let root = File::<_, 4096>::new(Arc::clone(&store), Policy::DEFAULT)
///     .save(&data[..])
///     .await
///     .unwrap();
///
/// let file = File::<_, 4096>::new(ContentGet::new(store), Policy::DEFAULT);
/// let reader = file.open_range(root.into(), 4_096..12_288).await.unwrap();
/// let mut stream = reader.into_segments();
/// let mut out = Vec::new();
/// while let Some(run) = stream.next().await {
///     out.extend_from_slice(&run.unwrap());
/// }
/// assert_eq!(out, &data[4_096..12_288]);
/// # });
/// ```
pub struct Segments<S, const B: usize = DEFAULT_BODY_SIZE>
where
    S: TrustedGet<ContentOnlyChunkSet<B>>,
{
    inner: SegmentsInner<S, B>,
}

/// Movable regardless of the store type: the stream owns plain state and
/// boxed futures, never a self-reference.
impl<S, const B: usize> Unpin for Segments<S, B> where S: TrustedGet<ContentOnlyChunkSet<B>> {}

impl<S, const B: usize> Stream for Segments<S, B>
where
    S: TrustedGet<ContentOnlyChunkSet<B>> + Clone + 'static,
{
    type Item = Result<Bytes, WalkError<S::Error>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut self.get_mut().inner {
            SegmentsInner::Plain(stream) => Pin::new(stream).poll_next(cx),
            SegmentsInner::Encrypted(stream) => Pin::new(stream).poll_next(cx),
        }
    }
}

impl<S, const B: usize> fmt::Debug for Segments<S, B>
where
    S: TrustedGet<ContentOnlyChunkSet<B>>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Segments").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests;
