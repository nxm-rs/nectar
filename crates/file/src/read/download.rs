//! Restartable download: drain completion-order frames into a data sink.

use alloc::boxed::Box;
use core::fmt;
use core::ops::Range;
use core::time::Duration;

use futures_util::stream::StreamExt;
use nectar_primitives::DEFAULT_BODY_SIZE;
use nectar_primitives::chunk::{AnyChunkSet, ChunkAddress};
use nectar_primitives::store::TrustedGet;

use super::body_size;
use super::error::DownloadError;
use super::frames::FileFrames;
use crate::config::Window;
use crate::num::u64_from_usize;
use crate::sink::DataSink;
use crate::walk::{Walk, WalkMode};

/// Progress snapshot delivered after each frame lands in the sink.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Progress {
    /// Bytes written so far.
    pub written: u64,
    /// Bytes the clipped download covers.
    pub total: u64,
}

/// Boxed progress callback: `Send` on multi-threaded targets, unbounded on
/// wasm32 and under the `unsync` feature.
#[cfg(not(any(target_arch = "wasm32", feature = "unsync")))]
pub type ProgressFn = Box<dyn FnMut(Progress) + Send>;
/// Boxed progress callback: `Send` on multi-threaded targets, unbounded on
/// wasm32 and under the `unsync` feature.
#[cfg(any(target_arch = "wasm32", feature = "unsync"))]
pub type ProgressFn = Box<dyn FnMut(Progress)>;

/// Builder of one download; construction is infallible.
///
/// Ranges use clip semantics like a read, and sink offsets are relative to
/// the clipped range start. A download is restartable, not resumable: after
/// a failure, run it again in full; the sink's idempotent overwrites make
/// the re-run safe.
///
/// ```
/// use nectar_file::{File, MemSink};
///
/// # nectar_testing::run(async {
/// let data: Vec<u8> = (0u32..40_000)
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
/// let mut sink = MemSink::new();
/// let written = file
///     .download()
///     .range(8_192..24_576)
///     .progress(Box::new(|progress| {
///         assert!(progress.written <= progress.total);
///     }))
///     .run(&mut sink)
///     .await
///     .unwrap();
/// assert_eq!(written, 16_384);
/// assert_eq!(sink.as_ref(), &data[8_192..24_576]);
/// # });
/// ```
pub struct DownloadBuilder<S, M: WalkMode, const B: usize = DEFAULT_BODY_SIZE> {
    store: S,
    root: ChunkAddress,
    context: M::Context,
    span: u64,
    window: Window,
    range: Range<u64>,
    progress: Option<ProgressFn>,
}

impl<S, M: WalkMode, const B: usize> DownloadBuilder<S, M, B> {
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
            progress: None,
        }
    }

    /// Fetch window the download drains against.
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

    /// Absolute byte range to download, clipped to the file.
    #[must_use]
    pub const fn range(mut self, range: Range<u64>) -> Self {
        self.range = range;
        self
    }

    /// Report progress after each frame lands in the sink.
    #[must_use]
    pub fn progress(mut self, callback: ProgressFn) -> Self {
        self.progress = Some(callback);
        self
    }
}

impl<S, M, const B: usize> DownloadBuilder<S, M, B>
where
    S: TrustedGet<AnyChunkSet<B>> + Clone + 'static,
    M: WalkMode,
{
    /// Run the download to completion, returning the bytes written.
    ///
    /// Frames land in completion order, each written once at its
    /// range-relative offset; any error is terminal for this run.
    pub async fn run<K: DataSink>(
        self,
        sink: &mut K,
    ) -> Result<u64, DownloadError<S::Error, K::Error>> {
        let mut frames: FileFrames<S, M, B> = FileFrames::new(Walk::new(
            self.store,
            self.root,
            self.context,
            self.span,
            self.range,
            self.window,
        ));
        let clipped = frames.range();
        let total = clipped.end.saturating_sub(clipped.start);
        let mut callback = self.progress;
        let mut written = 0u64;
        while let Some(frame) = frames.next().await {
            let frame = frame?;
            let offset = frame.offset.saturating_sub(clipped.start);
            sink.write_at(offset, frame.data.as_ref())
                .map_err(|source| DownloadError::Sink { offset, source })?;
            written = written.saturating_add(u64_from_usize(frame.data.len()));
            if let Some(callback) = callback.as_mut() {
                callback(Progress { written, total });
            }
        }
        Ok(written)
    }
}

impl<S, M: WalkMode, const B: usize> fmt::Debug for DownloadBuilder<S, M, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DownloadBuilder")
            .field("root", &self.root)
            .field("span", &self.span)
            .field("window", &self.window)
            .field("range", &self.range)
            .field("progress", &self.progress.is_some())
            .finish_non_exhaustive()
    }
}
