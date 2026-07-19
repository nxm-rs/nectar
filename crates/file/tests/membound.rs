//! Memory-bound witnesses for the file pipeline, through the public API.
//!
//! Each test pins one row of the pipeline's memory-bound contract: opening
//! retains one body, a read holds at most the fetch window in leaf bodies
//! plus the derived branch budget, buffered leaf references stay within the
//! window plus twice the fan-out, a bounded collect refuses an oversized
//! range before any fetch and tiles its buffer whatever the completion
//! order, and a write holds at most the put window in
//! flight with spill bounded by the spine height. Peaks are read off the
//! engines' stats and off a gauge store metering concurrent operations
//! independently of the engines' own accounting. The bounds are witnessed
//! at adversarial boundary sizes, range positions and completion orders,
//! not proven.

// Bench, example, and integration-test code: unwraps, direct indexing,
// casts, and assertions are setup and illustration, not shipped surface.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::panic,
    clippy::panic_in_result_fn,
    clippy::as_conversions,
    clippy::missing_panics_doc
)]

use core::future::{Future, poll_fn};
use core::pin::Pin;
use core::sync::atomic::{AtomicUsize, Ordering};
use core::task::{Context, Poll};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use futures::StreamExt;
use futures::executor::block_on;
use nectar_file::{
    BranchBudget, CollectError, File, FileReader, Frame, MemSink, Plain, PutWindow, Split,
    SplitStats, WalkMode, WalkStats, Window,
};
use nectar_primitives::chunk::{AnyChunkSet, Chunk, ChunkAddress, Verified};
use nectar_primitives::store::{ChunkGet, ChunkPut, ChunkStoreError};

/// Tiny body size: fan-out 8, so a few hundred leaves already build a deep
/// tree.
const TINY: usize = 256;
const BRANCHES: usize = 8;

type PlainFile = File<GaugeStore<TINY>, Plain, TINY>;

/// Distinct byte per file position so every node address is unique.
fn fill(len: usize) -> Vec<u8> {
    (0..len as u64)
        .map(|i| (i.wrapping_mul(2_654_435_761) >> 11) as u8)
        .collect()
}

/// A self-waking yield: `Pending` once with an immediate wake, so
/// `block_on` keeps polling.
fn yield_now() -> impl Future<Output = ()> {
    struct YieldNow(bool);
    impl Future for YieldNow {
        type Output = ();
        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            if self.0 {
                Poll::Ready(())
            } else {
                self.0 = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
    YieldNow(false)
}

/// Concurrency meter: a gauge of operations in flight, its peak, and a
/// running total.
#[derive(Debug, Default)]
struct Meter {
    in_flight: AtomicUsize,
    peak: AtomicUsize,
    total: AtomicUsize,
}

impl Meter {
    fn enter(&self) {
        let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(now, Ordering::SeqCst);
        self.total.fetch_add(1, Ordering::SeqCst);
    }

    fn exit(&self) {
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
    }

    fn peak(&self) -> usize {
        self.peak.load(Ordering::SeqCst)
    }

    fn total(&self) -> usize {
        self.total.load(Ordering::SeqCst)
    }
}

/// Parks one fetched address until `gate` other fetches have resolved.
#[derive(Debug, Clone)]
struct Park {
    address: ChunkAddress,
    released: Arc<AtomicUsize>,
    gate: usize,
}

/// Shared chunk store metering concurrent gets and puts independently of
/// the engines' stats; `delay` yields per operation force overlap, and an
/// optional [`Park`] holds one fetch back adversarially.
#[derive(Clone)]
struct GaugeStore<const B: usize> {
    chunks: Arc<Mutex<HashMap<ChunkAddress, Chunk<Verified, AnyChunkSet<B>>>>>,
    /// Accepted puts in first-poll order; the head leaf is the first entry.
    log: Arc<Mutex<Vec<ChunkAddress>>>,
    gets: Arc<Meter>,
    puts: Arc<Meter>,
    delay: usize,
    park: Option<Park>,
}

impl<const B: usize> GaugeStore<B> {
    fn new(delay: usize) -> Self {
        Self {
            chunks: Arc::new(Mutex::new(HashMap::new())),
            log: Arc::new(Mutex::new(Vec::new())),
            gets: Arc::new(Meter::default()),
            puts: Arc::new(Meter::default()),
            delay,
            park: None,
        }
    }

    /// A handle over the same chunks with fresh meters, so a read is
    /// metered apart from the write that built the tree.
    fn fresh(&self, delay: usize) -> Self {
        Self {
            chunks: Arc::clone(&self.chunks),
            log: Arc::new(Mutex::new(Vec::new())),
            gets: Arc::new(Meter::default()),
            puts: Arc::new(Meter::default()),
            delay,
            park: None,
        }
    }

    /// Park fetches of `address` until `gate` other fetches have resolved.
    fn parked(mut self, address: ChunkAddress, gate: usize) -> Self {
        self.park = Some(Park {
            address,
            released: Arc::new(AtomicUsize::new(0)),
            gate,
        });
        self
    }

    /// The first put the store accepted: the leaf at offset zero.
    fn first_put(&self) -> ChunkAddress {
        self.log.lock().unwrap()[0]
    }

    fn chunk_count(&self) -> usize {
        self.chunks.lock().unwrap().len()
    }
}

impl<const B: usize> ChunkGet<AnyChunkSet<B>> for GaugeStore<B> {
    type Trust = Verified;
    type Error = ChunkStoreError;

    async fn get(
        &self,
        address: &ChunkAddress,
    ) -> Result<Chunk<Verified, AnyChunkSet<B>>, ChunkStoreError> {
        self.gets.enter();
        let held = self
            .park
            .as_ref()
            .filter(|park| park.address == *address)
            .cloned();
        if let Some(park) = held {
            while park.released.load(Ordering::SeqCst) < park.gate {
                yield_now().await;
            }
        }
        for _ in 0..self.delay {
            yield_now().await;
        }
        let result = self
            .chunks
            .lock()
            .unwrap()
            .get(address)
            .cloned()
            .ok_or_else(|| ChunkStoreError::not_found(address));
        if result.is_ok()
            && let Some(park) = &self.park
            && park.address != *address
        {
            park.released.fetch_add(1, Ordering::SeqCst);
        }
        self.gets.exit();
        result
    }
}

impl<const B: usize> ChunkPut<AnyChunkSet<B>> for GaugeStore<B> {
    type Error = ChunkStoreError;

    async fn put(&self, chunk: Chunk<Verified, AnyChunkSet<B>>) -> Result<(), ChunkStoreError> {
        self.puts.enter();
        let address = *chunk.address();
        self.log.lock().unwrap().push(address);
        for _ in 0..self.delay {
            yield_now().await;
        }
        self.chunks.lock().unwrap().insert(address, chunk);
        self.puts.exit();
        Ok(())
    }
}

/// Stream `data` through a plain split, returning the root, the metered
/// store that accepted the puts, and the split's witnesses.
fn split_plain(
    data: &[u8],
    window: u16,
    delay: usize,
) -> (ChunkAddress, GaugeStore<TINY>, SplitStats) {
    let store = GaugeStore::new(delay);
    let mut split: Split<GaugeStore<TINY>, Plain, TINY> =
        Split::new(store.clone(), PutWindow::new(window).unwrap());
    let root = block_on(async {
        let mut buf = data;
        while !buf.is_empty() {
            let n = poll_fn(|cx| split.poll_write(cx, buf)).await.unwrap();
            buf = &buf[n..];
        }
        poll_fn(|cx| split.poll_finish(cx)).await.unwrap()
    });
    let stats = split.stats();
    (root, store, stats)
}

fn open_plain(store: GaugeStore<TINY>, root: ChunkAddress) -> PlainFile {
    block_on(File::open(store, root)).unwrap()
}

/// Drain a reader in order, pausing `pause` yields per segment to model a
/// slow consumer.
fn drain_reader<M: WalkMode>(
    reader: &mut FileReader<GaugeStore<TINY>, M, TINY>,
    pause: usize,
) -> Vec<u8> {
    block_on(async {
        let mut out = Vec::new();
        while let Some(segment) = reader.next_segment().await {
            out.extend_from_slice(&segment.unwrap());
            for _ in 0..pause {
                yield_now().await;
            }
        }
        out
    })
}

/// File sizes at the leaf and fan-out boundaries, one byte either side of
/// each.
fn boundary_sizes(branches: usize) -> Vec<usize> {
    let b = TINY;
    let kb = branches * b;
    let k2b = branches * kb;
    vec![
        0,
        1,
        b - 1,
        b,
        b + 1,
        kb - 1,
        kb,
        kb + 1,
        k2b - 1,
        k2b,
        k2b + 1,
        3 * kb + 517,
    ]
}

/// Chunk levels of the tree over `size` bytes: the open-level bound.
fn chunk_depth(size: usize, branches: usize) -> usize {
    let leaves = size.div_ceil(TINY).max(1);
    let mut depth = 1;
    let mut capacity = 1usize;
    while capacity < leaves {
        capacity *= branches;
        depth += 1;
    }
    depth
}

/// The derived branch budget for one window and fan-out.
fn budget(window: u16, branches: u32) -> usize {
    usize::try_from(BranchBudget::derive(Window::new(window).unwrap(), branches).get()).unwrap()
}

/// Occupancy, budget and frontier witnesses of one drained read.
fn assert_read_bounds(
    stats: &WalkStats,
    store: &GaugeStore<TINY>,
    window: u16,
    branches: usize,
    what: &str,
) {
    let w = usize::from(window);
    let budget = budget(window, u32::try_from(branches).unwrap());
    assert!(
        stats.peak_occupancy <= w,
        "{what}: occupancy {} exceeded window {w}",
        stats.peak_occupancy
    );
    assert!(
        stats.peak_branch_in_flight <= budget,
        "{what}: branch in flight {} exceeded budget {budget}",
        stats.peak_branch_in_flight
    );
    assert!(
        stats.peak_leaf_frontier <= w + 2 * branches,
        "{what}: leaf frontier {} exceeded {w} + 2 * {branches}",
        stats.peak_leaf_frontier
    );
    assert!(
        store.gets.peak() <= w + budget,
        "{what}: {} concurrent fetches exceeded {w} + {budget}",
        store.gets.peak()
    );
}

#[test]
fn open_fetches_exactly_one_chunk() {
    for size in boundary_sizes(BRANCHES) {
        let data = fill(size);
        let (root, built, _) = split_plain(&data, 4, 0);
        let store = built.fresh(0);
        let file = open_plain(store.clone(), root);
        assert_eq!(file.len(), size as u64);
        assert_eq!(
            store.gets.total(),
            1,
            "open fetched more than the root at {size}"
        );
        assert_eq!(store.gets.peak(), 1);
    }
}

#[test]
fn reader_holds_window_and_budget_at_every_boundary_size() {
    for size in boundary_sizes(BRANCHES) {
        let data = fill(size);
        let (root, built, _) = split_plain(&data, 8, 0);
        let chunk_count = built.chunk_count();
        for window in [1u16, 3, 16] {
            let store = built.fresh(2);
            let file = open_plain(store.clone(), root);
            let mut reader = file.read().window(Window::new(window).unwrap()).build();
            let out = drain_reader(&mut reader, 0);
            assert_eq!(out, data, "bytes diverged at {size}/{window}");
            let stats = reader.stats();
            assert_read_bounds(&stats, &store, window, BRANCHES, "reader");
            // Every node once: the walk retains frames and references,
            // never the tree.
            let expected = if size == 0 { 0 } else { chunk_count };
            assert_eq!(stats.fetches as usize, expected);
            assert_eq!(store.gets.total(), expected + 1);
        }
    }
}

#[test]
fn reader_bounds_hold_at_adversarial_range_positions() {
    let kb = (BRANCHES * TINY) as u64;
    let k2b = BRANCHES as u64 * kb;
    let size = 2 * k2b as usize + 3 * kb as usize + 517;
    let data = fill(size);
    let (root, built, _) = split_plain(&data, 8, 0);
    let span = size as u64;
    let ranges = [
        0..1,
        TINY as u64 - 1..TINY as u64 + 1,
        kb - 1..kb + 1,
        k2b - 1..k2b + 1,
        k2b..k2b,
        span - 1..span,
        span..span + 10,
        300..400,
        kb - 1..2 * kb + 1,
        0..span,
    ];
    for range in ranges {
        for window in [2u16, 4] {
            let store = built.fresh(2);
            let file = open_plain(store.clone(), root);
            let mut reader = file
                .read()
                .window(Window::new(window).unwrap())
                .range(range.clone())
                .build();
            let out = drain_reader(&mut reader, 0);
            let clipped_end = range.end.min(span) as usize;
            let clipped_start = (range.start as usize).min(clipped_end);
            assert_eq!(
                out,
                &data[clipped_start..clipped_end],
                "bytes diverged for {range:?}/{window}"
            );
            let stats = reader.stats();
            assert_read_bounds(&stats, &store, window, BRANCHES, "ranged reader");
        }
    }
}

#[test]
fn frames_and_download_hold_the_window_out_of_order() {
    let size = 60 * TINY + 9;
    let data = fill(size);
    let (root, built, _) = split_plain(&data, 8, 0);
    let window = 6u16;

    // Completion-order frames: the range is tiled exactly once whatever the
    // arrival order, within the same window and budget.
    let store = built.fresh(3);
    let file = open_plain(store.clone(), root);
    let mut frames = file
        .read()
        .window(Window::new(window).unwrap())
        .range(100..15_000)
        .frames();
    let clipped = frames.range();
    let mut collected: Vec<Frame> = block_on(async {
        let mut out = Vec::new();
        while let Some(frame) = frames.next().await {
            out.push(frame.unwrap());
        }
        out
    });
    collected.sort_by_key(|frame| frame.offset);
    let mut cursor = clipped.start;
    let mut assembled = Vec::new();
    for frame in &collected {
        assert_eq!(frame.offset, cursor, "frames must tile the range once");
        cursor += frame.data.len() as u64;
        assembled.extend_from_slice(&frame.data);
    }
    assert_eq!(cursor, clipped.end);
    assert_eq!(assembled, &data[100..15_000]);
    let stats = frames.stats();
    assert_read_bounds(&stats, &store, window, BRANCHES, "frames");

    // The download drain rides the same walk, so the same bounds hold while
    // frames land in the sink.
    let store = built.fresh(3);
    let file = open_plain(store.clone(), root);
    let mut sink = MemSink::new();
    let written = block_on(
        file.download()
            .window(Window::new(window).unwrap())
            .range(100..15_000)
            .run(&mut sink),
    )
    .unwrap();
    assert_eq!(written, 14_900);
    assert_eq!(sink.as_ref(), &data[100..15_000]);
    assert!(
        store.gets.peak() <= usize::from(window) + budget(window, BRANCHES as u32),
        "download fetches {} burst the window",
        store.gets.peak()
    );
}

#[test]
fn parked_head_leaf_never_bursts_the_window() {
    // The head leaf resolves last among the fetches the window admits and
    // the consumer pauses between segments: buffered out-of-order bodies
    // plus the parked head must still fit the window.
    let size = 40 * TINY;
    let data = fill(size);
    let (root, built, _) = split_plain(&data, 8, 0);
    let head = built.first_put();
    let window = 4u16;
    let store = built.fresh(4).parked(head, usize::from(window) - 1);
    let file = open_plain(store.clone(), root);
    let mut reader = file.read().window(Window::new(window).unwrap()).build();
    let out = drain_reader(&mut reader, 3);
    assert_eq!(out, data);
    let stats = reader.stats();
    assert_read_bounds(&stats, &store, window, BRANCHES, "parked head");
}

#[test]
fn collect_refuses_an_oversized_range_before_any_fetch() {
    let size = 20 * TINY + 11;
    let data = fill(size);
    let (root, built, _) = split_plain(&data, 4, 0);

    // One byte under the length: a typed refusal before the walk fetches.
    let store = built.fresh(0);
    let file = open_plain(store.clone(), root);
    let error = block_on(file.read().collect(size as u64 - 1)).unwrap_err();
    match error {
        CollectError::TooLarge { len, max } => {
            assert_eq!((len, max), (size as u64, size as u64 - 1));
        }
        other => panic!("expected TooLarge, got {other:?}"),
    }
    assert_eq!(
        store.gets.total(),
        1,
        "the refusal must precede every fetch beyond the open"
    );

    // At the exact bound: assembled once within the window.
    let store = built.fresh(2);
    let file = open_plain(store.clone(), root);
    let window = 4u16;
    let out = block_on(
        file.read()
            .window(Window::new(window).unwrap())
            .collect(size as u64),
    )
    .unwrap();
    assert_eq!(out, data);
    assert!(store.gets.peak() <= usize::from(window) + budget(window, BRANCHES as u32));

    // The empty file collects within a zero bound.
    let (root, built, _) = split_plain(&[], 4, 0);
    let store = built.fresh(0);
    let file = open_plain(store, root);
    assert!(block_on(file.collect(0)).unwrap().is_empty());
}

#[test]
fn collect_assembles_out_of_order_within_the_window() {
    // The head leaf resolves last among the fetches the window admits, so
    // frames land far from file order; the ranged buffer is still tiled
    // exactly once at range-relative offsets, within the window.
    let size = 40 * TINY + 13;
    let data = fill(size);
    let (root, built, _) = split_plain(&data, 8, 0);
    let head = built.first_put();
    let window = 4u16;
    let store = built.fresh(3).parked(head, usize::from(window) - 1);
    let file = open_plain(store.clone(), root);
    let out = block_on(
        file.read()
            .window(Window::new(window).unwrap())
            .range(100..size as u64 - 7)
            .collect(size as u64),
    )
    .unwrap();
    assert_eq!(out, &data[100..size - 7]);
    assert!(store.gets.peak() <= usize::from(window) + budget(window, BRANCHES as u32));
}

#[test]
fn writer_holds_put_window_spine_and_pending_at_every_boundary_size() {
    for size in boundary_sizes(BRANCHES) {
        let data = fill(size);
        for window in [1u16, 2, 16] {
            let (_, store, stats) = split_plain(&data, window, 3);
            let w = usize::from(window);
            assert!(
                stats.peak_put_in_flight <= w,
                "in flight {} exceeded window {w} at {size}",
                stats.peak_put_in_flight
            );
            assert!(
                store.puts.peak() <= w,
                "{} concurrent puts exceeded window {w} at {size}",
                store.puts.peak()
            );
            assert!(
                stats.peak_pending <= stats.peak_spine,
                "pending {} exceeded the spine height {} at {size}",
                stats.peak_pending,
                stats.peak_spine
            );
            assert!(
                stats.peak_spine <= chunk_depth(size, BRANCHES),
                "spine {} exceeded the depth bound {} at {size}",
                stats.peak_spine,
                chunk_depth(size, BRANCHES)
            );
            assert_eq!(stats.bytes, size as u64);
            assert_eq!(stats.puts, stats.leaves + stats.intermediates);
            assert_eq!(store.puts.total() as u64, stats.puts);
        }
    }
}

#[test]
fn peaks_stay_flat_as_the_tree_grows() {
    // Hundreds of chunks against single-digit peaks: the bounds are
    // streaming bounds, not small-tree artefacts.
    let size = BRANCHES * BRANCHES * BRANCHES * TINY + 5 * TINY + 7;
    let data = fill(size);
    let (root, built, write_stats) = split_plain(&data, 8, 1);
    let depth = chunk_depth(size, BRANCHES);
    assert!(write_stats.peak_spine <= depth);
    assert!(write_stats.peak_put_in_flight <= 8);
    assert!(
        write_stats.puts as usize > 10 * write_stats.peak_put_in_flight.max(1),
        "puts {} are not far above the put peak",
        write_stats.puts
    );

    let window = 16u16;
    let store = built.fresh(1);
    let file = open_plain(store.clone(), root);
    let mut reader = file.read().window(Window::new(window).unwrap()).build();
    let out = drain_reader(&mut reader, 0);
    assert_eq!(out, data);
    let stats = reader.stats();
    assert_read_bounds(&stats, &store, window, BRANCHES, "deep tree");
    assert!(
        stats.fetches as usize > 10 * stats.peak_occupancy.max(1),
        "fetches {} are not far above the occupancy peak",
        stats.fetches
    );
    assert!(
        stats.fetches as usize > 10 * stats.peak_leaf_frontier.max(1),
        "fetches {} are not far above the frontier peak",
        stats.fetches
    );
}

/// The encrypted profile halves the fan-out, deepening the tree; the same
/// bounds hold under the narrower geometry.
#[cfg(feature = "encryption")]
mod encrypted {
    use core::future::poll_fn;
    use core::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    use futures::executor::block_on;
    use nectar_file::{Encrypted, File, KeyError, KeySource, PutWindow, Split, SplitStats, Window};
    use nectar_primitives::chunk::encryption::{EncryptedChunkRef, EncryptionKey};

    use super::{
        GaugeStore, TINY, assert_read_bounds, boundary_sizes, chunk_depth, drain_reader, fill,
    };

    /// Encrypted fan-out at the tiny body: four 64-byte references.
    const ENC_BRANCHES: usize = 4;

    /// Deterministic source: counter keys starting at one, shared across
    /// clones.
    #[derive(Debug, Clone, Default)]
    struct SeqKeys(Arc<AtomicU64>);

    impl KeySource for SeqKeys {
        fn next_key(&self) -> Result<EncryptionKey, KeyError> {
            let n = self.0.fetch_add(1, Ordering::Relaxed) + 1;
            let mut bytes = [0u8; EncryptionKey::SIZE];
            bytes[..8].copy_from_slice(&n.to_le_bytes());
            Ok(EncryptionKey::from(bytes))
        }
    }

    /// Stream `data` through an encrypted split, returning the root, the
    /// metered store, and the split's witnesses.
    fn split_encrypted(
        data: &[u8],
        window: u16,
        delay: usize,
    ) -> (EncryptedChunkRef, GaugeStore<TINY>, SplitStats) {
        let store = GaugeStore::new(delay);
        let mut split: Split<GaugeStore<TINY>, Encrypted<SeqKeys>, TINY> = Split::with_mode(
            store.clone(),
            Encrypted::new(SeqKeys::default()),
            PutWindow::new(window).unwrap(),
        );
        let root = block_on(async {
            let mut buf = data;
            while !buf.is_empty() {
                let n = poll_fn(|cx| split.poll_write(cx, buf)).await.unwrap();
                buf = &buf[n..];
            }
            poll_fn(|cx| split.poll_finish(cx)).await.unwrap()
        });
        let stats = split.stats();
        (root, store, stats)
    }

    #[test]
    fn encrypted_bounds_track_the_narrower_fan_out() {
        for size in boundary_sizes(ENC_BRANCHES) {
            let data = fill(size);
            let (root, built, stats) = split_encrypted(&data, 2, 3);
            assert!(
                stats.peak_put_in_flight <= 2,
                "in flight {} exceeded window 2 at {size}",
                stats.peak_put_in_flight
            );
            assert!(built.puts.peak() <= 2);
            assert!(stats.peak_pending <= stats.peak_spine);
            assert!(
                stats.peak_spine <= chunk_depth(size, ENC_BRANCHES),
                "spine {} exceeded the depth bound {} at {size}",
                stats.peak_spine,
                chunk_depth(size, ENC_BRANCHES)
            );

            let store = built.fresh(2);
            let file: File<GaugeStore<TINY>, Encrypted, TINY> =
                block_on(File::open_encrypted(store.clone(), root)).unwrap();
            assert_eq!(store.gets.total(), 1, "open fetched more than the root");
            assert_eq!(file.len(), size as u64);
            let window = 3u16;
            let mut reader = file.read().window(Window::new(window).unwrap()).build();
            let out = drain_reader(&mut reader, 0);
            assert_eq!(out, data, "plaintext diverged at {size}");
            let stats = reader.stats();
            assert_read_bounds(&stats, &store, window, ENC_BRANCHES, "encrypted reader");
        }
    }
}

/// The batch ingest pre-seals leaves on the pool but drains the same put
/// window; the write-side bound survives the parallel path.
#[cfg(feature = "rayon")]
mod batch {
    use futures::executor::block_on;
    use nectar_file::{Plain, PutWindow, split_read_at};

    use super::{BRANCHES, GaugeStore, TINY, fill, split_plain};

    #[test]
    fn batch_ingest_holds_the_put_window() {
        let size = BRANCHES * BRANCHES * TINY + 999;
        let data = fill(size);
        let (streamed_root, streamed_store, _) = split_plain(&data, 4, 0);
        for window in [1u16, 4] {
            let store = GaugeStore::<TINY>::new(2);
            let root = block_on(split_read_at::<_, _, Plain, TINY>(
                data.clone(),
                store.clone(),
                PutWindow::new(window).unwrap(),
            ))
            .unwrap();
            assert_eq!(root, streamed_root, "batch root diverged at {window}");
            assert!(
                store.puts.peak() <= usize::from(window),
                "{} concurrent puts exceeded window {window}",
                store.puts.peak()
            );
            assert_eq!(store.chunk_count(), streamed_store.chunk_count());
        }
    }
}
