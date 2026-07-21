//! Walk-engine oracles: fetch-set equality against a serial walk, counting
//! store, liveness under an adversarial store, occupancy witnesses.

use core::future::{Future, poll_fn};
use core::ops::Range;
use core::pin::Pin;
use core::task::{Context, Poll};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::vec;
use std::vec::Vec;

use bytes::Bytes;
use futures::executor::block_on;
use nectar_primitives::chunk::{
    AnyChunk, AnyChunkSet, Chunk, ChunkAddress, ChunkOps, ContentChunk, Verified,
};
use nectar_primitives::store::{ChunkGet, ChunkStoreError, TrustedGet};

use super::{Frame, Plain, ShapeError, Walk, WalkError};
use crate::config::Window;

/// Tiny body size: fan-out 8, so a few hundred leaves already build a deep
/// tree.
const TINY: usize = 256;
const TINY_U64: u64 = 256;
const BRANCHES: usize = 8;

type TinyRegistry = AnyChunkSet<TINY>;
type TinyChunk = Chunk<Verified, TinyRegistry>;
type TinyWalk<S> = Walk<S, Plain, TINY>;

/// Distinct byte per file position so every leaf (and thus every node
/// address) is unique.
fn fill(len: usize) -> Vec<u8> {
    (0..len as u64)
        .map(|i| (i.wrapping_mul(2_654_435_761) >> 11) as u8)
        .collect()
}

fn seal(content: ContentChunk<TINY>) -> (ChunkAddress, TinyChunk) {
    let address = *content.address();
    let chunk = Chunk::from_envelope(AnyChunk::from(content)).unwrap();
    (address, chunk)
}

struct Tree {
    root: ChunkAddress,
    span: u64,
    chunks: HashMap<ChunkAddress, TinyChunk>,
    /// Leaf address to file offset; total because leaf bodies are unique.
    leaves: HashMap<ChunkAddress, u64>,
}

/// Hand splitter: leaves then packed intermediate levels, spans summed from
/// the children.
fn build_tree(data: &[u8]) -> Tree {
    let mut chunks = HashMap::new();
    let mut leaves = HashMap::new();
    if data.is_empty() {
        let (address, chunk) = seal(ContentChunk::<TINY>::new(Vec::new()).unwrap());
        chunks.insert(address, chunk);
        leaves.insert(address, 0);
        return Tree {
            root: address,
            span: 0,
            chunks,
            leaves,
        };
    }
    let mut level: Vec<(ChunkAddress, u64)> = Vec::new();
    for (index, block) in data.chunks(TINY).enumerate() {
        let (address, chunk) = seal(ContentChunk::<TINY>::new(block.to_vec()).unwrap());
        chunks.insert(address, chunk);
        leaves.insert(address, (index * TINY) as u64);
        level.push((address, block.len() as u64));
    }
    while level.len() > 1 {
        let mut next = Vec::new();
        for group in level.chunks(BRANCHES) {
            // A trailing single chunk carries up unchanged; wrapping it
            // would declare a span its one reference cannot cover.
            if let [only] = group {
                next.push(*only);
                continue;
            }
            let span: u64 = group.iter().map(|(_, s)| *s).sum();
            let mut wire = span.to_le_bytes().to_vec();
            for (address, _) in group {
                wire.extend_from_slice(address.as_bytes());
            }
            let (address, chunk) = seal(ContentChunk::<TINY>::try_from(Bytes::from(wire)).unwrap());
            chunks.insert(address, chunk);
            next.push((address, span));
        }
        level = next;
    }
    let (root, span) = level[0];
    Tree {
        root,
        span,
        chunks,
        leaves,
    }
}

/// Independent serial reference walk: DFS with the same range pruning,
/// returning the fetched addresses in visit order.
fn serial_walk(tree: &Tree, range: Range<u64>) -> Vec<ChunkAddress> {
    let range_end = range.end.min(tree.span);
    let range_start = range.start.min(range_end);
    let mut fetched = Vec::new();
    if range_start >= range_end {
        return fetched;
    }
    let mut stack = vec![(tree.root, 0u64, tree.span)];
    while let Some((address, start, span)) = stack.pop() {
        if start >= range_end || start + span <= range_start {
            continue;
        }
        fetched.push(address);
        if span <= TINY_U64 {
            continue;
        }
        let body = tree.chunks[&address].envelope().data().clone();
        let mut sub = TINY_U64;
        while sub * (BRANCHES as u64) < span {
            sub *= BRANCHES as u64;
        }
        let expected = span.div_ceil(sub);
        let mut children = Vec::new();
        for index in 0..expected {
            let at = index as usize * ChunkAddress::SIZE;
            let raw: [u8; 32] = body[at..at + ChunkAddress::SIZE].try_into().unwrap();
            let child_span = sub.min(span - index * sub);
            children.push((ChunkAddress::new(raw), start + index * sub, child_span));
        }
        for child in children.into_iter().rev() {
            stack.push(child);
        }
    }
    fetched
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

/// Counting store: logs every fetch, resolving after `delay` yields.
#[derive(Clone)]
struct Recording {
    chunks: Arc<HashMap<ChunkAddress, TinyChunk>>,
    log: Arc<Mutex<Vec<ChunkAddress>>>,
    delay: usize,
}

impl Recording {
    fn new(tree: &Tree, delay: usize) -> Self {
        Self {
            chunks: Arc::new(tree.chunks.clone()),
            log: Arc::new(Mutex::new(Vec::new())),
            delay,
        }
    }

    fn log(&self) -> Vec<ChunkAddress> {
        self.log.lock().unwrap().clone()
    }
}

impl ChunkGet<TinyRegistry> for Recording {
    type Trust = Verified;
    type Error = ChunkStoreError;

    async fn get(&self, address: &ChunkAddress) -> Result<TinyChunk, ChunkStoreError> {
        self.log.lock().unwrap().push(*address);
        for _ in 0..self.delay {
            yield_now().await;
        }
        self.chunks
            .get(address)
            .cloned()
            .ok_or_else(|| ChunkStoreError::not_found(address))
    }
}

/// Adversarial store: parks the leaf at `slow_offset` until `gate` other
/// leaves have resolved.
#[derive(Clone)]
struct HeadLast {
    chunks: Arc<HashMap<ChunkAddress, TinyChunk>>,
    leaves: Arc<HashMap<ChunkAddress, u64>>,
    slow_offset: u64,
    released: Arc<Mutex<usize>>,
    gate: usize,
}

impl ChunkGet<TinyRegistry> for HeadLast {
    type Trust = Verified;
    type Error = ChunkStoreError;

    async fn get(&self, address: &ChunkAddress) -> Result<TinyChunk, ChunkStoreError> {
        if self.leaves.get(address) == Some(&self.slow_offset) {
            while *self.released.lock().unwrap() < self.gate {
                yield_now().await;
            }
            for _ in 0..4 {
                yield_now().await;
            }
        } else {
            for _ in 0..4 {
                yield_now().await;
            }
            if self.leaves.contains_key(address) {
                *self.released.lock().unwrap() += 1;
            }
        }
        self.chunks
            .get(address)
            .cloned()
            .ok_or_else(|| ChunkStoreError::not_found(address))
    }
}

/// Hostile store: answers `from` with the chunk stored at `to`.
#[derive(Clone)]
struct Swapped {
    chunks: Arc<HashMap<ChunkAddress, TinyChunk>>,
    from: ChunkAddress,
    to: ChunkAddress,
}

impl ChunkGet<TinyRegistry> for Swapped {
    type Trust = Verified;
    type Error = ChunkStoreError;

    async fn get(&self, address: &ChunkAddress) -> Result<TinyChunk, ChunkStoreError> {
        let target = if *address == self.from {
            self.to
        } else {
            *address
        };
        self.chunks
            .get(&target)
            .cloned()
            .ok_or_else(|| ChunkStoreError::not_found(address))
    }
}

fn walk_range<S>(store: S, tree: &Tree, range: Range<u64>, window: u16) -> TinyWalk<S>
where
    S: TrustedGet<TinyRegistry> + Clone + 'static,
{
    Walk::new(
        store,
        tree.root,
        (),
        tree.span,
        range,
        Window::new(window).unwrap(),
    )
}

fn collect_ordered<S>(walk: &mut TinyWalk<S>) -> Result<Vec<Frame>, WalkError<ChunkStoreError>>
where
    S: TrustedGet<TinyRegistry, Error = ChunkStoreError> + Clone + 'static,
{
    block_on(async {
        let mut frames = Vec::new();
        while let Some(frame) = poll_fn(|cx| walk.poll_next_ordered(cx)).await {
            frames.push(frame?);
        }
        Ok(frames)
    })
}

fn collect_any<S>(walk: &mut TinyWalk<S>) -> Result<Vec<Frame>, WalkError<ChunkStoreError>>
where
    S: TrustedGet<TinyRegistry, Error = ChunkStoreError> + Clone + 'static,
{
    block_on(async {
        let mut frames = Vec::new();
        while let Some(frame) = poll_fn(|cx| walk.poll_next_any(cx)).await {
            frames.push(frame?);
        }
        Ok(frames)
    })
}

/// Assert the frames tile `[start, end)` gaplessly in order and concatenate
/// to `expected`.
fn assert_tiles(frames: &[Frame], start: u64, expected: &[u8]) {
    let mut cursor = start;
    let mut assembled = Vec::new();
    for frame in frames {
        assert_eq!(frame.offset, cursor, "frames must be gapless and ordered");
        assert!(!frame.data.is_empty(), "no empty frames");
        cursor += frame.data.len() as u64;
        assembled.extend_from_slice(&frame.data);
    }
    assert_eq!(assembled, expected);
}

fn sorted(mut addresses: Vec<ChunkAddress>) -> Vec<ChunkAddress> {
    addresses.sort();
    addresses
}

#[test]
fn ordered_read_is_byte_exact() {
    // 100 leaves at fan-out 8: a four-level tree.
    let data = fill(100 * TINY - 77);
    let tree = build_tree(&data);
    let store = Recording::new(&tree, 2);
    let mut walk = walk_range(store, &tree, 0..u64::MAX, 4);
    let frames = collect_ordered(&mut walk).unwrap();
    assert_tiles(&frames, 0, &data);
    assert!(walk.is_finished());
}

#[test]
fn fetch_set_equals_serial_walk() {
    let data = fill(40 * TINY + 100);
    let tree = build_tree(&data);
    let span = tree.span;
    let cases = [
        0..span,
        0..1,
        1000..1001,
        span - 1..span,
        255..513,
        300..2000,
        0..0,
        span..span + 10,
        span / 2..span,
    ];
    for range in cases {
        let store = Recording::new(&tree, 2);
        let mut walk = walk_range(store.clone(), &tree, range.clone(), 3);
        let frames = collect_ordered(&mut walk).unwrap();

        let clipped_end = range.end.min(span);
        let clipped_start = range.start.min(clipped_end);
        assert_tiles(
            &frames,
            clipped_start,
            &data[clipped_start as usize..clipped_end as usize],
        );

        let log = store.log();
        let serial = serial_walk(&tree, range.clone());
        assert_eq!(
            sorted(log.clone()),
            sorted(serial),
            "fetch set diverged for {range:?}"
        );
        let mut dedup = sorted(log.clone());
        dedup.dedup();
        assert_eq!(dedup.len(), log.len(), "a node was fetched twice");
    }
}

#[test]
fn window_one_stays_live_and_serial() {
    let data = fill(30 * TINY + 5);
    let tree = build_tree(&data);
    let store = Recording::new(&tree, 1);
    let mut walk = walk_range(store.clone(), &tree, 0..tree.span, 1);
    let frames = collect_ordered(&mut walk).unwrap();
    assert_tiles(&frames, 0, &data);
    // Descent progresses at the smallest window, one leaf body at a time,
    // and still fetches exactly the serial set.
    assert_eq!(
        sorted(store.log()),
        sorted(serial_walk(&tree, 0..tree.span))
    );
    assert!(walk.stats().peak_occupancy <= 1);
}

#[test]
fn empty_range_and_empty_file_fetch_nothing() {
    let data = fill(10 * TINY);
    let tree = build_tree(&data);
    let store = Recording::new(&tree, 0);
    let mut walk = walk_range(store.clone(), &tree, 5..5, 4);
    assert!(collect_ordered(&mut walk).unwrap().is_empty());
    assert_eq!(store.log().len(), 0);

    let empty = build_tree(&[]);
    let store = Recording::new(&empty, 0);
    let mut walk = walk_range(store.clone(), &empty, 0..u64::MAX, 4);
    assert!(collect_ordered(&mut walk).unwrap().is_empty());
    assert_eq!(store.log().len(), 0);
}

#[test]
fn tail_read_fetches_one_path() {
    // 100 leaves: depth four, so the last byte costs the root, two
    // intermediates, and one leaf.
    let data = fill(100 * TINY);
    let tree = build_tree(&data);
    let store = Recording::new(&tree, 1);
    let mut walk = walk_range(store.clone(), &tree, tree.span - 1..tree.span, 4);
    let frames = collect_ordered(&mut walk).unwrap();
    assert_tiles(&frames, tree.span - 1, &data[data.len() - 1..]);
    assert_eq!(
        store.log().len(),
        4,
        "range pruning must skip every other subtree"
    );
}

#[test]
fn address_mismatch_is_detected() {
    let data = fill(10 * TINY);
    let tree = build_tree(&data);
    let mut offsets: Vec<(u64, ChunkAddress)> = tree.leaves.iter().map(|(a, o)| (*o, *a)).collect();
    offsets.sort();
    let requested = offsets[3].1;
    let substituted = offsets[4].1;
    let store = Swapped {
        chunks: Arc::new(tree.chunks.clone()),
        from: requested,
        to: substituted,
    };
    let mut walk = walk_range(store, &tree, 0..tree.span, 4);
    let error = collect_ordered(&mut walk).unwrap_err();
    match error {
        WalkError::AddressMismatch {
            requested: want,
            returned,
        } => {
            assert_eq!(want, requested);
            assert_eq!(returned, substituted);
        }
        other => panic!("expected AddressMismatch, got {other:?}"),
    }
}

#[test]
fn store_error_is_terminal_without_retry() {
    let data = fill(12 * TINY);
    let mut tree = build_tree(&data);
    let mut offsets: Vec<(u64, ChunkAddress)> = tree.leaves.iter().map(|(a, o)| (*o, *a)).collect();
    offsets.sort();
    let missing = offsets[5].1;
    tree.chunks.remove(&missing);

    let store = Recording::new(&tree, 1);
    let mut walk = walk_range(store.clone(), &tree, 0..tree.span, 4);
    let error = collect_ordered(&mut walk).unwrap_err();
    match error {
        WalkError::Fetch { address, .. } => assert_eq!(address, missing),
        other => panic!("expected Fetch, got {other:?}"),
    }
    let attempts = store.log().iter().filter(|a| **a == missing).count();
    assert_eq!(attempts, 1, "the engine must not retry");
    assert!(walk.is_finished());
    block_on(async {
        assert!(poll_fn(|cx| walk.poll_next_ordered(cx)).await.is_none());
    });
}

#[test]
fn head_last_liveness_under_slow_consumer() {
    // The head leaf resolves last among the leaves the window admits, and
    // the consumer pauses between frames: the walk must still deliver in
    // order without exceeding the window.
    let leaves = 40;
    let window = 4u16;
    let data = fill(leaves * TINY);
    let tree = build_tree(&data);
    let store = HeadLast {
        chunks: Arc::new(tree.chunks.clone()),
        leaves: Arc::new(tree.leaves.clone()),
        slow_offset: 0,
        released: Arc::new(Mutex::new(0)),
        gate: usize::from(window) - 1,
    };
    let mut walk = walk_range(store, &tree, 0..tree.span, window);
    let frames = block_on(async {
        let mut frames = Vec::new();
        while let Some(frame) = poll_fn(|cx| walk.poll_next_ordered(cx)).await {
            frames.push(frame.unwrap());
            for _ in 0..3 {
                yield_now().await;
            }
        }
        frames
    });
    assert_tiles(&frames, 0, &data);
    assert!(
        walk.stats().peak_occupancy <= usize::from(window),
        "occupancy {} exceeded window {window}",
        walk.stats().peak_occupancy
    );
}

#[test]
fn occupancy_witnesses_hold() {
    let leaves = 200;
    let data = fill(leaves * TINY);
    let tree = build_tree(&data);
    for window in [5u16, 32] {
        let store = Recording::new(&tree, 3);
        let mut walk = walk_range(store, &tree, 0..tree.span, window);
        let frames = collect_ordered(&mut walk).unwrap();
        assert_tiles(&frames, 0, &data);
        let stats = walk.stats();
        let budget =
            crate::config::BranchBudget::derive(Window::new(window).unwrap(), BRANCHES as u32);
        assert!(stats.peak_occupancy <= usize::from(window));
        assert!(stats.peak_branch_in_flight <= budget.get() as usize);
        assert!(stats.peak_leaf_frontier <= usize::from(window) + 2 * BRANCHES);
        assert_eq!(
            stats.fetches as usize,
            serial_walk(&tree, 0..tree.span).len()
        );
    }
}

#[test]
fn any_drain_reassembles_the_range() {
    let data = fill(60 * TINY + 9);
    let tree = build_tree(&data);
    let range = 100..15_000u64;
    let store = Recording::new(&tree, 4);
    let mut walk = walk_range(store, &tree, range.clone(), 6);
    let mut frames = collect_any(&mut walk).unwrap();
    frames.sort_by_key(|frame| frame.offset);
    assert_tiles(
        &frames,
        range.start,
        &data[range.start as usize..range.end as usize],
    );
}

#[test]
fn short_intermediate_is_an_arity_error() {
    // A parent claiming three children but carrying two references.
    let mut chunks = HashMap::new();
    let blocks = fill(3 * TINY);
    let (a, ca) = seal(ContentChunk::<TINY>::new(blocks[..TINY].to_vec()).unwrap());
    let (b, cb) = seal(ContentChunk::<TINY>::new(blocks[TINY..2 * TINY].to_vec()).unwrap());
    chunks.insert(a, ca);
    chunks.insert(b, cb);
    let span = 3 * TINY_U64;
    let mut wire = span.to_le_bytes().to_vec();
    wire.extend_from_slice(a.as_bytes());
    wire.extend_from_slice(b.as_bytes());
    let (root, chunk) = seal(ContentChunk::<TINY>::try_from(Bytes::from(wire)).unwrap());
    chunks.insert(root, chunk);

    let store = Recording {
        chunks: Arc::new(chunks),
        log: Arc::new(Mutex::new(Vec::new())),
        delay: 0,
    };
    let mut walk: TinyWalk<_> = Walk::new(store, root, (), span, 0..span, Window::new(4).unwrap());
    let error = collect_ordered(&mut walk).unwrap_err();
    match error {
        WalkError::Shape(ShapeError::Arity { expected, have, .. }) => {
            assert_eq!((expected, have), (3, 2));
        }
        other => panic!("expected Arity, got {other:?}"),
    }
}

#[test]
fn short_leaf_is_a_length_error() {
    // The parent declares a 256-byte child; the referenced leaf carries 100.
    let mut chunks = HashMap::new();
    let blocks = fill(TINY + 100);
    let (a, ca) = seal(ContentChunk::<TINY>::new(blocks[..TINY].to_vec()).unwrap());
    let (b, cb) = seal(ContentChunk::<TINY>::new(blocks[TINY..].to_vec()).unwrap());
    chunks.insert(a, ca);
    chunks.insert(b, cb);
    let span = 2 * TINY_U64;
    let mut wire = span.to_le_bytes().to_vec();
    wire.extend_from_slice(a.as_bytes());
    wire.extend_from_slice(b.as_bytes());
    let (root, chunk) = seal(ContentChunk::<TINY>::try_from(Bytes::from(wire)).unwrap());
    chunks.insert(root, chunk);

    let store = Recording {
        chunks: Arc::new(chunks),
        log: Arc::new(Mutex::new(Vec::new())),
        delay: 0,
    };
    let mut walk: TinyWalk<_> = Walk::new(store, root, (), span, 0..span, Window::new(4).unwrap());
    let error = collect_ordered(&mut walk).unwrap_err();
    match error {
        WalkError::Shape(ShapeError::LeafLength { offset, span, len }) => {
            assert_eq!((offset, span, len), (TINY_U64, TINY_U64, 100));
        }
        other => panic!("expected LeafLength, got {other:?}"),
    }
}

#[test]
fn mid_leaf_range_is_clipped() {
    let data = fill(4 * TINY);
    let tree = build_tree(&data);
    let store = Recording::new(&tree, 1);
    let mut walk = walk_range(store.clone(), &tree, 300..400, 4);
    let frames = collect_ordered(&mut walk).unwrap();
    assert_eq!(frames.len(), 1);
    assert_tiles(&frames, 300, &data[300..400]);
    // Root plus the single overlapping leaf.
    assert_eq!(store.log().len(), 2);
}
