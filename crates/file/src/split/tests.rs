//! Split-engine oracles: root and chunk-set differentials against the
//! legacy buffered splitter, put-window witnesses, cancellation and fuse
//! behaviour.

#![allow(deprecated)]

use core::future::{Future, poll_fn};
use core::pin::Pin;
use core::task::{Context, Poll};
use std::collections::HashMap;
use std::io::Write as _;
use std::string::ToString;
use std::sync::{Arc, Mutex};
use std::vec;
use std::vec::Vec;

use futures::executor::block_on;
use futures::task::noop_waker;
use nectar_primitives::chunk::{AnyChunkSet, Chunk, ChunkAddress, ChunkOps, Verified};
use nectar_primitives::file::Splitter;
use nectar_primitives::store::{ChunkGet, ChunkPut, ChunkStoreError};

use super::{Split, SplitError, SplitStats};
use crate::config::{PutWindow, Window};
use crate::walk::{Plain, Walk};

/// Tiny body size: fan-out 8, so a few dozen leaves already build a deep
/// tree.
const TINY: usize = 256;
const BRANCHES: usize = 8;

type TinySplit = Split<TestStore<TINY>, Plain, TINY>;

/// Distinct byte per file position so every node address is unique.
fn fill(len: usize) -> Vec<u8> {
    (0..len as u64).map(pattern).collect()
}

/// The byte at absolute file position `i`.
fn pattern(i: u64) -> u8 {
    (i.wrapping_mul(2_654_435_761) >> 11) as u8
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

/// Shared put store: logs accepted puts in order, resolves after `delay`
/// yields, parks while `gate` is shut, refuses puts past `fail_after`.
struct TestStore<const B: usize> {
    chunks: Arc<Mutex<HashMap<ChunkAddress, Chunk<Verified, AnyChunkSet<B>>>>>,
    log: Arc<Mutex<Vec<ChunkAddress>>>,
    delay: usize,
    gate: Option<Arc<Mutex<bool>>>,
    fail_after: Option<usize>,
}

impl<const B: usize> Clone for TestStore<B> {
    fn clone(&self) -> Self {
        Self {
            chunks: Arc::clone(&self.chunks),
            log: Arc::clone(&self.log),
            delay: self.delay,
            gate: self.gate.clone(),
            fail_after: self.fail_after,
        }
    }
}

impl<const B: usize> TestStore<B> {
    fn new(delay: usize) -> Self {
        Self {
            chunks: Arc::new(Mutex::new(HashMap::new())),
            log: Arc::new(Mutex::new(Vec::new())),
            delay,
            gate: None,
            fail_after: None,
        }
    }

    fn gated(gate: Arc<Mutex<bool>>) -> Self {
        Self {
            gate: Some(gate),
            ..Self::new(0)
        }
    }

    fn failing_after(fail_after: usize) -> Self {
        Self {
            fail_after: Some(fail_after),
            ..Self::new(0)
        }
    }

    fn log(&self) -> Vec<ChunkAddress> {
        self.log.lock().unwrap().clone()
    }
}

impl<const B: usize> ChunkPut<AnyChunkSet<B>> for TestStore<B> {
    type Error = ChunkStoreError;

    async fn put(&self, chunk: Chunk<Verified, AnyChunkSet<B>>) -> Result<(), ChunkStoreError> {
        if let Some(gate) = &self.gate {
            while !*gate.lock().unwrap() {
                yield_now().await;
            }
        }
        for _ in 0..self.delay {
            yield_now().await;
        }
        if let Some(limit) = self.fail_after
            && self.log.lock().unwrap().len() >= limit
        {
            return Err(ChunkStoreError::Other("put refused".to_string().into()));
        }
        let address = *chunk.address();
        self.log.lock().unwrap().push(address);
        self.chunks.lock().unwrap().insert(address, chunk);
        Ok(())
    }
}

impl<const B: usize> ChunkGet<AnyChunkSet<B>> for TestStore<B> {
    type Trust = Verified;
    type Error = ChunkStoreError;

    async fn get(
        &self,
        address: &ChunkAddress,
    ) -> Result<Chunk<Verified, AnyChunkSet<B>>, ChunkStoreError> {
        self.chunks
            .lock()
            .unwrap()
            .get(address)
            .cloned()
            .ok_or_else(|| ChunkStoreError::not_found(address))
    }
}

/// The legacy buffered splitter as the oracle: root plus every produced
/// chunk address.
fn legacy_split<const B: usize>(data: &[u8]) -> (ChunkAddress, Vec<ChunkAddress>) {
    let mut splitter = Splitter::<B>::new(data.len() as u64);
    splitter.write_all(data).unwrap();
    let (root, chunks) = splitter.finish().unwrap();
    let addresses = chunks.iter().map(|chunk| *chunk.address()).collect();
    (root, addresses)
}

/// Stream `data` through a fresh split in `step`-byte writes.
fn stream_split<const B: usize>(
    data: &[u8],
    window: u16,
    step: usize,
    delay: usize,
) -> (ChunkAddress, TestStore<B>, SplitStats) {
    let store = TestStore::<B>::new(delay);
    let mut split: Split<TestStore<B>, Plain, B> =
        Split::new(store.clone(), PutWindow::new(window).unwrap());
    let root = block_on(async {
        for piece in data.chunks(step.max(1)) {
            let mut buf = piece;
            while !buf.is_empty() {
                let n = poll_fn(|cx| split.poll_write(cx, buf)).await.unwrap();
                buf = &buf[n..];
            }
        }
        poll_fn(|cx| split.poll_finish(cx)).await.unwrap()
    });
    let stats = split.stats();
    (root, store, stats)
}

fn sorted(mut addresses: Vec<ChunkAddress>) -> Vec<ChunkAddress> {
    addresses.sort();
    addresses
}

#[test]
fn roots_and_chunk_sets_match_the_legacy_splitter() {
    let b = TINY;
    let kb = BRANCHES * b;
    let k2b = BRANCHES * kb;
    let sizes = [
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
    ];
    for size in sizes {
        let data = fill(size);
        let (legacy_root, legacy_chunks) = legacy_split::<TINY>(&data);
        let (root, store, stats) = stream_split::<TINY>(&data, 4, 719, 1);
        assert_eq!(root, legacy_root, "root diverged at {size}");
        assert_eq!(
            sorted(store.log()),
            sorted(legacy_chunks),
            "chunk set diverged at {size}"
        );
        assert_eq!(stats.bytes, size as u64);
        assert_eq!(stats.leaves + stats.intermediates, stats.puts);
    }
}

#[test]
fn default_profile_roots_match_the_legacy_splitter() {
    const B: usize = nectar_primitives::DEFAULT_BODY_SIZE;
    const K: usize = 128;
    let sizes = [0, 1, B - 1, B, B + 1, K * B - 1, K * B, K * B + 1];
    for size in sizes {
        let data = fill(size);
        let (legacy_root, _) = legacy_split::<B>(&data);
        let (root, _, _) = stream_split::<B>(&data, 8, 65_536, 0);
        assert_eq!(root, legacy_root, "root diverged at {size}");
    }
}

#[test]
fn split_then_walk_round_trips() {
    let data = fill(37 * TINY + 41);
    let (root, store, _) = stream_split::<TINY>(&data, 4, usize::MAX, 1);
    let mut walk: Walk<TestStore<TINY>, Plain, TINY> = Walk::new(
        store,
        root,
        (),
        data.len() as u64,
        0..u64::MAX,
        Window::new(4).unwrap(),
    );
    let bytes = block_on(async {
        let mut bytes = Vec::new();
        while let Some(frame) = poll_fn(|cx| walk.poll_next_ordered(cx)).await {
            bytes.extend_from_slice(&frame.unwrap().data);
        }
        bytes
    });
    assert_eq!(bytes, data);
}

#[test]
fn put_window_witnesses_hold() {
    let data = fill(200 * TINY + 63);
    for window in [1u16, 4, 16] {
        let (_, store, stats) = stream_split::<TINY>(&data, window, 997, 3);
        assert!(
            stats.peak_put_in_flight <= usize::from(window),
            "in-flight {} exceeded window {window}",
            stats.peak_put_in_flight
        );
        assert!(
            stats.peak_pending <= stats.peak_spine,
            "pending {} exceeded the spine height {}",
            stats.peak_pending,
            stats.peak_spine
        );
        assert_eq!(stats.leaves + stats.intermediates, stats.puts);
        assert_eq!(store.log().len() as u64, stats.puts);
    }
}

#[test]
fn write_secures_capacity_before_consuming() {
    let gate = Arc::new(Mutex::new(false));
    let store = TestStore::<TINY>::gated(Arc::clone(&gate));
    let mut split: TinySplit = Split::new(store, PutWindow::new(1).unwrap());
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);

    // First leaf: capacity is free, bytes are consumed, the put parks.
    let data = fill(2 * TINY);
    let first = split.poll_write(&mut cx, &data[..TINY]);
    assert!(matches!(first, Poll::Ready(Ok(TINY))));
    assert_eq!(split.stats().bytes, TINY as u64);

    // Window full: repeated polls consume nothing.
    for _ in 0..5 {
        assert!(split.poll_write(&mut cx, &data[TINY..]).is_pending());
        assert_eq!(split.stats().bytes, TINY as u64);
    }

    // Opening the gate frees the slot and the write proceeds.
    *gate.lock().unwrap() = true;
    let resumed = split.poll_write(&mut cx, &data[TINY..]);
    assert!(matches!(resumed, Poll::Ready(Ok(TINY))));
    assert_eq!(split.stats().bytes, 2 * TINY as u64);
}

#[test]
fn empty_write_consumes_nothing() {
    let store = TestStore::<TINY>::new(0);
    let mut split: TinySplit = Split::new(store, PutWindow::DEFAULT);
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    assert!(matches!(split.poll_write(&mut cx, &[]), Poll::Ready(Ok(0))));
    assert_eq!(split.stats().bytes, 0);
}

#[test]
fn a_failed_put_poisons_the_fuse() {
    let store = TestStore::<TINY>::failing_after(0);
    let mut split: TinySplit = Split::new(store, PutWindow::new(2).unwrap());
    let data = fill(3 * TINY);
    let error = block_on(async {
        let mut buf = data.as_slice();
        loop {
            match poll_fn(|cx| split.poll_write(cx, buf)).await {
                Ok(n) => buf = &buf[n..],
                Err(error) => break error,
            }
            if buf.is_empty() {
                break poll_fn(|cx| split.poll_finish(cx)).await.unwrap_err();
            }
        }
    });
    assert!(matches!(error, SplitError::Put { .. }), "got {error:?}");

    // Every later poll returns the poisoned fuse.
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    assert!(matches!(
        split.poll_write(&mut cx, &data),
        Poll::Ready(Err(SplitError::Poisoned))
    ));
    assert!(matches!(
        split.poll_finish(&mut cx),
        Poll::Ready(Err(SplitError::Poisoned))
    ));
    assert!(split.is_finished());
}

#[test]
fn finish_is_fused_and_recallable() {
    let data = fill(20 * TINY + 3);
    let store = TestStore::<TINY>::new(2);
    let mut split: TinySplit = Split::new(store, PutWindow::new(2).unwrap());
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);

    let mut buf = data.as_slice();
    let mut polls = 0usize;
    while !buf.is_empty() {
        match split.poll_write(&mut cx, buf) {
            Poll::Ready(Ok(n)) => buf = &buf[n..],
            Poll::Ready(Err(error)) => panic!("write failed: {error:?}"),
            Poll::Pending => {
                polls += 1;
                assert!(polls < 100_000, "write stuck");
            }
        }
    }

    // Finish across many abandoned polls: progress is never lost.
    let root = loop {
        match split.poll_finish(&mut cx) {
            Poll::Ready(result) => break result.unwrap(),
            Poll::Pending => {
                polls += 1;
                assert!(polls < 100_000, "finish stuck");
            }
        }
    };
    assert!(split.is_finished());

    // Re-callable: the fused root is delivered again, and writes refuse.
    let again = split.poll_finish(&mut cx);
    assert!(matches!(again, Poll::Ready(Ok(address)) if address == root));
    assert!(matches!(
        split.poll_write(&mut cx, &data),
        Poll::Ready(Err(SplitError::Finished))
    ));

    let (legacy_root, _) = legacy_split::<TINY>(&data);
    assert_eq!(root, legacy_root);
}

/// Nightly gate: a stream past the `u32` span boundary keeps root equality
/// with the legacy splitter while memory stays bounded.
#[test]
#[ignore = "nightly: streams more than 4 GiB"]
fn huge_stream_root_matches_the_legacy_splitter() {
    use nectar_primitives::file::{ParallelSplitter, ReadAt};

    const B: usize = nectar_primitives::DEFAULT_BODY_SIZE;
    let size: u64 = (1u64 << 32) + (B as u64) + 17;

    /// Deterministic source: byte `i` is `pattern(i)`, never materialized.
    struct Pattern {
        len: u64,
    }

    impl ReadAt for Pattern {
        fn read_at(&self, offset: u64, buf: &mut [u8]) -> std::io::Result<usize> {
            let available = self.len.saturating_sub(offset);
            let take = buf.len().min(available as usize);
            for (i, slot) in buf[..take].iter_mut().enumerate() {
                *slot = pattern(offset + i as u64);
            }
            Ok(take)
        }

        fn len(&self) -> u64 {
            self.len
        }
    }

    #[derive(Clone)]
    struct Discard;

    impl ChunkPut<AnyChunkSet<B>> for Discard {
        type Error = std::convert::Infallible;

        async fn put(&self, _chunk: Chunk<Verified, AnyChunkSet<B>>) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    let legacy_root =
        ParallelSplitter::<B>::split_into(&Pattern { len: size }, |_chunk| {}).unwrap();

    let mut split: Split<Discard, Plain, B> = Split::new(Discard, PutWindow::new(16).unwrap());
    let root = block_on(async {
        let mut buf = vec![0u8; 1 << 20];
        let mut offset = 0u64;
        while offset < size {
            let take = buf.len().min((size - offset) as usize);
            for (i, slot) in buf[..take].iter_mut().enumerate() {
                *slot = pattern(offset + i as u64);
            }
            let mut piece = &buf[..take];
            while !piece.is_empty() {
                let n = poll_fn(|cx| split.poll_write(cx, piece)).await.unwrap();
                piece = &piece[n..];
            }
            offset += take as u64;
        }
        poll_fn(|cx| split.poll_finish(cx)).await.unwrap()
    });

    assert_eq!(root, legacy_root);
    assert_eq!(split.stats().bytes, size);
}
