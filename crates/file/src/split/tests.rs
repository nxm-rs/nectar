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

/// Encrypted-mode oracles: legacy joiner and splitter differentials, walk
/// round trip, per-mode geometry witnesses, key-source injection and
/// exhaustion.
#[cfg(feature = "encryption")]
mod encrypted {
    use core::future::poll_fn;
    use core::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::vec::Vec;

    use futures::executor::block_on;
    use nectar_primitives::chunk::encryption::{EncryptedChunkRef, EncryptionKey};
    use nectar_primitives::file::{EncryptedParallelSplitter, join};

    use super::{BRANCHES, TINY, TestStore, fill, sorted};
    use crate::config::{PutWindow, Window};
    use crate::geometry::{Mode, branches, max_depth};
    use crate::split::{KeyError, KeySource, RandomKeys, SealError, Split, SplitError, SplitStats};
    use crate::walk::{Encrypted, Walk};

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

    /// Finite source: refuses every key past `limit`.
    #[derive(Debug, Clone)]
    struct LimitedKeys {
        limit: u64,
        issued: Arc<AtomicU64>,
    }

    impl KeySource for LimitedKeys {
        fn next_key(&self) -> Result<EncryptionKey, KeyError> {
            let n = self.issued.fetch_add(1, Ordering::Relaxed);
            if n >= self.limit {
                return Err(KeyError::Exhausted { issued: self.limit });
            }
            let mut bytes = [0u8; EncryptionKey::SIZE];
            bytes[..8].copy_from_slice(&(n + 1).to_le_bytes());
            Ok(EncryptionKey::from(bytes))
        }
    }

    /// Stream `data` through a fresh encrypted split in `step`-byte writes.
    fn stream_split_encrypted<K: KeySource>(
        data: &[u8],
        mode: Encrypted<K>,
        step: usize,
    ) -> (EncryptedChunkRef, TestStore<TINY>, SplitStats) {
        let store = TestStore::<TINY>::new(1);
        let mut split: Split<TestStore<TINY>, Encrypted<K>, TINY> =
            Split::with_mode(store.clone(), mode, PutWindow::new(4).unwrap());
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

    /// Depth-boundary sizes for the encrypted fan-out.
    fn sizes() -> Vec<usize> {
        let b = TINY;
        let kb = ENC_BRANCHES * b;
        let k2b = ENC_BRANCHES * kb;
        std::vec![
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

    #[test]
    fn encrypted_roots_join_through_the_legacy_joiner() {
        for size in sizes() {
            let data = fill(size);
            // The rand-gated default source through the `Default` mode.
            let store = TestStore::<TINY>::new(1);
            let mut split: Split<TestStore<TINY>, Encrypted<RandomKeys>, TINY> =
                Split::new(store.clone(), PutWindow::new(4).unwrap());
            let root = block_on(async {
                let mut buf = data.as_slice();
                while !buf.is_empty() {
                    let n = poll_fn(|cx| split.poll_write(cx, buf)).await.unwrap();
                    buf = &buf[n..];
                }
                poll_fn(|cx| split.poll_finish(cx)).await.unwrap()
            });
            let plaintext = block_on(join(&store, root)).unwrap();
            assert_eq!(plaintext, data, "plaintext diverged at {size}");

            // The tree shape matches the legacy encrypted splitter.
            let (_, legacy_chunks) =
                EncryptedParallelSplitter::<TINY>::split_to_vec(&data).unwrap();
            let stats = split.stats();
            assert_eq!(
                stats.puts as usize,
                legacy_chunks.len(),
                "chunk count diverged at {size}"
            );
            assert_eq!(stats.bytes, size as u64);
            assert_eq!(stats.leaves + stats.intermediates, stats.puts);
        }
    }

    #[test]
    fn encrypted_split_then_walk_round_trips() {
        let size = 13 * TINY + 29;
        let data = fill(size);
        let (root, store, _) =
            stream_split_encrypted(&data, Encrypted::new(SeqKeys::default()), 719);
        let mut walk: Walk<TestStore<TINY>, Encrypted, TINY> = Walk::new(
            store,
            *root.address(),
            root.key().clone(),
            size as u64,
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
    fn per_mode_geometry_diverges_at_the_shared_body() {
        let body = TINY as u32;
        assert_eq!(branches(body, Mode::Plain), BRANCHES as u32);
        assert_eq!(branches(body, Mode::Encrypted), ENC_BRANCHES as u32);
        assert_eq!(max_depth(body, Mode::Plain), 20);
        assert_eq!(max_depth(body, Mode::Encrypted), 29);

        // Sixty-four leaves: a two-level plain tree against a three-level
        // encrypted tree over the same bytes.
        let data = fill(64 * TINY);
        let (_, _, plain_stats) = super::stream_split::<TINY>(&data, 4, 719, 1);
        assert_eq!(plain_stats.leaves, 64);
        assert_eq!(plain_stats.intermediates, 9);
        assert_eq!(plain_stats.peak_spine, 3);

        let (_, _, enc_stats) =
            stream_split_encrypted(&data, Encrypted::new(SeqKeys::default()), 719);
        assert_eq!(enc_stats.leaves, 64);
        assert_eq!(enc_stats.intermediates, 21);
        assert_eq!(enc_stats.peak_spine, 4);
    }

    #[test]
    fn a_deterministic_source_pins_the_padless_tree() {
        // Every node is full at this size, so no random padding enters the
        // ciphertexts and the whole tree is a function of the key stream.
        let data = fill(16 * TINY);
        let (first_root, first_store, _) =
            stream_split_encrypted(&data, Encrypted::new(SeqKeys::default()), 719);
        let (second_root, second_store, _) =
            stream_split_encrypted(&data, Encrypted::new(SeqKeys::default()), 97);
        assert_eq!(first_root, second_root);
        assert_eq!(sorted(first_store.log()), sorted(second_store.log()));
    }

    #[test]
    fn an_exhausted_key_source_poisons_the_split() {
        let data = fill(3 * TINY);
        let source = LimitedKeys {
            limit: 2,
            issued: Arc::new(AtomicU64::new(0)),
        };
        let store = TestStore::<TINY>::new(0);
        let mut split: Split<TestStore<TINY>, Encrypted<LimitedKeys>, TINY> =
            Split::with_mode(store, Encrypted::new(source), PutWindow::new(4).unwrap());
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
        assert!(
            matches!(
                error,
                SplitError::Seal(SealError::Key(KeyError::Exhausted { issued: 2 }))
            ),
            "got {error:?}"
        );

        // The fuse is shut for good.
        let waker = futures::task::noop_waker();
        let mut cx = core::task::Context::from_waker(&waker);
        assert!(matches!(
            split.poll_finish(&mut cx),
            core::task::Poll::Ready(Err(SplitError::Poisoned))
        ));
    }
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
