//! Split-engine oracles: pinned root vectors, segmentation invariance,
//! put-window witnesses, cancellation and fuse behaviour.

use core::future::poll_fn;
use core::task::{Context, Poll};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
#[cfg(all(
    feature = "rayon",
    not(target_arch = "wasm32"),
    not(feature = "unsync")
))]
use std::vec;
use std::vec::Vec;

use futures::task::noop_waker;
use nectar_primitives::chunk::{AnyChunkSet, Chunk, ChunkAddress, Verified};
use nectar_primitives::store::{ChunkGet, ChunkPut, ChunkStoreError};
use nectar_testing::{run, yield_now};

use super::{Split, SplitError, SplitStats};
use crate::config::{PutWindow, Window};
use crate::testutil::{FaultStore, failing_at, reject_all};
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

/// Shared put store: logs accepted puts in order, resolves after `delay`
/// yields, parks while `gate` is shut. Fault injection rides
/// [`FaultStore`](crate::testutil::FaultStore).
struct TestStore<const B: usize> {
    chunks: Arc<Mutex<HashMap<ChunkAddress, Chunk<Verified, AnyChunkSet<B>>>>>,
    log: Arc<Mutex<Vec<ChunkAddress>>>,
    delay: usize,
    gate: Option<Arc<Mutex<bool>>>,
}

impl<const B: usize> Clone for TestStore<B> {
    fn clone(&self) -> Self {
        Self {
            chunks: Arc::clone(&self.chunks),
            log: Arc::clone(&self.log),
            delay: self.delay,
            gate: self.gate.clone(),
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
        }
    }

    fn gated(gate: Arc<Mutex<bool>>) -> Self {
        Self {
            gate: Some(gate),
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

/// Roots pinned from the retired buffered splitter over [`fill`] bytes:
/// the absolute tree-shape anchor.
const TINY_ROOTS: &[(usize, &str)] = &[
    (
        0,
        "b34ca8c22b9e982354f9c7f50b470d66db428d880c8a904d5fe4ec9713171526",
    ),
    (
        1,
        "fe60ba40b87599ddfb9e8947c1c872a4a1a5b56f7d1b80f0a646005b38db52a5",
    ),
    (
        255,
        "2274f71be6e13006f33cae2cffbdb70213a81cb380a54589c8bc8c958988beb7",
    ),
    (
        256,
        "00df5cfbb91a2432d5dec04c8edb74be800236b9b4d86bcd7584e7bda5647eee",
    ),
    (
        257,
        "936af738c97abc8d653bf9f07f53db7eef837ec4b3c3e072906c2e5cb6cce1ec",
    ),
    (
        2047,
        "292bdaa2d157b5d0a86db51b43abf3e55e57d6fe453c080a2123246cfdf5177f",
    ),
    (
        2048,
        "c28c0c6920e4561250217e1917f855e2182b712d13adedf1d5b72ac1028cb7bb",
    ),
    (
        2049,
        "b2c3e42e1ccaee79ee4dfdf2120e73defb7a4e9ed9dda63c53bf6f75aff89748",
    ),
    (
        6661,
        "4263312e2b655c6ffcda9f56342481240eb24f80fe22c0e59339995004196963",
    ),
    (
        16383,
        "adac79432c40ae0c528769c172163f3378cc0cf17a680f2a87dc4236c8ecb606",
    ),
    (
        16384,
        "65dd63a9721ca6e4203b8cd83d679b361fcdb0bcb3c1327765969272ec2d5b79",
    ),
    (
        16385,
        "524b55fa4ceeb29e31dac831afad68510253978155a3140fd5428cf31474fea7",
    ),
];

/// Decode one pinned 64-hex-digit root.
fn pinned(hex: &str) -> ChunkAddress {
    let mut bytes = [0u8; 32];
    for (i, slot) in bytes.iter_mut().enumerate() {
        *slot = u8::from_str_radix(&hex[2 * i..2 * i + 2], 16).unwrap();
    }
    ChunkAddress::new(bytes)
}

/// Chunk count of the tree over `len` bytes: leaves, then each reference
/// level up to the root; a lone trailing reference carries up unwrapped.
fn tree_chunks(len: usize, body: usize, fanout: usize) -> u64 {
    let leaves = len.div_ceil(body).max(1);
    let mut total = leaves;
    let mut refs = leaves;
    while refs > 1 {
        let full = refs / fanout;
        let rem = refs % fanout;
        let chunks = full + usize::from(rem > 1);
        total += chunks;
        refs = chunks + usize::from(rem == 1);
    }
    total as u64
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
    let root = run(async {
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
fn roots_are_pinned_and_chunk_sets_are_segmentation_invariant() {
    for &(size, root_hex) in TINY_ROOTS {
        let data = fill(size);
        let (root, store, stats) = stream_split::<TINY>(&data, 4, 719, 1);
        assert_eq!(root, pinned(root_hex), "root diverged at {size}");
        // A different segmentation, window and put latency seals the same
        // chunk set: the tree is a function of the bytes alone.
        let (again, other, _) = stream_split::<TINY>(&data, 1, 97, 0);
        assert_eq!(again, root, "root diverged across segmentations at {size}");
        assert_eq!(
            sorted(store.log()),
            sorted(other.log()),
            "chunk set diverged at {size}"
        );
        assert_eq!(stats.puts, tree_chunks(size, TINY, BRANCHES));
        assert_eq!(stats.bytes, size as u64);
        assert_eq!(stats.leaves + stats.intermediates, stats.puts);
    }
}

#[test]
fn default_profile_roots_are_pinned() {
    const B: usize = nectar_primitives::DEFAULT_BODY_SIZE;
    /// Roots pinned from the retired buffered splitter at the default body.
    const DEFAULT_ROOTS: &[(usize, &str)] = &[
        (
            0,
            "b34ca8c22b9e982354f9c7f50b470d66db428d880c8a904d5fe4ec9713171526",
        ),
        (
            1,
            "fe60ba40b87599ddfb9e8947c1c872a4a1a5b56f7d1b80f0a646005b38db52a5",
        ),
        (
            4095,
            "fba8ab42bd2b0955350b2192f90be070a2f8a1f5fe3da23ef864b73467b8cc76",
        ),
        (
            4096,
            "8d574088e95657940066cd317ad3f234af570762360d893b901b18d7ef279deb",
        ),
        (
            4097,
            "e4bd4e55076ef72a28d8efea9b927fdeaf6b8f2250af3ec586f14dc062237532",
        ),
        (
            524287,
            "3f237da44a04b5ed1a0ef1a5989b0976817f09aea39442514f8d5e85c722f53a",
        ),
        (
            524288,
            "0d72b8eab6dd6da074f9b0c2e3179449fcb2c22ba4e2a7bb4d62ceb969e4809b",
        ),
        (
            524289,
            "7799c8cb78045124ec8e12ac01be66eee6023010dea4a64e54e3dc7b9f1fcf1c",
        ),
    ];
    for &(size, root_hex) in DEFAULT_ROOTS {
        let data = fill(size);
        let (root, _, _) = stream_split::<B>(&data, 8, 65_536, 0);
        assert_eq!(root, pinned(root_hex), "root diverged at {size}");
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
    let bytes = run(async {
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
fn a_synchronous_store_never_occupies_the_put_window() {
    // Every put settles on its opening poll, so no chunk is ever parked: the
    // whole tree is sealed and stored with an empty window throughout.
    let data = fill(200 * TINY + 63);
    let (_, store, stats) = stream_split::<TINY>(&data, 8, 997, 0);
    assert_eq!(stats.peak_put_in_flight, 0, "a settled put occupied a slot");
    assert_eq!(stats.leaves + stats.intermediates, stats.puts);
    assert_eq!(store.log().len() as u64, stats.puts);
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
    let store = reject_all::<_, TINY>(TestStore::<TINY>::new(0));
    let mut split = Split::<_, Plain, TINY>::new(store, PutWindow::new(2).unwrap());
    let data = fill(3 * TINY);
    let error = run(async {
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

/// A put failing after some succeed surfaces through the `collect_with`
/// one-shot as a typed `Put` error; `FaultStore` drives the fault and its
/// counter is shared across the split's per-put store clones.
#[test]
fn collect_with_surfaces_a_put_failure() {
    let data = fill(3 * TINY);
    let store: FaultStore<_, _, TINY> = failing_at(TestStore::<TINY>::new(0), 3);
    let error = run(Split::<_, Plain, TINY>::collect_with(
        store,
        PutWindow::new(2).unwrap(),
        &data,
    ))
    .unwrap_err();
    assert!(matches!(error, SplitError::Put { .. }), "got {error:?}");
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

    let (again, _, _) = stream_split::<TINY>(&data, 4, 719, 1);
    assert_eq!(root, again);
}

/// Encrypted-mode oracles: walk round trips, tree-shape witnesses, per-mode
/// geometry, key-source injection and exhaustion.
#[cfg(feature = "encryption")]
mod encrypted {
    use core::future::poll_fn;
    use core::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::vec::Vec;

    use nectar_primitives::chunk::encryption::{EncryptedChunkRef, EncryptionKey};
    use nectar_testing::run;

    use super::{BRANCHES, TINY, TestStore, fill, sorted, tree_chunks};
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
        let root = run(async {
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
    fn encrypted_roots_read_back_with_the_pinned_tree_shape() {
        run(async {
            for size in sizes() {
                let data = fill(size);
                // The rand-gated default source through the `Default` mode.
                let store = TestStore::<TINY>::new(1);
                let mut split: Split<TestStore<TINY>, Encrypted<RandomKeys>, TINY> =
                    Split::new(store.clone(), PutWindow::new(4).unwrap());
                let root = {
                    let mut buf = data.as_slice();
                    while !buf.is_empty() {
                        let n = poll_fn(|cx| split.poll_write(cx, buf)).await.unwrap();
                        buf = &buf[n..];
                    }
                    poll_fn(|cx| split.poll_finish(cx)).await.unwrap()
                };
                let mut walk: Walk<TestStore<TINY>, Encrypted, TINY> = Walk::new(
                    store,
                    *root.address(),
                    root.key().clone(),
                    size as u64,
                    0..u64::MAX,
                    Window::new(4).unwrap(),
                );
                let plaintext = {
                    let mut bytes = Vec::new();
                    while let Some(frame) = poll_fn(|cx| walk.poll_next_ordered(cx)).await {
                        bytes.extend_from_slice(&frame.unwrap().data);
                    }
                    bytes
                };
                assert_eq!(plaintext, data, "plaintext diverged at {size}");

                let stats = split.stats();
                assert_eq!(
                    stats.puts,
                    tree_chunks(size, TINY, ENC_BRANCHES),
                    "chunk count diverged at {size}"
                );
                assert_eq!(stats.bytes, size as u64);
                assert_eq!(stats.leaves + stats.intermediates, stats.puts);
            }
        });
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
        let bytes = run(async {
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

    /// Keys are drawn on the workers, so only the round trip is pinned:
    /// pooled encrypted output is not byte-reproducible.
    #[cfg(all(
        feature = "rayon",
        not(target_arch = "wasm32"),
        not(feature = "unsync")
    ))]
    #[test]
    fn pooled_encrypted_split_round_trips() {
        use crate::config::HashWindow;

        let size = 17 * TINY + 43;
        let data = fill(size);
        let store = TestStore::<TINY>::new(0);
        let mut split: Split<TestStore<TINY>, Encrypted<RandomKeys>, TINY> =
            Split::new(store.clone(), PutWindow::new(4).unwrap())
                .with_hash_window(HashWindow::new(4).unwrap());
        let root = run(async {
            let mut buf = data.as_slice();
            while !buf.is_empty() {
                let n = poll_fn(|cx| split.poll_write(cx, buf)).await.unwrap();
                buf = &buf[n..];
            }
            poll_fn(|cx| split.poll_finish(cx)).await.unwrap()
        });
        let mut walk: Walk<TestStore<TINY>, Encrypted, TINY> = Walk::new(
            store,
            *root.address(),
            root.key().clone(),
            size as u64,
            0..u64::MAX,
            Window::new(4).unwrap(),
        );
        let plaintext = run(async {
            let mut bytes = Vec::new();
            while let Some(frame) = poll_fn(|cx| walk.poll_next_ordered(cx)).await {
                bytes.extend_from_slice(&frame.unwrap().data);
            }
            bytes
        });
        assert_eq!(plaintext, data);
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
        let error = run(async {
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

/// Pooled-seal oracles: chunk streams identical to the serial engine,
/// hash-window bounds, backpressure, drop and worker-panic paths.
#[cfg(all(
    feature = "rayon",
    not(target_arch = "wasm32"),
    not(feature = "unsync")
))]
mod pooled {
    use core::future::poll_fn;
    use core::task::{Context, Poll};
    use core::time::Duration;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Instant;
    use std::vec::Vec;

    use bytes::Bytes;
    use futures::task::noop_waker;
    use nectar_primitives::chunk::{ChunkAddress, ContentChunk};
    use nectar_testing::run;

    use super::{BRANCHES, TINY, TestStore, fill, sorted, stream_split, tree_chunks};
    use crate::config::{HashWindow, PutWindow};
    use crate::geometry::Mode;
    use crate::split::{SealError, Sealed, Split, SplitError, SplitMode, SplitStats};
    use crate::walk::Plain;

    /// Wall-clock bound on every wait below: far above any scheduling
    /// delay, low enough that a stuck test fails instead of hanging.
    const BUDGET: Duration = Duration::from_secs(10);

    /// Gap between polls of a waited-on condition.
    const POLL_GAP: Duration = Duration::from_micros(50);

    /// Waits for `count` to reach `want`, panicking with both counts and
    /// `state` once [`BUDGET`] is spent.
    fn wait_for_count(state: &str, want: usize, count: impl Fn() -> usize) {
        let deadline = Instant::now() + BUDGET;
        loop {
            let seen = count();
            if seen >= want {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "deadline expired after {BUDGET:?} waiting for {state}: {seen} of {want}"
            );
            std::thread::sleep(POLL_GAP);
        }
    }

    /// Plain sealing that counts completed seals, so a seal landing is
    /// observable without a sleep, behind an optional worker-side stall.
    #[derive(Clone, Debug, Default)]
    struct CountedMode {
        stall: Duration,
        sealed: Arc<AtomicUsize>,
    }

    impl CountedMode {
        /// A mode whose every seal stalls on the worker, so drops race
        /// live jobs.
        fn stalling(stall: Duration) -> Self {
            Self {
                stall,
                sealed: Arc::default(),
            }
        }

        /// Seals finished so far, on the pool or inline.
        fn completed(&self) -> usize {
            self.sealed.load(Ordering::Relaxed)
        }
    }

    impl SplitMode for CountedMode {
        const MODE: Mode = Mode::Plain;

        type Ref = ChunkAddress;
        type Root = ChunkAddress;

        fn data_slots(branches: u64) -> u64 {
            branches
        }

        fn seal<const B: usize>(&self, payload: Bytes) -> Result<Sealed<Self, B>, SealError> {
            std::thread::sleep(self.stall);
            let chunk = ContentChunk::<B>::try_from(payload)?
                .seal::<nectar_primitives::chunk::AnyChunkSet<B>>();
            let address = *chunk.address();
            self.sealed.fetch_add(1, Ordering::Relaxed);
            Ok((chunk, address))
        }

        fn write_ref(reference: &ChunkAddress, out: &mut Vec<u8>) {
            out.extend_from_slice(reference.as_bytes());
        }

        fn into_root(reference: ChunkAddress) -> ChunkAddress {
            reference
        }
    }

    /// Stream `data` through a hash-windowed split in `step`-byte writes.
    fn pooled_split(
        data: &[u8],
        put_window: u16,
        hash_window: u16,
        step: usize,
        delay: usize,
    ) -> (ChunkAddress, TestStore<TINY>, SplitStats) {
        let store = TestStore::<TINY>::new(delay);
        let mut split: Split<TestStore<TINY>, Plain, TINY> =
            Split::new(store.clone(), PutWindow::new(put_window).unwrap())
                .with_hash_window(HashWindow::new(hash_window).unwrap());
        let root = run(async {
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

    #[test]
    fn pooled_chunk_streams_are_byte_identical_to_serial() {
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
            // Zero put delay settles every put at dispatch, so the ordered
            // log is the dispatch order: the wire chunk stream itself.
            let (serial_root, serial_store, serial_stats) = stream_split::<TINY>(&data, 4, 719, 0);
            let (root, store, stats) = pooled_split(&data, 4, 4, 719, 0);
            assert_eq!(root, serial_root, "root diverged at {size}");
            assert_eq!(
                store.log(),
                serial_store.log(),
                "chunk stream diverged at {size}"
            );
            assert_eq!(stats.puts, serial_stats.puts);
            assert_eq!(stats.bytes, size as u64);
            assert_eq!(stats.leaves + stats.intermediates, stats.puts);
            assert_eq!(stats.puts, tree_chunks(size, TINY, BRANCHES));
        }
    }

    #[test]
    fn pooled_bounds_hold_under_a_slow_store() {
        let data = fill(200 * TINY + 63);
        let (serial_root, serial_store, _) = stream_split::<TINY>(&data, 4, 719, 1);
        for put_window in [1u16, 4] {
            for hash_window in [1u16, 2, 8] {
                let (root, store, stats) = pooled_split(&data, put_window, hash_window, 997, 3);
                assert_eq!(root, serial_root);
                assert_eq!(sorted(store.log()), sorted(serial_store.log()));
                assert!(
                    stats.peak_hash_in_flight <= usize::from(hash_window),
                    "seals in flight {} exceeded the hash window {hash_window}",
                    stats.peak_hash_in_flight
                );
                assert!(
                    stats.peak_put_in_flight <= usize::from(put_window),
                    "puts in flight {} exceeded the put window {put_window}",
                    stats.peak_put_in_flight
                );
                assert!(
                    stats.peak_pending <= stats.peak_spine,
                    "pending {} exceeded the spine height {}",
                    stats.peak_pending,
                    stats.peak_spine
                );
            }
        }
    }

    #[test]
    fn pooled_write_backpressure_consumes_nothing_when_full() {
        let gate = Arc::new(Mutex::new(false));
        let store = TestStore::<TINY>::gated(Arc::clone(&gate));
        let mode = CountedMode::default();
        let mut split: Split<TestStore<TINY>, CountedMode, TINY> =
            Split::with_mode(store, mode.clone(), PutWindow::new(1).unwrap())
                .with_hash_window(HashWindow::new(1).unwrap());
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let data = fill(4 * TINY);

        // Two leaves fit: one behind the parked put, one on the pool. The
        // no-op waker cannot park, so the admission is polled to a
        // deadline.
        let deadline = Instant::now() + BUDGET;
        let mut consumed = 0usize;
        while consumed < 2 * TINY {
            assert!(
                Instant::now() < deadline,
                "deadline expired after {BUDGET:?} with {consumed} of {} bytes admitted",
                2 * TINY
            );
            match split.poll_write(&mut cx, &data[consumed..]) {
                Poll::Ready(Ok(n)) => consumed += n,
                Poll::Ready(Err(error)) => panic!("write failed: {error:?}"),
                Poll::Pending => std::thread::sleep(POLL_GAP),
            }
        }
        assert_eq!(consumed, 2 * TINY, "backpressure admitted a third leaf");

        // Once the second seal lands the front is ready but the put window
        // is full, so the deque stays occupied and every further poll
        // consumes nothing.
        wait_for_count("the second leaf seal", 2, || mode.completed());
        for _ in 0..100 {
            assert!(split.poll_write(&mut cx, &data[consumed..]).is_pending());
            assert_eq!(split.stats().bytes, (2 * TINY) as u64);
        }

        // Opening the gate drains the chain and the split finishes.
        *gate.lock().unwrap() = true;
        let root = run(async {
            let mut buf = &data[consumed..];
            while !buf.is_empty() {
                let n = poll_fn(|cx| split.poll_write(cx, buf)).await.unwrap();
                buf = &buf[n..];
            }
            poll_fn(|cx| split.poll_finish(cx)).await.unwrap()
        });
        let (serial_root, _, _) = stream_split::<TINY>(&data, 4, 719, 0);
        assert_eq!(root, serial_root);
        assert_eq!(split.stats().bytes, (4 * TINY) as u64);
    }

    /// Mode whose seal panics on the worker; the split must survive it.
    #[derive(Clone, Copy, Debug, Default)]
    struct PanicMode;

    impl SplitMode for PanicMode {
        const MODE: Mode = Mode::Plain;

        type Ref = ChunkAddress;
        type Root = ChunkAddress;

        fn data_slots(branches: u64) -> u64 {
            branches
        }

        fn seal<const B: usize>(&self, _payload: Bytes) -> Result<Sealed<Self, B>, SealError> {
            panic!("seal panicked on the worker")
        }

        fn write_ref(reference: &ChunkAddress, out: &mut Vec<u8>) {
            out.extend_from_slice(reference.as_bytes());
        }

        fn into_root(reference: ChunkAddress) -> ChunkAddress {
            reference
        }
    }

    #[test]
    fn a_worker_panic_is_a_typed_error_not_an_abort() {
        let store = TestStore::<TINY>::new(0);
        let mut split: Split<TestStore<TINY>, PanicMode, TINY> =
            Split::new(store, PutWindow::DEFAULT).with_hash_window(HashWindow::new(2).unwrap());
        let data = fill(TINY);
        let error = run(async {
            let mut buf = data.as_slice();
            while !buf.is_empty() {
                match poll_fn(|cx| split.poll_write(cx, buf)).await {
                    Ok(n) => buf = &buf[n..],
                    Err(error) => return error,
                }
            }
            poll_fn(|cx| split.poll_finish(cx)).await.unwrap_err()
        });
        assert!(matches!(error, SplitError::PoolDropped), "got {error:?}");

        // The fuse is shut for good.
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(matches!(
            split.poll_finish(&mut cx),
            Poll::Ready(Err(SplitError::Poisoned))
        ));
    }

    #[test]
    fn dropping_a_pooled_split_abandons_live_jobs() {
        let store = TestStore::<TINY>::new(0);
        let mode = CountedMode::stalling(Duration::from_millis(10));
        let mut split: Split<TestStore<TINY>, CountedMode, TINY> =
            Split::with_mode(store, mode.clone(), PutWindow::DEFAULT)
                .with_hash_window(HashWindow::new(4).unwrap());
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let data = fill(4 * TINY);
        let mut consumed = 0usize;
        for _ in 0..1_000 {
            match split.poll_write(&mut cx, &data[consumed..]) {
                Poll::Ready(Ok(n)) => consumed += n,
                Poll::Ready(Err(error)) => panic!("write failed: {error:?}"),
                Poll::Pending => {}
            }
            if consumed == data.len() {
                break;
            }
        }
        assert_eq!(consumed, data.len(), "the hash window never took the file");

        // Seals are still running on the pool; the drop must orphan them
        // harmlessly (each sender is dropped unsent, its receiver gone).
        let submitted = consumed / TINY;
        drop(split);
        wait_for_count("every orphaned seal to run", submitted, || mode.completed());
    }
}

/// Nightly gate: a stream past the `u32` span boundary keeps root equality
/// with the batch ingest while memory stays bounded.
#[cfg(all(
    feature = "rayon",
    not(target_arch = "wasm32"),
    not(feature = "unsync")
))]
#[test]
#[ignore = "nightly: streams more than 4 GiB"]
fn huge_stream_root_matches_the_batch_ingest() {
    use crate::parallel::{ReadAt, split_read_at};

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

        fn len(&self) -> std::io::Result<u64> {
            Ok(self.len)
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

    let batch_root = run(split_read_at::<_, _, Plain, B>(
        Pattern { len: size },
        Discard,
        PutWindow::new(16).unwrap(),
    ))
    .unwrap();

    let mut split: Split<Discard, Plain, B> = Split::new(Discard, PutWindow::new(16).unwrap());
    let root = run(async {
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

    assert_eq!(root, batch_root);
    assert_eq!(split.stats().bytes, size);
}

/// Shrinking stable coverage: root idempotence over write segmentation,
/// composed from the structural-regime generators.
mod properties {
    use arbitrary::Unstructured;
    use proptest::prelude::*;

    use super::{TINY, sorted, stream_split};
    use crate::generators;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        #[test]
        fn root_and_chunk_set_are_invariant_over_segmentation(
            seed in proptest::collection::vec(any::<u8>(), 0..512)
        ) {
            let mut u = Unstructured::new(&seed);
            let data = generators::body(&mut u).unwrap();
            let arm = |u: &mut Unstructured<'_>| {
                let window = u.int_in_range(1..=8u16).unwrap();
                let step = u.int_in_range(1..=1024usize).unwrap();
                let delay = u.int_in_range(0..=2usize).unwrap();
                stream_split::<TINY>(&data, window, step, delay)
            };
            let (root_a, store_a, stats) = arm(&mut u);
            let (root_b, store_b, _) = arm(&mut u);
            prop_assert_eq!(root_a, root_b);
            prop_assert_eq!(sorted(store_a.log()), sorted(store_b.log()));
            prop_assert_eq!(stats.bytes, data.len() as u64);
        }
    }
}
