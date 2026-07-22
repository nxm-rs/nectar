//! Ingest oracles: root and chunk-set differentials against the streaming
//! split engine, put-window witnesses, and typed failure paths.

use std::collections::HashMap;
use std::format;
use std::sync::{Arc, Mutex};
use std::vec;
use std::vec::Vec;

use nectar_primitives::chunk::{AnyChunkSet, Chunk, ChunkAddress, Verified};
use nectar_primitives::store::{ChunkPut, ChunkStoreError};
use nectar_testing::{run, yield_now};

use super::{ReadAt, ReadAtError, split_read_at};
use crate::config::PutWindow;
use crate::split::{Split, SplitError};
use crate::testutil::reject_all;
use crate::walk::Plain;

/// Tiny body size: fan-out 8, so a few dozen leaves already build a deep
/// tree.
const TINY: usize = 256;
const BRANCHES: usize = 8;

/// Distinct byte per file position so every node address is unique.
fn fill(len: usize) -> Vec<u8> {
    (0..len as u64).map(pattern).collect()
}

/// The byte at absolute file position `i`.
fn pattern(i: u64) -> u8 {
    (i.wrapping_mul(2_654_435_761) >> 11) as u8
}

/// Shared put store: logs accepted puts, resolves after `delay` yields,
/// tracks peak concurrent puts. Fault injection rides
/// [`FaultStore`](crate::testutil::FaultStore).
struct TestStore<const B: usize> {
    chunks: Arc<Mutex<HashMap<ChunkAddress, Chunk<Verified, AnyChunkSet<B>>>>>,
    log: Arc<Mutex<Vec<ChunkAddress>>>,
    active: Arc<Mutex<(usize, usize)>>,
    delay: usize,
}

impl<const B: usize> Clone for TestStore<B> {
    fn clone(&self) -> Self {
        Self {
            chunks: Arc::clone(&self.chunks),
            log: Arc::clone(&self.log),
            active: Arc::clone(&self.active),
            delay: self.delay,
        }
    }
}

impl<const B: usize> TestStore<B> {
    fn new(delay: usize) -> Self {
        Self {
            chunks: Arc::new(Mutex::new(HashMap::new())),
            log: Arc::new(Mutex::new(Vec::new())),
            active: Arc::new(Mutex::new((0, 0))),
            delay,
        }
    }

    fn log(&self) -> Vec<ChunkAddress> {
        self.log.lock().unwrap().clone()
    }

    fn peak_active(&self) -> usize {
        self.active.lock().unwrap().1
    }
}

impl<const B: usize> ChunkPut<AnyChunkSet<B>> for TestStore<B> {
    type Error = ChunkStoreError;

    async fn put(&self, chunk: Chunk<Verified, AnyChunkSet<B>>) -> Result<(), ChunkStoreError> {
        {
            let mut active = self.active.lock().unwrap();
            active.0 += 1;
            active.1 = active.1.max(active.0);
        }
        for _ in 0..self.delay {
            yield_now().await;
        }
        self.active.lock().unwrap().0 -= 1;
        let address = *chunk.address();
        self.log.lock().unwrap().push(address);
        self.chunks.lock().unwrap().insert(address, chunk);
        Ok(())
    }
}

/// The streaming split engine as the oracle: an independent ingest of the
/// same bytes, returning root plus every sealed chunk address.
fn stream_split<const B: usize>(data: &[u8]) -> (ChunkAddress, Vec<ChunkAddress>) {
    let store = TestStore::<B>::new(0);
    let root = run(Split::<TestStore<B>, Plain, B>::collect(
        store.clone(),
        data,
    ))
    .unwrap();
    (root, store.log())
}

fn ingest<const B: usize>(
    data: Vec<u8>,
    window: u16,
    delay: usize,
) -> (ChunkAddress, TestStore<B>) {
    let store = TestStore::<B>::new(delay);
    let root = run(split_read_at::<_, _, Plain, B>(
        data,
        store.clone(),
        PutWindow::new(window).unwrap(),
    ))
    .unwrap();
    (root, store)
}

fn sorted(mut addresses: Vec<ChunkAddress>) -> Vec<ChunkAddress> {
    addresses.sort();
    addresses
}

#[test]
fn roots_and_chunk_sets_match_the_streaming_engine() {
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
        let (streamed_root, streamed_chunks) = stream_split::<TINY>(&data);
        let (root, store) = ingest::<TINY>(data, 4, 1);
        assert_eq!(root, streamed_root, "root diverged at {size}");
        assert_eq!(
            sorted(store.log()),
            sorted(streamed_chunks),
            "chunk set diverged at {size}"
        );
    }
}

#[test]
fn default_profile_roots_match_the_streaming_engine() {
    const B: usize = nectar_primitives::DEFAULT_BODY_SIZE;
    let sizes = [0, 1, B - 1, B, B + 1, 5 * B + 123];
    for size in sizes {
        let data = fill(size);
        let (streamed_root, _) = stream_split::<B>(&data);
        let (root, _) = ingest::<B>(data, 8, 0);
        assert_eq!(root, streamed_root, "root diverged at {size}");
    }
}

#[test]
fn put_window_bounds_concurrent_puts() {
    let data = fill(200 * TINY + 63);
    for window in [1u16, 4, 16] {
        let (streamed_root, streamed_chunks) = stream_split::<TINY>(&data);
        let (root, store) = ingest::<TINY>(data.clone(), window, 3);
        assert_eq!(root, streamed_root);
        assert_eq!(store.log().len(), streamed_chunks.len());
        assert!(
            store.peak_active() <= usize::from(window),
            "puts in flight {} exceeded window {window}",
            store.peak_active()
        );
    }
}

/// The ordered walk reads the shared chunk map, so the pooled encrypted
/// ingest is checked end to end.
#[cfg(feature = "encryption")]
impl<const B: usize> nectar_primitives::store::ChunkGet<AnyChunkSet<B>> for TestStore<B> {
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

/// The pooled encrypted ingest hands the ordered walk a readable root.
#[cfg(feature = "encryption")]
#[test]
fn encrypted_ingest_reads_back_through_the_walk() {
    use core::future::poll_fn;

    use crate::config::Window;
    use crate::split::RandomKeys;
    use crate::walk::{Encrypted, Walk};

    let data = fill(17 * TINY + 43);
    let store = TestStore::<TINY>::new(0);
    let root = run(split_read_at::<_, _, Encrypted<RandomKeys>, TINY>(
        data.clone(),
        store.clone(),
        PutWindow::DEFAULT,
    ))
    .unwrap();
    let mut walk: Walk<TestStore<TINY>, Encrypted, TINY> = Walk::new(
        store,
        *root.address(),
        root.key().clone(),
        data.len() as u64,
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
fn bytes_and_slice_sources_agree() {
    let data = fill(11 * TINY + 7);
    let (from_vec, _) = ingest::<TINY>(data.clone(), 4, 0);
    let store = TestStore::<TINY>::new(0);
    let from_bytes = run(split_read_at::<_, _, Plain, TINY>(
        bytes::Bytes::from(data),
        store,
        PutWindow::DEFAULT,
    ))
    .unwrap();
    assert_eq!(from_vec, from_bytes);
}

#[cfg(unix)]
#[test]
fn a_filesystem_source_matches_the_buffer_path() {
    let data = fill(23 * TINY + 91);
    let path =
        std::env::temp_dir().join(format!("nectar-file-read-at-{}-{TINY}", std::process::id()));
    std::fs::write(&path, &data).unwrap();
    let file = std::fs::File::open(&path).unwrap();
    let store = TestStore::<TINY>::new(0);
    let root = run(split_read_at::<_, _, Plain, TINY>(
        file,
        store,
        PutWindow::DEFAULT,
    ))
    .unwrap();
    std::fs::remove_file(&path).unwrap();
    let (expected, _) = ingest::<TINY>(data, 4, 0);
    assert_eq!(root, expected);
}

/// Source failing every read at or past `fail_at`.
struct FailingSource {
    data: Vec<u8>,
    fail_at: u64,
}

impl ReadAt for FailingSource {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> std::io::Result<usize> {
        if offset >= self.fail_at {
            return Err(std::io::Error::other("device gone"));
        }
        self.data.read_at(offset, buf)
    }

    fn len(&self) -> std::io::Result<u64> {
        ReadAt::len(&self.data)
    }
}

#[test]
fn a_read_failure_is_typed_with_its_offset() {
    // A single failing leaf, so the parallel batch surfaces one offset.
    let source = FailingSource {
        data: fill(2 * TINY),
        fail_at: u64::try_from(TINY).unwrap(),
    };
    let store = TestStore::<TINY>::new(0);
    let error = run(split_read_at::<_, _, Plain, TINY>(
        source,
        store,
        PutWindow::DEFAULT,
    ))
    .unwrap_err();
    let ReadAtError::Read { offset, .. } = error else {
        panic!("expected a read error, got {error:?}");
    };
    assert_eq!(offset, u64::try_from(TINY).unwrap());
}

/// Source claiming more bytes than it can deliver.
struct LyingSource {
    data: Vec<u8>,
    claimed: u64,
}

impl ReadAt for LyingSource {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> std::io::Result<usize> {
        self.data.read_at(offset, buf)
    }

    fn len(&self) -> std::io::Result<u64> {
        Ok(self.claimed)
    }
}

#[test]
fn a_source_ending_early_is_a_short_read() {
    let source = LyingSource {
        data: fill(TINY + 100),
        claimed: u64::try_from(2 * TINY).unwrap(),
    };
    let store = TestStore::<TINY>::new(0);
    let error = run(split_read_at::<_, _, Plain, TINY>(
        source,
        store,
        PutWindow::DEFAULT,
    ))
    .unwrap_err();
    let ReadAtError::ShortRead { offset, remaining } = error else {
        panic!("expected a short read, got {error:?}");
    };
    assert_eq!(offset, u64::try_from(TINY + 100).unwrap());
    assert_eq!(remaining, TINY - 100);
}

/// Source reporting more bytes than the buffer holds.
struct OverrunSource;

impl ReadAt for OverrunSource {
    fn read_at(&self, _offset: u64, buf: &mut [u8]) -> std::io::Result<usize> {
        Ok(buf.len() + 1)
    }

    fn len(&self) -> std::io::Result<u64> {
        Ok(u64::try_from(TINY).unwrap())
    }
}

#[test]
fn an_overlong_read_count_is_refused() {
    let store = TestStore::<TINY>::new(0);
    let error = run(split_read_at::<_, _, Plain, TINY>(
        OverrunSource,
        store,
        PutWindow::DEFAULT,
    ))
    .unwrap_err();
    assert!(
        matches!(error, ReadAtError::ReadOverrun { count, capacity, .. }
            if count == TINY + 1 && capacity == TINY),
        "got {error:?}"
    );
}

/// Source whose sizing itself fails.
struct UnsizedSource;

impl ReadAt for UnsizedSource {
    fn read_at(&self, _offset: u64, _buf: &mut [u8]) -> std::io::Result<usize> {
        Ok(0)
    }

    fn len(&self) -> std::io::Result<u64> {
        Err(std::io::Error::other("no metadata"))
    }
}

#[test]
fn a_sizing_failure_is_typed() {
    let store = TestStore::<TINY>::new(0);
    let error = run(split_read_at::<_, _, Plain, TINY>(
        UnsizedSource,
        store,
        PutWindow::DEFAULT,
    ))
    .unwrap_err();
    assert!(matches!(error, ReadAtError::Length { .. }), "got {error:?}");
}

#[test]
fn a_failed_put_surfaces_as_a_split_error() {
    let store = reject_all::<_, TINY>(TestStore::<TINY>::new(0));
    let error = run(split_read_at::<_, _, Plain, TINY>(
        fill(6 * TINY),
        store,
        PutWindow::new(2).unwrap(),
    ))
    .unwrap_err();
    assert!(
        matches!(error, ReadAtError::Split(SplitError::Put { .. })),
        "got {error:?}"
    );
}

#[test]
fn read_at_sources_honour_offsets_and_ends() {
    let data = fill(100);
    let slice: &[u8] = &data;
    let mut buf = vec![0u8; 40];
    assert_eq!(slice.read_at(0, &mut buf).unwrap(), 40);
    assert_eq!(buf, data[..40]);
    assert_eq!(slice.read_at(80, &mut buf).unwrap(), 20);
    assert_eq!(buf[..20], data[80..]);
    assert_eq!(slice.read_at(100, &mut buf).unwrap(), 0);
    assert_eq!(slice.read_at(u64::MAX, &mut buf).unwrap(), 0);
    assert_eq!(ReadAt::len(slice).unwrap(), 100);
    let owned = bytes::Bytes::from(data.clone());
    assert_eq!(owned.read_at(60, &mut buf).unwrap(), 40);
    assert_eq!(buf, data[60..]);
    assert_eq!(ReadAt::len(&owned).unwrap(), 100);
}
