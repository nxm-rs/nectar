//! Adapter battery: reads over the shim, seek semantics, typed-to-io error
//! mapping, and the `AsyncRead` source feeding a save.

use std::io::{ErrorKind, SeekFrom};
use std::string::ToString;
use std::sync::{Arc, Mutex};
use std::vec::Vec;

use nectar_primitives::chunk::{AnyChunkSet, Chunk, ChunkAddress, ContentOnlyChunkSet, Verified};
use nectar_primitives::store::{ChunkGet, ChunkPut, ChunkStoreError, ContentGet, MemoryStore};
use nectar_testing::split_fixture;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use super::TokioReader;
use crate::handle::{File, Policy, Reader};
use crate::source::AsyncReadSource;
use crate::testutil::reject_all;

/// Tiny body size shared with the facade tests: fan-out 8, so small files
/// already build deep trees.
const TINY: usize = 256;

type TinyStore = MemoryStore<ContentOnlyChunkSet<TINY>>;

/// Distinct byte per file position so slices are position-sensitive.
fn fill(len: usize) -> Vec<u8> {
    (0..len as u64)
        .map(|i| (i.wrapping_mul(2_654_435_761) >> 11) as u8)
        .collect()
}

async fn open(data: &[u8]) -> Reader<TinyStore, TINY> {
    let (root, store) = split_fixture::<TINY>(data);
    File::<_, TINY>::new(store, Policy::DEFAULT)
        .open(root.into())
        .await
        .unwrap()
}

async fn open_range(data: &[u8], range: core::ops::Range<u64>) -> Reader<TinyStore, TINY> {
    let (root, store) = split_fixture::<TINY>(data);
    File::<_, TINY>::new(store, Policy::DEFAULT)
        .open_range(root.into(), range)
        .await
        .unwrap()
}

#[tokio::test]
async fn shim_reads_match_the_split_input() {
    for len in [
        0usize,
        1,
        TINY - 1,
        TINY,
        TINY + 1,
        8 * TINY,
        33 * TINY + 17,
    ] {
        let data = fill(len);
        let mut reader = TokioReader::from(open(&data).await);
        assert_eq!(reader.effective_len(), len as u64);
        let mut out = Vec::new();
        reader.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, data, "diverged at {len}");
        assert_eq!(reader.position(), len as u64);
    }
}

#[tokio::test]
async fn shim_seeks_resolve_against_start_current_and_end() {
    let data = fill(9 * TINY + 21);
    let mut reader = TokioReader::from(open(&data).await);

    assert_eq!(reader.seek(SeekFrom::Start(5)).await.unwrap(), 5);
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, &data[5..9]);

    assert_eq!(reader.seek(SeekFrom::Current(-3)).await.unwrap(), 6);
    reader.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, &data[6..10]);

    let tail = data.len() as u64 - 4;
    assert_eq!(reader.seek(SeekFrom::End(-4)).await.unwrap(), tail);
    let mut out = Vec::new();
    reader.read_to_end(&mut out).await.unwrap();
    assert_eq!(out, &data[data.len() - 4..]);

    // Seeking to the effective length is legal and reads as end of range.
    assert_eq!(
        reader.seek(SeekFrom::End(0)).await.unwrap(),
        data.len() as u64
    );
    let mut out = Vec::new();
    reader.read_to_end(&mut out).await.unwrap();
    assert!(out.is_empty());
}

#[tokio::test]
async fn shim_rejects_out_of_range_seeks_without_moving() {
    let data = fill(2 * TINY);
    let mut reader = TokioReader::from(open(&data).await);
    reader.seek(SeekFrom::Start(7)).await.unwrap();

    for bad in [
        SeekFrom::Start(data.len() as u64 + 1),
        SeekFrom::Current(-8),
        SeekFrom::End(1),
        SeekFrom::Current(i64::MIN),
    ] {
        let error = reader.seek(bad).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput, "{bad:?}");
    }
    assert_eq!(reader.position(), 7);
    let mut buf = [0u8; 2];
    reader.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, &data[7..9]);
}

#[tokio::test]
async fn shim_range_positions_are_range_relative() {
    let data = fill(6 * TINY);
    let mut reader = TokioReader::from(open_range(&data, 100..1000).await);
    assert_eq!(reader.effective_len(), 900);
    reader.seek(SeekFrom::End(-100)).await.unwrap();
    let mut out = Vec::new();
    reader.read_to_end(&mut out).await.unwrap();
    assert_eq!(out, &data[900..1000]);
}

/// Store failing every fetch after a countdown of successes.
#[derive(Clone)]
struct FailAfter {
    inner: Arc<TinyStore>,
    countdown: Arc<Mutex<usize>>,
}

impl ChunkGet<ContentOnlyChunkSet<TINY>> for FailAfter {
    type Trust = Verified;
    type Error = ChunkStoreError;

    async fn get(
        &self,
        address: &ChunkAddress,
    ) -> Result<Chunk<Verified, ContentOnlyChunkSet<TINY>>, ChunkStoreError> {
        {
            let mut left = self.countdown.lock().unwrap();
            if *left == 0 {
                return Err(ChunkStoreError::Other("outage".to_string().into()));
            }
            *left -= 1;
        }
        ChunkGet::get(self.inner.as_ref(), address).await
    }
}

#[tokio::test]
async fn walk_failures_surface_as_io_errors() {
    let data = fill(9 * TINY);
    let (root, store) = split_fixture::<TINY>(&data);
    let store = FailAfter {
        inner: Arc::new(store),
        countdown: Arc::new(Mutex::new(3)),
    };
    let file = File::<_, TINY>::new(store, Policy::DEFAULT);

    let mut out = Vec::new();
    let error = TokioReader::from(file.open(root.into()).await.unwrap())
        .read_to_end(&mut out)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Other);
}

/// Shared store handle: clones share one map, unlike the snapshot-cloning
/// memory store. Writes stay wide; reads narrow through [`ContentGet`].
#[derive(Clone, Default)]
struct SharedStore(Arc<MemoryStore<AnyChunkSet<TINY>>>);

impl ChunkPut<Chunk<Verified, AnyChunkSet<TINY>>> for SharedStore {
    type Error = std::convert::Infallible;

    async fn put(
        &self,
        chunk: Chunk<Verified, AnyChunkSet<TINY>>,
    ) -> Result<(), std::convert::Infallible> {
        ChunkPut::put(self.0.as_ref(), chunk).await
    }
}

impl ChunkGet<AnyChunkSet<TINY>> for SharedStore {
    type Trust = Verified;
    type Error = ChunkStoreError;

    async fn get(
        &self,
        address: &ChunkAddress,
    ) -> Result<Chunk<Verified, AnyChunkSet<TINY>>, ChunkStoreError> {
        ChunkGet::get(self.0.as_ref(), address).await
    }
}

#[tokio::test]
async fn an_async_read_source_saves_the_same_root_as_a_slice() {
    for len in [
        0usize,
        1,
        TINY - 1,
        TINY,
        TINY + 1,
        8 * TINY,
        33 * TINY + 17,
    ] {
        let data = fill(len);
        let store = SharedStore::default();
        let file = File::<_, TINY>::new(store.clone(), Policy::DEFAULT);
        let root = file.save(AsyncReadSource::new(&data[..])).await.unwrap();
        let (expected, _) = split_fixture::<TINY>(&data);
        assert_eq!(root, expected, "diverged at {len}");

        let reader = File::<_, TINY>::new(ContentGet::new(store), Policy::DEFAULT);
        let mut out = Vec::new();
        TokioReader::from(reader.open(root.into()).await.unwrap())
            .read_to_end(&mut out)
            .await
            .unwrap();
        assert_eq!(out, data, "read back diverged at {len}");
    }
}

#[tokio::test]
async fn put_failures_surface_as_a_typed_save_error() {
    let store = reject_all::<_, TINY>(MemoryStore::<AnyChunkSet<TINY>>::default());
    let file = File::<_, TINY>::new(store, Policy::DEFAULT);
    let data = fill(2 * TINY);
    let error = file
        .save(AsyncReadSource::new(&data[..]))
        .await
        .unwrap_err();
    assert!(
        matches!(
            error,
            crate::split::SaveError::Split(crate::split::SplitError::Put { .. })
        ),
        "got {error:?}"
    );
}
