//! Adapter battery: differential reads over both drivers, seek semantics,
//! typed-to-io error mapping, driver handover and the writer shim's
//! shutdown-to-root path.

use std::io::{ErrorKind, SeekFrom};
use std::string::ToString;
use std::sync::{Arc, Mutex};
use std::vec::Vec;

use nectar_primitives::chunk::{AnyChunkSet, Chunk, ChunkAddress, Verified};
use nectar_primitives::store::{ChunkGet, ChunkPut, ChunkStoreError, MemoryStore};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use super::{SpawnedReader, TokioReader, TokioWriter};
use crate::config::PutWindow;
use crate::read::File;
use crate::split::Split;
use crate::testutil::split_fixture;
use crate::walk::Plain;

/// Tiny body size shared with the facade tests: fan-out 8, so small files
/// already build deep trees.
const TINY: usize = 256;

type TinyStore = MemoryStore<AnyChunkSet<TINY>>;

/// Distinct byte per file position so slices are position-sensitive.
fn fill(len: usize) -> Vec<u8> {
    (0..len as u64)
        .map(|i| (i.wrapping_mul(2_654_435_761) >> 11) as u8)
        .collect()
}

async fn open(data: &[u8]) -> File<TinyStore, Plain, TINY> {
    let (root, store) = split_fixture::<TINY>(data);
    File::open(store, root).await.unwrap()
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
        let file = open(&data).await;
        let mut reader = TokioReader::from(file.read().build());
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
    let file = open(&data).await;
    let mut reader = TokioReader::from(file.read().build());

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
    let file = open(&data).await;
    let mut reader = TokioReader::from(file.read().build());
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
    let file = open(&data).await;
    let mut reader = TokioReader::from(file.read().range(100..1000).build());
    assert_eq!(reader.effective_len(), 900);
    reader.seek(SeekFrom::End(-100)).await.unwrap();
    let mut out = Vec::new();
    reader.read_to_end(&mut out).await.unwrap();
    assert_eq!(out, &data[900..1000]);
}

#[tokio::test]
async fn spawned_reads_match_the_split_input() {
    for len in [0usize, 1, TINY, 8 * TINY + 3, 33 * TINY + 17] {
        let data = fill(len);
        let file = open(&data).await;
        let mut reader = SpawnedReader::spawn(file.read().build());
        assert_eq!(reader.effective_len(), len as u64);
        let mut out = Vec::new();
        reader.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, data, "diverged at {len}");
        assert_eq!(reader.position(), len as u64);
    }
}

#[tokio::test]
async fn spawned_seek_restarts_the_driver_mid_stream() {
    let data = fill(17 * TINY + 5);
    let file = open(&data).await;
    let mut reader = SpawnedReader::spawn(file.read().build());
    let mut buf = [0u8; 300];
    reader.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf[..], &data[..300]);

    // A no-op seek keeps the driver and its prefetched frames.
    assert_eq!(reader.seek(SeekFrom::Current(0)).await.unwrap(), 300);

    reader.seek(SeekFrom::Start(4000)).await.unwrap();
    let mut buf = [0u8; 100];
    reader.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf[..], &data[4000..4100]);

    reader.seek(SeekFrom::Current(-100)).await.unwrap();
    let mut out = Vec::new();
    reader.read_to_end(&mut out).await.unwrap();
    assert_eq!(out, &data[4000..]);

    let error = reader.seek(SeekFrom::End(1)).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
}

#[tokio::test]
async fn spawn_carries_reader_progress_and_lead_bytes() {
    let data = fill(5 * TINY + 9);
    let file = open(&data).await;
    let mut inner = file.read().build();
    let mut buf = [0u8; 100];
    assert_eq!(inner.read(&mut buf).await.unwrap(), 100);

    let mut reader = SpawnedReader::spawn(inner);
    assert_eq!(reader.position(), 100);
    let mut out = Vec::new();
    reader.read_to_end(&mut out).await.unwrap();
    assert_eq!(out, &data[100..]);
}

/// Store failing every fetch after a countdown of successes.
#[derive(Clone)]
struct FailAfter {
    inner: Arc<TinyStore>,
    countdown: Arc<Mutex<usize>>,
}

impl ChunkGet<AnyChunkSet<TINY>> for FailAfter {
    type Trust = Verified;
    type Error = ChunkStoreError;

    async fn get(
        &self,
        address: &ChunkAddress,
    ) -> Result<Chunk<Verified, AnyChunkSet<TINY>>, ChunkStoreError> {
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
async fn walk_failures_surface_as_io_errors_on_both_drivers() {
    let data = fill(9 * TINY);
    let (root, store) = split_fixture::<TINY>(&data);
    let store = FailAfter {
        inner: Arc::new(store),
        countdown: Arc::new(Mutex::new(3)),
    };
    let file = File::<_, Plain, TINY>::open(store, root).await.unwrap();

    let mut out = Vec::new();
    let error = TokioReader::from(file.read().build())
        .read_to_end(&mut out)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Other);

    let mut out = Vec::new();
    let error = SpawnedReader::spawn(file.read().build())
        .read_to_end(&mut out)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Other);
}

/// Store refusing every put.
#[derive(Clone)]
struct RejectPuts;

impl ChunkPut<AnyChunkSet<TINY>> for RejectPuts {
    type Error = ChunkStoreError;

    async fn put(&self, _chunk: Chunk<Verified, AnyChunkSet<TINY>>) -> Result<(), ChunkStoreError> {
        Err(ChunkStoreError::Other("outage".to_string().into()))
    }
}

/// Shared store handle: clones share one map, unlike the snapshot-cloning
/// memory store.
#[derive(Clone, Default)]
struct SharedStore(Arc<TinyStore>);

impl ChunkPut<AnyChunkSet<TINY>> for SharedStore {
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

fn writer(store: SharedStore) -> TokioWriter<SharedStore, Plain, TINY> {
    TokioWriter::from(Split::new(store, PutWindow::new(2).unwrap()))
}

#[tokio::test]
async fn writer_roots_match_the_whole_buffer_split() {
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
        let mut writer = writer(store.clone());
        writer.write_all(&data).await.unwrap();
        writer.flush().await.unwrap();
        writer.shutdown().await.unwrap();
        assert!(writer.is_finished());
        assert_eq!(writer.stats().bytes, len as u64);
        let root = writer.into_inner().unwrap();
        let (expected, _) = split_fixture::<TINY>(&data);
        assert_eq!(root, expected, "diverged at {len}");

        let file = File::<_, Plain, TINY>::open(store, root).await.unwrap();
        let mut out = Vec::new();
        TokioReader::from(file.read().build())
            .read_to_end(&mut out)
            .await
            .unwrap();
        assert_eq!(out, data, "read back diverged at {len}");
    }
}

#[tokio::test]
async fn writer_shutdown_is_fused_and_later_writes_fail() {
    let data = fill(3 * TINY + 7);
    assert!(writer(SharedStore::default()).into_inner().is_none());

    let mut writer = writer(SharedStore::default());
    writer.write_all(&data).await.unwrap();
    writer.shutdown().await.unwrap();
    writer.shutdown().await.unwrap();

    let error = writer.write_all(b"late").await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Other);

    let (expected, _) = split_fixture::<TINY>(&data);
    assert_eq!(writer.into_inner().unwrap(), expected);
}

#[tokio::test]
async fn writer_put_failures_surface_as_io_errors() {
    let mut writer =
        TokioWriter::from(Split::<_, Plain, TINY>::new(RejectPuts, PutWindow::DEFAULT));
    let data = fill(2 * TINY);
    let error = async {
        writer.write_all(&data).await?;
        writer.shutdown().await
    }
    .await
    .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Other);
    assert!(writer.into_inner().is_none());
}
