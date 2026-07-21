//! Adapter battery: differential reads over both drivers, seek semantics,
//! typed-to-io error mapping and driver handover.
#![allow(deprecated)]

use std::io::{ErrorKind, SeekFrom};
use std::string::ToString;
use std::sync::{Arc, Mutex};
use std::vec::Vec;

use nectar_primitives::chunk::{AnyChunkSet, Chunk, ChunkAddress, Verified};
use nectar_primitives::file::split;
use nectar_primitives::store::{ChunkGet, ChunkStoreError, MemoryStore};
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use super::{SpawnedReader, TokioReader};
use crate::read::File;
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
    let (root, store) = split::<TINY>(data).unwrap();
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
    let (root, store) = split::<TINY>(&data).unwrap();
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
