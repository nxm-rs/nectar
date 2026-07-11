//! Async random-access write sink for out-of-order file reassembly.

use std::io;
use std::sync::Mutex;

use futures::StreamExt;

use super::error::FileError;
use super::joiner::GenericJoiner;
use super::mode::JoinMode;
use crate::store::{ChunkGet, MaybeSend, MaybeSync};

/// Async random-access write sink. Each leaf body is written at its absolute
/// offset; when every offset is written the sink is whole. Not `Send`-bound so
/// a single-threaded browser writable can implement it.
pub trait WriteAt {
    /// Error returned by the sink operations.
    type Error: core::error::Error + MaybeSend + MaybeSync + 'static;

    /// Write all of `buf` at `offset`.
    fn write_at(
        &self,
        offset: u64,
        buf: &[u8],
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + MaybeSend;

    /// Pre-size the sink so every in-range `write_at` lands.
    fn set_len(
        &self,
        len: u64,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + MaybeSend;

    /// Flush buffered writes. Defaults to a no-op.
    fn flush(&self) -> impl std::future::Future<Output = Result<(), Self::Error>> + MaybeSend {
        async { Ok(()) }
    }
}

#[cfg(all(not(target_arch = "wasm32"), unix))]
impl WriteAt for std::fs::File {
    type Error = io::Error;

    async fn write_at(&self, offset: u64, buf: &[u8]) -> io::Result<()> {
        use std::os::unix::fs::FileExt;
        self.write_all_at(buf, offset)
    }

    #[allow(clippy::use_self)]
    async fn set_len(&self, len: u64) -> io::Result<()> {
        std::fs::File::set_len(self, len)
    }

    async fn flush(&self) -> io::Result<()> {
        self.sync_all()
    }
}

#[cfg(all(not(target_arch = "wasm32"), windows))]
impl WriteAt for std::fs::File {
    type Error = io::Error;

    async fn write_at(&self, offset: u64, buf: &[u8]) -> io::Result<()> {
        use std::os::windows::fs::FileExt;
        let mut written = 0;
        while written < buf.len() {
            let n = self.seek_write(
                &buf[written..],
                offset + crate::cast::u64_from_usize(written),
            )?;
            if n == 0 {
                return Err(io::Error::from(io::ErrorKind::WriteZero));
            }
            written += n;
        }
        Ok(())
    }

    #[allow(clippy::use_self)]
    async fn set_len(&self, len: u64) -> io::Result<()> {
        std::fs::File::set_len(self, len)
    }

    async fn flush(&self) -> io::Result<()> {
        self.sync_all()
    }
}

impl WriteAt for Mutex<Vec<u8>> {
    type Error = io::Error;

    #[allow(clippy::arithmetic_side_effects, clippy::indexing_slicing)] // offset + buf.len() is a leaf position within the file size the sink was set to; the resize above guarantees vec.len() >= end
    async fn write_at(&self, offset: u64, buf: &[u8]) -> io::Result<()> {
        let mut vec = self
            .lock()
            .map_err(|_| io::Error::other("sink lock poisoned"))?;
        // In-memory sink: offsets are bounded by the Vec the sink grows.
        let offset = crate::cast::usize_from_u64(offset);
        let end = offset + buf.len();
        if vec.len() < end {
            vec.resize(end, 0);
        }
        vec[offset..end].copy_from_slice(buf);
        Ok(())
    }

    async fn set_len(&self, len: u64) -> io::Result<()> {
        let mut vec = self
            .lock()
            .map_err(|_| io::Error::other("sink lock poisoned"))?;
        vec.resize(crate::cast::usize_from_u64(len), 0);
        Ok(())
    }
}

impl<T: WriteAt + ?Sized> WriteAt for &T {
    type Error = T::Error;

    fn write_at(
        &self,
        offset: u64,
        buf: &[u8],
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + MaybeSend {
        (**self).write_at(offset, buf)
    }

    fn set_len(
        &self,
        len: u64,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + MaybeSend {
        (**self).set_len(len)
    }

    fn flush(&self) -> impl std::future::Future<Output = Result<(), Self::Error>> + MaybeSend {
        (**self).flush()
    }
}

impl<G, M, const BODY_SIZE: usize> GenericJoiner<G, M, BODY_SIZE>
where
    G: ChunkGet<BODY_SIZE> + 'static,
    M: JoinMode + MaybeSend + Sync,
{
    /// Reassemble the whole file into `sink`, writing each out-of-order leaf at
    /// its offset. Peak memory is the in-flight width, never the file size.
    /// Cancel-safe: dropping the returned future drops the chunk stream
    /// (cancelling in-flight fetches) and the sink; a partially-written sink is
    /// sparse-valid and safe to resume.
    pub async fn download_into<S: WriteAt>(self, sink: S) -> super::error::Result<()> {
        self.download_into_with_progress(sink, |_, _| {}).await
    }

    /// As [`download_into`](Self::download_into), invoking
    /// `on_progress(written, total)` after each leaf lands.
    #[allow(clippy::arithmetic_side_effects)] // written sums leaf body lengths, bounded by the u64 file size
    pub async fn download_into_with_progress<S: WriteAt, F: FnMut(u64, u64)>(
        self,
        sink: S,
        mut on_progress: F,
    ) -> super::error::Result<()> {
        let total = self.size();
        sink.set_len(total).await.map_err(FileError::sink)?;
        let mut stream = std::pin::pin!(self.into_offset_stream_chunked());
        let mut written = 0u64;
        while let Some(item) = stream.next().await {
            let (offset, body) = item?;
            sink.write_at(offset, &body)
                .await
                .map_err(FileError::sink)?;
            written += crate::cast::u64_from_usize(body.len());
            on_progress(written, total);
        }
        sink.flush().await.map_err(FileError::sink)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::executor::block_on;

    #[test]
    fn sparse_writes_reassemble() {
        block_on(async {
            let sink = Mutex::new(Vec::new());
            sink.set_len(10).await.unwrap();
            // High offset first, then a low offset.
            sink.write_at(6, b"world").await.unwrap();
            sink.write_at(0, b"hello").await.unwrap();
            sink.write_at(5, b" ").await.unwrap();
            assert_eq!(sink.into_inner().unwrap(), b"hello world");
        });
    }

    #[test]
    fn out_of_order_full_coverage() {
        block_on(async {
            let sink = Mutex::new(Vec::new());
            sink.set_len(6).await.unwrap();
            sink.write_at(4, b"23").await.unwrap();
            sink.write_at(0, b"01").await.unwrap();
            sink.write_at(2, b"45").await.unwrap();
            assert_eq!(sink.into_inner().unwrap(), b"014523");
        });
    }

    #[test]
    fn ref_forwarding_writes_through() {
        block_on(async {
            let sink = Mutex::new(Vec::new());
            let r = &sink;
            r.set_len(4).await.unwrap();
            r.write_at(0, b"abcd").await.unwrap();
            assert_eq!(sink.into_inner().unwrap(), b"abcd");
        });
    }

    #[test]
    fn flush_default_is_noop() {
        block_on(async {
            let sink = Mutex::new(Vec::new());
            sink.write_at(0, b"complete").await.unwrap();
            sink.flush().await.unwrap();
            assert_eq!(sink.into_inner().unwrap(), b"complete");
        });
    }

    #[test]
    fn set_len_presizes() {
        block_on(async {
            let sink = Mutex::new(Vec::new());
            sink.set_len(8).await.unwrap();
            assert_eq!(sink.into_inner().unwrap(), vec![0u8; 8]);
        });
    }
}

#[cfg(all(test, feature = "tokio"))]
mod download_tests {
    use super::*;

    use crate::DEFAULT_BODY_SIZE;
    use crate::chunk::AnyChunk;
    use crate::file::{Joiner, split};
    use futures::executor::block_on;
    use std::collections::HashMap;

    type Store = HashMap<crate::ChunkAddress, AnyChunk>;

    fn split_and_store(data: &[u8]) -> (crate::ChunkAddress, Store) {
        let (root, store) = split::<DEFAULT_BODY_SIZE>(data).unwrap();
        (root, store.into_chunks())
    }

    fn sample(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 256) as u8).collect()
    }

    /// `download_into` reassembles bytes equal to `read_all` across tree shapes.
    fn assert_download_matches(data: &[u8], width: usize) {
        block_on(async {
            let (root, store) = split_and_store(data);

            let expected = Joiner::new(store.clone(), root)
                .await
                .unwrap()
                .read_all()
                .await
                .unwrap();

            let joiner = Joiner::new(store, root)
                .await
                .unwrap()
                .with_concurrency(width);
            let sink = Mutex::new(Vec::new());
            joiner.download_into(&sink).await.unwrap();

            assert_eq!(sink.into_inner().unwrap(), expected);
        });
    }

    #[test]
    fn download_into_small() {
        assert_download_matches(b"hello world", DEFAULT_BODY_SIZE);
    }

    #[test]
    fn download_into_exact_chunk() {
        assert_download_matches(&sample(DEFAULT_BODY_SIZE), 8);
    }

    #[test]
    fn download_into_multi_level() {
        let refs_per_chunk = DEFAULT_BODY_SIZE / super::super::constants::REF_SIZE;
        assert_download_matches(&sample(DEFAULT_BODY_SIZE * (refs_per_chunk + 1) + 17), 8);
    }

    #[test]
    fn download_into_width_one() {
        assert_download_matches(&sample(DEFAULT_BODY_SIZE * 5 + 7), 1);
    }

    #[test]
    fn download_with_progress_monotonic() {
        block_on(async {
            let data = sample(DEFAULT_BODY_SIZE * 4 + 99);
            let (root, store) = split_and_store(&data);
            let joiner = Joiner::new(store, root).await.unwrap();
            let total = joiner.size();

            let sink = Mutex::new(Vec::new());
            let mut last = 0u64;
            let mut last_total = None;
            joiner
                .download_into_with_progress(&sink, |written, t| {
                    assert!(written >= last, "written must be non-decreasing");
                    assert!(written <= t, "written must not exceed total");
                    last = written;
                    last_total = Some(t);
                })
                .await
                .unwrap();

            assert_eq!(last, total, "final written equals size");
            assert_eq!(last_total, Some(total));
            assert_eq!(sink.into_inner().unwrap(), data);
        });
    }

    #[cfg(feature = "encryption")]
    mod encrypted {
        use super::*;
        use crate::chunk::encryption::EncryptedChunkRef;
        use crate::file::{EncryptedJoiner, split_encrypted};

        fn encrypted_split_and_store(data: &[u8]) -> (EncryptedChunkRef, Store) {
            let (root_ref, store) = split_encrypted::<DEFAULT_BODY_SIZE>(data).unwrap();
            (root_ref, store.into_chunks())
        }

        fn assert_encrypted_download_matches(data: &[u8], width: usize) {
            block_on(async {
                let (root_ref, store) = encrypted_split_and_store(data);

                let expected = EncryptedJoiner::new(store.clone(), root_ref.clone())
                    .await
                    .unwrap()
                    .read_all()
                    .await
                    .unwrap();

                let joiner = EncryptedJoiner::new(store, root_ref)
                    .await
                    .unwrap()
                    .with_concurrency(width);
                let sink = Mutex::new(Vec::new());
                joiner.download_into(&sink).await.unwrap();

                assert_eq!(sink.into_inner().unwrap(), expected);
            });
        }

        #[test]
        fn encrypted_download_into_small() {
            assert_encrypted_download_matches(b"hello world", DEFAULT_BODY_SIZE);
        }

        #[test]
        fn encrypted_download_into_multi_level() {
            let refs_per_chunk = DEFAULT_BODY_SIZE / super::super::super::constants::REF_SIZE;
            assert_encrypted_download_matches(
                &sample(DEFAULT_BODY_SIZE * (refs_per_chunk + 1) + 17),
                8,
            );
        }

        #[test]
        fn encrypted_download_into_width_one() {
            assert_encrypted_download_matches(&sample(DEFAULT_BODY_SIZE * 5 + 7), 1);
        }
    }
}
