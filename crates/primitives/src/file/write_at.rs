//! Async random-access write sink for out-of-order file reassembly.

use std::io;
use std::sync::Mutex;

use crate::store::{MaybeSend, MaybeSync};

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
            let n = self.seek_write(&buf[written..], offset + written as u64)?;
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

    async fn write_at(&self, offset: u64, buf: &[u8]) -> io::Result<()> {
        let mut vec = self
            .lock()
            .map_err(|_| io::Error::other("sink lock poisoned"))?;
        let end = offset as usize + buf.len();
        if vec.len() < end {
            vec.resize(end, 0);
        }
        vec[offset as usize..end].copy_from_slice(buf);
        Ok(())
    }

    async fn set_len(&self, len: u64) -> io::Result<()> {
        let mut vec = self
            .lock()
            .map_err(|_| io::Error::other("sink lock poisoned"))?;
        vec.resize(len as usize, 0);
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
