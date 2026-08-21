//! Byte sources the write side pulls from.
//!
//! [`Source`] is the one ingest seam a [`File::save`](crate::File::save)
//! drains. Three adapters cover the shapes the pipeline meets: an in-memory
//! slice (the blanket `&[u8]` impl), a positional [`ReadAt`] target through
//! [`ReadAtSource`], and an async byte stream through
//! [`AsyncReadSource`]. A pull of zero bytes
//! is the end of the source; a short non-zero pull is legal and the caller
//! pulls again.

use core::convert::Infallible;
use core::task::{Context, Poll};

// The positional adapter needs `io`; a test build links std even when
// the feature is off, so the in-crate tests keep covering it.
#[cfg(any(test, feature = "std"))]
use std::io;

// Only the positional adapter measures lengths.
#[cfg(any(test, feature = "std"))]
use crate::num::u64_from_usize;

/// Pull-based byte source feeding one write.
///
/// The caller owns the buffer and pulls until the source reports zero, so a
/// source never retains the bytes it hands over.
pub trait Source {
    /// Typed pull failure.
    type Error;

    /// Fill the front of `buf`, delivering the byte count; zero ends the
    /// source. A count past `buf.len()` breaks the contract and the driver
    /// clamps it, so a broken source cannot stall the write.
    fn poll_fill(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, Self::Error>>;
}

impl Source for &[u8] {
    type Error = Infallible;

    fn poll_fill(
        &mut self,
        _cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, Infallible>> {
        let take = <[u8]>::len(self).min(buf.len());
        let (Some(head), Some((dst, _))) = (self.get(..take), buf.split_at_mut_checked(take))
        else {
            return Poll::Ready(Ok(0));
        };
        dst.copy_from_slice(head);
        *self = self.get(take..).unwrap_or_default();
        Poll::Ready(Ok(take))
    }
}

impl<T: Source + ?Sized> Source for &mut T {
    type Error = T::Error;

    fn poll_fill(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, Self::Error>> {
        (**self).poll_fill(cx, buf)
    }
}

/// Random-access byte source; reads at distinct offsets are independent, so
/// a positional target needs no cursor of its own.
#[cfg(any(test, feature = "std"))]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
pub trait ReadAt {
    /// Read into `buf` at `offset`, returning the bytes read; zero at or
    /// past the end.
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize>;

    /// Total source length in bytes.
    fn len(&self) -> io::Result<u64>;

    /// Whether the source has no bytes.
    fn is_empty(&self) -> io::Result<bool> {
        Ok(self.len()? == 0)
    }
}

#[cfg(any(test, feature = "std"))]
impl ReadAt for [u8] {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        let Ok(offset) = usize::try_from(offset) else {
            return Ok(0);
        };
        let Some(tail) = self.get(offset..) else {
            return Ok(0);
        };
        let take = tail.len().min(buf.len());
        let (Some(src), Some((dst, _))) = (tail.get(..take), buf.split_at_mut_checked(take)) else {
            return Ok(0);
        };
        dst.copy_from_slice(src);
        Ok(take)
    }

    fn len(&self) -> io::Result<u64> {
        Ok(u64_from_usize(<[u8]>::len(self)))
    }
}

#[cfg(any(test, feature = "std"))]
impl ReadAt for alloc::vec::Vec<u8> {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        self.as_slice().read_at(offset, buf)
    }

    fn len(&self) -> io::Result<u64> {
        ReadAt::len(self.as_slice())
    }
}

#[cfg(all(any(test, feature = "std"), feature = "primitives"))]
impl ReadAt for bytes::Bytes {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        <[u8] as ReadAt>::read_at(self.as_ref(), offset, buf)
    }

    fn len(&self) -> io::Result<u64> {
        <[u8] as ReadAt>::len(self.as_ref())
    }
}

#[cfg(any(test, feature = "std"))]
impl<T: ReadAt + ?Sized> ReadAt for &T {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        (**self).read_at(offset, buf)
    }

    fn len(&self) -> io::Result<u64> {
        (**self).len()
    }
}

#[cfg(all(any(test, feature = "std"), unix))]
impl ReadAt for std::fs::File {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        std::os::unix::fs::FileExt::read_at(self, buf, offset)
    }

    fn len(&self) -> io::Result<u64> {
        self.metadata().map(|metadata| metadata.len())
    }
}

#[cfg(all(any(test, feature = "std"), windows))]
impl ReadAt for std::fs::File {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        std::os::windows::fs::FileExt::seek_read(self, buf, offset)
    }

    fn len(&self) -> io::Result<u64> {
        self.metadata().map(|metadata| metadata.len())
    }
}

/// Terminal failure pulling from a [`ReadAt`] target; every variant is
/// final for the write that met it.
#[cfg(any(test, feature = "std"))]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
#[derive(Debug, thiserror::Error)]
pub enum ReadAtError {
    /// Sizing the source failed.
    #[error("source length unavailable")]
    Length {
        /// Io error behind the failure.
        source: io::Error,
    },
    /// Reading a leaf body from the source failed.
    #[error("read failed at offset {offset}")]
    Read {
        /// Offset of the failed read.
        offset: u64,
        /// Io error behind the failure.
        source: io::Error,
    },
    /// The source reported its end before the declared length was reached.
    #[error("short read at offset {offset}: {remaining} bytes missing")]
    ShortRead {
        /// Offset of the zero-length read.
        offset: u64,
        /// Bytes still unread at that offset.
        remaining: usize,
    },
    /// The source reported more bytes than the read buffer holds.
    #[error("read overrun at offset {offset}: {count} bytes into {capacity}")]
    ReadOverrun {
        /// Offset of the overlong read.
        offset: u64,
        /// Byte count the source reported.
        count: usize,
        /// Buffer bytes the read had to fill.
        capacity: usize,
    },
}

/// [`Source`] adapter over a positional [`ReadAt`] target.
///
/// The declared length is read once, at the first pull, and every pull after
/// it fills its request exactly; running out early is a typed
/// [`ReadAtError::ShortRead`], never a silent truncation.
#[cfg(any(test, feature = "std"))]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
#[derive(Debug)]
pub struct ReadAtSource<R> {
    source: R,
    offset: u64,
    len: Option<u64>,
}

#[cfg(any(test, feature = "std"))]
impl<R> ReadAtSource<R> {
    /// Adapt `source`; its length is read at the first pull.
    pub const fn new(source: R) -> Self {
        Self {
            source,
            offset: 0,
            len: None,
        }
    }

    /// Consume back into the wrapped target.
    pub fn into_inner(self) -> R {
        self.source
    }
}

#[cfg(any(test, feature = "std"))]
impl<R: ReadAt> ReadAtSource<R> {
    /// The declared length, sized once and memoized.
    fn declared(&mut self) -> Result<u64, ReadAtError> {
        match self.len {
            Some(len) => Ok(len),
            None => {
                let len = self
                    .source
                    .len()
                    .map_err(|source| ReadAtError::Length { source })?;
                self.len = Some(len);
                Ok(len)
            }
        }
    }

    /// Fill the front of `buf`, capped by the bytes the source still owes.
    fn fill(&mut self, buf: &mut [u8]) -> Result<usize, ReadAtError> {
        let remaining = self.declared()?.saturating_sub(self.offset);
        if remaining == 0 || buf.is_empty() {
            return Ok(0);
        }
        // The request is capped by the buffer length, so the narrowing is
        // lossless.
        let take = usize::try_from(remaining.min(u64_from_usize(buf.len()))).unwrap_or(buf.len());
        let Some((body, _)) = buf.split_at_mut_checked(take) else {
            return Ok(0);
        };
        read_full(&self.source, self.offset, body)?;
        self.offset = self.offset.saturating_add(u64_from_usize(take));
        Ok(take)
    }
}

#[cfg(any(test, feature = "std"))]
impl<R: ReadAt> Source for ReadAtSource<R> {
    type Error = ReadAtError;

    fn poll_fill(
        &mut self,
        _cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, ReadAtError>> {
        Poll::Ready(self.fill(buf))
    }
}

/// Fill `buf` from `offset`, looping over short reads; running out of
/// source is an error, never a silent truncation.
#[cfg(any(test, feature = "std"))]
fn read_full<R: ReadAt + ?Sized>(
    source: &R,
    offset: u64,
    buf: &mut [u8],
) -> Result<(), ReadAtError> {
    let mut filled = 0usize;
    while filled < buf.len() {
        let at = offset.saturating_add(u64_from_usize(filled));
        let Some(rest) = buf.get_mut(filled..) else {
            return Ok(());
        };
        let capacity = rest.len();
        let count = source
            .read_at(at, rest)
            .map_err(|source| ReadAtError::Read { offset: at, source })?;
        if count == 0 {
            return Err(ReadAtError::ShortRead {
                offset: at,
                remaining: capacity,
            });
        }
        if count > capacity {
            return Err(ReadAtError::ReadOverrun {
                offset: at,
                count,
                capacity,
            });
        }
        filled = filled.saturating_add(count);
    }
    Ok(())
}

/// [`Source`] adapter over any [`AsyncRead`](::tokio::io::AsyncRead).
///
/// The stream declares no length, so the write side simply pulls until the
/// reader reports end of stream.
#[cfg(feature = "tokio")]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
#[derive(Debug)]
pub struct AsyncReadSource<R> {
    inner: R,
}

#[cfg(feature = "tokio")]
impl<R> AsyncReadSource<R> {
    /// Adapt `inner` into a pull source.
    pub const fn new(inner: R) -> Self {
        Self { inner }
    }

    /// Consume back into the wrapped reader.
    pub fn into_inner(self) -> R {
        self.inner
    }
}

#[cfg(feature = "tokio")]
impl<R: ::tokio::io::AsyncRead + Unpin> Source for AsyncReadSource<R> {
    type Error = io::Error;

    fn poll_fill(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, io::Error>> {
        let mut read = ::tokio::io::ReadBuf::new(buf);
        match core::pin::Pin::new(&mut self.inner).poll_read(cx, &mut read) {
            Poll::Ready(Ok(())) => Poll::Ready(Ok(read.filled().len())),
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use alloc::vec;
    use alloc::vec::Vec;

    use nectar_testing::run;

    use super::*;

    /// Drain a source through a `size`-byte buffer, as one write does.
    fn drain<S: Source>(mut source: S, size: usize) -> Result<Vec<u8>, S::Error> {
        run(async move {
            let mut buf = vec![0u8; size];
            let mut out = Vec::new();
            loop {
                let filled = core::future::poll_fn(|cx| source.poll_fill(cx, &mut buf)).await?;
                if filled == 0 {
                    return Ok(out);
                }
                out.extend_from_slice(&buf[..filled]);
            }
        })
    }

    #[test]
    fn a_slice_source_delivers_every_byte_then_ends() {
        let data: Vec<u8> = (0..500u32).map(|i| (i % 251) as u8).collect();
        assert_eq!(drain(data.as_slice(), 64).unwrap(), data);
        assert_eq!(drain(&[][..], 64).unwrap(), Vec::<u8>::new());
    }

    #[test]
    fn read_at_sources_honour_offsets_and_ends() {
        let data: Vec<u8> = (0..100u32).map(|i| (i % 251) as u8).collect();
        let slice: &[u8] = &data;
        let mut buf = vec![0u8; 40];
        assert_eq!(slice.read_at(0, &mut buf).unwrap(), 40);
        assert_eq!(buf, data[..40]);
        assert_eq!(slice.read_at(80, &mut buf).unwrap(), 20);
        assert_eq!(buf[..20], data[80..]);
        assert_eq!(slice.read_at(100, &mut buf).unwrap(), 0);
        assert_eq!(slice.read_at(u64::MAX, &mut buf).unwrap(), 0);
        assert_eq!(ReadAt::len(slice).unwrap(), 100);
    }

    #[test]
    fn a_shared_buffer_reads_at_the_same_offsets() {
        let data: Vec<u8> = (0..100u32).map(|i| (i % 251) as u8).collect();
        let owned = bytes::Bytes::from(data.clone());
        let mut buf = vec![0u8; 40];
        assert_eq!(owned.read_at(60, &mut buf).unwrap(), 40);
        assert_eq!(buf, data[60..]);
        assert_eq!(ReadAt::len(&owned).unwrap(), 100);
    }

    #[test]
    fn a_read_at_source_matches_the_slice_source() {
        let data: Vec<u8> = (0..1_000u32).map(|i| (i % 251) as u8).collect();
        let through_read_at = drain(ReadAtSource::new(data.clone()), 128).unwrap();
        assert_eq!(through_read_at, data);
    }
}
