//! Random-access byte sources for the batch ingest.

use alloc::vec::Vec;

use std::io;

use bytes::Bytes;

use crate::num::u64_from_usize;

/// Random-access byte source; reads at distinct offsets may run
/// concurrently from pool workers.
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

impl ReadAt for Vec<u8> {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        self.as_slice().read_at(offset, buf)
    }

    fn len(&self) -> io::Result<u64> {
        ReadAt::len(self.as_slice())
    }
}

impl ReadAt for Bytes {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        <[u8] as ReadAt>::read_at(self.as_ref(), offset, buf)
    }

    fn len(&self) -> io::Result<u64> {
        <[u8] as ReadAt>::len(self.as_ref())
    }
}

impl<T: ReadAt + ?Sized> ReadAt for &T {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        (**self).read_at(offset, buf)
    }

    fn len(&self) -> io::Result<u64> {
        (**self).len()
    }
}

#[cfg(unix)]
impl ReadAt for std::fs::File {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        std::os::unix::fs::FileExt::read_at(self, buf, offset)
    }

    fn len(&self) -> io::Result<u64> {
        self.metadata().map(|metadata| metadata.len())
    }
}

#[cfg(windows)]
impl ReadAt for std::fs::File {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        std::os::windows::fs::FileExt::seek_read(self, buf, offset)
    }

    fn len(&self) -> io::Result<u64> {
        self.metadata().map(|metadata| metadata.len())
    }
}
