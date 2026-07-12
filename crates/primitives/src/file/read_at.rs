//! Sync random-access read trait for parallel file splitting.

use std::io;

use bytes::Bytes;

/// Sync data source supporting offset-based reads.
///
/// Enables parallel splitting by allowing concurrent reads at different offsets.
pub trait ReadAt {
    /// Read data at offset into buffer, returning bytes read.
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize>;

    /// Total size of the data source.
    fn len(&self) -> u64;

    /// Whether the data source is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl ReadAt for [u8] {
    #[allow(clippy::arithmetic_side_effects, clippy::indexing_slicing)] // offset < self.len() is checked above, and to_read = min(buf.len(), self.len() - offset) bounds both slices
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        let offset = crate::cast::usize_from_u64(offset);
        if offset >= self.len() {
            return Ok(0);
        }
        let available = self.len() - offset;
        let to_read = buf.len().min(available);
        buf[..to_read].copy_from_slice(&self[offset..offset + to_read]);
        Ok(to_read)
    }

    fn len(&self) -> u64 {
        crate::cast::u64_from_usize(<[u8]>::len(self))
    }
}

impl ReadAt for Vec<u8> {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        self.as_slice().read_at(offset, buf)
    }

    fn len(&self) -> u64 {
        crate::cast::u64_from_usize(Self::len(self))
    }
}

impl ReadAt for Bytes {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        self.as_ref().read_at(offset, buf)
    }

    fn len(&self) -> u64 {
        crate::cast::u64_from_usize(Self::len(self))
    }
}

impl<T: ReadAt + ?Sized> ReadAt for &T {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        (**self).read_at(offset, buf)
    }

    fn len(&self) -> u64 {
        (**self).len()
    }
}

#[cfg(unix)]
impl ReadAt for std::fs::File {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        use std::os::unix::fs::FileExt;
        FileExt::read_at(self, buf, offset)
    }

    fn len(&self) -> u64 {
        self.metadata().map(|m| m.len()).unwrap_or(0)
    }
}

#[cfg(windows)]
impl ReadAt for std::fs::File {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        use std::os::windows::fs::FileExt;
        FileExt::seek_read(self, buf, offset)
    }

    fn len(&self) -> u64 {
        self.metadata().map(|m| m.len()).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_at_slice() {
        let data = b"hello world";
        let mut buf = [0u8; 5];

        let n = data.as_slice().read_at(0, &mut buf).unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf, b"hello");

        let n = data.as_slice().read_at(6, &mut buf).unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf, b"world");

        let n = data.as_slice().read_at(9, &mut buf).unwrap();
        assert_eq!(n, 2);
        assert_eq!(&buf[..2], b"ld");

        let n = data.as_slice().read_at(100, &mut buf).unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn test_read_at_vec() {
        let data = vec![0u8, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        let mut buf = [0u8; 3];

        let n = data.read_at(5, &mut buf).unwrap();
        assert_eq!(n, 3);
        assert_eq!(buf, [5, 6, 7]);

        assert_eq!(data.len(), 10);
    }

    #[test]
    fn test_read_at_bytes() {
        let data = Bytes::from_static(b"test data");
        let mut buf = [0u8; 4];

        let n = data.read_at(5, &mut buf).unwrap();
        assert_eq!(n, 4);
        assert_eq!(&buf, b"data");

        assert_eq!(ReadAt::len(&data), 9);
    }

    #[test]
    fn test_read_at_ref() {
        let data = b"reference";
        let r: &[u8] = data;
        let mut buf = [0u8; 3];

        let n = (&r).read_at(0, &mut buf).unwrap();
        assert_eq!(n, 3);
        assert_eq!(&buf, b"ref");
    }
}
