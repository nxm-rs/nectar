//! The in-memory positional target the positional-io swaps test against.

use std::io;

use positioned_io::{ReadAt, Size, WriteAt};

/// Growable in-memory positional bytes; unwritten gaps below the highest
/// written end read as zero.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MemWriteAt {
    data: Vec<u8>,
}

impl MemWriteAt {
    /// Create an empty target.
    #[must_use]
    pub const fn new() -> Self {
        Self { data: Vec::new() }
    }

    /// Highest written end in bytes.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.data.len()
    }

    /// Whether nothing has been written.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// The written bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    /// Consume into the written bytes.
    pub fn into_bytes(self) -> Vec<u8> {
        self.data
    }
}

impl ReadAt for MemWriteAt {
    fn read_at(&self, pos: u64, buf: &mut [u8]) -> io::Result<usize> {
        let Ok(pos) = usize::try_from(pos) else {
            return Ok(0);
        };
        let Some(tail) = self.data.get(pos..) else {
            return Ok(0);
        };
        let take = tail.len().min(buf.len());
        if take == 0 {
            return Ok(0);
        }
        buf[..take].copy_from_slice(&tail[..take]);
        Ok(take)
    }
}

impl WriteAt for MemWriteAt {
    fn write_at(&mut self, pos: u64, buf: &[u8]) -> io::Result<usize> {
        let Ok(pos) = usize::try_from(pos) else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "write offset overflows the address space",
            ));
        };
        let end = pos
            .checked_add(buf.len())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "write range overflows"))?;
        if buf.is_empty() {
            return Ok(0);
        }
        if end > self.data.len() {
            self.data.resize(end, 0);
        }
        self.data[pos..end].copy_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Size for MemWriteAt {
    fn size(&self) -> io::Result<Option<u64>> {
        let Ok(size) = u64::try_from(self.data.len()) else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "size overflows",
            ));
        };
        Ok(Some(size))
    }
}
