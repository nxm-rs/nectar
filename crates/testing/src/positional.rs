//! The in-memory positional target the rest of the workspace tests against.

use std::io;
use std::vec::Vec;

use nectar_primitives::store::{BoxedError, ReadAt, WriteAt};

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
    fn read_at(&self, pos: u64, buf: &mut [u8]) -> Result<usize, BoxedError> {
        let Ok(pos) = usize::try_from(pos) else {
            return Ok(0);
        };
        let Some(tail) = self.data.get(pos..) else {
            return Ok(0);
        };
        let take = tail.len().min(buf.len());
        if take == 0 {
            return Ok(0);
        };
        buf[..take].copy_from_slice(&tail[..take]);
        Ok(take)
    }

    fn size(&self) -> Option<u64> {
        u64::try_from(self.data.len()).ok()
    }
}

impl WriteAt for MemWriteAt {
    fn write_all_at(&mut self, pos: u64, buf: &[u8]) -> Result<(), BoxedError> {
        let Ok(pos) = usize::try_from(pos) else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "write offset overflows the address space",
            )
            .into());
        };
        let end = pos
            .checked_add(buf.len())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "write range overflows"))?;
        if !buf.is_empty() && end > self.data.len() {
            self.data.resize(end, 0);
        }
        self.data[pos..end].copy_from_slice(buf);
        Ok(())
    }
}
