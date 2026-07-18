//! Data sinks: positional byte targets a download writes into.
//!
//! Writes are idempotent overwrites: rewriting a region with the same bytes
//! leaves the sink unchanged, so a failed download is recovered by running
//! it again in full. A resumable sink reporting persisted progress is a
//! future subtrait, not part of this contract.

use alloc::collections::TryReserveError;
use alloc::vec::Vec;

#[cfg(feature = "std")]
mod fs;
#[cfg(test)]
mod tests;

#[cfg(feature = "std")]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
pub use fs::FsSink;

/// Positional byte target with idempotent overwrite semantics.
pub trait DataSink {
    /// Typed write failure.
    type Error;

    /// Write `data` at absolute byte `offset`, growing the sink as needed;
    /// rewriting a region with the same bytes must be idempotent.
    fn write_at(&mut self, offset: u64, data: &[u8]) -> Result<(), Self::Error>;
}

/// Growable in-memory sink; unwritten gaps below the highest written end
/// read as zero.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MemSink {
    data: Vec<u8>,
}

impl MemSink {
    /// Create an empty sink.
    pub const fn new() -> Self {
        Self { data: Vec::new() }
    }

    /// Highest written end in bytes.
    pub const fn len(&self) -> usize {
        self.data.len()
    }

    /// Whether nothing has been written.
    pub const fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl AsRef<[u8]> for MemSink {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

impl From<MemSink> for Vec<u8> {
    fn from(sink: MemSink) -> Self {
        sink.data
    }
}

/// Typed in-memory write failures.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MemSinkError {
    /// The write's end offset does not fit the address space.
    #[error("write of {len} bytes at {offset} overflows the address space")]
    EndOverflow {
        /// Requested write offset.
        offset: u64,
        /// Bytes the write carries.
        len: usize,
    },
    /// The backing buffer could not grow.
    #[error(transparent)]
    Reserve(#[from] TryReserveError),
}

impl DataSink for MemSink {
    type Error = MemSinkError;

    fn write_at(&mut self, offset: u64, data: &[u8]) -> Result<(), MemSinkError> {
        let overflow = MemSinkError::EndOverflow {
            offset,
            len: data.len(),
        };
        let start = usize::try_from(offset).map_err(|_| overflow.clone())?;
        let end = start.checked_add(data.len()).ok_or(overflow)?;
        if end > self.data.len() {
            let grow = end.saturating_sub(self.data.len());
            self.data.try_reserve(grow)?;
            self.data.resize(end, 0);
        }
        for (slot, byte) in self.data.iter_mut().skip(start).zip(data) {
            *slot = *byte;
        }
        Ok(())
    }
}
