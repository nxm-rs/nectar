//! Data sinks: positional byte targets a download writes into.
//!
//! Writes are idempotent overwrites: rewriting a region with the same bytes
//! leaves the sink unchanged, so a failed download is recovered by running
//! it again in full.

use alloc::collections::TryReserveError;
use alloc::vec::Vec;

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
#[derive(Debug, Clone, Default, PartialEq, Eq, derive_more::AsRef, derive_more::Into)]
pub struct MemSink {
    #[as_ref([u8])]
    data: Vec<u8>,
}

impl MemSink {
    /// Create an empty sink.
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

#[cfg(test)]
mod tests {
    use alloc::vec::Vec;

    use super::{DataSink, MemSink, MemSinkError};

    #[test]
    fn mem_sink_assembles_overwrites_and_zero_fills() {
        // Out-of-order writes assemble.
        let mut sink = MemSink::new();
        sink.write_at(6, b"world").unwrap();
        sink.write_at(0, b"hello ").unwrap();
        assert_eq!(sink.as_ref(), b"hello world");
        assert_eq!(sink.len(), 11);
        assert!(!sink.is_empty());
        assert_eq!(Vec::from(sink), b"hello world");

        // Rewriting the same bytes at the same offsets changes nothing.
        let mut sink = MemSink::new();
        sink.write_at(0, b"abcdef").unwrap();
        let before = sink.clone();
        sink.write_at(2, b"cd").unwrap();
        sink.write_at(0, b"abcdef").unwrap();
        assert_eq!(sink, before);
        // A genuine overwrite replaces exactly the covered region.
        sink.write_at(2, b"XY").unwrap();
        assert_eq!(sink.as_ref(), b"abXYef");

        // Unwritten gaps read as zero; an empty write past the end still
        // marks the extent.
        let mut sink = MemSink::new();
        sink.write_at(4, b"z").unwrap();
        assert_eq!(sink.as_ref(), b"\0\0\0\0z");
        sink.write_at(8, b"").unwrap();
        assert_eq!(sink.len(), 8);
    }

    #[test]
    fn mem_sink_rejects_end_overflow() {
        let mut sink = MemSink::new();
        let err = sink.write_at(u64::MAX, b"x").unwrap_err();
        assert_eq!(
            err,
            MemSinkError::EndOverflow {
                offset: u64::MAX,
                len: 1,
            }
        );
        assert!(sink.is_empty(), "a rejected write must leave no trace");
    }
}
