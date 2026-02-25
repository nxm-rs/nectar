//! Async I/O traits for file operations.

use std::future::Future;

/// Async random-access read trait.
pub trait AsyncReadAt: Send + Sync {
    /// Read data at offset into buffer.
    fn read_at(
        &self,
        offset: u64,
        buf: &mut [u8],
    ) -> impl Future<Output = std::io::Result<usize>> + Send;

    /// Total size of the data source.
    fn len(&self) -> u64;

    /// Whether the data source is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
