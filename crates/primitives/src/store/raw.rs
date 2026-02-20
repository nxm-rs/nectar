//! Raw-byte chunk storage traits.

use std::sync::Arc;

use crate::chunk::ChunkAddress;

/// Retrieve raw chunk data by address.
pub trait ChunkGetter {
    /// Load chunk data for the given address.
    fn get(&self, address: &ChunkAddress) -> Result<Vec<u8>, ChunkStoreError>;
}

/// Store raw chunk data by address.
///
/// The caller computes the content address before calling `put`.
// TODO: Replace `(address, data)` with a typed chunk parameter once
// encrypted chunk support lands in primitives. The chunk will know its
// own address (32 bytes for standard, 64 bytes for encrypted).
pub trait ChunkPutter {
    /// Store chunk data at the given address.
    fn put(&self, address: &ChunkAddress, data: &[u8]) -> Result<(), ChunkStoreError>;
}

/// Combined getter and putter.
pub trait ChunkStore: ChunkGetter + ChunkPutter {}
impl<T: ChunkGetter + ChunkPutter> ChunkStore for T {}

/// Errors from chunk storage operations.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ChunkStoreError {
    /// Chunk not found at the given address.
    #[error("chunk not found: {address_hex}")]
    NotFound {
        /// Hex-encoded address of the missing chunk.
        address_hex: String,
    },
    /// Catch-all for backend-specific errors.
    #[error("{0}")]
    Other(String),
}

// Blanket impls for references

impl<T: ChunkGetter> ChunkGetter for &T {
    fn get(&self, address: &ChunkAddress) -> Result<Vec<u8>, ChunkStoreError> {
        (**self).get(address)
    }
}

impl<T: ChunkPutter> ChunkPutter for &T {
    fn put(&self, address: &ChunkAddress, data: &[u8]) -> Result<(), ChunkStoreError> {
        (**self).put(address, data)
    }
}

// Blanket impls for Box

impl<T: ChunkGetter + ?Sized> ChunkGetter for Box<T> {
    fn get(&self, address: &ChunkAddress) -> Result<Vec<u8>, ChunkStoreError> {
        (**self).get(address)
    }
}

impl<T: ChunkPutter + ?Sized> ChunkPutter for Box<T> {
    fn put(&self, address: &ChunkAddress, data: &[u8]) -> Result<(), ChunkStoreError> {
        (**self).put(address, data)
    }
}

// Blanket impls for Arc

impl<T: ChunkGetter + ?Sized> ChunkGetter for Arc<T> {
    fn get(&self, address: &ChunkAddress) -> Result<Vec<u8>, ChunkStoreError> {
        (**self).get(address)
    }
}

impl<T: ChunkPutter + ?Sized> ChunkPutter for Arc<T> {
    fn put(&self, address: &ChunkAddress, data: &[u8]) -> Result<(), ChunkStoreError> {
        (**self).put(address, data)
    }
}
