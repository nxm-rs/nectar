//! Traits for chunk types and operations
//!
//! This module defines the core traits that all chunk types must implement.

use crate::chunk::error;
use crate::error::Result;
use bytes::Bytes;

use super::address::ChunkAddress;

/// Core trait for chunk header
pub trait ChunkHeader {
    /// Get the wire header bytes for this chunk: everything that precedes the
    /// BMT body in the chunk's encoding (empty for a content chunk,
    /// `id || signature` for a single-owner chunk).
    fn bytes(&self) -> Bytes;
}

/// Core trait for all chunk types in the system.
///
/// This trait defines the common interface that all chunk implementations must provide.
pub trait Chunk: Send + Sync + 'static {
    /// The header type for this chunk
    type Header: ChunkHeader;

    /// Get the address of this chunk
    fn address(&self) -> &ChunkAddress;

    /// Get the header for this chunk
    fn header(&self) -> &Self::Header;

    /// Get the raw data contained in this chunk
    fn data(&self) -> &Bytes;

    /// Get the total size of this chunk in bytes
    #[allow(clippy::arithmetic_side_effects)] // header and payload are both bounded by the chunk wire size, far below usize::MAX
    fn size(&self) -> usize {
        self.header().bytes().len() + self.data().len()
    }

    /// Verify that this chunk matches an expected address
    fn verify(&self, expected: &ChunkAddress) -> Result<()> {
        let actual = self.address();
        if actual != expected {
            return Err(error::ChunkError::verification_failed(*expected, *actual).into());
        }
        Ok(())
    }
}

/// Trait for chunks that contain a BMT body
pub trait BmtChunk: Chunk {
    /// Get the span of the chunk data
    fn span(&self) -> u64;
}
