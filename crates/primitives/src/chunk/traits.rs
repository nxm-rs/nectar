//! Traits for chunk types and operations
//!
//! This module defines the core traits that all chunk types must implement,
//! along with serialization and deserialization functionality.

use crate::SwarmAddress;
use crate::chunk::error;
use crate::error::Result;
use bytes::{BufMut, Bytes, BytesMut};

/// Type alias for chunk addresses
pub type ChunkAddress = SwarmAddress;

/// Core trait for chunk metadata
pub trait ChunkMetadata {
    /// Get the metadata bytes for this chunk
    fn bytes(&self) -> Bytes;
}

/// Core trait for chunk header
pub trait ChunkHeader {
    /// The metadata type for this chunk
    type Metadata: ChunkMetadata;

    /// Get the identifier byte for this chunk type
    fn id(&self) -> u8;

    /// Get the version byte for this chunk type
    fn version(&self) -> u8;

    /// Get the metadata bytes for this chunk
    fn metadata(&self) -> &Self::Metadata;

    /// Get the header bytes for this chunk
    fn bytes(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(2);
        buf.put_u8(self.id());
        buf.put_u8(self.version());
        buf.put_slice(self.metadata().bytes().as_ref());
        buf.freeze()
    }
}

/// Core trait for all chunk types in the system.
///
/// This trait defines the common interface that all chunk implementations must provide.
/// Each implementation must specify its type ID and version as associated constants.
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

/// Trait for serializing chunks with type prefix.
///
/// This trait provides methods for serializing a chunk with its type
/// ID and version prefix.
pub trait ChunkSerialization {
    /// Serialize this chunk with its type ID and version prefix
    fn serialize_with_prefix(&self) -> Bytes;
}

impl<T: Chunk> ChunkSerialization for T {
    fn serialize_with_prefix(&self) -> Bytes {
        let mut bytes = BytesMut::with_capacity(2 + self.size());
        bytes.extend(self.header().bytes());
        bytes.extend(self.data());
        bytes.freeze()
    }
}

/// Trait for chunks that contain a BMT body
pub trait BmtChunk: Chunk {
    /// Get the span of the chunk data
    fn span(&self) -> u64;
}
