//! Core primitives for a decentralized storage system.
//!
//! This crate provides the foundational types and traits for working with
//! chunks and storage-related access control in a decentralized storage network.

#![cfg_attr(not(test), warn(unused_crate_dependencies))]
#![warn(missing_docs)]

// Re-export dependencies that are part of our public API
pub use bytes;

// Core modules
pub mod address;
pub mod bmt;
pub mod chunk;
pub mod error;

// WASM bindings - compiled only when targeting wasm32
#[cfg(target_arch = "wasm32")]
pub mod wasm;

// Re-exports of primary types
pub use address::SwarmAddress;
pub use bmt::{BMTHasher, error::DigestError};
pub use chunk::{ChunkAddress, ChunkType, CustomChunk, error::ChunkError};
pub use error::{Error, Result};

/// Constants used throughout the crate
pub mod constants {
    // Re-export BMT constants
    pub use crate::bmt::constants::*;

    /// Size of a chunk address in bytes (same as hash size)
    pub const ADDRESS_SIZE: usize = HASH_SIZE;

    /// Maximum size of a chunk in bytes
    pub const MAX_CHUNK_SIZE: usize = 4096;

    pub const MAX_PO: usize = 31;

    pub const EXTENDED_PO: usize = MAX_PO + 5;
}
