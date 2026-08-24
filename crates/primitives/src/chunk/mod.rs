//! Chunk types and operations
//!
//! The chunk type system (the carriers, their headers, the registry and the
//! trust typestate) lives in [`nectar_primitives_core::chunk`] and is
//! re-exported here unchanged. This module adds what a verifier does not
//! need: the encryption layer, the typed references, and the wasm bindings.
//!
//! See [`nectar_primitives_core::chunk`] for the type-system walkthrough and
//! for how to grow a network's registry with a custom chunk type.

pub use nectar_primitives_core::chunk::*;

#[cfg(feature = "encryption")]
mod encrypted_content;
pub mod encryption;
mod reference;

// Re-export the reference types
pub use reference::{ChunkRef, RefKind, Reference, WrongRefKind};

// Re-export the encryption layer over content chunks
#[cfg(feature = "encryption")]
pub use encrypted_content::EncryptedContentChunk;
#[cfg(feature = "encryption")]
pub use encryption::ChunkEncrypt;
