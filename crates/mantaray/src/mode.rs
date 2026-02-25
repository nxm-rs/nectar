//! Manifest mode types for plain and encrypted manifests.

use nectar_primitives::chunk::ChunkAddress;
use nectar_primitives::file::EntryRef;

/// Trait defining the mode of a mantaray manifest.
pub trait ManifestMode: 'static {
    /// Size of references in bytes (32 for plain, 64 for encrypted).
    const REF_BYTES_SIZE: usize;
    /// The reference type accepted by `add()`.
    type Ref: Into<EntryRef>;
}

/// Plain manifest mode: 32-byte chunk addresses, no obfuscation.
#[derive(Debug)]
pub struct Plain;

impl ManifestMode for Plain {
    const REF_BYTES_SIZE: usize = 32;
    type Ref = ChunkAddress;
}

/// Encrypted manifest mode: 64-byte encrypted references, random obfuscation.
#[cfg(feature = "encryption")]
#[derive(Debug)]
pub struct Encrypted;

#[cfg(feature = "encryption")]
impl ManifestMode for Encrypted {
    const REF_BYTES_SIZE: usize = 64;
    type Ref = nectar_primitives::EncryptedChunkRef;
}
