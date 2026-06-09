//! Encrypted manifest reference (root address + obfuscation key).

use nectar_primitives::chunk::ChunkAddress;

use crate::obfuscation::ObfuscationKey;

/// Root reference for an encrypted manifest: chunk address + obfuscation key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ManifestRef {
    address: ChunkAddress,
    obfuscation_key: ObfuscationKey,
}

impl ManifestRef {
    /// Create a new manifest reference.
    pub const fn new(address: ChunkAddress, obfuscation_key: ObfuscationKey) -> Self {
        Self {
            address,
            obfuscation_key,
        }
    }

    /// The root chunk address.
    pub const fn address(&self) -> &ChunkAddress {
        &self.address
    }

    /// The obfuscation key.
    pub const fn obfuscation_key(&self) -> &ObfuscationKey {
        &self.obfuscation_key
    }

    /// Consume into (address, obfuscation_key).
    pub const fn into_parts(self) -> (ChunkAddress, ObfuscationKey) {
        (self.address, self.obfuscation_key)
    }
}
