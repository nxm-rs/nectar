//! Encrypted manifest reference (root address + obfuscation key).

use nectar_primitives::chunk::ChunkAddress;

use crate::error::MantarayError;
use crate::obfuscation::ObfuscationKey;

/// Root reference for an encrypted manifest: chunk address + obfuscation key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ManifestRef {
    address: ChunkAddress,
    obfuscation_key: ObfuscationKey,
}

impl ManifestRef {
    /// Wire width: root address followed by obfuscation key.
    pub const SIZE: usize = size_of::<ChunkAddress>() + ObfuscationKey::SIZE;

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

    /// Serialise to the fixed wire form: address followed by obfuscation key.
    pub const fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];
        let (address, key) = buf.split_at_mut(size_of::<ChunkAddress>());
        address.copy_from_slice(self.address.as_bytes());
        key.copy_from_slice(self.obfuscation_key.as_bytes());
        buf
    }

    /// Reconstruct from the fixed wire form: any 64 bytes are a valid reference.
    pub fn from_bytes(bytes: &[u8; Self::SIZE]) -> Self {
        let mut address = [0u8; size_of::<ChunkAddress>()];
        let mut key = [0u8; ObfuscationKey::SIZE];
        let (a, k) = bytes.split_at(size_of::<ChunkAddress>());
        address.copy_from_slice(a);
        key.copy_from_slice(k);
        Self::new(ChunkAddress::from(address), ObfuscationKey::from(key))
    }
}

impl From<&ManifestRef> for [u8; ManifestRef::SIZE] {
    fn from(r: &ManifestRef) -> Self {
        r.to_bytes()
    }
}

impl From<ManifestRef> for [u8; ManifestRef::SIZE] {
    fn from(r: ManifestRef) -> Self {
        r.to_bytes()
    }
}

impl From<[u8; Self::SIZE]> for ManifestRef {
    fn from(bytes: [u8; Self::SIZE]) -> Self {
        Self::from_bytes(&bytes)
    }
}

impl From<&ManifestRef> for Vec<u8> {
    fn from(r: &ManifestRef) -> Self {
        r.to_bytes().to_vec()
    }
}

impl TryFrom<&[u8]> for ManifestRef {
    type Error = MantarayError;

    fn try_from(slice: &[u8]) -> Result<Self, Self::Error> {
        let bytes: &[u8; Self::SIZE] =
            slice
                .try_into()
                .map_err(|_| MantarayError::EntrySizeMismatch {
                    expected: Self::SIZE,
                    actual: slice.len(),
                })?;
        Ok(Self::from_bytes(bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nectar_primitives::chunk::ChunkAddress;

    fn sample() -> ManifestRef {
        let address = ChunkAddress::from([0xcd; 32]);
        let obfuscation_key = ObfuscationKey::from([0xef; 32]);
        ManifestRef::new(address, obfuscation_key)
    }

    #[test]
    fn size_is_address_plus_key() {
        assert_eq!(ManifestRef::SIZE, 64);
    }

    #[test]
    fn to_bytes_layout() {
        let bytes = sample().to_bytes();
        assert_eq!(&bytes[..32], &[0xcd; 32]);
        assert_eq!(&bytes[32..], &[0xef; 32]);
    }

    #[test]
    fn fixed_array_roundtrip() {
        let reference = sample();
        let bytes: [u8; 64] = (&reference).into();

        assert_eq!(ManifestRef::from_bytes(&bytes), reference);
        assert_eq!(ManifestRef::from(bytes), reference);
        assert_eq!(<[u8; 64]>::from(reference), bytes);
    }

    #[test]
    fn slice_roundtrip() {
        let reference = sample();
        let bytes = Vec::from(&reference);
        assert_eq!(bytes.len(), 64);
        assert_eq!(ManifestRef::try_from(bytes.as_slice()).unwrap(), reference);
    }

    #[test]
    fn invalid_length() {
        let bad = [0u8; 48];
        let err = ManifestRef::try_from(bad.as_slice()).unwrap_err();
        assert!(matches!(
            err,
            MantarayError::EntrySizeMismatch {
                expected: 64,
                actual: 48
            }
        ));
    }
}
