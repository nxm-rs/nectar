//! Encryption key type.

use std::mem::size_of;

use alloy_primitives::B256;
use subtle::ConstantTimeEq;
use zeroize::{Zeroize, ZeroizeOnDrop};

use super::error::EncryptionError;

/// 32-byte encryption key for chunk encryption.
///
/// Key material is zeroed on drop via `zeroize`. `Copy` is intentionally not
/// implemented to prevent implicit unzeroed copies on the stack.
/// Equality is constant-time via `subtle::ConstantTimeEq`.
#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct EncryptionKey([u8; size_of::<B256>()]);

impl ConstantTimeEq for EncryptionKey {
    fn ct_eq(&self, other: &Self) -> subtle::Choice {
        self.0.ct_eq(&other.0)
    }
}

impl PartialEq for EncryptionKey {
    fn eq(&self, other: &Self) -> bool {
        self.ct_eq(other).into()
    }
}

impl Eq for EncryptionKey {}

impl EncryptionKey {
    /// Byte length of an encryption key.
    pub const SIZE: usize = size_of::<B256>();

    /// Access the raw key bytes.
    pub const fn as_bytes(&self) -> &[u8; Self::SIZE] {
        &self.0
    }

    /// Generate a random encryption key.
    #[cfg(feature = "encryption")]
    pub fn generate() -> Self {
        use rand::RngExt;
        Self(rand::rng().random())
    }
}

impl From<[u8; EncryptionKey::SIZE]> for EncryptionKey {
    fn from(bytes: [u8; EncryptionKey::SIZE]) -> Self {
        Self(bytes)
    }
}

impl From<B256> for EncryptionKey {
    fn from(b: B256) -> Self {
        Self(b.0)
    }
}

impl AsRef<[u8; EncryptionKey::SIZE]> for EncryptionKey {
    fn as_ref(&self) -> &[u8; EncryptionKey::SIZE] {
        &self.0
    }
}

impl AsRef<[u8]> for EncryptionKey {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl TryFrom<&[u8]> for EncryptionKey {
    type Error = EncryptionError;

    fn try_from(slice: &[u8]) -> Result<Self, Self::Error> {
        if slice.len() != Self::SIZE {
            return Err(EncryptionError::InvalidKeyLength { len: slice.len() });
        }
        let mut bytes = [0u8; Self::SIZE];
        bytes.copy_from_slice(slice);
        Ok(Self(bytes))
    }
}

impl std::fmt::Debug for EncryptionKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Show first 4 bytes as hex for identification
        write!(
            f,
            "EncryptionKey({:02x}{:02x}{:02x}{:02x}..)",
            self.0[0], self.0[1], self.0[2], self.0[3]
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_bytes_roundtrip() {
        let bytes = [42u8; EncryptionKey::SIZE];
        let key = EncryptionKey::from(bytes);
        assert_eq!(key.as_bytes(), &bytes);
    }

    #[test]
    fn from_b256() {
        let b = B256::repeat_byte(0xab);
        let key = EncryptionKey::from(b);
        assert_eq!(
            <EncryptionKey as AsRef<[u8; EncryptionKey::SIZE]>>::as_ref(&key),
            &[0xab; EncryptionKey::SIZE]
        );
    }

    #[test]
    fn try_from_slice_valid() {
        let slice = [7u8; EncryptionKey::SIZE];
        let key = EncryptionKey::try_from(slice.as_slice()).unwrap();
        assert_eq!(
            <EncryptionKey as AsRef<[u8; EncryptionKey::SIZE]>>::as_ref(&key),
            &slice
        );
    }

    #[test]
    fn try_from_slice_invalid() {
        let short = [0u8; 16];
        let err = EncryptionKey::try_from(short.as_slice()).unwrap_err();
        assert!(matches!(err, EncryptionError::InvalidKeyLength { len: 16 }));
    }

    #[test]
    fn debug_shows_hex_prefix() {
        let key = EncryptionKey::from([
            0xab, 0xcd, 0xef, 0x01, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0,
        ]);
        let dbg = format!("{:?}", key);
        assert!(dbg.contains("abcdef01"));
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn generate_produces_key() {
        let k1 = EncryptionKey::generate();
        let k2 = EncryptionKey::generate();
        // Extremely unlikely to collide
        assert_ne!(k1, k2);
    }

    #[test]
    fn constant_time_equality() {
        let k1 = EncryptionKey::from([0x42; EncryptionKey::SIZE]);
        let k2 = EncryptionKey::from([0x42; EncryptionKey::SIZE]);
        let k3 = EncryptionKey::from([0x43; EncryptionKey::SIZE]);
        assert_eq!(k1, k2);
        assert_ne!(k1, k3);
    }
}
