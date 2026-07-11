//! Typed nonce used in overlay address derivation.
//!
//! A [`Nonce`] is the 32-byte value mixed with an Ethereum address and a
//! [`NetworkId`](crate::NetworkId) when deriving the Swarm overlay address.
//! See [`compute_overlay`](crate::compute_overlay) for the canonical
//! derivation matching bee `pkg/crypto/crypto.go:45-57`.

use alloy_primitives::B256;
use derive_more::{AsRef, Display, From, Into};

use crate::error::WrongLength;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// 32-byte nonce mixed into the overlay address.
///
/// Persistent nodes (storers, bootnodes) keep the nonce stable across restarts
/// so their overlay address is stable. Ephemeral nodes (clients) may rotate
/// the nonce per run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into, AsRef)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
#[display("{_0}")]
#[from(B256, [u8; 32])]
#[into(B256, [u8; 32])]
#[as_ref([u8])]
pub struct Nonce(B256);

impl Nonce {
    /// Zero nonce, useful for tests and deterministic vectors.
    pub const ZERO: Self = Self(B256::ZERO);

    /// Construct from raw 32 bytes. `const` for static contexts; for runtime
    /// conversions prefer the `From` impls.
    #[inline]
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(B256::new(bytes))
    }

    /// Borrow the underlying 32 bytes.
    #[inline]
    pub const fn as_slice(&self) -> &[u8] {
        self.0.as_slice()
    }

    /// Sample a cryptographically random nonce via `alloy_primitives::B256::random`.
    pub fn random() -> Self {
        Self(B256::random())
    }
}

impl TryFrom<&[u8]> for Nonce {
    type Error = WrongLength;

    fn try_from(slice: &[u8]) -> Result<Self, Self::Error> {
        let bytes: [u8; 32] = slice.try_into().map_err(|_| WrongLength {
            expected: 32,
            got: slice.len(),
        })?;
        Ok(Self::new(bytes))
    }
}

#[cfg(any(test, feature = "arbitrary"))]
impl<'a> arbitrary::Arbitrary<'a> for Nonce {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(Self::new(u.arbitrary()?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_is_all_zero_bytes() {
        assert_eq!(Nonce::ZERO.as_slice(), &[0u8; 32]);
    }

    #[test]
    fn roundtrips_via_from_impls() {
        let bytes = [7u8; 32];
        let n = Nonce::new(bytes);
        assert_eq!(B256::from(n), B256::new(bytes));
        assert_eq!(Nonce::from(B256::new(bytes)), n);
        assert_eq!(<[u8; 32]>::from(n), bytes);
        assert_eq!(Nonce::from(bytes), n);
    }

    #[test]
    fn try_from_slice_valid() {
        let bytes = [9u8; 32];
        assert_eq!(
            Nonce::try_from(bytes.as_slice()).unwrap(),
            Nonce::new(bytes)
        );
    }

    #[test]
    fn try_from_slice_wrong_length() {
        let short = [0u8; 16];
        assert_eq!(
            Nonce::try_from(short.as_slice()).unwrap_err(),
            WrongLength {
                expected: 32,
                got: 16
            }
        );
    }

    #[test]
    fn display_matches_b256_lowercase_hex() {
        let n = Nonce::new([0xab; 32]);
        let rendered = format!("{n}");
        assert!(rendered.starts_with("0x"));
        assert_eq!(rendered.len(), 66);
        assert!(rendered.chars().skip(2).all(|c| c.is_ascii_hexdigit()));
    }
}
