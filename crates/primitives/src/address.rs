//! Typed overlay address for node identity.
//!
//! An [`OverlayAddress`] names a node by the canonical derivation
//! `keccak256(ethereum_address || network_id || nonce)` (see
//! [`compute_overlay`](crate::compute_overlay)). It is nominally distinct
//! from the content-address kind; cross-kind proximity goes through
//! [`XorMetric`](crate::XorMetric).

use alloy_primitives::B256;
use derive_more::{AsRef, Display, From, Into};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::error::{Result, WrongLength};
use crate::xor_metric::XorMetric;

/// 32-byte overlay address of a node.
///
/// Transparent over the same 32 wire bytes as the alias it replaces: every
/// handshake sign-data buffer and routing-table key serializes identically.
#[derive(
    Debug, Default, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into, AsRef,
)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
#[display("{_0}")]
#[from(B256, [u8; 32])]
#[into(B256, [u8; 32])]
#[as_ref([u8])]
#[repr(transparent)]
pub struct OverlayAddress(B256);

impl OverlayAddress {
    /// Width in bytes of an address.
    pub const SIZE: usize = size_of::<B256>();

    /// Zero address, useful for tests and sentinel slots.
    pub const ZERO: Self = Self(B256::ZERO);

    /// Construct from raw 32 bytes. `const` for static contexts; for runtime
    /// conversions prefer the `From` impls.
    #[inline]
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(B256::new(bytes))
    }

    /// Creates an address with only the first byte set, rest zeros.
    ///
    /// The first byte controls proximity order (leading bits determine PO).
    pub const fn with_first_byte(byte: u8) -> Self {
        let mut bytes = [0u8; 32];
        bytes[0] = byte;
        Self(B256::new(bytes))
    }

    /// Borrow the underlying 32 bytes.
    #[inline]
    pub const fn as_bytes(&self) -> &[u8] {
        self.0.as_slice()
    }

    /// Creates a new address from a slice, checking the length.
    ///
    /// The error carries expected and actual lengths via [`WrongLength`].
    pub fn from_slice(slice: &[u8]) -> Result<Self> {
        Ok(Self::try_from(slice)?)
    }

    /// Checks if this address is zeros.
    pub fn is_zero(&self) -> bool {
        self.0.is_zero()
    }

    /// Create a new zero-filled address.
    pub const fn zero() -> Self {
        Self::ZERO
    }
}

impl XorMetric for OverlayAddress {
    fn point(&self) -> &[u8; 32] {
        &self.0.0
    }
}

impl TryFrom<&[u8]> for OverlayAddress {
    type Error = WrongLength;

    fn try_from(slice: &[u8]) -> std::result::Result<Self, Self::Error> {
        let bytes: [u8; 32] = slice.try_into().map_err(|_| WrongLength {
            expected: 32,
            got: slice.len(),
        })?;
        Ok(Self::new(bytes))
    }
}

#[cfg(any(test, feature = "arbitrary"))]
impl<'a> arbitrary::Arbitrary<'a> for OverlayAddress {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(Self::new(u.arbitrary()?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::PrimitivesError;

    #[test]
    fn zero_is_all_zero_bytes() {
        assert_eq!(OverlayAddress::ZERO.as_bytes(), &[0u8; 32]);
        assert!(OverlayAddress::zero().is_zero());
    }

    #[test]
    fn roundtrips_via_from_impls() {
        let bytes = [0x5au8; 32];
        let addr = OverlayAddress::new(bytes);
        assert_eq!(B256::from(addr), B256::new(bytes));
        assert_eq!(OverlayAddress::from(B256::new(bytes)), addr);
        assert_eq!(<[u8; 32]>::from(addr), bytes);
        assert_eq!(OverlayAddress::from(bytes), addr);
    }

    #[test]
    fn with_first_byte_sets_only_the_first_byte() {
        let addr = OverlayAddress::with_first_byte(0x80);
        let mut expected = [0u8; 32];
        expected[0] = 0x80;
        assert_eq!(addr.as_bytes(), &expected);
    }

    #[test]
    fn try_from_slice_wrong_length() {
        let short = [0u8; 31];
        assert_eq!(
            OverlayAddress::try_from(short.as_slice()).unwrap_err(),
            WrongLength {
                expected: 32,
                got: 31
            }
        );
    }

    #[test]
    fn from_slice_carries_lengths() {
        let long = [0u8; 33];
        let err = OverlayAddress::from_slice(&long).unwrap_err();
        assert!(matches!(
            err,
            PrimitivesError::WrongLength(WrongLength {
                expected: 32,
                got: 33
            })
        ));
    }

    #[test]
    fn display_matches_b256_lowercase_hex() {
        let addr = OverlayAddress::new([0xab; 32]);
        let rendered = format!("{addr}");
        assert!(rendered.starts_with("0x"));
        assert_eq!(rendered.len(), 66);
        assert!(rendered.chars().skip(2).all(|c| c.is_ascii_hexdigit()));
    }
}
