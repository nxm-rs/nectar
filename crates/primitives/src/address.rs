//! Swarm address implementation
//!
//! This module provides the [`SwarmAddress`] type, a 32-byte identifier used
//! for addressing nodes in the swarm network. The XOR-metric operations
//! (distance, proximity) shared with the other address kinds live on the
//! [`XorMetric`](crate::XorMetric) trait.

use std::fmt;

use alloy_primitives::B256;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::error::{Result, WrongLength};
use crate::xor_metric::XorMetric;

/// A 256-bit address in the Swarm network
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct SwarmAddress(B256);

impl SwarmAddress {
    /// Width in bytes of an address.
    pub const SIZE: usize = size_of::<B256>();

    /// Creates an address with only the first byte set, rest zeros.
    ///
    /// The first byte controls proximity order (leading bits determine PO).
    pub const fn with_first_byte(byte: u8) -> Self {
        let mut bytes = [0u8; 32];
        bytes[0] = byte;
        Self(B256::new(bytes))
    }

    /// Creates a new SwarmAddress from raw bytes
    pub fn new(bytes: [u8; 32]) -> Self {
        Self(B256::from(bytes))
    }

    /// Returns the underlying bytes
    pub const fn as_bytes(&self) -> &[u8] {
        self.0.as_slice()
    }

    /// Creates a new address from a slice, checking the length.
    ///
    /// The error carries expected and actual lengths via [`WrongLength`].
    pub fn from_slice(slice: &[u8]) -> Result<Self> {
        Ok(Self::try_from(slice)?)
    }

    /// Checks if this address is zeros
    pub fn is_zero(&self) -> bool {
        self.0.is_zero()
    }

    /// Create a new zero-filled address
    pub const fn zero() -> Self {
        Self(B256::ZERO)
    }
}

impl XorMetric for SwarmAddress {
    fn point(&self) -> &[u8; 32] {
        &self.0.0
    }
}

impl Default for SwarmAddress {
    fn default() -> Self {
        Self(B256::ZERO)
    }
}

impl fmt::Display for SwarmAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<B256> for SwarmAddress {
    fn from(value: B256) -> Self {
        Self(value)
    }
}

impl From<[u8; 32]> for SwarmAddress {
    fn from(bytes: [u8; 32]) -> Self {
        Self::new(bytes)
    }
}

impl From<SwarmAddress> for B256 {
    fn from(addr: SwarmAddress) -> Self {
        addr.0
    }
}

impl AsRef<[u8]> for SwarmAddress {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl TryFrom<&[u8]> for SwarmAddress {
    type Error = WrongLength;

    fn try_from(slice: &[u8]) -> std::result::Result<Self, Self::Error> {
        let bytes: [u8; 32] = slice.try_into().map_err(|_| WrongLength {
            expected: 32,
            got: slice.len(),
        })?;
        Ok(Self::new(bytes))
    }
}

impl From<SwarmAddress> for [u8; 32] {
    fn from(addr: SwarmAddress) -> Self {
        addr.0.into()
    }
}

#[cfg(any(test, feature = "arbitrary"))]
impl<'a> arbitrary::Arbitrary<'a> for SwarmAddress {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(Self(B256::arbitrary(u)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::PrimitivesError;

    #[test]
    fn try_from_slice_valid() {
        let bytes = [0x5au8; 32];
        assert_eq!(
            SwarmAddress::try_from(bytes.as_slice()).unwrap(),
            SwarmAddress::new(bytes)
        );
    }

    #[test]
    fn try_from_slice_wrong_length() {
        let short = [0u8; 31];
        assert_eq!(
            SwarmAddress::try_from(short.as_slice()).unwrap_err(),
            WrongLength {
                expected: 32,
                got: 31
            }
        );
    }

    #[test]
    fn from_slice_carries_lengths() {
        let long = [0u8; 33];
        let err = SwarmAddress::from_slice(&long).unwrap_err();
        assert!(matches!(
            err,
            PrimitivesError::WrongLength(WrongLength {
                expected: 32,
                got: 33
            })
        ));
    }
}
