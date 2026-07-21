//! Typed content address for chunks.
//!
//! A [`ChunkAddress`] names a chunk by the hash the network validates it
//! against: the BMT root for content chunks, `keccak256(id || owner)` for
//! single-owner chunks. It is nominally distinct from the node-identity
//! address kind; cross-kind proximity goes through
//! [`XorMetric`](crate::XorMetric).

use alloy_primitives::B256;
use derive_more::{AsRef, Display, From, Into};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::bmt::DerivedAddress;
use crate::error::{Result, WrongLength};
use crate::xor_metric::XorMetric;

/// 32-byte content address of a chunk.
///
/// Transparent over the same 32 wire bytes as the alias it replaces: every
/// reference, manifest slot and store key serializes identically.
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
pub struct ChunkAddress(B256);

impl ChunkAddress {
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

/// Adopt a hasher-derived BMT root as an address; the conversion is one-way.
impl From<DerivedAddress> for ChunkAddress {
    fn from(derived: DerivedAddress) -> Self {
        Self(derived.into())
    }
}

impl XorMetric for ChunkAddress {
    fn point(&self) -> &[u8; 32] {
        &self.0.0
    }
}

impl TryFrom<&[u8]> for ChunkAddress {
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
impl<'a> arbitrary::Arbitrary<'a> for ChunkAddress {
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
        assert_eq!(ChunkAddress::ZERO.as_bytes(), &[0u8; 32]);
        assert!(ChunkAddress::zero().is_zero());
    }

    #[test]
    fn roundtrips_via_from_impls() {
        let bytes = [7u8; 32];
        let addr = ChunkAddress::new(bytes);
        assert_eq!(B256::from(addr), B256::new(bytes));
        assert_eq!(ChunkAddress::from(B256::new(bytes)), addr);
        assert_eq!(<[u8; 32]>::from(addr), bytes);
        assert_eq!(ChunkAddress::from(bytes), addr);
    }

    #[test]
    fn try_from_slice_wrong_length() {
        let short = [0u8; 31];
        assert_eq!(
            ChunkAddress::try_from(short.as_slice()).unwrap_err(),
            WrongLength {
                expected: 32,
                got: 31
            }
        );
    }

    #[test]
    fn from_slice_carries_lengths() {
        let long = [0u8; 33];
        let err = ChunkAddress::from_slice(&long).unwrap_err();
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
        let addr = ChunkAddress::new([0xab; 32]);
        let rendered = format!("{addr}");
        assert!(rendered.starts_with("0x"));
        assert_eq!(rendered.len(), 66);
        assert!(rendered.chars().skip(2).all(|c| c.is_ascii_hexdigit()));
    }
}
