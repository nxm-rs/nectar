//! Typed Swarm network identifier.
//!
//! [`NetworkId`] is mixed into the overlay address (see [`compute_overlay`])
//! so that a single keypair derives a different overlay on each network.
//! This is the Swarm-wide partitioning mechanism inherited from bee
//! (see `pkg/crypto/crypto.go:45-57` for the derivation, and
//! `pkg/swarm/swarm.go` for canonical IDs).
//!
//! [`compute_overlay`]: crate::compute_overlay

use derive_more::{Display, From, Into};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Swarm network identifier (u64 wire-compatible with bee).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord,
    Display, From, Into,
)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
#[display("{_0}")]
pub struct NetworkId(u64);

impl NetworkId {
    /// Canonical Swarm mainnet identifier.
    pub const MAINNET: Self = Self(1);

    /// Canonical Swarm testnet identifier (Sepolia).
    pub const TESTNET: Self = Self(10);

    /// Construct from a raw `u64`.
    #[inline]
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    /// Underlying numeric value.
    #[inline]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Eight-byte little-endian representation (used in
    /// [`compute_overlay`](crate::compute_overlay) per bee).
    #[inline]
    pub const fn to_le_bytes(self) -> [u8; 8] {
        self.0.to_le_bytes()
    }

    /// Eight-byte big-endian representation (used in the BzzAddress sign-data
    /// per bee `pkg/bzz/address.go:138-160`).
    #[inline]
    pub const fn to_be_bytes(self) -> [u8; 8] {
        self.0.to_be_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_ids() {
        assert_eq!(NetworkId::MAINNET.get(), 1);
        assert_eq!(NetworkId::TESTNET.get(), 10);
    }

    #[test]
    fn display_is_decimal() {
        assert_eq!(format!("{}", NetworkId::new(42)), "42");
    }

    #[test]
    fn le_be_byte_distinction() {
        let id = NetworkId::new(0x0102_0304_0506_0708);
        assert_eq!(id.to_le_bytes(), [0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01]);
        assert_eq!(id.to_be_bytes(), [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]);
    }
}
