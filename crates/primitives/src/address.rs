//! Swarm address implementation
//!
//! This module provides the SwarmAddress type, which is a 32-byte identifier
//! used for addressing chunks in the swarm network. It includes functionality
//! for calculating distances between addresses and determining their proximity.
//!
//! ## Example Usage
//!
//! ```
//! use nectar_primitives::SwarmAddress;
//! use alloy_primitives::B256;
//!
//! // Create addresses
//! let addr1 = SwarmAddress::from(B256::random());
//! let addr2 = SwarmAddress::from(B256::random());
//!
//! // Calculate proximity
//! let po = addr1.proximity(&addr2);
//! println!("Proximity order: {}", po);
//!
//! // Calculate distance
//! let distance = addr1.distance(&addr2);
//! println!("Distance: {}", distance);
//!
//! // Compare distances
//! let addr3 = SwarmAddress::from(B256::random());
//! if addr1.closer(&addr2, &addr3) {
//!     println!("addr1 is closer to addr2 than addr3");
//! }
//! ```

use std::cmp::Ordering;
use std::fmt;
use std::ops::Deref;

use alloy_primitives::{B256, U256, hex};

use crate::error::Result;

/// Maximum proximity order (based on 256-bit addresses)
const MAX_PO: usize = 31;
/// Extended proximity order for special operations
const EXTENDED_PO: usize = MAX_PO + 5;

/// A 256-bit address for a chunk in the Swarm network
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SwarmAddress(pub B256);

impl SwarmAddress {
    /// Creates a new SwarmAddress from raw bytes
    pub fn new(bytes: [u8; 32]) -> Self {
        Self(B256::from(bytes))
    }

    /// Returns the underlying bytes
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_slice()
    }

    /// Creates a new address from a slice, checking the length
    pub fn from_slice(slice: &[u8]) -> Result<Self> {
        let address = B256::try_from(slice)?;
        Ok(Self(address))
    }

    /// Checks if this address is zeros
    pub fn is_zero(&self) -> bool {
        self.0.is_zero()
    }

    /// Create a new zero-filled address
    pub fn zero() -> Self {
        Self(B256::ZERO)
    }

    /// Calculate the distance between Self and address `y` in big-endian
    #[inline(always)]
    pub fn distance(&self, y: &Self) -> U256 {
        let mut result = [0u8; 32];

        for (i, (&a, &b)) in self
            .0
            .as_slice()
            .iter()
            .zip(y.0.as_slice().iter())
            .enumerate()
        {
            result[i] = a ^ b;
        }

        U256::from_be_bytes(result)
    }

    /// Compares `x` and `y` to self in terms of the distance metric.
    /// It returns:
    ///   - `Ordering::Greater` if `self` is closer to `x` than `y`
    ///   - `Ordering::Less` if `self` is farther from `x` than `y`
    ///   - `Ordering::Equal` if `self` and `y` are equally close to `x`
    #[inline(always)]
    pub fn distance_cmp(&self, x: &Self, y: &Self) -> Ordering {
        let (ab, xb, yb) = (self.0.as_slice(), x.0.as_slice(), y.0.as_slice());

        for i in 0..ab.len() {
            let dx = xb[i] ^ ab[i];
            let dy = yb[i] ^ ab[i];

            if dx != dy {
                return match dx < dy {
                    true => Ordering::Greater,
                    false => Ordering::Less,
                };
            }
        }

        Ordering::Equal
    }

    /// Determine if self is closer to `a` than `y`
    pub fn closer(&self, x: &Self, y: &Self) -> bool {
        self.distance_cmp(x, y) == Ordering::Less
    }

    /// Check if this address is within the given proximity to another address
    pub fn is_within_proximity(&self, other: &Self, min_proximity: u8) -> bool {
        self.proximity(other) >= min_proximity
    }

    /// Calculate the proximity order between self and another address
    #[inline(always)]
    pub fn proximity(&self, other: &Self) -> u8 {
        self.proximity_helper(other, MAX_PO)
    }

    /// Calculate the extended proximity order between self and another address
    #[inline(always)]
    pub fn extended_proximity(&self, other: &Self) -> u8 {
        self.proximity_helper(other, EXTENDED_PO)
    }

    /// Helper function to calculate proximity with a maximum
    #[inline(always)]
    fn proximity_helper(&self, other: &Self, max: usize) -> u8 {
        let max_bytes = max / 8;
        let max_bits = max as u8;

        let bytes1 = self.0.as_slice();
        let bytes2 = other.0.as_slice();

        for i in 0..=max_bytes {
            let xor = bytes1[i] ^ bytes2[i];
            if xor != 0 {
                // Found a difference - use leading_zeros to count matching bits
                let leading_zeros = xor.leading_zeros() as u8;
                let proximity = (i as u8 * 8) + leading_zeros;

                // Return the smaller of proximity or max_bits
                return if proximity < max_bits {
                    proximity
                } else {
                    max_bits
                };
            }

            // If we're at the last byte we might need to check
            if i == max_bytes {
                return max_bits; // All bits match up to max
            }
        }

        // If we've examined all bytes and found no differences
        max_bits
    }
}

impl Default for SwarmAddress {
    fn default() -> Self {
        Self(B256::ZERO)
    }
}

impl fmt::Display for SwarmAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex::encode(&self.0.as_slice()[..8]))
    }
}

impl Deref for SwarmAddress {
    type Target = B256;

    fn deref(&self) -> &Self::Target {
        &self.0
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
