//! Swarm address implementation
//!
//! This module provides the SwarmAddress type, which is a 32-byte identifier
//! used for addressing chunks in the swarm network. It includes functionality
//! for calculating distances between addresses and determining their proximity.
//!
//! ## Proximity Order (PO)
//!
//! Proximity order is a key concept in Kademlia-based routing. It measures
//! how "close" two addresses are in the XOR metric space by counting the
//! number of leading matching bits.
//!
//! ### Standard vs Extended Proximity
//!
//! - **Standard PO** (`MAX_PO = 31`): Used for most routing operations.
//!   Returns a value 0-31, giving 32 Kademlia bins. The algorithm counts
//!   leading matching bits up to 32 bits (4 bytes) and caps at 31.
//!
//! - **Extended PO** (`EXTENDED_PO = 36`): Used for Kademlia bin balancing.
//!   When balancing bins, the algorithm needs finer granularity:
//!   `po + BitSuffixLength + 1` where `BitSuffixLength = 4` (default in Bee).
//!   For bin 31, this yields 31 + 4 + 1 = 36, hence `ExtendedPO = MaxPO + 5`.
//!
//! ### Compatibility with Bee
//!
//! This implementation matches Bee's `pkg/swarm/proximity.go` exactly:
//!
//! - `MaxPO = 31` and `ExtendedPO = 36` are identical to Bee
//! - Both count leading matching BITS (not bytes)
//! - Both cap at their respective maximum values
//!
//! The Go Bee implementation iterates bit-by-bit using `(oxo>>(7-j))&0x01`,
//! returning `i*8 + j` (byte index * 8 + bit index). Our implementation
//! uses `leading_zeros()` which is equivalent but more efficient.
//!
//! ### Distance vs Proximity
//!
//! - **Distance** (`distance()`): Returns the full 256-bit XOR distance as `U256`.
//! - **Proximity** (`proximity()`): Returns a small integer (0-31) representing
//!   the count of leading matching bits, capped at `MAX_PO`.
//!
//! Higher proximity = closer addresses = smaller XOR distance.
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
//!
//! [`extended_proximity()`]: SwarmAddress::extended_proximity

use std::cmp::Ordering;
use std::fmt;
use std::ops::Deref;

use alloy_primitives::{B256, U256};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::error::Result;

/// Maximum proximity order for standard routing operations.
///
/// Value 31 gives 32 Kademlia bins (0-31). Matches Bee's `MaxPO`.
pub const MAX_PO: u8 = 31;

/// Extended proximity order for Kademlia bin balancing.
///
/// Value 36 = MaxPO (31) + BitSuffixLength (4) + 1. Used when the Kademlia
/// bin balancing algorithm needs to check proximity at finer granularity
/// than standard routing. Matches Bee's `ExtendedPO`.
pub const EXTENDED_PO: u8 = MAX_PO + 5;

/// A 256-bit address for a chunk in the Swarm network
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
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
    #[must_use]
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

    /// Compares addresses `x` and `y` by their distance from `self`.
    ///
    /// Returns:
    /// - `Ordering::Less` if `x` is farther from `self` than `y` (i.e., `y` is closer)
    /// - `Ordering::Greater` if `x` is closer to `self` than `y`
    /// - `Ordering::Equal` if `x` and `y` are equidistant from `self`
    ///
    /// # Usage with `min_by`
    ///
    /// This comparator is designed for use with `Iterator::min_by` to find
    /// the address closest to `self`:
    ///
    /// ```
    /// # use nectar_primitives::SwarmAddress;
    /// # use alloy_primitives::B256;
    /// let target = SwarmAddress::zero();
    /// let addresses = vec![
    ///     SwarmAddress::from(B256::repeat_byte(0x01)),
    ///     SwarmAddress::from(B256::repeat_byte(0x02)),
    /// ];
    /// let closest = addresses.iter().min_by(|a, b| target.distance_cmp(a, b));
    /// ```
    ///
    /// Note: The ordering may seem inverted from intuition. `Greater` means `x`
    /// is closer (smaller distance), because `min_by` selects the element for
    /// which the comparator returns `Less` - and we want to select the one
    /// that is NOT closer (i.e., has a larger distance), leaving the closest.
    #[inline(always)]
    #[must_use]
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

    /// Determine if `self` is closer to `x` than to `y`.
    ///
    /// Returns `true` if `distance(self, x) < distance(self, y)`.
    #[must_use]
    pub fn closer(&self, x: &Self, y: &Self) -> bool {
        // distance_cmp returns Greater when x is closer to self
        self.distance_cmp(x, y) == Ordering::Greater
    }

    /// Check if this address is within the given proximity to another address
    pub fn is_within_proximity(&self, other: &Self, min_proximity: u8) -> bool {
        self.proximity(other) >= min_proximity
    }

    /// Calculate the proximity order between self and another address.
    ///
    /// Returns the number of leading bits that match between the two addresses,
    /// capped at `MAX_PO` (31). Use this for standard Kademlia routing operations.
    ///
    /// For operations requiring finer granularity (like reserve sampling),
    /// use [`extended_proximity()`](Self::extended_proximity) instead.
    #[inline(always)]
    #[must_use]
    pub fn proximity(&self, other: &Self) -> u8 {
        self.proximity_helper(other, MAX_PO.into())
    }

    /// Calculate the extended proximity order between self and another address.
    ///
    /// Returns the number of leading bits that match between the two addresses,
    /// capped at `EXTENDED_PO` (36). Use this for Kademlia bin balancing where
    /// the algorithm checks `po + BitSuffixLength + 1` (up to 36 for bin 31).
    ///
    /// For standard routing operations, use [`proximity()`](Self::proximity) instead.
    #[inline(always)]
    #[must_use]
    pub fn extended_proximity(&self, other: &Self) -> u8 {
        self.proximity_helper(other, EXTENDED_PO.into())
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
        write!(f, "{}", self.0)
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

impl From<SwarmAddress> for [u8; 32] {
    fn from(addr: SwarmAddress) -> Self {
        addr.0.into()
    }
}

#[cfg(feature = "arbitrary")]
impl<'a> arbitrary::Arbitrary<'a> for SwarmAddress {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(Self(B256::arbitrary(u)?))
    }
}
