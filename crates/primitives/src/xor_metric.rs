//! XOR-metric operations over 32-byte address-space points.
//!
//! Kademlia routing measures closeness between two 256-bit points as the
//! number of leading matching bits (the proximity order, PO) or as the full
//! XOR distance. The address kinds ([`SwarmAddress`](crate::SwarmAddress),
//! [`ChunkAddress`](crate::ChunkAddress)) are distinct nominal types over the
//! same point space, and the protocol compares across kinds (a chunk address
//! against a node overlay for the storage radius and pushsync targeting), so
//! the ops live here on a shared trait rather than on any one kind.
//!
//! ## Standard vs extended proximity
//!
//! - **Standard PO** ([`MAX_PO`] = 31): most routing operations; 32 bins.
//! - **Extended PO** ([`EXTENDED_PO`] = 36): Kademlia bin balancing, which
//!   checks `po + BitSuffixLength + 1` (`BitSuffixLength = 4`), so bin 31
//!   needs 31 + 4 + 1 = 36.
//!
//! Matches the Swarm reference implementation: leading matching bits (not
//! bytes), capped at the respective maximum.
//!
//! ## Example
//!
//! ```
//! use nectar_primitives::{SwarmAddress, XorMetric};
//! use alloy_primitives::B256;
//!
//! let addr1 = SwarmAddress::from(B256::random());
//! let addr2 = SwarmAddress::from(B256::random());
//!
//! let po = addr1.proximity(&addr2);
//! let distance = addr1.distance(&addr2);
//!
//! let addr3 = SwarmAddress::from(B256::random());
//! if addr1.closer(&addr2, &addr3) {
//!     println!("addr1 is closer to addr2 than addr3");
//! }
//! ```

use std::cmp::Ordering;

use alloy_primitives::U256;

use crate::{Bin, ProximityOrder};

/// Maximum proximity order for standard routing operations.
///
/// Value 31 gives 32 Kademlia bins (0-31).
pub const MAX_PO: u8 = 31;

/// Extended proximity order for Kademlia bin balancing.
///
/// Value 36 = MaxPO (31) + BitSuffixLength (4) + 1. Used when the Kademlia
/// bin balancing algorithm needs to check proximity at finer granularity
/// than standard routing.
pub const EXTENDED_PO: u8 = MAX_PO + 5;

/// XOR-metric operations over a 32-byte point.
///
/// Every method takes `&impl XorMetric`, so proximity and distance are legal
/// across address kinds; the kinds stay nominally distinct everywhere else.
pub trait XorMetric {
    /// The 32-byte point this value occupies in the XOR metric space.
    fn point(&self) -> &[u8; 32];

    /// Calculate the distance between `self` and `y` in big-endian.
    #[allow(clippy::indexing_slicing)] // i < 32 from enumerating a 32-byte point, matching result's length
    #[inline(always)]
    #[must_use]
    fn distance(&self, y: &impl XorMetric) -> U256 {
        let mut result = [0u8; 32];

        for (i, (&a, &b)) in self.point().iter().zip(y.point().iter()).enumerate() {
            result[i] = a ^ b;
        }

        U256::from_be_bytes(result)
    }

    /// Compares points `x` and `y` by their distance from `self`.
    ///
    /// Returns:
    /// - `Ordering::Less` if `x` is farther from `self` than `y` (i.e., `y` is closer)
    /// - `Ordering::Greater` if `x` is closer to `self` than `y`
    /// - `Ordering::Equal` if `x` and `y` are equidistant from `self`
    ///
    /// # Usage with `min_by`
    ///
    /// This comparator is designed for use with `Iterator::min_by` to find
    /// the point closest to `self`:
    ///
    /// ```
    /// # use nectar_primitives::{SwarmAddress, XorMetric};
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
    #[allow(clippy::indexing_slicing)] // ab, xb and yb are all 32-byte points and i < ab.len()
    #[inline(always)]
    #[must_use]
    fn distance_cmp(&self, x: &impl XorMetric, y: &impl XorMetric) -> Ordering {
        let (ab, xb, yb) = (self.point(), x.point(), y.point());

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
    fn closer(&self, x: &impl XorMetric, y: &impl XorMetric) -> bool {
        // distance_cmp returns Greater when x is closer to self
        self.distance_cmp(x, y) == Ordering::Greater
    }

    /// Check if this point is within the given proximity to another point.
    fn is_within_proximity(&self, other: &impl XorMetric, min_proximity: ProximityOrder) -> bool {
        self.proximity(other) >= min_proximity
    }

    /// Calculate the proximity order between `self` and another point.
    ///
    /// Returns the number of leading bits that match between the two points,
    /// capped at [`MAX_PO`] (31). Use this for standard Kademlia routing
    /// operations.
    ///
    /// For operations requiring finer granularity (like reserve sampling),
    /// use [`extended_proximity()`](Self::extended_proximity) instead.
    #[inline(always)]
    #[must_use]
    fn proximity(&self, other: &impl XorMetric) -> ProximityOrder {
        // `proximity_up_to` is bounded by MAX_PO, so the cast is sound.
        ProximityOrder::new_unchecked(proximity_up_to(self.point(), other.point(), MAX_PO.into()))
    }

    /// Calculate the extended proximity order between `self` and another point.
    ///
    /// Returns the number of leading bits that match between the two points,
    /// capped at [`EXTENDED_PO`] (36). Use this for Kademlia bin balancing
    /// where the algorithm checks `po + BitSuffixLength + 1` (up to 36 for
    /// bin 31).
    ///
    /// Returns a raw `u8` because the extended range exceeds `ProximityOrder`'s
    /// invariant (`0..=MAX_PO`). For standard routing, use
    /// [`proximity()`](Self::proximity) instead.
    #[inline(always)]
    #[must_use]
    fn extended_proximity(&self, other: &impl XorMetric) -> u8 {
        proximity_up_to(self.point(), other.point(), EXTENDED_PO.into())
    }

    /// XOR distance - bitwise XOR of the two 32-byte points as a new value of
    /// the receiver's kind. Useful when callers want the raw distance bytes
    /// (e.g. for content-routing bias) rather than the proximity-order metric.
    #[allow(clippy::indexing_slicing)] // i < 32 from enumerating a 32-byte point, matching out's length
    #[inline(always)]
    #[must_use]
    fn xor(&self, other: &impl XorMetric) -> Self
    where
        Self: Sized + From<[u8; 32]>,
    {
        let mut out = [0u8; 32];
        for (i, (a, b)) in self.point().iter().zip(other.point()).enumerate() {
            out[i] = a ^ b;
        }
        Self::from(out)
    }

    /// Kademlia bin index of `self` relative to `anchor` - semantic alias for
    /// `Bin::from(self.proximity(anchor))`. The routing-table convention is
    /// "the bin a peer occupies is its proximity order to our own overlay".
    #[inline(always)]
    #[must_use]
    fn bin(&self, anchor: &impl XorMetric) -> Bin {
        Bin::from(self.proximity(anchor))
    }
}

// References are points too, so iterator adaptors (`min_by` over `&T` items)
// pass without an explicit deref.
impl<T: XorMetric> XorMetric for &T {
    fn point(&self) -> &[u8; 32] {
        (**self).point()
    }
}

/// Count of leading matching bits between two points, capped at `max`.
#[allow(
    clippy::arithmetic_side_effects,
    clippy::indexing_slicing,
    clippy::as_conversions
)]
// max is MAX_PO (31) or EXTENDED_PO (36), so i <= max_bytes = 4 < 32 and i * 8 + leading_zeros <= 40 fits u8; the casts to u8 (max, i, and the u8 xor's leading_zeros <= 8) are all within those bounds
#[inline(always)]
fn proximity_up_to(bytes1: &[u8; 32], bytes2: &[u8; 32], max: usize) -> u8 {
    let max_bytes = max / 8;
    let max_bits = max as u8;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ChunkAddress, SwarmAddress};
    use alloy_primitives::B256;

    #[test]
    fn proximity_counts_leading_matching_bits() {
        let base = SwarmAddress::with_first_byte(0b0000_0000);
        let one_bit = SwarmAddress::with_first_byte(0b0100_0000);
        assert_eq!(base.proximity(&one_bit).get(), 1);

        let full_match = SwarmAddress::zero();
        assert_eq!(base.proximity(&full_match).get(), MAX_PO);
    }

    #[test]
    fn extended_proximity_exceeds_standard_cap() {
        let base = SwarmAddress::zero();
        assert_eq!(base.extended_proximity(&base), EXTENDED_PO);
        assert_eq!(base.proximity(&base).get(), MAX_PO);
    }

    #[test]
    fn distance_is_symmetric_xor() {
        let a = SwarmAddress::from(B256::repeat_byte(0x0f));
        let b = SwarmAddress::from(B256::repeat_byte(0xf0));
        assert_eq!(a.distance(&b), b.distance(&a));
        assert_eq!(
            a.distance(&b),
            U256::from_be_bytes([0xffu8; 32]),
            "0x0f ^ 0xf0 = 0xff in every byte"
        );
    }

    #[test]
    fn distance_cmp_orders_by_closeness() {
        let target = SwarmAddress::zero();
        let near = SwarmAddress::from(B256::repeat_byte(0x01));
        let far = SwarmAddress::from(B256::repeat_byte(0x02));
        assert_eq!(target.distance_cmp(&near, &far), Ordering::Greater);
        assert_eq!(target.distance_cmp(&far, &near), Ordering::Less);
        assert_eq!(target.distance_cmp(&near, &near), Ordering::Equal);
        assert!(target.closer(&near, &far));
    }

    #[test]
    fn cross_kind_proximity_is_legal() {
        // The protocol compares a chunk address against a node overlay
        // (storage radius, pushsync targeting); the trait keeps that legal
        // across the distinct kinds.
        let chunk = ChunkAddress::from(B256::repeat_byte(0xaa));
        let overlay = SwarmAddress::from(B256::repeat_byte(0xaa));
        assert_eq!(chunk.proximity(&overlay).get(), MAX_PO);
        assert_eq!(chunk.distance(&overlay), U256::ZERO);
        assert_eq!(overlay.bin(&chunk), Bin::from(overlay.proximity(&chunk)));
    }

    #[test]
    fn xor_returns_receiver_kind() {
        let a = ChunkAddress::from(B256::repeat_byte(0x0f));
        let b = SwarmAddress::from(B256::repeat_byte(0xf0));
        let d: ChunkAddress = a.xor(&b);
        assert_eq!(d, ChunkAddress::from(B256::repeat_byte(0xff)));
    }
}
