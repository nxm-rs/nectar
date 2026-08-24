//! XOR-metric operations over 32-byte address-space points.
//!
//! Kademlia routing measures closeness between two 256-bit points as the
//! number of leading matching bits (the proximity order, PO) or as the full
//! XOR distance. The address kinds ([`OverlayAddress`](crate::OverlayAddress),
//! [`ChunkAddress`]) are distinct nominal types over the
//! same point space, and the protocol compares across kinds (a chunk address
//! against a node overlay for the storage radius and pushsync targeting), so
//! the ops live here on a shared trait rather than on any one kind.
//!
//! ## Standard vs extended proximity
//!
//! - **Standard PO** ([`MAX_PO`] = 31): most routing operations; 32 bins.
//! - **Extended PO** ([`SwarmSpec::extended_proximity_order`]): Kademlia bin
//!   balancing, which checks `po + bit_suffix_length + 1`, so bin 31 needs
//!   31 + 4 + 1 = 36 on the canonical specs.
//!
//! Matches the Swarm reference implementation: leading matching bits (not
//! bytes), capped at the respective maximum.
//!
//! ## Example
//!
//! ```
//! use nectar_primitives::{OverlayAddress, XorMetric};
//! use alloy_primitives::B256;
//!
//! let addr1 = OverlayAddress::from(B256::random());
//! let addr2 = OverlayAddress::from(B256::random());
//!
//! let po = addr1.proximity(&addr2);
//! let distance = addr1.distance(&addr2);
//!
//! let addr3 = OverlayAddress::from(B256::random());
//! if addr1.closer(&addr2, &addr3) {
//!     println!("addr1 is closer to addr2 than addr3");
//! }
//! ```

use core::cmp::Ordering;

use alloy_primitives::U256;

use crate::{Bin, ChunkAddress, ProximityOrder, SwarmSpec};

/// Maximum proximity order for standard routing operations.
///
/// Value 31 gives 32 Kademlia bins (0-31). The protocol ceiling that
/// [`ProximityOrder`] and [`Bin`] validate against; spec methods narrow
/// below it per network.
pub const MAX_PO: u8 = 31;

/// XOR-metric operations over a 32-byte point.
///
/// Every method takes `&impl XorMetric`, so proximity and distance are legal
/// across address kinds; the kinds stay nominally distinct everywhere else.
pub trait XorMetric {
    /// The 32-byte point this value occupies in the XOR metric space.
    fn point(&self) -> &[u8; 32];

    /// Calculate the distance between `self` and `y` in big-endian.
    #[inline(always)]
    #[must_use]
    fn distance(&self, y: &impl XorMetric) -> U256 {
        U256::from_be_bytes(*self.point()) ^ U256::from_be_bytes(*y.point())
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
    /// # use nectar_primitives::{OverlayAddress, XorMetric};
    /// # use alloy_primitives::B256;
    /// let target = OverlayAddress::zero();
    /// let addresses = vec![
    ///     OverlayAddress::from(B256::repeat_byte(0x01)),
    ///     OverlayAddress::from(B256::repeat_byte(0x02)),
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
    fn distance_cmp(&self, x: &impl XorMetric, y: &impl XorMetric) -> Ordering {
        let self_point = U256::from_be_bytes(*self.point());
        let to_x = self_point ^ U256::from_be_bytes(*x.point());
        let to_y = self_point ^ U256::from_be_bytes(*y.point());
        // MSB-first byte order is plain big-endian integer order, so the scan
        // the byte version performed is an integer compare of the two xors.
        to_x.cmp(&to_y).reverse()
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
        // `proximity_up_to` caps at MAX_PO, so the result is a valid `ProximityOrder`.
        ProximityOrder::new_unchecked(proximity_up_to(self.point(), other.point(), MAX_PO))
    }

    /// Calculate the extended proximity order between `self` and another point.
    ///
    /// Returns the number of leading bits that match between the two points,
    /// capped at `spec.extended_proximity_order()`. Use this for Kademlia bin
    /// balancing, which needs finer granularity than standard routing.
    ///
    /// Returns a raw `u8` because the extended range exceeds `ProximityOrder`'s
    /// invariant (`0..=MAX_PO`). For standard routing, use
    /// [`proximity()`](Self::proximity) instead.
    #[inline(always)]
    #[must_use]
    fn extended_proximity(&self, other: &impl XorMetric, spec: &impl SwarmSpec) -> u8 {
        proximity_up_to(self.point(), other.point(), spec.extended_proximity_order())
    }

    /// XOR distance - bitwise XOR of the two 32-byte points as a new value of
    /// the receiver's kind. Useful when callers want the raw distance bytes
    /// (e.g. for content-routing bias) rather than the proximity-order metric.
    #[inline(always)]
    #[must_use]
    fn xor(&self, other: &impl XorMetric) -> Self
    where
        Self: Sized + From<[u8; 32]>,
    {
        Self::from(self.distance(other).to_be_bytes())
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

/// The content-address kind occupies the same point space as the node-identity
/// kind; the impl lives here rather than beside the type because the metric is
/// routing, not verification.
impl XorMetric for ChunkAddress {
    fn point(&self) -> &[u8; 32] {
        self.as_array()
    }
}

/// Count of leading matching bits between two points, capped at `max`.
#[inline(always)]
fn proximity_up_to(bytes1: &[u8; 32], bytes2: &[u8; 32], max: u8) -> u8 {
    let xor = U256::from_be_bytes(*bytes1) ^ U256::from_be_bytes(*bytes2);
    // 256 leading zeros means the xor is zero and every bit matches.
    u8::try_from(xor.leading_zeros()).map_or(max, |matching| matching.min(max))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ChunkAddress, Mainnet, NetworkId, OverlayAddress};
    use alloy_primitives::B256;

    #[test]
    fn proximity_counts_leading_matching_bits() {
        let base = OverlayAddress::with_first_byte(0b0000_0000);
        let one_bit = OverlayAddress::with_first_byte(0b0100_0000);
        assert_eq!(base.proximity(&one_bit).get(), 1);

        let full_match = OverlayAddress::zero();
        assert_eq!(base.proximity(&full_match).get(), MAX_PO);
    }

    #[test]
    fn extended_proximity_exceeds_standard_cap() {
        let base = OverlayAddress::zero();
        assert_eq!(base.extended_proximity(&base, &Mainnet), 36);
        assert_eq!(
            base.extended_proximity(&base, &Mainnet),
            Mainnet.extended_proximity_order()
        );
        assert_eq!(base.proximity(&base).get(), MAX_PO);
    }

    #[test]
    fn extended_proximity_resolves_and_caps_past_standard() {
        // Match through byte 3, differ at bit 34 (byte 4): standard proximity
        // saturates at MAX_PO, the extended value resolves the finer 34.
        let base = OverlayAddress::zero();
        let mut bytes = [0u8; 32];
        bytes[4] = 0b0010_0000;
        let deeper = OverlayAddress::from(B256::from(bytes));
        assert_eq!(base.proximity(&deeper).get(), MAX_PO);
        assert_eq!(base.extended_proximity(&deeper, &Mainnet), 34);

        // Differ only at bit 39: the raw count (39) exceeds the cap, so it
        // clamps to the canonical 36.
        let mut past_cap = [0u8; 32];
        past_cap[4] = 0b0000_0001;
        let past_cap = OverlayAddress::from(B256::from(past_cap));
        assert_eq!(base.extended_proximity(&past_cap, &Mainnet), 36);

        // A narrowed spec caps the extended value below the canonical 36.
        struct Shallow;
        impl SwarmSpec for Shallow {
            const NETWORK_ID: NetworkId = NetworkId::TESTNET;
            const MAX_PROXIMITY_ORDER: ProximityOrder = ProximityOrder::new_unchecked(7);
        }
        assert_eq!(base.extended_proximity(&base, &Shallow), 12);
    }

    #[test]
    fn distance_is_symmetric_xor() {
        let a = OverlayAddress::from(B256::repeat_byte(0x0f));
        let b = OverlayAddress::from(B256::repeat_byte(0xf0));
        assert_eq!(a.distance(&b), b.distance(&a));
        assert_eq!(
            a.distance(&b),
            U256::from_be_bytes([0xffu8; 32]),
            "0x0f ^ 0xf0 = 0xff in every byte"
        );
    }

    #[test]
    fn distance_cmp_orders_by_closeness() {
        let target = OverlayAddress::zero();
        let near = OverlayAddress::from(B256::repeat_byte(0x01));
        let far = OverlayAddress::from(B256::repeat_byte(0x02));
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
        let overlay = OverlayAddress::from(B256::repeat_byte(0xaa));
        assert_eq!(chunk.proximity(&overlay).get(), MAX_PO);
        assert_eq!(chunk.distance(&overlay), U256::ZERO);
        assert_eq!(overlay.bin(&chunk), Bin::from(overlay.proximity(&chunk)));
    }

    #[test]
    fn xor_returns_receiver_kind() {
        let a = ChunkAddress::from(B256::repeat_byte(0x0f));
        let b = OverlayAddress::from(B256::repeat_byte(0xf0));
        let d: ChunkAddress = a.xor(&b);
        assert_eq!(d, ChunkAddress::from(B256::repeat_byte(0xff)));
    }
}
