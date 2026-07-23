//! Canonical Swarm network knobs: network id, kademlia tuning, postage floors.
//!
//! Knobs are associated consts, so a network is a type ([`Mainnet`],
//! [`Testnet`]) and a spec-parameterized value rejects an out-of-range one at
//! compile time rather than at a validation call.

use core::time::Duration;

use crate::{Bin, NetworkId, ProximityOrder};

/// Canonical Swarm network spec.
///
/// Only [`NETWORK_ID`](Self::NETWORK_ID) is required; the rest default to the
/// reference client's values and may be overridden per deployment. Defaulted
/// consts can be added without breaking implementors.
pub trait SwarmSpec {
    /// Network identifier used in [`compute_overlay`](crate::compute_overlay)
    /// and the BzzAddress sign-data.
    const NETWORK_ID: NetworkId;

    /// Maximum proximity order (= number of bins minus one).
    const MAX_PROXIMITY_ORDER: ProximityOrder = ProximityOrder::MAX;

    /// Peers per bin before the bin counts as saturated.
    const SATURATION_PEERS: u8 = 8;

    /// Soft cap above which a non-bootnode rejects further inbound dials.
    const OVER_SATURATION_PEERS: u8 = 18;

    /// Soft cap in bootnode mode.
    const BOOTNODE_OVER_SATURATION_PEERS: u8 = 20;

    /// Peers needed in the deepest bins to anchor neighborhood depth.
    const NEIGHBORHOOD_LOW_WATERMARK: u8 = 2;

    /// Clock skew tolerated between local and remote timestamps during
    /// handshake and hive verification. Deployments commonly relax this well
    /// past the reference client's 5s.
    const CLOCK_SKEW_TOLERANCE: Duration = Duration::from_secs(6 * 60 * 60);

    /// Minimum collision-bucket depth a postage batch may declare, from the
    /// PostageStamp contract's `minimumBucketDepth()`. A floor, not a fixed
    /// width.
    const MIN_BUCKET_DEPTH: u8 = 16;

    /// Convenience: the deepest bin, derived from
    /// [`MAX_PROXIMITY_ORDER`](Self::MAX_PROXIMITY_ORDER).
    const MAX_BIN: Bin = Bin::new_unchecked(Self::MAX_PROXIMITY_ORDER.get());

    /// Convenience: the routing-table bin count
    /// (`MAX_PROXIMITY_ORDER` + 1).
    // A PO is range-validated to `MAX_PO` (31), so the saturating step never
    // saturates; it is the const-callable form of the increment.
    const BIN_COUNT: usize = Self::MAX_BIN.as_index().saturating_add(1);
}

/// Canonical mainnet spec ([`NetworkId::MAINNET`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Mainnet;

impl SwarmSpec for Mainnet {
    const NETWORK_ID: NetworkId = NetworkId::MAINNET;
}

/// Canonical testnet (Sepolia) spec ([`NetworkId::TESTNET`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Testnet;

impl SwarmSpec for Testnet {
    const NETWORK_ID: NetworkId = NetworkId::TESTNET;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_the_reference_client() {
        assert_eq!(Mainnet::NETWORK_ID, NetworkId::MAINNET);
        assert_eq!(Mainnet::MAX_PROXIMITY_ORDER, ProximityOrder::MAX);
        assert_eq!(Mainnet::SATURATION_PEERS, 8);
        assert_eq!(Mainnet::OVER_SATURATION_PEERS, 18);
        assert_eq!(Mainnet::BOOTNODE_OVER_SATURATION_PEERS, 20);
        assert_eq!(Mainnet::NEIGHBORHOOD_LOW_WATERMARK, 2);
        assert_eq!(Mainnet::CLOCK_SKEW_TOLERANCE, Duration::from_secs(21600));
        assert_eq!(Mainnet::MIN_BUCKET_DEPTH, 16);
        assert_eq!(Mainnet::BIN_COUNT, 32);
        assert_eq!(Mainnet::MAX_BIN, Bin::MAX);
    }

    #[test]
    fn testnet_distinct_from_mainnet() {
        assert_ne!(Mainnet::NETWORK_ID, Testnet::NETWORK_ID);
    }

    #[test]
    fn override_saturation_via_custom_impl() {
        struct Tight;
        impl SwarmSpec for Tight {
            const NETWORK_ID: NetworkId = NetworkId::TESTNET;
            const SATURATION_PEERS: u8 = 4;
        }
        assert_eq!(Tight::SATURATION_PEERS, 4);
        assert_eq!(Tight::OVER_SATURATION_PEERS, 18); // default unchanged
    }

    #[test]
    fn bin_geometry_follows_a_lowered_proximity_order() {
        struct Shallow;
        impl SwarmSpec for Shallow {
            const NETWORK_ID: NetworkId = NetworkId::TESTNET;
            const MAX_PROXIMITY_ORDER: ProximityOrder = ProximityOrder::new_unchecked(7);
        }
        assert_eq!(Shallow::MAX_BIN, Bin::new(7).unwrap());
        assert_eq!(Shallow::BIN_COUNT, 8);
    }
}
