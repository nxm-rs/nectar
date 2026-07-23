//! Canonical Swarm network spec - `NETWORK_ID`, kademlia tuning, defaults.
//!
//! Every Swarm node implementation needs a small set of canonical knobs:
//! the network ID, the kademlia saturation thresholds, the bootnode-mode
//! over-saturation cap, the neighborhood-depth low-watermark, the clock-skew
//! tolerance used during handshake, the postage minimum bucket depth. Bee
//! hard-codes these in `pkg/topology/kademlia/kademlia.go:54-56` and
//! `pkg/bzz/timestamp.go`.
//!
//! The spec is a type, not a value: every knob is an associated const, so a
//! network is named as a type parameter ([`Mainnet`], [`Testnet`]) and a knob
//! is read without an instance in hand. A type parameterized by a spec (a
//! postage bucket depth, say) can then refuse a value the network would refuse
//! at compile time instead of at a validation call.

use core::time::Duration;

use crate::{Bin, NetworkId, ProximityOrder};

/// Canonical Swarm network spec.
///
/// Const defaults mirror bee's hard-coded constants. Implementors only have to
/// give [`NETWORK_ID`](Self::NETWORK_ID); they may override any of the others
/// to customise a deployment (e.g. a dense testnet with tighter saturation).
///
/// Trait can grow with defaulted consts without breaking implementors -
/// `#[non_exhaustive]` is not a valid attribute on traits in Rust, but the
/// default-value discipline gives equivalent backward-compatibility.
pub trait SwarmSpec {
    /// Network identifier used in [`compute_overlay`](crate::compute_overlay)
    /// and the BzzAddress sign-data.
    const NETWORK_ID: NetworkId;

    /// Maximum proximity order (= number of bins minus one).
    const MAX_PROXIMITY_ORDER: ProximityOrder = ProximityOrder::MAX;

    /// Minimum desired peers per bin before the bin is considered saturated.
    /// Bee default: 8 (`defaultSaturationPeers`).
    const SATURATION_PEERS: u8 = 8;

    /// Soft cap: above this, non-bootnode peers reject further inbound dials.
    /// Bee default: 18 (`defaultOverSaturationPeers`).
    const OVER_SATURATION_PEERS: u8 = 18;

    /// Soft cap for **bootnode** mode (higher than regular).
    /// Bee default: 20 (`defaultBootNodeOverSaturationPeers`).
    const BOOTNODE_OVER_SATURATION_PEERS: u8 = 20;

    /// Minimum peers required in the deepest bins to maintain neighborhood
    /// depth (bee default: 2 - `nnLowWatermark`).
    const NEIGHBORHOOD_LOW_WATERMARK: u8 = 2;

    /// Maximum clock skew permitted between local and remote timestamps
    /// during handshake / hive verification. Bee's `bzz/timestamp.go`
    /// hard-codes 5s but operational deployments commonly relax to minutes
    /// or hours; this default is 6h to match what was previously embedded
    /// in vertex.
    const CLOCK_SKEW_TOLERANCE: Duration = Duration::from_secs(6 * 60 * 60);

    /// Minimum collision-bucket depth a postage batch may declare, mirroring
    /// the PostageStamp contract's `minimumBucketDepth()` (16 on mainnet).
    /// A floor rather than a fixed width: a batch may declare a deeper one.
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
    fn defaults_match_bee() {
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
