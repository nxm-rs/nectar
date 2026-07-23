//! Canonical Swarm network spec - `network_id`, kademlia tuning, defaults.
//!
//! Every Swarm node implementation needs a small set of canonical knobs:
//! the network ID, the kademlia saturation thresholds, the bootnode-mode
//! over-saturation cap, the neighborhood-depth low-watermark, the clock-skew
//! tolerance used during handshake. Bee hard-codes these in
//! `pkg/topology/kademlia/kademlia.go:54-56` and `pkg/bzz/timestamp.go`.
//!
//! This trait surfaces them on the spec object so vertex / apiarist /
//! reth-swarm derive them from one place instead of duplicating consts.

use std::time::Duration;

use crate::{Bin, NetworkId, ProximityOrder};

/// Canonical Swarm network spec.
///
/// Default-method bodies mirror bee's hard-coded constants. Implementors only
/// have to provide `network_id`; they may override any of the others to
/// customise a deployment (e.g. a dense testnet with tighter saturation).
///
/// Trait can grow with default methods without breaking implementors -
/// `#[non_exhaustive]` is not a valid attribute on traits in Rust, but the
/// default-method discipline gives equivalent backward-compatibility.
pub trait SwarmSpec {
    /// Network identifier used in [`compute_overlay`](crate::compute_overlay)
    /// and the BzzAddress sign-data.
    fn network_id(&self) -> NetworkId;

    /// Maximum proximity order (= number of bins minus one).
    fn max_proximity_order(&self) -> ProximityOrder {
        ProximityOrder::MAX
    }

    /// Minimum desired peers per bin before the bin is considered saturated.
    /// Bee default: 8 (`defaultSaturationPeers`).
    fn saturation_peers(&self) -> u8 {
        8
    }

    /// Soft cap: above this, non-bootnode peers reject further inbound dials.
    /// Bee default: 18 (`defaultOverSaturationPeers`).
    fn over_saturation_peers(&self) -> u8 {
        18
    }

    /// Soft cap for **bootnode** mode (higher than regular).
    /// Bee default: 20 (`defaultBootNodeOverSaturationPeers`).
    fn bootnode_over_saturation_peers(&self) -> u8 {
        20
    }

    /// Minimum peers required in the deepest bins to maintain neighborhood
    /// depth (bee default: 2 - `nnLowWatermark`).
    fn neighborhood_low_watermark(&self) -> u8 {
        2
    }

    /// Maximum clock skew permitted between local and remote timestamps
    /// during handshake / hive verification. Bee's `bzz/timestamp.go`
    /// hard-codes 5s but operational deployments commonly relax to minutes
    /// or hours; this default is 6h to match what was previously embedded
    /// in vertex.
    fn clock_skew_tolerance(&self) -> Duration {
        Duration::from_secs(6 * 60 * 60)
    }

    /// Minimum collision-bucket depth a postage batch may declare, mirroring
    /// the PostageStamp contract's `minimumBucketDepth()` (16 on mainnet).
    /// A floor rather than a fixed width: a batch may declare a deeper one.
    fn min_bucket_depth(&self) -> u8 {
        16
    }

    /// Convenience: the routing-table bin count (`max_proximity_order() + 1`).
    #[allow(clippy::arithmetic_side_effects)] // max_proximity_order() <= MAX_PO (31), so + 1 cannot overflow usize
    fn bin_count(&self) -> usize {
        usize::from(self.max_proximity_order().get()) + 1
    }

    /// Convenience: the deepest bin, derived from `max_proximity_order`.
    fn max_bin(&self) -> Bin {
        // PO is range-validated; the conversion is total.
        Bin::from(self.max_proximity_order())
    }
}

/// Concrete static spec used when callers just need to plug a `network_id`
/// into the canonical defaults.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StaticSpec {
    network_id: NetworkId,
}

impl StaticSpec {
    /// Construct a spec for `network_id` with all default knobs.
    pub const fn new(network_id: NetworkId) -> Self {
        Self { network_id }
    }
}

impl SwarmSpec for StaticSpec {
    fn network_id(&self) -> NetworkId {
        self.network_id
    }
}

/// Canonical mainnet spec ([`NetworkId::MAINNET`]).
pub const MAINNET: StaticSpec = StaticSpec::new(NetworkId::MAINNET);

/// Canonical testnet (Sepolia) spec ([`NetworkId::TESTNET`]).
pub const TESTNET: StaticSpec = StaticSpec::new(NetworkId::TESTNET);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_bee() {
        let s = MAINNET;
        assert_eq!(s.network_id(), NetworkId::MAINNET);
        assert_eq!(s.max_proximity_order(), ProximityOrder::MAX);
        assert_eq!(s.saturation_peers(), 8);
        assert_eq!(s.over_saturation_peers(), 18);
        assert_eq!(s.bootnode_over_saturation_peers(), 20);
        assert_eq!(s.neighborhood_low_watermark(), 2);
        assert_eq!(s.clock_skew_tolerance(), Duration::from_secs(21600));
        assert_eq!(s.min_bucket_depth(), 16);
        assert_eq!(s.bin_count(), 32);
        assert_eq!(s.max_bin(), Bin::MAX);
    }

    #[test]
    fn testnet_distinct_from_mainnet() {
        assert_ne!(MAINNET.network_id(), TESTNET.network_id());
    }

    #[test]
    fn override_saturation_via_custom_impl() {
        struct Tight;
        impl SwarmSpec for Tight {
            fn network_id(&self) -> NetworkId {
                NetworkId::TESTNET
            }
            fn saturation_peers(&self) -> u8 {
                4
            }
        }
        assert_eq!(Tight.saturation_peers(), 4);
        assert_eq!(Tight.over_saturation_peers(), 18); // default unchanged
    }
}
