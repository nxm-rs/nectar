//! Swarm network identity.
//!
//! The [`SwarmSpec`] trait carries the per-network knobs (network id, kademlia
//! tuning, postage floors) as associated consts, and the canonical
//! [`Mainnet`] and [`Testnet`] markers implement it. The identity values a
//! runtime keys on live beside it: [`NetworkId`], the named-swarm table
//! ([`NamedSwarm`]), the runtime wrapper [`Swarm`], and the typed proximity
//! order and bin kinds the spec constants are built from.
//!
//! The overlay derivation and the chunk machinery that consume these values
//! live in `nectar-primitives-core`, which re-exports every item at its
//! original path. The [`SwarmPrimitives`] bundle threads the specification
//! and a chunk body size through one generic parameter.

#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::get_unwrap,
        clippy::indexing_slicing,
        clippy::string_slice,
        clippy::arithmetic_side_effects,
        clippy::panic,
        clippy::unreachable,
        clippy::panic_in_result_fn,
        clippy::as_conversions
    )
)]

#[cfg(any(test, feature = "std"))]
extern crate std;

pub mod bin;
pub mod bundle;
pub mod network_id;
pub mod proximity_order;
pub mod spec;

/// Maximum proximity order for standard routing operations.
///
/// Value 31 gives 32 Kademlia bins (0-31). The protocol ceiling that
/// [`ProximityOrder`] and [`Bin`] validate against; spec methods narrow
/// below it per network.
pub const MAX_PO: u8 = 31;

pub use bin::{Bin, BinError};
pub use bundle::{SpecOf, SwarmPrimitives};
pub use network_id::NetworkId;
pub use proximity_order::{ProximityOrder, ProximityOrderError};
pub use spec::{Mainnet, NamedSwarm, Swarm, SwarmKind, SwarmSpec, Testnet};
