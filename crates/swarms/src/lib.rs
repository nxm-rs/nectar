//! Canonical type definitions for Ethereum Swarm networks.
//!
//! This crate provides types for identifying Swarm networks, similar to how
//! `alloy-chains` provides types for Ethereum chains.
//!
//! # Features
//!
//! - `std` (default): Enable standard library support
//! - `serde`: Enable serde serialization/deserialization
//! - `arbitrary`: Enable arbitrary trait implementations for testing

#![cfg_attr(not(test), warn(unused_crate_dependencies))]
#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]

mod named;
mod swarm;

pub use named::NamedSwarm;
pub use swarm::{Swarm, SwarmKind};
