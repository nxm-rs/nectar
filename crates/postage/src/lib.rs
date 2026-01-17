//! Postage stamp primitives for Ethereum Swarm.
//!
//! This crate provides the core types for postage stamps in the Swarm network:
//! - [`BatchId`]: A 32-byte identifier for a postage batch
//! - [`Batch`]: A postage batch with depth, owner, and other properties
//! - [`Stamp`]: A postage stamp proving payment for chunk storage
//! - [`MarshalledStamp`]: A 113-byte serialized stamp format
//!
//! # Features
//!
//! - `std` (default): Enable standard library support
//! - `serde`: Enable serde serialization/deserialization

#![cfg_attr(not(test), warn(unused_crate_dependencies))]
#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]

mod batch;
mod stamp;

pub use batch::{Batch, BatchId};
pub use stamp::{MarshalledStamp, Stamp, StampError, STAMP_SIZE};
