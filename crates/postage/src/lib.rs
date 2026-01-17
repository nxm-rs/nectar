//! Postage stamp primitives for Ethereum Swarm.
//!
//! This crate provides the core types for postage stamps in the Swarm network:
//!
//! - [`BatchId`]: A 32-byte identifier for a postage batch
//! - [`Batch`]: A postage batch with depth, owner, and other properties
//! - [`Stamp`]: A postage stamp proving payment for chunk storage
//! - [`StampIndex`]: The bucket and position index within a stamp
//! - [`MarshalledStamp`]: A 113-byte serialized stamp format
//! - [`Stamper`]: Trait for entities that can stamp chunks
//!
//! # Stamping Flow
//!
//! 1. A chunk's address determines which collision bucket it belongs to
//! 2. The stamper tracks usage within each bucket
//! 3. When stamping, the stamper assigns the next available index in the bucket
//! 4. The stamp is signed by the batch owner
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
mod stamper;

pub use batch::{Batch, BatchId};
pub use stamp::{MarshalledStamp, Stamp, StampError, StampIndex, STAMP_SIZE};
pub use stamper::{calculate_bucket, BatchExt, Stamper};
