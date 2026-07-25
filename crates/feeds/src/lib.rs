//! Swarm feeds: owner-signed mutable references over single-owner chunks.
//!
//! A feed is the `(topic, owner)` pair. The update at index `i` is a plain
//! single-owner chunk (no new wire type) whose body wraps the payload's
//! content-addressed body:
//!
//! - `id = keccak256(topic || i.marshal())`
//! - `address = keccak256(id || owner)`
//!
//! [`Updater`] signs and publishes updates through
//! [`ChunkPut`](nectar_primitives::store::ChunkPut); [`Getter`] fetches and
//! certifies them through [`ChunkGet`](nectar_primitives::store::ChunkGet)
//! and locates the latest [`Sequence`] update by probing
//! [`ChunkHas`](nectar_primitives::store::ChunkHas). No clock and no network
//! live in this crate.
//!
//! ```
//! use alloy_primitives::address;
//! use nectar_feeds::{Feed, Index, Sequence, Topic};
//!
//! let feed: Feed = Feed::new(
//!     Topic::from_label("example"),
//!     address!("0x8d3766440f0d7b949a5e32995d09619a7f86e632"),
//! );
//! let first = feed.update_address(&Sequence::ZERO);
//! let second = feed.update_address(&Sequence::ZERO.next().unwrap());
//! assert_ne!(first, second);
//! ```

#![cfg_attr(not(test), warn(unused_crate_dependencies))]
#![cfg_attr(docsrs, feature(doc_cfg))]
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

mod error;
mod feed;
mod getter;
mod index;
mod sequence;
mod topic;
mod update;
mod updater;

#[cfg(any(test, feature = "arbitrary"))]
pub mod generators;

pub use error::{FeedError, Result};
pub use feed::Feed;
pub use getter::{Getter, Latest};
pub use index::Index;
pub use sequence::Sequence;
pub use topic::Topic;
pub use update::FeedUpdate;
pub use updater::Updater;
