//! Swarm feeds over single-owner chunks.
//!
//! A feed is a mutable, owner-controlled sequence of updates published to
//! deterministic single-owner chunk (SOC) addresses. A reader who knows the
//! feed `topic` and `owner` can reconstruct every update address without any
//! out-of-band coordination, because each update lives at a content address
//! derived purely from the feed identity and an index.
//!
//! # Identity and addressing
//!
//! A [`Feed`] is the pair `{ topic, owner }`. For an update at some index `i`:
//!
//! - `id = keccak256(topic_bytes || index.marshal())`
//! - `address = keccak256(id || owner)`
//!
//! The `id` is the SOC id and the `address` is the SOC (and therefore the
//! network) address. The topic is used raw: callers that want a string topic
//! must pre-hash it (see [`Topic::from_bytes`]). The index marshalling is
//! scheme-specific and defined by the [`Index`] trait.
//!
//! # Schemes and versioning stance
//!
//! Two indexing schemes exist:
//!
//! - **Sequence** ([`Sequence`]): a monotonic big-endian `u64` counter. This is
//!   the canonical scheme, used for append-only logs, and the one exercised on
//!   the live network.
//! - **Epoch** ([`Epoch`], behind the `epoch` cargo feature): a time-indexed
//!   binary grid that supports efficient lookup of the update live at a given
//!   timestamp. It is experimental and is not used on the live network; build
//!   with `--features epoch` to opt in.
//!
//! Update payloads are V2 bare data: the published bytes are the update payload
//! verbatim, with no envelope. The V1 legacy payload (a timestamp prefix
//! followed by a reference) is not implemented here; reading it back is
//! deferred to nectar issue #92. The mutable resource update (MRU) construct is
//! not supported.
//!
//! Writing and latest-lookup are scheme-owned: a writer drives a stateful
//! [`WriteCursor`], and the latest update is located by a scheme-specific
//! [`LatestFinder`]. The cursor chooses the publish index from the new update's
//! timestamp, which is what lets the time-indexed epoch scheme place an update
//! on the grid. Both traits are generic over the body size and the chunk store
//! traits so a scheme never reaches into the writer or reader internals. No
//! clock is read inside this crate: a caller that wants the update live at a
//! specific moment passes that time to [`FeedReader::find_at`].
//!
//! # Reading and writing
//!
//! - [`FeedWriter`] signs and publishes updates through a [`ChunkPut`].
//! - [`FeedReader`] fetches updates through a [`ChunkGet`] and resolves the
//!   latest update through a [`LatestFinder`].
//!
//! The convenience aliases [`SequenceFeedReader`] and [`SequenceFeedWriter`]
//! pin the sequence scheme at [`DEFAULT_BODY_SIZE`].

#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]

extern crate alloc;

mod error;
mod feed;
mod index;
mod reader;
mod sequence;
mod topic;
mod update;
mod writer;

#[cfg(feature = "epoch")]
mod epoch;

pub use error::{FeedError, Result};
pub use feed::Feed;
pub use index::{Index, LatestFinder, WriteCursor};
pub use reader::FeedReader;
pub use sequence::{Sequence, SequenceCursor};
pub use topic::Topic;
pub use update::FeedUpdate;
pub use writer::FeedWriter;

#[cfg(feature = "epoch")]
pub use epoch::{Epoch, EpochCursor};

// Re-export the store surface and a memory store so consumers can wire a feed
// reader or writer without depending on `nectar-primitives` directly.
pub use nectar_primitives::store::{ChunkGet, ChunkPut, MemoryStore};
pub use nectar_primitives::{DEFAULT_BODY_SIZE, SingleOwnerChunk};

/// A [`FeedReader`] using the sequence scheme at [`DEFAULT_BODY_SIZE`].
pub type SequenceFeedReader<G> = FeedReader<Sequence, G, DEFAULT_BODY_SIZE>;

/// A [`FeedWriter`] using the sequence scheme at [`DEFAULT_BODY_SIZE`].
pub type SequenceFeedWriter<P, S> = FeedWriter<SequenceCursor, P, S, DEFAULT_BODY_SIZE>;
