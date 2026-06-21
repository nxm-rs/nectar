//! Feed indexing traits.
//!
//! A feed scheme is defined by three traits:
//!
//! - [`Index`] turns a position into the bytes mixed into the update id. It is
//!   deliberately minimal: it marshals to bytes and names a first position, and
//!   nothing else. There is no `next`, because choosing the next position is the
//!   cursor's job.
//! - [`WriteCursor`] is the stateful write head. It is a separate type from the
//!   index so that schemes which need history to place the next update (the
//!   epoch scheme needs the previous update time) have somewhere to keep it. A
//!   time-indexed scheme must derive the publish index from the new update's
//!   timestamp, so the cursor chooses the index before the write
//!   ([`index_for`](WriteCursor::index_for)) and records what was published
//!   afterward ([`record`](WriteCursor::record)). The writer is generic over the
//!   cursor and never inspects the scheme directly.
//! - [`LatestFinder`] is the scheme-owned latest-update lookup. Rather than
//!   exposing a generic probe iterator, each scheme implements its own async
//!   search over a [`ChunkGet`], because the optimal walk differs per scheme
//!   (linear for sequence, binary over the grid for epoch).

use nectar_primitives::store::ChunkGet;

use crate::error::Result;
use crate::feed::Feed;
use crate::update::FeedUpdate;

/// A position within a feed.
///
/// The marshalled bytes are mixed into the update id as
/// `keccak256(topic || index.marshal())`. Marshalling is **not** hashed; the
/// raw bytes go straight into the id preimage, so the byte layout is wire
/// conformance critical and must match the Swarm wire spec exactly.
pub trait Index: Clone {
    /// The wire bytes for this index, appended after the topic in the id
    /// preimage. The sequence scheme uses 8 big-endian bytes.
    fn marshal(&self) -> impl AsRef<[u8]>;

    /// The first index in the scheme (e.g. sequence number zero).
    fn first() -> Self;
}

/// A stateful write head over a feed scheme.
///
/// The cursor owns whatever state a scheme needs to choose the next publish
/// index. A write is two steps: [`index_for`](WriteCursor::index_for) computes
/// the index to publish at from the cursor's history and the new update's
/// timestamp `at`, then [`record`](WriteCursor::record) folds the published
/// index and timestamp back into the cursor. Splitting selection from recording
/// lets a time-indexed scheme derive the index from `at` (which a single
/// "advance after write" step could not), and lets the writer drive the cursor
/// through `&self` then `&mut self` without moving it out.
///
/// `at` is the update's timestamp in Unix seconds. Time-indexed schemes use it;
/// the sequence scheme ignores it.
pub trait WriteCursor {
    /// The index type this cursor produces.
    type Index: Index;

    /// The index to publish the next update at, given the new update's
    /// timestamp `at` (when the scheme is time-indexed).
    fn index_for(&self, at: Option<u64>) -> Self::Index;

    /// Record that an update was published at `index` with timestamp `at`,
    /// advancing the cursor so the following [`index_for`](Self::index_for)
    /// reflects it.
    fn record(&mut self, index: &Self::Index, at: Option<u64>);

    /// Whether the scheme's index space is spent, so a further publish has no
    /// valid index. The writer checks this before a publish and surfaces
    /// [`crate::FeedError::IndexExhausted`] instead of producing a wrong index
    /// or panicking. The default is `false`; an unbounded scheme never exhausts.
    fn exhausted(&self) -> bool {
        false
    }
}

/// A scheme-owned search for the latest update in a feed.
///
/// Implemented on the index type itself: given a feed, a chunk store, an
/// explicit target time `at`, and an optional lower bound (`after`, an index
/// already known to exist), locate the most recent update at or before `at` and
/// return it, or `None` if the feed is empty (or has no update past `after`).
///
/// The target time `at` is supplied by the caller, never read from a system
/// clock inside this crate: a primitive must be deterministic and `no_std`/wasm
/// clean. The sequence scheme ignores `at`; the epoch scheme uses it as the
/// lookup target on the grid.
///
/// The search is async because it probes the store, and generic over the body
/// size `BS` and the store `G` so a scheme works at any chunk size against any
/// retrieval backend.
pub trait LatestFinder: Index + Sized {
    /// Find the latest update in `feed` at or before `at`, searching strictly
    /// after `after` when it is `Some`.
    fn find_latest<const BS: usize, G: ChunkGet<BS>>(
        feed: &Feed<BS>,
        store: &G,
        at: Option<u64>,
        after: Option<&Self>,
    ) -> impl core::future::Future<Output = Result<Option<FeedUpdate<Self, BS>>>>;
}
