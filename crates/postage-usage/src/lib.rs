//! Self-hosted postage batch utilization snapshots.
//!
//! Issuing postage stamps requires per-bucket counters so every stamp assigns a
//! fresh storage slot. This crate serializes those counters into a compact
//! snapshot stored *inside the batch it describes*, as single-owner chunks at
//! addresses derived from the batch id and owner alone, so a user can recover
//! their issuer state on any machine from just their key and batch id.
//!
//! # Layout
//!
//! Snapshot chunk `n` has single-owner chunk id
//! `keccak256("swarm-batch-usage" || batch_id || u16_be(n))`, owned and stamped
//! by the batch owner. Chunk 0 is the root: it commits to the batch geometry, a
//! sequence number, the slots the snapshot chunks occupy, and the digests of the
//! leaf chunks holding the counter table. Chunk ids never change, so each
//! snapshot chunk occupies one storage slot for the life of the batch (a newer
//! timestamp at the same address and stamp index overwrites in place).
//!
//! Counters use patched frame-of-reference packing sized to the *spread* of the
//! counters, not the batch depth. A table is immutable (monotone fill
//! watermarks) or mutable (wrapping ring cursors); see [`UsageTable`].
//!
//! See `README.md` for the full format specification.
//!
//! # Example
//!
//! ```
//! use alloy_primitives::{Address, B256};
//! use nectar_postage_usage::{
//!     BatchId, Mutability, PublishedSequence, RootInfo, Snapshot, ChunkAddress, UsageTable,
//! };
//!
//! let batch_id = BatchId::new([0x42; 32]);
//! let owner = Address::repeat_byte(0x11);
//!
//! // Issue a stamp for an uploaded chunk through the snapshot's issuing handle,
//! // then plan a persist.
//! let table = UsageTable::new(batch_id, 20, 16, Mutability::Immutable).unwrap();
//! let mut snapshot = Snapshot::new(table);
//! let address = ChunkAddress::from(B256::repeat_byte(0x99));
//! snapshot.issuer(owner).record_address(&address).unwrap();
//! // This table is fresh and was never published, so the live network read
//! // returns no root chunk and the floor is `NONE`.
//! let plan = snapshot
//!     .revalidate(PublishedSequence::NONE)
//!     .unwrap()
//!     .plan_persist(&owner)
//!     .unwrap();
//!
//! // Publish each plan chunk as a single-owner chunk stamped with
//! // `plan.chunks[n].stamp_index`. Reading back:
//! let root = RootInfo::parse(&plan.chunks[0].payload).unwrap();
//! let leaves: Vec<_> = plan.chunks[1..].iter().map(|c| &c.payload).collect();
//! let recovered = root.assemble(&leaves).unwrap();
//! assert_eq!(recovered, snapshot);
//! ```
//!
//! # Recovery
//!
//! [`Snapshot::new`] is for a genuinely fresh, never-persisted table: it starts
//! the persist history at sequence 0 with no allocated slots. Recovered or
//! extracted state must never go through it, because resetting the sequence to 0
//! and dropping the slots would downgrade the version at the snapshot's own chunk
//! addresses and re-allocate colliding slots, overwriting a newer persisted
//! version in place. Recovered state round-trips through [`Snapshot::from_parts`]
//! instead, which keeps the table, sequence, and slots bound together;
//! [`RootInfo::assemble`] uses it when decoding from the network, and
//! [`Snapshot::into_parts`] yields the same indivisible [`SnapshotParts`] value
//! when extracting state from a live snapshot.
//!
//! Both in-memory downgrade routes off a recovered or extracted snapshot are
//! closed. The move route is closed because [`SnapshotParts`] holds its table
//! privately and never yields it by value. The clone route is closed because
//! [`Snapshot::table`] and [`SnapshotParts::table`] return a borrowed
//! [`TableView`] that exposes only read getters and does not deref to the
//! table, so cloning or copying it yields another borrowed view, never an owned
//! table that [`Snapshot::new`] would accept. No public API hands out an owned
//! [`UsageTable`] taken from a recovered snapshot.
//!
//! Two residual paths to a sequence-0 persist are protocol-level rather than
//! in-memory representability concerns, so the type guards here do not close
//! them; the [`PublishedSequence`] floor on [`Snapshot::revalidate`] does
//! (nectar issue #70). First, the public table constructors ([`UsageTable::new`]
//! and friends) must keep minting a fresh table for a genuinely new batch, so a
//! forged fresh table persisted at sequence 0 is caught by the floor, not by the
//! type system here. Second, the reserve overwrites a snapshot chunk by stamp
//! timestamp rather than by snapshot sequence, so full cross-version monotonicity
//! against the *published* sequence needs a compare-and-swap against the live
//! root chunk. The floor precondition implemented on [`Snapshot::revalidate`]
//! supplies exactly that compare-and-swap: the consumer reads the published
//! sequence from the live root chunk, hands it in as the floor, and a persist
//! whose next sequence does not strictly exceed it is rejected. This crate closes
//! the in-memory representability of the downgrade, and the floor closes the
//! persist-time downgrade.

#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]
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
        clippy::panic_in_result_fn
    )
)]

extern crate alloc;

mod codec;
mod error;
mod snapshot;
mod table;

#[cfg(feature = "client")]
mod client;
#[cfg(feature = "issuer")]
mod issuer;
#[cfg(feature = "seal")]
mod seal;

pub use codec::RootInfo;
pub use error::UsageError;
pub use snapshot::{
    Issuer, PersistPlan, PlannedChunk, PublishedSequence, Snapshot, SnapshotParts, Validated,
};
pub use table::{Mutability, TableView, UsageTable};

#[cfg(feature = "issuer")]
pub use issuer::SnapshotIssuer;

#[cfg(feature = "seal")]
pub use seal::{SealError, SealedChunk, seal_plan};

#[cfg(feature = "client")]
pub use client::{BatchStamper, ClientError, SnapshotSink, SnapshotSource};

pub use nectar_primitives::{ChunkAddress, SocId};

/// Postage types re-exported so a downstream caller naming
/// [`PlannedChunk::stamp_index`] or calling [`UsageTable::from_batch`] does not
/// need a direct `nectar-postage` dependency.
pub use nectar_postage::{Batch, BatchId, BucketDepth, StampIndex};

use alloy_primitives::{Address, Keccak256};

/// Result alias for this crate.
pub type Result<T> = core::result::Result<T, UsageError>;

/// Domain separator for snapshot chunk ids.
pub const USAGE_DOMAIN: &[u8] = b"swarm-batch-usage";

/// The snapshot format magic ("SBU" plus the format version).
pub const MAGIC: [u8; 4] = *b"SBU1";

/// Size of the fixed root header in bytes.
pub const ROOT_HEADER_SIZE: usize = 66;

/// Maximum payload size of a snapshot chunk.
pub const MAX_PAYLOAD_SIZE: usize = nectar_primitives::DEFAULT_BODY_SIZE;

/// Maximum number of exception entries in a snapshot.
pub const MAX_EXCEPTIONS: usize = 128;

/// Maximum bucket (uniformity) depth supported by the format.
pub const MAX_BUCKET_DEPTH: u8 = 16;

/// Maximum value of `depth - bucket_depth` supported by the format, chosen
/// so counters fit in a `u32`.
pub const MAX_COUNTER_BITS: u8 = 31;

/// Maximum delta bit width.
pub(crate) const MAX_WIDTH: u8 = 32;

/// Returns the single-owner chunk id of snapshot chunk `index` (0 is the
/// root, `n >= 1` is leaf `n - 1`).
pub fn usage_chunk_id(batch_id: &BatchId, index: u16) -> SocId {
    let mut hasher = Keccak256::new();
    hasher.update(USAGE_DOMAIN);
    hasher.update(batch_id);
    hasher.update(index.to_be_bytes());
    SocId::from(hasher.finalize())
}

/// Returns the address of snapshot chunk `index` for a batch owned by
/// `owner`, i.e. the single-owner chunk address `keccak256(id || owner)`.
pub fn usage_chunk_address(batch_id: &BatchId, owner: &Address, index: u16) -> ChunkAddress {
    let mut hasher = Keccak256::new();
    hasher.update(usage_chunk_id(batch_id, index));
    hasher.update(owner);
    ChunkAddress::from(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_ids_are_domain_separated_and_indexed() {
        let a = BatchId::new([0x01; 32]);
        let b = BatchId::new([0x02; 32]);
        assert_ne!(usage_chunk_id(&a, 0), usage_chunk_id(&b, 0));
        assert_ne!(usage_chunk_id(&a, 0), usage_chunk_id(&a, 1));
        // Deterministic.
        assert_eq!(usage_chunk_id(&a, 3), usage_chunk_id(&a, 3));
    }

    #[test]
    fn chunk_address_binds_owner() {
        let batch = BatchId::new([0x01; 32]);
        let alice = Address::repeat_byte(0xaa);
        let bob = Address::repeat_byte(0xbb);
        assert_ne!(
            usage_chunk_address(&batch, &alice, 0),
            usage_chunk_address(&batch, &bob, 0)
        );
    }
}
