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
//! use nectar_postage_usage::{RootInfo, Snapshot, SwarmAddress, UsageTable};
//!
//! let batch_id = B256::repeat_byte(0x42);
//! let owner = Address::repeat_byte(0x11);
//!
//! // Record an uploaded chunk against the shared table, then plan a persist.
//! let mut snapshot = Snapshot::new(UsageTable::new(batch_id, 20, 16).unwrap());
//! let address = SwarmAddress::from(B256::repeat_byte(0x99));
//! snapshot.record_address(&owner, &address).unwrap();
//! let plan = snapshot.plan_persist(&owner).unwrap();
//!
//! // Publish each plan chunk as a single-owner chunk stamped with
//! // `plan.chunks[n].stamp_index`. Reading back:
//! let root = RootInfo::parse(&plan.chunks[0].payload).unwrap();
//! let leaves: Vec<_> = plan.chunks[1..].iter().map(|c| &c.payload).collect();
//! let recovered = root.assemble(&leaves).unwrap();
//! assert_eq!(recovered, snapshot);
//! ```

#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]

extern crate alloc;

mod codec;
mod error;
mod snapshot;
mod table;

#[cfg(feature = "issuer")]
mod issuer;
#[cfg(feature = "seal")]
mod seal;

pub use codec::{Encoded, RootInfo};
pub use error::UsageError;
pub use snapshot::{PersistPlan, PlannedChunk, Snapshot};
pub use table::UsageTable;

#[cfg(feature = "issuer")]
pub use issuer::SnapshotIssuer;

#[cfg(feature = "seal")]
pub use seal::{SealError, SealedChunk, seal_plan};

pub use nectar_primitives::SwarmAddress;

use alloy_primitives::{Address, B256, Keccak256};
use nectar_postage::BatchId;

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
pub fn usage_chunk_id(batch_id: &BatchId, index: u16) -> B256 {
    let mut hasher = Keccak256::new();
    hasher.update(USAGE_DOMAIN);
    hasher.update(batch_id);
    hasher.update(index.to_be_bytes());
    hasher.finalize()
}

/// Returns the address of snapshot chunk `index` for a batch owned by
/// `owner`, i.e. the single-owner chunk address `keccak256(id || owner)`.
pub fn usage_chunk_address(batch_id: &BatchId, owner: &Address, index: u16) -> SwarmAddress {
    let mut hasher = Keccak256::new();
    hasher.update(usage_chunk_id(batch_id, index));
    hasher.update(owner);
    SwarmAddress::from(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_ids_are_domain_separated_and_indexed() {
        let a = B256::repeat_byte(1);
        let b = B256::repeat_byte(2);
        assert_ne!(usage_chunk_id(&a, 0), usage_chunk_id(&b, 0));
        assert_ne!(usage_chunk_id(&a, 0), usage_chunk_id(&a, 1));
        // Deterministic.
        assert_eq!(usage_chunk_id(&a, 3), usage_chunk_id(&a, 3));
    }

    #[test]
    fn chunk_address_binds_owner() {
        let batch = B256::repeat_byte(1);
        let alice = Address::repeat_byte(0xaa);
        let bob = Address::repeat_byte(0xbb);
        assert_ne!(
            usage_chunk_address(&batch, &alice, 0),
            usage_chunk_address(&batch, &bob, 0)
        );
    }
}
