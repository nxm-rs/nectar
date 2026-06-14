//! A [`StampIssuer`] that stamps content through a [`Snapshot`], the single
//! owner-aware issuance path so a snapshot can back a `BatchStamper` directly.

use alloy_primitives::Address;
use nectar_postage::{BatchId, StampDigest, StampError};
use nectar_postage_issuer::StampIssuer;
use nectar_primitives::SwarmAddress;

use crate::Snapshot;
use crate::error::UsageError;

/// Maps a usage table error onto a stamp issuer error.
const fn map_usage_error(err: UsageError) -> StampError {
    match err {
        UsageError::BucketFull { bucket, capacity } => StampError::BucketFull { bucket, capacity },
        _ => StampError::InvalidIndex,
    }
}

/// A [`StampIssuer`] that stamps content through a [`Snapshot`]'s table, so
/// content stamping and snapshot allocation share one table and never collide.
///
/// It issues through the snapshot's reserved-aware
/// [`Issuer`](crate::Issuer): on a mutable batch the ring skips the reserved
/// slots so it never evicts the batch-state chunks. It owns the snapshot by
/// value to drop into `BatchStamper::new`; recover it with
/// [`into_snapshot`](Self::into_snapshot).
#[derive(Debug, Clone)]
pub struct SnapshotIssuer {
    snapshot: Snapshot,
    owner: Address,
}

impl SnapshotIssuer {
    /// Wraps a snapshot and the batch owner address.
    pub const fn new(snapshot: Snapshot, owner: Address) -> Self {
        Self { snapshot, owner }
    }

    /// Returns a reference to the wrapped snapshot.
    pub const fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }

    /// Returns a mutable reference to the wrapped snapshot, for example to plan
    /// a persist between batches of content stamping.
    pub const fn snapshot_mut(&mut self) -> &mut Snapshot {
        &mut self.snapshot
    }

    /// Consumes the adapter and returns the wrapped snapshot.
    pub fn into_snapshot(self) -> Snapshot {
        self.snapshot
    }

    /// Returns the batch owner address.
    pub const fn owner(&self) -> Address {
        self.owner
    }
}

impl StampIssuer for SnapshotIssuer {
    fn prepare_stamp(
        &mut self,
        address: &SwarmAddress,
        timestamp: u64,
    ) -> core::result::Result<StampDigest, StampError> {
        let index = self
            .snapshot
            .record_address(self.owner, address)
            .map_err(map_usage_error)?;
        Ok(StampDigest::new(
            *address,
            self.snapshot.table_ref().batch_id(),
            index,
            timestamp,
        ))
    }

    fn batch_id(&self) -> BatchId {
        self.snapshot.table_ref().batch_id()
    }

    fn batch_depth(&self) -> u8 {
        self.snapshot.table_ref().depth()
    }

    fn bucket_depth(&self) -> u8 {
        self.snapshot.table_ref().bucket_depth()
    }

    fn max_bucket_utilization(&self) -> u32 {
        self.snapshot.table_ref().max_count()
    }

    fn bucket_utilization(&self, bucket: u32) -> u32 {
        self.snapshot.table_ref().count(bucket).unwrap_or(0)
    }

    fn bucket_has_capacity(&self, bucket: u32) -> bool {
        // A mutable ring always has a slot (it wraps); an immutable bucket has
        // capacity until its watermark reaches the bound.
        self.snapshot.table_ref().is_mutable()
            || self
                .snapshot
                .table_ref()
                .has_capacity(bucket)
                .unwrap_or(false)
    }

    fn stamps_issued(&self) -> Option<u64> {
        // Immutable issuance is monotone, so the counter sum is the lifetime
        // count. A mutable ring keeps only a wrapping cursor whose sum is a
        // checksum, so there is no lifetime count to give: return `None` rather
        // than forwarding the checksum as if it were a count.
        if self.snapshot.table_ref().is_mutable() {
            None
        } else {
            Some(self.snapshot.table_ref().total_issued())
        }
    }
}
