//! [`StampIssuer`] implementations so a [`UsageTable`] or a [`Snapshot`] can
//! back a `BatchStamper` directly.

use alloy_primitives::Address;
use nectar_postage::{BatchId, StampDigest, StampError, StampIndex, calculate_bucket};
use nectar_postage_issuer::StampIssuer;
use nectar_primitives::SwarmAddress;

use crate::error::UsageError;
use crate::{Snapshot, UsageTable};

/// Maps a usage table error onto a stamp issuer error.
const fn map_usage_error(err: UsageError) -> StampError {
    match err {
        UsageError::BucketFull { bucket, capacity } => StampError::BucketFull { bucket, capacity },
        _ => StampError::InvalidIndex,
    }
}

impl StampIssuer for UsageTable {
    fn prepare_stamp(
        &mut self,
        address: &SwarmAddress,
        timestamp: u64,
    ) -> core::result::Result<StampDigest, StampError> {
        let bucket = calculate_bucket(address, self.bucket_depth);
        let index = self.record(bucket).map_err(map_usage_error)?;
        Ok(StampDigest::new(
            *address,
            self.batch_id,
            StampIndex::new(bucket, index),
            timestamp,
        ))
    }

    fn batch_id(&self) -> BatchId {
        self.batch_id
    }

    fn batch_depth(&self) -> u8 {
        self.depth
    }

    fn bucket_depth(&self) -> u8 {
        self.bucket_depth
    }

    fn max_bucket_utilization(&self) -> u32 {
        self.max_count()
    }

    fn bucket_utilization(&self, bucket: u32) -> u32 {
        self.count(bucket).unwrap_or(0)
    }

    fn bucket_has_capacity(&self, bucket: u32) -> bool {
        self.has_capacity(bucket).unwrap_or(false)
    }

    fn stamps_issued(&self) -> u64 {
        self.issued
    }
}

/// A [`StampIssuer`] that stamps content through a [`Snapshot`]'s table, so
/// content stamping and snapshot allocation share one table and never collide.
///
/// Owner-aware, unlike stamping a bare [`UsageTable`]: on a mutable batch it
/// skips the reserved slots so the ring never evicts the batch-state chunks. It
/// owns the snapshot by value to drop into `BatchStamper::new`; recover it with
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
            .record_address(&self.owner, address)
            .map_err(map_usage_error)?;
        Ok(StampDigest::new(
            *address,
            self.snapshot.table().batch_id(),
            index,
            timestamp,
        ))
    }

    fn batch_id(&self) -> BatchId {
        self.snapshot.table().batch_id()
    }

    fn batch_depth(&self) -> u8 {
        self.snapshot.table().depth()
    }

    fn bucket_depth(&self) -> u8 {
        self.snapshot.table().bucket_depth()
    }

    fn max_bucket_utilization(&self) -> u32 {
        self.snapshot.table().max_count()
    }

    fn bucket_utilization(&self, bucket: u32) -> u32 {
        self.snapshot.table().count(bucket).unwrap_or(0)
    }

    fn bucket_has_capacity(&self, bucket: u32) -> bool {
        // A mutable ring always has a slot (it wraps); an immutable bucket has
        // capacity until its watermark reaches the bound.
        self.snapshot.table().is_mutable()
            || self.snapshot.table().has_capacity(bucket).unwrap_or(false)
    }

    fn stamps_issued(&self) -> u64 {
        self.snapshot.table().total_issued()
    }
}
