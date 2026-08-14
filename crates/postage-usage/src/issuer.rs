//! A [`StampIssuer`] that stamps content through a [`Snapshot`], the single
//! owner-aware issuance path so a snapshot can back a `BatchStamper` directly.

use alloy_primitives::Address;
use core::cell::{Ref, RefCell};
use nectar_postage::{BatchDepth, BatchId, Bucket, StampError};
use nectar_postage_issuer::{Prepared, StampIssuer};
use nectar_primitives::{ChunkAddress, Mainnet, SwarmSpec};

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
///
/// The table walk reads more than one word, so the cell serializes it and
/// leaves the issuer `!Sync`.
#[derive(Debug)]
pub struct SnapshotIssuer<S: SwarmSpec = Mainnet> {
    snapshot: RefCell<Snapshot<S>>,
    owner: Address,
}

// The spec is a type-level tag, so this carries no bound on `S` beyond
// `SwarmSpec`; deriving would demand `S: Clone` of a marker type that holds no
// data.
impl<S: SwarmSpec> Clone for SnapshotIssuer<S> {
    fn clone(&self) -> Self {
        Self {
            snapshot: RefCell::new(self.snapshot.borrow().clone()),
            owner: self.owner,
        }
    }
}

impl<S: SwarmSpec> SnapshotIssuer<S> {
    /// Wraps a snapshot and the batch owner address.
    pub const fn new(snapshot: Snapshot<S>, owner: Address) -> Self {
        Self {
            snapshot: RefCell::new(snapshot),
            owner,
        }
    }

    /// Borrows the wrapped snapshot.
    pub fn snapshot(&self) -> Ref<'_, Snapshot<S>> {
        self.snapshot.borrow()
    }

    /// Returns a mutable reference to the wrapped snapshot, for example to plan
    /// a persist between batches of content stamping.
    pub fn snapshot_mut(&mut self) -> &mut Snapshot<S> {
        self.snapshot.get_mut()
    }

    /// Consumes the adapter and returns the wrapped snapshot.
    pub fn into_snapshot(self) -> Snapshot<S> {
        self.snapshot.into_inner()
    }

    /// Returns the batch owner address.
    pub const fn owner(&self) -> Address {
        self.owner
    }
}

impl<S: SwarmSpec> StampIssuer for SnapshotIssuer<S> {
    type Spec = S;

    fn reserve(
        &self,
        address: &ChunkAddress,
        timestamp: u64,
    ) -> core::result::Result<Prepared<S>, StampError> {
        let snapshot = &mut *self.snapshot.borrow_mut();
        let index = snapshot
            .record_address(self.owner, address)
            .map_err(map_usage_error)?;
        let table = snapshot.table_ref();
        let bucket_depth = table.bucket_depth();
        Ok(Prepared::new(
            *address,
            table.batch_id(),
            Bucket::checked(index.bucket(), bucket_depth)?,
            BatchDepth::new(table.depth(), bucket_depth)?,
            index.index(),
            timestamp,
        ))
    }

    fn batch_id(&self) -> BatchId {
        self.snapshot.borrow().table_ref().batch_id()
    }

    fn batch_depth(&self) -> u8 {
        self.snapshot.borrow().table_ref().depth()
    }

    fn bucket_depth(&self) -> u8 {
        self.snapshot.borrow().table_ref().bucket_depth().get()
    }

    fn max_bucket_utilization(&self) -> u32 {
        self.snapshot.borrow().table_ref().max_count()
    }

    fn bucket_utilization(&self, bucket: u32) -> u32 {
        self.snapshot
            .borrow()
            .table_ref()
            .count(bucket)
            .unwrap_or(0)
    }

    fn bucket_has_capacity(&self, bucket: u32) -> bool {
        // A mutable ring always has a slot (it wraps); an immutable bucket has
        // capacity until its watermark reaches the bound.
        let snapshot = self.snapshot.borrow();
        snapshot.table_ref().is_mutable()
            || snapshot.table_ref().has_capacity(bucket).unwrap_or(false)
    }

    fn stamps_issued(&self) -> Option<u64> {
        // Immutable issuance is monotone, so the counter sum is the lifetime
        // count. A mutable ring keeps only a wrapping cursor whose sum is a
        // checksum, so there is no lifetime count to give: return `None` rather
        // than forwarding the checksum as if it were a count.
        let snapshot = self.snapshot.borrow();
        if snapshot.table_ref().is_mutable() {
            None
        } else {
            Some(snapshot.table_ref().total_issued())
        }
    }
}
