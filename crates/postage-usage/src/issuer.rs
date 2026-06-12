//! [`StampIssuer`] implementation so a [`UsageTable`] can back a
//! `BatchStamper` directly.

use nectar_postage::{BatchId, StampDigest, StampError, StampIndex, calculate_bucket};
use nectar_postage_issuer::StampIssuer;
use nectar_primitives::SwarmAddress;

use crate::UsageTable;
use crate::error::UsageError;

impl StampIssuer for UsageTable {
    fn prepare_stamp(
        &mut self,
        address: &SwarmAddress,
        timestamp: u64,
    ) -> core::result::Result<StampDigest, StampError> {
        let bucket = calculate_bucket(address, self.bucket_depth);
        let index = self.record(bucket).map_err(|err| match err {
            UsageError::BucketFull { bucket, capacity } => {
                StampError::BucketFull { bucket, capacity }
            }
            _ => StampError::InvalidIndex,
        })?;
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
