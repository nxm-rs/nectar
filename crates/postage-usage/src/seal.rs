//! Turns a persist plan into signed single-owner chunks and stamps.

use alloc::vec::Vec;

use alloy_signer::SignerSync;
use nectar_postage::{Stamp, StampDigest};
use nectar_primitives::{Chunk, SingleOwnerChunk};
use thiserror::Error;

use crate::snapshot::{PersistPlan, Snapshot};

/// Errors produced while sealing a persist plan.
#[derive(Debug, Error)]
pub enum SealError {
    /// The signer failed to produce a signature.
    #[error(transparent)]
    Signer(#[from] alloy_signer::Error),
    /// Building the single-owner chunk failed.
    #[error(transparent)]
    Chunk(#[from] nectar_primitives::PrimitivesError),
    /// The sealed chunk address does not match the plan; the signer does not
    /// belong to the owner the plan was made for.
    #[error("sealed chunk address does not match plan")]
    AddressMismatch,
    /// The seal timestamp does not strictly exceed the timestamp the previous
    /// persist was sealed with in this process. Overwriting a metadata chunk in
    /// place needs a strictly newer timestamp, so the reserve would refuse to
    /// replace the previous version with this one.
    #[error("seal timestamp {timestamp} does not exceed previous seal timestamp {previous}")]
    NonIncreasingTimestamp {
        /// The timestamp the seal was attempted with.
        timestamp: u64,
        /// The timestamp the previous persist was sealed with.
        previous: u64,
    },
}

/// A snapshot chunk ready for upload: the signed single-owner chunk and the
/// stamp attaching it to the batch.
#[derive(Debug, Clone)]
pub struct SealedChunk {
    /// The signed single-owner chunk.
    pub chunk: SingleOwnerChunk,
    /// The stamp for the chunk.
    pub stamp: Stamp,
}

/// Signs every chunk of a persist plan and stamps it with its planned slot,
/// enforcing in-process stamp-timestamp monotonicity.
///
/// `signer` must be the batch owner key: it signs both the single-owner chunks
/// and the stamps. `timestamp` must strictly exceed the timestamp the previous
/// persist of this `snapshot` was sealed with in this process
/// ([`Snapshot::last_seal_timestamp`], also surfaced on
/// [`PersistPlan::previous_timestamp`]); otherwise this returns
/// [`SealError::NonIncreasingTimestamp`] and seals nothing. Because each
/// metadata chunk keeps the same address and stamp index for the life of the
/// batch, a newer version overwrites the previous one in place only with a
/// strictly newer timestamp, so a non-increasing timestamp would leave the stale
/// version standing. This is the single-owner clock-skew guard, complementary to
/// the cross-process [`PublishedSequence`](crate::PublishedSequence) floor.
///
/// On success the snapshot records `timestamp` as its new floor, so the next
/// persist's [`PersistPlan::previous_timestamp`] reflects it and monotonicity
/// holds across the whole persist/seal cycle without caller bookkeeping. `plan`
/// must have been planned from `snapshot`.
#[must_use = "the sealed chunks are the snapshot to upload; dropping them discards the seal"]
pub fn seal_plan(
    snapshot: &mut Snapshot,
    plan: &PersistPlan,
    timestamp: u64,
    signer: &impl SignerSync,
) -> core::result::Result<Vec<SealedChunk>, SealError> {
    if let Some(previous) = snapshot.last_seal_timestamp()
        && timestamp <= previous
    {
        return Err(SealError::NonIncreasingTimestamp {
            timestamp,
            previous,
        });
    }

    let sealed = plan
        .chunks
        .iter()
        .map(|planned| {
            let chunk = SingleOwnerChunk::new(planned.id, planned.payload.clone(), signer)?;
            if *chunk.address() != planned.address {
                return Err(SealError::AddressMismatch);
            }
            let digest = StampDigest::new(
                planned.address,
                plan.batch_id,
                planned.stamp_index,
                timestamp,
            );
            let signature = signer.sign_message_sync(digest.to_prehash().as_slice())?;
            let stamp = Stamp::new(
                plan.batch_id,
                planned.stamp_index.bucket(),
                planned.stamp_index.index(),
                timestamp,
                signature,
            );
            Ok(SealedChunk { chunk, stamp })
        })
        .collect::<core::result::Result<Vec<_>, SealError>>()?;

    // Only advance the floor once the whole plan sealed: a mid-plan failure
    // leaves the snapshot's timestamp untouched so the seal can be retried.
    snapshot.record_seal_timestamp(timestamp);
    Ok(sealed)
}
