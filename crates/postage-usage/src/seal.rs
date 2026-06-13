//! Turns a persist plan into signed single-owner chunks and stamps.

use alloc::vec::Vec;

use alloy_signer::SignerSync;
use nectar_postage::{Stamp, StampDigest};
use nectar_primitives::{Chunk, SingleOwnerChunk};
use thiserror::Error;

use crate::snapshot::PersistPlan;

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

/// Signs every chunk of a persist plan and stamps it with its planned slot.
///
/// `signer` must be the batch owner key: it signs both the single-owner
/// chunks and the stamps. `timestamp` must be strictly greater than the one
/// used for the previous persist, so the new versions overwrite the old ones
/// in place.
pub fn seal_plan(
    plan: &PersistPlan,
    timestamp: u64,
    signer: &impl SignerSync,
) -> core::result::Result<Vec<SealedChunk>, SealError> {
    plan.chunks
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
        .collect()
}
