//! The sign job: one unit per admission batch.

use alloc::vec::Vec;
use std::panic::{AssertUnwindSafe, catch_unwind};

use super::StampResult;
use super::signer::{SignPrehash, sign_digests};
use crate::error::SigningError;
use nectar_postage::StampDigest;

/// Runs one sign job over a whole admission batch, one tagged result per
/// digest on every path.
///
/// A panic crosses the batch: every address in it yields
/// [`SigningError::Dropped`].
pub(crate) fn sign_batch<Sg>(signer: &Sg, digests: &[StampDigest]) -> Vec<StampResult>
where
    Sg: SignPrehash + ?Sized,
{
    // reinvention: panic boundary; an unwinding signer drops its reply unsent instead of aborting.
    catch_unwind(AssertUnwindSafe(|| sign_digests(signer, digests))).map_or_else(
        |_| {
            digests
                .iter()
                .map(|digest| StampResult {
                    address: digest.chunk_address,
                    result: Err(SigningError::Dropped),
                })
                .collect()
        },
        |results| {
            digests
                .iter()
                .zip(results)
                .map(|(digest, result)| StampResult {
                    address: digest.chunk_address,
                    result,
                })
                .collect()
        },
    )
}
