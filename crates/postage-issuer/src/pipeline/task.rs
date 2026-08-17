//! The sign job: one unit per admission batch.

use alloc::vec::Vec;
use std::panic::{AssertUnwindSafe, catch_unwind};

use super::StampResult;
use super::signer::{SignPrehash, sign_digest, sign_digests};
use crate::error::SigningError;
use nectar_postage::StampDigest;

/// Runs one sign job to its tagged result.
///
/// Job contract: exactly one tagged result on every path; a panic becomes
/// [`SigningError::Dropped`] for the address captured at admission.
pub(crate) fn sign_task<Sg>(signer: &Sg, digest: &StampDigest) -> StampResult
where
    Sg: SignPrehash + ?Sized,
{
    let address = digest.chunk_address;
    // The signer outlives a caught panic; its interior state across that
    // panic is the caller's contract.
    let result = catch_unwind(AssertUnwindSafe(|| sign_digest(signer, digest)))
        .unwrap_or_else(|_| Err(SigningError::Dropped));
    StampResult { address, result }
}

/// Runs one sign job over a whole admission batch.
///
/// Job contract: exactly one tagged result per digest on every path; a panic
/// crosses the batch, so every address in it yields
/// [`SigningError::Dropped`].
pub(crate) fn sign_batch<Sg>(signer: &Sg, digests: &[StampDigest]) -> Vec<StampResult>
where
    Sg: SignPrehash + ?Sized,
{
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
