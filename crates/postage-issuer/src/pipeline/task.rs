//! The sign job: one unit per admission batch.

use alloc::vec::Vec;
use std::panic::{AssertUnwindSafe, catch_unwind};

use super::StampResult;
use super::signer::{SignPrehash, sign_digest, sign_digests};
use crate::error::SigningError;
use nectar_postage::{Stamp, StampDigest};

/// Signs one digest, a panic becoming [`SigningError::Dropped`].
///
/// The signer outlives a caught panic; its interior state across that panic
/// is the caller's contract.
pub(crate) fn sign_caught<Sg>(signer: &Sg, digest: &StampDigest) -> Result<Stamp, SigningError>
where
    Sg: SignPrehash + ?Sized,
{
    catch_unwind(AssertUnwindSafe(|| sign_digest(signer, digest)))
        .unwrap_or_else(|_| Err(SigningError::Dropped))
}

/// Runs one sign job over a whole admission batch, one tagged result per
/// digest on every path.
///
/// A panic crosses the batch: every address in it yields
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
