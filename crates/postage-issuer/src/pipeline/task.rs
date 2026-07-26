//! The sign job: one unit per admitted digest, shared by every engine.

use std::panic::{AssertUnwindSafe, catch_unwind};
#[cfg(feature = "parallel")]
use std::sync::Arc;
#[cfg(feature = "parallel")]
use std::sync::mpsc::Sender;

use super::StampResult;
use super::signer::{SignPrehash, sign_digest};
use crate::error::SigningError;
#[cfg(feature = "parallel")]
use nectar_marker::{MaybeSend, MaybeSync};
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

/// Spawns one sign job onto the rayon pool.
///
/// A dropped receiver discards the send.
#[cfg(feature = "parallel")]
pub(crate) fn spawn_sign<Sg>(signer: Arc<Sg>, digest: StampDigest, results: Sender<StampResult>)
where
    Sg: SignPrehash + MaybeSend + MaybeSync + 'static,
{
    rayon::spawn(move || {
        let _ = results.send(sign_task(signer.as_ref(), &digest));
    });
}
