//! The sign task: one spawned unit per admitted digest.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::mpsc::Sender;

use super::StampResult;
use super::signer::{SignPrehash, sign_digest};
use crate::error::SigningError;
use nectar_marker::{MaybeSend, MaybeSync};
use nectar_postage::StampDigest;

/// Spawns one sign task for an admitted digest.
///
/// Job contract: exactly one tagged result is sent on every path; a panic
/// becomes [`SigningError::Dropped`] for the address captured at admission.
/// A dropped receiver discards the send.
pub(crate) fn spawn_sign<Sg>(signer: Arc<Sg>, digest: StampDigest, results: Sender<StampResult>)
where
    Sg: SignPrehash + MaybeSend + MaybeSync + 'static,
{
    rayon::spawn(move || {
        let address = digest.chunk_address;
        // The signer outlives a caught panic; its interior state across that
        // panic is the caller's contract.
        let result = catch_unwind(AssertUnwindSafe(|| sign_digest(signer.as_ref(), &digest)))
            .unwrap_or_else(|_| Err(SigningError::Dropped));
        let _ = results.send(StampResult { address, result });
    });
}
