//! Sealed prehash signing: the pipeline's only signer seam.

use alloy_primitives::{B256, Signature};
#[cfg(feature = "std")]
use alloy_signer::SignerSync;

use crate::error::SigningError;
use nectar_postage::{Stamp, StampDigest};

mod sealed {
    pub trait Sealed {}
}

/// Signs the 32-byte stamp prehash.
///
/// Sealed: obtain an implementation through [`Eip191`], which adapts any
/// synchronous signer, so the prehash handling is never hand-written.
pub trait SignPrehash: sealed::Sealed {
    /// Signs `prehash`, the keccak256 stamp digest.
    fn sign_prehash(&self, prehash: &B256) -> Result<Signature, SigningError>;
}

/// EIP-191 personal-message adapter over a synchronous signer.
#[derive(Debug, Clone)]
pub struct Eip191<S>(S);

impl<S> Eip191<S> {
    /// Wraps `signer`.
    pub const fn new(signer: S) -> Self {
        Self(signer)
    }

    /// Returns the wrapped signer.
    pub fn into_inner(self) -> S {
        self.0
    }
}

impl<S> sealed::Sealed for Eip191<S> {}

#[cfg(feature = "std")]
impl<S: SignerSync> SignPrehash for Eip191<S> {
    fn sign_prehash(&self, prehash: &B256) -> Result<Signature, SigningError> {
        self.0
            .sign_message_sync(prehash.as_slice())
            .map_err(SigningError::from)
    }
}

/// Signs an allocated digest into a wire-valid stamp.
pub(crate) fn sign_digest<Sg>(signer: &Sg, digest: &StampDigest) -> Result<Stamp, SigningError>
where
    Sg: SignPrehash + ?Sized,
{
    let prehash = digest.to_prehash();
    let signature = signer.sign_prehash(&prehash)?;
    Ok(Stamp::with_index(
        digest.batch_id,
        digest.index,
        digest.timestamp,
        signature,
    ))
}
