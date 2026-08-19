//! Sealed prehash signing: the pipeline's only signer seam.

use alloy_primitives::{Address, B256, Signature};
#[cfg(feature = "std")]
use alloy_signer::{Signer, SignerSync};

use crate::error::{IssuerError, SigningError};
use nectar_postage::{Batch, Stamp, StampDigest};
use nectar_primitives::{Mainnet, SwarmSpec};

pub(crate) mod sealed {
    pub trait Sealed {}
}

/// Signs the 32-byte stamp prehash, and names the key it signs with.
///
/// Sealed: obtain an implementation through [`Eip191`], which adapts any
/// synchronous signer, so the prehash handling is never hand-written.
pub trait SignPrehash: sealed::Sealed {
    /// The address stamps signed here recover to.
    fn address(&self) -> Address;

    /// Signs `prehash`, the keccak256 stamp digest.
    fn sign_prehash(&self, prehash: &B256) -> Result<Signature, SigningError>;
}

/// A signer proven to hold the key of the batch it stamps for.
///
/// Sealed through [`SignPrehash`]: [`BatchSigner::bind`] is the only route.
pub trait BoundSigner: SignPrehash {
    /// The network the batch was bought on.
    type Spec: SwarmSpec;

    /// The batch this signer owns.
    fn batch(&self) -> &Batch<Self::Spec>;
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
impl<S: SignerSync + Signer> SignPrehash for Eip191<S> {
    fn address(&self) -> Address {
        self.0.address()
    }

    fn sign_prehash(&self, prehash: &B256) -> Result<Signature, SigningError> {
        self.0
            .sign_message_sync(prehash.as_slice())
            .map_err(SigningError::from)
    }
}

/// A signer bound to the batch whose owner it is.
#[derive(Debug, Clone)]
pub struct BatchSigner<Sg, S: SwarmSpec = Mainnet> {
    signer: Sg,
    batch: Batch<S>,
}

impl<Sg: SignPrehash, S: SwarmSpec> BatchSigner<Sg, S> {
    /// Binds `signer` to `batch`.
    ///
    /// # Errors
    ///
    /// [`IssuerError::NotBatchOwner`] when the signer is not the batch owner.
    pub fn bind(signer: Sg, batch: Batch<S>) -> Result<Self, IssuerError> {
        let address = signer.address();
        if address != batch.owner() {
            return Err(IssuerError::NotBatchOwner {
                owner: batch.owner(),
                signer: address,
            });
        }
        Ok(Self { signer, batch })
    }
}

#[cfg(feature = "std")]
impl<K: SignerSync + Signer, S: SwarmSpec> BatchSigner<Eip191<K>, S> {
    /// [`bind`](Self::bind) over the [`Eip191`] adapter, so a synchronous
    /// signer plugs in directly.
    ///
    /// # Errors
    ///
    /// [`IssuerError::NotBatchOwner`] when the signer is not the batch owner.
    pub fn from_signer(signer: K, batch: Batch<S>) -> Result<Self, IssuerError> {
        Self::bind(Eip191::new(signer), batch)
    }
}

impl<Sg, S: SwarmSpec> sealed::Sealed for BatchSigner<Sg, S> {}

impl<Sg: SignPrehash, S: SwarmSpec> SignPrehash for BatchSigner<Sg, S> {
    fn address(&self) -> Address {
        self.signer.address()
    }

    fn sign_prehash(&self, prehash: &B256) -> Result<Signature, SigningError> {
        self.signer.sign_prehash(prehash)
    }
}

impl<Sg: SignPrehash, S: SwarmSpec> BoundSigner for BatchSigner<Sg, S> {
    type Spec = S;

    fn batch(&self) -> &Batch<S> {
        &self.batch
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

#[cfg(all(test, feature = "std"))]
mod tests {
    use super::*;
    use crate::testing::{batch_owned_by, key};
    use alloy_signer_local::PrivateKeySigner;
    use nectar_postage::BatchId;

    #[test]
    fn bind_accepts_the_batch_owner() {
        let signer = key(1);
        let batch = batch_owned_by(signer.address(), BatchId::ZERO);

        let bound = BatchSigner::from_signer(signer.clone(), batch.clone()).unwrap();
        assert_eq!(bound.address(), signer.address());
        assert_eq!(bound.batch(), &batch);
    }

    #[test]
    fn bind_refuses_a_signer_that_does_not_own_the_batch() {
        let owner = key(1).address();
        let stranger = key(2);
        let batch = batch_owned_by(owner, BatchId::ZERO);

        let refused = BatchSigner::from_signer(stranger.clone(), batch).unwrap_err();
        let IssuerError::NotBatchOwner {
            owner: expected,
            signer,
        } = refused
        else {
            panic!("the owner check must refuse a stranger");
        };
        assert_eq!(expected, owner);
        assert_eq!(signer, stranger.address());
    }

    #[test]
    fn the_adapter_reports_the_wrapped_key() {
        let signer = PrivateKeySigner::random();
        let address = signer.address();

        assert_eq!(Eip191::new(signer).address(), address);
    }
}
