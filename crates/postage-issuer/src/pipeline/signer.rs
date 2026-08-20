//! Sealed prehash signing: the pipeline's only signer seam.

use alloc::vec::Vec;

use alloy_primitives::{Address, B256, Signature};
#[cfg(feature = "std")]
use alloy_signer::{Signer, SignerSync};
#[cfg(feature = "sign-parallel")]
use rayon::prelude::*;

use crate::error::{IssuerError, SigningError};
use crate::issuer::StampIssuer;
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

    /// Signs an admission batch, one result per prehash in input order.
    fn sign_prehashes(&self, prehashes: &[B256]) -> Vec<Result<Signature, SigningError>> {
        prehashes
            .iter()
            .map(|prehash| self.sign_prehash(prehash))
            .collect()
    }
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
pub struct Eip191<S> {
    signer: S,
    address: Address,
}

#[cfg(feature = "std")]
impl<S: Signer> Eip191<S> {
    /// Wraps `signer`, reading its address once.
    pub fn new(signer: S) -> Self {
        let address = signer.address();
        Self { signer, address }
    }
}

impl<S> Eip191<S> {
    /// Wraps `signer` at a stated address, for a signer that reports none of
    /// its own, such as a remote signer behind a service.
    pub const fn with_address(signer: S, address: Address) -> Self {
        Self { signer, address }
    }

    /// Returns the wrapped signer.
    pub fn into_inner(self) -> S {
        self.signer
    }
}

impl<S> sealed::Sealed for Eip191<S> {}

#[cfg(feature = "std")]
fn eip191_sign<S: SignerSync>(signer: &S, prehash: &B256) -> Result<Signature, SigningError> {
    signer
        .sign_message_sync(prehash.as_slice())
        .map_err(SigningError::from)
}

#[cfg(all(feature = "std", not(feature = "sign-parallel")))]
impl<S: SignerSync> SignPrehash for Eip191<S> {
    fn address(&self) -> Address {
        self.address
    }

    fn sign_prehash(&self, prehash: &B256) -> Result<Signature, SigningError> {
        eip191_sign(&self.signer, prehash)
    }
}

#[cfg(feature = "sign-parallel")]
impl<S: SignerSync + Sync> SignPrehash for Eip191<S> {
    fn address(&self) -> Address {
        self.address
    }
    fn sign_prehash(&self, prehash: &B256) -> Result<Signature, SigningError> {
        eip191_sign(&self.signer, prehash)
    }

    fn sign_prehashes(&self, prehashes: &[B256]) -> Vec<Result<Signature, SigningError>> {
        prehashes
            .par_iter()
            .map(|prehash| eip191_sign(&self.signer, prehash))
            .collect()
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
impl<K: SignerSync + Signer + Sync, S: SwarmSpec> BatchSigner<Eip191<K>, S> {
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

/// Refuses a signer whose batch is not the one `issuer` allocates from.
pub(crate) fn allocates_from<I, Sg>(issuer: &I, signer: &Sg) -> Result<(), IssuerError>
where
    I: StampIssuer + ?Sized,
    Sg: BoundSigner<Spec = I::Spec>,
{
    if issuer.batch_id() != signer.batch().id() {
        return Err(IssuerError::BatchMismatch {
            issuer: issuer.batch_id(),
            signer: signer.batch().id(),
        });
    }
    if issuer.bucket_depth() != signer.batch().bucket_depth().get() {
        return Err(IssuerError::BucketDepthMismatch {
            issuer: issuer.bucket_depth(),
            batch: signer.batch().bucket_depth().get(),
        });
    }
    Ok(())
}

const fn seal(digest: &StampDigest, signature: Signature) -> Stamp {
    Stamp::with_index(digest.batch_id, digest.index, digest.timestamp, signature)
}

/// Signs an allocated digest into a wire-valid stamp.
pub(crate) fn sign_digest<Sg>(signer: &Sg, digest: &StampDigest) -> Result<Stamp, SigningError>
where
    Sg: SignPrehash + ?Sized,
{
    let prehash = digest.to_prehash();
    Ok(seal(digest, signer.sign_prehash(&prehash)?))
}

/// Signs a whole admission batch, one result per digest in input order.
#[cfg(feature = "std")]
pub(crate) fn sign_digests<Sg>(
    signer: &Sg,
    digests: &[StampDigest],
) -> Vec<Result<Stamp, SigningError>>
where
    Sg: SignPrehash + ?Sized,
{
    let prehashes: Vec<B256> = digests.iter().map(StampDigest::to_prehash).collect();
    let mut signatures = signer.sign_prehashes(&prehashes).into_iter();
    digests
        .iter()
        .map(|digest| match signatures.next() {
            Some(Ok(signature)) => Ok(seal(digest, signature)),
            Some(Err(error)) => Err(error),
            // A short reply must still leave one result per digest.
            None => Err(SigningError::Dropped),
        })
        .collect()
}

#[cfg(all(test, feature = "std"))]
mod tests {
    use super::*;
    use crate::testing::{batch_owned_by, key};
    use alloc::sync::Arc;
    use alloy_primitives::U256;
    use alloy_signer_local::PrivateKeySigner;
    use core::sync::atomic::{AtomicUsize, Ordering};
    use nectar_postage::BatchId;
    use std::time::Duration;

    fn prehashes(n: usize) -> Vec<B256> {
        (0..n).map(|_| B256::random()).collect()
    }

    /// Tracks the highest number of concurrent signing calls.
    struct Gauge {
        current: AtomicUsize,
        max: Arc<AtomicUsize>,
    }

    impl SignerSync for Gauge {
        fn sign_hash_sync(&self, _hash: &B256) -> Result<Signature, alloy_signer::Error> {
            self.sign_message_sync(&[])
        }

        fn sign_message_sync(&self, _message: &[u8]) -> Result<Signature, alloy_signer::Error> {
            let now = self.current.fetch_add(1, Ordering::SeqCst) + 1;
            self.max.fetch_max(now, Ordering::SeqCst);
            std::thread::sleep(Duration::from_millis(1));
            self.current.fetch_sub(1, Ordering::SeqCst);
            Ok(Signature::new(U256::from(1), U256::from(2), false))
        }

        fn chain_id_sync(&self) -> Option<u64> {
            None
        }
    }

    fn peak_concurrency(batch: usize) -> usize {
        let max = Arc::new(AtomicUsize::new(0));
        let signer = Eip191::with_address(
            Gauge {
                current: AtomicUsize::new(0),
                max: Arc::clone(&max),
            },
            Address::ZERO,
        );

        let signed = signer.sign_prehashes(&prehashes(batch));

        assert_eq!(signed.len(), batch);
        max.load(Ordering::SeqCst)
    }

    /// Two proves overlap without demanding the whole batch on few-core CI.
    #[cfg(feature = "sign-parallel")]
    #[test]
    fn the_parallel_body_overlaps_a_batch() {
        assert!(peak_concurrency(64) >= 2, "the batch signed serially");
    }

    #[cfg(not(feature = "sign-parallel"))]
    #[test]
    fn the_serial_default_signs_one_at_a_time() {
        assert_eq!(peak_concurrency(64), 1);
    }

    /// ECDSA here is deterministic, so item-for-item equality is exact.
    #[test]
    fn the_batched_body_matches_the_serial_loop() {
        let signer = Eip191::new(PrivateKeySigner::random());
        let prehashes = prehashes(32);

        let batched: Vec<_> = signer
            .sign_prehashes(&prehashes)
            .into_iter()
            .map(Result::unwrap)
            .collect();
        let serial: Vec<_> = prehashes
            .iter()
            .map(|prehash| signer.sign_prehash(prehash).unwrap())
            .collect();

        assert_eq!(batched, serial);
    }

    #[test]
    fn every_batched_signature_recovers_to_the_signer() {
        let key = PrivateKeySigner::random();
        let address = key.address();
        let signer = Eip191::new(key);
        let prehashes = prehashes(16);

        let signatures = signer.sign_prehashes(&prehashes);

        assert_eq!(signatures.len(), prehashes.len());
        for (prehash, signature) in prehashes.iter().zip(signatures) {
            let recovered = signature
                .unwrap()
                .recover_address_from_msg(prehash.as_slice())
                .unwrap();
            assert_eq!(recovered, address);
        }
    }

    #[test]
    fn an_empty_batch_signs_nothing() {
        let signer = Eip191::new(PrivateKeySigner::random());

        assert!(signer.sign_prehashes(&[]).is_empty());
    }

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
