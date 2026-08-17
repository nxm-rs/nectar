//! Sealed prehash signing: the pipeline's only signer seam.

use alloc::vec::Vec;

use alloy_primitives::{B256, Signature};
#[cfg(feature = "std")]
use alloy_signer::SignerSync;
#[cfg(feature = "sign-parallel")]
use rayon::prelude::*;

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

    /// Signs an admission batch, one result per prehash in input order.
    fn sign_prehashes(&self, prehashes: &[B256]) -> Vec<Result<Signature, SigningError>> {
        prehashes
            .iter()
            .map(|prehash| self.sign_prehash(prehash))
            .collect()
    }
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
fn eip191_sign<S: SignerSync>(signer: &S, prehash: &B256) -> Result<Signature, SigningError> {
    signer
        .sign_message_sync(prehash.as_slice())
        .map_err(SigningError::from)
}

#[cfg(all(feature = "std", not(feature = "sign-parallel")))]
impl<S: SignerSync> SignPrehash for Eip191<S> {
    fn sign_prehash(&self, prehash: &B256) -> Result<Signature, SigningError> {
        eip191_sign(&self.0, prehash)
    }
}

#[cfg(feature = "sign-parallel")]
impl<S: SignerSync + Sync> SignPrehash for Eip191<S> {
    fn sign_prehash(&self, prehash: &B256) -> Result<Signature, SigningError> {
        eip191_sign(&self.0, prehash)
    }

    fn sign_prehashes(&self, prehashes: &[B256]) -> Vec<Result<Signature, SigningError>> {
        prehashes
            .par_iter()
            .map(|prehash| eip191_sign(&self.0, prehash))
            .collect()
    }
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
    use alloc::sync::Arc;
    use alloy_primitives::U256;
    use alloy_signer_local::PrivateKeySigner;
    use core::sync::atomic::{AtomicUsize, Ordering};
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
        let signer = Eip191::new(Gauge {
            current: AtomicUsize::new(0),
            max: Arc::clone(&max),
        });

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
}
