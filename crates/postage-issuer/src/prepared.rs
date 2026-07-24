//! Serial-prepare / parallel-sign helpers for any [`StampIssuer`].
//!
//! Index allocation needs `&mut` and stays serial; only the signing fans out
//! across threads. [`ShardedIssuerFor`](crate::ShardedIssuerFor) additionally
//! has a fully concurrent allocation path in
//! [`sign_stamps_parallel`](crate::sign_stamps_parallel).

use alloy_primitives::B256;
use alloy_signer::Signature;
use rayon::prelude::*;

use crate::error::SigningError;
use crate::issuer::StampIssuer;
use crate::sharded::StampResult;
use nectar_postage::{Stamp, StampDigest, StampError, current_timestamp};
use nectar_primitives::ChunkAddress;

/// A prepared stamp: the allocated digest for an address, or the allocation
/// failure.
#[derive(Debug, Clone)]
pub struct StampPreparation {
    /// The chunk address the preparation is for.
    pub address: ChunkAddress,
    /// The allocated digest, or why allocation failed.
    pub result: Result<StampDigest, StampError>,
}

/// Allocates a digest per address from any issuer, in input order.
///
/// Sign the output with [`sign_prepared_parallel`], or with a custom strategy
/// via [`StampDigest::to_prehash`] and
/// [`BatchStamper::stamp_from_signature`](crate::BatchStamper::stamp_from_signature).
pub fn prepare_stamps<I>(issuer: &mut I, addresses: &[ChunkAddress]) -> Vec<StampPreparation>
where
    I: StampIssuer + ?Sized,
{
    addresses
        .iter()
        .map(|address| StampPreparation {
            address: *address,
            result: issuer.prepare_stamp(address, current_timestamp()),
        })
        .collect()
}

/// Signs prepared digests in parallel, yielding results in input order.
///
/// The signer receives the 32-byte prehash and must sign it as an EIP-191
/// personal message. A preparation that failed allocation passes its error
/// through unsigned.
pub fn sign_prepared_parallel<Sg, E>(
    preparations: &[StampPreparation],
    signer: &Sg,
) -> Vec<StampResult>
where
    Sg: Fn(&B256) -> Result<Signature, E> + Sync,
    E: Into<SigningError>,
{
    preparations
        .par_iter()
        .map(|preparation| StampResult {
            address: preparation.address,
            result: sign_preparation(preparation, signer),
        })
        .collect()
}

/// Prepares and signs stamps for the given addresses through any issuer.
///
/// Serial allocation followed by parallel signing; results are in input
/// order.
pub fn stamp_parallel<I, Sg, E>(
    issuer: &mut I,
    signer: &Sg,
    addresses: &[ChunkAddress],
) -> Vec<StampResult>
where
    I: StampIssuer + ?Sized,
    Sg: Fn(&B256) -> Result<Signature, E> + Sync,
    E: Into<SigningError>,
{
    let preparations = prepare_stamps(issuer, addresses);
    sign_prepared_parallel(&preparations, signer)
}

fn sign_preparation<Sg, E>(
    preparation: &StampPreparation,
    signer: &Sg,
) -> Result<Stamp, SigningError>
where
    Sg: Fn(&B256) -> Result<Signature, E>,
    E: Into<SigningError>,
{
    let digest = preparation.result.clone()?;
    let prehash = digest.to_prehash();
    let sig = signer(&prehash).map_err(Into::into)?;
    Ok(Stamp::with_index(
        digest.batch_id,
        digest.index,
        digest.timestamp,
        sig,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MemoryIssuer, RingIssuer};
    use alloy_signer::SignerSync;
    use alloy_signer_local::PrivateKeySigner;
    use nectar_postage::{Batch, BatchId, BucketDepth, StampIndex};

    fn sign_fn(
        signer: &PrivateKeySigner,
    ) -> impl Fn(&B256) -> Result<Signature, SigningError> + Sync + '_ {
        move |prehash| {
            Ok(signer
                .sign_message_sync(prehash.as_slice())
                .map_err(alloy_signer::Error::other)?)
        }
    }

    #[test]
    fn test_stamp_parallel_memory_issuer() {
        let mut issuer = MemoryIssuer::new(BatchId::ZERO, 24, BucketDepth::new(16).unwrap());
        let signer = PrivateKeySigner::random();

        let addresses: Vec<_> = (0..100)
            .map(|_| ChunkAddress::from(B256::random()))
            .collect();

        let results = stamp_parallel(&mut issuer, &sign_fn(&signer), &addresses);

        assert_eq!(results.len(), 100);
        for (result, address) in results.iter().zip(&addresses) {
            assert_eq!(result.address, *address);
            let stamp = result.result.as_ref().unwrap();
            assert_eq!(stamp.batch(), BatchId::ZERO);
        }
        assert_eq!(issuer.stamps_issued(), Some(100));
    }

    #[test]
    fn test_stamp_parallel_signature_recovers() {
        let mut issuer = MemoryIssuer::new(BatchId::ZERO, 20, BucketDepth::new(16).unwrap());
        let signer = PrivateKeySigner::random();

        let address = ChunkAddress::from(B256::random());
        let results = stamp_parallel(&mut issuer, &sign_fn(&signer), &[address]);

        let stamp = results[0].result.as_ref().unwrap();
        let digest = StampDigest::new(
            address,
            stamp.batch(),
            StampIndex::new(stamp.bucket(), stamp.index()),
            stamp.timestamp(),
        );
        let recovered = stamp
            .signature()
            .recover_address_from_msg(digest.to_prehash().as_slice())
            .unwrap();
        assert_eq!(recovered, signer.address());
    }

    #[test]
    fn test_stamp_parallel_bucket_full_passes_through_in_order() {
        // depth=17, bucket_depth=16 gives 2 slots per bucket.
        let mut issuer = MemoryIssuer::new(BatchId::ZERO, 17, BucketDepth::new(16).unwrap());
        let signer = PrivateKeySigner::random();

        let address = ChunkAddress::new([0xAB; 32]);
        let results = stamp_parallel(&mut issuer, &sign_fn(&signer), &[address, address, address]);

        assert!(results[0].result.is_ok());
        assert!(results[1].result.is_ok());
        assert!(matches!(
            results[2].result,
            Err(SigningError::Stamp(StampError::BucketFull { .. }))
        ));
    }

    #[test]
    fn test_two_phase_matches_combined() {
        let mut issuer = MemoryIssuer::new(BatchId::ZERO, 24, BucketDepth::new(16).unwrap());
        let signer = PrivateKeySigner::random();

        let addresses: Vec<_> = (0..10)
            .map(|_| ChunkAddress::from(B256::random()))
            .collect();

        let preparations = prepare_stamps(&mut issuer, &addresses);
        assert_eq!(preparations.len(), addresses.len());
        for (preparation, address) in preparations.iter().zip(&addresses) {
            assert_eq!(preparation.address, *address);
            let digest = preparation.result.as_ref().unwrap();
            assert_eq!(digest.chunk_address, *address);
        }

        let results = sign_prepared_parallel(&preparations, &sign_fn(&signer));
        for (result, preparation) in results.iter().zip(&preparations) {
            let stamp = result.result.as_ref().unwrap();
            let digest = preparation.result.as_ref().unwrap();
            assert_eq!(stamp.stamp_index(), digest.index);
            assert_eq!(stamp.timestamp(), digest.timestamp);
        }
    }

    #[test]
    fn test_stamp_parallel_signer_error_passes_through_in_order() {
        let mut issuer = MemoryIssuer::new(BatchId::ZERO, 24, BucketDepth::new(16).unwrap());

        let addresses: Vec<_> = (0..20)
            .map(|_| ChunkAddress::from(B256::random()))
            .collect();

        // A signer that always fails: allocation succeeds, so every result must
        // carry the signer error through in input order.
        let failing = |_: &B256| -> Result<Signature, SigningError> {
            Err(alloy_signer::Error::message("signer offline").into())
        };
        let results = stamp_parallel(&mut issuer, &failing, &addresses);

        assert_eq!(results.len(), addresses.len());
        for (result, address) in results.iter().zip(&addresses) {
            assert_eq!(result.address, *address);
            assert!(matches!(result.result, Err(SigningError::Signer(_))));
        }
    }

    #[test]
    fn test_stamp_parallel_ring_issuer() {
        // The helper is issuer-generic: a mutable batch's ring issuer works too.
        let mutable = Batch::new(
            BatchId::ZERO,
            0,
            0,
            Default::default(),
            20,
            BucketDepth::new(16).unwrap(),
            false,
        );
        let mut issuer = RingIssuer::external(&mutable).unwrap();
        let signer = PrivateKeySigner::random();

        let addresses: Vec<_> = (0..10)
            .map(|_| ChunkAddress::from(B256::random()))
            .collect();

        let results = stamp_parallel(&mut issuer, &sign_fn(&signer), &addresses);
        assert!(results.iter().all(|r| r.result.is_ok()));
    }
}
