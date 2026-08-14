//! Admission micro-batch: serial digest allocation for the pipeline window.

use alloc::vec::Vec;

use crate::issuer::StampIssuer;
use crate::stamper::stamp_timestamp;
use nectar_clock::Clock;
use nectar_postage::{StampDigest, StampError};
use nectar_primitives::ChunkAddress;

/// A prepared stamp: the allocated digest for an address, or the allocation
/// failure.
#[derive(Debug, Clone)]
pub(crate) struct StampPreparation {
    /// The chunk address the preparation is for.
    pub(crate) address: ChunkAddress,
    /// The allocated digest, or why allocation failed.
    pub(crate) result: Result<StampDigest, StampError>,
}

/// Allocates a digest per address from any issuer, in input order, with a
/// single clock read for the whole batch.
pub(crate) fn prepare_stamps<I, C>(
    issuer: &mut I,
    addresses: &[ChunkAddress],
    clock: &C,
) -> Vec<StampPreparation>
where
    I: StampIssuer + ?Sized,
    C: Clock,
{
    let timestamp = stamp_timestamp(clock);
    addresses
        .iter()
        .map(|address| StampPreparation {
            address: *address,
            result: issuer.prepare_stamp(address, timestamp),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MemoryIssuer, RingIssuer};
    use alloy_primitives::B256;
    use nectar_clock::ManualClock;
    use nectar_postage::{Batch, BatchId, BucketDepth};

    #[test]
    fn test_prepare_stamps_allocates_in_input_order() {
        let mut issuer: MemoryIssuer =
            MemoryIssuer::new(BatchId::ZERO, 24, BucketDepth::new(16).unwrap());
        let clock = ManualClock::new(1_234_567_890);

        let addresses: Vec<_> = (0..10)
            .map(|_| ChunkAddress::from(B256::random()))
            .collect();

        let preparations = prepare_stamps(&mut issuer, &addresses, &clock);

        assert_eq!(preparations.len(), addresses.len());
        for (preparation, address) in preparations.iter().zip(&addresses) {
            assert_eq!(preparation.address, *address);
            let digest = preparation.result.as_ref().unwrap();
            assert_eq!(digest.chunk_address, *address);
            assert_eq!(digest.timestamp, 1_234_567_890);
        }
        assert_eq!(issuer.stamps_issued(), Some(10));
    }

    #[test]
    fn test_prepare_stamps_bucket_full_passes_through_in_order() {
        // depth=17, bucket_depth=16 gives 2 slots per bucket.
        let mut issuer: MemoryIssuer =
            MemoryIssuer::new(BatchId::ZERO, 17, BucketDepth::new(16).unwrap());
        let clock = ManualClock::new(0);

        let address = ChunkAddress::new([0xAB; 32]);
        let preparations = prepare_stamps(&mut issuer, &[address, address, address], &clock);

        assert!(preparations[0].result.is_ok());
        assert!(preparations[1].result.is_ok());
        assert!(matches!(
            preparations[2].result,
            Err(StampError::BucketFull { .. })
        ));
        // The refused slot consumed no index.
        assert_eq!(issuer.stamps_issued(), Some(2));
    }

    #[test]
    fn test_prepare_stamps_ring_issuer() {
        // The micro-batch is issuer-generic: a mutable batch's ring issuer
        // works too.
        let mutable: Batch = Batch::new(
            BatchId::ZERO,
            0,
            0,
            Default::default(),
            20,
            BucketDepth::new(16).unwrap(),
            false,
        );
        let mut issuer = RingIssuer::external(&mutable).unwrap();
        let clock = ManualClock::new(0);

        let addresses: Vec<_> = (0..10)
            .map(|_| ChunkAddress::from(B256::random()))
            .collect();

        let preparations = prepare_stamps(&mut issuer, &addresses, &clock);
        assert!(preparations.iter().all(|p| p.result.is_ok()));
    }
}
