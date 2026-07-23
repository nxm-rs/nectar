//! Issuing for a network other than mainnet, end to end.
//!
//! The spec parameter is load-bearing rather than decorative: a deployment that
//! raises the collision-bucket floor above mainnet's refuses a depth mainnet
//! accepts, and an issuer built for it stamps, fills, dilutes, and reserves at
//! that geometry.

// The crate-level `cfg_attr(test, ..)` exemption does not reach a separate test
// binary, and a fixture that unwraps a known-good depth is setup, not shipped
// surface. Nothing else in this file needs an exemption.
#![allow(clippy::unwrap_used)]

use alloy_signer_local::PrivateKeySigner;
use nectar_postage_issuer::{
    Batch, BatchId, BatchStamper, BucketDepth, IssuerError, Mainnet, MemoryIssuerFor, NetworkId,
    Reserved, RingIssuerFor, ShardedIssuerFor, SigningError, StampError, StampIssuer, Stamper,
    SwarmSpec, calculate_bucket,
};
use nectar_primitives::ChunkAddress;

/// A deployment whose collision-bucket floor is deeper than mainnet's 16.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Deep;

impl SwarmSpec for Deep {
    const NETWORK_ID: NetworkId = NetworkId::TESTNET;
    const MIN_BUCKET_DEPTH: u8 = 20;
}

/// The bucket depth `Deep` demands.
fn deep() -> BucketDepth<Deep> {
    BucketDepth::new(20).unwrap()
}

/// A `Deep` batch: 2^20 buckets of 2^(depth - 20) slots each.
fn deep_batch(depth: u8, immutable: bool) -> Batch<Deep> {
    Batch::new(
        BatchId::ZERO,
        0,
        0,
        Default::default(),
        depth,
        deep(),
        immutable,
    )
}

/// An address whose leading 20 bits select `bucket`.
fn address_in(bucket: u32) -> ChunkAddress {
    let mut bytes = [0u8; 32];
    bytes[..4].copy_from_slice(&(bucket << 12).to_be_bytes());
    ChunkAddress::new(bytes)
}

#[test]
fn the_floor_is_the_deployments_own() {
    // Mainnet's floor admits 16; `Deep` refuses it and demands 20.
    assert!(BucketDepth::<Mainnet>::new(16).is_ok());
    assert!(matches!(
        BucketDepth::<Deep>::new(16),
        Err(StampError::BucketDepthBelowMinimum {
            bucket_depth: 16,
            minimum: 20
        })
    ));
    assert_eq!(BucketDepth::<Deep>::new(20).unwrap().get(), 20);
}

#[test]
fn a_deep_issuer_stamps_through_a_batch_stamper() {
    // depth 22 over bucket depth 20 gives 4 slots per bucket.
    let batch = deep_batch(22, true);
    let issuer = MemoryIssuerFor::from_batch(&batch).unwrap();
    assert_eq!(issuer.bucket_depth(), 20);
    assert_eq!(issuer.bucket_count(), 1 << 20);
    assert_eq!(issuer.bucket_capacity(), 4);

    let mut stamper = BatchStamper::new(issuer, PrivateKeySigner::random());
    let address = address_in(0xABCDE);

    for expected in 0..4u32 {
        let stamp = stamper.stamp(&address).unwrap();
        assert_eq!(stamp.bucket(), 0xABCDE);
        assert_eq!(stamp.index(), expected);
    }

    // The fifth stamp exhausts the bucket at this geometry.
    assert!(matches!(
        stamper.stamp(&address),
        Err(SigningError::Stamp(StampError::BucketFull {
            bucket: 0xABCDE,
            capacity: 4
        }))
    ));
    assert_eq!(stamper.issuer().stamps_issued(), Some(4));
}

#[test]
fn a_deep_issuer_dilutes_and_a_deep_sharded_issuer_stamps() {
    let mut issuer = MemoryIssuerFor::<Deep>::new(BatchId::ZERO, 21, deep());
    let address = address_in(1);
    assert_eq!(issuer.prepare_stamp(&address, 1).unwrap().index.index(), 0);
    assert_eq!(issuer.prepare_stamp(&address, 2).unwrap().index.index(), 1);
    assert!(issuer.prepare_stamp(&address, 3).is_err());

    issuer.dilute(22).unwrap();
    assert_eq!(issuer.bucket_capacity(), 4);
    assert_eq!(issuer.prepare_stamp(&address, 4).unwrap().index.index(), 2);
    assert!(matches!(
        issuer.dilute(21),
        Err(IssuerError::DepthDecrease {
            current: 22,
            requested: 21
        })
    ));

    let sharded = ShardedIssuerFor::from_batch(&deep_batch(22, true)).unwrap();
    assert_eq!(sharded.bucket_depth(), 20);
    let digest = sharded.prepare_stamp(&address, 5).unwrap();
    assert_eq!(digest.index.bucket(), calculate_bucket(&address, 20));
    assert_eq!(sharded.stamps_issued(), 1);
}

#[test]
fn a_deep_reserved_ring_never_emits_a_reserved_slot() {
    // depth 22 over bucket depth 20 gives 4 slots per bucket; reserve two.
    let batch = deep_batch(22, false);
    let address = address_in(0x0FEDC);
    let bucket = calculate_bucket(&address, 20);
    let mut ring: RingIssuerFor<Deep, Reserved> =
        RingIssuerFor::reserved(&batch, [(bucket, 1), (bucket, 3)]).unwrap();

    // Far past one wrap, so every wrap is exercised.
    for timestamp in 0..40u64 {
        let digest = ring.prepare_stamp(&address, timestamp).unwrap();
        assert_eq!(digest.index.bucket(), bucket);
        let slot = digest.index.index();
        assert!(slot == 0 || slot == 2, "ring emitted reserved slot {slot}");
    }
}
