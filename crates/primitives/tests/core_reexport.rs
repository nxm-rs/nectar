//! The carve is a re-export, not a fork.
//!
//! Every item `nectar-primitives-core` owns must still resolve at its original
//! `nectar_primitives` path *and* be the very same type, so a consumer that
//! mixes the two crates keeps compiling. A duplicated definition would satisfy
//! the paths but fail the identity bindings below.
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        clippy::as_conversions,
        clippy::panic
    )
)]

use alloy_primitives::{B256, address, b256, hex};
use nectar_primitives::{
    ChunkAddress, ChunkOps, DefaultContentChunk, DefaultSingleOwnerChunk, Hasher, Nonce, Prover,
    XorMetric,
};

/// Same type, not a look-alike: each binding fails to compile if the item is
/// redefined in `nectar-primitives` rather than re-exported.
#[test]
fn core_items_keep_their_identity() {
    let _: ChunkAddress = nectar_primitives_core::ChunkAddress::ZERO;
    let _: nectar_primitives_core::ChunkAddress = ChunkAddress::ZERO;
    let _: Nonce = nectar_primitives_core::Nonce::ZERO;
    let wrong_length = nectar_primitives_core::error::WrongLength {
        expected: 32,
        got: 0,
    };
    let _: nectar_primitives::PrimitivesError = wrong_length.into();
    let _: nectar_primitives::wire::Cursor<'_> = nectar_primitives_core::wire::Cursor::new(&[]);
    let _: nectar_primitives::ChunkTypeId = nectar_primitives_core::ChunkTypeId::CONTENT;

    // The store error the core defines is the one `store` exports.
    let _: nectar_primitives::ChunkStoreError =
        nectar_primitives_core::error::ChunkStoreError::not_found(&ChunkAddress::ZERO);
}

/// The `Default*` aliases are re-exported, not redeclared: the identity
/// function only coerces when both crates name one type.
#[test]
fn default_aliases_keep_their_identity() {
    fn same<T>(x: T) -> T {
        x
    }

    let _: fn(nectar_primitives::DefaultHasher) -> nectar_primitives_core::DefaultHasher = same;
    let _: fn(
        nectar_primitives::DefaultContentChunk,
    ) -> nectar_primitives_core::DefaultContentChunk = same;
    let _: fn(
        nectar_primitives::DefaultSingleOwnerChunk,
    ) -> nectar_primitives_core::DefaultSingleOwnerChunk = same;
    let _: fn(nectar_primitives::DefaultAnyChunk) -> nectar_primitives_core::DefaultAnyChunk = same;
}

/// The content-address path: the address is the BMT root of the body, reached
/// through the re-exported carrier.
#[test]
fn content_address_survives_the_carve() {
    let chunk = DefaultContentChunk::new(b"foo".to_vec()).unwrap();
    let expected = b256!("2387e8e7d8a48c2a9339c97c1dc3461a9a7aa07e994c5cb8b38fd7c1b3e6ea48");

    assert_eq!(chunk.address().as_ref(), expected);
    chunk.verify(&ChunkAddress::from(expected)).unwrap();
}

/// The single-owner path: owner recovery and `keccak256(id || owner)` on the
/// go-interop vector.
#[test]
fn single_owner_recovery_survives_the_carve() {
    let wire = hex!(
        "000000000000000000000000000000000000000000000000000000000000000\
        05acd384febc133b7b245e5ddc62d82d2cded9182d2716126cd8844509af65a05\
        3deb418208027f548e3e88343af6f84a8772fb3cebc0a1833a0ea7ec0c134831\
        1b0300000000000000666f6f"
    );

    let chunk = DefaultSingleOwnerChunk::try_from(wire.as_slice()).unwrap();
    assert_eq!(
        chunk.owner().unwrap(),
        address!("8d3766440f0d7b949a5e32995d09619a7f86e632")
    );
    assert_eq!(
        chunk.address().as_ref(),
        b256!("9d453ebb73b2fedaaf44ceddcf7a0aa37f3e3d6453fea5841c31f0ea6d61dc85")
    );
    chunk.verify(chunk.address()).unwrap();
}

/// The BMT inclusion path: a generated proof verifies against the root.
#[test]
fn bmt_proof_verify_survives_the_carve() {
    let data = b"hello world";
    let mut hasher = Hasher::new();
    hasher.set_span(data.len() as u64);
    hasher.update(data);
    let root = hasher.sum();

    let proof = hasher.generate_proof(data, 0).unwrap();
    assert!(proof.verify(&root).unwrap());
    assert!(!proof.verify(&B256::ZERO).unwrap());
}

/// Routing math stays in `nectar-primitives` but keeps applying to the core's
/// address type: the impl moved crates, the behaviour did not.
#[test]
fn chunk_address_still_carries_the_xor_metric() {
    let a = ChunkAddress::new([0x00; 32]);
    let b = ChunkAddress::new([0x01; 32]);

    assert_eq!(a.point(), &[0x00u8; 32]);
    assert_eq!(u8::from(a.proximity(&b)), 7);
    assert!(a.closer(&a, &b));
}

/// The typestate currency and its registry travel together: parse then verify
/// still lands a `Verified` chunk through the re-exported path.
#[test]
fn typestate_currency_survives_the_carve() {
    use nectar_primitives::{Chunk, ChunkRegistry, StandardChunkSet, Unverified};

    let chunk = DefaultContentChunk::new(b"typestate".to_vec()).unwrap();
    let claimed = *chunk.address();
    let typed = StandardChunkSet::encode_typed(&chunk.into());

    let verified = Chunk::<Unverified, StandardChunkSet>::parse(claimed, &typed)
        .unwrap()
        .verify()
        .unwrap();
    assert_eq!(verified.address(), &claimed);
}
