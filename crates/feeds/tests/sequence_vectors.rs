//! Cross-client conformance vectors for the sequence scheme.
//!
//! Three layers: the single-owner chunk anchor from the reference client's
//! test vectors, feed-level id and address derivations for a fixed identity,
//! and a reference-captured full signed update pinning the exact wire bytes.
//! A feed update must remain a plain single-owner chunk
//! (`address = keccak256(id || owner)`) on the wire.
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        clippy::panic
    )
)]

use alloy_primitives::{B256, Signature, address, b256, hex};
use alloy_signer_local::PrivateKeySigner;
use bytes::Bytes;
use nectar_feeds::{Feed, Getter, Sequence, Topic};
use nectar_primitives::chunk::{ChunkAddress, ChunkOps, SocId};
use nectar_primitives::{
    Chunk, DEFAULT_BODY_SIZE, DefaultContentChunk, DefaultMemoryStore, DefaultSingleOwnerChunk,
};

/// The single-owner chunk anchor: id of all zeroes, payload `"foo"`, and the
/// known-good signature from the reference client's vectors. Pins the SOC
/// address and the wrapped content-addressed body address.
#[test]
fn reference_soc_anchor() {
    let payload = b"foo".to_vec();
    let sig = hex!(
        "5acd384febc133b7b245e5ddc62d82d2cded9182d2716126cd8844509af65a05\
         3deb418208027f548e3e88343af6f84a8772fb3cebc0a1833a0ea7ec0c134831\
         1b"
    );
    let signature = Signature::try_from(sig.as_slice()).unwrap();

    let soc =
        DefaultSingleOwnerChunk::with_signature(SocId::ZERO, signature, payload.clone()).unwrap();

    let expected_soc_address =
        b256!("9d453ebb73b2fedaaf44ceddcf7a0aa37f3e3d6453fea5841c31f0ea6d61dc85");
    assert_eq!(soc.address().as_ref(), expected_soc_address.as_slice());

    let cac = DefaultContentChunk::new(payload).unwrap();
    let expected_cac_address =
        b256!("2387e8e7d8a48c2a9339c97c1dc3461a9a7aa07e994c5cb8b38fd7c1b3e6ea48");
    assert_eq!(cac.address().as_ref(), expected_cac_address.as_slice());
    assert_eq!(soc.unwrap_cac().address(), cac.address());
}

/// Feed-level vectors: fixed topic and owner, derived update id and address
/// at sequence numbers 0, 1 and 42.
#[test]
fn sequence_feed_vectors() {
    let topic = Topic::from_label("testtopic");
    let owner = address!("654bFE2E030Ff82B8741c7a0BF9eC26Ea523b31C");
    let feed = Feed::<DEFAULT_BODY_SIZE>::new(topic, owner);

    // topic = keccak256("testtopic")
    assert_eq!(
        B256::from(topic),
        b256!("65cf9694019c5d902d773447898b875265abd8c57e6b95e926cf491254e3ad8e"),
    );

    let cases: [(u64, B256, B256); 3] = [
        (
            0,
            b256!("115cba53a31da205fbcbe3e367c7ff9e6ed5a3bf602d34c4204cc0352c68a817"),
            b256!("d8020d494642ee61d705dab9b68a050a6fed29c25b3924986c02ff1c5f7a0241"),
        ),
        (
            1,
            b256!("5e756defc3f5631327eed03099dcf3e6532292483465ca8903a3c528ebd465d3"),
            b256!("83776e6c2310c4036f4f6e9f10a67c84b574b7e4fd19329f3608e2d11cbc04ed"),
        ),
        (
            42,
            b256!("59ffcd1f278b875f7352fc3cd6e9048ae22da2d8c05a36f3cd941ff9ce6fc230"),
            b256!("f26d4798f6eb23493cfc7c5b453805de421f578e518445d244270ecf10c7fae3"),
        ),
    ];

    for (n, expected_id, expected_address) in cases {
        let index = Sequence::new(n);
        assert_eq!(
            feed.update_id(&index),
            SocId::from(expected_id),
            "id at {n}"
        );
        assert_eq!(
            feed.update_address(&index),
            ChunkAddress::from(expected_address),
            "address at {n}",
        );
    }
}

/// Reference-captured fixture for a full signed sequence update: topic
/// `keccak256("testtopic")`, a fixed secp256k1 key, sequence index 0 and
/// payload `"data"`. Re-signing reproduces the reference signature byte for
/// byte, and the serialized chunk (`id || signature || span || payload`)
/// matches the captured wire bytes exactly.
#[test]
fn reference_sequence_update_vector() {
    let key = hex!("2c7536e3605d9c16a7a3d7b1898e529396a65c23a3bcbd4012a11cf2731b0fbc");
    let signer = PrivateKeySigner::from_slice(&key).unwrap();
    let owner = signer.address();
    assert_eq!(owner, address!("654bFE2E030Ff82B8741c7a0BF9eC26Ea523b31C"));

    let feed = Feed::<DEFAULT_BODY_SIZE>::new(Topic::from_label("testtopic"), owner);
    let index = Sequence::ZERO;
    let expected_id = SocId::from(b256!(
        "115cba53a31da205fbcbe3e367c7ff9e6ed5a3bf602d34c4204cc0352c68a817"
    ));
    assert_eq!(feed.update_id(&index), expected_id);

    // Deterministic (RFC 6979) signature over keccak256(id || body_hash).
    let expected_sig = hex!(
        "22027e843e0f65f3f1f061be80f2c4fc1ae7cc2e11a8260d44068b6ecb753cd6\
         7408358be2dca9d0ed020cdc7ad1598aeef1636a0452be4386981692ca930811\
         1b"
    );
    let expected_sig = Signature::try_from(expected_sig.as_slice()).unwrap();

    let expected_soc_address =
        b256!("d8020d494642ee61d705dab9b68a050a6fed29c25b3924986c02ff1c5f7a0241");
    let expected_chunk = hex!(
        "115cba53a31da205fbcbe3e367c7ff9e6ed5a3bf602d34c4204cc0352c68a817\
         22027e843e0f65f3f1f061be80f2c4fc1ae7cc2e11a8260d44068b6ecb753cd6\
         7408358be2dca9d0ed020cdc7ad1598aeef1636a0452be4386981692ca930811\
         1b040000000000000064617461"
    );

    let soc =
        DefaultSingleOwnerChunk::new(expected_id, b"data".to_vec(), &signer).expect("sign update");

    assert_eq!(soc.signature(), &expected_sig);
    assert_eq!(soc.address().as_ref(), expected_soc_address.as_slice());
    assert_eq!(soc.owner().unwrap(), owner);
    assert_eq!(soc.address(), &feed.update_address(&index));

    let chunk_bytes: Bytes = soc.into();
    assert_eq!(chunk_bytes.as_ref(), expected_chunk.as_slice());
}

/// The public read path reproduces the reference vector: a store holding the
/// captured chunk serves it back through the getter with the payload intact.
#[test]
fn getter_reads_reference_vector() {
    let key = hex!("2c7536e3605d9c16a7a3d7b1898e529396a65c23a3bcbd4012a11cf2731b0fbc");
    let signer = PrivateKeySigner::from_slice(&key).unwrap();
    let feed = Feed::<DEFAULT_BODY_SIZE>::new(Topic::from_label("testtopic"), signer.address());

    let soc =
        DefaultSingleOwnerChunk::new(feed.update_id(&Sequence::ZERO), b"data".to_vec(), &signer)
            .unwrap();
    let store = DefaultMemoryStore::from_chunks([Chunk::from_envelope(soc.into()).unwrap()]);

    let update = nectar_testing::run(Getter::new(feed, &store).at(Sequence::ZERO)).unwrap();
    assert_eq!(update.payload().as_ref(), b"data");
    assert_eq!(
        update.address().as_ref(),
        b256!("d8020d494642ee61d705dab9b68a050a6fed29c25b3924986c02ff1c5f7a0241").as_slice(),
    );
}
