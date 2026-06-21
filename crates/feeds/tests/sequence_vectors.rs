//! Cross-client conformance vectors for the sequence feed scheme.
//!
//! Two layers of vectors:
//!
//! 1. Single-owner chunk anchors from the Swarm SOC test vectors, pinning the
//!    SOC address and inner content-addressed chunk address for a known id,
//!    payload, and signature. These tie our chunk substrate to the Swarm wire
//!    spec byte for byte.
//! 2. Feed-level vectors: for a fixed topic and owner, the derived update id and
//!    update address at several sequence numbers. These pin our id and address
//!    derivation (`keccak256(topic || index) -> keccak256(id || owner)`).
//!
//! The feed-level vectors below were computed by this implementation and are
//! the shared cross-client baseline for the sequence scheme.

use alloy_primitives::{B256, Signature, address, b256, hex};
use alloy_signer_local::PrivateKeySigner;
use bytes::Bytes;
use nectar_feeds::{Feed, Sequence, Topic};
use nectar_primitives::chunk::{Chunk, SingleOwnerChunk};
use nectar_primitives::{DEFAULT_BODY_SIZE, DefaultContentChunk, DefaultSingleOwnerChunk};

/// The SOC test anchor: id of all zeroes, payload `"foo"`, and the known good
/// signature from the Swarm SOC vectors. The SOC address and the inner CAC
/// address are pinned.
#[test]
fn reference_soc_anchor() {
    let id = B256::ZERO;
    let payload = b"foo".to_vec();
    let sig = hex!(
        "5acd384febc133b7b245e5ddc62d82d2cded9182d2716126cd8844509af65a05\
         3deb418208027f548e3e88343af6f84a8772fb3cebc0a1833a0ea7ec0c134831\
         1b"
    );
    assert_eq!(sig.len(), 65, "reference signature is 65 bytes");
    let signature = Signature::try_from(sig.as_slice()).unwrap();

    let soc = DefaultSingleOwnerChunk::with_signature(id, signature, payload.clone()).unwrap();

    // SOC address = keccak256(id || owner).
    let expected_soc_address =
        b256!("9d453ebb73b2fedaaf44ceddcf7a0aa37f3e3d6453fea5841c31f0ea6d61dc85");
    assert_eq!(soc.address().as_ref(), &expected_soc_address);

    // Inner content-addressed chunk address (the wrapped CAC for `"foo"`).
    let cac = DefaultContentChunk::new(payload).unwrap();
    let expected_cac_address =
        b256!("2387e8e7d8a48c2a9339c97c1dc3461a9a7aa07e994c5cb8b38fd7c1b3e6ea48");
    assert_eq!(cac.address().as_ref(), &expected_cac_address);
}

/// Feed-level regression vectors: fixed topic and owner, derived update id and
/// address at sequence numbers 0, 1, and 42.
///
/// The owner is the address of the key used in `reference_sequence_update_vector`
/// so every feed-level vector in this file shares one identity; the sequence-0
/// address therefore matches the bee-captured chunk address there.
#[test]
fn sequence_feed_vectors() {
    let topic = Topic::from_bytes(b"testtopic");
    let owner = address!("654bFE2E030Ff82B8741c7a0BF9eC26Ea523b31C");
    let feed = Feed::<DEFAULT_BODY_SIZE>::new(topic, owner);

    // topic = keccak256("testtopic")
    assert_eq!(
        topic.as_bytes(),
        &b256!("65cf9694019c5d902d773447898b875265abd8c57e6b95e926cf491254e3ad8e"),
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
        let index = Sequence(n);
        assert_eq!(feed.update_id(&index), expected_id, "id for seq {n}");
        assert_eq!(
            feed.update_address(&index).0,
            expected_address,
            "address for seq {n}",
        );
    }
}

/// Canonical Swarm-reference fixture for a full signed sequence update.
///
/// Captured from the Go reference for a fixed input: topic
/// `keccak256("testtopic")`, the fixed secp256k1 key whose address is
/// `0x654bFE2E030Ff82B8741c7a0BF9eC26Ea523b31C`, sequence index 0, and payload
/// `"data"`. It anchors the full update path end to end: the
/// `topic || index || owner` derivation, the single-owner chunk id, the
/// deterministic (RFC 6979) signature over `keccak256(id || wrapped-cac-bmt)`,
/// the single-owner chunk address, and the exact serialized chunk bytes
/// (`id || signature || span || payload`).
///
/// Re-signing the same input in this implementation reproduces the reference
/// signature byte for byte, proving the signing preimage and curve parameters
/// agree with the reference.
#[test]
fn reference_sequence_update_vector() {
    // Fixed secp256k1 private key from the Swarm reference SOC test vectors.
    let key = hex!("2c7536e3605d9c16a7a3d7b1898e529396a65c23a3bcbd4012a11cf2731b0fbc");
    let signer = PrivateKeySigner::from_slice(&key).expect("decode key");
    let owner = signer.address();
    assert_eq!(
        owner,
        address!("654bFE2E030Ff82B8741c7a0BF9eC26Ea523b31C"),
        "owner derived from the fixed key",
    );

    let topic = Topic::from_bytes(b"testtopic");
    assert_eq!(
        topic.as_bytes(),
        &b256!("65cf9694019c5d902d773447898b875265abd8c57e6b95e926cf491254e3ad8e"),
    );
    let feed = Feed::<DEFAULT_BODY_SIZE>::new(topic, owner);

    // Sequence index 0: id = keccak256(topic || 8 big-endian zero bytes).
    let index = Sequence(0);
    let expected_id = b256!("115cba53a31da205fbcbe3e367c7ff9e6ed5a3bf602d34c4204cc0352c68a817");
    assert_eq!(feed.update_id(&index), expected_id, "reference feed id");

    // Reference signature over keccak256(id || wrapped-cac-bmt) for payload "data".
    let expected_sig = hex!(
        "22027e843e0f65f3f1f061be80f2c4fc1ae7cc2e11a8260d44068b6ecb753cd6\
         7408358be2dca9d0ed020cdc7ad1598aeef1636a0452be4386981692ca930811\
         1b"
    );
    let expected_sig = Signature::try_from(expected_sig.as_slice()).expect("decode sig");

    // Reference single-owner chunk address and full serialized chunk bytes.
    let expected_soc_address =
        b256!("d8020d494642ee61d705dab9b68a050a6fed29c25b3924986c02ff1c5f7a0241");
    let expected_chunk = hex!(
        "115cba53a31da205fbcbe3e367c7ff9e6ed5a3bf602d34c4204cc0352c68a817\
         22027e843e0f65f3f1f061be80f2c4fc1ae7cc2e11a8260d44068b6ecb753cd6\
         7408358be2dca9d0ed020cdc7ad1598aeef1636a0452be4386981692ca930811\
         1b040000000000000064617461"
    );

    // Build the same update in this implementation and re-sign deterministically.
    let soc = SingleOwnerChunk::<DEFAULT_BODY_SIZE>::new(expected_id, b"data".to_vec(), &signer)
        .expect("sign update");

    assert_eq!(
        soc.signature(),
        &expected_sig,
        "re-signed signature matches the reference byte for byte",
    );
    assert_eq!(
        soc.address().as_ref(),
        &expected_soc_address,
        "single-owner chunk address matches the reference",
    );
    assert_eq!(
        soc.owner().expect("recover owner"),
        owner,
        "recovered owner matches the feed owner",
    );
    assert_eq!(
        soc.address(),
        &feed.update_address(&index),
        "reference address matches the feed's derived update address",
    );

    let chunk_bytes: Bytes = soc.into();
    assert_eq!(
        chunk_bytes.as_ref(),
        expected_chunk.as_slice(),
        "serialized chunk bytes match the reference",
    );
}
