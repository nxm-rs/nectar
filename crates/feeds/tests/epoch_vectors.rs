//! Cross-client conformance vectors and end-to-end behaviour for the epoch
//! scheme, exercised through the public reader and writer surface.
//!
//! Regression vectors: for a fixed topic and owner, the marshalled index bytes,
//! derived update id, and update address for two epochs on the grid. The
//! marshalling is `keccak256(start_be8 || level)` and the id/address derivation
//! is `keccak256(topic || marshal) -> keccak256(id || owner)`, pinning our epoch
//! byte layout against the Swarm reference. The finder-behaviour tests live
//! next to the finder code in `src/epoch.rs`, which can reach the crate-private
//! grid walks directly; this file covers the public entry points.

#![cfg(feature = "epoch")]

use alloy_primitives::{B256, address, b256};
use alloy_signer_local::PrivateKeySigner;
use nectar_feeds::{Epoch, EpochCursor, Feed, FeedReader, FeedWriter, MemoryStore, Topic};
use nectar_primitives::DEFAULT_BODY_SIZE;

/// A distinct payload per update index in the roundtrip.
fn payload_for(i: usize) -> Vec<u8> {
    format!("epoch-update-{i}").into_bytes()
}

/// Fixed-topic, fixed-owner regression vectors for two epochs on the grid:
/// the root epoch `{0, 32}` and a level-1 epoch `{2, 1}`.
#[test]
fn epoch_feed_vectors() {
    let topic = Topic::from_bytes(b"testtopic");
    let owner = address!("8d3766440f0d7b949a5e32995d09619a7f86e632");
    let feed = Feed::<DEFAULT_BODY_SIZE>::new(topic, owner);

    // topic = keccak256("testtopic"), shared with the sequence vectors.
    assert_eq!(
        topic.as_bytes(),
        &b256!("65cf9694019c5d902d773447898b875265abd8c57e6b95e926cf491254e3ad8e"),
    );

    // (epoch, expected marshal, expected id, expected address).
    let cases: [(Epoch, B256, B256, B256); 2] = [
        (
            Epoch::new(0, 32),
            b256!("2a28926b78c626adb7a249a23e9769bf038b52447e60543086a797f9b3061c70"),
            b256!("95ad0ab574b50fe560d9c09d75476c6c339b6edb477caacb151a2e58832dca54"),
            b256!("aade3ad8ffce03aa9432d0461462759f6722e896c3a9913c432fae966a7137f1"),
        ),
        (
            Epoch::new(2, 1),
            b256!("f87baae5dd89c9a2f298888318d026514532f41501b2f9e5f1642cb10dd717b8"),
            b256!("fb08ab90ceb2f53c608740502ded3304a4738a215c0a5f2fcdeadd8ea169e897"),
            b256!("1565091350e47d86c5a84c2d9105d4d46e43cafc50732471b0075adf22242edd"),
        ),
    ];

    for (epoch, expected_marshal, expected_id, expected_address) in cases {
        use nectar_feeds::Index;
        assert_eq!(
            epoch.marshal().as_ref(),
            expected_marshal.as_slice(),
            "marshal for {epoch:?}",
        );
        assert_eq!(feed.update_id(&epoch), expected_id, "id for {epoch:?}");
        assert_eq!(
            feed.update_address(&epoch).0,
            expected_address,
            "address for {epoch:?}",
        );
    }

    // The root epoch is the scheme's first index.
    assert_eq!(<Epoch as nectar_feeds::Index>::first(), Epoch::ROOT);
    assert_eq!(Epoch::ROOT, Epoch::new(0, 32));
}

/// Grid helper checks for the epoch grid math.
#[test]
fn epoch_grid_math() {
    // length = 2^level, comparison timestamp = 2^level * start.
    assert_eq!(Epoch::new(0, 32).length(), 1u64 << 32);
    assert_eq!(Epoch::new(2, 1).length(), 2);
    assert_eq!(Epoch::new(2, 1).timestamp(), 4);

    // child_at picks the child whose interval contains the target.
    let root = Epoch::ROOT;
    let c = root.child_at(0);
    assert_eq!(c.level, 31);
    assert_eq!(c.start, 0);
    assert!(c.is_left());

    // lca of two equal-ish times climbs only as far as needed.
    let l = Epoch::lca(10, 8);
    assert!(l.level <= 32);

    // lca with no prior update is the root.
    assert_eq!(Epoch::lca(1000, 0), Epoch::ROOT);

    // left sister of a right child sits one length earlier at the same level.
    let right = Epoch::new(2, 1); // start & length(=2) != 0 -> right sister
    assert!(!right.is_left());
    assert_eq!(right.left(), Epoch::new(0, 1));

    // child_at descends one level per call and terminates at level 0 (the finest
    // grid resolution the finders stop at). Walking the full root->leaf chain for
    // a representative target must not underflow the level.
    let mut e = Epoch::ROOT;
    let target = 123_456u64;
    while e.level > 0 {
        e = e.child_at(target);
    }
    assert_eq!(e.level, 0, "descent stops at the finest resolution");
    assert_eq!(e.length(), 1, "a level-0 epoch spans one second");
    assert!(
        e.start <= target && target < e.start + e.length(),
        "the level-0 epoch contains the target",
    );

    // is_left / left round-trip at the grid floor (level 0): a right level-0
    // epoch's left sister is the adjacent second below it.
    let right0 = Epoch::new(target | 1, 0);
    assert!(
        !right0.is_left(),
        "an odd start is a right sister at level 0"
    );
    assert_eq!(right0.left(), Epoch::new((target | 1) - 1, 0));
    assert!(right0.left().is_left(), "the left sister has an even start");

    // parent() climbs toward the root without overflowing the level; the topmost
    // climb reaches the root epoch.
    let mut up = Epoch::new(0, MAX_LEVEL_MINUS_ONE);
    up = up.parent();
    assert_eq!(up.level, 32, "parent of a level-31 epoch is the root level");
}

/// One below the top level, used to drive `parent()` up to the root in tests.
const MAX_LEVEL_MINUS_ONE: u8 = 31;

/// End-to-end epoch roundtrip through the generic [`FeedWriter`].
///
/// Publishing through the writer exercises the epoch write path that the cursor
/// redesign enables: the writer asks the cursor to choose the publish index
/// from each update's timestamp ([`WriteCursor::index_for`]), publishes, and
/// records the placement so the next index descends from it. This is what a
/// single advance-after-write step could not do, and it is the path the
/// pre-redesign writer drove with `unsafe`.
///
/// The roundtrip then proves two things through the generic reader and writer:
///
/// 1. Every update the writer published is retrievable at exactly the index the
///    writer chose ([`FeedReader::get`]), with the payload that was signed.
/// 2. [`FeedReader::find_at`] at a time at or after the last publish resolves to
///    the most recent update. (Start-anchored comparison timestamps mean every
///    update nests at the same `start`, so the version live at the present is
///    always the latest published, which is the property the dedicated finder
///    test pins.)
#[tokio::test]
async fn epoch_roundtrip_through_writer() {
    let signer = PrivateKeySigner::random();
    let owner = signer.address();
    let feed = Feed::<DEFAULT_BODY_SIZE>::new(Topic::from_bytes(b"epoch-roundtrip"), owner);
    let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();

    // A shared store: the writer borrows it to publish, the reader borrows it to
    // resolve, so every publish is visible to the read-back without copying.
    let mut writer = FeedWriter::new(feed, EpochCursor::new(), &store, signer.clone())
        .expect("signer owns the feed");

    let times: [u64; 5] = [5, 9, 17, 100, 1000];
    let mut published = Vec::with_capacity(times.len());

    for (i, &at) in times.iter().enumerate() {
        // The writer chooses the grid index from the update timestamp, then
        // publishes and records it. Capture the chosen index for the read-back.
        let index = writer.next_index(Some(at));
        let payload = payload_for(i);
        let address = writer
            .append_at(Some(at), payload.clone())
            .await
            .expect("append");
        assert_eq!(
            address,
            feed.update_address(&index),
            "published address matches the index the writer chose at {at}",
        );
        published.push((at, index, payload));
    }

    // Each update timestamp descended to a distinct grid epoch.
    for (a, (_, ia, _)) in published.iter().enumerate() {
        for (b, (_, ib, _)) in published.iter().enumerate() {
            if a != b {
                assert_ne!(ia, ib, "epochs {a} and {b} collided");
            }
        }
    }

    let reader: FeedReader<Epoch, &MemoryStore, DEFAULT_BODY_SIZE> = FeedReader::new(feed, &store);

    // Every update the writer published reads back at the exact index it chose,
    // with the payload that was signed over.
    for (at, index, payload) in &published {
        let update = reader
            .get(index)
            .await
            .unwrap_or_else(|e| panic!("read back the update published at {at}: {e}"));
        assert_eq!(update.index(), index, "index roundtrips at {at}");
        assert_eq!(update.payload().as_ref(), payload.as_slice());
        assert_eq!(update.owner().expect("recover owner"), owner);
    }

    // A query at or past the last publish time resolves to the final update.
    let (last_at, last_index, _) = published.last().unwrap();
    for query in [*last_at, last_at * 10, 100_000] {
        let latest = reader
            .find_at(query, None)
            .await
            .expect("find_at latest")
            .expect("a feed with updates has a latest");
        assert_eq!(
            latest.chunk().id(),
            feed.update_id(last_index),
            "find_at at {query} did not resolve the latest update",
        );
    }
}
