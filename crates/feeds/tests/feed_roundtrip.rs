//! End-to-end roundtrip across the feed writer, an in-memory store, and the
//! feed reader.
//!
//! A [`FeedWriter`] signs and publishes a run of sequence updates into a shared
//! [`MemoryStore`]; a [`FeedReader`] over the same store reads each one back,
//! verifying the payload and the owner recovered from the single-owner chunk
//! signature. The reader's latest-update lookup must converge on the last
//! published index, and a foreign-owner chunk returned for a feed address is
//! rejected with [`FeedError::OwnerMismatch`].
//!
//! The writer and reader hold their store by value, so both borrow the same
//! [`MemoryStore`] by shared reference: `&MemoryStore` implements the typed
//! chunk store traits and is `Send + Sync`, so a single store backs both ends
//! without cloning divergent copies.

use alloy_signer_local::PrivateKeySigner;
use bytes::Bytes;
use nectar_feeds::{Feed, FeedError, FeedReader, FeedWriter, Sequence, SequenceCursor, Topic};
use nectar_primitives::DEFAULT_BODY_SIZE;
use nectar_primitives::DefaultContentChunk;
use nectar_primitives::chunk::{AnyChunk, ChunkAddress, SingleOwnerChunk};
use nectar_primitives::store::{ChunkStoreError, MemoryStore, SyncChunkGet};

/// Number of sequence updates published and read back in the roundtrip.
const N: u64 = 12;

fn payload_for(i: u64) -> Bytes {
    Bytes::from(format!("feed-update-{i}").into_bytes())
}

#[tokio::test]
async fn sequence_roundtrip_reads_back_and_converges_to_latest() {
    let signer = PrivateKeySigner::random();
    let owner = signer.address();
    let feed = Feed::<DEFAULT_BODY_SIZE>::new(Topic::from_bytes(b"feed-roundtrip"), owner);
    let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();

    // Writer and reader share one store by borrowing it: `&MemoryStore`
    // implements the typed chunk store traits, so publishes are visible to the
    // reader without copying the store.
    let mut writer = FeedWriter::new(feed, SequenceCursor::new(), &store, signer.clone())
        .expect("signer owns the feed");

    let mut addresses = Vec::with_capacity(N as usize);
    for i in 0..N {
        assert_eq!(writer.next_index(None), Sequence(i));
        let address = writer.append(payload_for(i)).await.expect("append");
        assert_eq!(address, feed.update_address(&Sequence(i)));
        addresses.push(address);
    }
    assert_eq!(writer.next_index(None), Sequence(N));

    let reader: FeedReader<Sequence, &MemoryStore, DEFAULT_BODY_SIZE> =
        FeedReader::new(feed, &store);

    // Each index reads back with its payload and the recovered owner.
    for i in 0..N {
        let update = reader.get(&Sequence(i)).await.expect("read back");
        assert_eq!(update.index(), &Sequence(i));
        assert_eq!(update.payload(), &payload_for(i));
        assert_eq!(update.owner().expect("recover owner"), owner);
        assert_eq!(update.address(), &addresses[i as usize]);
    }

    // The latest lookup converges on the final published index.
    let latest = reader
        .latest(None)
        .await
        .expect("latest lookup")
        .expect("non-empty feed has a latest update");
    assert_eq!(latest.index(), &Sequence(N - 1));
    assert_eq!(latest.payload(), &payload_for(N - 1));

    // Nothing exists strictly past the last index.
    let past_last = reader
        .latest(Some(&Sequence(N - 1)))
        .await
        .expect("latest past last");
    assert!(past_last.is_none(), "no update exists past the final index");
}

/// A store that always returns one planted chunk for any address, modelling a
/// backend that substitutes a foreign-owner chunk for a feed slot. The reader
/// must reject what it gets back on the owner check, not trust the substitution.
struct SubstitutingStore {
    chunk: AnyChunk<DEFAULT_BODY_SIZE>,
}

impl SyncChunkGet<DEFAULT_BODY_SIZE> for SubstitutingStore {
    type Error = ChunkStoreError;

    fn get(&self, _address: &ChunkAddress) -> Result<AnyChunk<DEFAULT_BODY_SIZE>, Self::Error> {
        Ok(self.chunk.clone())
    }
}

#[tokio::test]
async fn foreign_owner_chunk_is_rejected_with_owner_mismatch() {
    let owner_signer = PrivateKeySigner::random();
    let owner = owner_signer.address();
    let feed = Feed::<DEFAULT_BODY_SIZE>::new(Topic::from_bytes(b"feed-owner-check"), owner);

    // A foreign key signs a single-owner chunk over the feed's index-0 id. Its
    // recovered owner is the foreign address, not the feed owner.
    let foreign = PrivateKeySigner::random();
    assert_ne!(
        foreign.address(),
        owner,
        "foreign signer differs from owner"
    );

    let id = feed.update_id(&Sequence(0));
    let foreign_soc =
        SingleOwnerChunk::<DEFAULT_BODY_SIZE>::new(id, Bytes::from_static(b"forged"), &foreign)
            .expect("build foreign soc");
    assert_eq!(
        foreign_soc.owner().expect("recover foreign owner"),
        foreign.address(),
    );

    // A backend hands this foreign chunk back for the feed address. The reader
    // recovers the signer, finds it is not the feed owner, and rejects it.
    let store = SubstitutingStore {
        chunk: foreign_soc.into(),
    };
    let reader: FeedReader<Sequence, SubstitutingStore, DEFAULT_BODY_SIZE> =
        FeedReader::new(feed, store);

    let result = reader.get(&Sequence(0)).await;
    match result {
        Err(FeedError::OwnerMismatch { expected, actual }) => {
            assert_eq!(expected, owner, "expected owner is the feed owner");
            assert_eq!(
                actual,
                foreign.address(),
                "actual owner is the foreign signer"
            );
        }
        other => panic!("foreign-owner chunk must be rejected with OwnerMismatch, got {other:?}"),
    }
}

#[tokio::test]
async fn content_chunk_at_feed_address_is_rejected_as_not_single_owner() {
    let owner_signer = PrivateKeySigner::random();
    let owner = owner_signer.address();
    let feed = Feed::<DEFAULT_BODY_SIZE>::new(Topic::from_bytes(b"feed-cac-check"), owner);

    // A backend returns a content-addressed chunk (not a single-owner chunk) for
    // the feed slot. Feed resolution must reject it on the SOC discrimination,
    // not attempt to treat it as an update.
    let cac = DefaultContentChunk::new(b"not a soc".to_vec()).expect("build cac");
    let store = SubstitutingStore { chunk: cac.into() };
    let reader: FeedReader<Sequence, SubstitutingStore, DEFAULT_BODY_SIZE> =
        FeedReader::new(feed, store);

    match reader.get(&Sequence(0)).await {
        Err(FeedError::NotSingleOwner) => {}
        other => panic!("a content chunk must be rejected with NotSingleOwner, got {other:?}"),
    }
}
