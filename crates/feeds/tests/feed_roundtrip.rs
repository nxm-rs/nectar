//! Write-read round trips, latest-update search and the trust seam through
//! the public API, all over in-memory stores.
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

use alloy_primitives::hex;
use alloy_signer_local::PrivateKeySigner;
use nectar_feeds::{Feed, FeedError, Getter, Index, Sequence, Topic, Updater};
use nectar_primitives::chunk::{ChunkAddress, ChunkOps, TrustedSource, Unverified};
use nectar_primitives::store::{ChunkGet, ChunkHas, ChunkStoreError};
use nectar_primitives::{
    Chunk, ChunkRegistry, DEFAULT_BODY_SIZE, DefaultContentChunk, DefaultMemoryStore,
    StandardChunkSet,
};
use nectar_testing::run;
use proptest::prelude::*;

fn signer() -> PrivateKeySigner {
    let pk = hex!("2c7536e3605d9c16a7a3d7b1898e529396a65c23a3bcbd4012a11cf2731b0fbc");
    PrivateKeySigner::from_slice(&pk).unwrap()
}

fn feed_for(signer: &PrivateKeySigner) -> Feed {
    Feed::new(Topic::from_label("roundtrip"), signer.address())
}

#[test]
fn append_then_read_round_trips() {
    run(async {
        let signer = signer();
        let feed = feed_for(&signer);
        let store = DefaultMemoryStore::new();
        let mut updater = Updater::new(feed, &store, &signer);

        let payloads: [&[u8]; 3] = [b"first", b"second", b"third"];
        for payload in payloads {
            updater.append(payload.to_vec()).await.unwrap();
        }
        assert_eq!(updater.next_index(), Some(Sequence::new(3)));

        let getter = Getter::new(feed, &store);
        for (n, payload) in (0u64..).zip(payloads) {
            let update = getter.at(Sequence::new(n)).await.unwrap();
            assert_eq!(update.payload().as_ref(), payload);
            assert_eq!(update.index(), &Sequence::new(n));
            assert_eq!(update.address(), &feed.update_address(&Sequence::new(n)));
            assert_eq!(update.content().data().as_ref(), payload);
        }
    });
}

#[test]
fn empty_feed_has_no_latest() {
    run(async {
        let signer = signer();
        let getter = Getter::new(feed_for(&signer), DefaultMemoryStore::new());

        for latest in [
            getter.latest().await.unwrap(),
            getter.latest_linear_from(Sequence::ZERO).await.unwrap(),
        ] {
            assert!(latest.update.is_none());
            assert_eq!(latest.next, Some(Sequence::ZERO));
        }
    });
}

#[test]
fn finders_agree_while_the_feed_grows() {
    run(async {
        let signer = signer();
        let feed = feed_for(&signer);
        let store = DefaultMemoryStore::new();
        let mut updater = Updater::new(feed, &store, &signer);
        let getter = Getter::new(feed, &store);

        for n in 0u64..33 {
            updater.append(n.to_be_bytes().to_vec()).await.unwrap();

            let latest = getter.latest().await.unwrap();
            let linear = getter.latest_linear_from(Sequence::ZERO).await.unwrap();

            for found in [&latest, &linear] {
                let update = found.update.as_ref().unwrap();
                assert_eq!(update.index(), &Sequence::new(n));
                assert_eq!(update.payload().as_ref(), n.to_be_bytes());
                assert_eq!(found.next, Some(Sequence::new(n + 1)));
            }
        }
    });
}

#[test]
fn latest_from_respects_the_floor() {
    run(async {
        let signer = signer();
        let feed = feed_for(&signer);
        let store = DefaultMemoryStore::new();
        let mut updater = Updater::new(feed, &store, &signer);
        for n in 0u64..5 {
            updater.append(n.to_be_bytes().to_vec()).await.unwrap();
        }
        let getter = Getter::new(feed, &store);

        let latest = getter.latest_from(Sequence::new(3)).await.unwrap();
        assert_eq!(latest.update.unwrap().index(), &Sequence::new(4));
        assert_eq!(latest.next, Some(Sequence::new(5)));

        // A floor past the head is an empty result carrying the floor back.
        let latest = getter.latest_from(Sequence::new(5)).await.unwrap();
        assert!(latest.update.is_none());
        assert_eq!(latest.next, Some(Sequence::new(5)));
    });
}

#[test]
fn wrong_signer_is_rejected_before_the_write() {
    run(async {
        let signer = signer();
        let other = PrivateKeySigner::from_slice(&[0x42u8; 32]).unwrap();
        let feed = Feed::<DEFAULT_BODY_SIZE>::new(Topic::from_label("roundtrip"), other.address());
        let store = DefaultMemoryStore::new();
        let mut updater = Updater::new(feed, &store, &signer);

        let err = updater.append(b"payload".to_vec()).await.unwrap_err();
        assert!(matches!(err, FeedError::OwnerMismatch { .. }));
        assert!(store.is_empty());
        // The rejected append does not advance the cursor.
        assert_eq!(updater.next_index(), Some(Sequence::ZERO));
    });
}

#[test]
fn sequence_space_exhausts_at_the_top_slot() {
    run(async {
        let signer = signer();
        let feed = feed_for(&signer);
        let store = DefaultMemoryStore::new();
        let mut updater = Updater::resume(feed, &store, &signer, Sequence::MAX);

        let update = updater.append(b"last".to_vec()).await.unwrap();
        assert_eq!(update.index(), &Sequence::MAX);
        assert_eq!(updater.next_index(), None);
        assert!(matches!(
            updater.append(b"over".to_vec()).await.unwrap_err(),
            FeedError::Exhausted
        ));

        // The finder resumed at the top slot reports the space as spent.
        let getter = Getter::new(feed, &store);
        let latest = getter.latest_from(Sequence::MAX).await.unwrap();
        assert_eq!(latest.update.unwrap().index(), &Sequence::MAX);
        assert_eq!(latest.next, None);
        let linear = getter.latest_linear_from(Sequence::MAX).await.unwrap();
        assert_eq!(linear.update.unwrap().index(), &Sequence::MAX);
        assert_eq!(linear.next, None);
    });
}

#[test]
fn missing_update_surfaces_the_store_error() {
    run(async {
        let signer = signer();
        let getter = Getter::new(feed_for(&signer), DefaultMemoryStore::new());
        assert!(matches!(
            getter.at(Sequence::ZERO).await.unwrap_err(),
            FeedError::Store(_)
        ));
    });
}

/// Store double reading back unverified parses of what the inner store holds,
/// exercising the getter's certification path.
struct Unverifying<'a>(&'a DefaultMemoryStore);

impl ChunkGet<StandardChunkSet> for Unverifying<'_> {
    type Trust = Unverified;
    type Error = ChunkStoreError;

    async fn get(
        &self,
        address: &ChunkAddress,
    ) -> Result<Chunk<Unverified, StandardChunkSet>, Self::Error> {
        let chunk = ChunkGet::get(self.0, address).await?;
        Chunk::parse(*address, &chunk.typed_bytes())
            .map_err(|_| ChunkStoreError::not_found(address))
    }
}

impl ChunkHas for Unverifying<'_> {
    async fn has(&self, address: &ChunkAddress) -> bool {
        ChunkHas::has(self.0, address).await
    }
}

#[test]
fn unverified_reads_are_certified() {
    run(async {
        let signer = signer();
        let feed = feed_for(&signer);
        let store = DefaultMemoryStore::new();
        let mut updater = Updater::new(feed, &store, &signer);
        updater.append(b"payload".to_vec()).await.unwrap();

        let getter = Getter::new(feed, Unverifying(&store));
        let update = getter.at(Sequence::ZERO).await.unwrap();
        assert_eq!(update.payload().as_ref(), b"payload");

        let latest = getter.latest().await.unwrap();
        assert_eq!(latest.update.unwrap().index(), &Sequence::ZERO);
    });
}

/// Store that serves one fixed slot's bytes under whatever address is asked
/// for: certification must reject the relabelled chunk.
struct Rebinding<'a> {
    inner: &'a DefaultMemoryStore,
    from: ChunkAddress,
}

impl ChunkGet<StandardChunkSet> for Rebinding<'_> {
    type Trust = Unverified;
    type Error = ChunkStoreError;

    async fn get(
        &self,
        address: &ChunkAddress,
    ) -> Result<Chunk<Unverified, StandardChunkSet>, Self::Error> {
        let chunk = ChunkGet::get(self.inner, &self.from).await?;
        Chunk::parse(*address, &chunk.typed_bytes())
            .map_err(|_| ChunkStoreError::not_found(address))
    }
}

#[test]
fn relabelled_chunk_fails_certification() {
    run(async {
        let signer = signer();
        let feed = feed_for(&signer);
        let store = DefaultMemoryStore::new();
        let mut updater = Updater::new(feed, &store, &signer);
        updater.append(b"payload".to_vec()).await.unwrap();

        let getter = Getter::new(
            feed,
            Rebinding {
                inner: &store,
                from: feed.update_address(&Sequence::ZERO),
            },
        );
        assert!(matches!(
            getter.at(Sequence::new(1)).await.unwrap_err(),
            FeedError::Chunk(_)
        ));
    });
}

/// Trusted store lying about type: a content chunk vouched for at the feed
/// slot must still be rejected as not single-owner.
struct LyingTrusted {
    bytes: Vec<u8>,
    source: TrustedSource,
}

impl ChunkGet<StandardChunkSet> for LyingTrusted {
    type Trust = nectar_primitives::Verified;
    type Error = ChunkStoreError;

    async fn get(
        &self,
        address: &ChunkAddress,
    ) -> Result<Chunk<nectar_primitives::Verified, StandardChunkSet>, Self::Error> {
        let parsed = Chunk::<Unverified, StandardChunkSet>::parse(*address, &self.bytes)
            .map_err(|_| ChunkStoreError::not_found(address))?;
        Ok(parsed.assume_verified(&self.source))
    }
}

#[test]
fn content_chunk_at_a_feed_slot_is_rejected() {
    run(async {
        let signer = signer();
        let feed = feed_for(&signer);
        let content = DefaultContentChunk::new(&b"not a feed update"[..]).unwrap();
        let store = LyingTrusted {
            bytes: StandardChunkSet::encode_typed(&content.into()),
            // Test-only vouching; the store deliberately lies about type.
            source: unsafe { TrustedSource::grant() },
        };

        let getter = Getter::new(feed, store);
        assert!(matches!(
            getter.at(Sequence::ZERO).await.unwrap_err(),
            FeedError::NotSingleOwner(_)
        ));
    });
}

proptest! {
    /// Round trip over generator-drawn feeds: whatever identity and payload,
    /// an appended update reads back byte-identical.
    #[test]
    fn arbitrary_feed_round_trips(
        seed in proptest::collection::vec(any::<u8>(), 64..256),
        payload in proptest::collection::vec(any::<u8>(), 1..64),
    ) {
        let mut u = arbitrary::Unstructured::new(&seed);
        let Ok((feed, signer)) =
            nectar_feeds::generators::feed_with_signer::<DEFAULT_BODY_SIZE>(&mut u)
        else {
            return Ok(());
        };

        run(async {
            let store = DefaultMemoryStore::new();
            let mut updater = Updater::new(feed, &store, &signer);
            let written = updater.append(payload.clone()).await.unwrap();

            let getter = Getter::new(feed, &store);
            let read = getter.at(Sequence::ZERO).await.unwrap();
            prop_assert_eq!(read.payload().as_ref(), payload.as_slice());
            prop_assert_eq!(read.address(), written.address());
            prop_assert_eq!(read.chunk(), written.chunk());

            let latest = getter.latest().await.unwrap();
            prop_assert_eq!(latest.next, Sequence::ZERO.next());
            Ok(())
        })?;
    }
}
