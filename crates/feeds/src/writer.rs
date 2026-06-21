//! Feed writer: sign and publish updates.

use alloy_signer::{Signer, SignerSync};
use bytes::Bytes;
use nectar_primitives::chunk::{Chunk, ChunkAddress, SingleOwnerChunk};
use nectar_primitives::store::ChunkPut;

use crate::error::{FeedError, Result};
use crate::feed::Feed;
use crate::index::WriteCursor;

/// Publishes signed updates to a feed.
///
/// Generic over the write cursor `C` (the scheme's stateful write head), the
/// chunk store `P` updates are published through, the signer `S`, and the body
/// size `BS`. The signer's address must equal the feed owner; this is checked
/// at construction.
///
/// Each published update is a single-owner chunk at
/// `keccak256(keccak256(topic || index.marshal()) || owner)`, signed by `S`.
#[derive(Debug)]
pub struct FeedWriter<C, P, S, const BS: usize> {
    feed: Feed<BS>,
    cursor: C,
    store: P,
    signer: S,
}

impl<C, P, S, const BS: usize> FeedWriter<C, P, S, BS>
where
    C: WriteCursor,
    P: ChunkPut<BS>,
    S: SignerSync + Signer,
{
    /// Create a writer for `feed`, publishing through `store`, signing with
    /// `signer`, starting at the position `cursor` points to.
    ///
    /// Returns [`crate::FeedError::OwnerMismatch`] if the signer's address does
    /// not match the feed owner.
    pub fn new(feed: Feed<BS>, cursor: C, store: P, signer: S) -> Result<Self> {
        let actual = signer.address();
        if actual != feed.owner() {
            return Err(FeedError::OwnerMismatch {
                expected: feed.owner(),
                actual,
            });
        }
        Ok(Self {
            feed,
            cursor,
            store,
            signer,
        })
    }

    /// The feed this writer publishes to.
    pub const fn feed(&self) -> &Feed<BS> {
        &self.feed
    }

    /// The index the writer would publish to next for an update timestamped
    /// `at`. For a time-indexed scheme the index depends on `at`; the sequence
    /// scheme ignores it and returns the current counter.
    pub fn next_index(&self, at: Option<u64>) -> C::Index {
        self.cursor.index_for(at)
    }

    /// Publish `payload` at an explicit `index` without touching the cursor.
    ///
    /// Because this bypasses the cursor entirely, it also bypasses the cursor's
    /// exhaustion check by design: an explicit-index publish can land at any
    /// index (including `Sequence(u64::MAX)`) regardless of whether
    /// [`exhausted`](WriteCursor::exhausted) would be true on the cursor path.
    ///
    /// Returns the published single-owner chunk address.
    pub async fn update_at(
        &self,
        index: &C::Index,
        payload: impl Into<Bytes>,
    ) -> Result<ChunkAddress> {
        let id = self.feed.update_id(index);
        let soc = SingleOwnerChunk::<BS>::new(id, payload, &self.signer)?;
        let address = *soc.address();
        self.store
            .put(soc.into())
            .await
            .map_err(FeedError::store_put)?;
        Ok(address)
    }

    /// Publish `payload` for an update timestamped `at`, choosing the publish
    /// index from `at` and recording it on the cursor.
    ///
    /// The cursor picks the index ([`WriteCursor::index_for`]) before the write
    /// and records the publication ([`WriteCursor::record`]) after it succeeds,
    /// so a time-indexed scheme derives the grid position from the new update's
    /// timestamp. The cursor is driven through `&self` then `&mut self` with no
    /// move-out.
    ///
    /// Returns the published single-owner chunk address.
    pub async fn append_at(
        &mut self,
        at: Option<u64>,
        payload: impl Into<Bytes>,
    ) -> Result<ChunkAddress> {
        if self.cursor.exhausted() {
            return Err(FeedError::IndexExhausted);
        }
        let index = self.cursor.index_for(at);
        let address = self.update_at(&index, payload).await?;
        self.cursor.record(&index, at);
        Ok(address)
    }

    /// Publish `payload` without a timestamp. Equivalent to
    /// `append_at(None, payload)`.
    pub async fn append(&mut self, payload: impl Into<Bytes>) -> Result<ChunkAddress> {
        self.append_at(None, payload).await
    }
}

#[cfg(test)]
mod tests {
    use alloy_signer_local::PrivateKeySigner;
    use nectar_primitives::DEFAULT_BODY_SIZE;
    use nectar_primitives::chunk::Chunk;
    use nectar_primitives::store::{ChunkGet, MemoryStore};

    use crate::feed::Feed;
    use crate::sequence::{Sequence, SequenceCursor};
    use crate::topic::Topic;

    use super::*;

    fn signer() -> PrivateKeySigner {
        // Fixed key so the derived owner is stable across runs.
        let pk = [
            0x2c, 0x75, 0x36, 0xe3, 0x60, 0x5d, 0x9c, 0x16, 0xa7, 0xa3, 0xd7, 0xb1, 0x89, 0x8e,
            0x52, 0x93, 0x96, 0xa6, 0x5c, 0x23, 0xa3, 0xbc, 0xbd, 0x40, 0x12, 0xa1, 0x1c, 0xf2,
            0x73, 0x1b, 0x0f, 0xbc,
        ];
        PrivateKeySigner::from_slice(&pk).unwrap()
    }

    fn feed_for(signer: &PrivateKeySigner) -> Feed<DEFAULT_BODY_SIZE> {
        Feed::new(Topic::from_bytes(b"feeds-writer-test"), signer.address())
    }

    #[tokio::test]
    async fn update_at_publishes_a_readable_soc() {
        let signer = signer();
        let owner = signer.address();
        let feed = feed_for(&signer);
        let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();

        let payload = Bytes::from_static(b"hello feeds");
        let writer =
            FeedWriter::new(feed, SequenceCursor::new(), &store, signer).expect("owner matches");
        let address = writer
            .update_at(&Sequence(0), payload.clone())
            .await
            .expect("publish");

        // The published address is the deterministic feed update address.
        assert_eq!(address, feed.update_address(&Sequence(0)));

        // Read the chunk straight back out of the store and verify it.
        let any = ChunkGet::get(&store, &address).await.expect("stored");
        let soc = any.as_single_owner().expect("single-owner chunk");
        assert_eq!(soc.owner().unwrap(), owner);
        assert_eq!(soc.data(), &payload);
        assert_eq!(soc.address(), &address);
    }

    #[tokio::test]
    async fn append_advances_the_sequence_index() {
        let signer = signer();
        let feed = feed_for(&signer);
        let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();

        let mut writer =
            FeedWriter::new(feed, SequenceCursor::new(), &store, signer).expect("owner matches");

        assert_eq!(writer.next_index(None), Sequence(0));
        let a0 = writer.append(Bytes::from_static(b"v0")).await.expect("v0");
        assert_eq!(writer.next_index(None), Sequence(1));
        let a1 = writer.append(Bytes::from_static(b"v1")).await.expect("v1");
        assert_eq!(writer.next_index(None), Sequence(2));

        // Each append lands at the address the index derives, and they differ.
        assert_eq!(a0, feed.update_address(&Sequence(0)));
        assert_eq!(a1, feed.update_address(&Sequence(1)));
        assert_ne!(a0, a1);

        // Both updates are retrievable with the right payloads.
        let v0 = ChunkGet::get(&store, &a0).await.expect("get v0");
        let v1 = ChunkGet::get(&store, &a1).await.expect("get v1");
        assert_eq!(v0.data().as_ref(), b"v0");
        assert_eq!(v1.data().as_ref(), b"v1");
    }

    #[tokio::test]
    async fn append_at_surfaces_index_exhausted() {
        let signer = signer();
        let feed = feed_for(&signer);
        let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();

        // A cursor seated at the last representable sequence number: the first
        // append lands at `u64::MAX`, after which the counter has no successor.
        let mut writer = FeedWriter::new(feed, SequenceCursor::at(u64::MAX), &store, signer)
            .expect("owner matches");

        let address = writer
            .append(Bytes::from_static(b"last"))
            .await
            .expect("the final index is publishable");
        assert_eq!(address, feed.update_address(&Sequence(u64::MAX)));

        // The cursor is now exhausted; the next append is refused before any
        // write, so nothing is published at a wrapped or duplicate index.
        let err = writer
            .append(Bytes::from_static(b"overflow"))
            .await
            .expect_err("exhausted cursor refuses a further publish");
        assert!(matches!(err, FeedError::IndexExhausted));

        // No second chunk was written for the exhausted publish: the store still
        // holds only the single update at `u64::MAX`.
        let any = ChunkGet::get(&store, &feed.update_address(&Sequence(u64::MAX)))
            .await
            .expect("the final update is stored");
        assert_eq!(any.data().as_ref(), b"last");
    }

    #[tokio::test]
    async fn new_rejects_a_foreign_signer() {
        let owner_signer = signer();
        let feed = feed_for(&owner_signer);
        let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();

        // A different signer (a distinct fixed key) cannot author this feed.
        let foreign = PrivateKeySigner::from_slice(&[0x11u8; 32]).unwrap();
        let err = FeedWriter::new(feed, SequenceCursor::new(), &store, foreign)
            .expect_err("foreign signer rejected");
        assert!(matches!(err, FeedError::OwnerMismatch { .. }));
    }
}
