//! Write side: sign and publish updates over a chunk store.

use core::fmt;

use alloy_signer::SignerSync;
use bytes::Bytes;
use nectar_primitives::chunk::{AnyChunkSet, Chunk, SingleOwnerChunk, Verified};
use nectar_primitives::store::ChunkPut;
use nectar_primitives::{DEFAULT_BODY_SIZE, PrimitivesError};

use crate::error::{FeedError, Result};
use crate::feed::Feed;
use crate::index::Index;
use crate::sequence::Sequence;
use crate::update::FeedUpdate;

/// Write handle over a feed: a [`Feed`], a store, a signer and the next
/// sequence position.
pub struct Updater<S, Sig, const BODY_SIZE: usize = DEFAULT_BODY_SIZE> {
    feed: Feed<BODY_SIZE>,
    store: S,
    signer: Sig,
    next: Option<Sequence>,
}

impl<S, Sig, const BODY_SIZE: usize> fmt::Debug for Updater<S, Sig, BODY_SIZE> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Updater")
            .field("feed", &self.feed)
            .field("next", &self.next)
            .finish_non_exhaustive()
    }
}

impl<S, Sig, const BODY_SIZE: usize> Updater<S, Sig, BODY_SIZE> {
    /// Updater for a fresh feed, appending from index zero.
    pub const fn new(feed: Feed<BODY_SIZE>, store: S, signer: Sig) -> Self {
        Self::resume(feed, store, signer, Sequence::ZERO)
    }

    /// Updater resuming at a known next free index (see
    /// [`Latest::next`](crate::Latest::next)).
    pub const fn resume(feed: Feed<BODY_SIZE>, store: S, signer: Sig, next: Sequence) -> Self {
        Self {
            feed,
            store,
            signer,
            next: Some(next),
        }
    }

    /// The feed this updater publishes to.
    pub const fn feed(&self) -> &Feed<BODY_SIZE> {
        &self.feed
    }

    /// The next append position; `None` once the sequence space is spent.
    pub const fn next_index(&self) -> Option<Sequence> {
        self.next
    }
}

impl<S, Sig, const BODY_SIZE: usize> Updater<S, Sig, BODY_SIZE>
where
    S: ChunkPut<AnyChunkSet<BODY_SIZE>>,
    Sig: SignerSync,
{
    /// Sign and publish `payload` at the next sequence position, advancing
    /// the position on success.
    pub async fn append(
        &mut self,
        payload: impl Into<Bytes>,
    ) -> Result<FeedUpdate<Sequence, BODY_SIZE>> {
        let index = self.next.ok_or(FeedError::Exhausted)?;
        let update = self.put_at(index, payload).await?;
        self.next = index.next();
        Ok(update)
    }

    /// Sign and publish `payload` at an explicit index: the payload becomes
    /// the content-addressed body of a single-owner chunk signed under the
    /// derived update id.
    ///
    /// The signer must be the feed owner; a mismatch is rejected before the
    /// write.
    pub async fn put_at<I: Index>(
        &self,
        index: I,
        payload: impl Into<Bytes>,
    ) -> Result<FeedUpdate<I, BODY_SIZE>> {
        let id = self.feed.update_id(&index);
        let soc = SingleOwnerChunk::<BODY_SIZE>::new(id, payload, &self.signer)?;
        let owner = soc.owner().map_err(PrimitivesError::from)?;
        if owner != self.feed.owner() {
            return Err(FeedError::OwnerMismatch {
                expected: self.feed.owner(),
                actual: owner,
            });
        }
        let sealed = Chunk::<Verified, AnyChunkSet<BODY_SIZE>>::from_envelope(soc.clone().into())?;
        self.store.put(sealed).await.map_err(FeedError::store)?;
        Ok(FeedUpdate::new(index, soc))
    }
}
