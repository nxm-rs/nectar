//! Read side: fetch, certify and interpret updates over a chunk store.

use core::fmt;

use nectar_primitives::DEFAULT_BODY_SIZE;
use nectar_primitives::chunk::{AnyChunkSet, Chunk, IntoVerified};
use nectar_primitives::store::{ChunkGet, ChunkHas};

use crate::error::{FeedError, Result};
use crate::feed::Feed;
use crate::index::Index;
use crate::sequence::Sequence;
use crate::update::FeedUpdate;

/// Read handle over a feed: a [`Feed`] plus a chunk store.
pub struct Getter<S, const BODY_SIZE: usize = DEFAULT_BODY_SIZE> {
    feed: Feed<BODY_SIZE>,
    store: S,
}

/// Latest sequence update plus the next free index.
#[derive(Debug, Clone)]
pub struct Latest<const BODY_SIZE: usize = DEFAULT_BODY_SIZE> {
    /// Latest present update at or above the search floor; `None` when the
    /// floor slot itself is absent.
    pub update: Option<FeedUpdate<Sequence, BODY_SIZE>>,
    /// First absent index, the next publish slot; `None` when the sequence
    /// space is fully occupied.
    pub next: Option<Sequence>,
}

impl<S, const BODY_SIZE: usize> fmt::Debug for Getter<S, BODY_SIZE> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Getter")
            .field("feed", &self.feed)
            .finish_non_exhaustive()
    }
}

impl<S, const BODY_SIZE: usize> Getter<S, BODY_SIZE> {
    /// Create a getter over `feed` reading from `store`.
    pub const fn new(feed: Feed<BODY_SIZE>, store: S) -> Self {
        Self { feed, store }
    }

    /// The feed this getter reads.
    pub const fn feed(&self) -> &Feed<BODY_SIZE> {
        &self.feed
    }
}

impl<S, const BODY_SIZE: usize> Getter<S, BODY_SIZE>
where
    S: ChunkGet<AnyChunkSet<BODY_SIZE>>,
    Chunk<S::Trust, AnyChunkSet<BODY_SIZE>>: IntoVerified<Registry = AnyChunkSet<BODY_SIZE>>,
{
    /// Fetch and certify the update at `index`.
    ///
    /// Certification at the derived address binds the id and the owner in
    /// one fixed-width keccak preimage, so no separate owner check exists.
    pub async fn at<I: Index>(&self, index: I) -> Result<FeedUpdate<I, BODY_SIZE>> {
        let address = self.feed.update_address(&index);
        let chunk = self.store.get(&address).await.map_err(FeedError::store)?;
        let chunk = chunk.into_verified()?;
        if *chunk.address() != address {
            return Err(FeedError::AddressMismatch {
                expected: address,
                actual: *chunk.address(),
            });
        }
        let soc = chunk
            .into_envelope()
            .into_single_owner()
            .ok_or(FeedError::NotSingleOwner(address))?;
        Ok(FeedUpdate::new(index, soc))
    }
}

impl<S, const BODY_SIZE: usize> Getter<S, BODY_SIZE>
where
    S: ChunkGet<AnyChunkSet<BODY_SIZE>> + ChunkHas,
    Chunk<S::Trust, AnyChunkSet<BODY_SIZE>>: IntoVerified<Registry = AnyChunkSet<BODY_SIZE>>,
{
    async fn present(&self, index: u64) -> bool {
        self.store
            .has(&self.feed.update_address(&Sequence::new(index)))
            .await
    }

    /// Certify the update at `index` and pair it with its successor slot.
    async fn found(&self, index: u64) -> Result<Latest<BODY_SIZE>> {
        let seq = Sequence::new(index);
        let next = seq.next();
        let update = self.at(seq).await?;
        Ok(Latest {
            update: Some(update),
            next,
        })
    }

    /// Latest update by exponential-then-binary probing, from index zero.
    ///
    /// Assumes gapless publication: a hole reads as the end of the feed.
    pub async fn latest(&self) -> Result<Latest<BODY_SIZE>> {
        self.latest_from(Sequence::ZERO).await
    }

    /// [`latest`](Self::latest) from a floor slot, for resuming with a
    /// known-present hint. An absent floor yields an empty result with
    /// `next = floor`.
    pub async fn latest_from(&self, floor: Sequence) -> Result<Latest<BODY_SIZE>> {
        let base = floor.get();
        if !self.present(base).await {
            return Ok(Latest {
                update: None,
                next: Some(floor),
            });
        }

        // Exponential phase: double the probe offset until a miss brackets
        // the boundary between present and absent.
        let mut lo = base;
        let mut off: u64 = 1;
        let mut hi = loop {
            let Some(idx) = base.checked_add(off) else {
                // The offset left the index space; the top slot decides.
                if self.present(u64::MAX).await {
                    return self.found(u64::MAX).await;
                }
                break u64::MAX;
            };
            if self.present(idx).await {
                if idx == u64::MAX {
                    return self.found(u64::MAX).await;
                }
                lo = idx;
                off = off.saturating_mul(2);
            } else {
                break idx;
            }
        };

        // Binary phase: `lo` present, `hi` absent; converge to adjacency.
        while let Some(gap) = hi.checked_sub(lo) {
            if gap <= 1 {
                break;
            }
            let Some(mid) = lo.checked_add(gap / 2) else {
                break;
            };
            if self.present(mid).await {
                lo = mid;
            } else {
                hi = mid;
            }
        }
        self.found(lo).await
    }

    /// Latest update by stepwise scan from a floor slot, one probe per
    /// index. The baseline the probing search is measured against.
    pub async fn latest_linear_from(&self, floor: Sequence) -> Result<Latest<BODY_SIZE>> {
        if !self.present(floor.get()).await {
            return Ok(Latest {
                update: None,
                next: Some(floor),
            });
        }
        let mut last = floor.get();
        loop {
            let Some(candidate) = last.checked_add(1) else {
                return self.found(last).await;
            };
            if !self.present(candidate).await {
                return self.found(last).await;
            }
            last = candidate;
        }
    }
}
