//! Feed reader: fetch and resolve updates.

use core::marker::PhantomData;

use nectar_primitives::chunk::{Chunk, ChunkAddress};
use nectar_primitives::store::ChunkGet;

use crate::error::{FeedError, Result};
use crate::feed::Feed;
use crate::index::{Index, LatestFinder};
use crate::update::FeedUpdate;

/// Fetch and resolve the update at `address` from `store` for `feed`.
///
/// Shared between [`FeedReader::get`] and every [`LatestFinder`] probe so the
/// chunk-to-update resolution (single-owner decode, address verification, owner
/// check) lives in exactly one place.
///
/// A store get that the backend classifies as a genuine miss (via
/// [`ChunkGet::is_not_found`]) resolves to `Ok(None)`, the empty-slot signal a
/// finder walks past. Every other store error propagates as
/// [`FeedError::StoreGet`]; it is never folded into absence.
pub(crate) async fn fetch_update<I: Index, G: ChunkGet<BS>, const BS: usize>(
    feed: &Feed<BS>,
    store: &G,
    index: I,
    address: &ChunkAddress,
) -> Result<Option<FeedUpdate<I, BS>>> {
    let chunk = match store.get(address).await {
        Ok(chunk) => chunk,
        Err(err) if store.is_not_found(&err) => return Ok(None),
        Err(err) => return Err(FeedError::store_get(err)),
    };
    let soc = chunk.into_single_owner().ok_or(FeedError::NotSingleOwner)?;

    // Recover the signer and compare it to the feed owner before integrity
    // verification. A single-owner chunk addresses as `keccak256(id || owner)`,
    // so a chunk a backend hands back for this address whose recovered owner is
    // not the feed owner is a deliberate substitution; surface it as an owner
    // mismatch rather than letting the downstream address check report a generic
    // verification failure.
    let owner = soc.owner().map_err(|e| FeedError::Primitives(e.into()))?;
    if owner != feed.owner() {
        return Err(FeedError::OwnerMismatch {
            expected: feed.owner(),
            actual: owner,
        });
    }

    // Owner matches: verify the chunk is internally consistent and addresses to
    // the queried slot (id, span, and any dispersed-replica constraints).
    soc.verify(address)?;
    Ok(Some(FeedUpdate::new(index, soc)))
}

/// Reads updates from a feed.
///
/// Generic over the index type `I`, the chunk store `G` updates are fetched
/// through, and the body size `BS`. [`get`](FeedReader::get) fetches a known
/// index; [`latest`](FeedReader::latest) and [`find_at`](FeedReader::find_at)
/// resolve the most recent update via the scheme's [`LatestFinder`].
#[derive(Debug)]
pub struct FeedReader<I, G, const BS: usize> {
    feed: Feed<BS>,
    store: G,
    _index: PhantomData<fn() -> I>,
}

impl<I, G, const BS: usize> FeedReader<I, G, BS>
where
    I: Index,
    G: ChunkGet<BS>,
{
    /// Create a reader for `feed`, fetching through `store`.
    pub const fn new(feed: Feed<BS>, store: G) -> Self {
        Self {
            feed,
            store,
            _index: PhantomData,
        }
    }

    /// The feed this reader reads from.
    pub const fn feed(&self) -> &Feed<BS> {
        &self.feed
    }

    /// Fetch the update at `index`.
    ///
    /// Returns [`crate::FeedError::NotFound`] if no chunk exists at the derived
    /// address, [`crate::FeedError::NotSingleOwner`] if the stored chunk is not
    /// a single-owner chunk, and [`crate::FeedError::OwnerMismatch`] if it is
    /// signed by an address other than the feed owner.
    pub async fn get(&self, index: &I) -> Result<FeedUpdate<I, BS>> {
        let address = self.feed.update_address(index);
        fetch_update(&self.feed, &self.store, index.clone(), &address)
            .await?
            .ok_or(FeedError::NotFound { address })
    }

    /// Resolve the latest update, searching strictly after `after` when given.
    ///
    /// `after` is an exclusive floor: it names an index already known to exist,
    /// and the search begins at the slot immediately above it.
    ///
    /// This is the sequence-style entry that has no time target (`at = None`).
    /// For a time-indexed scheme that needs to search at a specific moment, use
    /// [`find_at`](Self::find_at) and pass the clock-derived target.
    ///
    /// Returns `Ok(None)` for an empty feed (or one with no update past
    /// `after`). Available only when the index scheme implements
    /// [`LatestFinder`].
    pub async fn latest(&self, after: Option<&I>) -> Result<Option<FeedUpdate<I, BS>>>
    where
        I: LatestFinder,
    {
        I::find_latest(&self.feed, &self.store, None, after).await
    }

    /// Resolve the update live at the explicit target time `at`, searching
    /// strictly after `after` when given.
    ///
    /// The caller supplies the target time (this crate never reads a system
    /// clock), so a time-indexed scheme like the epoch grid can be searched
    /// deterministically. A scheme that does not index by time ignores `at`.
    ///
    /// Returns `Ok(None)` for an empty feed (or one with no update at or before
    /// `at` past `after`). Available only when the index scheme implements
    /// [`LatestFinder`].
    pub async fn find_at(&self, at: u64, after: Option<&I>) -> Result<Option<FeedUpdate<I, BS>>>
    where
        I: LatestFinder,
    {
        I::find_latest(&self.feed, &self.store, Some(at), after).await
    }
}
