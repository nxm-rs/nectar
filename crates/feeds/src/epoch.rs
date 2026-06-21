//! Epoch feed scheme: a time-indexed binary grid.
//!
//! An epoch is a half-open time interval `[start, start + 2^level)` on a binary
//! grid. The scheme lets a reader find the update live at any timestamp with a
//! logarithmic walk, at the cost of a stateful write cursor that must remember
//! the previous update time to place the next epoch.
//!
//! # Grid math
//!
//! Each epoch spans `2^level` seconds. An epoch is partitioned into a left and
//! a right child one level down; the child whose interval contains a target
//! timestamp is selected by testing the `2^(level-1)` bit of the timestamp. The
//! root epoch `{start: 0, level: 32}` covers the whole representable grid.
//!
//! The canonical comparison timestamp of an epoch is `2^level * start`, not the
//! start alone. This is the value compared against the lookup target to decide
//! whether an epoch was published at or before the target.
//!
//! # Finders
//!
//! Two latest-update searches are provided over the same grid:
//!
//! - A sequential finder that walks ancestors then descends, probing one epoch
//!   at a time. It is the simple, allocation-free baseline walk.
//! - A concurrent finder that fires overlapping store gets along the
//!   descent path and resolves once it has bracketed the answer with a found
//!   epoch directly above a not-found epoch. It drives its in-flight gets with
//!   a self-contained, `core`-only poll set (no executor crate), cancels stale
//!   gets, never assumes a multi-threaded runtime, and never adds a `Send`
//!   bound, so it runs unchanged on a single-threaded wasm executor.
//!
//! Gated behind the `epoch` feature; the sequence scheme is the default.

use alloc::boxed::Box;
use alloc::vec::Vec;
use core::future::Future;
use core::pin::Pin;
use core::task::Poll;

use alloy_primitives::Keccak256;
use nectar_primitives::store::ChunkGet;

use crate::error::Result;
use crate::feed::Feed;
use crate::index::{Index, LatestFinder, WriteCursor};
use crate::update::FeedUpdate;

/// The top level of the epoch grid. The root epoch is `{start: 0, level: 32}`.
const MAX_LEVEL: u8 = 32;

/// An epoch index: the half-open interval `[start, start + 2^level)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Epoch {
    /// The epoch start time (Unix seconds), aligned to its level.
    pub start: u64,
    /// The epoch level: the interval spans `2^level` seconds.
    pub level: u8,
}

impl Epoch {
    /// The root epoch `{start: 0, level: 32}` that covers the whole grid.
    pub const ROOT: Self = Self {
        start: 0,
        level: MAX_LEVEL,
    };

    /// Construct an epoch from its start and level.
    pub const fn new(start: u64, level: u8) -> Self {
        Self { start, level }
    }

    /// The span of the epoch in seconds: `2^level`.
    ///
    /// A level at or above 64 would shift past the width of a `u64`; such a
    /// level is not representable on the grid (the root is level 32), so the
    /// span saturates to `u64::MAX` rather than invoking undefined shift
    /// behaviour.
    pub const fn length(&self) -> u64 {
        if self.level >= 64 {
            u64::MAX
        } else {
            1u64 << self.level
        }
    }

    /// The canonical comparison timestamp: `2^level * start`.
    ///
    /// This is the value compared against a lookup target, not `start` alone.
    pub const fn timestamp(&self) -> u64 {
        self.length().wrapping_mul(self.start)
    }

    /// The ancestor one level up.
    ///
    /// Caller responsibility: must not be called on a top-level epoch.
    pub const fn parent(&self) -> Self {
        let length = self.length() << 1;
        Self {
            start: (self.start / length) * length,
            level: self.level + 1,
        }
    }

    /// The left sister of this epoch.
    ///
    /// Caller responsibility: valid only for a right sister, not a left one.
    pub const fn left(&self) -> Self {
        Self {
            start: self.start - self.length(),
            level: self.level,
        }
    }

    /// The child epoch (one level down) whose interval contains `at`.
    ///
    /// Level 0 is the finest grid resolution and has no child, so calling this
    /// on a level-0 epoch floors: it returns the epoch unchanged rather than
    /// underflowing the level. Callers descending the grid stop at level 0.
    ///
    /// Caller responsibility: `at` must fall within this epoch.
    pub const fn child_at(&self, at: u64) -> Self {
        let Some(child_level) = self.level.checked_sub(1) else {
            return *self;
        };
        let child_length = 1u64 << child_level;
        let mut start = self.start;
        if at & child_length != 0 {
            start |= child_length;
        }
        Self {
            start,
            level: child_level,
        }
    }

    /// Whether this epoch is the left sister of its parent.
    pub const fn is_left(&self) -> bool {
        self.start & self.length() == 0
    }

    /// The lowest common ancestor epoch of two Unix times.
    ///
    /// With `after == 0` (no prior update is known) this is the root epoch.
    pub const fn lca(at: u64, after: u64) -> Self {
        if after == 0 {
            return Self::ROOT;
        }
        let diff = at.saturating_sub(after);
        let mut length = 1u64;
        let mut level: u8 = 0;
        while level < MAX_LEVEL && (length < diff || at / length != after / length) {
            length <<= 1;
            level += 1;
        }
        let start = (after / length) * length;
        Self { start, level }
    }

    /// The next epoch to publish at time `at`, given the current epoch `prev`
    /// (if any) and the time `last` of the previous update.
    ///
    /// With no previous epoch the next position is the root. Otherwise, if `at`
    /// still falls within the current epoch the next position is the matching
    /// child; if it has moved past the epoch, the descent restarts from the
    /// lowest common ancestor of `at` and `last`.
    ///
    /// When the current epoch is already at level 0 (a sub-second republish onto
    /// the finest grid resolution) there is no child to descend to, so the
    /// level-0 epoch is returned unchanged. This floors the descent rather than
    /// underflowing the level or shifting past the width of a `u64`.
    pub const fn next(prev: Option<Self>, last: u64, at: u64) -> Self {
        match prev {
            None => Self::ROOT,
            Some(e) if e.level == 0 => e,
            Some(e) => {
                if e.start.saturating_add(e.length()) > at {
                    e.child_at(at)
                } else {
                    Self::lca(at, last).child_at(at)
                }
            }
        }
    }
}

impl Index for Epoch {
    fn marshal(&self) -> impl AsRef<[u8]> {
        let mut hasher = Keccak256::new();
        hasher.update(self.start.to_be_bytes());
        hasher.update([self.level]);
        hasher.finalize()
    }

    fn first() -> Self {
        Self::ROOT
    }
}

/// The write head for the epoch scheme.
///
/// Unlike the sequence cursor, this carries the previous update epoch and time
/// so that the next position can be placed on the grid relative to them. The
/// epoch an update occupies is a function of the previous state and the new
/// update's timestamp, which is why the scheme picks its index from `at` in
/// [`index_for`](WriteCursor::index_for) rather than tracking a single "current"
/// position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EpochCursor {
    /// The previous published epoch, if any update has been published.
    prev: Option<Epoch>,
    /// The time of the last published update, if any.
    last_time: u64,
}

impl EpochCursor {
    /// A cursor with no prior update.
    pub const fn new() -> Self {
        Self {
            prev: None,
            last_time: 0,
        }
    }

    /// The time of the last published update, `0` if none yet.
    pub const fn last_time(&self) -> u64 {
        self.last_time
    }
}

impl Default for EpochCursor {
    fn default() -> Self {
        Self::new()
    }
}

impl WriteCursor for EpochCursor {
    type Index = Epoch;

    fn index_for(&self, at: Option<u64>) -> Self::Index {
        // The grid position of an epoch update depends on the new update's
        // timestamp, so an epoch write must carry one. With no timestamp the
        // first publishable position is the root epoch.
        let at = at.unwrap_or(0);
        Epoch::next(self.prev, self.last_time, at)
    }

    fn record(&mut self, index: &Self::Index, at: Option<u64>) {
        self.prev = Some(*index);
        self.last_time = at.unwrap_or(0);
    }
}

/// Fetch the update at `epoch`.
///
/// Delegates to the shared [`crate::reader::fetch_update`] so the epoch
/// resolution path runs the identical chunk-to-update checks as the sequence
/// path: single-owner decode, owner recovery against the feed owner, and
/// address verification (`keccak256(id || owner)` pinned to the queried slot).
/// The address verification is what rejects an owner-signed chunk whose id is
/// for a different epoch than the one queried; without it such a chunk would be
/// accepted as the queried slot.
///
/// A genuine store miss (classified through [`ChunkGet::is_not_found`]) resolves
/// to `Ok(None)`, the empty-grid-slot signal the finders walk past. Every other
/// store error propagates as [`crate::FeedError::StoreGet`]; it is never folded
/// into absence. A present chunk that is not a single-owner chunk, is signed by
/// an address other than the feed owner, or does not address to the queried
/// epoch slot surfaces the corresponding [`crate::FeedError`].
async fn get_epoch<const BS: usize, G: ChunkGet<BS>>(
    feed: &Feed<BS>,
    store: &G,
    epoch: Epoch,
) -> Result<Option<FeedUpdate<Epoch, BS>>> {
    let address = feed.update_address(&epoch);
    crate::reader::fetch_update(feed, store, epoch, &address).await
}

/// Sequential epoch finder: walk ancestors to the common epoch, then descend.
///
/// A two-phase grid search. `common` climbs from the lowest common ancestor of
/// `at` and `after` until it finds an epoch whose update is present and not in
/// the future, or reaches the root with nothing. `at_phase` then descends from
/// that epoch toward the finest resolution, carrying the best (deepest,
/// in-the-past) update found so far.
///
/// This is the allocation-free counterpart to the concurrent finder, kept as a
/// reference walk that the finder-behaviour tests cross-check the concurrent
/// path against. The concurrent path is the one wired into [`LatestFinder`], so
/// the sequential chain compiles only under test.
#[cfg(test)]
async fn find_latest_sequential<const BS: usize, G: ChunkGet<BS>>(
    feed: &Feed<BS>,
    store: &G,
    at: u64,
    after: u64,
) -> Result<Option<FeedUpdate<Epoch, BS>>> {
    let (epoch, found) = common(feed, store, at, after).await?;
    at_phase(feed, store, at, epoch, found).await
}

/// Climb from `lca(at, after)` to the lowest ancestor whose update exists and
/// whose comparison timestamp is at or before `at`. Returns that epoch and its
/// update, or the root epoch with `None` when the feed is empty.
#[cfg(test)]
async fn common<const BS: usize, G: ChunkGet<BS>>(
    feed: &Feed<BS>,
    store: &G,
    at: u64,
    after: u64,
) -> Result<(Epoch, Option<FeedUpdate<Epoch, BS>>)> {
    let mut e = Epoch::lca(at, after);
    loop {
        match get_epoch(feed, store, e).await? {
            None => {
                if e.level == MAX_LEVEL {
                    return Ok((e, None));
                }
                e = e.parent();
            }
            Some(update) => {
                if e.timestamp() <= at {
                    return Ok((e, Some(update)));
                }
                e = e.parent();
            }
        }
    }
}

/// Descend from `e` toward the finest resolution, carrying the best update
/// found so far (`carry`). Returns the update live at `at`.
#[cfg(test)]
async fn at_phase<const BS: usize, G: ChunkGet<BS>>(
    feed: &Feed<BS>,
    store: &G,
    mut at: u64,
    mut e: Epoch,
    mut carry: Option<FeedUpdate<Epoch, BS>>,
) -> Result<Option<FeedUpdate<Epoch, BS>>> {
    loop {
        match get_epoch(feed, store, e).await? {
            None => {
                // Epoch not found on this branch.
                if e.is_left() {
                    return Ok(carry);
                }
                // Traverse the earlier (left sister) branch.
                at = e.start - 1;
                e = e.left();
            }
            Some(update) => {
                if e.timestamp() > at {
                    // Published in the future relative to the target.
                    if e.is_left() {
                        return Ok(carry);
                    }
                    at = e.start - 1;
                    e = e.left();
                } else if e.level == 0 {
                    // Matching update time or finest resolution.
                    return Ok(Some(update));
                } else {
                    // Continue descending toward `at`, keeping this update.
                    let child = e.child_at(at);
                    carry = Some(update);
                    e = child;
                }
            }
        }
    }
}

/// One descent path in the concurrent finder.
///
/// A path tracks the deepest found epoch (`top`) and the deepest not-found
/// epoch (`bottom`) seen along it. When `top` sits exactly one level above
/// `bottom` the answer is bracketed: either `top` is the result, or the search
/// recurses onto a fresh path through `bottom`'s left sister.
struct Path<const BS: usize> {
    /// Deepest found epoch and its update on this path.
    top: Option<(Epoch, FeedUpdate<Epoch, BS>)>,
    /// Deepest not-found epoch on this path.
    bottom: Option<Epoch>,
    /// Whether this path has been retired (its in-flight gets are stale).
    cancelled: bool,
}

impl<const BS: usize> Path<BS> {
    const fn new() -> Self {
        Self {
            top: None,
            bottom: None,
            cancelled: false,
        }
    }
}

/// The outcome of one store get fired along a path.
struct Probe<const BS: usize> {
    /// The path the probe belongs to.
    path: usize,
    /// The epoch probed.
    epoch: Epoch,
    /// The update if found and not published in the future, else `None`.
    update: Option<FeedUpdate<Epoch, BS>>,
}

/// A boxed, non-`Send` future producing one probe result, or a store error to
/// propagate.
type ProbeFuture<'a, const BS: usize> = Pin<Box<dyn Future<Output = Result<Probe<BS>>> + 'a>>;

/// A self-contained, non-`Send` poll set: a bag of probe futures polled
/// concurrently without any executor crate. Each call to [`next`](Self::next)
/// returns the first probe to complete.
///
/// Every inner future is polled with the outer task's waker, so a future
/// pending on a real async store wakes the outer executor when its IO
/// completes; synchronous stores complete in the first poll round. This keeps
/// the finder free of any runtime dependency and free of a `Send` bound.
struct ProbeSet<'a, const BS: usize> {
    futures: Vec<ProbeFuture<'a, BS>>,
}

impl<'a, const BS: usize> ProbeSet<'a, BS> {
    const fn new() -> Self {
        Self {
            futures: Vec::new(),
        }
    }

    fn push(&mut self, fut: ProbeFuture<'a, BS>) {
        self.futures.push(fut);
    }

    fn is_empty(&self) -> bool {
        self.futures.is_empty()
    }

    /// Drive the set to the next completed probe, or `None` when empty. A probe
    /// that hit a store error yields that error for the caller to propagate.
    async fn next(&mut self) -> Option<Result<Probe<BS>>> {
        if self.is_empty() {
            return None;
        }
        core::future::poll_fn(|cx| {
            let mut i = 0;
            while i < self.futures.len() {
                match self.futures[i].as_mut().poll(cx) {
                    Poll::Ready(probe) => {
                        // Drop the completed future; its result is in `probe`.
                        drop(self.futures.swap_remove(i));
                        return Poll::Ready(Some(probe));
                    }
                    Poll::Pending => i += 1,
                }
            }
            Poll::Pending
        })
        .await
    }
}

/// Concurrent epoch finder.
///
/// Descends the `child_at(at)` chain from the root, firing a store get at every
/// epoch on the path concurrently through a self-contained [`ProbeSet`]. Per
/// path it tracks the deepest found epoch and the deepest not-found epoch; once
/// a found epoch sits one level above a not-found epoch the answer is
/// bracketed. Stale in-flight gets are dropped by marking their path cancelled
/// and ignoring their results. No timer or runtime assumption is made and no
/// `Send` bound is imposed, so this drives on any executor including
/// single-threaded wasm.
async fn find_latest_concurrent<const BS: usize, G: ChunkGet<BS>>(
    feed: &Feed<BS>,
    store: &G,
    at: u64,
) -> Result<Option<FeedUpdate<Epoch, BS>>> {
    let mut paths: Vec<Path<BS>> = Vec::new();
    let mut inflight: ProbeSet<'_, BS> = ProbeSet::new();

    // Spawn the descent chain for `path_idx` starting at `start`.
    fn spawn_chain<'a, const BS: usize, G: ChunkGet<BS>>(
        feed: &'a Feed<BS>,
        store: &'a G,
        inflight: &mut ProbeSet<'a, BS>,
        path_idx: usize,
        at: u64,
        start: Epoch,
    ) {
        let mut e = start;
        loop {
            inflight.push(Box::pin(async move {
                // A genuine miss is absence; a real store error propagates.
                let update = match get_epoch(feed, store, e).await? {
                    // Found but published in the future is treated as absence.
                    Some(u) if e.timestamp() <= at => Some(u),
                    _ => None,
                };
                Ok(Probe {
                    path: path_idx,
                    epoch: e,
                    update,
                })
            }));
            if e.level == 0 {
                break;
            }
            e = e.child_at(at);
        }
    }

    paths.push(Path::new());
    spawn_chain(feed, store, &mut inflight, 0, at, Epoch::ROOT);

    while let Some(probe) = inflight.next().await {
        let probe = probe?;
        let path = &mut paths[probe.path];
        if path.cancelled {
            continue;
        }

        match probe.update {
            Some(update) => {
                if probe.epoch.level == 0 {
                    return Ok(Some(update));
                }
                // Ignore a shallower find than the deepest already seen.
                if let Some((top_epoch, _)) = &path.top
                    && top_epoch.level <= probe.epoch.level
                {
                    continue;
                }
                path.top = Some((probe.epoch, update));
            }
            None => {
                if probe.epoch.level == MAX_LEVEL {
                    path.cancelled = true;
                    return Ok(None);
                }
                match &path.bottom {
                    Some(b) if b.level >= probe.epoch.level => {}
                    _ => path.bottom = Some(probe.epoch),
                }
            }
        }

        // Bracketed: a found epoch directly above a not-found epoch.
        let (top_level, bottom) = match (&paths[probe.path].top, paths[probe.path].bottom) {
            (Some((te, _)), Some(be)) => (te.level, be),
            _ => continue,
        };
        if top_level == bottom.level + 1 {
            paths[probe.path].cancelled = true;
            if bottom.is_left() {
                let top = paths[probe.path].top.take().map(|(_, u)| u);
                return Ok(top);
            }
            // Recurse on a fresh path through the left sister, carrying the
            // found update forward as the new path's top.
            let carried = paths[probe.path].top.take();
            let new_at = bottom.start - 1;
            let new_idx = paths.len();
            let mut new_path = Path::new();
            new_path.top = carried;
            paths.push(new_path);
            spawn_chain(feed, store, &mut inflight, new_idx, new_at, bottom.left());
        }
    }

    Ok(None)
}

/// The largest representable lookup target on the grid: the span of the root
/// epoch minus one, the last second the root `{0, 32}` covers.
///
/// Used as the lookup target when a caller asks for the latest update without
/// naming a time (`at = None`). A search at the top of the grid tracks the most
/// recent published epoch. No system clock is read in this crate; a caller that
/// wants "live at a specific moment" passes that moment through
/// [`FeedReader::find_at`](crate::FeedReader::find_at).
const GRID_MAX_TIME: u64 = (1u64 << MAX_LEVEL) - 1;

impl Epoch {
    /// Find the latest update at an explicit target time `at`, using the
    /// concurrent grid walk.
    ///
    /// `after` is an advisory lower-bound hint (an epoch start, `0` for none).
    /// The concurrent path may ignore it: it re-walks from the grid root rather
    /// than starting the descent above `after`. The sequential counterpart
    /// honours the hint to prune its walk. For a well-formed hint (one that
    /// names a position at or before the answer) both paths resolve to the same
    /// update; the hint only ever changes how much of the grid is probed, never
    /// the result.
    ///
    /// Precondition: `at` must be within the representable grid, i.e.
    /// `at <= (1 << 32) - 1` (the span of the root epoch `{0, 32}`, around the
    /// year 2106 in Unix seconds). A target above the root epoch span has its
    /// high time bits silently masked off by the `child_at` descent and resolves
    /// against a partially covered grid rather than failing, so callers must not
    /// pass an out-of-grid timestamp.
    ///
    /// This is the time-parameterised entry the grid search needs;
    /// [`LatestFinder::find_latest`] forwards a caller-supplied target here.
    /// Both finders resolve to the same update for a given `at`; the concurrent
    /// walk is the primary path.
    pub(crate) async fn find_at<const BS: usize, G: ChunkGet<BS>>(
        feed: &Feed<BS>,
        store: &G,
        at: u64,
        after: u64,
    ) -> Result<Option<FeedUpdate<Self, BS>>> {
        debug_assert!(
            at <= GRID_MAX_TIME,
            "find_at target {at} exceeds the representable grid (GRID_MAX_TIME); high time bits are silently masked off by the descent",
        );
        let _ = after;
        find_latest_concurrent(feed, store, at).await
    }

    /// Find the latest update at `at` via the sequential grid walk.
    ///
    /// The single-probe walk: ancestors first, then a descent. `after` is an
    /// advisory lower-bound hint (an epoch start) that this path honours to
    /// prune its walk. It resolves identically to [`find_at`](Self::find_at) and
    /// exists as the allocation-free counterpart the finder-behaviour tests
    /// cross-check the concurrent path against, so it compiles only under test.
    #[cfg(test)]
    pub(crate) async fn find_at_sequential<const BS: usize, G: ChunkGet<BS>>(
        feed: &Feed<BS>,
        store: &G,
        at: u64,
        after: u64,
    ) -> Result<Option<FeedUpdate<Self, BS>>> {
        find_latest_sequential(feed, store, at, after).await
    }
}

impl LatestFinder for Epoch {
    fn find_latest<const BS: usize, G: ChunkGet<BS>>(
        feed: &Feed<BS>,
        store: &G,
        at: Option<u64>,
        after: Option<&Self>,
    ) -> impl core::future::Future<Output = Result<Option<FeedUpdate<Self, BS>>>> {
        // `after` is an advisory lower-bound hint expressed as an epoch; its
        // start time is the floor. The concurrent (primary) path may ignore it
        // and re-walk from the grid root; the sequential path honours it to
        // prune. With no hint the search runs from the root (`after = 0`).
        let after_time = after.map(|e| e.start).unwrap_or(0);
        // No clock is read here: with no caller-supplied target the search runs
        // at the top of the grid and tracks the most recent published epoch.
        let at = at.unwrap_or(GRID_MAX_TIME);
        async move { Self::find_at(feed, store, at, after_time).await }
    }
}

#[cfg(test)]
mod tests {
    use alloy_signer_local::PrivateKeySigner;
    use nectar_primitives::DEFAULT_BODY_SIZE;
    use nectar_primitives::DefaultContentChunk;
    use nectar_primitives::chunk::{AnyChunk, Chunk, ChunkAddress, SingleOwnerChunk};
    use nectar_primitives::store::{ChunkStoreError, SyncChunkGet, SyncChunkPut};

    use super::*;
    use crate::FeedError;
    use crate::topic::Topic;

    /// A single-update feed resolves to that update at and after its publish
    /// time on both finders. The first update lands on the root epoch.
    #[tokio::test]
    async fn finder_single_update() {
        let signer = PrivateKeySigner::random();
        let owner = signer.address();
        let feed = Feed::<DEFAULT_BODY_SIZE>::new(Topic::from_bytes(b"testtopic"), owner);
        let store = nectar_primitives::store::MemoryStore::<DEFAULT_BODY_SIZE>::new();

        let at = 123_456u64;
        let cursor = EpochCursor::new();
        let epoch = cursor.index_for(Some(at));
        assert_eq!(epoch, Epoch::ROOT, "first update lands on the root epoch");

        let id = feed.update_id(&epoch);
        let soc =
            SingleOwnerChunk::<DEFAULT_BODY_SIZE>::new(id, at.to_be_bytes().to_vec(), &signer)
                .expect("build soc");
        assert_eq!(soc.address(), &feed.update_address(&epoch));
        SyncChunkPut::put(&store, soc.into()).expect("put");

        for query in [at, at + 1, at * 2] {
            let found = Epoch::find_at(&feed, &store, query, 0)
                .await
                .expect("concurrent find")
                .expect("update present");
            assert_eq!(found.chunk().id(), id, "concurrent at {query}");

            let found_seq = Epoch::find_at_sequential(&feed, &store, query, 0)
                .await
                .expect("sequential find")
                .expect("update present");
            assert_eq!(found_seq.chunk().id(), id, "sequential at {query}");
        }
    }

    /// Publish updates at strictly increasing timestamps, then assert both
    /// finders resolve to the latest published update for any query at or after
    /// the last publish time. An empty feed resolves to no update.
    #[tokio::test]
    async fn finder_resolves_latest_update() {
        let signer = PrivateKeySigner::random();
        let owner = signer.address();
        let feed = Feed::<DEFAULT_BODY_SIZE>::new(Topic::from_bytes(b"testtopic"), owner);
        let store = nectar_primitives::store::MemoryStore::<DEFAULT_BODY_SIZE>::new();

        let times: [u64; 5] = [5, 9, 17, 100, 1000];
        let mut cursor = EpochCursor::new();
        let mut last_epoch = Epoch::ROOT;

        for &at in &times {
            let epoch = cursor.index_for(Some(at));
            let id = feed.update_id(&epoch);
            let soc =
                SingleOwnerChunk::<DEFAULT_BODY_SIZE>::new(id, at.to_be_bytes().to_vec(), &signer)
                    .expect("build soc");
            assert_eq!(soc.address(), &feed.update_address(&epoch));
            SyncChunkPut::put(&store, soc.into()).expect("put");
            cursor.record(&epoch, Some(at));
            last_epoch = epoch;
        }

        let expected_id = feed.update_id(&last_epoch);
        for query in [*times.last().unwrap(), 2000, 100_000] {
            let found = Epoch::find_at(&feed, &store, query, 0)
                .await
                .expect("concurrent find")
                .expect("update present");
            assert_eq!(
                found.chunk().id(),
                expected_id,
                "concurrent finder at {query} did not resolve the latest update",
            );

            let found_seq = Epoch::find_at_sequential(&feed, &store, query, 0)
                .await
                .expect("sequential find")
                .expect("update present");
            assert_eq!(
                found_seq.chunk().id(),
                expected_id,
                "sequential finder at {query} did not resolve the latest update",
            );
        }

        let empty_feed = Feed::<DEFAULT_BODY_SIZE>::new(Topic::from_bytes(b"empty"), owner);
        assert!(
            Epoch::find_at(&empty_feed, &store, 1000, 0)
                .await
                .expect("concurrent find on empty")
                .is_none(),
        );
        assert!(
            Epoch::find_at_sequential(&empty_feed, &store, 1000, 0)
                .await
                .expect("sequential find on empty")
                .is_none(),
        );
    }

    /// A store that always returns one planted chunk for any address, modelling
    /// a backend that substitutes a foreign or mis-addressed chunk for an epoch
    /// slot.
    struct SubstitutingStore {
        chunk: AnyChunk<DEFAULT_BODY_SIZE>,
    }

    impl SyncChunkGet<DEFAULT_BODY_SIZE> for SubstitutingStore {
        type Error = ChunkStoreError;

        fn get(
            &self,
            _address: &ChunkAddress,
        ) -> core::result::Result<AnyChunk<DEFAULT_BODY_SIZE>, Self::Error> {
            Ok(self.chunk.clone())
        }
    }

    /// A foreign-owner single-owner chunk returned for an epoch slot is rejected
    /// on the owner check by both epoch finders.
    #[tokio::test]
    async fn epoch_foreign_owner_chunk_is_rejected_with_owner_mismatch() {
        let owner_signer = PrivateKeySigner::random();
        let owner = owner_signer.address();
        let feed = Feed::<DEFAULT_BODY_SIZE>::new(Topic::from_bytes(b"epoch-owner-check"), owner);

        let foreign = PrivateKeySigner::random();
        assert_ne!(
            foreign.address(),
            owner,
            "foreign signer differs from owner"
        );

        let id = feed.update_id(&Epoch::ROOT);
        let foreign_soc =
            SingleOwnerChunk::<DEFAULT_BODY_SIZE>::new(id, b"forged".to_vec(), &foreign)
                .expect("build foreign soc");
        let store = SubstitutingStore {
            chunk: foreign_soc.into(),
        };

        for which in ["concurrent", "sequential"] {
            let result = if which == "concurrent" {
                Epoch::find_at(&feed, &store, 1000, 0).await
            } else {
                Epoch::find_at_sequential(&feed, &store, 1000, 0).await
            };
            match result {
                Err(FeedError::OwnerMismatch { expected, actual }) => {
                    assert_eq!(expected, owner);
                    assert_eq!(actual, foreign.address());
                }
                other => panic!("{which} finder must reject a foreign owner, got {other:?}"),
            }
        }
    }

    /// A content-addressed (non-SOC) chunk returned for an epoch slot is
    /// rejected on the SOC discrimination by both epoch finders.
    #[tokio::test]
    async fn epoch_content_chunk_is_rejected_as_not_single_owner() {
        let owner_signer = PrivateKeySigner::random();
        let owner = owner_signer.address();
        let feed = Feed::<DEFAULT_BODY_SIZE>::new(Topic::from_bytes(b"epoch-cac-check"), owner);

        let cac = DefaultContentChunk::new(b"not a soc".to_vec()).expect("build cac");
        let store = SubstitutingStore { chunk: cac.into() };

        for which in ["concurrent", "sequential"] {
            let result = if which == "concurrent" {
                Epoch::find_at(&feed, &store, 1000, 0).await
            } else {
                Epoch::find_at_sequential(&feed, &store, 1000, 0).await
            };
            match result {
                Err(FeedError::NotSingleOwner) => {}
                other => panic!("{which} finder must reject a content chunk, got {other:?}"),
            }
        }
    }

    /// Address verification on the epoch read path: an owner-signed chunk whose
    /// id is for a *different* epoch than the one queried must be rejected, not
    /// silently accepted as the queried slot. The owner check alone passes (the
    /// feed owner signed it); only `verify(&queried_address)`, which pins
    /// `keccak256(id || owner)` to the queried slot, catches the substitution.
    /// Exercises both finders.
    #[tokio::test]
    async fn epoch_owner_signed_wrong_epoch_chunk_is_rejected() {
        let signer = PrivateKeySigner::random();
        let owner = signer.address();
        let feed = Feed::<DEFAULT_BODY_SIZE>::new(Topic::from_bytes(b"epoch-wrong-slot"), owner);

        // The feed owner signs an update id derived from a level-1 epoch, but
        // the store hands it back for every queried slot (including the root the
        // finders probe first). Its address is `keccak256(Id(other_epoch) ||
        // owner)`, which does not match the queried slot's address, so address
        // verification must reject it.
        let other_epoch = Epoch::new(2, 1);
        let wrong_id = feed.update_id(&other_epoch);
        let wrong_soc =
            SingleOwnerChunk::<DEFAULT_BODY_SIZE>::new(wrong_id, b"wrong-slot".to_vec(), &signer)
                .expect("build wrong-slot soc");
        // Sanity: the chunk addresses to the other epoch, not the root slot the
        // finders probe first.
        assert_eq!(wrong_soc.address(), &feed.update_address(&other_epoch));
        assert_ne!(wrong_soc.address(), &feed.update_address(&Epoch::ROOT));

        let store = SubstitutingStore {
            chunk: wrong_soc.into(),
        };

        for which in ["concurrent", "sequential"] {
            let result = if which == "concurrent" {
                Epoch::find_at(&feed, &store, 1000, 0).await
            } else {
                Epoch::find_at_sequential(&feed, &store, 1000, 0).await
            };
            assert!(
                result.is_err(),
                "{which} finder must reject an owner-signed chunk for a different epoch, got {result:?}",
            );
        }
    }

    /// A level-0 epoch is the finest grid resolution and has no child. Advancing
    /// the cursor at the same second (a sub-second republish onto a level-0
    /// epoch) must not panic or shift past the width of a `u64`; it returns a
    /// defined level-0 epoch.
    #[test]
    fn level_zero_republish_floors_without_panic() {
        let at = 1_700_000_000u64;

        // Walk the cursor down to a level-0 epoch by republishing at the same
        // second repeatedly; each step descends one level until it floors.
        let mut cursor = EpochCursor::new();
        let mut last = Epoch::ROOT;
        for _ in 0..(MAX_LEVEL as usize + 4) {
            let epoch = cursor.index_for(Some(at));
            cursor.record(&epoch, Some(at));
            last = epoch;
        }
        assert_eq!(
            last.level, 0,
            "republishing at one second floors at level 0"
        );

        // The level-0 epoch contains the second and spans exactly one second.
        assert_eq!(last.length(), 1);
        assert!(last.start <= at && at < last.start + last.length());

        // next() on a level-0 epoch returns it unchanged rather than
        // underflowing the level.
        let floored = Epoch::next(Some(last), at, at);
        assert_eq!(floored, last, "next on a level-0 epoch is the epoch itself");

        // child_at on a level-0 epoch floors to the same epoch.
        assert_eq!(last.child_at(at), last);

        // length() never shifts past the width of a u64 for an out-of-grid level.
        assert_eq!(Epoch::new(0, 64).length(), u64::MAX);
        assert_eq!(Epoch::new(0, 200).length(), u64::MAX);
    }
}
