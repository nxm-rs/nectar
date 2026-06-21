//! Sequence feed scheme: a monotonic big-endian `u64` counter.
//!
//! The default feed scheme. Updates are appended at sequence numbers
//! `0, 1, 2, ...`; the index marshals to 8 big-endian bytes (never hashed)
//! mixed into the update id. Latest-lookup walks forward from a known floor.

use futures::stream::{FuturesUnordered, StreamExt};
use nectar_primitives::store::ChunkGet;

use crate::error::Result;
use crate::feed::Feed;
use crate::index::{Index, LatestFinder, WriteCursor};
use crate::reader::fetch_update;
use crate::update::FeedUpdate;

/// The number of concurrent lookaheads the async finder launches per interval.
///
/// A level `l` probes the offset `2^l - 1` from the interval base, so eight
/// levels span `2^8` updates per round of concurrent gets.
const DEFAULT_LEVELS: u32 = 8;

/// A sequence index: a monotonic `u64` counter.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, derive_more::From, derive_more::Into,
)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Sequence(pub u64);

impl Sequence {
    /// The sequence number.
    pub const fn get(&self) -> u64 {
        self.0
    }
}

impl Index for Sequence {
    fn marshal(&self) -> impl AsRef<[u8]> {
        self.0.to_be_bytes()
    }

    fn first() -> Self {
        Self(0)
    }
}

/// The write head for the sequence scheme: the next sequence number to publish.
///
/// The cursor ignores the update timestamp and increments a monotonic counter.
/// When the last published index is `u64::MAX` the counter has no successor; the
/// cursor reports itself [`exhausted`](WriteCursor::exhausted) so the writer
/// surfaces [`crate::FeedError::IndexExhausted`] rather than wrapping or
/// panicking.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SequenceCursor {
    /// The next sequence number to publish, or `None` once the space is spent.
    next: Option<u64>,
}

impl SequenceCursor {
    /// A cursor starting at sequence number zero.
    pub const fn new() -> Self {
        Self { next: Some(0) }
    }

    /// A cursor starting at a specific sequence number.
    pub const fn at(next: u64) -> Self {
        Self { next: Some(next) }
    }
}

impl WriteCursor for SequenceCursor {
    type Index = Sequence;

    fn index_for(&self, _at: Option<u64>) -> Self::Index {
        // On an exhausted cursor the writer short-circuits on `exhausted()`
        // before reaching here; `u64::MAX` is a defensive fallback.
        Sequence(self.next.unwrap_or(u64::MAX))
    }

    fn record(&mut self, index: &Self::Index, _at: Option<u64>) {
        // `None` once the published index has no successor; the next publish
        // then trips `exhausted()` in the writer.
        self.next = index.0.checked_add(1);
    }

    fn exhausted(&self) -> bool {
        self.next.is_none()
    }
}

impl LatestFinder for Sequence {
    fn find_latest<const BS: usize, G: ChunkGet<BS>>(
        feed: &Feed<BS>,
        store: &G,
        at: Option<u64>,
        after: Option<&Self>,
    ) -> impl core::future::Future<Output = Result<Option<FeedUpdate<Self, BS>>>> {
        // The sequence scheme is not time-indexed: the latest update is the
        // highest present counter, independent of any target time.
        let _ = at;
        // `after` is an index already known to exist, so the first candidate is
        // the slot immediately above it; with no hint the search starts at zero.
        let from = after.map_or(0, |a| a.0.saturating_add(1));
        find_latest_async(feed, store, from)
    }
}

/// Probe the update at sequence `idx`.
///
/// A genuine store miss resolves to `Ok(None)` (the floor of the feed);
/// [`fetch_update`] classifies that through [`ChunkGet::is_not_found`]. Every
/// other error propagates so the search surfaces a backend failure rather than
/// mistaking it for the end of the feed.
async fn probe<const BS: usize, G: ChunkGet<BS>>(
    feed: &Feed<BS>,
    store: &G,
    idx: u64,
) -> Result<Option<FeedUpdate<Sequence, BS>>> {
    let address = feed.update_address(&Sequence(idx));
    fetch_update(feed, store, Sequence(idx), &address).await
}

/// Linear baseline scan from `from` upward.
///
/// Walks one slot at a time: the first miss stops the scan and the last present
/// chunk is the latest. Returns `(latest, current, next)` where `current` is the
/// index of the last present update (or `None` if `from` itself is missing) and
/// `next` is the first missing index. This is the simple, allocation-free
/// definition the concurrent finder is measured against.
#[cfg(test)]
async fn find_latest_linear<const BS: usize, G: ChunkGet<BS>>(
    feed: &Feed<BS>,
    store: &G,
    from: u64,
) -> Result<(Option<FeedUpdate<Sequence, BS>>, Option<Sequence>, Sequence)> {
    let mut latest = None;
    let mut current = None;
    let mut idx = from;
    loop {
        match probe(feed, store, idx).await? {
            Some(update) => {
                latest = Some(update);
                current = Some(Sequence(idx));
                idx += 1;
            }
            None => return Ok((latest, current, Sequence(idx))),
        }
    }
}

/// A single probe outcome collected from a concurrent interval round: the level
/// within the interval, the absolute sequence index, and the update if present.
struct Probe<const BS: usize> {
    level: u32,
    index: u64,
    update: Option<FeedUpdate<Sequence, BS>>,
}

/// The highest level at which an update was found within the current interval.
struct Found<const BS: usize> {
    level: u32,
    index: u64,
    update: FeedUpdate<Sequence, BS>,
}

/// Async exponential-then-binary latest-update search.
///
/// Probes the interval base first; if it is absent the feed has no update at or
/// past the search floor, so the latest is `None`. Otherwise it launches
/// `DEFAULT_LEVELS` concurrent gets at offsets `base + (2^l - 1)` for descending
/// levels, tracking the highest found level and the lowest not-found level with
/// stale-result guards. When those two levels meet, a level-0 match is the
/// answer and any higher match brackets the latest into a sub-interval that is
/// searched the same way. A match at a sub-interval's maximum level proves the
/// very next slot is empty, so the search can return immediately.
async fn find_latest_async<const BS: usize, G: ChunkGet<BS>>(
    feed: &Feed<BS>,
    store: &G,
    from: u64,
) -> Result<Option<FeedUpdate<Sequence, BS>>> {
    // Probe the base of the search. Absent base means an empty feed (or nothing
    // past the hint), matching the linear scan's `from` miss.
    let Some(base_update) = probe(feed, store, from).await? else {
        return Ok(None);
    };

    let mut base = from;
    // The maximum level to probe within the current interval. The top-level
    // interval always uses the full `DEFAULT_LEVELS` lookahead; sub-intervals
    // narrow it to the bracket that contains the latest update.
    let mut level = DEFAULT_LEVELS;
    let mut not_found = DEFAULT_LEVELS;
    let mut found = Found {
        level: 0,
        index: base,
        update: base_update,
    };

    loop {
        // Launch concurrent gets across the interval at descending levels.
        let mut round = FuturesUnordered::new();
        for l in (1..=level).rev() {
            // A probe offset past `u64::MAX` cannot address a real slot, so skip
            // out-of-range levels rather than overflowing the add. The probe
            // space is naturally bounded once a slot misses, so the search still
            // converges on the highest present index.
            let Some(index) = base.checked_add((1u64 << l) - 1) else {
                continue;
            };
            round.push(async move {
                let update = probe(feed, store, index).await?;
                Ok::<_, crate::FeedError>(Probe {
                    level: l,
                    index,
                    update,
                })
            });
        }

        let mut bracket = None;
        while let Some(result) = round.next().await {
            let Probe {
                level: l,
                index,
                update,
            } = result?;
            match update {
                None => {
                    // Ignore a miss above an already-recorded miss: the lower
                    // not-found level is the tighter bound.
                    if not_found < l {
                        continue;
                    }
                    not_found = l - 1;
                }
                Some(update) => {
                    // Ignore a hit below the highest found level: a higher level
                    // already proved a later update exists.
                    if found.level > l {
                        continue;
                    }
                    // A hit at the interval's maximum level inside a sub-interval
                    // proves `index + 1` is empty (the parent interval already
                    // bounded the latest below the next level), so return now.
                    if level == l && l < DEFAULT_LEVELS {
                        return Ok(Some(update));
                    }
                    found = Found {
                        level: l,
                        index,
                        update,
                    };
                }
            }

            // Found and not-found levels meet: the latest is bracketed.
            if found.level == not_found {
                if found.level == 0 {
                    // The base of this interval is itself the latest update.
                    bracket = Some(Bracket::Done);
                } else {
                    // Recurse into the sub-interval rooted at the latest hit.
                    bracket = Some(Bracket::Recurse);
                }
                break;
            }

            // Inconsistent feed: a miss landed below a confirmed hit. Rescan the
            // sub-interval up to the found level to resolve the contradiction.
            if not_found < found.level {
                bracket = Some(Bracket::Retry);
                break;
            }
        }

        match bracket {
            // The interval drained without the levels meeting: the full
            // lookahead found nothing past the base, so the base is the latest.
            None | Some(Bracket::Done) => return Ok(Some(found.update)),
            Some(Bracket::Recurse) => {
                base = found.index;
                level = found.level;
                not_found = found.level;
                found.level = 0;
            }
            Some(Bracket::Retry) => {
                let resume = level;
                base = found.index;
                level = resume;
                not_found = resume;
                found.level = 0;
            }
        }
    }
}

/// How an interval round resolved: the latest is the base, recurse into the
/// bracketed sub-interval, or rescan to resolve an inconsistent feed.
enum Bracket {
    Done,
    Recurse,
    Retry,
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::Address;
    use proptest::prelude::*;

    use crate::topic::Topic;

    #[test]
    fn marshal_is_8_be_bytes() {
        assert_eq!(Sequence(0).marshal().as_ref(), [0u8; 8].as_slice());
        assert_eq!(
            Sequence(1).marshal().as_ref(),
            1u64.to_be_bytes().as_slice()
        );
        assert_eq!(
            Sequence(u64::MAX).marshal().as_ref(),
            [0xffu8; 8].as_slice()
        );
    }

    #[test]
    fn first_is_zero() {
        assert_eq!(Sequence::first(), Sequence(0));
    }

    #[test]
    fn cursor_advances_monotonically() {
        let mut c = SequenceCursor::new();
        assert_eq!(c.index_for(None), Sequence(0));
        c.record(&Sequence(0), None);
        assert_eq!(c.index_for(None), Sequence(1));
        c.record(&Sequence(1), Some(12345));
        assert_eq!(c.index_for(None), Sequence(2));
        assert!(!c.exhausted());
    }

    #[test]
    fn cursor_reports_exhaustion_at_max() {
        let mut c = SequenceCursor::at(u64::MAX);
        assert_eq!(c.index_for(None), Sequence(u64::MAX));
        assert!(!c.exhausted());
        c.record(&Sequence(u64::MAX), None);
        assert!(c.exhausted(), "no successor to u64::MAX");
    }

    proptest! {
        #[test]
        fn marshal_round_trips_be(n in any::<u64>()) {
            let seq = Sequence(n);
            let bytes = seq.marshal();
            let arr: [u8; 8] = bytes.as_ref().try_into().unwrap();
            prop_assert_eq!(u64::from_be_bytes(arr), n);
        }

        #[test]
        fn address_is_deterministic(n in any::<u64>(), topic in any::<[u8; 32]>(), owner in any::<[u8; 20]>()) {
            let feed: Feed = Feed::new(Topic::new(topic.into()), Address::from(owner));
            let a = feed.update_address(&Sequence(n));
            let b = feed.update_address(&Sequence(n));
            prop_assert_eq!(a, b);
        }
    }

    mod finder {
        use super::*;
        use alloy_signer_local::PrivateKeySigner;
        use nectar_primitives::DEFAULT_BODY_SIZE;
        use nectar_primitives::chunk::SingleOwnerChunk;
        use nectar_primitives::store::{MemoryStore, SyncChunkPut};

        // A fixed signer so the feed owner and the SOC signatures agree across a
        // test, and the derived addresses are reproducible.
        fn signer() -> PrivateKeySigner {
            PrivateKeySigner::from_slice(&alloy_primitives::hex!(
                "2c7536e3605d9c16a7a3d7b1898e529396a65c23a3bcbd4012a11cf2731b0fbc"
            ))
            .unwrap()
        }

        fn feed_for(s: &PrivateKeySigner) -> Feed {
            Feed::new(Topic::from_bytes(b"latest-finder"), s.address())
        }

        // Publish contiguous updates at indices `0..n` into a fresh store.
        fn store_with(feed: &Feed, s: &PrivateKeySigner, n: u64) -> MemoryStore {
            let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
            for i in 0..n {
                let id = feed.update_id(&Sequence(i));
                let soc = SingleOwnerChunk::<DEFAULT_BODY_SIZE>::new(id, vec![i as u8], s).unwrap();
                SyncChunkPut::put(&store, soc.into()).unwrap();
            }
            store
        }

        // Publish an update at one explicit index into `store`, with `payload`.
        fn put_at(store: &MemoryStore, feed: &Feed, s: &PrivateKeySigner, idx: u64, payload: u8) {
            let id = feed.update_id(&Sequence(idx));
            let soc = SingleOwnerChunk::<DEFAULT_BODY_SIZE>::new(id, vec![payload], s).unwrap();
            SyncChunkPut::put(store, soc.into()).unwrap();
        }

        // Publish updates at an explicit set of (possibly non-contiguous) indices.
        fn store_with_indices(feed: &Feed, s: &PrivateKeySigner, indices: &[u64]) -> MemoryStore {
            let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
            for &i in indices {
                put_at(&store, feed, s, i, i as u8);
            }
            store
        }

        // Run a future to completion on a single-threaded current-thread runtime;
        // the finder inherits no `Send` bound, so a current-thread executor is the
        // honest harness.
        fn block_on<F: core::future::Future>(fut: F) -> F::Output {
            tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap()
                .block_on(fut)
        }

        #[test]
        fn async_finds_last_of_many() {
            let s = signer();
            let feed = feed_for(&s);
            // Span more than one DEFAULT_LEVELS round to exercise sub-interval
            // recursion.
            const N: u64 = 300;
            let store = store_with(&feed, &s, N);

            let got = block_on(Sequence::find_latest(&feed, &store, None, None)).unwrap();
            let update = got.expect("non-empty feed has a latest update");
            assert_eq!(*update.index(), Sequence(N - 1));

            let next = block_on(Sequence::find_latest(
                &feed,
                &store,
                None,
                Some(&Sequence(N - 1)),
            ))
            .unwrap();
            assert!(next.is_none(), "nothing exists past the last index");
        }

        #[test]
        fn async_matches_linear_scan() {
            let s = signer();
            let feed = feed_for(&s);
            for n in [1u64, 2, 3, 5, 8, 9, 16, 17, 100] {
                let store = store_with(&feed, &s, n);
                let async_got = block_on(Sequence::find_latest(&feed, &store, None, None)).unwrap();
                let (linear_got, current, next) =
                    block_on(find_latest_linear(&feed, &store, 0)).unwrap();

                assert_eq!(current, Some(Sequence(n - 1)), "n = {n}");
                assert_eq!(next, Sequence(n), "n = {n}");
                let async_index = async_got.as_ref().map(|u| *u.index());
                assert_eq!(
                    async_index,
                    linear_got.map(|u| *u.index()),
                    "async and linear disagree at n = {n}",
                );
                assert_eq!(async_index, Some(Sequence(n - 1)), "n = {n}");
            }
        }

        #[test]
        fn single_update() {
            let s = signer();
            let feed = feed_for(&s);
            let store = store_with(&feed, &s, 1);

            let got = block_on(Sequence::find_latest(&feed, &store, None, None)).unwrap();
            assert_eq!(got.map(|u| *u.index()), Some(Sequence(0)));
        }

        #[test]
        fn empty_feed() {
            let s = signer();
            let feed = feed_for(&s);
            let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();

            let got = block_on(Sequence::find_latest(&feed, &store, None, None)).unwrap();
            assert!(got.is_none());

            let (linear, current, next) = block_on(find_latest_linear(&feed, &store, 0)).unwrap();
            assert!(linear.is_none());
            assert_eq!(current, None);
            assert_eq!(next, Sequence(0));
        }

        // A near-`u64::MAX` search floor with a present update at `u64::MAX`. The
        // concurrent finder must not overflow when computing probe offsets above
        // the present index, and must resolve the extreme index without panic.
        #[test]
        fn async_handles_index_near_u64_max() {
            let s = signer();
            let feed = feed_for(&s);
            let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
            put_at(&store, &feed, &s, u64::MAX, 0xab);

            // Floor at `u64::MAX - 1`, so the search starts at `u64::MAX` and the
            // first concurrent round would otherwise add a positive offset to it.
            let got = block_on(Sequence::find_latest(
                &feed,
                &store,
                None,
                Some(&Sequence(u64::MAX - 1)),
            ))
            .unwrap();
            assert_eq!(got.map(|u| *u.index()), Some(Sequence(u64::MAX)));

            // A floor of `u64::MAX` itself saturates the start back to `u64::MAX`
            // (there is no successor slot to begin at), so the search probes that
            // extreme index without overflowing the probe offset.
            let at_max = block_on(Sequence::find_latest(
                &feed,
                &store,
                None,
                Some(&Sequence(u64::MAX)),
            ))
            .unwrap();
            assert_eq!(at_max.map(|u| *u.index()), Some(Sequence(u64::MAX)));
        }

        // A sparse feed (present at 0,1,2; hole at 3; present at 4) exercises the
        // inconsistent-feed Retry branch and the stale-result guards. The finder
        // defines "latest" as the highest present index reachable from the floor
        // without crossing a hole, so it converges on index 2.
        #[test]
        fn async_sparse_feed_stops_at_first_hole() {
            let s = signer();
            let feed = feed_for(&s);
            let store = store_with_indices(&feed, &s, &[0, 1, 2, 4]);

            let async_got = block_on(Sequence::find_latest(&feed, &store, None, None)).unwrap();
            let (linear_got, current, next) =
                block_on(find_latest_linear(&feed, &store, 0)).unwrap();

            assert_eq!(current, Some(Sequence(2)), "contiguous prefix ends at 2");
            assert_eq!(next, Sequence(3), "first hole is at index 3");
            assert_eq!(
                async_got.map(|u| *u.index()),
                linear_got.map(|u| *u.index()),
                "async and linear agree on the sparse feed",
            );
        }

        // Two updates signed for the same index share one SOC address, so the
        // address-keyed store keeps whichever was put last. The finder has no
        // freshness signal to detect this and returns the last writer's payload.
        // This pins the address-keyed last-writer-wins behaviour as intended.
        #[test]
        fn equivocating_index_is_last_writer_wins() {
            let s = signer();
            let feed = feed_for(&s);
            let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();

            put_at(&store, &feed, &s, 0, 0x01);
            put_at(&store, &feed, &s, 0, 0x02);

            let got = block_on(Sequence::find_latest(&feed, &store, None, None))
                .unwrap()
                .expect("a present update");
            assert_eq!(*got.index(), Sequence(0));
            assert_eq!(
                got.payload().as_ref(),
                &[0x02],
                "the later put for the same index wins",
            );
        }
    }
}
