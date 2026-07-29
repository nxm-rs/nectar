//! Read side: fetch, certify and interpret updates over a chunk store.

use core::fmt;
use core::num::{NonZeroU16, NonZeroUsize};
use std::collections::BTreeSet;

use futures_util::stream::{FuturesUnordered, StreamExt};
use nectar_governor::{Admission, Window};
use nectar_primitives::DEFAULT_BODY_SIZE;
use nectar_primitives::chunk::{Chunk, IntoVerified, SingleOwnerOnlyChunkSet};
use nectar_primitives::store::{ChunkGet, ChunkHas};

use crate::error::{FeedError, Result};
use crate::feed::Feed;
use crate::index::Index;
use crate::probe::{self, Answers, Step};
use crate::sequence::Sequence;
use crate::update::FeedUpdate;

/// Read handle over a feed: a [`Feed`] plus a chunk store.
pub struct Getter<S, const BODY_SIZE: usize = DEFAULT_BODY_SIZE> {
    feed: Feed<BODY_SIZE>,
    store: S,
    window: NonZeroUsize,
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
            .field("window", &self.window)
            .finish_non_exhaustive()
    }
}

impl<S, const BODY_SIZE: usize> Getter<S, BODY_SIZE> {
    /// Create a getter over `feed` reading from `store`.
    pub const fn new(feed: Feed<BODY_SIZE>, store: S) -> Self {
        Self {
            feed,
            store,
            window: NonZeroUsize::MIN,
        }
    }

    /// Set the concurrent presence-probe window of the latest-update
    /// finders.
    ///
    /// A store-capacity knob, not a pure speedup: a wide window can induce
    /// timeouts a presence probe must report as absence. Width one is the
    /// sequential scan; widths past `u16::MAX` clamp to it.
    #[must_use]
    pub const fn with_window(mut self, window: NonZeroUsize) -> Self {
        self.window = window;
        self
    }

    /// The feed this getter reads.
    pub const fn feed(&self) -> &Feed<BODY_SIZE> {
        &self.feed
    }
}

impl<S, const BODY_SIZE: usize> Getter<S, BODY_SIZE>
where
    S: ChunkGet<SingleOwnerOnlyChunkSet<BODY_SIZE>>,
    Chunk<S::Trust, SingleOwnerOnlyChunkSet<BODY_SIZE>>:
        IntoVerified<Registry = SingleOwnerOnlyChunkSet<BODY_SIZE>>,
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
        Ok(FeedUpdate::new(index, chunk.into_envelope()))
    }
}

impl<S, const BODY_SIZE: usize> Getter<S, BODY_SIZE>
where
    S: ChunkGet<SingleOwnerOnlyChunkSet<BODY_SIZE>> + ChunkHas,
    Chunk<S::Trust, SingleOwnerOnlyChunkSet<BODY_SIZE>>:
        IntoVerified<Registry = SingleOwnerOnlyChunkSet<BODY_SIZE>>,
{
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

    /// Run a replay resolver to a verdict over at most
    /// [`window`](Self::with_window) concurrent presence probes, absorbing
    /// each answer as it lands: no probe waits on an unrelated one.
    ///
    /// Only a completed probe answers an index; a speculative answer the
    /// replay never consults is inert, so any width and any completion order
    /// commit the boundary the sequential scan would.
    ///
    /// The faulting index leads each round's plan and is the head slot;
    /// [`Admission`] reserves it a slot, so speculation never starves the
    /// probe the replay is blocked on and the set never drains verdictless.
    async fn drive(
        &self,
        floor: Sequence,
        resolve: fn(u64, &Answers) -> Step,
    ) -> Result<Latest<BODY_SIZE>> {
        let slots = NonZeroU16::try_from(self.window).unwrap_or(NonZeroU16::MAX);
        let admission = Admission::new(Window::from(slots));
        // One clamped width: planning past what the window admits only grows
        // the scratch plan, so the clamp binds the plan too.
        let width = NonZeroUsize::from(slots);
        let base = floor.get();
        let mut answers = Answers::new();
        // Indices probed but not yet landed, and the round's probe plan.
        let mut outstanding = BTreeSet::new();
        let mut plan = Vec::new();
        let mut in_flight = FuturesUnordered::new();
        loop {
            let fault = match resolve(base, &answers) {
                Step::Empty => {
                    return Ok(Latest {
                        update: None,
                        next: Some(floor),
                    });
                }
                Step::Commit { lo } => return self.found(lo).await,
                Step::Fault(fault) => fault,
            };
            fault.plan(width, &mut plan);
            let head = plan.first().copied();
            for &index in &plan {
                if answers.contains_key(&index) || outstanding.contains(&index) {
                    continue;
                }
                let head_served =
                    head.is_some_and(|head| index == head || outstanding.contains(&head));
                if !admission.admits(outstanding.len(), head_served) {
                    break;
                }
                let address = self.feed.update_address(&Sequence::new(index));
                let store = &self.store;
                outstanding.insert(index);
                in_flight.push(async move { (index, store.has(&address).await) });
            }
            // The head is never already answered and the window always has a
            // free slot here, so the set is never empty. Were it empty the
            // re-resolve would repeat verbatim, so the loop would spin without
            // yielding; the assertion turns that into a test failure rather
            // than a wedged executor.
            debug_assert!(
                !in_flight.is_empty(),
                "probe set drained with a fault pending"
            );
            if let Some((index, present)) = in_flight.next().await {
                outstanding.remove(&index);
                answers.insert(index, present);
            }
        }
    }

    /// Latest update by exponential-then-binary probing, from index zero.
    ///
    /// Assumes gapless publication: a hole reads as the end of the feed.
    /// The returned update is certified; absence rests on unverified
    /// presence answers, so a lying or unavailable store truncates the scan
    /// to an earlier genuine update, never a forged one.
    pub async fn latest(&self) -> Result<Latest<BODY_SIZE>> {
        self.latest_from(Sequence::ZERO).await
    }

    /// [`latest`](Self::latest) from a floor slot, for resuming with a
    /// known-present hint. An absent floor yields an empty result with
    /// `next = floor`.
    pub async fn latest_from(&self, floor: Sequence) -> Result<Latest<BODY_SIZE>> {
        self.drive(floor, probe::resolve_probing).await
    }

    /// Latest update by stepwise scan from a floor slot. The baseline the
    /// probing search is measured against; the absence caveat of
    /// [`latest`](Self::latest) applies.
    pub async fn latest_linear_from(&self, floor: Sequence) -> Result<Latest<BODY_SIZE>> {
        self.drive(floor, probe::resolve_linear).await
    }
}
