//! Read side: fetch, certify and interpret updates over a chunk store.

use core::convert::Infallible;
use core::fmt;
use core::future::poll_fn;
use core::num::{NonZeroU16, NonZeroUsize};
use std::collections::BTreeSet;

use nectar_governor::{Admission, BoxFuture, Driver, FuturesUnordered, WalkPolicy, Window};
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
    async fn drive(
        &self,
        floor: Sequence,
        resolve: fn(u64, &Answers) -> Step,
    ) -> Result<Latest<BODY_SIZE>> {
        let mut driver = Driver::new(ProbePolicy::new(
            &self.feed,
            &self.store,
            resolve,
            floor,
            self.window,
        ));
        loop {
            match poll_fn(|cx| driver.poll(cx, ())).await {
                Some(Ok(Verdict::Empty)) => {
                    return Ok(Latest {
                        update: None,
                        next: Some(floor),
                    });
                }
                Some(Ok(Verdict::Commit(lo))) => return self.found(lo).await,
                Some(Err(error)) => match error {},
                // A fault always keeps its own probe in flight, so the set
                // cannot drain verdictless; resuming over the retained
                // answers keeps this arm total without a panic path.
                None => driver = Driver::new(driver.into_policy()),
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

/// One landed presence answer, routed by index.
type Probed = (u64, bool);

/// Terminal outcome of a finder walk.
enum Verdict {
    /// The floor slot is absent.
    Empty,
    /// The boundary update lives at this index.
    Commit(u64),
}

/// Presence-probe walk policy: each landed answer re-resolves the replay and
/// tops the window back up.
///
/// The head slot is the faulting index; [`Admission`] reserves it a slot, so
/// speculation never starves the probe the replay is blocked on.
struct ProbePolicy<'a, S, const BODY_SIZE: usize> {
    feed: &'a Feed<BODY_SIZE>,
    store: &'a S,
    resolve: fn(u64, &Answers) -> Step,
    /// The search floor the resolver replays from.
    base: u64,
    /// Probe-plan width; the admission window is its clamp.
    width: NonZeroUsize,
    admission: Admission,
    answers: Answers,
    /// Indices probed but not yet landed.
    outstanding: BTreeSet<u64>,
    /// Scratch for the fault's probe plan.
    plan: Vec<u64>,
    /// Staged terminal outcome awaiting hand-over.
    verdict: Option<Verdict>,
}

impl<'a, S, const BODY_SIZE: usize> ProbePolicy<'a, S, BODY_SIZE> {
    fn new(
        feed: &'a Feed<BODY_SIZE>,
        store: &'a S,
        resolve: fn(u64, &Answers) -> Step,
        floor: Sequence,
        width: NonZeroUsize,
    ) -> Self {
        let slots = NonZeroU16::try_from(width).unwrap_or(NonZeroU16::MAX);
        Self {
            feed,
            store,
            resolve,
            base: floor.get(),
            width,
            admission: Admission::new(Window::from(slots)),
            answers: Answers::new(),
            outstanding: BTreeSet::new(),
            plan: Vec::new(),
            verdict: None,
        }
    }
}

impl<'a, S, const BODY_SIZE: usize> WalkPolicy<'a> for ProbePolicy<'a, S, BODY_SIZE>
where
    S: ChunkHas,
{
    type Fetched = Probed;
    type Frame = Verdict;
    type Error = Infallible;
    type Drain = ();

    fn admit(&mut self, in_flight: &mut FuturesUnordered<BoxFuture<'a, Probed>>) {
        if self.verdict.is_some() {
            return;
        }
        let fault = match (self.resolve)(self.base, &self.answers) {
            Step::Empty => {
                self.verdict = Some(Verdict::Empty);
                return;
            }
            Step::Commit { lo } => {
                self.verdict = Some(Verdict::Commit(lo));
                return;
            }
            Step::Fault(fault) => fault,
        };
        fault.plan(self.width, &mut self.plan);
        // The faulting index leads the plan: it is the head slot.
        let head = self.plan.first().copied();
        for &index in &self.plan {
            if self.answers.contains_key(&index) || self.outstanding.contains(&index) {
                continue;
            }
            let head_served =
                head.is_some_and(|head| index == head || self.outstanding.contains(&head));
            if !self.admission.admits(self.outstanding.len(), head_served) {
                break;
            }
            let address = self.feed.update_address(&Sequence::new(index));
            let store = self.store;
            self.outstanding.insert(index);
            in_flight.push(Box::pin(async move { (index, store.has(&address).await) }));
        }
    }

    fn take_ready(&mut self, (): ()) -> Option<Result<Verdict, Infallible>> {
        self.verdict.take().map(Ok)
    }

    fn absorb(&mut self, (index, present): Probed) -> Result<(), Infallible> {
        self.outstanding.remove(&index);
        self.answers.insert(index, present);
        Ok(())
    }

    fn drained(&self) -> Result<(), Infallible> {
        Ok(())
    }
}
