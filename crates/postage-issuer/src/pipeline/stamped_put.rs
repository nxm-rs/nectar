//! Stamping store decorator: a bare-chunk sink facing up, a stamped-pair
//! sink facing down, so existing persistence call sites stamp at put with no
//! changes.

use alloc::collections::BTreeMap;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::fmt;
use core::future::{Future, poll_fn};
use core::num::NonZeroUsize;
use core::task::{Poll, Waker};

#[cfg(feature = "std")]
use alloy_signer::{Signer, SignerSync};
use nectar_clock::Clock;
#[cfg(feature = "std")]
use nectar_clock::SystemClock;
use nectar_marker::{MaybeSend, MaybeSync};
#[cfg(feature = "std")]
use nectar_postage::Batch;
use nectar_postage::{Stamp, StampDigest, StampError, StampedChunk, Validated};
use nectar_primitives::{AnyChunkSet, Chunk, ChunkAddress, ChunkGet, ChunkHas, ChunkPut, Verified};

use super::shared::{Shared, new_shared, wake_all, with_state};
#[cfg(feature = "std")]
use super::signer::BatchSigner;
#[cfg(feature = "std")]
use super::signer::Eip191;
#[cfg(not(feature = "std"))]
use super::signer::sign_digest;
use super::signer::{BoundSigner, SignPrehash, allocates_from};
#[cfg(feature = "std")]
use super::task::sign_task;
use crate::error::{IssuerError, SigningError};
use crate::issuer::StampIssuer;
use crate::stamper::stamp_timestamp;

/// Memory switch for the issued map (~145 B per unique address).
///
/// Anything below full tracking reintroduces duplicate allocation: an
/// untracked duplicate burns a fresh index, and a repetitive region can
/// refuse with `BucketFull`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IssuedBound {
    /// Track every unique address.
    #[default]
    Unbounded,
    /// Track at most this many addresses; later addresses go untracked.
    AtMost(NonZeroUsize),
    /// Track nothing.
    Off,
}

impl IssuedBound {
    /// Whether a new address enters a tracking set already holding `tracked`.
    pub const fn tracks(self, tracked: usize) -> bool {
        match self {
            Self::Off => false,
            Self::Unbounded => true,
            Self::AtMost(bound) => tracked < bound.get(),
        }
    }
}

/// A stamped put failure.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum StampedPutError<E> {
    /// Index allocation refused; no index consumed, a retry is free.
    #[error(transparent)]
    Stamp(#[from] StampError),
    /// Signing failed; the allocated index is burnt.
    #[error("stamp signing failed")]
    Sign(#[source] SigningError),
    /// The sink refused the pair; the signed stamp is retained for reuse.
    #[error("stamped sink refused the pair")]
    Put(#[source] E),
    /// An earlier failure has already surfaced; the decorator is shut.
    #[error("stamping is poisoned by an earlier failure")]
    Poisoned,
}

/// One address's stamping progress, shared across clones.
///
/// Hand-rolled over `futures::Shared`: folds duplicates onto one allocation,
/// sign and delivery, and redelivers the retained stamp on failure or
/// cancellation, which `Shared` can't express.
enum Issued {
    /// Allocated; signing in flight. Wakers re-poll when it resolves.
    Pending(Vec<Waker>),
    /// Signed; one put holds the delivery. Wakers re-poll when it settles.
    Delivering(Stamp, Vec<Waker>),
    /// Signed; no delivery in flight. The next put takes the delivery.
    Signed(Stamp),
    /// Signed and delivered to the sink; duplicates short-circuit.
    Stored,
}

/// The clone-shared issuing state: one watermark, one issued map.
struct State<I> {
    issuer: I,
    issued: BTreeMap<ChunkAddress, Issued>,
    bound: IssuedBound,
}

impl<I> State<I> {
    fn tracks(&self) -> bool {
        self.bound.tracks(self.issued.len())
    }
}

type SharedState<I> = Shared<State<I>>;

/// Publishes a sign outcome to the issued map; returns the wakers to wake
/// after the lock drops. A failure removes the entry, so a later put
/// allocates afresh instead of wedging on a dead `Pending`.
fn resolve<I>(
    state: &mut State<I>,
    address: &ChunkAddress,
    result: &Result<Stamp, SigningError>,
    tracked: bool,
) -> Vec<Waker> {
    if !tracked {
        return Vec::new();
    }
    let wakers = match state.issued.get_mut(address) {
        Some(Issued::Pending(wakers)) => core::mem::take(wakers),
        _ => Vec::new(),
    };
    match result {
        Ok(stamp) => {
            state.issued.insert(*address, Issued::Signed(stamp.clone()));
        }
        Err(_) => {
            state.issued.remove(address);
        }
    }
    wakers
}

/// Ends a held delivery: swaps `Delivering` for `next(stamp)` and returns
/// the parked wakers.
fn end_delivery<I>(
    shared: &SharedState<I>,
    address: &ChunkAddress,
    next: impl FnOnce(&Stamp) -> Issued,
) -> Vec<Waker> {
    with_state(shared, |state| match state.issued.get_mut(address) {
        Some(Issued::Delivering(stamp, wakers)) => {
            let woken = core::mem::take(wakers);
            let replacement = next(stamp);
            state.issued.insert(*address, replacement);
            woken
        }
        _ => Vec::new(),
    })
}

/// Held delivery of one address. Dropping without [`stored`](Self::stored)
/// hands the signed stamp back, so a failed or cancelled delivery never
/// wedges parked duplicates.
struct DeliveryGuard<'a, I> {
    shared: &'a SharedState<I>,
    address: ChunkAddress,
    armed: bool,
}

impl<I> DeliveryGuard<'_, I> {
    /// Marks the address stored and wakes parked duplicates.
    fn stored(mut self) {
        self.armed = false;
        wake_all(end_delivery(self.shared, &self.address, |_| Issued::Stored));
    }
}

impl<I> Drop for DeliveryGuard<'_, I> {
    fn drop(&mut self) {
        if self.armed {
            wake_all(end_delivery(self.shared, &self.address, |stamp| {
                Issued::Signed(stamp.clone())
            }));
        }
    }
}

/// Signs one digest inline through the crate's sole panic boundary,
/// converting a signer panic into [`SigningError::Dropped`].
#[cfg(all(feature = "std", not(feature = "parallel")))]
fn sign_now<Sg: SignPrehash + ?Sized>(
    signer: &Sg,
    digest: &StampDigest,
) -> Result<Stamp, SigningError> {
    sign_task(signer, digest).result
}

/// Signs one digest inline. Without `std` there is no unwind boundary: a
/// signer panic propagates.
#[cfg(not(any(feature = "std", feature = "parallel")))]
fn sign_now<Sg: SignPrehash + ?Sized>(
    signer: &Sg,
    digest: &StampDigest,
) -> Result<Stamp, SigningError> {
    sign_digest(signer, digest)
}

/// One step of the put flow, decided under a single lock.
enum Step {
    /// The address is already stored; nothing to do.
    Done,
    /// This put took the delivery of the already-signed stamp.
    Deliver(Stamp),
    /// This put owns the allocation and drives the signing.
    Own { digest: StampDigest, tracked: bool },
    /// Another put signs or delivers this address; wait for it.
    Wait,
    /// Allocation refused.
    Refused(StampError),
}

/// Stamping decorator over a stamped-pair sink.
///
/// Takes bare chunks and puts validated pairs: the signer is bound to the
/// batch owner at construction.
///
/// # Contracts
///
/// - Clone-shared state: every clone drives one issuer watermark and one
///   issued map. The issuer is held behind a shared handle and never cloned.
/// - Per-address idempotence: the issued map is consulted before
///   allocation. A duplicate put reuses the signed stamp, or returns
///   without a second sink put once the first succeeded; in-flight
///   duplicates share one allocation and one sink delivery, a failed or
///   cancelled delivery handing the signed stamp to the next put. Cost is
///   ~145 B per unique address; see [`IssuedBound`] for the bound and off
///   switches.
/// - The decorator stamps per put, not per reachable chunk: a wrapped
///   re-commit stamps only newly-put chunks.
/// - Transport retries compose below the pair sink, reusing the already
///   signed stamp; a re-put through the decorator is idempotent anyway.
/// - Wrapping a purely local store burns indices for chunks that may never
///   reach the network; filter for presence upstream where that matters.
/// - Put-only sites need `P: ChunkPut<StampedChunk<Verified, Validated, B>>`;
///   commit and apply sites need `TrustedGet + ChunkHas` as well, so a pure
///   network sender takes a local or teed inner there.
/// - A split's put slots double as sign-plus-put slots here, so signing
///   never overlaps wider than the put window; take
///   [`StagedPut`](super::StagedPut) for a slow signer, which signs in a
///   stage of its own. Prefer an owned clone (`Split::new` or `collect`)
///   over a borrowed relay, which serializes one signer round-trip per
///   chunk. The inline engine signs on the driving thread, so async callers
///   drive a split inside a blocking task.
///
/// # Example
///
/// ```
/// use alloy_signer::Signer;
/// use alloy_signer_local::PrivateKeySigner;
/// use nectar_postage_issuer::{
///     Batch, BatchId, BucketDepth, MemoryIssuer, StampIndifferent, StampedPut,
/// };
/// use nectar_primitives::{AnyChunkSet, MemoryStore};
///
/// let signer = PrivateKeySigner::random();
/// let batch: Batch =
///     Batch::new(BatchId::ZERO, 1_000, 0, signer.address(), 20, BucketDepth::new(16)?, true);
/// let issuer = MemoryIssuer::from_batch(&batch)?;
/// let sink = StampIndifferent::new(MemoryStore::<AnyChunkSet<4096>>::new());
/// let store = StampedPut::from_signer(issuer, signer, sink, batch)?;
/// assert_eq!(store.remaining_capacity(), 16);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[cfg(feature = "std")]
pub struct StampedPut<I, Sg, P, C = SystemClock> {
    shared: SharedState<I>,
    signer: Arc<Sg>,
    inner: P,
    clock: C,
}

/// Without `std` there is no default clock; construct via
/// [`with_parts`](Self::with_parts).
#[cfg(not(feature = "std"))]
pub struct StampedPut<I, Sg, P, C> {
    shared: SharedState<I>,
    signer: Arc<Sg>,
    inner: P,
    clock: C,
}

impl<I, Sg, P, C> fmt::Debug for StampedPut<I, Sg, P, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StampedPut").finish_non_exhaustive()
    }
}

impl<I, Sg, P: Clone, C: Clone> Clone for StampedPut<I, Sg, P, C> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
            signer: Arc::clone(&self.signer),
            inner: self.inner.clone(),
            clock: self.clock.clone(),
        }
    }
}

#[cfg(feature = "std")]
impl<I, Sg, P> StampedPut<I, Sg, P>
where
    I: StampIssuer,
    Sg: BoundSigner<Spec = I::Spec>,
{
    /// Creates a decorator reading stamp timestamps from the system clock,
    /// tracking every unique address.
    ///
    /// # Errors
    ///
    /// [`IssuerError::BatchMismatch`] when the issuer allocates from a batch
    /// the signer does not own.
    pub fn new(issuer: I, signer: Sg, inner: P) -> Result<Self, IssuerError> {
        Self::with_parts(issuer, signer, inner, SystemClock)
    }
}

#[cfg(feature = "std")]
impl<I, K, P> StampedPut<I, BatchSigner<Eip191<K>, I::Spec>, P>
where
    I: StampIssuer,
    K: SignerSync + Signer,
{
    /// [`new`](Self::new) over the [`Eip191`] adapter, binding `signer` to
    /// the batch it owns.
    ///
    /// # Errors
    ///
    /// [`IssuerError::NotBatchOwner`] when the signer is not the batch owner,
    /// or [`IssuerError::BatchMismatch`] when the issuer serves another batch.
    pub fn from_signer(
        issuer: I,
        signer: K,
        inner: P,
        batch: Batch<I::Spec>,
    ) -> Result<Self, IssuerError> {
        Self::new(issuer, BatchSigner::from_signer(signer, batch)?, inner)
    }
}

impl<I, Sg, P, C> StampedPut<I, Sg, P, C>
where
    I: StampIssuer,
    Sg: BoundSigner<Spec = I::Spec>,
{
    /// Creates a decorator from explicit parts, tracking every unique
    /// address.
    ///
    /// # Errors
    ///
    /// [`IssuerError::BatchMismatch`] or [`IssuerError::BucketDepthMismatch`]
    /// when the issuer does not allocate from the batch the signer owns.
    pub fn with_parts(issuer: I, signer: Sg, inner: P, clock: C) -> Result<Self, IssuerError> {
        allocates_from(&issuer, &signer)?;
        Ok(Self {
            shared: new_shared(State {
                issuer,
                issued: BTreeMap::new(),
                bound: IssuedBound::Unbounded,
            }),
            signer: Arc::new(signer),
            inner,
            clock,
        })
    }
}

impl<I, Sg, P, C> StampedPut<I, Sg, P, C> {
    /// Replaces the issued-map bound. Applies to every clone: the map is
    /// clone-shared.
    #[must_use]
    pub fn with_issued_bound(self, bound: IssuedBound) -> Self {
        with_state(&self.shared, |state| state.bound = bound);
        self
    }

    /// Replaces the timestamp source.
    #[must_use]
    pub fn with_clock<D>(self, clock: D) -> StampedPut<I, Sg, P, D> {
        StampedPut {
            shared: self.shared,
            signer: self.signer,
            inner: self.inner,
            clock,
        }
    }

    /// The wrapped sink.
    pub const fn inner(&self) -> &P {
        &self.inner
    }
}

impl<I: StampIssuer, Sg, P, C> StampedPut<I, Sg, P, C> {
    /// Free slots in the fullest bucket. Preflight a nearly-full batch
    /// before a split: one refused put poisons it.
    pub fn remaining_capacity(&self) -> u32 {
        with_state(&self.shared, |state| {
            state
                .issuer
                .bucket_capacity()
                .saturating_sub(state.issuer.max_bucket_utilization())
        })
    }
}

impl<I, Sg, P, C> StampedPut<I, Sg, P, C>
where
    I: StampIssuer + MaybeSend + 'static,
    Sg: SignPrehash + MaybeSend + MaybeSync + 'static,
    C: Clock,
{
    /// Decides one put step under a single lock: consult the issued map
    /// before allocating.
    fn step(&self, address: &ChunkAddress) -> Step {
        with_state(&self.shared, |state| match state.issued.get(address) {
            Some(Issued::Stored) => Step::Done,
            Some(Issued::Signed(stamp)) => {
                let stamp = stamp.clone();
                state
                    .issued
                    .insert(*address, Issued::Delivering(stamp.clone(), Vec::new()));
                Step::Deliver(stamp)
            }
            Some(Issued::Pending(_) | Issued::Delivering(..)) => Step::Wait,
            None => {
                let tracked = state.tracks();
                match state.issuer.reserve(address, stamp_timestamp(&self.clock)) {
                    Ok(permit) => {
                        if tracked {
                            state.issued.insert(*address, Issued::Pending(Vec::new()));
                        }
                        Step::Own {
                            digest: permit.digest(),
                            tracked,
                        }
                    }
                    Err(error) => Step::Refused(error),
                }
            }
        })
    }

    /// Waits while another put signs or delivers this address,
    /// re-registering the waker on every poll (the first registration is
    /// the split's noop waker).
    async fn wait_progress(&self, address: &ChunkAddress) {
        poll_fn(|cx| {
            with_state(&self.shared, |state| match state.issued.get_mut(address) {
                Some(Issued::Pending(wakers) | Issued::Delivering(_, wakers)) => {
                    if !wakers.iter().any(|waker| waker.will_wake(cx.waker())) {
                        wakers.push(cx.waker().clone());
                    }
                    Poll::Pending
                }
                _ => Poll::Ready(()),
            })
        })
        .await;
    }

    /// Signs an allocated digest on the pool, resolving the issued map and
    /// waking duplicate waiters on completion.
    #[cfg(feature = "parallel")]
    async fn sign(&self, digest: StampDigest, tracked: bool) -> Result<Stamp, SigningError> {
        let signer = Arc::clone(&self.signer);
        let shared = self.shared.clone();
        let address = digest.chunk_address;
        // The resolve/wake fold runs on the pool thread before the reply is
        // sent, so waiters wake in the same order as an inline sign; a lost
        // job reads as a dropped signature.
        nectar_tasks::submit(move || {
            let result = sign_task(signer.as_ref(), &digest).result;
            let wakers = with_state(&shared, |state| resolve(state, &address, &result, tracked));
            wake_all(wakers);
            result
        })
        .await
        .unwrap_or(Err(SigningError::Dropped))
    }

    /// Signs an allocated digest inline, resolving the issued map and
    /// waking duplicate waiters.
    #[cfg(not(feature = "parallel"))]
    async fn sign(&self, digest: StampDigest, tracked: bool) -> Result<Stamp, SigningError> {
        let address = digest.chunk_address;
        let result = sign_now(self.signer.as_ref(), &digest);
        let wakers = with_state(&self.shared, |state| {
            resolve(state, &address, &result, tracked)
        });
        wake_all(wakers);
        result
    }
}

impl<I, Sg, P, C, const B: usize> ChunkPut<Chunk<Verified, AnyChunkSet<B>>>
    for StampedPut<I, Sg, P, C>
where
    I: StampIssuer + MaybeSend + 'static,
    Sg: BoundSigner<Spec = I::Spec> + MaybeSend + MaybeSync + 'static,
    P: ChunkPut<StampedChunk<Verified, Validated, B>>,
    C: Clock,
{
    type Error = StampedPutError<P::Error>;

    async fn put(&self, chunk: Chunk<Verified, AnyChunkSet<B>>) -> Result<(), Self::Error> {
        // The put future runs on the split's driver thread; a
        // pool-thread-driven split would starve the pool its sign jobs
        // need.
        #[cfg(feature = "parallel")]
        debug_assert!(
            rayon::current_thread_index().is_none(),
            "stamped put polled from a rayon pool thread"
        );
        let address = *chunk.address();
        // A signed tracked owner loops back to race for the delivery, so
        // the sink sees each tracked address exactly once.
        let (stamp, held) = loop {
            match self.step(&address) {
                Step::Done => return Ok(()),
                Step::Refused(error) => return Err(StampedPutError::Stamp(error)),
                Step::Deliver(stamp) => break (stamp, true),
                Step::Own { digest, tracked } => {
                    let stamp = self
                        .sign(digest, tracked)
                        .await
                        .map_err(StampedPutError::Sign)?;
                    if !tracked {
                        break (stamp, false);
                    }
                }
                Step::Wait => self.wait_progress(&address).await,
            }
        };
        // Armed before the fallible pairing: an early return past a held
        // delivery would park every duplicate for good.
        let guard = held.then(|| DeliveryGuard {
            shared: &self.shared,
            address,
            armed: true,
        });
        let pair = StampedChunk::new(chunk, stamp)
            .issued_by(self.signer.batch(), self.signer.address())?;
        self.inner.put(pair).await.map_err(StampedPutError::Put)?;
        if let Some(guard) = guard {
            guard.stored();
        }
        Ok(())
    }
}

impl<I, Sg, P, C, const B: usize> ChunkGet<AnyChunkSet<B>> for StampedPut<I, Sg, P, C>
where
    I: MaybeSend,
    Sg: MaybeSend + MaybeSync,
    P: ChunkGet<AnyChunkSet<B>>,
    C: MaybeSend + MaybeSync,
{
    type Trust = P::Trust;
    type Error = P::Error;

    fn get(
        &self,
        address: &ChunkAddress,
    ) -> impl Future<Output = Result<Chunk<Self::Trust, AnyChunkSet<B>>, Self::Error>> + MaybeSend
    {
        self.inner.get(address)
    }
}

impl<I, Sg, P, C> ChunkHas for StampedPut<I, Sg, P, C>
where
    I: MaybeSend,
    Sg: MaybeSend + MaybeSync,
    P: ChunkHas,
    C: MaybeSend + MaybeSync,
{
    fn has(&self, address: &ChunkAddress) -> impl Future<Output = bool> + MaybeSend {
        self.inner.has(address)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{batch_at_depth, key};
    use crate::{BatchId, BucketDepth, MemoryIssuer};
    #[cfg(feature = "parallel")]
    use alloy_primitives::U256;
    use alloy_primitives::{Address, B256, Signature};
    use alloy_signer_local::PrivateKeySigner;
    use core::convert::Infallible;
    use nectar_file::{File, Policy};
    use nectar_postage::{StampedAddress, calculate_bucket};
    use nectar_primitives::{ContentChunk, MemoryStore};
    use nectar_testing::run;
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicBool, Ordering};

    type TestChunk = Chunk<Verified, AnyChunkSet<4096>>;

    fn issuer(depth: u8) -> MemoryIssuer {
        MemoryIssuer::new(BatchId::ZERO, depth, BucketDepth::new(16).unwrap())
    }

    /// A decorator whose signer owns the batch its issuer allocates from.
    fn stamped<Sg: SignPrehash, P>(
        depth: u8,
        signer: impl FnOnce(Address) -> Sg,
        inner: P,
    ) -> StampedPut<MemoryIssuer, BatchSigner<Sg>, P> {
        let owner = key(1).address();
        let batch = batch_at_depth(owner, BatchId::ZERO, depth);
        let bound = BatchSigner::bind(signer(owner), batch).unwrap();
        StampedPut::new(issuer(depth), bound, inner).unwrap()
    }

    fn owner_key(_owner: Address) -> Eip191<PrivateKeySigner> {
        Eip191::new(key(1))
    }

    fn bucket_depth() -> BucketDepth {
        BucketDepth::new(16).unwrap()
    }

    fn sealed(payload: &'static [u8]) -> TestChunk {
        let content = ContentChunk::new(payload).unwrap();
        Chunk::from_envelope(content.into()).unwrap()
    }

    #[cfg(feature = "parallel")]
    fn fixed_signature() -> Signature {
        Signature::new(U256::from(1), U256::from(2), false)
    }

    /// Fails every signing call.
    struct FailingSigner(Address);

    impl crate::pipeline::signer::sealed::Sealed for FailingSigner {}

    impl SignPrehash for FailingSigner {
        fn address(&self) -> Address {
            self.0
        }

        fn sign_prehash(&self, _prehash: &B256) -> Result<Signature, SigningError> {
            Err(SigningError::Signer(alloy_signer::Error::message(
                "signer offline",
            )))
        }
    }

    /// Records every pair the sink receives.
    #[derive(Debug, Clone, Default)]
    struct CountingSink {
        seen: Arc<StdMutex<Vec<(ChunkAddress, Stamp)>>>,
    }

    impl ChunkPut<StampedChunk<Verified, Validated>> for CountingSink {
        type Error = Infallible;

        async fn put(&self, stamped: StampedChunk<Verified, Validated>) -> Result<(), Self::Error> {
            self.seen
                .lock()
                .unwrap()
                .push((*stamped.address(), stamped.stamp().clone()));
            Ok(())
        }
    }

    #[derive(Debug, PartialEq, thiserror::Error)]
    #[error("sink refused")]
    struct SinkRefused;

    /// Refuses the first put, accepts afterwards; records every stamp.
    #[derive(Debug, Clone, Default)]
    struct FailOnceSink {
        failed: Arc<AtomicBool>,
        seen: Arc<StdMutex<Vec<Stamp>>>,
    }

    impl ChunkPut<StampedChunk<Verified, Validated>> for FailOnceSink {
        type Error = SinkRefused;

        async fn put(&self, stamped: StampedChunk<Verified, Validated>) -> Result<(), Self::Error> {
            self.seen.lock().unwrap().push(stamped.stamp().clone());
            if self.failed.swap(true, Ordering::SeqCst) {
                Ok(())
            } else {
                Err(SinkRefused)
            }
        }
    }

    /// A stamped sink that also serves reads, for the delegation bounds.
    #[derive(Debug, Clone, Default)]
    struct StoreSink {
        store: Arc<MemoryStore<AnyChunkSet<4096>>>,
    }

    impl ChunkPut<StampedChunk<Verified, Validated>> for StoreSink {
        type Error = Infallible;

        async fn put(&self, stamped: StampedChunk<Verified, Validated>) -> Result<(), Self::Error> {
            let (chunk, _stamp) = stamped.into_parts();
            ChunkPut::put(&self.store, chunk).await
        }
    }

    impl ChunkGet<AnyChunkSet<4096>> for StoreSink {
        type Trust = Verified;
        type Error = <MemoryStore<AnyChunkSet<4096>> as ChunkGet<AnyChunkSet<4096>>>::Error;

        async fn get(&self, address: &ChunkAddress) -> Result<TestChunk, Self::Error> {
            ChunkGet::get(&self.store, address).await
        }
    }

    impl ChunkHas for StoreSink {
        async fn has(&self, address: &ChunkAddress) -> bool {
            ChunkHas::has(&self.store, address).await
        }
    }

    /// The pinned regression: a zero region larger than one bucket's
    /// capacity must split once-stamped, not refuse with `BucketFull`.
    #[test]
    fn zero_region_larger_than_bucket_capacity_stamps_once() {
        run(async {
            // Depth 20 / bucket depth 16: capacity 16; 1 MiB of zeros is 256
            // identical leaves in one bucket.
            let sink = CountingSink::default();
            let store = stamped(20, owner_key, sink.clone());
            let data = vec![0u8; 1 << 20];

            File::<_, 4096>::new(store.clone(), Policy::DEFAULT)
                .save(&data[..])
                .await
                .expect("dedup keeps the zero region under bucket capacity");

            // One stamp and one sink put per unique address: the leaf, the
            // repeated intermediate, and the root.
            let seen = sink.seen.lock().unwrap();
            assert_eq!(seen.len(), 3);
            let mut per_address: BTreeMap<ChunkAddress, usize> = BTreeMap::new();
            for (address, _) in seen.iter() {
                *per_address.entry(*address).or_insert(0) += 1;
            }
            assert!(per_address.values().all(|&count| count == 1));
            // Exactly one allocation per touched bucket.
            let mut buckets: BTreeMap<u32, u32> = BTreeMap::new();
            for address in per_address.keys() {
                *buckets
                    .entry(calculate_bucket(address, bucket_depth()).value())
                    .or_insert(0) += 1;
            }
            let fullest = buckets.values().copied().max().unwrap();
            assert_eq!(store.remaining_capacity(), 16 - fullest);
        });
    }

    #[test]
    fn every_delivered_pair_validates_against_the_batch() {
        run(async {
            let batch = batch_at_depth(key(1).address(), BatchId::ZERO, 20);
            let sink = CountingSink::default();
            let store = stamped(20, owner_key, sink.clone());

            store.put(sealed(b"paid")).await.unwrap();
            store.put(sealed(b"paid twice")).await.unwrap();

            let seen = sink.seen.lock().unwrap();
            assert_eq!(seen.len(), 2);
            for (address, stamp) in seen.iter() {
                StampedAddress::new(*address, stamp.clone())
                    .validate(&batch)
                    .expect("the issued stamp pays for the address it is paired with");
            }
        });
    }

    #[test]
    fn construction_refuses_a_signer_bound_to_another_batch() {
        let elsewhere = batch_at_depth(key(1).address(), BatchId::new([0xEE; 32]), 20);
        let bound = BatchSigner::from_signer(key(1), elsewhere).unwrap();

        let refused = StampedPut::new(issuer(20), bound, CountingSink::default()).unwrap_err();
        assert!(matches!(refused, IssuerError::BatchMismatch { .. }));
    }

    #[test]
    fn construction_refuses_a_signer_that_does_not_own_the_batch() {
        let batch = batch_at_depth(key(1).address(), BatchId::ZERO, 20);

        let refused = StampedPut::from_signer(issuer(20), key(2), CountingSink::default(), batch)
            .unwrap_err();
        assert!(matches!(refused, IssuerError::NotBatchOwner { .. }));
    }

    /// Left open, this cuts every stamp into a bucket the batch refuses, and
    /// the refusal lands after the issued map has parked the address.
    #[test]
    fn construction_refuses_an_issuer_cutting_buckets_at_another_depth() {
        let batch = batch_at_depth(key(1).address(), BatchId::ZERO, 20);
        let wide = MemoryIssuer::new(BatchId::ZERO, 20, BucketDepth::new(17).unwrap());
        let bound = BatchSigner::bind(Eip191::new(key(1)), batch).unwrap();

        let refused = StampedPut::new(wide, bound, CountingSink::default()).unwrap_err();
        assert!(matches!(refused, IssuerError::BucketDepthMismatch { .. }));
    }

    #[test]
    fn issued_bound_off_reintroduces_bucket_refusal() {
        run(async {
            // Depth 17 / bucket depth 16: two slots per bucket.
            let sink = CountingSink::default();
            let store = stamped(17, owner_key, sink).with_issued_bound(IssuedBound::Off);
            let chunk = sealed(b"repetitive");

            store.put(chunk.clone()).await.unwrap();
            store.put(chunk.clone()).await.unwrap();
            let error = store.put(chunk).await.unwrap_err();
            assert!(matches!(
                error,
                StampedPutError::Stamp(StampError::BucketFull { .. })
            ));
        });
    }

    #[test]
    fn duplicate_put_short_circuits_after_store() {
        run(async {
            let sink = CountingSink::default();
            let store = stamped(20, owner_key, sink.clone());
            let chunk = sealed(b"dedup");

            store.put(chunk.clone()).await.unwrap();
            store.put(chunk).await.unwrap();

            assert_eq!(sink.seen.lock().unwrap().len(), 1);
            assert_eq!(store.remaining_capacity(), 15);
        });
    }

    #[test]
    fn clones_share_one_issuer_and_issued_map() {
        run(async {
            let sink = CountingSink::default();
            let store = stamped(20, owner_key, sink.clone());
            let clone = store.clone();
            let chunk = sealed(b"shared");

            store.put(chunk.clone()).await.unwrap();
            clone.put(chunk).await.unwrap();

            assert_eq!(sink.seen.lock().unwrap().len(), 1);
            assert_eq!(clone.remaining_capacity(), 15);
        });
    }

    #[test]
    fn sink_refusal_keeps_the_signed_stamp_for_reuse() {
        run(async {
            let sink = FailOnceSink::default();
            let store = stamped(20, owner_key, sink.clone());
            let chunk = sealed(b"retry");

            let error = store.put(chunk.clone()).await.unwrap_err();
            assert!(matches!(error, StampedPutError::Put(SinkRefused)));
            assert!(
                core::error::Error::source(&error)
                    .expect("the sink error is the source")
                    .is::<SinkRefused>()
            );
            store.put(chunk).await.unwrap();

            // Both attempts carried the same stamp: one allocation total.
            let seen = sink.seen.lock().unwrap();
            assert_eq!(seen.len(), 2);
            assert_eq!(seen[0], seen[1]);
            assert_eq!(store.remaining_capacity(), 15);
        });
    }

    #[test]
    fn signer_failure_burns_the_index_and_frees_the_entry() {
        run(async {
            // Depth 17 / bucket depth 16: two slots per bucket.
            let sink = CountingSink::default();
            let store = stamped(17, FailingSigner, sink.clone());
            let chunk = sealed(b"burn");

            for _ in 0..2 {
                let error = store.put(chunk.clone()).await.unwrap_err();
                assert!(matches!(error, StampedPutError::Sign(_)));
                let signing = core::error::Error::source(&error)
                    .expect("the signing error is the source")
                    .downcast_ref::<SigningError>()
                    .expect("the source is the signing error");
                assert!(signing.is_systemic());
            }
            // Each failure burnt an index rather than wedging on a dead
            // entry; the bucket is now full.
            let error = store.put(chunk).await.unwrap_err();
            assert!(matches!(
                error,
                StampedPutError::Stamp(StampError::BucketFull { .. })
            ));
            assert!(sink.seen.lock().unwrap().is_empty());
        });
    }

    #[test]
    fn bounded_map_tracks_only_the_first_addresses() {
        run(async {
            let sink = CountingSink::default();
            let store = stamped(20, owner_key, sink.clone())
                .with_issued_bound(IssuedBound::AtMost(NonZeroUsize::new(1).unwrap()));
            let tracked = sealed(b"tracked");
            let tracked_address = *tracked.address();
            let untracked = sealed(b"untracked");
            let untracked_address = *untracked.address();

            store.put(tracked.clone()).await.unwrap();
            store.put(tracked).await.unwrap();
            store.put(untracked.clone()).await.unwrap();
            store.put(untracked).await.unwrap();

            let seen = sink.seen.lock().unwrap();
            let tracked_stamps: Vec<_> = seen
                .iter()
                .filter(|(address, _)| *address == tracked_address)
                .collect();
            let untracked_stamps: Vec<_> = seen
                .iter()
                .filter(|(address, _)| *address == untracked_address)
                .collect();
            assert_eq!(tracked_stamps.len(), 1);
            // Untracked duplicates each burn a fresh index.
            assert_eq!(untracked_stamps.len(), 2);
            assert_ne!(untracked_stamps[0].1.index(), untracked_stamps[1].1.index());
        });
    }

    #[test]
    fn get_and_has_delegate_to_the_inner_sink() {
        run(async {
            let sink = StoreSink::default();
            let store = stamped(20, owner_key, sink);
            let chunk = sealed(b"delegate");
            let address = *chunk.address();

            assert!(!store.has(&address).await);
            store.put(chunk.clone()).await.unwrap();
            assert!(store.has(&address).await);
            let read = store.get(&address).await.unwrap();
            assert_eq!(read.address(), chunk.address());
        });
    }

    /// Two in-flight puts of one address share one allocation and one
    /// delivery: the waiter parks on the pending entry, then exactly one
    /// of the pair carries the signed stamp to the sink.
    #[cfg(feature = "parallel")]
    #[test]
    fn concurrent_duplicates_share_one_allocation_and_delivery() {
        use core::pin::pin;
        use core::task::{Context, Poll, Waker};
        use std::thread;
        use std::time::{Duration, Instant};

        /// Signs after a delay, pinning the pending window open.
        struct SlowSigner(Address);

        impl crate::pipeline::signer::sealed::Sealed for SlowSigner {}

        impl SignPrehash for SlowSigner {
            fn address(&self) -> Address {
                self.0
            }

            fn sign_prehash(&self, _prehash: &B256) -> Result<Signature, SigningError> {
                thread::sleep(Duration::from_millis(50));
                Ok(fixed_signature())
            }
        }

        let sink = CountingSink::default();
        let store = stamped(20, SlowSigner, sink.clone());
        let chunk = sealed(b"concurrent");

        let mut owner = pin!(store.put(chunk.clone()));
        let mut waiter = pin!(store.put(chunk));

        // The split's synchronous first poll: noop waker, both park.
        let noop = &mut Context::from_waker(Waker::noop());
        assert!(owner.as_mut().poll(noop).is_pending());
        assert!(waiter.as_mut().poll(noop).is_pending());

        let waker = nectar_tasks::unpark_current();
        let cx = &mut Context::from_waker(&waker);
        let budget = Duration::from_secs(10);
        let start = Instant::now();
        let mut owner_done = false;
        let mut waiter_done = false;
        while !(owner_done && waiter_done) {
            assert!(start.elapsed() < budget, "lost wake");
            // The waiter polls first each round, so it can take the
            // delivery before the owner resumes from signing.
            if !waiter_done && let Poll::Ready(result) = waiter.as_mut().poll(cx) {
                result.unwrap();
                waiter_done = true;
            }
            if !owner_done && let Poll::Ready(result) = owner.as_mut().poll(cx) {
                result.unwrap();
                owner_done = true;
            }
            if !(owner_done && waiter_done) {
                thread::park_timeout(budget.saturating_sub(start.elapsed()));
            }
        }

        assert_eq!(sink.seen.lock().unwrap().len(), 1);
        assert_eq!(store.remaining_capacity(), 15);
    }
}
