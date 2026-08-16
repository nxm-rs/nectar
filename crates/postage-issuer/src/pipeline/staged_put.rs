//! The put stage over the sign stage: a bare-chunk sink facing up, a
//! stamped-pair sink facing down, with the two windows independent.

use alloc::boxed::Box;
use alloc::collections::BTreeSet;
use alloc::vec::Vec;
use core::future::poll_fn;
use core::task::{Context, Poll, Waker, ready};

use nectar_clock::Clock;
use nectar_governor::{PutSink, Window};
use nectar_marker::{MaybeSend, MaybeSync};
use nectar_postage::{StampedChunk, Unvalidated};
use nectar_primitives::{AnyChunkSet, Chunk, ChunkAddress, ChunkPut, Verified};
use nectar_tasks::{BoxFuture, Spawn};

use super::shared::{Shared, new_shared, park, wake_all, with_state};
use super::sign_stage::{SealResult, SignStage};
use super::stamped_put::{IssuedBound, StampedPutError};
use super::{SignPrehash, StampPipeline};
use crate::error::SigningError;
use crate::issuer::StampIssuer;

type Delivery<E> = BoxFuture<'static, Result<(), E>>;

/// The two stages and the state they share.
struct Engine<'p, Sg, C, I, S, P, const BODY_SIZE: usize>
where
    I: StampIssuer + ?Sized,
    P: ChunkPut<StampedChunk<Verified, Unvalidated, BODY_SIZE>>,
{
    stage: SignStage<'p, Sg, C, I, S, BODY_SIZE>,
    puts: PutSink<Delivery<P::Error>>,
    sink: P,
    /// Addresses already admitted; a later instance needs no second pair.
    seen: BTreeSet<ChunkAddress>,
    bound: IssuedBound,
    /// The first failure, surfaced once and then reported as poisoned.
    failure: Option<StampedPutError<P::Error>>,
    poisoned: bool,
    /// Pollers parked on a full pipeline. The stage wakes only its latest
    /// registration, which is one caller's waker out of many, so a poll that
    /// settles hands the wake on to the rest.
    parked: Vec<Waker>,
}

impl<Sg, C, I, S, P, const BODY_SIZE: usize> Engine<'_, Sg, C, I, S, P, BODY_SIZE>
where
    I: StampIssuer + ?Sized,
    P: ChunkPut<StampedChunk<Verified, Unvalidated, BODY_SIZE>>,
{
    /// Whether a new address enters the seen set under the bound.
    fn tracks(&self) -> bool {
        match self.bound {
            IssuedBound::Off => false,
            IssuedBound::Unbounded => true,
            IssuedBound::AtMost(bound) => self.seen.len() < bound.get(),
        }
    }

    fn fail(&mut self, error: StampedPutError<P::Error>) {
        if !self.poisoned {
            self.failure = Some(error);
            self.poisoned = true;
        }
    }

    fn take_failure(&mut self) -> Option<StampedPutError<P::Error>> {
        match self.failure.take() {
            Some(error) => Some(error),
            None => self.poisoned.then_some(StampedPutError::Poisoned),
        }
    }

    /// Parks `cx` on a pending poll, or claims the parked wakers on a settled
    /// one, whose caller passes the wake on.
    fn settle<T>(&mut self, cx: &Context<'_>, poll: &Poll<T>) -> Vec<Waker> {
        if poll.is_pending() {
            park(&mut self.parked, cx.waker());
            return Vec::new();
        }
        core::mem::take(&mut self.parked)
    }
}

impl<Sg, C, I, S, P, const BODY_SIZE: usize> Engine<'_, Sg, C, I, S, P, BODY_SIZE>
where
    Sg: SignPrehash + MaybeSend + MaybeSync + 'static,
    C: Clock,
    I: StampIssuer + ?Sized,
    S: Spawn,
    P: ChunkPut<StampedChunk<Verified, Unvalidated, BODY_SIZE>> + Clone + 'static,
{
    /// Folds settled deliveries back in and moves sealed pairs into the put
    /// window while slots are free; the first failure poisons.
    fn pump(&mut self, cx: &mut Context<'_>) {
        loop {
            while let Poll::Ready(Some(delivered)) = self.puts.poll_step(cx) {
                if let Err(error) = delivered {
                    return self.fail(StampedPutError::Put(error));
                }
            }
            if self.poisoned || !self.puts.admits() {
                return;
            }
            let Poll::Ready(Some(SealResult { result, .. })) = self.stage.poll_next(cx) else {
                return;
            };
            match result {
                Ok(pair) => self.dispatch(pair),
                Err(SigningError::Stamp(error)) => return self.fail(StampedPutError::Stamp(error)),
                Err(error) => return self.fail(StampedPutError::Sign(error)),
            }
        }
    }

    /// Starts one delivery; a sink that settles inline never holds a slot.
    fn dispatch(&mut self, pair: StampedChunk<Verified, Unvalidated, BODY_SIZE>) {
        let sink = self.sink.clone();
        let put: Delivery<P::Error> = Box::pin(async move { sink.put(pair).await });
        if let Some(Err(error)) = self.puts.push(put) {
            self.fail(StampedPutError::Put(error));
        }
    }

    fn is_drained(&self) -> bool {
        self.stage.is_drained() && self.puts.is_empty()
    }
}

/// Two-stage stamping decorator over a stamped-pair sink.
///
/// Takes bare chunks and puts pairs, with signing in a stage of its own, so a
/// put slot is occupied for store latency alone and a slow signer no longer
/// bounds put throughput. Backpressure chains: the put window fills, the sign
/// stage buffers one window of pairs, admission parks, the caller stalls.
///
/// # Contracts
///
/// - A put resolves at admission, not at delivery.
///   [`flush`](Self::flush) drives every admitted chunk to the sink and is
///   the only place the last deliveries can be observed, so a caller that
///   trusts a root without flushing trusts an unwritten tree.
/// - One failure poisons: a refused allocation, a failed signature or a
///   refused delivery surfaces once from the next put or flush, and every
///   later call reports [`StampedPutError::Poisoned`]. The failing chunk is
///   not identified, because deliveries settle unordered.
/// - Per-address idempotence: an address admitted once is never stamped or
///   delivered twice. Cost is one address per unique chunk; see
///   [`IssuedBound`] for the bound and off switches.
/// - The stage holds at most one sign window of chunks in flight and one of
///   sealed pairs, so a caller sizes memory from the sign window, not from
///   the input.
/// - Wrapping a purely local store burns indices for chunks that may never
///   reach the network; filter for presence upstream where that matters.
pub struct StagedPut<'p, Sg, C, I: StampIssuer + ?Sized, S, P, const BODY_SIZE: usize>
where
    P: ChunkPut<StampedChunk<Verified, Unvalidated, BODY_SIZE>>,
{
    shared: Shared<Engine<'p, Sg, C, I, S, P, BODY_SIZE>>,
}

impl<Sg, C, I: StampIssuer + ?Sized, S, P, const BODY_SIZE: usize> core::fmt::Debug
    for StagedPut<'_, Sg, C, I, S, P, BODY_SIZE>
where
    P: ChunkPut<StampedChunk<Verified, Unvalidated, BODY_SIZE>>,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("StagedPut").finish_non_exhaustive()
    }
}

impl<Sg, C, I: StampIssuer + ?Sized, S, P, const BODY_SIZE: usize> Clone
    for StagedPut<'_, Sg, C, I, S, P, BODY_SIZE>
where
    P: ChunkPut<StampedChunk<Verified, Unvalidated, BODY_SIZE>>,
{
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl<Sg, C> StampPipeline<Sg, C>
where
    Sg: SignPrehash + MaybeSend + MaybeSync + 'static,
    C: Clock,
{
    /// The two-stage stamping decorator over `sink`: signing runs in the
    /// stage under this pipeline's sign window, delivery under `puts`.
    ///
    /// A put resolves at admission, so [`StagedPut::flush`] is not optional.
    pub fn staged_put<'p, I, S, P, const BODY_SIZE: usize>(
        &'p self,
        issuer: &'p I,
        spawner: S,
        sink: P,
        puts: Window,
    ) -> StagedPut<'p, Sg, C, I, S, P, BODY_SIZE>
    where
        I: StampIssuer + ?Sized,
        S: Spawn,
        P: ChunkPut<StampedChunk<Verified, Unvalidated, BODY_SIZE>> + Clone + 'static,
    {
        StagedPut {
            shared: new_shared(Engine {
                stage: self.sign_stage(issuer, spawner),
                puts: PutSink::new(puts),
                sink,
                seen: BTreeSet::new(),
                bound: IssuedBound::Unbounded,
                failure: None,
                poisoned: false,
                parked: Vec::new(),
            }),
        }
    }
}

impl<Sg, C, I: StampIssuer + ?Sized, S, P, const BODY_SIZE: usize>
    StagedPut<'_, Sg, C, I, S, P, BODY_SIZE>
where
    P: ChunkPut<StampedChunk<Verified, Unvalidated, BODY_SIZE>>,
{
    /// Replaces the seen-set bound. Applies to every clone: the set is
    /// clone-shared.
    #[must_use]
    pub fn with_issued_bound(self, bound: IssuedBound) -> Self {
        with_state(&self.shared, |engine| engine.bound = bound);
        self
    }

    /// Sign jobs admitted and not yet sealed.
    pub fn signs_in_flight(&self) -> usize {
        with_state(&self.shared, |engine| engine.stage.in_flight())
    }

    /// Sealed pairs awaiting a put slot.
    pub fn sealed(&self) -> usize {
        with_state(&self.shared, |engine| engine.stage.sealed())
    }

    /// Deliveries in flight.
    pub fn puts_in_flight(&self) -> usize {
        with_state(&self.shared, |engine| engine.puts.len())
    }
}

impl<Sg, C, I, S, P, const BODY_SIZE: usize> StagedPut<'_, Sg, C, I, S, P, BODY_SIZE>
where
    Sg: SignPrehash + MaybeSend + MaybeSync + 'static,
    C: Clock,
    I: StampIssuer + ?Sized,
    S: Spawn,
    P: ChunkPut<StampedChunk<Verified, Unvalidated, BODY_SIZE>> + Clone + 'static,
{
    /// Seals and delivers every admitted chunk, then reports the first
    /// failure, if any.
    ///
    /// # Errors
    ///
    /// The first stage failure, or [`StampedPutError::Poisoned`] once one has
    /// already surfaced.
    pub async fn flush(&self) -> Result<(), StampedPutError<P::Error>> {
        poll_fn(|cx| self.drive(cx, |engine, cx| Self::poll_flush(engine, cx))).await
    }

    fn poll_put(
        &self,
        cx: &mut Context<'_>,
        slot: &mut Option<Chunk<Verified, AnyChunkSet<BODY_SIZE>>>,
    ) -> Poll<Result<(), StampedPutError<P::Error>>> {
        self.drive(cx, |engine, cx| Self::poll_admit(engine, cx, slot))
    }

    /// Runs one poll under the lock, then hands the wake on outside it.
    fn drive<T>(
        &self,
        cx: &mut Context<'_>,
        poll: impl FnOnce(&mut Engine<'_, Sg, C, I, S, P, BODY_SIZE>, &mut Context<'_>) -> Poll<T>,
    ) -> Poll<T> {
        let (polled, wakers) = with_state(&self.shared, |engine| {
            let polled = poll(engine, cx);
            let wakers = engine.settle(cx, &polled);
            (polled, wakers)
        });
        wake_all(wakers);
        polled
    }

    fn poll_flush(
        engine: &mut Engine<'_, Sg, C, I, S, P, BODY_SIZE>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StampedPutError<P::Error>>> {
        engine.pump(cx);
        if let Some(error) = engine.take_failure() {
            return Poll::Ready(Err(error));
        }
        if engine.is_drained() {
            return Poll::Ready(Ok(()));
        }
        Poll::Pending
    }

    fn poll_admit(
        engine: &mut Engine<'_, Sg, C, I, S, P, BODY_SIZE>,
        cx: &mut Context<'_>,
        slot: &mut Option<Chunk<Verified, AnyChunkSet<BODY_SIZE>>>,
    ) -> Poll<Result<(), StampedPutError<P::Error>>> {
        engine.pump(cx);
        if let Some(error) = engine.take_failure() {
            return Poll::Ready(Err(error));
        }
        let Some(chunk) = slot.as_ref() else {
            return Poll::Ready(Ok(()));
        };
        let address = *chunk.address();
        if engine.seen.contains(&address) {
            *slot = None;
            return Poll::Ready(Ok(()));
        }
        let tracks = engine.tracks();
        ready!(engine.stage.poll_admit(cx, slot));
        if tracks {
            engine.seen.insert(address);
        }
        Poll::Ready(Ok(()))
    }
}

impl<Sg, C, I, S, P, const BODY_SIZE: usize> ChunkPut<Chunk<Verified, AnyChunkSet<BODY_SIZE>>>
    for StagedPut<'_, Sg, C, I, S, P, BODY_SIZE>
where
    Sg: SignPrehash + MaybeSend + MaybeSync + 'static,
    C: Clock,
    I: StampIssuer + MaybeSync + ?Sized,
    S: Spawn,
    P: ChunkPut<StampedChunk<Verified, Unvalidated, BODY_SIZE>> + Clone + 'static,
{
    type Error = StampedPutError<P::Error>;

    async fn put(
        &self,
        chunk: Chunk<Verified, AnyChunkSet<BODY_SIZE>>,
    ) -> Result<(), Self::Error> {
        let mut slot = Some(chunk);
        poll_fn(|cx| self.poll_put(cx, &mut slot)).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BatchId, BucketDepth, MemoryIssuer, StampError};
    use alloc::sync::Arc;
    use alloc::vec::Vec;
    use alloy_primitives::{B256, Signature, U256};
    use alloy_signer::SignerSync;
    use core::convert::Infallible;
    use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use nectar_file::{File, Policy};
    use nectar_postage::Stamp;
    use nectar_primitives::{ContentChunk, DEFAULT_BODY_SIZE};
    use nectar_tasks::TaskHandle;
    use std::sync::{Mutex, mpsc};

    type TestChunk = Chunk<Verified, AnyChunkSet<DEFAULT_BODY_SIZE>>;

    fn issuer(depth: u8) -> MemoryIssuer {
        MemoryIssuer::new(BatchId::ZERO, depth, BucketDepth::new(16).unwrap())
    }

    fn window(slots: u16) -> Window {
        Window::new(slots).unwrap()
    }

    fn chunk(payload: &[u8]) -> TestChunk {
        let content: ContentChunk<DEFAULT_BODY_SIZE> = ContentChunk::new(payload.to_vec()).unwrap();
        Chunk::from_envelope(content.into()).unwrap()
    }

    fn fixed_signature() -> Signature {
        Signature::new(U256::from(1), U256::from(2), false)
    }

    /// Deterministic signature without ECDSA cost.
    struct FixedSigner;

    impl SignerSync for FixedSigner {
        fn sign_hash_sync(&self, _hash: &B256) -> Result<Signature, alloy_signer::Error> {
            Ok(fixed_signature())
        }

        fn sign_message_sync(&self, _message: &[u8]) -> Result<Signature, alloy_signer::Error> {
            Ok(fixed_signature())
        }

        fn chain_id_sync(&self) -> Option<u64> {
            None
        }
    }

    /// Blocks each signing call until released over the channel.
    struct BlockingSigner(Mutex<mpsc::Receiver<()>>);

    impl SignerSync for BlockingSigner {
        fn sign_hash_sync(&self, _hash: &B256) -> Result<Signature, alloy_signer::Error> {
            Ok(fixed_signature())
        }

        fn sign_message_sync(&self, _message: &[u8]) -> Result<Signature, alloy_signer::Error> {
            let _ = self.0.lock().unwrap().recv();
            Ok(fixed_signature())
        }

        fn chain_id_sync(&self) -> Option<u64> {
            None
        }
    }

    /// Tracks the highest number of concurrent signing calls.
    struct Gauge {
        current: Arc<AtomicUsize>,
        peak: Arc<AtomicUsize>,
    }

    impl SignerSync for Gauge {
        fn sign_hash_sync(&self, _hash: &B256) -> Result<Signature, alloy_signer::Error> {
            Ok(fixed_signature())
        }

        fn sign_message_sync(&self, _message: &[u8]) -> Result<Signature, alloy_signer::Error> {
            let now = self.current.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(now, Ordering::SeqCst);
            // Past the debug-build cost of hashing one leaf, so the split
            // feeds the stage faster than a signature completes.
            std::thread::sleep(std::time::Duration::from_millis(50));
            self.current.fetch_sub(1, Ordering::SeqCst);
            Ok(fixed_signature())
        }

        fn chain_id_sync(&self) -> Option<u64> {
            None
        }
    }

    /// Completes each job synchronously inside `spawn`.
    struct InlineSpawner;

    impl Spawn for InlineSpawner {
        fn spawn(&self, mut task: nectar_tasks::BoxFuture<'static, ()>) -> TaskHandle {
            let mut cx = Context::from_waker(core::task::Waker::noop());
            // Sign jobs are single-poll futures.
            assert!(task.as_mut().poll(&mut cx).is_ready());
            TaskHandle::new(|| {})
        }
    }

    /// Runs each job on its own thread.
    struct ThreadSpawner;

    impl Spawn for ThreadSpawner {
        fn spawn(&self, mut task: nectar_tasks::BoxFuture<'static, ()>) -> TaskHandle {
            std::thread::spawn(move || {
                let mut cx = Context::from_waker(core::task::Waker::noop());
                // Sign jobs are single-poll futures.
                assert!(task.as_mut().poll(&mut cx).is_ready());
            });
            TaskHandle::new(|| {})
        }
    }

    /// Records every pair the sink receives.
    #[derive(Clone, Default)]
    struct CountingSink {
        seen: Arc<Mutex<Vec<(ChunkAddress, Stamp)>>>,
    }

    impl ChunkPut<StampedChunk<Verified, Unvalidated>> for CountingSink {
        type Error = Infallible;

        async fn put(
            &self,
            stamped: StampedChunk<Verified, Unvalidated>,
        ) -> Result<(), Self::Error> {
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

    /// Refuses every pair.
    #[derive(Clone, Default)]
    struct RefusingSink;

    impl ChunkPut<StampedChunk<Verified, Unvalidated>> for RefusingSink {
        type Error = SinkRefused;

        async fn put(
            &self,
            _stamped: StampedChunk<Verified, Unvalidated>,
        ) -> Result<(), Self::Error> {
            Err(SinkRefused)
        }
    }

    /// Parks every delivery until released, counting the parked ones.
    #[derive(Clone, Default)]
    struct ParkingSink {
        parked: Arc<AtomicUsize>,
        released: Arc<AtomicBool>,
    }

    impl ChunkPut<StampedChunk<Verified, Unvalidated>> for ParkingSink {
        type Error = Infallible;

        async fn put(
            &self,
            _stamped: StampedChunk<Verified, Unvalidated>,
        ) -> Result<(), Self::Error> {
            self.parked.fetch_add(1, Ordering::SeqCst);
            let released = Arc::clone(&self.released);
            poll_fn(move |cx| {
                if released.load(Ordering::SeqCst) {
                    return Poll::Ready(());
                }
                cx.waker().wake_by_ref();
                Poll::Pending
            })
            .await;
            Ok(())
        }
    }

    /// The gate: sign concurrency is bounded by the sign window, not by the
    /// put window, so a slow signer no longer moves put throughput.
    #[test]
    fn sign_concurrency_is_not_bounded_by_the_put_window() {
        let issuer = issuer(24);
        let (release, blocked) = mpsc::channel();
        let pipeline = StampPipeline::from_signer(BlockingSigner(Mutex::new(blocked)))
            .with_window(window(16));
        let sink = CountingSink::default();
        let staged = pipeline.staged_put(&issuer, ThreadSpawner, sink.clone(), window(2));

        nectar_testing::run(async {
            for index in 0..16u32 {
                staged.put(chunk(&index.to_be_bytes())).await.unwrap();
            }
            // Every admitted chunk sits in the signer, on a put window of two.
            assert_eq!(staged.signs_in_flight(), 16);
            assert!(sink.seen.lock().unwrap().is_empty());

            for _ in 0..16 {
                release.send(()).unwrap();
            }
            staged.flush().await.unwrap();
        });

        assert_eq!(sink.seen.lock().unwrap().len(), 16);
        assert_eq!(issuer.stamps_issued(), Some(16));
    }

    /// A full put window backs pressure up through the sealed buffer into
    /// admission rather than growing either without bound.
    #[test]
    fn a_full_put_window_parks_admission() {
        let issuer = issuer(24);
        let pipeline = StampPipeline::from_signer(FixedSigner).with_window(window(4));
        let sink = ParkingSink::default();
        let staged = pipeline.staged_put(&issuer, InlineSpawner, sink.clone(), window(2));

        let mut admitted = 0;
        for index in 0..64u32 {
            let mut slot = Some(chunk(&index.to_be_bytes()));
            let mut cx = Context::from_waker(core::task::Waker::noop());
            if staged.poll_put(&mut cx, &mut slot).is_pending() {
                break;
            }
            admitted += 1;
        }

        // Two parked deliveries and one window of sealed pairs: admission
        // stops there rather than buffering the whole input.
        assert_eq!(sink.parked.load(Ordering::SeqCst), 2);
        assert_eq!(staged.sealed(), 4);
        assert_eq!(admitted, 6);
        assert_eq!(issuer.stamps_issued(), Some(6));

        sink.released.store(true, Ordering::SeqCst);
        nectar_testing::run(staged.flush()).unwrap();
        assert_eq!(sink.parked.load(Ordering::SeqCst), 6);
    }

    #[test]
    fn a_duplicate_address_is_stamped_and_delivered_once() {
        let issuer = issuer(20);
        let pipeline = StampPipeline::from_signer(FixedSigner);
        let sink = CountingSink::default();
        let staged = pipeline.staged_put(&issuer, InlineSpawner, sink.clone(), window(4));

        nectar_testing::run(async {
            let repeated = chunk(b"dedup");
            staged.put(repeated.clone()).await.unwrap();
            staged.put(repeated).await.unwrap();
            staged.flush().await.unwrap();
        });

        assert_eq!(sink.seen.lock().unwrap().len(), 1);
        assert_eq!(issuer.stamps_issued(), Some(1));
    }

    #[test]
    fn the_bound_off_reintroduces_bucket_refusal() {
        // Depth 17 / bucket depth 16: two slots per bucket.
        let issuer = issuer(17);
        let pipeline = StampPipeline::from_signer(FixedSigner);
        let sink = CountingSink::default();
        let staged = pipeline
            .staged_put(&issuer, InlineSpawner, sink, window(4))
            .with_issued_bound(IssuedBound::Off);

        nectar_testing::run(async {
            let repeated = chunk(b"repetitive");
            for _ in 0..3 {
                let _ = staged.put(repeated.clone()).await;
            }
            let error = staged.flush().await.unwrap_err();
            assert!(matches!(
                error,
                StampedPutError::Stamp(StampError::BucketFull { .. })
            ));
            // The failure surfaces once, then the decorator reports poisoned.
            assert!(matches!(
                staged.flush().await.unwrap_err(),
                StampedPutError::Poisoned
            ));
        });
    }

    #[test]
    fn a_refused_delivery_surfaces_once_and_then_poisons() {
        let issuer = issuer(20);
        let pipeline = StampPipeline::from_signer(FixedSigner);
        let staged = pipeline.staged_put(&issuer, InlineSpawner, RefusingSink, window(4));

        nectar_testing::run(async {
            let _ = staged.put(chunk(b"refused")).await;
            let error = staged.flush().await.unwrap_err();
            assert!(matches!(error, StampedPutError::Put(SinkRefused)));
            assert!(matches!(
                staged.put(chunk(b"after")).await.unwrap_err(),
                StampedPutError::Poisoned
            ));
        });
    }

    /// A split over the staged decorator stores every chunk exactly once,
    /// with the repeated leaves of a zero region stamped once each.
    #[test]
    fn a_split_flushes_every_chunk_exactly_once() {
        let issuer = issuer(20);
        let pipeline = StampPipeline::from_signer(FixedSigner);
        let sink = CountingSink::default();
        let staged = pipeline.staged_put(&issuer, InlineSpawner, sink.clone(), window(4));

        nectar_testing::run(async {
            let data = alloc::vec![0u8; 1 << 20];
            File::<_, DEFAULT_BODY_SIZE>::new(&staged, Policy::DEFAULT)
                .save(&data[..])
                .await
                .unwrap();
            staged.flush().await.unwrap();
        });

        // The leaf, the repeated intermediate and the root.
        let seen = sink.seen.lock().unwrap();
        assert_eq!(seen.len(), 3);
        let mut addresses: Vec<_> = seen.iter().map(|(address, _)| *address).collect();
        addresses.sort_unstable();
        addresses.dedup();
        assert_eq!(addresses.len(), 3);
        assert_eq!(issuer.stamps_issued(), Some(3));
    }

    /// The gate end to end: a split feeding the staged decorator overlaps
    /// more signatures than its put window holds, so signer latency stops
    /// bounding upload throughput.
    #[test]
    fn a_split_overlaps_signatures_past_the_put_window() {
        let issuer = issuer(22);
        let peak = Arc::new(AtomicUsize::new(0));
        let gauge = Gauge {
            current: Arc::new(AtomicUsize::new(0)),
            peak: Arc::clone(&peak),
        };
        let pipeline = StampPipeline::from_signer(gauge).with_window(window(16));
        let sink = CountingSink::default();
        let staged = pipeline.staged_put(&issuer, ThreadSpawner, sink.clone(), window(2));

        nectar_testing::run(async {
            let data: Vec<u8> = (0..(128u32 * 1024))
                .map(|byte| u8::try_from(byte % 251).unwrap_or_default())
                .collect();
            File::<_, DEFAULT_BODY_SIZE>::new(&staged, Policy::DEFAULT)
                .save(&data[..])
                .await
                .unwrap();
            staged.flush().await.unwrap();
        });

        // 32 leaves plus the spine, every one delivered.
        assert!(sink.seen.lock().unwrap().len() > 32);
        // A put window of two bounded the old arrangement to two concurrent
        // signatures; anything past it proves the stages are independent.
        assert!(
            peak.load(Ordering::SeqCst) > 2,
            "sign concurrency {} did not pass the put window",
            peak.load(Ordering::SeqCst)
        );
    }
}
