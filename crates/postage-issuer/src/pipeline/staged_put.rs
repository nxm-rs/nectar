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
use nectar_postage::{StampedChunk, Validated};
use nectar_primitives::{AnyChunkSet, Chunk, ChunkAddress, ChunkPut, Verified};
use nectar_tasks::{BoxFuture, Spawn};

use super::shared::{Parked, Shared, Unpark, new_shared, park, with_state};
use super::sign_stage::{SealResult, SignStage};
use super::stamped_put::{IssuedBound, StampedPutError};
use super::{BoundSigner, SignPrehash, StampPipeline};
use crate::error::{IssuerError, SigningError};
use crate::issuer::StampIssuer;

type Delivery<E> = BoxFuture<'static, Result<(), E>>;

struct Engine<'p, Sg, C, I, S, P, const BODY_SIZE: usize>
where
    I: StampIssuer + ?Sized,
    P: ChunkPut<StampedChunk<Verified, Validated, BODY_SIZE>>,
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
    /// Pollers parked on a full pipeline. The machinery below wakes only its
    /// latest registration, so a poller that leaves hands the wake on.
    parked: Vec<Waker>,
}

impl<Sg, C, I, S, P, const BODY_SIZE: usize> Parked for Engine<'_, Sg, C, I, S, P, BODY_SIZE>
where
    I: StampIssuer + ?Sized,
    P: ChunkPut<StampedChunk<Verified, Validated, BODY_SIZE>>,
{
    fn parked(&mut self) -> &mut Vec<Waker> {
        &mut self.parked
    }
}

impl<Sg, C, I, S, P, const BODY_SIZE: usize> Engine<'_, Sg, C, I, S, P, BODY_SIZE>
where
    I: StampIssuer + ?Sized,
    P: ChunkPut<StampedChunk<Verified, Validated, BODY_SIZE>>,
{
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
}

impl<Sg, C, I, S, P, const BODY_SIZE: usize> Engine<'_, Sg, C, I, S, P, BODY_SIZE>
where
    Sg: BoundSigner<Spec = I::Spec> + MaybeSend + MaybeSync + 'static,
    C: Clock,
    I: StampIssuer + ?Sized,
    S: Spawn,
    P: ChunkPut<StampedChunk<Verified, Validated, BODY_SIZE>> + Clone + 'static,
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
    fn dispatch(&mut self, pair: StampedChunk<Verified, Validated, BODY_SIZE>) {
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
/// put slot holds store latency alone.
///
/// # Contracts
///
/// - A put resolves at admission, not at delivery. [`flush`](Self::flush)
///   drives every admitted chunk to the sink and is the only place the last
///   deliveries can be observed; an unflushed root names an unwritten tree.
/// - One failure poisons: a refused allocation, a failed signature or a
///   refused delivery surfaces once from the next put or flush, and every
///   later call reports [`StampedPutError::Poisoned`]. The failing chunk is
///   not identified, because deliveries settle unordered. A `BucketFull` is
///   terminal here, unlike the per-item refusal of the other surfaces, so
///   preflight a nearly-full batch with
///   [`remaining_capacity`](Self::remaining_capacity).
/// - Per-address idempotence: an address admitted once is never stamped or
///   delivered twice. Cost is one address per unique chunk; see
///   [`IssuedBound`] for the bound and off switches.
/// - Chunks resident here are two sign windows' worth, one awaiting a
///   signature and one sealed awaiting a put slot, so memory sizes from the
///   sign window rather than from the input.
/// - Wrapping a purely local store burns indices for chunks that may never
///   reach the network; filter for presence upstream where that matters.
pub struct StagedPut<'p, Sg, C, I: StampIssuer + ?Sized, S, P, const BODY_SIZE: usize>
where
    P: ChunkPut<StampedChunk<Verified, Validated, BODY_SIZE>>,
{
    shared: Shared<Engine<'p, Sg, C, I, S, P, BODY_SIZE>>,
}

impl<Sg, C, I: StampIssuer + ?Sized, S, P, const BODY_SIZE: usize> core::fmt::Debug
    for StagedPut<'_, Sg, C, I, S, P, BODY_SIZE>
where
    P: ChunkPut<StampedChunk<Verified, Validated, BODY_SIZE>>,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("StagedPut").finish_non_exhaustive()
    }
}

impl<Sg, C, I: StampIssuer + ?Sized, S, P, const BODY_SIZE: usize> Clone
    for StagedPut<'_, Sg, C, I, S, P, BODY_SIZE>
where
    P: ChunkPut<StampedChunk<Verified, Validated, BODY_SIZE>>,
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
    ///
    /// # Errors
    ///
    /// [`IssuerError::BatchMismatch`] or [`IssuerError::BucketDepthMismatch`]
    /// when the issuer does not allocate from the batch the signer owns.
    pub fn staged_put<'p, I, S, P, const BODY_SIZE: usize>(
        &'p self,
        issuer: &'p I,
        spawner: S,
        sink: P,
        puts: Window,
    ) -> Result<StagedPut<'p, Sg, C, I, S, P, BODY_SIZE>, IssuerError>
    where
        Sg: BoundSigner<Spec = I::Spec>,
        I: StampIssuer + ?Sized,
        S: Spawn,
        P: ChunkPut<StampedChunk<Verified, Validated, BODY_SIZE>> + Clone + 'static,
    {
        Ok(StagedPut {
            shared: new_shared(Engine {
                stage: self.sign_stage(issuer, spawner)?,
                puts: PutSink::new(puts),
                sink,
                seen: BTreeSet::new(),
                bound: IssuedBound::Unbounded,
                failure: None,
                poisoned: false,
                parked: Vec::new(),
            }),
        })
    }
}

impl<Sg, C, I: StampIssuer + ?Sized, S, P, const BODY_SIZE: usize>
    StagedPut<'_, Sg, C, I, S, P, BODY_SIZE>
where
    P: ChunkPut<StampedChunk<Verified, Validated, BODY_SIZE>>,
{
    /// Replaces the seen-set bound. Applies to every clone: the set is
    /// clone-shared.
    #[must_use]
    pub fn with_issued_bound(self, bound: IssuedBound) -> Self {
        with_state(&self.shared, |engine| engine.bound = bound);
        self
    }

    /// Sign jobs with a signature outstanding.
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

    /// Free slots in the fullest bucket. Preflight a nearly-full batch before
    /// a split: one refused allocation poisons the decorator.
    pub fn remaining_capacity(&self) -> u32 {
        with_state(&self.shared, |engine| engine.stage.remaining_capacity())
    }
}

impl<Sg, C, I, S, P, const BODY_SIZE: usize> StagedPut<'_, Sg, C, I, S, P, BODY_SIZE>
where
    Sg: BoundSigner<Spec = I::Spec> + MaybeSend + MaybeSync + 'static,
    C: Clock,
    I: StampIssuer + ?Sized,
    S: Spawn,
    P: ChunkPut<StampedChunk<Verified, Validated, BODY_SIZE>> + Clone + 'static,
{
    /// Seals and delivers every admitted chunk, then reports the first
    /// failure, if any.
    ///
    /// # Errors
    ///
    /// The first stage failure, or [`StampedPutError::Poisoned`] once one has
    /// already surfaced.
    pub async fn flush(&self) -> Result<(), StampedPutError<P::Error>> {
        let _unpark = Unpark::new(&self.shared);
        poll_fn(|cx| self.drive(cx, |engine, cx| Self::poll_flush(engine, cx))).await
    }

    fn poll_put(
        &self,
        cx: &mut Context<'_>,
        slot: &mut Option<Chunk<Verified, AnyChunkSet<BODY_SIZE>>>,
    ) -> Poll<Result<(), StampedPutError<P::Error>>> {
        self.drive(cx, |engine, cx| Self::poll_admit(engine, cx, slot))
    }

    /// Runs one poll under the lock, parking `cx` where it made no progress.
    fn drive<T>(
        &self,
        cx: &mut Context<'_>,
        poll: impl FnOnce(&mut Engine<'_, Sg, C, I, S, P, BODY_SIZE>, &mut Context<'_>) -> Poll<T>,
    ) -> Poll<T> {
        with_state(&self.shared, |engine| {
            let polled = poll(engine, cx);
            if polled.is_pending() {
                park(&mut engine.parked, cx.waker());
            }
            polled
        })
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
        let tracks = engine.bound.tracks(engine.seen.len());
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
    Sg: BoundSigner<Spec = I::Spec> + MaybeSend + MaybeSync + 'static,
    C: Clock,
    I: StampIssuer + MaybeSync + ?Sized,
    S: Spawn,
    P: ChunkPut<StampedChunk<Verified, Validated, BODY_SIZE>> + Clone + 'static,
{
    type Error = StampedPutError<P::Error>;

    async fn put(&self, chunk: Chunk<Verified, AnyChunkSet<BODY_SIZE>>) -> Result<(), Self::Error> {
        let _unpark = Unpark::new(&self.shared);
        let mut slot = Some(chunk);
        poll_fn(|cx| self.poll_put(cx, &mut slot)).await
    }
}

#[cfg(test)]
mod tests {
    use super::super::shared::wake_all;
    use super::super::testutil::{
        BlockingSigner, CountingSink, FixedSigner, Gauge, InlineSpawner, RefusingSink, SinkRefused,
        ThreadSpawner, WakeCount, bound, chunk, issuer, window,
    };
    use super::*;
    use crate::{MemoryIssuer, StampError};
    use alloc::sync::Arc;
    use alloc::vec::Vec;
    use core::convert::Infallible;
    use core::future::Future;
    use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use futures_util::stream::{FuturesUnordered, StreamExt};
    use nectar_file::{File, Policy};
    use nectar_primitives::DEFAULT_BODY_SIZE;
    use std::sync::Mutex;
    use std::sync::mpsc;
    use std::time::Duration;

    /// Parks every delivery until released, counting the parked ones and
    /// keeping their wakers so nothing is re-polled by accident.
    #[derive(Clone, Default)]
    struct ParkingSink {
        parked: Arc<AtomicUsize>,
        waiting: Arc<Mutex<Vec<Waker>>>,
        released: Arc<AtomicBool>,
    }

    impl ParkingSink {
        fn release(&self) {
            self.released.store(true, Ordering::SeqCst);
            wake_all(core::mem::take(&mut *self.waiting.lock().unwrap()));
        }
    }

    impl ChunkPut<StampedChunk<Verified, Validated>> for ParkingSink {
        type Error = Infallible;

        async fn put(
            &self,
            _stamped: StampedChunk<Verified, Validated>,
        ) -> Result<(), Self::Error> {
            self.parked.fetch_add(1, Ordering::SeqCst);
            let released = Arc::clone(&self.released);
            let waiting = Arc::clone(&self.waiting);
            poll_fn(move |cx| {
                if released.load(Ordering::SeqCst) {
                    return Poll::Ready(());
                }
                waiting.lock().unwrap().push(cx.waker().clone());
                Poll::Pending
            })
            .await;
            Ok(())
        }
    }

    /// Occupies a put slot for one poll, then records the pair.
    #[derive(Clone, Default)]
    struct YieldingSink {
        seen: Arc<AtomicUsize>,
    }

    impl ChunkPut<StampedChunk<Verified, Validated>> for YieldingSink {
        type Error = Infallible;

        async fn put(
            &self,
            _stamped: StampedChunk<Verified, Validated>,
        ) -> Result<(), Self::Error> {
            nectar_testing::yield_now().await;
            self.seen.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    /// The gate: sign concurrency is bounded by the sign window, not by the
    /// put window, so a slow signer no longer moves put throughput.
    #[test]
    fn sign_concurrency_is_not_bounded_by_the_put_window() {
        let issuer = issuer(24);
        let (release, blocked) = mpsc::channel();
        let pipeline =
            StampPipeline::new(bound(BlockingSigner(Mutex::new(blocked)))).with_window(window(16));
        let sink = CountingSink::default();
        let staged = pipeline
            .staged_put(&issuer, ThreadSpawner, sink.clone(), window(2))
            .unwrap();

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
        let pipeline = StampPipeline::new(bound(FixedSigner)).with_window(window(4));
        let sink = ParkingSink::default();
        let staged = pipeline
            .staged_put(&issuer, InlineSpawner, sink.clone(), window(2))
            .unwrap();

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

        sink.release();
        nectar_testing::run(staged.flush()).unwrap();
        assert_eq!(sink.parked.load(Ordering::SeqCst), 6);
    }

    type Held<'p> = StagedPut<
        'p,
        crate::BatchSigner<FixedSigner>,
        nectar_clock::SystemClock,
        MemoryIssuer,
        InlineSpawner,
        ParkingSink,
        DEFAULT_BODY_SIZE,
    >;

    /// One delivery parked, one sealed pair waiting and the sign window full:
    /// the next put parks, and only a drain moves the pipeline.
    fn filled(staged: &Held<'_>, sink: &ParkingSink) {
        let mut noop = Context::from_waker(core::task::Waker::noop());
        for index in 0..2u32 {
            let mut slot = Some(chunk(&index.to_be_bytes()));
            assert!(staged.poll_put(&mut noop, &mut slot).is_ready());
        }
        assert_eq!(sink.parked.load(Ordering::SeqCst), 1);
    }

    /// A put dropped while parked may hold the pipeline's only live
    /// registration, so its exit must hand the wake to its parked peers.
    #[test]
    fn a_cancelled_put_hands_its_wake_on() {
        let issuer = issuer(24);
        let pipeline = StampPipeline::new(bound(FixedSigner)).with_window(window(1));
        let sink = ParkingSink::default();
        let staged = pipeline
            .staged_put(&issuer, InlineSpawner, sink.clone(), window(1))
            .unwrap();
        filled(&staged, &sink);

        let peer = Arc::new(WakeCount::default());
        let mut parked = Box::pin(staged.put(chunk(b"parked peer")));
        assert!(
            parked
                .as_mut()
                .poll(&mut Context::from_waker(&Waker::from(Arc::clone(&peer))))
                .is_pending()
        );

        let mut noop = Context::from_waker(core::task::Waker::noop());
        let mut cancelled = Box::pin(staged.put(chunk(b"cancelled")));
        assert!(cancelled.as_mut().poll(&mut noop).is_pending());
        assert_eq!(peer.count(), 0);

        // The cancelled put registered last, so its peer waits on a waker
        // nothing will fire again.
        drop(cancelled);
        assert!(peer.count() > 0, "a parked peer was left without a waker");
    }

    /// A put that settles takes the registration its peers were waiting on,
    /// so it must hand the wake on before it leaves.
    #[test]
    fn a_settling_put_hands_its_wake_on() {
        let issuer = issuer(24);
        let pipeline = StampPipeline::new(bound(FixedSigner)).with_window(window(1));
        let sink = ParkingSink::default();
        let staged = pipeline
            .staged_put(&issuer, InlineSpawner, sink.clone(), window(1))
            .unwrap();
        filled(&staged, &sink);

        let peer = Arc::new(WakeCount::default());
        let mut parked = Box::pin(staged.put(chunk(b"parked peer")));
        assert!(
            parked
                .as_mut()
                .poll(&mut Context::from_waker(&Waker::from(Arc::clone(&peer))))
                .is_pending()
        );

        let driver = Arc::new(WakeCount::default());
        let driver_waker = Waker::from(Arc::clone(&driver));
        let mut settling = Box::pin(staged.put(chunk(b"settling")));
        let mut driver_cx = Context::from_waker(&driver_waker);
        assert!(settling.as_mut().poll(&mut driver_cx).is_pending());

        // The delivery frees a put slot, waking whoever registered last.
        sink.release();
        assert!(driver.count() > 0);
        assert!(settling.as_mut().poll(&mut driver_cx).is_ready());
        assert!(peer.count() > 0, "a parked peer was left without a waker");
    }

    #[test]
    fn a_duplicate_address_is_stamped_and_delivered_once() {
        let issuer = issuer(20);
        let pipeline = StampPipeline::new(bound(FixedSigner));
        let sink = CountingSink::default();
        let staged = pipeline
            .staged_put(&issuer, InlineSpawner, sink.clone(), window(4))
            .unwrap();

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
        let pipeline = StampPipeline::new(bound(FixedSigner));
        let sink = CountingSink::default();
        let staged = pipeline
            .staged_put(&issuer, InlineSpawner, sink, window(4))
            .unwrap()
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
        let pipeline = StampPipeline::new(bound(FixedSigner));
        let staged = pipeline
            .staged_put(&issuer, InlineSpawner, RefusingSink, window(4))
            .unwrap();

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

    /// Sibling put futures hold one registration each in the stage, so a
    /// settling poll must hand the wake to the peers it displaced.
    #[test]
    fn concurrent_puts_never_wedge_on_a_displaced_wake() {
        let issuer = issuer(24);
        let (gauge, _peak) = Gauge::new(Duration::from_millis(1));
        let pipeline = StampPipeline::new(bound(gauge)).with_window(window(4));
        let sink = CountingSink::default();
        let staged = pipeline
            .staged_put(&issuer, ThreadSpawner, sink.clone(), window(1))
            .unwrap();

        nectar_testing::run(async {
            let mut puts: FuturesUnordered<_> = (0..32u32)
                .map(|index| staged.put(chunk(&index.to_be_bytes())))
                .collect();
            while let Some(result) = puts.next().await {
                result.unwrap();
            }
            staged.flush().await.unwrap();
        });

        assert_eq!(sink.seen.lock().unwrap().len(), 32);
        assert_eq!(issuer.stamps_issued(), Some(32));
    }

    /// The parked-wake handoff has to hold across executors, not only across
    /// siblings in one task: each thread parks on a waker of its own.
    #[test]
    fn threads_sharing_one_decorator_never_wedge() {
        let issuer = issuer(24);
        let (gauge, _peak) = Gauge::new(Duration::from_millis(1));
        let pipeline = StampPipeline::new(bound(gauge)).with_window(window(4));
        let sink = YieldingSink::default();
        let staged = pipeline
            .staged_put(&issuer, ThreadSpawner, sink.clone(), window(1))
            .unwrap();

        std::thread::scope(|scope| {
            for task in 0..4u32 {
                let staged = staged.clone();
                scope.spawn(move || {
                    nectar_testing::run(async {
                        for index in 0..16u32 {
                            let payload = [task.to_be_bytes(), index.to_be_bytes()].concat();
                            staged.put(chunk(&payload)).await.unwrap();
                        }
                    });
                });
            }
        });
        nectar_testing::run(staged.flush()).unwrap();

        assert_eq!(sink.seen.load(Ordering::SeqCst), 64);
        assert_eq!(issuer.stamps_issued(), Some(64));
    }

    /// A split over the staged decorator stores every chunk exactly once,
    /// with the repeated leaves of a zero region stamped once each.
    #[test]
    fn a_split_flushes_every_chunk_exactly_once() {
        let issuer = issuer(20);
        let pipeline = StampPipeline::new(bound(FixedSigner));
        let sink = CountingSink::default();
        let staged = pipeline
            .staged_put(&issuer, InlineSpawner, sink.clone(), window(4))
            .unwrap();

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
        // Past the debug-build cost of hashing one leaf, so the split feeds
        // the stage faster than a signature completes.
        let (gauge, peak) = Gauge::new(Duration::from_millis(50));
        let pipeline = StampPipeline::new(bound(gauge)).with_window(window(16));
        let sink = CountingSink::default();
        let staged = pipeline
            .staged_put(&issuer, ThreadSpawner, sink.clone(), window(2))
            .unwrap();

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
