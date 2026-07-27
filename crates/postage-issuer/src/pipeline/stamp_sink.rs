//! Poll-native stamping: [`StampSink`] admits addresses one at a time and
//! yields completions unordered, with sign jobs routed through a caller
//! supplied [`Spawn`] executor.
//!
//! The [pipeline module](super) contracts hold unchanged, stated once
//! there. Sink-shaped deltas: after fail-fast every further offered address
//! queues [`SigningError::NotAdmitted`] instead of allocating, and
//! [`StampSink::poll_next`] returning `Ready(None)` means drained now, not
//! terminated.

use alloc::boxed::Box;
use alloc::collections::VecDeque;
use alloc::sync::Arc;
use core::fmt;
use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll, Waker};
use std::sync::{Mutex, MutexGuard, PoisonError};

use nectar_clock::Clock;
use nectar_kernel::InFlight;
use nectar_marker::{MaybeSend, MaybeSync};
use nectar_postage::StampDigest;
use nectar_primitives::ChunkAddress;
use nectar_tasks::{Spawn, TaskHandle};

use super::task::sign_task;
use super::{SignPrehash, StampPipeline, StampResult};
use crate::error::SigningError;
use crate::issuer::StampIssuer;
use crate::prepared::prepare_stamps;

fn lock(mutex: &Mutex<SlotState>) -> MutexGuard<'_, SlotState> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// One job's completion cell: at most one result, plus the latest waker.
#[derive(Default)]
struct Slot {
    state: Mutex<SlotState>,
}

#[derive(Default)]
struct SlotState {
    result: Option<StampResult>,
    waker: Option<Waker>,
}

impl Slot {
    /// Stores the result and wakes the latest registered waker.
    fn complete(&self, result: StampResult) {
        let waker = {
            let mut state = lock(&self.state);
            state.result = Some(result);
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

/// Awaits one job's completion; dropping it aborts the job.
///
/// The waker registration is overwritten on every poll: the first poll may
/// carry a noop waker (a synchronous split-style first poll), and only the
/// latest waker is entitled to the wakeup.
struct Completion {
    slot: Arc<Slot>,
    _task: TaskHandle,
}

impl Future for Completion {
    type Output = StampResult;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<StampResult> {
        let mut state = lock(&self.slot.state);
        if state.result.is_none() {
            state.waker = Some(cx.waker().clone());
        }
        state.result.take().map_or(Poll::Pending, Poll::Ready)
    }
}

impl<Sg, C> StampPipeline<Sg, C>
where
    Sg: SignPrehash + MaybeSend + MaybeSync + 'static,
    C: Clock,
{
    /// Poll-native counterpart of [`stamp`](Self::stamp) for channel-fed
    /// and splitter-fed inputs: offer addresses through
    /// [`StampSink::poll_admit`], collect through
    /// [`StampSink::poll_next`]. Sign jobs run on `spawner`.
    pub const fn sink<'p, I, S>(
        &'p self,
        issuer: &'p mut I,
        spawner: S,
    ) -> StampSink<'p, Sg, C, I, S>
    where
        I: StampIssuer + ?Sized,
        S: Spawn,
    {
        StampSink {
            pipeline: self,
            issuer,
            spawner,
            in_flight: InFlight::new(),
            ready: VecDeque::new(),
            failed: false,
            paused: false,
            admit_waker: None,
        }
    }
}

/// Poll-native stamping sink returned by [`StampPipeline::sink`].
///
/// Dropping the sink aborts its in-flight jobs and abandons at most one
/// window of allocated, unsigned indices; issuer state is coherent at every
/// yield point.
pub struct StampSink<'p, Sg, C, I: ?Sized, S> {
    pipeline: &'p StampPipeline<Sg, C>,
    issuer: &'p mut I,
    spawner: S,
    in_flight: InFlight<'static, StampResult>,
    /// Results complete at admission: allocation failures and `NotAdmitted`.
    ready: VecDeque<StampResult>,
    failed: bool,
    paused: bool,
    /// Admitter parked by [`pause`](Self::pause), woken by
    /// [`resume`](Self::resume).
    admit_waker: Option<Waker>,
}

impl<Sg, C, I: ?Sized, S> fmt::Debug for StampSink<'_, Sg, C, I, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StampSink")
            .field("in_flight", &self.in_flight.len())
            .field("ready", &self.ready.len())
            .field("paused", &self.paused)
            .field("failed", &self.failed)
            .finish_non_exhaustive()
    }
}

impl<Sg, C, I: ?Sized, S> StampSink<'_, Sg, C, I, S> {
    /// Admitted jobs not yet yielded.
    pub const fn in_flight(&self) -> usize {
        self.in_flight.len()
    }

    /// Whether admission is paused.
    pub const fn is_paused(&self) -> bool {
        self.paused
    }

    /// Whether fail-fast has stopped admission.
    pub const fn is_failed(&self) -> bool {
        self.failed
    }

    /// Pauses admission: the checkpoint hook. [`poll_admit`](Self::poll_admit)
    /// parks until [`resume`](Self::resume); draining
    /// [`poll_next`](Self::poll_next) to `Ready(None)` then reaches a
    /// consistent checkpoint.
    pub const fn pause(&mut self) {
        self.paused = true;
    }

    /// Reopens admission and wakes a parked admitter.
    pub fn resume(&mut self) {
        self.paused = false;
        if let Some(waker) = self.admit_waker.take() {
            waker.wake();
        }
    }
}

impl<Sg, C, I, S> StampSink<'_, Sg, C, I, S>
where
    Sg: SignPrehash + MaybeSend + MaybeSync + 'static,
    C: Clock,
    I: StampIssuer + ?Sized,
    S: Spawn,
{
    /// Offers `address` for admission.
    ///
    /// `Ready` consumes the address: it was admitted, or its result (an
    /// allocation failure, or `NotAdmitted` after fail-fast) is queued for
    /// [`poll_next`](Self::poll_next). `Pending` does not consume it: the
    /// window is full or admission is paused, the waker fires when a slot
    /// frees or [`resume`](Self::resume) runs, and the same address must be
    /// offered again.
    pub fn poll_admit(&mut self, cx: &mut Context<'_>, address: ChunkAddress) -> Poll<()> {
        if self.paused {
            self.admit_waker = Some(cx.waker().clone());
            return Poll::Pending;
        }
        loop {
            if self.failed {
                self.ready.push_back(StampResult {
                    address,
                    result: Err(SigningError::NotAdmitted),
                });
                return Poll::Ready(());
            }
            if self.room() > 0 {
                self.admit_batch(&[address]);
                return Poll::Ready(());
            }
            match self.harvest(cx) {
                Poll::Ready(Some(result)) => self.ready.push_back(result),
                // Unreachable with a nonzero window; loop back to admit.
                Poll::Ready(None) => {}
                Poll::Pending => return Poll::Pending,
            }
        }
    }

    /// Polls for the next completion.
    ///
    /// `Ready(None)` reports the sink drained: nothing queued and nothing in
    /// flight. The sink stays usable; further admissions restart the stream.
    pub fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<StampResult>> {
        if let Some(result) = self.ready.pop_front() {
            return Poll::Ready(Some(result));
        }
        self.harvest(cx)
    }

    /// Window slots currently free.
    pub(super) fn room(&self) -> usize {
        usize::from(self.pipeline.window.get()).saturating_sub(self.in_flight.len())
    }

    /// Allocates a digest per address with one clock read and submits each
    /// for signing; an allocation failure queues its result instead. The
    /// batch must not exceed [`room`](Self::room).
    pub(super) fn admit_batch(&mut self, batch: &[ChunkAddress]) {
        for preparation in prepare_stamps(&mut *self.issuer, batch, &self.pipeline.clock) {
            match preparation.result {
                Ok(digest) => self.submit(digest),
                Err(error) => self.ready.push_back(StampResult {
                    address: preparation.address,
                    result: Err(SigningError::Stamp(error)),
                }),
            }
        }
    }

    /// Spawns the sign job and tracks its completion in the in-flight set.
    fn submit(&mut self, digest: StampDigest) {
        let slot = Arc::new(Slot::default());
        let completion = Arc::clone(&slot);
        let signer = Arc::clone(&self.pipeline.signer);
        let task = self.spawner.spawn(Box::pin(async move {
            completion.complete(sign_task(signer.as_ref(), &digest));
        }));
        self.in_flight
            .push(Box::pin(Completion { slot, _task: task }));
    }

    /// Polls the in-flight set for one completion and applies fail-fast.
    fn harvest(&mut self, cx: &mut Context<'_>) -> Poll<Option<StampResult>> {
        let polled = self.in_flight.poll(cx);
        if let Poll::Ready(Some(result)) = &polled
            && self.pipeline.fail_fast
            && !self.failed
            && matches!(&result.result, Err(error) if error.is_systemic())
        {
            self.failed = true;
        }
        polled
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BatchId, BucketDepth, MemoryIssuer, StampError};
    use alloc::vec::Vec;
    use alloy_primitives::{B256, Signature, U256};
    use alloy_signer::SignerSync;
    use nectar_kernel::Window;
    use std::sync::mpsc;
    use std::task::Wake;
    use std::time::{Duration, Instant};

    fn issuer24() -> MemoryIssuer {
        MemoryIssuer::new(BatchId::ZERO, 24, BucketDepth::new(16).unwrap())
    }

    fn addresses(n: usize) -> Vec<ChunkAddress> {
        (0..n).map(|_| ChunkAddress::from(B256::random())).collect()
    }

    fn window(slots: u16) -> Window {
        Window::new(slots).unwrap()
    }

    fn fixed_signature() -> Signature {
        Signature::new(U256::from(1), U256::from(2), false)
    }

    fn noop_cx() -> Context<'static> {
        Context::from_waker(Waker::noop())
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

    /// Fails every signing call.
    struct FailingSigner;

    impl SignerSync for FailingSigner {
        fn sign_hash_sync(&self, _hash: &B256) -> Result<Signature, alloy_signer::Error> {
            Err(alloy_signer::Error::message("signer offline"))
        }

        fn sign_message_sync(&self, _message: &[u8]) -> Result<Signature, alloy_signer::Error> {
            Err(alloy_signer::Error::message("signer offline"))
        }

        fn chain_id_sync(&self) -> Option<u64> {
            None
        }
    }

    /// Panics on every signing call.
    struct PanickingSigner;

    impl SignerSync for PanickingSigner {
        fn sign_hash_sync(&self, _hash: &B256) -> Result<Signature, alloy_signer::Error> {
            panic!("signer panicked")
        }

        fn sign_message_sync(&self, _message: &[u8]) -> Result<Signature, alloy_signer::Error> {
            panic!("signer panicked")
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

    /// Signals each wake over a channel.
    struct SignalWaker(mpsc::Sender<()>);

    impl Wake for SignalWaker {
        fn wake(self: Arc<Self>) {
            let _ = self.0.send(());
        }

        fn wake_by_ref(self: &Arc<Self>) {
            let _ = self.0.send(());
        }
    }

    /// Completes each job synchronously inside `spawn`.
    struct InlineSpawner;

    impl Spawn for InlineSpawner {
        fn spawn(&self, mut task: nectar_tasks::BoxFuture<'static, ()>) -> TaskHandle {
            // Sign jobs are single-poll futures.
            assert!(task.as_mut().poll(&mut noop_cx()).is_ready());
            TaskHandle::new(|| {})
        }
    }

    /// Runs each job on its own thread.
    struct ThreadSpawner;

    impl Spawn for ThreadSpawner {
        fn spawn(&self, mut task: nectar_tasks::BoxFuture<'static, ()>) -> TaskHandle {
            std::thread::spawn(move || {
                // Sign jobs are single-poll futures.
                assert!(task.as_mut().poll(&mut noop_cx()).is_ready());
            });
            TaskHandle::new(|| {})
        }
    }

    /// Feeds every address, harvesting while the window is full, then drains
    /// to `Ready(None)`; asserts the window bound throughout.
    fn drive<Sg, C, I, S>(
        sink: &mut StampSink<'_, Sg, C, I, S>,
        input: &[ChunkAddress],
        bound: usize,
    ) -> Vec<StampResult>
    where
        Sg: SignPrehash + MaybeSend + MaybeSync + 'static,
        C: Clock,
        I: StampIssuer + ?Sized,
        S: Spawn,
    {
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut cx = noop_cx();
        let mut results = Vec::new();
        for &address in input {
            loop {
                assert!(sink.in_flight() <= bound);
                match sink.poll_admit(&mut cx, address) {
                    Poll::Ready(()) => break,
                    Poll::Pending => {
                        assert!(Instant::now() < deadline, "admission stalled");
                        if let Poll::Ready(Some(result)) = sink.poll_next(&mut cx) {
                            results.push(result);
                        }
                    }
                }
            }
        }
        loop {
            match sink.poll_next(&mut cx) {
                Poll::Ready(Some(result)) => results.push(result),
                Poll::Ready(None) => break,
                Poll::Pending => {
                    assert!(Instant::now() < deadline, "drain stalled");
                    std::thread::yield_now();
                }
            }
        }
        results
    }

    fn sorted(mut addresses: Vec<ChunkAddress>) -> Vec<ChunkAddress> {
        addresses.sort_unstable();
        addresses
    }

    #[test]
    fn multiset_one_to_one_unordered_inline() {
        let mut issuer = issuer24();
        let pipeline = StampPipeline::from_signer(FixedSigner).with_window(window(4));
        let input = addresses(50);

        let mut sink = pipeline.sink(&mut issuer, InlineSpawner);
        let results = drive(&mut sink, &input, 4);
        drop(sink);

        assert_eq!(results.len(), 50);
        assert!(results.iter().all(|r| r.result.is_ok()));
        assert_eq!(
            sorted(results.into_iter().map(|r| r.address).collect()),
            sorted(input)
        );
        assert_eq!(issuer.stamps_issued(), Some(50));
    }

    #[test]
    fn multiset_one_to_one_unordered_threaded() {
        let mut issuer = issuer24();
        let pipeline = StampPipeline::from_signer(FixedSigner).with_window(window(4));
        let input = addresses(50);

        let mut sink = pipeline.sink(&mut issuer, ThreadSpawner);
        let results = drive(&mut sink, &input, 4);
        drop(sink);

        assert_eq!(results.len(), 50);
        assert!(results.iter().all(|r| r.result.is_ok()));
        assert_eq!(
            sorted(results.into_iter().map(|r| r.address).collect()),
            sorted(input)
        );
        assert_eq!(issuer.stamps_issued(), Some(50));
    }

    #[test]
    fn duplicates_allocate_independently_mixed_ok_err() {
        // depth=17, bucket_depth=16 gives 2 slots per bucket.
        let mut issuer = MemoryIssuer::new(BatchId::ZERO, 17, BucketDepth::new(16).unwrap());
        let pipeline = StampPipeline::from_signer(FixedSigner).with_window(window(4));
        let address = ChunkAddress::new([0xAB; 32]);

        let mut sink = pipeline.sink(&mut issuer, InlineSpawner);
        let results = drive(&mut sink, &[address, address, address], 4);
        assert!(!sink.is_failed());
        drop(sink);

        assert_eq!(results.len(), 3);
        assert!(results.iter().all(|r| r.address == address));
        assert_eq!(results.iter().filter(|r| r.result.is_ok()).count(), 2);
        assert!(results.iter().any(|r| matches!(
            r.result,
            Err(SigningError::Stamp(StampError::BucketFull { .. }))
        )));
        // BucketFull consumed no index and did not trip fail-fast.
        assert_eq!(issuer.stamps_issued(), Some(2));
    }

    #[test]
    fn fail_fast_queues_not_admitted_after_systemic_failure() {
        let mut issuer = issuer24();
        let pipeline = StampPipeline::from_signer(FailingSigner).with_window(window(4));
        let input = addresses(10);

        let mut sink = pipeline.sink(&mut issuer, InlineSpawner);
        let results = drive(&mut sink, &input, 4);
        assert!(sink.is_failed());
        drop(sink);

        assert_eq!(results.len(), 10);
        let signer = results
            .iter()
            .filter(|r| matches!(r.result, Err(SigningError::Signer(_))))
            .count();
        let not_admitted = results
            .iter()
            .filter(|r| matches!(r.result, Err(SigningError::NotAdmitted)))
            .count();
        // Exactly one window was admitted before the first harvested failure.
        assert_eq!(signer, 4);
        assert_eq!(not_admitted, 6);
        // Utilization equals the admitted count, not the offered count.
        assert_eq!(issuer.stamps_issued(), Some(4));
        assert_eq!(
            sorted(results.into_iter().map(|r| r.address).collect()),
            sorted(input)
        );
    }

    #[test]
    fn fail_fast_off_yields_every_error() {
        let mut issuer = issuer24();
        let pipeline = StampPipeline::from_signer(FailingSigner)
            .with_window(window(4))
            .with_fail_fast(false);

        let mut sink = pipeline.sink(&mut issuer, InlineSpawner);
        let results = drive(&mut sink, &addresses(10), 4);
        assert!(!sink.is_failed());
        drop(sink);

        assert_eq!(results.len(), 10);
        assert!(
            results
                .iter()
                .all(|r| matches!(r.result, Err(SigningError::Signer(_))))
        );
        assert_eq!(issuer.stamps_issued(), Some(10));
    }

    #[test]
    fn panicking_signer_keeps_one_to_one_without_hanging() {
        let mut issuer = issuer24();
        let pipeline = StampPipeline::from_signer(PanickingSigner).with_window(window(4));
        let input = addresses(10);

        let mut sink = pipeline.sink(&mut issuer, InlineSpawner);
        let results = drive(&mut sink, &input, 4);
        drop(sink);

        assert_eq!(results.len(), 10);
        let dropped = results
            .iter()
            .filter(|r| matches!(r.result, Err(SigningError::Dropped)))
            .count();
        let not_admitted = results
            .iter()
            .filter(|r| matches!(r.result, Err(SigningError::NotAdmitted)))
            .count();
        assert_eq!(dropped, 4);
        assert_eq!(not_admitted, 6);
        assert_eq!(issuer.stamps_issued(), Some(4));
    }

    #[test]
    fn pause_parks_admission_and_resume_wakes() {
        let mut issuer = issuer24();
        let pipeline = StampPipeline::from_signer(FixedSigner).with_window(window(4));
        let address = ChunkAddress::new([0xCD; 32]);

        let mut sink = pipeline.sink(&mut issuer, InlineSpawner);
        sink.pause();
        assert!(sink.is_paused());

        let (wake_tx, wake_rx) = mpsc::channel();
        let waker = Waker::from(Arc::new(SignalWaker(wake_tx)));
        let mut cx = Context::from_waker(&waker);
        assert!(sink.poll_admit(&mut cx, address).is_pending());
        // A paused sink drains to `Ready(None)`: the checkpoint is consistent.
        assert!(matches!(sink.poll_next(&mut noop_cx()), Poll::Ready(None)));

        sink.resume();
        wake_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("resume must wake the parked admitter");
        assert!(sink.poll_admit(&mut noop_cx(), address).is_ready());
        match sink.poll_next(&mut noop_cx()) {
            Poll::Ready(Some(result)) => assert!(result.result.is_ok()),
            other => panic!("expected a completion, got {other:?}"),
        }
        drop(sink);
        assert_eq!(issuer.stamps_issued(), Some(1));
    }

    #[test]
    fn completion_wakes_latest_registration_not_first() {
        let (release_tx, release_rx) = mpsc::channel();
        let mut issuer = issuer24();
        let pipeline = StampPipeline::from_signer(BlockingSigner(Mutex::new(release_rx)))
            .with_window(window(4));
        let address = ChunkAddress::new([0xEF; 32]);

        let mut sink = pipeline.sink(&mut issuer, ThreadSpawner);
        assert!(sink.poll_admit(&mut noop_cx(), address).is_ready());
        // First registration is the noop waker, split-style.
        assert!(sink.poll_next(&mut noop_cx()).is_pending());

        // The second poll must overwrite it with the real waker.
        let (wake_tx, wake_rx) = mpsc::channel();
        let waker = Waker::from(Arc::new(SignalWaker(wake_tx)));
        let mut cx = Context::from_waker(&waker);
        assert!(sink.poll_next(&mut cx).is_pending());

        release_tx.send(()).unwrap();
        // Were the first registration kept, the noop waker would swallow this.
        wake_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("completion must wake the latest registered waker");
        match sink.poll_next(&mut noop_cx()) {
            Poll::Ready(Some(result)) => assert!(result.result.is_ok()),
            other => panic!("expected a completion, got {other:?}"),
        }
        assert!(matches!(sink.poll_next(&mut noop_cx()), Poll::Ready(None)));
    }

    #[test]
    fn dropped_sink_abandons_at_most_one_window() {
        let mut issuer = issuer24();
        let pipeline = StampPipeline::from_signer(FixedSigner).with_window(window(4));

        {
            let mut sink = pipeline.sink(&mut issuer, InlineSpawner);
            for &address in &addresses(4) {
                assert!(sink.poll_admit(&mut noop_cx(), address).is_ready());
            }
        }
        assert_eq!(issuer.stamps_issued(), Some(4));

        // The issuer stays coherent for a fresh sink.
        let mut sink = pipeline.sink(&mut issuer, InlineSpawner);
        let results = drive(&mut sink, &addresses(5), 4);
        drop(sink);
        assert_eq!(results.len(), 5);
        assert!(results.iter().all(|r| r.result.is_ok()));
        assert_eq!(issuer.stamps_issued(), Some(9));
    }

    #[test]
    fn drained_sink_reports_none_and_stays_usable() {
        let mut issuer = issuer24();
        let pipeline = StampPipeline::from_signer(FixedSigner).with_window(window(4));

        let mut sink = pipeline.sink(&mut issuer, InlineSpawner);
        assert!(matches!(sink.poll_next(&mut noop_cx()), Poll::Ready(None)));

        let first = drive(&mut sink, &addresses(1), 4);
        assert_eq!(first.len(), 1);
        assert!(matches!(sink.poll_next(&mut noop_cx()), Poll::Ready(None)));

        // Drained is not terminated: admission restarts the stream.
        let second = drive(&mut sink, &addresses(1), 4);
        assert_eq!(second.len(), 1);
    }
}
