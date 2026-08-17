//! Poll-native stamping: [`StampSink`] admits addresses one at a time and
//! yields completions unordered, with one sign job per admission batch
//! routed through a caller supplied [`Spawn`] executor.
//!
//! The [pipeline module](super) contracts hold unchanged, stated once
//! there. Sink-shaped deltas: after fail-fast every further offered address
//! queues [`SigningError::NotAdmitted`] instead of allocating, and
//! [`StampSink::poll_next`] returning `Ready(None)` means drained now, not
//! terminated.

use alloc::boxed::Box;
use alloc::collections::VecDeque;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::fmt;
use core::pin::Pin;
use core::task::{Context, Poll, Waker};

use futures_util::FutureExt;
use futures_util::stream::{FuturesUnordered, Stream};
use nectar_clock::Clock;
use nectar_governor::Admission;
use nectar_marker::{MaybeSend, MaybeSync};
use nectar_primitives::ChunkAddress;
use nectar_tasks::{BoxFuture, Spawn, submit_on};

use super::task::sign_batch;
use super::{SignPrehash, StampPipeline, StampResult};
use crate::error::SigningError;
use crate::issuer::StampIssuer;
use crate::permit::{AdmissionWindow, Prepared, WindowToken};
use crate::stamper::stamp_timestamp;

impl<Sg, C> StampPipeline<Sg, C>
where
    Sg: SignPrehash + MaybeSend + MaybeSync + 'static,
    C: Clock,
{
    /// Poll-native counterpart of [`stamp`](Self::stamp) for channel-fed
    /// and splitter-fed inputs: offer addresses through
    /// [`StampSink::poll_admit`], collect through
    /// [`StampSink::poll_next`]. Sign jobs run on `spawner`.
    pub fn sink<'p, I, S>(&'p self, issuer: &'p I, spawner: S) -> StampSink<'p, Sg, C, I, S>
    where
        I: StampIssuer + ?Sized,
        S: Spawn,
    {
        StampSink {
            pipeline: self,
            issuer,
            spawner,
            admission: AdmissionWindow::new(self.window),
            in_flight: FuturesUnordered::new(),
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
/// window of allocated, unsigned indices plus one window of signed results
/// still queued; issuer state is coherent at every yield point.
pub struct StampSink<'p, Sg, C, I: ?Sized, S> {
    pipeline: &'p StampPipeline<Sg, C>,
    issuer: &'p I,
    spawner: S,
    /// Occupancy: one token per admitted job, released as its result yields.
    admission: AdmissionWindow,
    /// One entry per admission batch, each resolving to a result per digest.
    in_flight: FuturesUnordered<BoxFuture<'static, Vec<StampResult>>>,
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
            .field("in_flight", &self.admission.in_flight())
            .field("batches", &self.in_flight.len())
            .field("ready", &self.ready.len())
            .field("paused", &self.paused)
            .field("failed", &self.failed)
            .finish_non_exhaustive()
    }
}

impl<Sg, C, I: ?Sized, S> StampSink<'_, Sg, C, I, S> {
    /// Admitted jobs not yet harvested. Batching groups them into fewer
    /// tasks, so this counts digests rather than tasks.
    pub fn in_flight(&self) -> usize {
        self.admission.in_flight()
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
            if self.admits() {
                self.admit_batch(&[address]);
                return Poll::Ready(());
            }
            match self.harvest(cx) {
                Poll::Ready(Some(())) => {}
                // A drained set holds no token, so this cannot happen; refuse
                // rather than spin if it ever does.
                Poll::Ready(None) => {
                    debug_assert!(self.admits(), "an admission token outlived its job");
                    self.ready.push_back(StampResult {
                        address,
                        result: Err(SigningError::NotAdmitted),
                    });
                    return Poll::Ready(());
                }
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
        match self.harvest(cx) {
            Poll::Ready(Some(())) => Poll::Ready(self.ready.pop_front()),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }

    /// Whether the window admits one more job.
    ///
    /// Completions yield unordered, so no slot is reserved for a serial head:
    /// every admission counts as head-served, opening the full window.
    pub(super) fn admits(&self) -> bool {
        Admission::new(self.pipeline.window).admits(self.admission.in_flight(), true)
    }

    /// Window slots currently free, sizing a refill micro-batch.
    pub(super) fn room(&self) -> usize {
        self.admission.room()
    }

    /// Claims a slot per address with one clock read and submits the claimed
    /// slots as one sign job; a refusal queues its result instead. The batch
    /// must not exceed [`room`](Self::room).
    pub(super) fn admit_batch(&mut self, batch: &[ChunkAddress]) {
        let timestamp = stamp_timestamp(&self.pipeline.clock);
        let mut admitted = Vec::with_capacity(batch.len());
        for &address in batch {
            // Backpressure before the claim, so a full window never burns a
            // slot.
            let Some(token) = self.admission.try_acquire() else {
                self.ready.push_back(StampResult {
                    address,
                    result: Err(SigningError::NotAdmitted),
                });
                continue;
            };
            match self.issuer.reserve(&address, timestamp) {
                Ok(permit) => admitted.push(permit.with_token(token)),
                Err(error) => self.ready.push_back(StampResult {
                    address,
                    result: Err(SigningError::Stamp(error)),
                }),
            }
        }
        if !admitted.is_empty() {
            self.submit(admitted);
        }
    }

    /// Submits one sign job for the whole batch and tracks its completion in
    /// the in-flight set.
    ///
    /// The handoff owns the job's abort handle, so dropping it aborts the
    /// task; a cancelled handoff (the task dropped before replying) maps to a
    /// systemic [`SigningError::Dropped`] per admitted address, so a lost job
    /// never wedges the sink.
    fn submit(&mut self, permits: Vec<Prepared<I::Spec>>) {
        let signer = Arc::clone(&self.pipeline.signer);
        let mut digests = Vec::with_capacity(permits.len());
        // Held past the permits, so the slots free when the results yield, not
        // when the signatures land.
        let mut tokens: Vec<WindowToken> = Vec::with_capacity(permits.len());
        for mut permit in permits {
            digests.push(permit.digest());
            tokens.extend(permit.take_token());
        }
        let addresses: Vec<ChunkAddress> =
            digests.iter().map(|digest| digest.chunk_address).collect();
        let handoff = submit_on(&self.spawner, async move {
            sign_batch(signer.as_ref(), &digests)
        });
        self.in_flight.push(Box::pin(handoff.map(move |reply| {
            drop(tokens);
            reply.unwrap_or_else(|| {
                addresses
                    .into_iter()
                    .map(|address| StampResult {
                        address,
                        result: Err(SigningError::Dropped),
                    })
                    .collect()
            })
        })));
    }

    /// Polls the in-flight set for one completed batch, queues its results and
    /// applies fail-fast. `Ready(None)` reports the set drained.
    fn harvest(&mut self, cx: &mut Context<'_>) -> Poll<Option<()>> {
        let batch = match Pin::new(&mut self.in_flight).poll_next(cx) {
            Poll::Ready(Some(batch)) => batch,
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Pending => return Poll::Pending,
        };
        debug_assert!(!batch.is_empty(), "an empty batch was submitted");
        for result in batch {
            if self.pipeline.fail_fast
                && !self.failed
                && matches!(&result.result, Err(error) if error.is_systemic())
            {
                self.failed = true;
            }
            self.ready.push_back(result);
        }
        Poll::Ready(Some(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BatchId, BucketDepth, MemoryIssuer, StampError};
    use alloc::vec::Vec;
    use alloy_primitives::{B256, Signature, U256};
    use alloy_signer::SignerSync;
    use core::sync::atomic::{AtomicUsize, Ordering};
    use nectar_governor::Window;
    use nectar_tasks::TaskHandle;
    use std::sync::{Mutex, mpsc};
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

    /// Tracks the highest number of concurrent signing calls.
    struct Gauge {
        current: Arc<AtomicUsize>,
        max: Arc<AtomicUsize>,
    }

    impl SignerSync for Gauge {
        fn sign_hash_sync(&self, _hash: &B256) -> Result<Signature, alloy_signer::Error> {
            Ok(fixed_signature())
        }

        fn sign_message_sync(&self, _message: &[u8]) -> Result<Signature, alloy_signer::Error> {
            let now = self.current.fetch_add(1, Ordering::SeqCst) + 1;
            self.max.fetch_max(now, Ordering::SeqCst);
            std::thread::sleep(Duration::from_millis(1));
            self.current.fetch_sub(1, Ordering::SeqCst);
            Ok(fixed_signature())
        }

        fn chain_id_sync(&self) -> Option<u64> {
            None
        }
    }

    /// Fails the first call, succeeds afterwards.
    struct FailOnce(AtomicUsize);

    impl SignerSync for FailOnce {
        fn sign_hash_sync(&self, _hash: &B256) -> Result<Signature, alloy_signer::Error> {
            Ok(fixed_signature())
        }

        fn sign_message_sync(&self, _message: &[u8]) -> Result<Signature, alloy_signer::Error> {
            if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                Err(alloy_signer::Error::message("first call fails"))
            } else {
                Ok(fixed_signature())
            }
        }

        fn chain_id_sync(&self) -> Option<u64> {
            None
        }
    }

    /// Completes each job synchronously inside `spawn`, counting the spawns.
    struct CountingSpawner(Arc<AtomicUsize>);

    impl Spawn for CountingSpawner {
        fn spawn(&self, mut task: nectar_tasks::BoxFuture<'static, ()>) -> TaskHandle {
            self.0.fetch_add(1, Ordering::SeqCst);
            // Sign jobs are single-poll futures.
            assert!(task.as_mut().poll(&mut noop_cx()).is_ready());
            TaskHandle::new(|| {})
        }
    }

    /// Drops each job without polling it: the sign task and its sender die
    /// unrun, cancelling the completion receiver.
    struct DroppingSpawner;

    impl Spawn for DroppingSpawner {
        fn spawn(&self, _task: nectar_tasks::BoxFuture<'static, ()>) -> TaskHandle {
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
        let issuer = issuer24();
        let pipeline = StampPipeline::from_signer(FixedSigner).with_window(window(4));
        let input = addresses(50);

        let mut sink = pipeline.sink(&issuer, InlineSpawner);
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
        let issuer = issuer24();
        let pipeline = StampPipeline::from_signer(FixedSigner).with_window(window(4));
        let input = addresses(50);

        let mut sink = pipeline.sink(&issuer, ThreadSpawner);
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

    /// The threaded sink must overlap sign jobs, not serialise the window.
    #[test]
    fn threaded_sink_reaches_window_concurrency() {
        let issuer = issuer24();
        let max = Arc::new(AtomicUsize::new(0));
        let gauge = Gauge {
            current: Arc::new(AtomicUsize::new(0)),
            max: Arc::clone(&max),
        };
        let pipeline = StampPipeline::from_signer(gauge).with_window(window(4));

        let mut sink = pipeline.sink(&issuer, ThreadSpawner);
        let results = drive(&mut sink, &addresses(64), 4);
        drop(sink);

        assert_eq!(results.len(), 64);
        assert!(max.load(Ordering::SeqCst) <= 4);
        // A serialised window collapses the peak to 1; >= 2 proves genuine
        // overlap without demanding the full window on few-core CI.
        assert!(
            max.load(Ordering::SeqCst) >= 2,
            "async sink serialised the window"
        );
        assert_eq!(issuer.stamps_issued(), Some(64));
    }

    #[test]
    fn duplicates_allocate_independently_mixed_ok_err() {
        // depth=17, bucket_depth=16 gives 2 slots per bucket.
        let issuer: MemoryIssuer =
            MemoryIssuer::new(BatchId::ZERO, 17, BucketDepth::new(16).unwrap());
        let pipeline = StampPipeline::from_signer(FixedSigner).with_window(window(4));
        let address = ChunkAddress::new([0xAB; 32]);

        let mut sink = pipeline.sink(&issuer, InlineSpawner);
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
        let issuer = issuer24();
        let pipeline = StampPipeline::from_signer(FailingSigner).with_window(window(4));
        let input = addresses(10);

        let mut sink = pipeline.sink(&issuer, InlineSpawner);
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
        let issuer = issuer24();
        let pipeline = StampPipeline::from_signer(FailingSigner)
            .with_window(window(4))
            .with_fail_fast(false);

        let mut sink = pipeline.sink(&issuer, InlineSpawner);
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
        let issuer = issuer24();
        let pipeline = StampPipeline::from_signer(PanickingSigner).with_window(window(4));
        let input = addresses(10);

        let mut sink = pipeline.sink(&issuer, InlineSpawner);
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

    /// A sign task dropped before it runs must yield [`SigningError::Dropped`]
    /// rather than leaving its completion pending forever.
    #[test]
    fn dropped_unrun_task_yields_dropped_without_hanging() {
        let issuer = issuer24();
        let pipeline = StampPipeline::from_signer(FixedSigner).with_window(window(4));
        let input = addresses(10);

        let mut sink = pipeline.sink(&issuer, DroppingSpawner);
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
        // Fail-fast trips after the first window of lost jobs.
        assert_eq!(dropped, 4);
        assert_eq!(not_admitted, 6);
        assert_eq!(issuer.stamps_issued(), Some(4));
    }

    /// A permit that evaporates mid-flight must return its window token, or
    /// admission wedges once the window has been filled once.
    #[test]
    fn evaporated_permits_release_their_window_tokens() {
        let issuer = issuer24();
        let pipeline = StampPipeline::from_signer(FixedSigner)
            .with_window(window(2))
            .with_fail_fast(false);

        let mut sink = pipeline.sink(&issuer, DroppingSpawner);
        let results = drive(&mut sink, &addresses(10), 2);

        // Ten jobs passed through a window of two, so every token came back.
        assert_eq!(results.len(), 10);
        assert!(
            results
                .iter()
                .all(|r| matches!(r.result, Err(SigningError::Dropped)))
        );
        assert_eq!(sink.room(), 2);
        assert_eq!(sink.in_flight(), 0);
        // Each evaporated permit burnt its slot, and nothing else.
        assert_eq!(issuer.stamps_issued(), Some(10));

        // The window recovered, so the same sink admits a full window again.
        let more = drive(&mut sink, &addresses(2), 2);
        assert_eq!(more.len(), 2);
        assert_eq!(issuer.stamps_issued(), Some(12));
    }

    /// Dropping the sink with jobs parked in the signer must leave the issuer
    /// coherent: the parked slots burn, and nothing else moves.
    #[test]
    fn a_sink_dropped_mid_flight_burns_only_the_parked_slots() {
        let issuer = issuer24();
        let (release_tx, release_rx) = mpsc::channel();
        let pipeline = StampPipeline::from_signer(BlockingSigner(Mutex::new(release_rx)))
            .with_window(window(2));

        {
            let mut sink = pipeline.sink(&issuer, ThreadSpawner);
            for &address in &addresses(2) {
                assert!(sink.poll_admit(&mut noop_cx(), address).is_ready());
            }
            // The window is full while both jobs sit in the signer.
            let extra = addresses(1)[0];
            assert!(sink.poll_admit(&mut noop_cx(), extra).is_pending());
        }
        release_tx.send(()).unwrap();
        release_tx.send(()).unwrap();

        assert_eq!(issuer.stamps_issued(), Some(2));

        // A fresh sink over the same issuer admits a full window.
        let pipeline = StampPipeline::from_signer(FixedSigner).with_window(window(2));
        let mut sink = pipeline.sink(&issuer, InlineSpawner);
        let results = drive(&mut sink, &addresses(4), 2);
        assert_eq!(results.len(), 4);
        assert!(results.iter().all(|r| r.result.is_ok()));
        assert_eq!(issuer.stamps_issued(), Some(6));
    }

    #[test]
    fn pause_parks_admission_and_resume_wakes() {
        let issuer = issuer24();
        let pipeline = StampPipeline::from_signer(FixedSigner).with_window(window(4));
        let address = ChunkAddress::new([0xCD; 32]);

        let mut sink = pipeline.sink(&issuer, InlineSpawner);
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
        let issuer = issuer24();
        let pipeline = StampPipeline::from_signer(BlockingSigner(Mutex::new(release_rx)))
            .with_window(window(4));
        let address = ChunkAddress::new([0xEF; 32]);

        let mut sink = pipeline.sink(&issuer, ThreadSpawner);
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
        let issuer = issuer24();
        let pipeline = StampPipeline::from_signer(FixedSigner).with_window(window(4));

        {
            let mut sink = pipeline.sink(&issuer, InlineSpawner);
            for &address in &addresses(4) {
                assert!(sink.poll_admit(&mut noop_cx(), address).is_ready());
            }
        }
        assert_eq!(issuer.stamps_issued(), Some(4));

        // The issuer stays coherent for a fresh sink.
        let mut sink = pipeline.sink(&issuer, InlineSpawner);
        let results = drive(&mut sink, &addresses(5), 4);
        drop(sink);
        assert_eq!(results.len(), 5);
        assert!(results.iter().all(|r| r.result.is_ok()));
        assert_eq!(issuer.stamps_issued(), Some(9));
    }

    #[test]
    fn drained_sink_reports_none_and_stays_usable() {
        let issuer = issuer24();
        let pipeline = StampPipeline::from_signer(FixedSigner).with_window(window(4));

        let mut sink = pipeline.sink(&issuer, InlineSpawner);
        assert!(matches!(sink.poll_next(&mut noop_cx()), Poll::Ready(None)));

        let first = drive(&mut sink, &addresses(1), 4);
        assert_eq!(first.len(), 1);
        assert!(matches!(sink.poll_next(&mut noop_cx()), Poll::Ready(None)));

        // Drained is not terminated: admission restarts the stream.
        let second = drive(&mut sink, &addresses(1), 4);
        assert_eq!(second.len(), 1);
    }

    /// The batch is the unit of work: one spawn per admission batch, not one
    /// per digest.
    #[test]
    fn an_admission_batch_costs_one_task() {
        let issuer = issuer24();
        let spawns = Arc::new(AtomicUsize::new(0));
        let pipeline = StampPipeline::from_signer(FixedSigner).with_window(window(16));

        let mut sink = pipeline.sink(&issuer, CountingSpawner(Arc::clone(&spawns)));
        sink.admit_batch(&addresses(16));

        assert_eq!(spawns.load(Ordering::SeqCst), 1);
        let mut results = Vec::new();
        while let Poll::Ready(Some(result)) = sink.poll_next(&mut noop_cx()) {
            results.push(result);
        }
        assert_eq!(results.len(), 16);
        assert!(results.iter().all(|r| r.result.is_ok()));
        assert_eq!(issuer.stamps_issued(), Some(16));
    }

    /// A refusal inside a batch is complete at admission, so it never reaches
    /// the sign job and never grows it.
    #[test]
    fn a_refused_address_leaves_the_batch() {
        // depth=17, bucket_depth=16 gives 2 slots per bucket.
        let issuer: MemoryIssuer =
            MemoryIssuer::new(BatchId::ZERO, 17, BucketDepth::new(16).unwrap());
        let spawns = Arc::new(AtomicUsize::new(0));
        let pipeline = StampPipeline::from_signer(FixedSigner).with_window(window(8));
        let address = ChunkAddress::new([0xAB; 32]);

        let mut sink = pipeline.sink(&issuer, CountingSpawner(Arc::clone(&spawns)));
        sink.admit_batch(&[address, address, address]);

        assert_eq!(spawns.load(Ordering::SeqCst), 1);
        let mut results = Vec::new();
        while let Poll::Ready(Some(result)) = sink.poll_next(&mut noop_cx()) {
            results.push(result);
        }
        assert_eq!(results.len(), 3);
        assert_eq!(results.iter().filter(|r| r.result.is_ok()).count(), 2);
        assert!(results.iter().any(|r| matches!(
            r.result,
            Err(SigningError::Stamp(StampError::BucketFull { .. }))
        )));
        assert_eq!(issuer.stamps_issued(), Some(2));
    }

    /// Fail-fast is per batch, not per set: a systemic failure among
    /// batch-mates stops admission as soon as its batch lands.
    #[test]
    fn a_batch_mate_failure_stops_admission() {
        let issuer = issuer24();
        let pipeline =
            StampPipeline::from_signer(FailOnce(AtomicUsize::new(0))).with_window(window(8));

        let mut sink = pipeline.sink(&issuer, InlineSpawner);
        sink.admit_batch(&addresses(8));
        assert!(!sink.is_failed());

        let mut results = Vec::new();
        while let Poll::Ready(Some(result)) = sink.poll_next(&mut noop_cx()) {
            results.push(result);
        }

        assert_eq!(results.len(), 8);
        assert_eq!(results.iter().filter(|r| r.result.is_ok()).count(), 7);
        // The whole batch still yielded, Ok siblings included.
        assert!(sink.is_failed());
        assert_eq!(issuer.stamps_issued(), Some(8));
    }
}
