//! Streaming stamp pipeline: unordered completion over a bounded sign window.
//!
//! [`StampPipeline::stamp`] admits addresses into the window, claiming a slot
//! per address at admission with one clock read per admission batch, and
//! yields a [`StampResult`] per input address instance as signatures complete,
//! in arbitrary order.
//!
//! # Contracts
//!
//! - Multiset 1:1: exactly one result per input address instance, tagged with
//!   its address. Duplicates are never deduped; each instance allocates a
//!   fresh index, so mixed Ok/Err for one address is reachable. The retry set
//!   is the set of addresses with no Ok result, computable from results
//!   alone; collecting into a map drops multiplicity. Dedup upstream where
//!   capacity hygiene matters.
//! - Every admitted job yields exactly one result on every path; under `std`
//!   a signer panic is caught and yields [`SigningError::Dropped`] for the
//!   address captured at admission. Without `std` a signer panic propagates.
//! - Fail-fast (default on): a `Signer` or `Dropped` result stops admission.
//!   Already-admitted completions still yield, Ok and per-item Err alike
//!   (signed stamps are wire-valid; discarding them burns capacity), then
//!   [`SigningError::NotAdmitted`] yields once per never-admitted address in
//!   unspecified order. A lazy input is fully consumed to enumerate that
//!   tail. Allocation failures such as `BucketFull` pass through per item,
//!   consume no index and never trigger fail-fast.
//! - The input iterator must never depend on consuming this pipeline's
//!   output. `next` may block for one signer round-trip, so async callers
//!   wrap iteration in a blocking task.
//! - Dropping a [`Stamped`] abandons at most one window of allocated,
//!   unsigned indices plus one window of signed results still queued; issuer
//!   state is coherent at every yield point.

use alloc::collections::VecDeque;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::fmt;
#[cfg(feature = "std")]
use core::task::{Context, Poll};

use nectar_clock::Clock;
#[cfg(feature = "std")]
use nectar_clock::SystemClock;
use nectar_governor::Window;
use nectar_marker::{MaybeSend, MaybeSync};
use nectar_postage::Stamp;
use nectar_primitives::ChunkAddress;

use crate::error::SigningError;
use crate::issuer::StampIssuer;
#[cfg(not(feature = "std"))]
use crate::permit::{AdmissionWindow, Prepared};
#[cfg(not(feature = "std"))]
use crate::stamper::stamp_timestamp;

#[cfg(feature = "std")]
mod bridge;
// The shared cell behind the decorators: a mutex under std, a cell wherever
// the Send/Sync bounds relax. Hosted no-std builds without `unsync` have
// neither, so the surface is absent there.
#[cfg(any(feature = "std", not(multi_thread)))]
mod shared;
#[cfg(feature = "std")]
mod sign_stage;
mod signer;
#[cfg(feature = "std")]
mod staged_put;
#[cfg(feature = "std")]
mod stamp_sink;
#[cfg(any(feature = "std", not(multi_thread)))]
mod stamped_put;
#[cfg(feature = "std")]
mod task;

#[cfg(feature = "std")]
pub use sign_stage::{SealResult, SignStage};
#[cfg(not(feature = "std"))]
use signer::sign_digest;
pub use signer::{Eip191, SignPrehash};
#[cfg(feature = "std")]
pub use staged_put::StagedPut;
#[cfg(feature = "std")]
pub use stamp_sink::StampSink;
#[cfg(any(feature = "std", not(multi_thread)))]
pub use stamped_put::{IssuedBound, StampedPut, StampedPutError};

/// A completed stamping attempt, tagged with its input address.
#[derive(Debug)]
pub struct StampResult {
    /// The chunk address the attempt was for.
    pub address: ChunkAddress,
    /// The signed stamp, or why the attempt failed.
    pub result: Result<Stamp, SigningError>,
}

/// Sizes the sign window as `clamp(16 x available_parallelism, 64, 1024)`.
#[cfg(feature = "std")]
fn default_window() -> Window {
    let threads = std::thread::available_parallelism().map_or(1, core::num::NonZeroUsize::get);
    let slots = threads.saturating_mul(16).clamp(64, 1024);
    // The clamp bounds are nonzero and fit u16.
    u16::try_from(slots)
        .ok()
        .and_then(Window::new)
        .unwrap_or(Window::DEFAULT)
}

/// The published many-chunk stamping entry.
///
/// [`stamp`](Self::stamp) streams any address iterator through a bounded
/// sign window over any [`StampIssuer`]; results yield unordered under the
/// module contracts. The signer sits behind the sealed [`SignPrehash`] seam;
/// [`from_signer`](Self::from_signer) adapts any synchronous signer via
/// [`Eip191`].
#[cfg(feature = "std")]
pub struct StampPipeline<Sg, C = SystemClock> {
    signer: Arc<Sg>,
    clock: C,
    window: Window,
    fail_fast: bool,
}

/// Without `std` there is no default clock; construct via
/// [`with_parts`](Self::with_parts).
#[cfg(not(feature = "std"))]
pub struct StampPipeline<Sg, C> {
    signer: Arc<Sg>,
    clock: C,
    window: Window,
    fail_fast: bool,
}

impl<Sg, C> fmt::Debug for StampPipeline<Sg, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StampPipeline")
            .field("window", &self.window)
            .field("fail_fast", &self.fail_fast)
            .finish_non_exhaustive()
    }
}

#[cfg(feature = "std")]
impl<Sg> StampPipeline<Sg> {
    /// Creates a pipeline reading stamp timestamps from the system clock,
    /// with the window sized to `clamp(16 x available_parallelism, 64,
    /// 1024)` and fail-fast on.
    pub fn new(signer: Sg) -> Self {
        Self {
            signer: Arc::new(signer),
            clock: SystemClock,
            window: default_window(),
            fail_fast: true,
        }
    }
}

#[cfg(feature = "std")]
impl<S> StampPipeline<Eip191<S>> {
    /// [`new`](Self::new) over the [`Eip191`] adapter, so a synchronous
    /// signer plugs in directly.
    pub fn from_signer(signer: S) -> Self {
        Self::new(Eip191::new(signer))
    }
}

impl<Sg, C> StampPipeline<Sg, C> {
    /// Creates a pipeline from explicit parts, with fail-fast on. The
    /// defaults-free constructor; [`Window::DEFAULT`] is sixteen slots.
    pub fn with_parts(signer: Sg, clock: C, window: Window) -> Self {
        Self {
            signer: Arc::new(signer),
            clock,
            window,
            fail_fast: true,
        }
    }

    /// Replaces the sign window.
    #[must_use]
    pub const fn with_window(mut self, window: Window) -> Self {
        self.window = window;
        self
    }

    /// Sets whether a systemic signer failure stops admission.
    #[must_use]
    pub const fn with_fail_fast(mut self, fail_fast: bool) -> Self {
        self.fail_fast = fail_fast;
        self
    }

    /// Replaces the timestamp source.
    #[must_use]
    pub fn with_clock<D>(self, clock: D) -> StampPipeline<Sg, D> {
        StampPipeline {
            signer: self.signer,
            clock,
            window: self.window,
            fail_fast: self.fail_fast,
        }
    }

    /// The sign window.
    pub const fn window(&self) -> Window {
        self.window
    }

    /// Whether a systemic signer failure stops admission.
    pub const fn fail_fast(&self) -> bool {
        self.fail_fast
    }
}

impl<Sg, C> StampPipeline<Sg, C>
where
    Sg: SignPrehash + MaybeSend + MaybeSync + 'static,
    C: Clock,
{
    /// Stamps every address in `addresses` through `issuer`, yielding one
    /// [`StampResult`] per input address instance in completion order.
    ///
    /// The input must never depend on consuming this iterator's output, and
    /// each `next` may block for one signer round-trip.
    ///
    /// # Example
    ///
    /// ```
    /// use alloy_signer_local::PrivateKeySigner;
    /// use nectar_postage_issuer::{BatchId, BucketDepth, MemoryIssuer, StampPipeline};
    /// use nectar_primitives::ChunkAddress;
    ///
    /// let issuer: MemoryIssuer = MemoryIssuer::new(BatchId::ZERO, 24, BucketDepth::new(16)?);
    /// let pipeline = StampPipeline::from_signer(PrivateKeySigner::random());
    ///
    /// let addresses = [ChunkAddress::new([0xAB; 32]), ChunkAddress::new([0xCD; 32])];
    /// let results: Vec<_> = pipeline.stamp(&issuer, addresses).collect();
    ///
    /// assert_eq!(results.len(), 2);
    /// assert!(results.iter().all(|r| r.result.is_ok()));
    /// # Ok::<(), nectar_postage_issuer::StampError>(())
    /// ```
    pub fn stamp<'p, I, A>(
        &'p self,
        issuer: &'p I,
        addresses: A,
    ) -> Stamped<'p, Sg, C, I, A::IntoIter>
    where
        I: StampIssuer + ?Sized,
        A: IntoIterator<Item = ChunkAddress>,
    {
        // A blocking drain on a pool thread starves the pool the sign tasks
        // need.
        #[cfg(feature = "parallel")]
        debug_assert!(
            rayon::current_thread_index().is_none(),
            "stamp must not be called from a rayon pool thread"
        );

        Stamped::new(self, issuer, addresses.into_iter())
    }
}

/// Unordered completion stream returned by [`StampPipeline::stamp`].
///
/// A blocking bridge over [`StampSink`]: admission, windowing and fail-fast
/// live in the sink; this iterator feeds input in window-sized batches,
/// parks between completions and orders the fail-fast tail after the
/// admitted results.
#[cfg(feature = "std")]
#[must_use = "iterators are lazy; nothing is admitted until polled"]
pub struct Stamped<'p, Sg, C, I: ?Sized, A> {
    sink: StampSink<'p, Sg, C, I, bridge::BlockingSpawn>,
    /// Sign jobs the bridge runs inline, one per pending poll.
    #[cfg(not(feature = "parallel"))]
    jobs: bridge::Jobs,
    input: A,
    input_done: bool,
    /// The fail-fast tail: addresses never admitted.
    not_admitted: VecDeque<ChunkAddress>,
}

/// Unordered completion stream returned by [`StampPipeline::stamp`].
///
/// Without `std` there is no executor seam: admitted digests sign inline,
/// one per `next`.
#[cfg(not(feature = "std"))]
#[must_use = "iterators are lazy; nothing is admitted until polled"]
pub struct Stamped<'p, Sg, C, I: StampIssuer + ?Sized, A> {
    pipeline: &'p StampPipeline<Sg, C>,
    issuer: &'p I,
    /// Occupancy: one token per admitted permit, released as its result yields.
    admission: AdmissionWindow,
    input: A,
    input_done: bool,
    failed: bool,
    /// Allocation failures, complete at admission.
    ready: VecDeque<StampResult>,
    /// The fail-fast tail: addresses never admitted.
    not_admitted: VecDeque<ChunkAddress>,
    /// Admitted permits awaiting the inline signing step.
    prepared: VecDeque<Prepared<I::Spec>>,
}

#[cfg(feature = "std")]
impl<Sg, C, I: ?Sized, A> fmt::Debug for Stamped<'_, Sg, C, I, A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Stamped")
            .field("sink", &self.sink)
            .field("not_admitted", &self.not_admitted.len())
            .field("input_done", &self.input_done)
            .finish_non_exhaustive()
    }
}

#[cfg(not(feature = "std"))]
impl<Sg, C, I: StampIssuer + ?Sized, A> fmt::Debug for Stamped<'_, Sg, C, I, A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Stamped")
            .field("in_window", &self.prepared.len())
            .field("ready", &self.ready.len())
            .field("not_admitted", &self.not_admitted.len())
            .field("input_done", &self.input_done)
            .field("failed", &self.failed)
            .finish_non_exhaustive()
    }
}

#[cfg(feature = "std")]
impl<'p, Sg, C, I, A> Stamped<'p, Sg, C, I, A>
where
    Sg: SignPrehash + MaybeSend + MaybeSync + 'static,
    C: Clock,
    I: StampIssuer + ?Sized,
{
    #[cfg(feature = "parallel")]
    fn new(pipeline: &'p StampPipeline<Sg, C>, issuer: &'p I, input: A) -> Self {
        Self {
            sink: pipeline.sink(issuer, bridge::BlockingSpawn),
            input,
            input_done: false,
            not_admitted: VecDeque::new(),
        }
    }

    #[cfg(not(feature = "parallel"))]
    fn new(pipeline: &'p StampPipeline<Sg, C>, issuer: &'p I, input: A) -> Self {
        let spawn = bridge::BlockingSpawn::default();
        let jobs = spawn.jobs();
        Self {
            sink: pipeline.sink(issuer, spawn),
            jobs,
            input,
            input_done: false,
            not_admitted: VecDeque::new(),
        }
    }
}

#[cfg(feature = "std")]
impl<Sg, C, I, A> Stamped<'_, Sg, C, I, A>
where
    Sg: SignPrehash + MaybeSend + MaybeSync + 'static,
    C: Clock,
    I: StampIssuer + ?Sized,
    A: Iterator<Item = ChunkAddress>,
{
    /// Refills the sink's window: one micro-batch, one clock read.
    fn refill(&mut self) {
        if self.input_done || self.sink.is_failed() {
            return;
        }
        let room = self.sink.room();
        if room == 0 {
            return;
        }
        let mut batch = Vec::with_capacity(room);
        while batch.len() < room {
            match self.input.next() {
                Some(address) => batch.push(address),
                None => {
                    self.input_done = true;
                    break;
                }
            }
        }
        if !batch.is_empty() {
            self.sink.admit_batch(&batch);
        }
    }

    /// Fail-fast: consumes the rest of the input as the never-admitted
    /// tail, yielded after the admitted completions drain.
    fn stop_admission(&mut self) {
        self.not_admitted.extend(self.input.by_ref());
        self.input_done = true;
    }

    /// Blocks until the sink can make progress: runs one queued sign job
    /// inline, or parks until a completion unparks this thread.
    fn wait(&mut self) {
        #[cfg(not(feature = "parallel"))]
        if self.jobs.run_one() {
            return;
        }
        std::thread::park();
    }
}

#[cfg(feature = "std")]
impl<Sg, C, I, A> Iterator for Stamped<'_, Sg, C, I, A>
where
    Sg: SignPrehash + MaybeSend + MaybeSync + 'static,
    C: Clock,
    I: StampIssuer + ?Sized,
    A: Iterator<Item = ChunkAddress>,
{
    type Item = StampResult;

    fn next(&mut self) -> Option<StampResult> {
        let waker = bridge::unpark_current();
        let mut cx = Context::from_waker(&waker);
        loop {
            self.refill();
            match self.sink.poll_next(&mut cx) {
                Poll::Ready(Some(result)) => {
                    if self.sink.is_failed() && !self.input_done {
                        self.stop_admission();
                    }
                    return Some(result);
                }
                Poll::Ready(None) => {
                    if let Some(address) = self.not_admitted.pop_front() {
                        return Some(StampResult {
                            address,
                            result: Err(SigningError::NotAdmitted),
                        });
                    }
                    if self.input_done {
                        return None;
                    }
                }
                Poll::Pending => self.wait(),
            }
        }
    }
}

#[cfg(not(feature = "std"))]
impl<'p, Sg, C, I: StampIssuer + ?Sized, A> Stamped<'p, Sg, C, I, A> {
    fn new(pipeline: &'p StampPipeline<Sg, C>, issuer: &'p I, input: A) -> Self {
        Self {
            pipeline,
            issuer,
            admission: AdmissionWindow::new(pipeline.window),
            input,
            input_done: false,
            failed: false,
            ready: VecDeque::new(),
            not_admitted: VecDeque::new(),
            prepared: VecDeque::new(),
        }
    }
}

#[cfg(not(feature = "std"))]
impl<Sg, C, I, A> Stamped<'_, Sg, C, I, A>
where
    Sg: SignPrehash + MaybeSend + MaybeSync + 'static,
    C: Clock,
    I: StampIssuer + ?Sized,
    A: Iterator<Item = ChunkAddress>,
{
    /// Refills the window: claims a micro-batch of slots with one clock read.
    fn admit(&mut self) {
        if self.failed || self.input_done {
            return;
        }
        let room = self.admission.room();
        if room == 0 {
            return;
        }
        let mut batch = Vec::with_capacity(room);
        while batch.len() < room {
            match self.input.next() {
                Some(address) => batch.push(address),
                None => {
                    self.input_done = true;
                    break;
                }
            }
        }
        if batch.is_empty() {
            return;
        }
        let timestamp = stamp_timestamp(&self.pipeline.clock);
        for address in batch {
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
                Ok(permit) => self.prepared.push_back(permit.with_token(token)),
                Err(error) => self.ready.push_back(StampResult {
                    address,
                    result: Err(SigningError::Stamp(error)),
                }),
            }
        }
    }

    /// Fail-fast: stops admission and consumes the rest of the input as the
    /// never-admitted tail.
    fn stop_admission(&mut self) {
        self.failed = true;
        self.not_admitted.extend(self.input.by_ref());
        self.input_done = true;
    }

    /// Signs the next admitted digest inline, if any. There is no unwind
    /// boundary: a signer panic propagates.
    fn complete_one(&mut self) -> Option<StampResult> {
        let permit = self.prepared.pop_front()?;
        let address = *permit.address();
        let result = sign_digest(self.pipeline.signer.as_ref(), &permit.digest());
        Some(StampResult { address, result })
    }
}

#[cfg(not(feature = "std"))]
impl<Sg, C, I, A> Iterator for Stamped<'_, Sg, C, I, A>
where
    Sg: SignPrehash + MaybeSend + MaybeSync + 'static,
    C: Clock,
    I: StampIssuer + ?Sized,
    A: Iterator<Item = ChunkAddress>,
{
    type Item = StampResult;

    fn next(&mut self) -> Option<StampResult> {
        loop {
            self.admit();
            if let Some(result) = self.ready.pop_front() {
                return Some(result);
            }
            if let Some(result) = self.complete_one() {
                if self.pipeline.fail_fast
                    && !self.failed
                    && matches!(&result.result, Err(error) if error.is_systemic())
                {
                    self.stop_admission();
                }
                return Some(result);
            }
            if let Some(address) = self.not_admitted.pop_front() {
                return Some(StampResult {
                    address,
                    result: Err(SigningError::NotAdmitted),
                });
            }
            if self.input_done {
                return None;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BatchId, BucketDepth, MemoryIssuer, StampError};
    use alloy_primitives::{B256, Signature, U256};
    use alloy_signer::SignerSync;
    use alloy_signer_local::PrivateKeySigner;
    use nectar_clock::ManualClock;
    use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};

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

    /// Counts signing calls.
    #[cfg(not(feature = "parallel"))]
    struct CountingSigner(Arc<AtomicUsize>);

    #[cfg(not(feature = "parallel"))]
    impl SignerSync for CountingSigner {
        fn sign_hash_sync(&self, _hash: &B256) -> Result<Signature, alloy_signer::Error> {
            Ok(fixed_signature())
        }

        fn sign_message_sync(&self, _message: &[u8]) -> Result<Signature, alloy_signer::Error> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(fixed_signature())
        }

        fn chain_id_sync(&self) -> Option<u64> {
            None
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
            std::thread::sleep(std::time::Duration::from_millis(1));
            self.current.fetch_sub(1, Ordering::SeqCst);
            Ok(fixed_signature())
        }

        fn chain_id_sync(&self) -> Option<u64> {
            None
        }
    }

    /// Counts reads; always reports the same instant.
    struct CountingClock {
        now_ns: i64,
        reads: AtomicI64,
    }

    impl Clock for CountingClock {
        fn now_ns(&self) -> i64 {
            self.reads.fetch_add(1, Ordering::SeqCst);
            self.now_ns
        }
    }

    /// Flags whether the wrapped iterator was consumed to exhaustion.
    struct Tracked<'a, I> {
        inner: I,
        exhausted: &'a AtomicBool,
    }

    impl<I: Iterator> Iterator for Tracked<'_, I> {
        type Item = I::Item;

        fn next(&mut self) -> Option<I::Item> {
            let item = self.inner.next();
            if item.is_none() {
                self.exhausted.store(true, Ordering::SeqCst);
            }
            item
        }
    }

    fn sorted(mut addresses: Vec<ChunkAddress>) -> Vec<ChunkAddress> {
        addresses.sort_unstable();
        addresses
    }

    #[test]
    fn multiset_one_to_one_unordered() {
        let issuer = issuer24();
        let pipeline = StampPipeline::from_signer(FixedSigner);
        let input = addresses(100);

        let results: Vec<_> = pipeline.stamp(&issuer, input.iter().copied()).collect();

        assert_eq!(results.len(), 100);
        assert!(results.iter().all(|r| r.result.is_ok()));
        assert_eq!(
            sorted(results.into_iter().map(|r| r.address).collect()),
            sorted(input)
        );
        assert_eq!(issuer.stamps_issued(), Some(100));
    }

    #[test]
    fn empty_input_yields_nothing() {
        let issuer = issuer24();
        let pipeline = StampPipeline::from_signer(FixedSigner);

        assert!(pipeline.stamp(&issuer, []).next().is_none());
        assert_eq!(issuer.stamps_issued(), Some(0));
    }

    #[test]
    fn duplicates_allocate_independently_mixed_ok_err() {
        // depth=17, bucket_depth=16 gives 2 slots per bucket.
        let issuer: MemoryIssuer =
            MemoryIssuer::new(BatchId::ZERO, 17, BucketDepth::new(16).unwrap());
        let pipeline = StampPipeline::from_signer(FixedSigner);
        let address = ChunkAddress::new([0xAB; 32]);

        let results: Vec<_> = pipeline
            .stamp(&issuer, [address, address, address])
            .collect();

        assert_eq!(results.len(), 3);
        assert!(results.iter().all(|r| r.address == address));
        let ok = results.iter().filter(|r| r.result.is_ok()).count();
        assert_eq!(ok, 2);
        assert!(results.iter().any(|r| matches!(
            r.result,
            Err(SigningError::Stamp(StampError::BucketFull { .. }))
        )));
        // BucketFull consumed no index and did not trip fail-fast.
        assert_eq!(issuer.stamps_issued(), Some(2));
        assert!(
            !results
                .iter()
                .any(|r| matches!(r.result, Err(SigningError::NotAdmitted)))
        );
        // The retry set is the set of addresses with no Ok result: empty here,
        // since one instance of the address succeeded.
        let retry: Vec<_> = results
            .iter()
            .filter(|r| {
                !results
                    .iter()
                    .any(|other| other.address == r.address && other.result.is_ok())
            })
            .collect();
        assert!(retry.is_empty());
    }

    #[test]
    fn fail_fast_yields_admitted_then_not_admitted_tail() {
        let issuer = issuer24();
        let pipeline = StampPipeline::from_signer(FailingSigner).with_window(window(8));
        let input = addresses(100);
        let exhausted = AtomicBool::new(false);

        let results: Vec<_> = pipeline
            .stamp(
                &issuer,
                Tracked {
                    inner: input.iter().copied(),
                    exhausted: &exhausted,
                },
            )
            .collect();

        assert_eq!(results.len(), 100);
        // Exactly one window was admitted before the first systemic failure.
        assert!(
            results[..8]
                .iter()
                .all(|r| matches!(r.result, Err(SigningError::Signer(_))))
        );
        assert!(
            results[8..]
                .iter()
                .all(|r| matches!(r.result, Err(SigningError::NotAdmitted)))
        );
        // Utilization equals the admitted count, not the input count.
        assert_eq!(issuer.stamps_issued(), Some(8));
        // The lazy input was fully consumed to enumerate the tail.
        assert!(exhausted.load(Ordering::SeqCst));
        assert_eq!(
            sorted(results.into_iter().map(|r| r.address).collect()),
            sorted(input)
        );
    }

    #[test]
    fn fail_fast_keeps_yielding_admitted_ok_completions() {
        let issuer = issuer24();
        let pipeline =
            StampPipeline::from_signer(FailOnce(AtomicUsize::new(0))).with_window(window(8));
        let input = addresses(32);

        let results: Vec<_> = pipeline.stamp(&issuer, input.iter().copied()).collect();

        assert_eq!(results.len(), 32);
        let ok = results.iter().filter(|r| r.result.is_ok()).count();
        let signer = results
            .iter()
            .filter(|r| matches!(r.result, Err(SigningError::Signer(_))))
            .count();
        let not_admitted = results
            .iter()
            .filter(|r| matches!(r.result, Err(SigningError::NotAdmitted)))
            .count();
        // Admission keeps refilling until the failing completion lands, so
        // the admitted count is only bounded, not exact.
        let admitted = usize::try_from(issuer.stamps_issued().unwrap()).unwrap();
        assert!((8..=32).contains(&admitted), "admitted {admitted}");
        assert_eq!(signer, 1);
        // Every admitted sibling of the failure still yielded, Ok included.
        assert_eq!(ok + signer, admitted);
        assert_eq!(not_admitted, 32 - admitted);
    }

    #[test]
    fn fail_fast_off_yields_every_error() {
        let issuer = issuer24();
        let pipeline = StampPipeline::from_signer(FailingSigner)
            .with_window(window(8))
            .with_fail_fast(false);

        let results: Vec<_> = pipeline.stamp(&issuer, addresses(40)).collect();

        assert_eq!(results.len(), 40);
        assert!(
            results
                .iter()
                .all(|r| matches!(r.result, Err(SigningError::Signer(_))))
        );
        assert_eq!(issuer.stamps_issued(), Some(40));
    }

    #[test]
    fn panicking_signer_keeps_one_to_one_without_hanging() {
        let issuer = issuer24();
        let pipeline = StampPipeline::from_signer(PanickingSigner).with_window(window(8));
        let input = addresses(20);

        let results: Vec<_> = pipeline.stamp(&issuer, input.iter().copied()).collect();

        assert_eq!(results.len(), 20);
        let dropped = results
            .iter()
            .filter(|r| matches!(r.result, Err(SigningError::Dropped)))
            .count();
        let not_admitted = results
            .iter()
            .filter(|r| matches!(r.result, Err(SigningError::NotAdmitted)))
            .count();
        assert_eq!(dropped, 8);
        assert_eq!(not_admitted, 12);
        assert_eq!(issuer.stamps_issued(), Some(8));
        assert_eq!(
            sorted(results.into_iter().map(|r| r.address).collect()),
            sorted(input)
        );
    }

    #[test]
    fn one_clock_read_per_admission() {
        let issuer = issuer24();
        let clock = CountingClock {
            now_ns: 1_234_567_890,
            reads: AtomicI64::new(0),
        };
        let pipeline = StampPipeline::with_parts(Eip191::new(FixedSigner), &clock, window(64));

        let results: Vec<_> = pipeline.stamp(&issuer, addresses(10)).collect();

        // One admission covers the whole input, so the clock was read once
        // and every timestamp matches it.
        assert_eq!(clock.reads.load(Ordering::SeqCst), 1);
        for result in &results {
            assert_eq!(result.result.as_ref().unwrap().timestamp(), 1_234_567_890);
        }
    }

    /// One sign job per admission batch, not one per digest: a round-trip
    /// covers the whole micro-batch, and the rest of it yields without one.
    #[cfg(not(feature = "parallel"))]
    #[test]
    fn next_signs_one_admission_batch_per_call() {
        let issuer = issuer24();
        let calls = Arc::new(AtomicUsize::new(0));
        let pipeline =
            StampPipeline::from_signer(CountingSigner(Arc::clone(&calls))).with_window(window(8));

        let mut stream = pipeline.stamp(&issuer, addresses(20));
        assert!(stream.next().is_some());
        assert_eq!(calls.load(Ordering::SeqCst), 8);

        for _ in 0..7 {
            assert!(stream.next().is_some());
        }
        assert_eq!(calls.load(Ordering::SeqCst), 8);

        assert!(stream.next().is_some());
        assert_eq!(calls.load(Ordering::SeqCst), 16);
    }

    #[test]
    fn manual_clock_sets_timestamps() {
        let issuer = issuer24();
        let clock = ManualClock::new(2_000_000_000);
        let pipeline = StampPipeline::from_signer(FixedSigner).with_clock(&clock);

        let results: Vec<_> = pipeline.stamp(&issuer, addresses(16)).collect();

        assert_eq!(results.len(), 16);
        for result in &results {
            assert_eq!(result.result.as_ref().unwrap().timestamp(), 2_000_000_000);
        }
    }

    #[test]
    fn window_bounds_concurrent_signing() {
        let issuer = issuer24();
        let max = Arc::new(AtomicUsize::new(0));
        let gauge = Gauge {
            current: Arc::new(AtomicUsize::new(0)),
            max: Arc::clone(&max),
        };
        let pipeline = StampPipeline::from_signer(gauge).with_window(window(4));

        let results: Vec<_> = pipeline.stamp(&issuer, addresses(64)).collect();

        assert_eq!(results.len(), 64);
        assert!(max.load(Ordering::SeqCst) <= 4);
        // A serialised window collapses the peak to 1; >= 2 proves genuine
        // overlap without demanding the full window on few-core CI. Gated
        // because the non-parallel build signs inline (peak 1 is correct).
        #[cfg(feature = "parallel")]
        assert!(
            max.load(Ordering::SeqCst) >= 2,
            "blocking iterator serialised the window"
        );
        assert_eq!(issuer.stamps_issued(), Some(64));
    }

    #[test]
    fn dropped_stream_abandons_at_most_two_windows() {
        let issuer = issuer24();
        let pipeline = StampPipeline::from_signer(FixedSigner).with_window(window(4));

        {
            let mut stream = pipeline.stamp(&issuer, addresses(20));
            for _ in 0..3 {
                assert!(stream.next().is_some());
            }
        }

        // At most the three yields, one signed window buffered behind them and
        // one window in flight.
        let allocated = issuer.stamps_issued().unwrap();
        assert!((3..=11).contains(&allocated), "allocated {allocated}");

        // The issuer stays coherent for a fresh run.
        let results: Vec<_> = pipeline.stamp(&issuer, addresses(5)).collect();
        assert_eq!(results.len(), 5);
        assert!(results.iter().all(|r| r.result.is_ok()));
        assert_eq!(issuer.stamps_issued(), Some(allocated + 5));
    }

    /// Every stamp of a batch carries the signature over its own digest.
    #[test]
    fn eip191_signatures_recover_to_signer_across_a_batch() {
        use nectar_postage::{StampDigest, StampIndex};

        let issuer = issuer24();
        let signer = PrivateKeySigner::random();
        let signer_address = signer.address();
        let pipeline = StampPipeline::from_signer(signer).with_window(window(8));

        let results: Vec<_> = pipeline.stamp(&issuer, addresses(8)).collect();

        assert_eq!(results.len(), 8);
        for result in &results {
            let stamp = result.result.as_ref().unwrap();
            let digest = StampDigest::new(
                result.address,
                stamp.batch(),
                StampIndex::new(stamp.bucket(), stamp.index()),
                stamp.timestamp(),
            );
            let recovered = stamp
                .signature()
                .recover_address_from_msg(digest.to_prehash().as_slice())
                .unwrap();
            assert_eq!(recovered, signer_address);
        }
    }

    #[test]
    fn default_window_is_clamped() {
        let pipeline = StampPipeline::from_signer(FixedSigner);
        let slots = pipeline.window().get();
        assert!((64..=1024).contains(&slots), "window {slots}");
    }

    #[test]
    fn a_lock_free_issuer_stamps_by_value() {
        let issuer: MemoryIssuer =
            MemoryIssuer::new(BatchId::ZERO, 24, BucketDepth::new(16).unwrap());
        let pipeline = StampPipeline::from_signer(FixedSigner);

        let results: Vec<_> = pipeline.stamp(&issuer, addresses(50)).collect();

        assert_eq!(results.len(), 50);
        assert!(results.iter().all(|r| r.result.is_ok()));
        assert_eq!(issuer.stamps_issued(), Some(50));
    }

    #[test]
    fn a_lock_free_issuer_admits_concurrently_over_shared_handles() {
        let issuer: MemoryIssuer =
            MemoryIssuer::new(BatchId::ZERO, 24, BucketDepth::new(16).unwrap());
        let pipeline = StampPipeline::from_signer(FixedSigner);

        std::thread::scope(|scope| {
            for _ in 0..4 {
                scope.spawn(|| {
                    let handle = &issuer;
                    let results: Vec<_> = pipeline.stamp(&handle, addresses(100)).collect();
                    assert_eq!(results.len(), 100);
                    assert!(results.iter().all(|r| r.result.is_ok()));
                });
            }
        });

        assert_eq!(issuer.stamps_issued(), Some(400));
    }
}
