//! Streaming stamp pipeline: unordered completion over a bounded sign window.
//!
//! [`StampPipeline::stamp`] admits addresses into the window, allocating
//! indices at admission with one clock read per admission batch, and yields a
//! [`StampResult`] per input address instance as signatures complete, in
//! arbitrary order.
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
//!   unsigned indices; issuer state is coherent at every yield point.

use alloc::collections::VecDeque;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::fmt;
#[cfg(all(feature = "std", not(feature = "parallel")))]
use std::panic::{AssertUnwindSafe, catch_unwind};
#[cfg(feature = "parallel")]
use std::sync::mpsc::{Receiver, Sender, channel};

use nectar_clock::Clock;
#[cfg(feature = "std")]
use nectar_clock::SystemClock;
use nectar_kernel::Window;
use nectar_marker::{MaybeSend, MaybeSync};
use nectar_postage::{Stamp, StampDigest};
use nectar_primitives::ChunkAddress;

use crate::error::SigningError;
use crate::issuer::StampIssuer;
use crate::prepared::prepare_stamps;

mod signer;
// The shared cell behind the decorator: a mutex under std, a cell wherever
// the Send/Sync bounds relax. Hosted no-std builds without `unsync` have
// neither, so the surface is absent there.
#[cfg(any(feature = "std", not(multi_thread)))]
mod stamped_put;
#[cfg(feature = "std")]
mod stamp_sink;
#[cfg(feature = "std")]
mod task;

#[cfg(not(feature = "parallel"))]
use signer::sign_digest;
pub use signer::{Eip191, SignPrehash};
#[cfg(any(feature = "std", not(multi_thread)))]
pub use stamped_put::{IssuedBound, StampedPut, StampedPutError};
#[cfg(feature = "std")]
pub use stamp_sink::StampSink;

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
    /// let mut issuer = MemoryIssuer::new(BatchId::ZERO, 24, BucketDepth::new(16)?);
    /// let pipeline = StampPipeline::from_signer(PrivateKeySigner::random());
    ///
    /// let addresses = [ChunkAddress::new([0xAB; 32]), ChunkAddress::new([0xCD; 32])];
    /// let results: Vec<_> = pipeline.stamp(&mut issuer, addresses).collect();
    ///
    /// assert_eq!(results.len(), 2);
    /// assert!(results.iter().all(|r| r.result.is_ok()));
    /// # Ok::<(), nectar_postage_issuer::StampError>(())
    /// ```
    pub fn stamp<'p, I, A>(
        &'p self,
        issuer: &'p mut I,
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

        Stamped {
            pipeline: self,
            issuer,
            input: addresses.into_iter(),
            input_done: false,
            failed: false,
            ready: VecDeque::new(),
            not_admitted: VecDeque::new(),
            #[cfg(feature = "parallel")]
            channel: channel(),
            #[cfg(feature = "parallel")]
            in_flight: 0,
            #[cfg(not(feature = "parallel"))]
            prepared: VecDeque::new(),
        }
    }
}

/// Unordered completion stream returned by [`StampPipeline::stamp`].
#[must_use = "iterators are lazy; nothing is admitted until polled"]
pub struct Stamped<'p, Sg, C, I: ?Sized, A> {
    pipeline: &'p StampPipeline<Sg, C>,
    issuer: &'p mut I,
    input: A,
    input_done: bool,
    failed: bool,
    /// Allocation failures, complete at admission.
    ready: VecDeque<StampResult>,
    /// The fail-fast tail: addresses never admitted.
    not_admitted: VecDeque<ChunkAddress>,
    /// Both ends held: the sender half keeps the channel connected while
    /// tasks are in flight, so the drain counts instead of detecting
    /// disconnects.
    #[cfg(feature = "parallel")]
    channel: (Sender<StampResult>, Receiver<StampResult>),
    #[cfg(feature = "parallel")]
    in_flight: usize,
    /// Admitted digests awaiting the inline signing step.
    #[cfg(not(feature = "parallel"))]
    prepared: VecDeque<StampDigest>,
}

impl<Sg, C, I: ?Sized, A> fmt::Debug for Stamped<'_, Sg, C, I, A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Stamped")
            .field("in_window", &self.in_window())
            .field("ready", &self.ready.len())
            .field("not_admitted", &self.not_admitted.len())
            .field("input_done", &self.input_done)
            .field("failed", &self.failed)
            .finish_non_exhaustive()
    }
}

impl<Sg, C, I: ?Sized, A> Stamped<'_, Sg, C, I, A> {
    /// Allocated, unsigned stamps currently held in flight.
    #[cfg(feature = "parallel")]
    const fn in_window(&self) -> usize {
        self.in_flight
    }

    /// Allocated, unsigned stamps currently held in flight.
    #[cfg(not(feature = "parallel"))]
    fn in_window(&self) -> usize {
        self.prepared.len()
    }
}

impl<Sg, C, I, A> Stamped<'_, Sg, C, I, A>
where
    Sg: SignPrehash + MaybeSend + MaybeSync + 'static,
    C: Clock,
    I: StampIssuer + ?Sized,
    A: Iterator<Item = ChunkAddress>,
{
    /// Refills the window: allocates a micro-batch with one clock read and
    /// submits every successful allocation for signing.
    fn admit(&mut self) {
        if self.failed || self.input_done {
            return;
        }
        let room = usize::from(self.pipeline.window.get()).saturating_sub(self.in_window());
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
        for preparation in prepare_stamps(&mut *self.issuer, &batch, &self.pipeline.clock) {
            match preparation.result {
                Ok(digest) => self.submit(digest),
                Err(error) => self.ready.push_back(StampResult {
                    address: preparation.address,
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

    #[cfg(feature = "parallel")]
    fn submit(&mut self, digest: StampDigest) {
        task::spawn_sign(
            Arc::clone(&self.pipeline.signer),
            digest,
            self.channel.0.clone(),
        );
        self.in_flight = self.in_flight.saturating_add(1);
    }

    #[cfg(not(feature = "parallel"))]
    fn submit(&mut self, digest: StampDigest) {
        self.prepared.push_back(digest);
    }

    /// Blocks for the next in-flight completion, if any.
    #[cfg(feature = "parallel")]
    fn complete_one(&mut self) -> Option<StampResult> {
        if self.in_flight == 0 {
            return None;
        }
        match self.channel.1.recv() {
            Ok(result) => {
                self.in_flight = self.in_flight.saturating_sub(1);
                Some(result)
            }
            // Unreachable: self.channel.0 keeps the channel connected. Treat
            // it as a drained window rather than panicking.
            Err(_) => {
                self.in_flight = 0;
                None
            }
        }
    }

    /// Signs the next admitted digest inline, if any.
    #[cfg(all(feature = "std", not(feature = "parallel")))]
    fn complete_one(&mut self) -> Option<StampResult> {
        let digest = self.prepared.pop_front()?;
        let address = digest.chunk_address;
        // The signer outlives a caught panic; its interior state across that
        // panic is the caller's contract.
        let result = catch_unwind(AssertUnwindSafe(|| {
            sign_digest(self.pipeline.signer.as_ref(), &digest)
        }))
        .unwrap_or_else(|_| Err(SigningError::Dropped));
        Some(StampResult { address, result })
    }

    /// Signs the next admitted digest inline, if any. Without `std` there is
    /// no unwind boundary: a signer panic propagates.
    #[cfg(not(any(feature = "std", feature = "parallel")))]
    fn complete_one(&mut self) -> Option<StampResult> {
        let digest = self.prepared.pop_front()?;
        let address = digest.chunk_address;
        let result = sign_digest(self.pipeline.signer.as_ref(), &digest);
        Some(StampResult { address, result })
    }
}

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
    use crate::{BatchId, BucketDepth, MemoryIssuer, ShardedIssuer, StampError};
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
        let mut issuer = issuer24();
        let pipeline = StampPipeline::from_signer(FixedSigner);
        let input = addresses(100);

        let results: Vec<_> = pipeline.stamp(&mut issuer, input.iter().copied()).collect();

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
        let mut issuer = issuer24();
        let pipeline = StampPipeline::from_signer(FixedSigner);

        assert!(pipeline.stamp(&mut issuer, []).next().is_none());
        assert_eq!(issuer.stamps_issued(), Some(0));
    }

    #[test]
    fn duplicates_allocate_independently_mixed_ok_err() {
        // depth=17, bucket_depth=16 gives 2 slots per bucket.
        let mut issuer = MemoryIssuer::new(BatchId::ZERO, 17, BucketDepth::new(16).unwrap());
        let pipeline = StampPipeline::from_signer(FixedSigner);
        let address = ChunkAddress::new([0xAB; 32]);

        let results: Vec<_> = pipeline
            .stamp(&mut issuer, [address, address, address])
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
        let mut issuer = issuer24();
        let pipeline = StampPipeline::from_signer(FailingSigner).with_window(window(8));
        let input = addresses(100);
        let exhausted = AtomicBool::new(false);

        let results: Vec<_> = pipeline
            .stamp(
                &mut issuer,
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
        let mut issuer = issuer24();
        let pipeline =
            StampPipeline::from_signer(FailOnce(AtomicUsize::new(0))).with_window(window(8));
        let input = addresses(32);

        let results: Vec<_> = pipeline.stamp(&mut issuer, input.iter().copied()).collect();

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
        let mut issuer = issuer24();
        let pipeline = StampPipeline::from_signer(FailingSigner)
            .with_window(window(8))
            .with_fail_fast(false);

        let results: Vec<_> = pipeline.stamp(&mut issuer, addresses(40)).collect();

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
        let mut issuer = issuer24();
        let pipeline = StampPipeline::from_signer(PanickingSigner).with_window(window(8));
        let input = addresses(20);

        let results: Vec<_> = pipeline.stamp(&mut issuer, input.iter().copied()).collect();

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
        let mut issuer = issuer24();
        let clock = CountingClock {
            now_ns: 1_234_567_890,
            reads: AtomicI64::new(0),
        };
        let pipeline = StampPipeline::with_parts(Eip191::new(FixedSigner), &clock, window(64));

        let results: Vec<_> = pipeline.stamp(&mut issuer, addresses(10)).collect();

        // One admission covers the whole input, so the clock was read once
        // and every timestamp matches it.
        assert_eq!(clock.reads.load(Ordering::SeqCst), 1);
        for result in &results {
            assert_eq!(result.result.as_ref().unwrap().timestamp(), 1_234_567_890);
        }
    }

    #[test]
    fn manual_clock_sets_timestamps() {
        let mut issuer = issuer24();
        let clock = ManualClock::new(2_000_000_000);
        let pipeline = StampPipeline::from_signer(FixedSigner).with_clock(&clock);

        let results: Vec<_> = pipeline.stamp(&mut issuer, addresses(16)).collect();

        assert_eq!(results.len(), 16);
        for result in &results {
            assert_eq!(result.result.as_ref().unwrap().timestamp(), 2_000_000_000);
        }
    }

    #[test]
    fn window_bounds_concurrent_signing() {
        let mut issuer = issuer24();
        let max = Arc::new(AtomicUsize::new(0));
        let gauge = Gauge {
            current: Arc::new(AtomicUsize::new(0)),
            max: Arc::clone(&max),
        };
        let pipeline = StampPipeline::from_signer(gauge).with_window(window(4));

        let results: Vec<_> = pipeline.stamp(&mut issuer, addresses(64)).collect();

        assert_eq!(results.len(), 64);
        assert!(max.load(Ordering::SeqCst) <= 4);
        assert_eq!(issuer.stamps_issued(), Some(64));
    }

    #[test]
    fn dropped_stream_abandons_at_most_one_window() {
        let mut issuer = issuer24();
        let pipeline = StampPipeline::from_signer(FixedSigner).with_window(window(4));

        {
            let mut stream = pipeline.stamp(&mut issuer, addresses(20));
            for _ in 0..3 {
                assert!(stream.next().is_some());
            }
        }

        // At most the three yields plus one window were allocated.
        let allocated = issuer.stamps_issued().unwrap();
        assert!((3..=7).contains(&allocated), "allocated {allocated}");

        // The issuer stays coherent for a fresh run.
        let results: Vec<_> = pipeline.stamp(&mut issuer, addresses(5)).collect();
        assert_eq!(results.len(), 5);
        assert!(results.iter().all(|r| r.result.is_ok()));
        assert_eq!(issuer.stamps_issued(), Some(allocated + 5));
    }

    #[test]
    fn eip191_signature_recovers_to_signer() {
        use nectar_postage::StampIndex;

        let mut issuer = issuer24();
        let signer = PrivateKeySigner::random();
        let signer_address = signer.address();
        let pipeline = StampPipeline::from_signer(signer);
        let address = ChunkAddress::from(B256::random());

        let results: Vec<_> = pipeline.stamp(&mut issuer, [address]).collect();

        let stamp = results[0].result.as_ref().unwrap();
        let digest = StampDigest::new(
            address,
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

    #[test]
    fn default_window_is_clamped() {
        let pipeline = StampPipeline::from_signer(FixedSigner);
        let slots = pipeline.window().get();
        assert!((64..=1024).contains(&slots), "window {slots}");
    }

    #[test]
    fn sharded_issuer_stamps_by_value() {
        let mut issuer = ShardedIssuer::new(BatchId::ZERO, 24, BucketDepth::new(16).unwrap());
        let pipeline = StampPipeline::from_signer(FixedSigner);

        let results: Vec<_> = pipeline.stamp(&mut issuer, addresses(50)).collect();

        assert_eq!(results.len(), 50);
        assert!(results.iter().all(|r| r.result.is_ok()));
        assert_eq!(issuer.stamps_issued(), 50);
    }

    #[test]
    fn sharded_issuer_admits_concurrently_over_shared_handles() {
        let issuer = ShardedIssuer::new(BatchId::ZERO, 24, BucketDepth::new(16).unwrap());
        let pipeline = StampPipeline::from_signer(FixedSigner);

        std::thread::scope(|scope| {
            for _ in 0..4 {
                scope.spawn(|| {
                    let mut handle = &issuer;
                    let results: Vec<_> = pipeline.stamp(&mut handle, addresses(100)).collect();
                    assert_eq!(results.len(), 100);
                    assert!(results.iter().all(|r| r.result.is_ok()));
                });
            }
        });

        assert_eq!(issuer.stamps_issued(), 400);
    }
}
