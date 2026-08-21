//! Fixtures shared by the pipeline test modules.

use alloc::sync::Arc;
use alloc::vec::Vec;
use core::sync::atomic::{AtomicUsize, Ordering};
use core::task::{Context, Waker};

use alloy_primitives::{Address, B256, Signature, U256};
use nectar_governor::Window;
use nectar_postage::{Batch, Stamp, StampedChunk, Validated};
use nectar_primitives::{
    AnyChunkSet, Chunk, ChunkAddress, ChunkPut, ContentChunk, DEFAULT_BODY_SIZE, Verified,
};
use nectar_tasks::{BoxFuture, Spawn, TaskHandle};
use std::sync::{Mutex, mpsc};
use std::task::Wake;
use std::time::Duration;

use super::signer::sealed::Sealed;
use super::{BatchSigner, SignPrehash};
use crate::error::SigningError;
use crate::testing::batch_at_depth;
use crate::{BatchId, BucketDepth, MemoryIssuer};

pub(super) type TestChunk = Chunk<Verified, AnyChunkSet<DEFAULT_BODY_SIZE>>;

pub(super) fn issuer(depth: u8) -> MemoryIssuer {
    MemoryIssuer::new(BatchId::ZERO, depth, BucketDepth::new(16).unwrap())
}

pub(super) fn issuer24() -> MemoryIssuer {
    issuer(24)
}

/// Binds `signer` to the batch every fixture issuer allocates from. The
/// fixture signers all report [`Address::ZERO`], so the batch is owned by it.
pub(super) fn bound<Sg: SignPrehash>(signer: Sg) -> BatchSigner<Sg> {
    BatchSigner::bind(signer, fixture_batch()).unwrap()
}

fn fixture_batch() -> Batch {
    batch_at_depth(Address::ZERO, BatchId::ZERO, 24)
}

pub(super) const fn window(slots: u16) -> Window {
    Window::new(slots).unwrap()
}

pub(super) const fn noop_cx() -> Context<'static> {
    Context::from_waker(Waker::noop())
}

pub(super) fn sorted(mut addresses: Vec<ChunkAddress>) -> Vec<ChunkAddress> {
    addresses.sort_unstable();
    addresses
}

pub(super) fn chunk(payload: &[u8]) -> TestChunk {
    let content: ContentChunk<DEFAULT_BODY_SIZE> = ContentChunk::new(payload.to_vec()).unwrap();
    Chunk::from_envelope(content.into()).unwrap()
}

pub(super) fn fixed_signature() -> Signature {
    Signature::new(U256::from(1), U256::from(2), false)
}

fn offline() -> SigningError {
    SigningError::Signer(alloy_signer::Error::message("signer offline"))
}

/// Deterministic signature without ECDSA cost.
pub(super) struct FixedSigner;

impl Sealed for FixedSigner {}

impl SignPrehash for FixedSigner {
    fn address(&self) -> Address {
        Address::ZERO
    }

    fn sign_prehash(&self, _prehash: &B256) -> Result<Signature, SigningError> {
        Ok(fixed_signature())
    }
}

/// Fails every signing call.
pub(super) struct FailingSigner;

impl Sealed for FailingSigner {}

impl SignPrehash for FailingSigner {
    fn address(&self) -> Address {
        Address::ZERO
    }

    fn sign_prehash(&self, _prehash: &B256) -> Result<Signature, SigningError> {
        Err(offline())
    }
}

/// Blocks each signing call until released over the channel.
pub(super) struct BlockingSigner(pub(super) Mutex<mpsc::Receiver<()>>);

impl Sealed for BlockingSigner {}

impl SignPrehash for BlockingSigner {
    fn address(&self) -> Address {
        Address::ZERO
    }

    fn sign_prehash(&self, _prehash: &B256) -> Result<Signature, SigningError> {
        let _ = self.0.lock().unwrap().recv();
        Ok(fixed_signature())
    }
}

/// Tracks the highest number of concurrent signing calls.
pub(super) struct Gauge {
    current: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
    delay: Duration,
}

impl Gauge {
    /// A gauge delaying each signature by `delay`, with its peak counter.
    pub(super) fn new(delay: Duration) -> (Self, Arc<AtomicUsize>) {
        let peak = Arc::new(AtomicUsize::new(0));
        let gauge = Self {
            current: Arc::new(AtomicUsize::new(0)),
            peak: Arc::clone(&peak),
            delay,
        };
        (gauge, peak)
    }
}

impl Sealed for Gauge {}

impl SignPrehash for Gauge {
    fn address(&self) -> Address {
        Address::ZERO
    }

    fn sign_prehash(&self, _prehash: &B256) -> Result<Signature, SigningError> {
        let now = self.current.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(now, Ordering::SeqCst);
        std::thread::sleep(self.delay);
        self.current.fetch_sub(1, Ordering::SeqCst);
        Ok(fixed_signature())
    }
}

/// Counts the wakes one poller receives.
#[derive(Default)]
pub(super) struct WakeCount(AtomicUsize);

impl WakeCount {
    pub(super) fn count(self: &Arc<Self>) -> usize {
        self.0.load(Ordering::SeqCst)
    }
}

// reinvention: test-only wake counter for the pump's poll drivers.
impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// Completes each job synchronously inside `spawn`.
pub(super) struct InlineSpawner;

impl Spawn for InlineSpawner {
    fn spawn(&self, mut task: BoxFuture<'static, ()>) -> TaskHandle {
        // Sign jobs are single-poll futures.
        assert!(task.as_mut().poll(&mut noop_cx()).is_ready());
        TaskHandle::new(|| {})
    }
}

/// Runs each job on its own thread.
pub(super) struct ThreadSpawner;

impl Spawn for ThreadSpawner {
    fn spawn(&self, mut task: BoxFuture<'static, ()>) -> TaskHandle {
        std::thread::spawn(move || {
            // Sign jobs are single-poll futures.
            assert!(task.as_mut().poll(&mut noop_cx()).is_ready());
        });
        TaskHandle::new(|| {})
    }
}

/// Records every pair the sink receives.
#[derive(Debug, Clone, Default)]
pub(super) struct CountingSink {
    pub(super) seen: Arc<Mutex<Vec<(ChunkAddress, Stamp)>>>,
}

impl<const B: usize> ChunkPut<StampedChunk<Verified, Validated, B>> for CountingSink {
    type Error = core::convert::Infallible;

    async fn put(&self, stamped: StampedChunk<Verified, Validated, B>) -> Result<(), Self::Error> {
        self.seen
            .lock()
            .unwrap()
            .push((*stamped.address(), stamped.stamp().clone()));
        Ok(())
    }
}

#[derive(Debug, PartialEq, thiserror::Error)]
#[error("sink refused")]
pub(super) struct SinkRefused;

/// Refuses every pair.
#[derive(Clone, Default)]
pub(super) struct RefusingSink;

impl<const B: usize> ChunkPut<StampedChunk<Verified, Validated, B>> for RefusingSink {
    type Error = SinkRefused;

    async fn put(&self, _stamped: StampedChunk<Verified, Validated, B>) -> Result<(), Self::Error> {
        Err(SinkRefused)
    }
}
