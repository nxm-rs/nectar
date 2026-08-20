//! The three split arms and the instruments they share.

use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Waker};
use std::time::{Duration, Instant};

use alloy_primitives::{B256, Signature, U256};
use alloy_signer::SignerSync;
use nectar_file::{File, Policy, PutWindow};
use nectar_postage_issuer::{
    Batch, BatchId, BatchSigner, BucketDepth, Eip191, IssuedBound, MemoryIssuer, StampPipeline,
    StampedChunk, StampedPut, StampedPutError, Validated, Window,
};
use nectar_primitives::{AnyChunkSet, Chunk, ChunkPut, ContentChunk, DEFAULT_BODY_SIZE, Verified};
use nectar_tasks::{BoxFuture, Spawn, TaskHandle};

pub const BODY: usize = DEFAULT_BODY_SIZE;

/// Depth 26 over bucket depth 16: 1024 slots per bucket, so no sweep cell
/// runs into a refusal.
const DEPTH: u8 = 26;

/// Depth 17 over bucket depth 16: two slots per bucket.
const FULL_DEPTH: u8 = 17;

type Bare = Chunk<Verified, AnyChunkSet<BODY>>;
type Pair = StampedChunk<Verified, Validated, BODY>;

#[derive(Debug, Clone, Copy)]
pub struct Outcome {
    pub elapsed: Duration,
    pub delivered: usize,
    pub peak_signs: usize,
}

impl Outcome {
    pub fn chunks_per_second(&self) -> f64 {
        let seconds = self.elapsed.as_secs_f64();
        if seconds > 0.0 {
            self.delivered as f64 / seconds
        } else {
            0.0
        }
    }

    pub fn bytes_per_second(&self, bytes: usize) -> f64 {
        let seconds = self.elapsed.as_secs_f64();
        if seconds > 0.0 {
            bytes as f64 / seconds
        } else {
            0.0
        }
    }
}

/// Distinct pseudo-random bytes, so no two leaves share an address and the
/// per-address idempotence of the decorators never hides a chunk.
pub fn corpus(bytes: usize) -> Vec<u8> {
    let mut state = 0x2545_f491_4f6c_dd1du64;
    (0..bytes)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 24) as u8
        })
        .collect()
}

/// Signer with a fixed round-trip latency, tracking peak concurrency.
#[derive(Debug, Clone, Default)]
pub struct LatentSigner {
    latency: Duration,
    live: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
}

impl LatentSigner {
    pub fn new(latency: Duration) -> Self {
        Self {
            latency,
            live: Arc::default(),
            peak: Arc::default(),
        }
    }

    pub fn peak(&self) -> usize {
        self.peak.load(Ordering::SeqCst)
    }

    fn signature(&self) -> Signature {
        let live = self.live.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(live, Ordering::SeqCst);
        if !self.latency.is_zero() {
            std::thread::sleep(self.latency);
        }
        self.live.fetch_sub(1, Ordering::SeqCst);
        Signature::new(U256::from(1), U256::from(2), false)
    }
}

impl SignerSync for LatentSigner {
    fn sign_hash_sync(&self, _hash: &B256) -> Result<Signature, alloy_signer::Error> {
        Ok(self.signature())
    }

    fn sign_message_sync(&self, _message: &[u8]) -> Result<Signature, alloy_signer::Error> {
        Ok(self.signature())
    }

    fn chain_id_sync(&self) -> Option<u64> {
        None
    }
}

/// One thread per sign job: a remote signer's concurrency without a runtime.
#[derive(Debug)]
pub struct ThreadSpawner;

impl Spawn for ThreadSpawner {
    fn spawn(&self, mut task: BoxFuture<'static, ()>) -> TaskHandle {
        std::thread::spawn(move || {
            // Sign jobs are single-poll futures.
            assert!(
                task.as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_ready()
            );
        });
        TaskHandle::new(|| {})
    }
}

#[derive(Debug, Clone, Default)]
pub struct BareSink(Arc<AtomicUsize>);

impl BareSink {
    pub fn delivered(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }
}

impl ChunkPut<Bare> for BareSink {
    type Error = Infallible;

    async fn put(&self, _chunk: Bare) -> Result<(), Self::Error> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub struct PairSink(Arc<AtomicUsize>);

impl PairSink {
    pub fn delivered(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }
}

impl ChunkPut<Pair> for PairSink {
    type Error = Infallible;

    async fn put(&self, _pair: Pair) -> Result<(), Self::Error> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

fn issuer(depth: u8) -> MemoryIssuer {
    MemoryIssuer::new(BatchId::ZERO, depth, BucketDepth::new(16).unwrap())
}

/// The batch every arm allocates from, owned by the address the bench
/// signers report.
fn batch(depth: u8) -> Batch {
    Batch::new(
        BatchId::ZERO,
        1_000,
        100,
        alloy_primitives::Address::ZERO,
        depth,
        BucketDepth::new(16).unwrap(),
        true,
    )
}

/// Wraps a bench signer, which reports no address of its own.
fn bind<S: SignerSync + Sync>(signer: S, depth: u8) -> BatchSigner<Eip191<S>> {
    BatchSigner::bind(
        Eip191::with_address(signer, alloy_primitives::Address::ZERO),
        batch(depth),
    )
    .unwrap()
}

const fn policy(puts: u16) -> Policy {
    Policy::DEFAULT.with_put_window(PutWindow::new(puts).unwrap())
}

const fn window(slots: u16) -> Window {
    Window::new(slots).unwrap()
}

fn chunk(payload: &[u8]) -> Bare {
    let content: ContentChunk<BODY> = ContentChunk::new(payload.to_vec()).unwrap();
    Chunk::from_envelope(content.into()).unwrap()
}

/// The baseline: a split into a bare-chunk sink, no stamping.
pub fn plain(data: &[u8], puts: u16) -> Outcome {
    let sink = BareSink::default();
    let start = Instant::now();
    nectar_testing::run(File::<_, BODY>::new(&sink, policy(puts)).save(data)).unwrap();
    Outcome {
        elapsed: start.elapsed(),
        delivered: sink.delivered(),
        peak_signs: 0,
    }
}

/// The inline decorator: a put slot holds sign latency plus store latency.
pub fn stamped(data: &[u8], puts: u16, latency: Duration) -> Outcome {
    let signer = LatentSigner::new(latency);
    let sink = PairSink::default();
    let store = StampedPut::new(issuer(DEPTH), bind(signer.clone(), DEPTH), sink.clone()).unwrap();
    let start = Instant::now();
    nectar_testing::run(File::<_, BODY>::new(&store, policy(puts)).save(data)).unwrap();
    Outcome {
        elapsed: start.elapsed(),
        delivered: sink.delivered(),
        peak_signs: signer.peak(),
    }
}

/// The staged decorator: signing runs under a window of its own.
pub fn staged(data: &[u8], puts: u16, latency: Duration, signs: u16) -> Outcome {
    let signer = LatentSigner::new(latency);
    let issuer = issuer(DEPTH);
    let pipeline = StampPipeline::new(bind(signer.clone(), DEPTH)).with_window(window(signs));
    let sink = PairSink::default();
    let staged = pipeline
        .staged_put(&issuer, ThreadSpawner, sink.clone(), window(puts))
        .unwrap();
    let start = Instant::now();
    nectar_testing::run(async {
        File::<_, BODY>::new(&staged, policy(puts))
            .save(data)
            .await
            .unwrap();
        staged.flush().await.unwrap();
    });
    Outcome {
        elapsed: start.elapsed(),
        delivered: sink.delivered(),
        peak_signs: signer.peak(),
    }
}

/// What a decorator does once one allocation is refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Refusal {
    /// Pairs the sink took; three means the address after the refusal still
    /// got through.
    pub delivered: usize,
    /// Whether every later call reports a failure.
    pub shut: bool,
}

/// A repeated address to exhaust two bucket slots, then a fresh address.
fn probe_input() -> (Bare, Bare) {
    (chunk(b"repetitive"), chunk(b"a bucket of its own"))
}

/// The inline decorator under a refusal: the error is per put.
pub fn stamped_refusal() -> Refusal {
    let (repeated, distinct) = probe_input();
    let sink = PairSink::default();
    let store = StampedPut::new(
        issuer(FULL_DEPTH),
        bind(LatentSigner::default(), FULL_DEPTH),
        sink.clone(),
    )
    .unwrap()
    .with_issued_bound(IssuedBound::Off);

    let shut = nectar_testing::run(async {
        for _ in 0..3 {
            let _ = store.put(repeated.clone()).await;
        }
        store.put(distinct).await.is_err()
    });

    Refusal {
        delivered: sink.delivered(),
        shut,
    }
}

/// The staged decorator under a refusal: the whole pipeline poisons.
pub fn staged_refusal() -> Refusal {
    let (repeated, distinct) = probe_input();
    let issuer = issuer(FULL_DEPTH);
    let pipeline =
        StampPipeline::new(bind(LatentSigner::default(), FULL_DEPTH)).with_window(window(4));
    let sink = PairSink::default();
    let staged = pipeline
        .staged_put(&issuer, ThreadSpawner, sink.clone(), window(4))
        .unwrap()
        .with_issued_bound(IssuedBound::Off);

    let shut = nectar_testing::run(async {
        for _ in 0..3 {
            let _ = staged.put(repeated.clone()).await;
        }
        let refused = staged.put(distinct).await.is_err();
        refused && matches!(staged.flush().await, Err(StampedPutError::Poisoned))
    });

    Refusal {
        delivered: sink.delivered(),
        shut,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 64 KiB: sixteen leaves and a root, small enough that the inline arm
    /// stays inside the per-test timeout at 50 ms a signature.
    const BYTES: usize = 64 * 1024;
    const LATENCY: Duration = Duration::from_millis(50);
    const PUTS: u16 = 2;
    const SIGNS: u16 = 256;

    #[test]
    fn a_slow_signer_no_longer_serializes_the_staged_split() {
        let data = corpus(BYTES);
        let plain = plain(&data, PUTS);
        let staged = staged(&data, PUTS, LATENCY, SIGNS);

        assert_eq!(staged.delivered, plain.delivered);
        assert!(
            staged.peak_signs > usize::from(PUTS),
            "sign concurrency {} never passed the put window {PUTS}",
            staged.peak_signs
        );
        // Serializing on the put window would cost delivered/PUTS round
        // trips, well past this bound at these settings.
        assert!(
            staged.elapsed < plain.elapsed + 6 * LATENCY,
            "staged split took {:?} against a plain split of {:?}",
            staged.elapsed,
            plain.elapsed
        );
    }

    #[test]
    fn the_inline_decorator_still_pays_the_signer_in_its_put_slots() {
        let data = corpus(BYTES);
        let stamped = stamped(&data, PUTS, LATENCY);
        let staged = staged(&data, PUTS, LATENCY, SIGNS);

        assert_eq!(stamped.delivered, staged.delivered);
        assert!(
            stamped.peak_signs <= usize::from(PUTS),
            "inline sign concurrency {} passed the put window {PUTS}",
            stamped.peak_signs
        );
        assert!(
            stamped.elapsed > staged.elapsed * 2,
            "inline {:?} against staged {:?}",
            stamped.elapsed,
            staged.elapsed
        );
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn the_rayon_engine_overlaps_its_put_slots_up_to_the_pool_width() {
        let data = corpus(BYTES);
        // A single-threaded pool binds before the put window does.
        let bound = usize::from(PUTS).min(rayon::current_num_threads());
        assert_eq!(stamped(&data, PUTS, LATENCY).peak_signs, bound);
    }

    #[test]
    fn a_refusal_stops_the_staged_decorator_and_not_the_inline_one() {
        let inline = stamped_refusal();
        assert_eq!(
            inline,
            Refusal {
                delivered: 3,
                shut: false
            }
        );

        let staged = staged_refusal();
        assert!(staged.shut, "a refusal left the staged decorator open");
        assert!(
            staged.delivered <= 2,
            "the address after the refusal reached the sink"
        );
    }
}
