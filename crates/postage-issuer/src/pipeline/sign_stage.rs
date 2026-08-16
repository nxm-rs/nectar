//! The sign stage: chunks in, sealed pairs out, unordered, under a sign
//! window of its own.
//!
//! The [pipeline module](super) contracts hold unchanged, stated once there,
//! with the input an instance of a chunk rather than of an address. Stage
//! shaped deltas: sealed pairs buffer at one sign window, so a stalled drain
//! parks [`SignStage::poll_admit`] rather than growing the buffer, and the
//! [`Stream`] ends only once [`SignStage::close`] has run and the stage has
//! drained.

use alloc::collections::{BTreeMap, VecDeque};
use core::fmt;
use core::pin::Pin;
use core::task::{Context, Poll, ready};

use futures_util::stream::Stream;
use nectar_clock::Clock;
use nectar_governor::Window;
use nectar_marker::{MaybeSend, MaybeSync};
use nectar_postage::{StampedChunk, Unvalidated};
use nectar_primitives::{AnyChunkSet, Chunk, ChunkAddress, DEFAULT_BODY_SIZE, Verified};
use nectar_tasks::Spawn;

use super::{SignPrehash, StampPipeline, StampResult, StampSink};
use crate::error::SigningError;
use crate::issuer::StampIssuer;

/// A completed sealing attempt, tagged with its input address.
#[derive(Debug)]
pub struct SealResult<const BODY_SIZE: usize = DEFAULT_BODY_SIZE> {
    /// The chunk address the attempt was for.
    pub address: ChunkAddress,
    /// The sealed pair, or why the attempt failed.
    pub result: Result<StampedChunk<Verified, Unvalidated, BODY_SIZE>, SigningError>,
}

impl<Sg, C> StampPipeline<Sg, C>
where
    Sg: SignPrehash + MaybeSend + MaybeSync + 'static,
    C: Clock,
{
    /// The sign stage over `issuer`: offer chunks through
    /// [`SignStage::poll_admit`], collect sealed pairs through
    /// [`SignStage::poll_next`]. Sign jobs run on `spawner`.
    ///
    /// The stage occupies no put capacity, so a put stage draining it holds a
    /// slot for store latency alone.
    pub fn sign_stage<'p, I, S, const BODY_SIZE: usize>(
        &'p self,
        issuer: &'p I,
        spawner: S,
    ) -> SignStage<'p, Sg, C, I, S, BODY_SIZE>
    where
        I: StampIssuer + ?Sized,
        S: Spawn,
    {
        SignStage {
            sink: self.sink(issuer, spawner),
            window: self.window(),
            awaiting: BTreeMap::new(),
            sealed: VecDeque::new(),
            closed: false,
        }
    }
}

/// Sign stage returned by [`StampPipeline::sign_stage`].
///
/// Dropping the stage aborts its in-flight jobs and abandons at most one
/// window of allocated, unsigned indices; issuer state is coherent at every
/// yield point.
pub struct SignStage<'p, Sg, C, I: ?Sized, S, const BODY_SIZE: usize = DEFAULT_BODY_SIZE> {
    sink: StampSink<'p, Sg, C, I, S>,
    window: Window,
    /// Chunks admitted for signing, in admission order per address. Instances
    /// of one address are interchangeable, so any of them pairs with a result.
    awaiting: BTreeMap<ChunkAddress, VecDeque<Chunk<Verified, AnyChunkSet<BODY_SIZE>>>>,
    /// Sealed pairs awaiting the drain, capped at one sign window.
    sealed: VecDeque<SealResult<BODY_SIZE>>,
    closed: bool,
}

impl<Sg, C, I: ?Sized, S, const BODY_SIZE: usize> fmt::Debug
    for SignStage<'_, Sg, C, I, S, BODY_SIZE>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SignStage")
            .field("sink", &self.sink)
            .field("awaiting", &self.awaiting.len())
            .field("sealed", &self.sealed.len())
            .field("closed", &self.closed)
            .finish_non_exhaustive()
    }
}

impl<Sg, C, I: ?Sized, S, const BODY_SIZE: usize> SignStage<'_, Sg, C, I, S, BODY_SIZE> {
    /// Sign jobs admitted and not yet sealed.
    pub fn in_flight(&self) -> usize {
        self.sink.in_flight()
    }

    /// Sealed pairs buffered for the drain.
    pub fn sealed(&self) -> usize {
        self.sealed.len()
    }

    /// Whether fail-fast has stopped admission.
    pub const fn is_failed(&self) -> bool {
        self.sink.is_failed()
    }

    /// The sign window sealed pairs buffer against.
    pub const fn window(&self) -> Window {
        self.window
    }

    /// Whether every admitted chunk has been sealed and collected.
    pub fn is_drained(&self) -> bool {
        self.sealed.is_empty() && self.awaiting.is_empty() && self.in_flight() == 0
    }

    /// Ends admission, so the [`Stream`] finishes once the stage drains.
    pub const fn close(&mut self) {
        self.closed = true;
    }

    /// Sealed-pair slots free to buffer into.
    fn room(&self) -> usize {
        usize::from(self.window.get()).saturating_sub(self.sealed.len())
    }
}

impl<Sg, C, I, S, const BODY_SIZE: usize> SignStage<'_, Sg, C, I, S, BODY_SIZE>
where
    Sg: SignPrehash + MaybeSend + MaybeSync + 'static,
    C: Clock,
    I: StampIssuer + ?Sized,
    S: Spawn,
{
    /// Offers `chunk` for signing, taking it from the slot on admission.
    ///
    /// `Ready` consumes the chunk: it was admitted, or its failure is queued
    /// for [`poll_next`](Self::poll_next). `Pending` leaves it in the slot:
    /// the sign window is full or a window of sealed pairs awaits the drain,
    /// and the same slot must be offered again once a drain has run.
    pub fn poll_admit(
        &mut self,
        cx: &mut Context<'_>,
        slot: &mut Option<Chunk<Verified, AnyChunkSet<BODY_SIZE>>>,
    ) -> Poll<()> {
        debug_assert!(!self.closed, "admission after close");
        self.harvest(cx);
        let Some(chunk) = slot.as_ref() else {
            return Poll::Ready(());
        };
        if self.room() == 0 {
            return Poll::Pending;
        }
        let address = *chunk.address();
        ready!(self.sink.poll_admit(cx, address));
        // Before any harvest, so every queued result finds its chunk.
        if let Some(chunk) = slot.take() {
            self.awaiting.entry(address).or_default().push_back(chunk);
        }
        Poll::Ready(())
    }

    /// Polls for the next sealed pair.
    ///
    /// `Ready(None)` reports the stage drained after
    /// [`close`](Self::close); an open stage parks instead, because the
    /// admitter that would refill it shares this task.
    pub fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<SealResult<BODY_SIZE>>> {
        self.harvest(cx);
        if let Some(sealed) = self.sealed.pop_front() {
            return Poll::Ready(Some(sealed));
        }
        if self.closed && self.is_drained() {
            return Poll::Ready(None);
        }
        Poll::Pending
    }

    /// Buffers completions up to one sign window.
    fn harvest(&mut self, cx: &mut Context<'_>) {
        while self.room() > 0 {
            match self.sink.poll_next(cx) {
                Poll::Ready(Some(result)) => {
                    let sealed = self.pair(result);
                    self.sealed.push_back(sealed);
                }
                Poll::Ready(None) | Poll::Pending => return,
            }
        }
    }

    /// Pairs one stamp with the chunk it was allocated for.
    fn pair(&mut self, result: StampResult) -> SealResult<BODY_SIZE> {
        let address = result.address;
        let chunk = self.take_awaiting(&address);
        debug_assert!(chunk.is_some(), "a sealed address kept no chunk");
        let result = match (result.result, chunk) {
            (Ok(stamp), Some(chunk)) => Ok(StampedChunk::new(chunk, stamp)),
            (Ok(_), None) => Err(SigningError::Dropped),
            (Err(error), _) => Err(error),
        };
        SealResult { address, result }
    }

    fn take_awaiting(
        &mut self,
        address: &ChunkAddress,
    ) -> Option<Chunk<Verified, AnyChunkSet<BODY_SIZE>>> {
        let queue = self.awaiting.get_mut(address)?;
        let chunk = queue.pop_front();
        if queue.is_empty() {
            self.awaiting.remove(address);
        }
        chunk
    }
}

impl<Sg, C, I, S, const BODY_SIZE: usize> Stream for SignStage<'_, Sg, C, I, S, BODY_SIZE>
where
    Sg: SignPrehash + MaybeSend + MaybeSync + 'static,
    C: Clock,
    I: StampIssuer + ?Sized,
    S: Spawn + Unpin,
{
    type Item = SealResult<BODY_SIZE>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Self::poll_next(self.get_mut(), cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BatchId, BucketDepth, MemoryIssuer, StampError};
    use alloc::vec::Vec;
    use alloy_primitives::{B256, Signature, U256};
    use alloy_signer::SignerSync;
    use core::task::Waker;
    use futures_util::StreamExt;
    use nectar_primitives::ContentChunk;
    use nectar_tasks::TaskHandle;
    use std::time::{Duration, Instant};

    type TestChunk = Chunk<Verified, AnyChunkSet<DEFAULT_BODY_SIZE>>;

    fn issuer24() -> MemoryIssuer {
        MemoryIssuer::new(BatchId::ZERO, 24, BucketDepth::new(16).unwrap())
    }

    fn window(slots: u16) -> Window {
        Window::new(slots).unwrap()
    }

    fn noop_cx() -> Context<'static> {
        Context::from_waker(Waker::noop())
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

    /// Completes each job synchronously inside `spawn`.
    struct InlineSpawner;

    impl Spawn for InlineSpawner {
        fn spawn(&self, mut task: nectar_tasks::BoxFuture<'static, ()>) -> TaskHandle {
            // Sign jobs are single-poll futures.
            assert!(task.as_mut().poll(&mut noop_cx()).is_ready());
            TaskHandle::new(|| {})
        }
    }

    /// Feeds every chunk, draining while the stage is full.
    fn drive<Sg, C, I, S, const B: usize>(
        stage: &mut SignStage<'_, Sg, C, I, S, B>,
        input: Vec<Chunk<Verified, AnyChunkSet<B>>>,
    ) -> Vec<SealResult<B>>
    where
        Sg: SignPrehash + MaybeSend + MaybeSync + 'static,
        C: Clock,
        I: StampIssuer + ?Sized,
        S: Spawn,
    {
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut cx = noop_cx();
        let mut results = Vec::new();
        for chunk in input {
            let mut slot = Some(chunk);
            while slot.is_some() {
                assert!(Instant::now() < deadline, "admission stalled");
                if stage.poll_admit(&mut cx, &mut slot).is_pending()
                    && let Poll::Ready(Some(sealed)) = stage.poll_next(&mut cx)
                {
                    results.push(sealed);
                }
            }
        }
        stage.close();
        loop {
            match stage.poll_next(&mut cx) {
                Poll::Ready(Some(sealed)) => results.push(sealed),
                Poll::Ready(None) => return results,
                Poll::Pending => {
                    assert!(Instant::now() < deadline, "drain stalled");
                    std::thread::yield_now();
                }
            }
        }
    }

    fn sorted(mut addresses: Vec<ChunkAddress>) -> Vec<ChunkAddress> {
        addresses.sort_unstable();
        addresses
    }

    #[test]
    fn multiset_one_to_one_over_sealed_pairs() {
        let issuer = issuer24();
        let pipeline = StampPipeline::from_signer(FixedSigner).with_window(window(4));
        let input: Vec<TestChunk> = (0..50u32)
            .map(|index| chunk(&index.to_be_bytes()))
            .collect();
        let addresses: Vec<_> = input.iter().map(|chunk| *chunk.address()).collect();

        let mut stage = pipeline.sign_stage(&issuer, InlineSpawner);
        let results = drive(&mut stage, input);

        assert_eq!(results.len(), 50);
        assert_eq!(
            sorted(results.iter().map(|sealed| sealed.address).collect()),
            sorted(addresses)
        );
        // Every pair carries the chunk the stamp was allocated for.
        for sealed in &results {
            let pair = sealed.result.as_ref().unwrap();
            assert_eq!(pair.address(), &sealed.address);
        }
        assert_eq!(issuer.stamps_issued(), Some(50));
    }

    #[test]
    fn duplicate_chunks_allocate_independently() {
        // depth=17, bucket_depth=16 gives 2 slots per bucket.
        let issuer: MemoryIssuer =
            MemoryIssuer::new(BatchId::ZERO, 17, BucketDepth::new(16).unwrap());
        let pipeline = StampPipeline::from_signer(FixedSigner).with_window(window(4));
        let repeated = chunk(b"repetitive");

        let mut stage = pipeline.sign_stage(&issuer, InlineSpawner);
        let results = drive(&mut stage, alloc::vec![repeated.clone(), repeated.clone(), repeated]);

        assert_eq!(results.len(), 3);
        assert_eq!(results.iter().filter(|r| r.result.is_ok()).count(), 2);
        assert!(results.iter().any(|r| matches!(
            r.result,
            Err(SigningError::Stamp(StampError::BucketFull { .. }))
        )));
        assert_eq!(issuer.stamps_issued(), Some(2));
    }

    #[test]
    fn fail_fast_yields_a_not_admitted_tail() {
        let issuer = issuer24();
        let pipeline = StampPipeline::from_signer(FailingSigner).with_window(window(4));
        let input: Vec<TestChunk> = (0..10u32).map(|index| chunk(&index.to_be_bytes())).collect();

        let mut stage = pipeline.sign_stage(&issuer, InlineSpawner);
        let results = drive(&mut stage, input);

        assert_eq!(results.len(), 10);
        let signed = results
            .iter()
            .filter(|r| matches!(r.result, Err(SigningError::Signer(_))))
            .count();
        let not_admitted = results
            .iter()
            .filter(|r| matches!(r.result, Err(SigningError::NotAdmitted)))
            .count();
        assert!(signed >= 1, "no signature was attempted");
        assert_eq!(signed + not_admitted, 10);
        // Utilization equals the admitted count, not the offered count.
        assert_eq!(issuer.stamps_issued(), Some(signed as u64));
        assert!(stage.is_failed());
    }

    /// The buffer between the stages is one sign window of pairs: a drain
    /// that never runs parks admission rather than growing it.
    #[test]
    fn a_stalled_drain_parks_admission_at_one_window() {
        let issuer = issuer24();
        let pipeline = StampPipeline::from_signer(FixedSigner).with_window(window(4));

        let mut stage = pipeline.sign_stage(&issuer, InlineSpawner);
        let mut admitted = 0;
        for index in 0..64u32 {
            let mut slot = Some(chunk(&index.to_be_bytes()));
            if stage.poll_admit(&mut noop_cx(), &mut slot).is_pending() {
                break;
            }
            admitted += 1;
        }

        assert_eq!(stage.sealed(), 4);
        // One window buffered, and no admission ran past it.
        assert_eq!(admitted, 4);
        assert_eq!(issuer.stamps_issued(), Some(4));
    }

    #[test]
    fn the_stream_ends_only_once_closed_and_drained() {
        nectar_testing::run(async {
            let issuer = issuer24();
            let pipeline = StampPipeline::from_signer(FixedSigner).with_window(window(4));

            let mut stage = pipeline.sign_stage(&issuer, InlineSpawner);
            let mut slot = Some(chunk(b"streamed"));
            assert!(stage.poll_admit(&mut noop_cx(), &mut slot).is_ready());
            assert!(stage.next().await.is_some());
            // Open and drained: the stream parks rather than terminating.
            assert!(Pin::new(&mut stage).poll_next(&mut noop_cx()).is_pending());

            stage.close();
            assert!(stage.next().await.is_none());
        });
    }
}
