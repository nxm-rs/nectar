//! Downward-facing stamped sink and the bridges into it.
//!
//! The seam: `ChunkPut` faces up (persistence callers put sealed chunks),
//! [`PutStamped`] faces down (the sink receives the `(chunk, stamp)` pair
//! in-band). A network sink implements [`PutStamped`] directly;
//! [`StampIndifferent`] admits a plain chunk store; [`Tee`] fans one put out
//! to a local leg and a forward leg.

use core::future::Future;

use nectar_primitives::marker::{MaybeSend, MaybeSync};
use nectar_primitives::{AnyChunkSet, ChunkPut, DEFAULT_BODY_SIZE};

use crate::StampedChunk;

/// Async stamped-chunk sink (`&self`).
///
/// The stamp travels in-band with the chunk it pays for. The contract is
/// delivery only: per-address idempotence belongs to a decorator layered
/// above the sink, never to this trait. Implementors use interior
/// mutability, mirroring `ChunkPut`. Ingest ordering: batch existence and
/// authenticity gates run before chunk certification; the typed decode
/// takes the stamp structurally before paying the chunk's acceptance rule.
pub trait PutStamped<const BODY_SIZE: usize = DEFAULT_BODY_SIZE>: MaybeSend + MaybeSync {
    /// Error type for stamped put operations.
    type Error: core::error::Error + MaybeSend + MaybeSync + 'static;

    /// Sink a stamped chunk.
    fn put_stamped(
        &self,
        stamped: StampedChunk<BODY_SIZE>,
    ) -> impl Future<Output = Result<(), Self::Error>> + MaybeSend;
}

impl<const BODY_SIZE: usize, T: PutStamped<BODY_SIZE> + ?Sized> PutStamped<BODY_SIZE> for &T {
    type Error = T::Error;

    fn put_stamped(
        &self,
        stamped: StampedChunk<BODY_SIZE>,
    ) -> impl Future<Output = Result<(), Self::Error>> + MaybeSend {
        (**self).put_stamped(stamped)
    }
}

impl<const BODY_SIZE: usize, T: PutStamped<BODY_SIZE> + ?Sized> PutStamped<BODY_SIZE>
    for alloc::sync::Arc<T>
{
    type Error = T::Error;

    fn put_stamped(
        &self,
        stamped: StampedChunk<BODY_SIZE>,
    ) -> impl Future<Output = Result<(), Self::Error>> + MaybeSend {
        (**self).put_stamped(stamped)
    }
}

/// Admits a stamp-indifferent store as a stamped sink.
///
/// `put_stamped` splits the pair and persists the chunk; the stamp is
/// dropped. Correct only for local-persist targets whose storage does not
/// account for payment.
#[derive(Debug, Clone, Default)]
pub struct StampIndifferent<S> {
    inner: S,
}

impl<S> StampIndifferent<S> {
    /// Wrap a store.
    #[inline]
    #[must_use]
    pub const fn new(inner: S) -> Self {
        Self { inner }
    }

    /// The wrapped store.
    #[inline]
    #[must_use]
    pub const fn inner(&self) -> &S {
        &self.inner
    }

    /// Unwrap into the store.
    #[inline]
    #[must_use]
    pub fn into_inner(self) -> S {
        self.inner
    }
}

impl<const BODY_SIZE: usize, S> PutStamped<BODY_SIZE> for StampIndifferent<S>
where
    S: ChunkPut<AnyChunkSet<BODY_SIZE>>,
{
    type Error = S::Error;

    fn put_stamped(
        &self,
        stamped: StampedChunk<BODY_SIZE>,
    ) -> impl Future<Output = Result<(), Self::Error>> + MaybeSend {
        let (chunk, _stamp) = stamped.into_parts();
        self.inner.put(chunk)
    }
}

/// Fans one stamped put out to two sinks: a local leg, then a forward leg.
///
/// Legs run sequentially and fail fast: a local failure means the pair never
/// reached the forward leg; a forward failure leaves the local write in
/// place. Either failure fails the put.
#[derive(Debug, Clone, Default)]
pub struct Tee<L, F> {
    local: L,
    forward: F,
}

impl<L, F> Tee<L, F> {
    /// Pair a local leg with a forward leg.
    #[inline]
    #[must_use]
    pub const fn new(local: L, forward: F) -> Self {
        Self { local, forward }
    }

    /// The local leg.
    #[inline]
    #[must_use]
    pub const fn local(&self) -> &L {
        &self.local
    }

    /// The forward leg.
    #[inline]
    #[must_use]
    pub const fn forward(&self) -> &F {
        &self.forward
    }

    /// Unwrap into the legs.
    #[inline]
    #[must_use]
    pub fn into_parts(self) -> (L, F) {
        (self.local, self.forward)
    }
}

impl<const BODY_SIZE: usize, L, F> PutStamped<BODY_SIZE> for Tee<L, F>
where
    L: PutStamped<BODY_SIZE>,
    F: PutStamped<BODY_SIZE>,
{
    type Error = TeeError<L::Error, F::Error>;

    async fn put_stamped(&self, stamped: StampedChunk<BODY_SIZE>) -> Result<(), Self::Error> {
        self.local
            .put_stamped(stamped.clone())
            .await
            .map_err(TeeError::Local)?;
        self.forward
            .put_stamped(stamped)
            .await
            .map_err(TeeError::Forward)
    }
}

/// A [`Tee`] put failure, tagged with the leg that refused it.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TeeError<L, F> {
    /// The local leg failed; the pair never reached the forward leg.
    #[error("local leg refused the put")]
    Local(#[source] L),
    /// The forward leg failed; the local write stands.
    #[error("forward leg refused the put")]
    Forward(#[source] F),
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::error::Error as _;
    use std::sync::Arc;
    use std::sync::Mutex;

    use alloy_primitives::Signature;
    use nectar_primitives::{Chunk, ChunkAddress, ChunkHas, ContentChunk, MemoryStore, Verified};
    use nectar_testing::run;

    use super::*;
    use crate::{BatchId, Stamp};

    type Store = MemoryStore<AnyChunkSet<DEFAULT_BODY_SIZE>>;

    fn stamped(payload: &'static [u8]) -> StampedChunk<DEFAULT_BODY_SIZE> {
        let chunk = ContentChunk::new(payload).expect("valid content chunk");
        let chunk: Chunk<Verified, AnyChunkSet<DEFAULT_BODY_SIZE>> =
            Chunk::from_envelope(chunk.into()).expect("locally built chunk certifies");
        let sig = Signature::from_raw(&[1u8; 65]).expect("valid signature");
        let stamp = Stamp::new(BatchId::new([0xaa; 32]), 3, 7, 42, sig);
        StampedChunk::new(chunk, stamp)
    }

    #[derive(Debug, Default)]
    struct RecordingSink {
        seen: Mutex<Vec<ChunkAddress>>,
    }

    impl PutStamped for RecordingSink {
        type Error = Infallible;

        async fn put_stamped(&self, stamped: StampedChunk) -> Result<(), Self::Error> {
            self.seen
                .lock()
                .expect("recording lock")
                .push(*stamped.address());
            Ok(())
        }
    }

    #[derive(Debug, thiserror::Error)]
    #[error("leg refused")]
    struct LegRefused;

    #[derive(Debug, Default)]
    struct FailSink;

    impl PutStamped for FailSink {
        type Error = LegRefused;

        async fn put_stamped(&self, _stamped: StampedChunk) -> Result<(), Self::Error> {
            Err(LegRefused)
        }
    }

    #[test]
    fn stamp_indifferent_persists_the_chunk() {
        run(async {
            let store = Store::new();
            let sink = StampIndifferent::new(&store);
            let pair = stamped(b"local persist");
            let address = *pair.address();

            sink.put_stamped(pair).await.expect("infallible put");
            assert!(store.has(&address).await);
        });
    }

    #[test]
    fn tee_delivers_to_both_legs() {
        run(async {
            let store = Store::new();
            let recorder = RecordingSink::default();
            let tee = Tee::new(StampIndifferent::new(&store), &recorder);
            let pair = stamped(b"both legs");
            let address = *pair.address();

            tee.put_stamped(pair).await.expect("both legs accept");
            assert!(store.has(&address).await);
            assert_eq!(*recorder.seen.lock().expect("recording lock"), [address]);
        });
    }

    #[test]
    fn tee_local_failure_short_circuits() {
        run(async {
            let recorder = RecordingSink::default();
            let tee = Tee::new(FailSink, &recorder);
            let pair = stamped(b"local refusal");

            let err = tee.put_stamped(pair).await.expect_err("local leg fails");
            assert!(matches!(err, TeeError::Local(LegRefused)));
            assert!(recorder.seen.lock().expect("recording lock").is_empty());
            assert!(
                err.source()
                    .expect("the leg error is the source")
                    .downcast_ref::<LegRefused>()
                    .is_some()
            );
        });
    }

    #[test]
    fn tee_forward_failure_keeps_local_write() {
        run(async {
            let store = Store::new();
            let tee = Tee::new(StampIndifferent::new(&store), FailSink);
            let pair = stamped(b"forward refusal");
            let address = *pair.address();

            let err = tee.put_stamped(pair).await.expect_err("forward leg fails");
            assert!(matches!(err, TeeError::Forward(LegRefused)));
            assert!(store.has(&address).await);
            assert!(
                err.source()
                    .expect("the leg error is the source")
                    .downcast_ref::<LegRefused>()
                    .is_some()
            );
        });
    }

    async fn sink_via<S: PutStamped>(sink: S, pair: StampedChunk) -> Result<(), S::Error> {
        sink.put_stamped(pair).await
    }

    #[test]
    fn blanket_impls_delegate() {
        run(async {
            let recorder = Arc::new(RecordingSink::default());
            let pair = stamped(b"delegation");
            let address = *pair.address();

            sink_via(Arc::clone(&recorder), pair.clone())
                .await
                .expect("arc delegates");
            sink_via(&*recorder, pair).await.expect("ref delegates");
            assert_eq!(
                *recorder.seen.lock().expect("recording lock"),
                [address, address]
            );
        });
    }
}
