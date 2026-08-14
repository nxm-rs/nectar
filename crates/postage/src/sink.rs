//! The stamped chunk as a unit of transfer, and the bridge beneath it.
//!
//! One seam carries both directions: a network sink is a
//! `ChunkPut<StampedChunk<Verified, V, BODY_SIZE>>`, and
//! [`StampIndifferent`] puts a plain chunk store beneath one. A consumer that
//! needs payment proof names [`Validated`], and an unproven pair is then a
//! type error at the call:
//!
//! ```compile_fail
//! use nectar_postage::{StampedChunk, Unvalidated, Validated};
//! use nectar_primitives::{ChunkPut, Verified};
//!
//! async fn pushsync<S: ChunkPut<StampedChunk<Verified, Validated>>>(
//!     sink: S,
//!     pair: StampedChunk<Verified, Unvalidated>,
//! ) -> Result<(), S::Error> {
//!     sink.put(pair).await
//! }
//! ```

use core::future::Future;

use nectar_primitives::marker::MaybeSend;
use nectar_primitives::{AnyChunkSet, Chunk, ChunkAddress, ChunkPut, PutUnit, Verified};

use crate::{StampedChunk, ValidationState};

impl<V: ValidationState, const BODY_SIZE: usize> PutUnit for StampedChunk<Verified, V, BODY_SIZE> {
    #[inline]
    fn address(&self) -> &ChunkAddress {
        Self::address(self)
    }
}

/// Admits a stamp-indifferent store as a stamped sink.
///
/// The put splits the pair and persists the chunk; the stamp is dropped.
/// Correct only for local-persist targets whose storage does not account for
/// payment.
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

impl<V: ValidationState, const BODY_SIZE: usize, S> ChunkPut<StampedChunk<Verified, V, BODY_SIZE>>
    for StampIndifferent<S>
where
    S: ChunkPut<Chunk<Verified, AnyChunkSet<BODY_SIZE>>>,
{
    type Error = S::Error;

    fn put(
        &self,
        stamped: StampedChunk<Verified, V, BODY_SIZE>,
    ) -> impl Future<Output = Result<(), Self::Error>> + MaybeSend {
        let (chunk, _stamp) = stamped.into_parts();
        self.inner.put(chunk)
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::error::Error as _;
    use std::sync::Arc;
    use std::sync::Mutex;

    use arbitrary::Unstructured;
    use nectar_primitives::{
        Chunk, ChunkHas, ContentChunk, DEFAULT_BODY_SIZE, MemoryStore, Tee, TeeError,
    };
    use nectar_testing::run;

    use super::*;
    use crate::{Batch, Unvalidated, Validated, generators};

    type Store = MemoryStore<AnyChunkSet<DEFAULT_BODY_SIZE>>;

    fn signed(payload: &'static [u8]) -> (Batch, StampedChunk<Verified, Unvalidated>) {
        let chunk: Chunk<Verified, AnyChunkSet<DEFAULT_BODY_SIZE>> = Chunk::from_envelope(
            ContentChunk::new(payload)
                .expect("valid content chunk")
                .into(),
        )
        .expect("locally built chunk certifies");
        let mut u = Unstructured::new(&[7u8; 128]);
        let (batch, stamp) =
            generators::batch_and_stamp(&mut u, chunk.address()).expect("coherent stamp");
        (batch, StampedChunk::new(chunk, stamp))
    }

    fn stamped(payload: &'static [u8]) -> StampedChunk {
        let (batch, pair) = signed(payload);
        pair.validate(&batch).expect("coherent pairing")
    }

    #[derive(Debug, Default)]
    struct RecordingSink {
        seen: Mutex<Vec<ChunkAddress>>,
    }

    impl ChunkPut<StampedChunk> for RecordingSink {
        type Error = Infallible;

        async fn put(&self, stamped: StampedChunk) -> Result<(), Self::Error> {
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

    impl ChunkPut<StampedChunk> for FailSink {
        type Error = LegRefused;

        async fn put(&self, _stamped: StampedChunk) -> Result<(), Self::Error> {
            Err(LegRefused)
        }
    }

    // A sink that speaks both units at once: the collapse gives it two
    // instantiations of one verb rather than two verbs.
    #[derive(Debug, Default)]
    struct DualSink {
        bare: Mutex<Vec<ChunkAddress>>,
        paid: Mutex<Vec<ChunkAddress>>,
    }

    impl ChunkPut<Chunk<Verified, AnyChunkSet<DEFAULT_BODY_SIZE>>> for DualSink {
        type Error = Infallible;

        async fn put(
            &self,
            chunk: Chunk<Verified, AnyChunkSet<DEFAULT_BODY_SIZE>>,
        ) -> Result<(), Self::Error> {
            self.bare.lock().expect("bare lock").push(*chunk.address());
            Ok(())
        }
    }

    impl ChunkPut<StampedChunk> for DualSink {
        type Error = Infallible;

        async fn put(&self, stamped: StampedChunk) -> Result<(), Self::Error> {
            self.paid
                .lock()
                .expect("paid lock")
                .push(*stamped.address());
            Ok(())
        }
    }

    #[test]
    fn stamp_indifferent_persists_the_chunk() {
        run(async {
            let store = Store::new();
            let sink = StampIndifferent::new(&store);
            let pair = stamped(b"local persist");
            let address = *pair.address();

            sink.put(pair).await.expect("infallible put");
            assert!(store.has(&address).await);
        });
    }

    #[test]
    fn stamp_indifferent_accepts_an_unvalidated_pair() {
        run(async {
            let store = Store::new();
            let sink = StampIndifferent::new(&store);
            let (_, pair) = signed(b"unproven stamp");
            let address = *pair.address();

            sink.put(pair).await.expect("infallible put");
            assert!(store.has(&address).await);
        });
    }

    #[test]
    fn tee_carries_the_stamped_unit() {
        run(async {
            let store = Store::new();
            let recorder = RecordingSink::default();
            let tee = Tee::new(StampIndifferent::new(&store), &recorder);
            let pair = stamped(b"both legs");
            let address = *pair.address();

            tee.put(pair).await.expect("both legs accept");
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

            let err = tee.put(pair).await.expect_err("local leg fails");
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

            let err = tee.put(pair).await.expect_err("forward leg fails");
            assert!(matches!(err, TeeError::Forward(LegRefused)));
            assert!(store.has(&address).await);
        });
    }

    async fn demand_paid<S: ChunkPut<StampedChunk<Verified, Validated>>>(
        sink: S,
        pair: StampedChunk,
    ) -> Result<(), S::Error> {
        sink.put(pair).await
    }

    #[test]
    fn blanket_impls_delegate() {
        run(async {
            let recorder = Arc::new(RecordingSink::default());
            let pair = stamped(b"delegation");
            let address = *pair.address();

            demand_paid(Arc::clone(&recorder), pair.clone())
                .await
                .expect("arc delegates");
            demand_paid(&*recorder, pair).await.expect("ref delegates");
            assert_eq!(
                *recorder.seen.lock().expect("recording lock"),
                [address, address]
            );
        });
    }

    #[test]
    fn one_sink_serves_both_units() {
        run(async {
            let sink = DualSink::default();
            let pair = stamped(b"two units");
            let address = *pair.address();
            let (chunk, _) = pair.clone().into_parts();

            sink.put(chunk).await.expect("bare unit accepted");
            demand_paid(&sink, pair).await.expect("paid unit accepted");
            assert_eq!(*sink.bare.lock().expect("bare lock"), [address]);
            assert_eq!(*sink.paid.lock().expect("paid lock"), [address]);
        });
    }
}
