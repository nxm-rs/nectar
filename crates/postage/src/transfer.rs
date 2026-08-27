//! The stamped chunk as a unit of transfer, and the bridge beneath it.
//!
//! Not named `sink`: that name is reserved for the `futures` types.
//!
//! A put target that demands payment proof names [`Validated`]; handing it
//! an unproven pair is then a type error:
//!
//! ```compile_fail
//! use nectar_postage::{StampedChunk, Unvalidated, Validated};
//! use nectar_primitives::{ChunkPut, Verified};
//!
//! async fn pushsync<S: ChunkPut<StampedChunk<Verified, Validated>>>(
//!     target: S,
//!     pair: StampedChunk<Verified, Unvalidated>,
//! ) -> Result<(), S::Error> {
//!     target.put(pair).await
//! }
//! ```

use core::future::Future;

use nectar_primitives::marker::MaybeSend;
use nectar_primitives::{AnyChunkSet, Chunk, ChunkPut, Verified};

use crate::{StampedChunk, ValidationState};

/// Admits a stamp-indifferent store as a stamped put target.
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
    use std::sync::Arc;
    use std::sync::Mutex;

    use arbitrary::Unstructured;
    use nectar_primitives::{
        Chunk, ChunkAddress, ContentChunk, DEFAULT_BODY_SIZE, MemoryStore, PutUnit, Tee,
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
    struct RecordingPut {
        seen: Mutex<Vec<ChunkAddress>>,
    }

    impl ChunkPut<StampedChunk> for RecordingPut {
        type Error = Infallible;

        async fn put(&self, stamped: StampedChunk) -> Result<(), Self::Error> {
            self.seen
                .lock()
                .expect("recording lock")
                .push(*stamped.address());
            Ok(())
        }
    }

    #[derive(Debug, Default)]
    struct DualPut {
        bare: Mutex<Vec<ChunkAddress>>,
        paid: Mutex<Vec<ChunkAddress>>,
    }

    impl ChunkPut<Chunk<Verified, AnyChunkSet<DEFAULT_BODY_SIZE>>> for DualPut {
        type Error = Infallible;

        async fn put(
            &self,
            chunk: Chunk<Verified, AnyChunkSet<DEFAULT_BODY_SIZE>>,
        ) -> Result<(), Self::Error> {
            self.bare.lock().expect("bare lock").push(*chunk.address());
            Ok(())
        }
    }

    impl ChunkPut<StampedChunk> for DualPut {
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
            let target = StampIndifferent::new(&store);
            let pair = stamped(b"local persist");
            let address = *pair.address();

            target.put(pair).await.expect("infallible put");
            assert!(store.get(&address).is_some());
        });
    }

    #[test]
    fn stamp_indifferent_accepts_an_unvalidated_pair() {
        run(async {
            let store = Store::new();
            let target = StampIndifferent::new(&store);
            let (_, pair) = signed(b"unproven stamp");
            let address = *pair.address();

            target.put(pair).await.expect("infallible put");
            assert!(store.get(&address).is_some());
        });
    }

    #[test]
    fn tee_carries_the_stamped_unit() {
        run(async {
            let store = Store::new();
            let recorder = RecordingPut::default();
            let tee = Tee::new(StampIndifferent::new(&store), &recorder);
            let pair = stamped(b"both legs");
            let address = *pair.address();

            tee.put(pair).await.expect("both legs accept");
            assert!(store.get(&address).is_some());
            assert_eq!(*recorder.seen.lock().expect("recording lock"), [address]);
        });
    }

    #[test]
    fn put_unit_address_is_the_chunk_address() {
        let pair = stamped(b"unit address");
        assert_eq!(PutUnit::address(&pair), pair.address());
    }

    async fn demand_paid<S: ChunkPut<StampedChunk<Verified, Validated>>>(
        target: S,
        pair: StampedChunk,
    ) -> Result<(), S::Error> {
        target.put(pair).await
    }

    #[test]
    fn blanket_impls_delegate() {
        run(async {
            let recorder = Arc::new(RecordingPut::default());
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
    fn one_put_serves_both_units() {
        run(async {
            let target = DualPut::default();
            let pair = stamped(b"two units");
            let address = *pair.address();
            let (chunk, _) = pair.clone().into_parts();

            target.put(chunk).await.expect("bare unit accepted");
            demand_paid(&target, pair)
                .await
                .expect("paid unit accepted");
            assert_eq!(*target.bare.lock().expect("bare lock"), [address]);
            assert_eq!(*target.paid.lock().expect("paid lock"), [address]);
        });
    }
}
