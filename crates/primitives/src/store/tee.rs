//! Fan-out over the put seam.

use super::typed::{ChunkPut, PutUnit};

/// Fans one put out to two sinks: a local leg, then a forward leg.
///
/// Legs run sequentially and fail fast: a local failure means the unit never
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

impl<U, L, F> ChunkPut<U> for Tee<L, F>
where
    U: PutUnit + Clone,
    L: ChunkPut<U>,
    F: ChunkPut<U>,
{
    type Error = TeeError<L::Error, F::Error>;

    async fn put(&self, unit: U) -> Result<(), Self::Error> {
        self.local
            .put(unit.clone())
            .await
            .map_err(TeeError::Local)?;
        self.forward.put(unit).await.map_err(TeeError::Forward)
    }
}

/// A [`Tee`] put failure, tagged with the leg that refused it.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TeeError<L, F> {
    /// The local leg failed; the unit never reached the forward leg.
    #[error("local leg refused the put")]
    Local(#[source] L),
    /// The forward leg failed; the local write stands.
    #[error("forward leg refused the put")]
    Forward(#[source] F),
}

#[cfg(test)]
mod tests {
    use alloc::sync::Arc;
    use alloc::vec::Vec;
    use core::convert::Infallible;

    use nectar_testing::run;

    use super::super::{ChunkHas, MemoryStore};
    use super::*;
    use crate::chunk::{Chunk, ChunkAddress, ContentChunk, StandardChunkSet, Verified};

    type Sealed = Chunk<Verified, StandardChunkSet>;

    fn sealed(payload: &'static [u8]) -> Sealed {
        Chunk::from_envelope(
            ContentChunk::new(payload)
                .expect("valid content chunk")
                .into(),
        )
        .expect("locally built chunk certifies")
    }

    #[derive(Debug, Default)]
    struct RecordingSink {
        seen: parking_lot::Mutex<Vec<ChunkAddress>>,
    }

    impl ChunkPut for RecordingSink {
        type Error = Infallible;

        async fn put(&self, unit: Sealed) -> Result<(), Self::Error> {
            self.seen.lock().push(*PutUnit::address(&unit));
            Ok(())
        }
    }

    #[derive(Debug, thiserror::Error)]
    #[error("leg refused")]
    struct LegRefused;

    #[derive(Debug, Default)]
    struct FailSink;

    impl ChunkPut for FailSink {
        type Error = LegRefused;

        async fn put(&self, _unit: Sealed) -> Result<(), Self::Error> {
            Err(LegRefused)
        }
    }

    #[test]
    fn delivers_to_both_legs() {
        run(async {
            let store = MemoryStore::<StandardChunkSet>::new();
            let recorder = RecordingSink::default();
            let tee = Tee::new(&store, &recorder);
            let chunk = sealed(b"both legs");
            let address = *chunk.address();

            tee.put(chunk).await.expect("both legs accept");
            assert!(store.has(&address).await);
            assert_eq!(*recorder.seen.lock(), [address]);
        });
    }

    #[test]
    fn local_failure_short_circuits() {
        run(async {
            let recorder = RecordingSink::default();
            let tee = Tee::new(FailSink, &recorder);

            let err = tee
                .put(sealed(b"local refusal"))
                .await
                .expect_err("local leg fails");
            assert!(matches!(err, TeeError::Local(LegRefused)));
            assert!(recorder.seen.lock().is_empty());
        });
    }

    #[test]
    fn forward_failure_keeps_local_write() {
        run(async {
            let store = MemoryStore::<StandardChunkSet>::new();
            let tee = Tee::new(&store, FailSink);
            let chunk = sealed(b"forward refusal");
            let address = *chunk.address();

            let err = tee.put(chunk).await.expect_err("forward leg fails");
            assert!(matches!(err, TeeError::Forward(LegRefused)));
            assert!(store.has(&address).await);
        });
    }

    #[test]
    fn blanket_impls_delegate() {
        run(async {
            let recorder = Arc::new(RecordingSink::default());
            let chunk = sealed(b"delegation");
            let address = *chunk.address();

            let tee = Tee::new(Arc::clone(&recorder), &*recorder);
            tee.put(chunk).await.expect("both legs accept");
            assert_eq!(*recorder.seen.lock(), [address, address]);
        });
    }
}
