//! Content-only narrowing store adapter.

use crate::chunk::{AnyChunkSet, Chunk, ChunkAddress, ContentOnlyChunkSet, Verified};

use super::typed::{ChunkGet, ChunkHas, ChunkPut, PutUnit};

/// Content-only view over a store typed at [`AnyChunkSet`].
///
/// Gets narrow through the Verified-preserving narrowing, so no acceptance
/// rule re-runs; a chunk of any other type is a typed error, not a panic.
#[derive(Debug, Clone, Copy, Default)]
pub struct ContentGet<T>(T);

impl<T> ContentGet<T> {
    /// Wrap a store typed at [`AnyChunkSet`].
    pub const fn new(inner: T) -> Self {
        Self(inner)
    }

    /// Borrow the inner store.
    pub const fn inner(&self) -> &T {
        &self.0
    }

    /// Consume into the inner store.
    pub fn into_inner(self) -> T {
        self.0
    }
}

/// Error of the narrowing get.
#[derive(Debug, thiserror::Error)]
pub enum ContentGetError<E> {
    /// The inner store failed.
    #[error(transparent)]
    Inner(E),
    /// The chunk at the address is not a content chunk.
    #[error("chunk at {0} is not a content chunk")]
    NotContent(ChunkAddress),
}

impl<const BODY_SIZE: usize, T> ChunkGet<ContentOnlyChunkSet<BODY_SIZE>> for ContentGet<T>
where
    T: ChunkGet<AnyChunkSet<BODY_SIZE>, Trust = Verified>,
{
    type Trust = Verified;
    type Error = ContentGetError<T::Error>;

    async fn get(
        &self,
        address: &ChunkAddress,
    ) -> Result<Chunk<Verified, ContentOnlyChunkSet<BODY_SIZE>>, Self::Error> {
        let chunk = self.0.get(address).await.map_err(ContentGetError::Inner)?;
        chunk
            .narrow_content()
            .ok_or(ContentGetError::NotContent(*address))
    }
}

// Writes pass through untouched: only the read face narrows, so one wrapped
// store serves a read-narrowed, write-wide surface.
impl<U: PutUnit, T: ChunkPut<U>> ChunkPut<U> for ContentGet<T> {
    type Error = T::Error;

    async fn put(&self, unit: U) -> Result<(), Self::Error> {
        self.0.put(unit).await
    }
}

impl<T: ChunkHas> ChunkHas for ContentGet<T> {
    async fn has(&self, address: &ChunkAddress) -> bool {
        self.0.has(address).await
    }
}

#[cfg(test)]
mod tests {
    use super::super::typed::ChunkPut;
    use super::super::{ChunkStoreError, MemoryStore};
    use super::*;
    use crate::chunk::{ChunkOps, ContentChunk, SingleOwnerChunk, SocId, StandardChunkSet};
    use nectar_testing::run;

    #[test]
    fn get_narrows_content_and_rejects_others() {
        let signer = alloy_signer_local::PrivateKeySigner::from_slice(&[0x42u8; 32]).unwrap();
        let soc = SingleOwnerChunk::new(SocId::ZERO, b"stored soc".to_vec(), &signer).unwrap();
        let soc_addr = *soc.address();
        let content = ContentChunk::new(&b"stored cac"[..]).unwrap();
        let cac_addr = *content.address();

        let store = MemoryStore::<StandardChunkSet>::new();
        run(ChunkPut::put(
            &store,
            Chunk::from_envelope(soc.into()).unwrap(),
        ))
        .unwrap();
        run(ChunkPut::put(
            &store,
            Chunk::from_envelope(content.into()).unwrap(),
        ))
        .unwrap();

        let narrow = ContentGet::new(store);
        let got = run(ChunkGet::get(&narrow, &cac_addr)).unwrap();
        assert_eq!(got.address(), &cac_addr);

        assert!(matches!(
            run(ChunkGet::get(&narrow, &soc_addr)),
            Err(ContentGetError::NotContent(a)) if a == soc_addr
        ));
        assert!(matches!(
            run(ChunkGet::get(&narrow, &ChunkAddress::default())),
            Err(ContentGetError::Inner(ChunkStoreError::NotFound(_)))
        ));
    }
}
