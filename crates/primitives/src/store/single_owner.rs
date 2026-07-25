//! Single-owner narrowing store adapter.

use crate::chunk::{AnyChunkSet, Chunk, ChunkAddress, SingleOwnerOnlyChunkSet, Verified};

use super::typed::ChunkGet;

/// Single-owner view over a store typed at [`AnyChunkSet`].
///
/// Gets narrow through the Verified-preserving narrowing, so no acceptance
/// rule re-runs; a chunk of any other type is a typed error, not a panic.
#[derive(Debug, Clone, Copy, Default)]
pub struct SingleOwnerGet<T>(T);

impl<T> SingleOwnerGet<T> {
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
pub enum SingleOwnerGetError<E> {
    /// The inner store failed.
    #[error(transparent)]
    Inner(E),
    /// The chunk at the address is not a single-owner chunk.
    #[error("chunk at {0} is not a single-owner chunk")]
    NotSingleOwner(ChunkAddress),
}

impl<const BODY_SIZE: usize, T> ChunkGet<SingleOwnerOnlyChunkSet<BODY_SIZE>> for SingleOwnerGet<T>
where
    T: ChunkGet<AnyChunkSet<BODY_SIZE>, Trust = Verified>,
{
    type Trust = Verified;
    type Error = SingleOwnerGetError<T::Error>;

    async fn get(
        &self,
        address: &ChunkAddress,
    ) -> Result<Chunk<Verified, SingleOwnerOnlyChunkSet<BODY_SIZE>>, Self::Error> {
        let chunk = self
            .0
            .get(address)
            .await
            .map_err(SingleOwnerGetError::Inner)?;
        chunk
            .narrow_single_owner()
            .ok_or(SingleOwnerGetError::NotSingleOwner(*address))
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
    fn get_narrows_single_owner_and_rejects_others() {
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

        let narrow = SingleOwnerGet::new(store);
        let got = run(ChunkGet::get(&narrow, &soc_addr)).unwrap();
        assert_eq!(got.address(), &soc_addr);
        assert_eq!(got.owner(), Some(signer.address()));

        assert!(matches!(
            run(ChunkGet::get(&narrow, &cac_addr)),
            Err(SingleOwnerGetError::NotSingleOwner(a)) if a == cac_addr
        ));
        assert!(matches!(
            run(ChunkGet::get(&narrow, &ChunkAddress::default())),
            Err(SingleOwnerGetError::Inner(ChunkStoreError::NotFound(_)))
        ));
    }
}
