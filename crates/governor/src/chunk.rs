//! Chunk-typed fetch over the trusted store surface.

use nectar_primitives::chunk::{Chunk, ChunkAddress, ChunkRegistry, Verified};
use nectar_primitives::store::TrustedGet;

/// Fetch one chunk from a trusted store, carrying `payload` back with the
/// outcome: the payload-in-future routing a drained
/// [`FuturesUnordered`](crate::FuturesUnordered) completion relies on.
pub async fn get_verified<S, R, P>(
    store: S,
    address: ChunkAddress,
    payload: P,
) -> (P, Result<Chunk<Verified, R>, S::Error>)
where
    R: ChunkRegistry,
    S: TrustedGet<R>,
{
    let fetched = store.get(&address).await;
    (payload, fetched)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nectar_primitives::bytes::Bytes;
    use nectar_primitives::chunk::{ChunkOps, ContentChunk, ContentOnlyChunkSet};
    use nectar_primitives::store::{ChunkPut, ContentGet, MemoryStore};
    use nectar_primitives::{Chunk, DEFAULT_BODY_SIZE, StandardChunkSet};
    use nectar_testing::run;

    #[test]
    fn payload_rides_the_fetch() {
        run(async {
            let store = MemoryStore::<StandardChunkSet>::new();
            let content =
                ContentChunk::<DEFAULT_BODY_SIZE>::new(Bytes::from_static(b"kernel")).unwrap();
            let address = *content.address();
            let sealed: Chunk = Chunk::from_envelope(content.into()).unwrap();
            store.put(sealed).await.unwrap();

            let (payload, fetched) = get_verified::<_, ContentOnlyChunkSet, _>(
                ContentGet::new(store.clone()),
                address,
                42u32,
            )
            .await;
            assert_eq!(payload, 42);
            assert_eq!(fetched.unwrap().address(), &address);

            let missing = ChunkAddress::from([0xEE; 32]);
            let (payload, fetched) =
                get_verified::<_, ContentOnlyChunkSet, _>(ContentGet::new(store), missing, 7u32)
                    .await;
            assert_eq!(payload, 7);
            assert!(fetched.is_err());
        });
    }
}
