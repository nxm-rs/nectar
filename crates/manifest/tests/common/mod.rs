//! Shared helpers for the manifest integration tests.

use std::error::Error;
use std::sync::Arc;

use nectar_file::{Plain, Split};
use nectar_primitives::{ChunkAddress, DEFAULT_BODY_SIZE, MemoryStore};

/// Split `data` whole through the streaming engine into a fresh store,
/// returning the root and the split store.
pub(crate) async fn split_whole(
    data: &[u8],
) -> Result<(ChunkAddress, MemoryStore), Box<dyn Error>> {
    let store = Arc::new(MemoryStore::default());
    let root =
        Split::<Arc<MemoryStore>, Plain, DEFAULT_BODY_SIZE>::collect(Arc::clone(&store), data)
            .await?;
    let store = Arc::into_inner(store).ok_or("split still holds the store")?;
    Ok((root, store))
}
