//! Shared fixtures for the file-pipeline fuzz targets.

use std::sync::Arc;

use nectar_testing::run;
use nectar_file::{Plain, Split};
use nectar_primitives::chunk::{AnyChunkSet, ChunkAddress};
use nectar_primitives::store::MemoryStore;

/// Upper bound on a tiled input length.
pub const MAX_LEN: usize = 32 * 1024;

/// Tile `seed` to `copies` repetitions, capped at [`MAX_LEN`] bytes.
pub fn tile(seed: &[u8], copies: u16) -> Vec<u8> {
    if seed.is_empty() {
        return Vec::new();
    }
    let len = seed
        .len()
        .saturating_mul(usize::from(copies.max(1)))
        .min(MAX_LEN);
    seed.iter().copied().cycle().take(len).collect()
}

/// Split `data` whole into a fresh shared memory store, returning the root
/// and the store.
pub fn split<const B: usize>(data: &[u8]) -> (ChunkAddress, Arc<MemoryStore<AnyChunkSet<B>>>) {
    let store = Arc::new(MemoryStore::new());
    let root = run(Split::<Arc<MemoryStore<AnyChunkSet<B>>>, Plain, B>::collect(Arc::clone(&store), data))
    .expect("split over a memory store succeeds");
    (root, store)
}
