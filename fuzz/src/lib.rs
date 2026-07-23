//! Shared fixtures for the file-pipeline fuzz targets.

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
