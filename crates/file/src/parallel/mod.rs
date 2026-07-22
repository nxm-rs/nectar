//! Thread-pool ingest over a random-access source.
//!
//! [`split_read_at`] reads leaf bodies in file order and feeds them through
//! a hash-windowed [`Split`], so leaf sealing runs on the rayon pool while
//! the ascent, intermediate sealing and put dispatch stay on the calling
//! task. Retained memory stays at the hash window of leaf bodies plus the
//! spine and the put window.

mod error;
mod source;
#[cfg(test)]
mod tests;

use alloc::vec;
use core::future::poll_fn;

use nectar_primitives::chunk::AnyChunkSet;
use nectar_primitives::store::ChunkPut;

pub use error::ReadAtError;
pub use source::ReadAt;

use crate::config::{HashWindow, PutWindow};
use crate::num::u64_from_usize;
use crate::split::{Split, SplitMode};

/// Split `source` into the tree under `store`, sealing leaves on the rayon
/// pool; the mode `M` picks the reference grammar.
///
/// For deterministic modes the root and chunk set equal the streaming split
/// of the same bytes. The mode is default-constructed once and cloned onto
/// the pool workers; a key-drawing mode draws its keys on the workers in
/// completion order, so its output is round-trip-verified rather than
/// byte-reproducible. Dropping the future abandons in-flight puts; a leaf
/// seal already queued on the pool finishes and is discarded.
///
/// ```
/// use nectar_file::{Plain, PutWindow, split_read_at};
/// use nectar_primitives::chunk::AnyChunkSet;
/// use nectar_primitives::store::MemoryStore;
///
/// # nectar_testing::run(async {
/// let data = vec![7u8; 10_000];
/// let store = MemoryStore::<AnyChunkSet<4096>>::new();
/// let root = split_read_at::<_, _, Plain, 4096>(data, store, PutWindow::DEFAULT)
///     .await
///     .unwrap();
/// assert_eq!(root.as_bytes().len(), 32);
/// # });
/// ```
pub async fn split_read_at<R, S, M, const B: usize>(
    source: R,
    store: S,
    window: PutWindow,
) -> Result<M::Root, ReadAtError<S::Error>>
where
    R: ReadAt,
    S: ChunkPut<AnyChunkSet<B>> + Clone + 'static,
    M: SplitMode + Default + Clone,
    M::Ref: Send,
{
    let size = source
        .len()
        .map_err(|source| ReadAtError::Length { source })?;
    let mut split = Split::<S, M, B>::with_mode(store, M::default(), window)
        .with_hash_window(HashWindow::DEFAULT);
    let mut buf = vec![0u8; B];
    let mut offset = 0u64;
    while offset < size {
        // The remainder is capped by the body size, so the narrowing is
        // lossless, and `take` never exceeds the buffer length.
        let take = usize::try_from(size.saturating_sub(offset).min(u64_from_usize(B))).unwrap_or(B);
        let Some((body, _)) = buf.split_at_mut_checked(take) else {
            break;
        };
        read_full(&source, offset, body)?;
        let mut piece = buf.get(..take).unwrap_or_default();
        while !piece.is_empty() {
            let n = poll_fn(|cx| split.poll_write(cx, piece)).await?;
            piece = piece.get(n..).unwrap_or_default();
        }
        offset = offset.saturating_add(u64_from_usize(take));
    }
    poll_fn(|cx| split.poll_finish(cx))
        .await
        .map_err(ReadAtError::from)
}

/// Fill `buf` from `offset`, looping over short reads; running out of
/// source is an error, never a silent truncation.
fn read_full<R, E>(source: &R, offset: u64, buf: &mut [u8]) -> Result<(), ReadAtError<E>>
where
    R: ReadAt + ?Sized,
{
    let mut filled = 0usize;
    while filled < buf.len() {
        let at = offset.saturating_add(u64_from_usize(filled));
        let Some(rest) = buf.get_mut(filled..) else {
            return Ok(());
        };
        let capacity = rest.len();
        let count = source
            .read_at(at, rest)
            .map_err(|source| ReadAtError::Read { offset: at, source })?;
        if count == 0 {
            return Err(ReadAtError::ShortRead {
                offset: at,
                remaining: capacity,
            });
        }
        if count > capacity {
            return Err(ReadAtError::ReadOverrun {
                offset: at,
                count,
                capacity,
            });
        }
        filled = filled.saturating_add(count);
    }
    Ok(())
}
