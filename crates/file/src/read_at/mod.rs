//! Random-access byte sources and short-read-safe filling.
//!
//! [`ReadAt`] is the batch ingest's input seam: a source addressed by offset,
//! read into fixed leaf bodies without holding the whole source resident.

mod error;
mod source;

use crate::num::u64_from_usize;

pub use error::ReadAtError;
pub use source::ReadAt;

/// Fill `buf` from `offset`, looping over short reads; running out of
/// source is an error, never a silent truncation.
pub(crate) fn read_full<R, E>(source: &R, offset: u64, buf: &mut [u8]) -> Result<(), ReadAtError<E>>
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
