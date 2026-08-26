//! The positional byte seam.
//!
//! [`ReadAt`] is the shared handle a load pulls from; [`WriteAt`] is the
//! exclusive handle a load writes into. Both are positional: the caller
//! names the offset and the target keeps no cursor. Gap bytes below the
//! highest written end of a target read as zero. The markers ride as
//! supertraits, so an object borrows bare.

use alloc::vec::Vec;

use crate::marker::{MaybeSend, MaybeSync};
use nectar_primitives_core::error::BoxedError;

/// Exclusive positional write; frames land at their offsets in completion
/// order, so the caller, not the target, owns the layout.
pub trait WriteAt: MaybeSend {
    /// Land the whole buffer at the offset; partial landing is not a result.
    fn write_all_at(&mut self, pos: u64, buf: &[u8]) -> Result<(), BoxedError>;
}

/// Shared positional read.
pub trait ReadAt: MaybeSync {
    /// Deliver the bytes at the offset into the buffer, up to the buffer
    /// length; zero is the end of the target.
    fn read_at(&self, pos: u64, buf: &mut [u8]) -> Result<usize, BoxedError>;

    /// The declared length in bytes, or `None` when the target does not
    /// know it.
    fn size(&self) -> Option<u64>;
}

impl ReadAt for [u8] {
    fn read_at(&self, pos: u64, buf: &mut [u8]) -> Result<usize, BoxedError> {
        let Ok(pos) = usize::try_from(pos) else {
            return Ok(0);
        };
        let Some(tail) = self.get(pos..) else {
            return Ok(0);
        };
        let take = tail.len().min(buf.len());
        let Some((dst, _)) = buf.split_at_mut_checked(take) else {
            return Ok(0);
        };
        let Some(head) = tail.get(..take) else {
            return Ok(0);
        };
        dst.copy_from_slice(head);
        Ok(take)
    }

    fn size(&self) -> Option<u64> {
        u64::try_from(self.len()).ok()
    }
}

impl ReadAt for Vec<u8> {
    fn read_at(&self, pos: u64, buf: &mut [u8]) -> Result<usize, BoxedError> {
        self.as_slice().read_at(pos, buf)
    }

    fn size(&self) -> Option<u64> {
        self.as_slice().size()
    }
}

#[cfg(all(feature = "std", unix))]
impl ReadAt for std::fs::File {
    fn read_at(&self, pos: u64, buf: &mut [u8]) -> Result<usize, BoxedError> {
        std::os::unix::fs::FileExt::read_at(self, buf, pos).map_err(Into::into)
    }

    fn size(&self) -> Option<u64> {
        self.metadata().ok().map(|meta| meta.len())
    }
}
