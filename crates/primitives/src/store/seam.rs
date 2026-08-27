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

/// A write offset the target cannot address: it is outside its own index
/// space.
#[derive(Debug, thiserror::Error)]
#[error("the write offset is outside the target")]
struct OffsetOutside;

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

impl WriteAt for Vec<u8> {
    fn write_all_at(&mut self, pos: u64, buf: &[u8]) -> Result<(), BoxedError> {
        let Ok(pos) = usize::try_from(pos) else {
            return Err(OffsetOutside.into());
        };
        if !buf.is_empty() {
            let Some(end) = pos.checked_add(buf.len()) else {
                return Err(OffsetOutside.into());
            };
            if end > self.len() {
                self.resize(end, 0);
            }
        }
        let Some((_, tail)) = self.split_at_mut_checked(pos) else {
            return Err(OffsetOutside.into());
        };
        let Some(dst) = tail.get_mut(..buf.len()) else {
            return Err(OffsetOutside.into());
        };
        dst.copy_from_slice(buf);
        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_vec_lands_frames_at_offsets_with_zero_gaps() {
        let mut bytes: Vec<u8> = Vec::new();
        bytes.write_all_at(8, b"xy").unwrap();
        bytes.write_all_at(2, b"ab").unwrap();
        bytes.write_all_at(0, b"").unwrap();
        assert_eq!(bytes, vec![0, 0, b'a', b'b', 0, 0, 0, 0, b'x', b'y']);
        assert_eq!(bytes.read_at(1, &mut [0u8; 4]).unwrap(), 4);
        assert_eq!(bytes.read_at(8, &mut [0u8; 4]).unwrap(), 2);
        assert_eq!(bytes.read_at(10, &mut [0u8; 4]).unwrap(), 0);
        assert_eq!(bytes.size(), Some(10));
    }

    #[test]
    fn a_vec_refuses_an_offset_outside_its_index_space() {
        let mut bytes: Vec<u8> = Vec::new();
        assert!(bytes.write_all_at(u64::MAX, b"z").is_err());
        assert!(bytes.is_empty());
    }
}
