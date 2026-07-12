//! Fallible byte reader for wire decoding.
//!
//! [`Cursor`] is the single site at which a short read can occur: every
//! fixed-size field is read as a whole via [`FromCursor`], so downstream
//! indexing and length checks disappear. It is a thin wrapper over the std
//! slice splitters (`split_first_chunk` and `split_at_checked`), and so never
//! panics on underrun.
//!
//! ```
//! use nectar_primitives::wire::Cursor;
//!
//! let data = [0x20, 0xaa, 0xbb];
//! let mut cur = Cursor::new(&data);
//! let tag = cur.take::<u8>()?;
//! let field = cur.take::<[u8; 2]>()?;
//! assert_eq!(tag, 0x20);
//! assert_eq!(field, [0xaa, 0xbb]);
//! assert!(cur.finish().is_empty());
//! # Ok::<(), nectar_primitives::wire::Underrun>(())
//! ```

use thiserror::Error;

/// A short read: the buffer held fewer bytes than a field required.
///
/// The lengths are public so a decoder can surface `expected`/`available` in
/// its own error without recomputing them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("buffer underrun: need {expected} bytes, have {available}")]
pub struct Underrun {
    /// Bytes the field required.
    pub expected: usize,
    /// Bytes remaining in the buffer.
    pub available: usize,
}

/// A value that reads exactly its own wire bytes from a [`Cursor`].
///
/// Implementors state their byte order internally; endian-ambiguous bare
/// integers are not exposed. Downstream crates implement this for their own
/// domain types to read them via [`Cursor::take`].
pub trait FromCursor: Sized {
    /// Reads `Self` from the cursor, advancing it past the consumed bytes.
    /// On [`Underrun`] the cursor is left untouched.
    fn take_from(cur: &mut Cursor<'_>) -> Result<Self, Underrun>;
}

impl FromCursor for u8 {
    fn take_from(cur: &mut Cursor<'_>) -> Result<Self, Underrun> {
        cur.take::<[Self; 1]>().map(|[byte]| byte)
    }
}

impl<const N: usize> FromCursor for [u8; N] {
    fn take_from(cur: &mut Cursor<'_>) -> Result<Self, Underrun> {
        match cur.bytes.split_first_chunk::<N>() {
            Some((head, tail)) => {
                cur.bytes = tail;
                Ok(*head)
            }
            None => Err(Underrun {
                expected: N,
                available: cur.bytes.len(),
            }),
        }
    }
}

/// A cursor advancing over a byte slice, yielding fixed- and variable-width
/// fields or an [`Underrun`].
///
/// The unread tail is the sole state: each successful read advances it, and a
/// failed read leaves it untouched.
#[derive(Debug, Clone)]
pub struct Cursor<'a> {
    bytes: &'a [u8],
}

impl<'a> Cursor<'a> {
    /// Wraps a byte slice for reading.
    pub const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    /// Reads the next `N` bytes as a fixed-size array, advancing the cursor.
    pub const fn take_array<const N: usize>(&mut self) -> Result<&'a [u8; N], Underrun> {
        match self.bytes.split_first_chunk::<N>() {
            Some((head, tail)) => {
                self.bytes = tail;
                Ok(head)
            }
            None => Err(Underrun {
                expected: N,
                available: self.bytes.len(),
            }),
        }
    }

    /// Reads the next value as a whole via its [`FromCursor`] impl, advancing
    /// the cursor.
    pub fn take<T: FromCursor>(&mut self) -> Result<T, Underrun> {
        T::take_from(self)
    }

    /// Reads the next `n` bytes as a slice, advancing the cursor.
    pub const fn take_slice(&mut self, n: usize) -> Result<&'a [u8], Underrun> {
        match self.bytes.split_at_checked(n) {
            Some((head, tail)) => {
                self.bytes = tail;
                Ok(head)
            }
            None => Err(Underrun {
                expected: n,
                available: self.bytes.len(),
            }),
        }
    }

    /// Reads a single byte, advancing the cursor.
    pub fn take_u8(&mut self) -> Result<u8, Underrun> {
        let &[byte] = self.take_array::<1>()?;
        Ok(byte)
    }

    /// Reads a big-endian `u16`, advancing the cursor.
    pub fn take_u16_be(&mut self) -> Result<u16, Underrun> {
        self.take_array::<2>().map(|b| u16::from_be_bytes(*b))
    }

    /// The unread tail, without advancing.
    pub const fn remaining(&self) -> &'a [u8] {
        self.bytes
    }

    /// Whether the buffer is fully consumed.
    pub const fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// Consumes the cursor, returning the unread tail.
    pub const fn finish(self) -> &'a [u8] {
        self.bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn take_array_advances_and_underruns() {
        let data = [1u8, 2, 3, 4, 5];
        let mut cur = Cursor::new(&data);

        assert_eq!(cur.take::<[u8; 2]>().unwrap(), [1, 2]);
        assert_eq!(cur.remaining(), &[3, 4, 5]);

        let err = cur.take::<[u8; 4]>().unwrap_err();
        assert_eq!(
            err,
            Underrun {
                expected: 4,
                available: 3,
            }
        );
        // A failed read leaves the tail untouched.
        assert_eq!(cur.remaining(), &[3, 4, 5]);
    }

    #[test]
    fn take_variable_width() {
        let data = [10u8, 20, 30];
        let mut cur = Cursor::new(&data);

        assert_eq!(cur.take_slice(2).unwrap(), &[10, 20]);
        assert_eq!(cur.take_slice(2).unwrap_err().available, 1);
        assert_eq!(cur.take_slice(1).unwrap(), &[30]);
        assert!(cur.is_empty());
    }

    #[test]
    fn scalar_reads() {
        let data = [0xabu8, 0x12];
        let mut cur = Cursor::new(&data);

        assert_eq!(cur.take::<u8>().unwrap(), 0xab);
        assert_eq!(cur.take::<u8>().unwrap(), 0x12);
        assert_eq!(cur.take::<u8>().unwrap_err().expected, 1);
    }

    #[test]
    fn domain_type_reads_through_take() {
        struct BeLen(u16);

        impl FromCursor for BeLen {
            fn take_from(cur: &mut Cursor<'_>) -> Result<Self, Underrun> {
                cur.take::<[u8; 2]>().map(|b| Self(u16::from_be_bytes(b)))
            }
        }

        let data = [0x12u8, 0x34];
        let mut cur = Cursor::new(&data);
        assert_eq!(cur.take::<BeLen>().unwrap().0, 0x1234);
        assert!(cur.is_empty());
    }

    #[test]
    fn finish_returns_tail() {
        let data = [1u8, 2, 3];
        let mut cur = Cursor::new(&data);
        let _ = cur.take::<u8>().unwrap();
        assert_eq!(cur.finish(), &[2, 3]);
    }

    #[test]
    fn empty_buffer() {
        let mut cur = Cursor::new(&[]);
        assert!(cur.is_empty());
        assert_eq!(cur.take::<u8>().unwrap_err().available, 0);
        assert_eq!(cur.take::<[u8; 0]>().unwrap(), [0u8; 0]);
    }
}
