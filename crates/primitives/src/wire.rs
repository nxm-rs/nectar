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
    /// Read failure for this type. [`Underrun`] converts into it, so impls can
    /// add their own validation errors on top of short reads.
    type Error: From<Underrun>;

    /// Reads `Self` from the cursor, advancing it past the consumed bytes.
    /// On failure the cursor keeps only the fields already taken; a failed
    /// single-field read leaves it untouched.
    fn take_from(cur: &mut Cursor<'_>) -> Result<Self, Self::Error>;
}

impl FromCursor for u8 {
    type Error = Underrun;

    fn take_from(cur: &mut Cursor<'_>) -> Result<Self, Underrun> {
        cur.take::<[Self; 1]>().map(|[byte]| byte)
    }
}

impl<const N: usize> FromCursor for [u8; N] {
    type Error = Underrun;

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
    pub fn take<T: FromCursor>(&mut self) -> Result<T, T::Error> {
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

/// A writer appending fixed- and variable-width fields to a byte buffer.
///
/// The dual of [`Cursor`]: every method appends and cannot fail, so an encoder
/// built only from these primitives cannot emit a misaligned wire image. The
/// borrowed buffer is the sole state; a reader over the finished bytes recovers
/// each field in the order it was put.
///
/// ```
/// use nectar_primitives::wire::{Cursor, Writer};
///
/// let mut buf = Vec::new();
/// let mut w = Writer::new(&mut buf);
/// w.put_u16_be(0x20);
/// w.put_array(&[0xaa, 0xbb]);
///
/// let mut cur = Cursor::new(&buf);
/// assert_eq!(cur.take_u16_be()?, 0x20);
/// assert_eq!(cur.take_array::<2>()?, &[0xaa, 0xbb]);
/// # Ok::<(), nectar_primitives::wire::Underrun>(())
/// ```
#[derive(Debug)]
pub struct Writer<'a> {
    bytes: &'a mut Vec<u8>,
}

impl<'a> Writer<'a> {
    /// Wraps a growable buffer for appending. Existing contents are kept, so a
    /// writer can extend a partially built image.
    pub const fn new(bytes: &'a mut Vec<u8>) -> Self {
        Self { bytes }
    }

    /// Appends a fixed-size array.
    pub fn put_array<const N: usize>(&mut self, arr: &[u8; N]) {
        self.bytes.extend_from_slice(arr);
    }

    /// Appends a byte slice.
    pub fn put_slice(&mut self, bytes: &[u8]) {
        self.bytes.extend_from_slice(bytes);
    }

    /// Appends a single byte.
    pub fn put_u8(&mut self, byte: u8) {
        self.bytes.push(byte);
    }

    /// Appends a big-endian `u16`.
    pub fn put_u16_be(&mut self, value: u16) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    /// Appends `n` zero bytes, e.g. to pad a field to its declared width.
    pub fn put_zeros(&mut self, n: usize) {
        self.bytes.resize(self.bytes.len().saturating_add(n), 0);
    }

    /// Bytes written so far, including any pre-existing contents.
    pub const fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Whether the buffer is empty.
    pub const fn is_empty(&self) -> bool {
        self.bytes.is_empty()
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
            type Error = Underrun;

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
    fn take_surfaces_impl_validation_errors() {
        #[derive(Debug, PartialEq)]
        enum TagError {
            Short,
            Odd,
        }

        impl From<Underrun> for TagError {
            fn from(_: Underrun) -> Self {
                Self::Short
            }
        }

        #[derive(Debug)]
        struct EvenTag(u8);

        impl FromCursor for EvenTag {
            type Error = TagError;

            fn take_from(cur: &mut Cursor<'_>) -> Result<Self, TagError> {
                let byte = cur.take::<u8>()?;
                if byte % 2 == 0 {
                    Ok(Self(byte))
                } else {
                    Err(TagError::Odd)
                }
            }
        }

        let mut cur = Cursor::new(&[4u8, 5]);
        assert_eq!(cur.take::<EvenTag>().unwrap().0, 4);
        assert_eq!(cur.take::<EvenTag>().unwrap_err(), TagError::Odd);
        assert_eq!(cur.take::<EvenTag>().unwrap_err(), TagError::Short);
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

    #[test]
    fn writer_appends_each_width() {
        let mut buf = Vec::new();
        let mut w = Writer::new(&mut buf);
        assert!(w.is_empty());

        w.put_u8(0xab);
        w.put_u16_be(0x1234);
        w.put_array(&[0xaa, 0xbb]);
        w.put_slice(&[0xcc, 0xdd]);
        w.put_zeros(3);

        assert_eq!(w.len(), 10);
        assert_eq!(buf, [0xab, 0x12, 0x34, 0xaa, 0xbb, 0xcc, 0xdd, 0, 0, 0]);
    }

    #[test]
    fn writer_extends_existing_contents() {
        let mut buf = vec![0x01, 0x02];
        let mut w = Writer::new(&mut buf);
        w.put_u8(0x03);
        assert_eq!(buf, [0x01, 0x02, 0x03]);
    }

    #[test]
    fn writer_and_cursor_round_trip() {
        // Each field a reader takes matches what the writer put, in order.
        let mut buf = Vec::new();
        let mut w = Writer::new(&mut buf);
        w.put_u8(0x7f);
        w.put_u16_be(0xbeef);
        w.put_array(&[1u8, 2, 3, 4]);
        w.put_slice(&[9u8, 8]);

        let mut cur = Cursor::new(&buf);
        assert_eq!(cur.take_u8().unwrap(), 0x7f);
        assert_eq!(cur.take_u16_be().unwrap(), 0xbeef);
        assert_eq!(cur.take_array::<4>().unwrap(), &[1, 2, 3, 4]);
        assert_eq!(cur.take(2).unwrap(), &[9, 8]);
        assert!(cur.is_empty());
    }
}
