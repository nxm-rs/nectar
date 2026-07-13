//! Order-statistic subtree key-count carried by the node grammar.
//!
//! The count is a pure function of the key set: `subtree_count(f) =
//! (f.entry?1:0) + child_count(f)`, so canonical bytes and dedup hold
//! unchanged. It rides the wire as a canonical (minimal-length) uleb128; the
//! decoder rejects an overlong encoding and caps the read length, so the only
//! fallible byte access is the cursor.

use nectar_primitives::wire::{Cursor, FromCursor, ToWriter, Underrun, Writer};

/// The greatest number of uleb128 bytes a `u64` count occupies: `ceil(64 / 7)`.
/// The decoder reads at most this many bytes before rejecting, so a hostile
/// image cannot drive an unbounded read.
pub(crate) const MAX_WIRE_BYTES: usize = 10;

/// A subtree's distinct key-count: the order-statistic annotation a
/// referenced-child fork or segment descriptor carries.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SubtreeCount(u64);

impl SubtreeCount {
    /// The count as a `u64`.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// The count from a `u64`.
    #[must_use]
    pub const fn new(count: u64) -> Self {
        Self(count)
    }

    /// The canonical uleb128 byte length of this count.
    pub(crate) const fn wire_len(self) -> usize {
        let mut value = self.0;
        let mut len: usize = 1;
        while value >= 0x80 {
            value >>= 7;
            len = len.saturating_add(1);
        }
        len
    }
}

/// A rejection reading a subtree count: a short read, an overlong (non-minimal)
/// encoding, or a run wider than a `u64`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum CountError {
    /// The buffer ended inside the count.
    #[error(transparent)]
    Underrun(#[from] Underrun),
    /// A non-minimal encoding: a multi-byte run ending in a zero payload byte.
    #[error("non-canonical subtree count encoding")]
    Overlong,
    /// The count did not terminate within a `u64`'s worth of bytes.
    #[error("subtree count exceeds the wire bound")]
    TooWide,
}

impl FromCursor for SubtreeCount {
    type Error = CountError;

    /// Reads a canonical uleb128 count, rejecting an overlong or over-wide run.
    fn take_from(cur: &mut Cursor<'_>) -> Result<Self, CountError> {
        let mut value: u64 = 0;
        for index in 0..MAX_WIRE_BYTES {
            let byte = cur.take::<u8>()?;
            let low = u64::from(byte & 0x7F);
            let shift = u32::try_from(index.saturating_mul(7)).map_err(|_| CountError::TooWide)?;
            // A payload that shifts past the u64, or whose bits do not survive
            // the round-trip, is over-wide: the high byte carries only the bits
            // that fit.
            let shifted = low.checked_shl(shift).ok_or(CountError::TooWide)?;
            if shifted.checked_shr(shift) != Some(low) {
                return Err(CountError::TooWide);
            }
            value |= shifted;
            if byte & 0x80 == 0 {
                // Minimal-length: a multi-byte run never ends in a zero payload.
                if index > 0 && byte == 0 {
                    return Err(CountError::Overlong);
                }
                return Ok(Self(value));
            }
        }
        Err(CountError::TooWide)
    }
}

impl ToWriter for SubtreeCount {
    /// Emits the canonical minimal-length uleb128 encoding.
    fn put_into(&self, w: &mut Writer<'_>) {
        let mut value = self.0;
        loop {
            let byte = u8::try_from(value & 0x7F).unwrap_or(0);
            value >>= 7;
            if value == 0 {
                w.put(&byte);
                return;
            }
            w.put(&(byte | 0x80));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode(value: u64) -> Vec<u8> {
        let mut buf = Vec::new();
        Writer::new(&mut buf).put(&SubtreeCount::new(value));
        buf
    }

    fn decode(bytes: &[u8]) -> Result<SubtreeCount, CountError> {
        let mut cur = Cursor::new(bytes);
        cur.take::<SubtreeCount>()
    }

    #[test]
    fn canonical_encodings_round_trip() {
        for value in [
            0u64,
            1,
            0x7F,
            0x80,
            0x3FFF,
            0x4000,
            u64::from(u32::MAX),
            u64::MAX,
        ] {
            let bytes = encode(value);
            assert_eq!(bytes.len(), SubtreeCount::new(value).wire_len());
            assert_eq!(decode(&bytes).unwrap().get(), value);
        }
    }

    #[test]
    fn low_values_are_single_byte() {
        assert_eq!(encode(0), [0x00]);
        assert_eq!(encode(1), [0x01]);
        assert_eq!(encode(0x7F), [0x7F]);
        assert_eq!(encode(0x80), [0x80, 0x01]);
    }

    #[test]
    fn overlong_encodings_reject() {
        // Zero in two bytes, and one in two bytes: both end in a zero payload.
        assert_eq!(decode(&[0x80, 0x00]), Err(CountError::Overlong));
        assert_eq!(decode(&[0x81, 0x00]), Err(CountError::Overlong));
        assert_eq!(decode(&[0xFF, 0x00]), Err(CountError::Overlong));
    }

    #[test]
    fn over_wide_runs_reject() {
        // Eleven continuation bytes never terminate within a u64.
        assert_eq!(decode(&[0x80; 11]), Err(CountError::TooWide));
        // A tenth byte carrying more than the top bit overflows the u64.
        assert_eq!(
            decode(&[0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x02]),
            Err(CountError::TooWide)
        );
    }

    #[test]
    fn a_truncated_run_underruns() {
        assert!(matches!(decode(&[0x80]), Err(CountError::Underrun(_))));
        assert!(matches!(decode(&[]), Err(CountError::Underrun(_))));
    }

    #[test]
    fn u64_max_is_ten_bytes() {
        assert_eq!(SubtreeCount::new(u64::MAX).wire_len(), MAX_WIRE_BYTES);
        assert_eq!(encode(u64::MAX).len(), MAX_WIRE_BYTES);
    }
}
