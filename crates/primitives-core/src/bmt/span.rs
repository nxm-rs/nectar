//! Level decoding for wire spans the reference client packs with a
//! redundancy level in byte 7.

/// The level and payload length carried by a wire span.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpanLevel {
    /// Decoded redundancy level; 0 for an unencoded span.
    pub level: u8,
    /// The span with the level byte contribution removed.
    pub length: u64,
}

const BYTE7_MASK: u64 = 0xFF << 56;

/// Byte 7's flag bit of a wire span: the smallest span whose most
/// significant byte enters the level region.
pub const BYTE7_FLAG: u64 = 0x80 << 56;
const LENGTH_MASK: u64 = 0x00FF_FFFF_FFFF_FFFF;

/// Decodes the level and payload length of a wire span.
///
/// Mirrors the reference client's `DecodeSpan`
/// (`SPEC.md#span-level-decoding`). Byte 7 strictly above [`BYTE7_FLAG`]
/// carries the level; exactly the flag is not encoded.
pub const fn decode_span(span: u64) -> SpanLevel {
    if (span & BYTE7_MASK) > BYTE7_FLAG {
        SpanLevel {
            level: level_of_byte7(span),
            length: span & LENGTH_MASK,
        }
    } else {
        SpanLevel {
            level: 0,
            length: span,
        }
    }
}

// The level holds the low seven bits of byte 7, which fit a `u8`.
#[allow(clippy::as_conversions)]
const fn level_of_byte7(span: u64) -> u8 {
    ((span >> 56) & 0x7f) as u8
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_byte7_of_exactly_0x80_is_not_encoded() {
        let span = 0x80u64 << 56 | 5;
        assert_eq!(
            decode_span(span),
            SpanLevel {
                level: 0,
                length: span
            }
        );
    }

    #[test]
    fn a_byte7_of_0x81_decodes_to_level_1_with_byte7_cleared() {
        let span = 0x81u64 << 56 | 0x1234;
        assert_eq!(
            decode_span(span),
            SpanLevel {
                level: 1,
                length: 0x1234
            }
        );
    }

    #[test]
    fn an_unencoded_span_passes_through_whole() {
        let span = 4096;
        assert_eq!(
            decode_span(span),
            SpanLevel {
                level: 0,
                length: span
            }
        );
    }

    #[test]
    fn the_full_level_range_decodes() {
        for level in 1..=0x7f {
            let span = (0x80u64 | level as u64) << 56 | 1;
            let decoded = decode_span(span);
            assert_eq!(decoded.level, level);
            assert_eq!(decoded.length, 1);
        }
    }

    #[test]
    fn the_flag_bit_is_the_2_63_boundary() {
        assert_eq!(BYTE7_FLAG, 1u64 << 63);
    }
}
