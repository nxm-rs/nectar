//! Shared pure-computation helpers for sync and async file operations.

use crate::bmt::SPAN_SIZE;

/// Result of validating a read range against file parameters.
pub(crate) enum ReadRangeCheck {
    /// Offset is past end or length is zero — return empty.
    Empty,
    /// File fits in a single chunk; `actual_len` bytes from `offset`.
    SingleChunk { offset: u64, actual_len: usize },
    /// Multi-chunk read; `actual_len` bytes from `offset`.
    MultiChunk { offset: u64, actual_len: usize },
}

/// Validate a read range against span and body size, returning the action to take.
#[inline]
pub(crate) fn validate_read_range<const BODY_SIZE: usize>(
    offset: u64,
    len: usize,
    span: u64,
) -> ReadRangeCheck {
    if offset >= span {
        return ReadRangeCheck::Empty;
    }

    let actual_len = len.min((span - offset) as usize);
    if actual_len == 0 {
        return ReadRangeCheck::Empty;
    }

    if span <= BODY_SIZE as u64 {
        return ReadRangeCheck::SingleChunk { offset, actual_len };
    }

    ReadRangeCheck::MultiChunk { offset, actual_len }
}

/// Build an intermediate chunk payload: span (LE u64) prepended to reference data.
#[inline]
pub(crate) fn build_intermediate_payload(span: u64, ref_data: &[u8]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(SPAN_SIZE + ref_data.len());
    payload.extend_from_slice(&span.to_le_bytes());
    payload.extend_from_slice(ref_data);
    payload
}
