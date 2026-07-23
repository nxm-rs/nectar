//! Shared fuzz and test oracles for the chunk wire codecs.
//!
//! One oracle per invariant: the fuzz target and the stable pins call the
//! same body, so the rungs cannot drift. Oracles return `Err` instead of
//! panicking; call sites assert.

use bytes::Bytes;
use thiserror::Error;

use crate::chunk::error::ChunkError;
use crate::{AnyChunk, ChunkAddress, ChunkOps, ContentChunk, SingleOwnerChunk};

/// A violated oracle invariant, named by the failing check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("{0}")]
pub struct Violation(&'static str);

impl Violation {
    /// Name the violated invariant.
    #[must_use]
    pub const fn new(check: &'static str) -> Self {
        Self(check)
    }
}

/// Drive every chunk decode entry point over one input and force the lazy
/// address and owner computations. `Err` is an acceptable outcome for
/// arbitrary bytes; the invariant is that no path panics. Returns the wire
/// decode keyed by the address of whichever direct parse succeeded.
pub fn chunk_decode<const BODY_SIZE: usize>(data: &[u8]) -> crate::Result<AnyChunk<BODY_SIZE>> {
    let bytes = Bytes::copy_from_slice(data);

    // Address-mismatch arm: the zero address matches (almost) no input, so
    // both trial parses and their address computations run to `Err`.
    let _ = AnyChunk::<BODY_SIZE>::from_wire_bytes(&ChunkAddress::default(), bytes.clone());

    let content = ContentChunk::<BODY_SIZE>::try_from(data);
    let soc = SingleOwnerChunk::<BODY_SIZE>::try_from(data);
    if let Ok(soc) = &soc {
        // ECDSA public-key recovery over bytes 32..97 must not panic.
        let _ = soc.owner();
        let _ = soc.address();
    }

    // Ok arm: key the wire decoder by the address of whichever direct parse
    // succeeded, CAC first (the same trial order the decoder uses).
    let address = content
        .ok()
        .map(|c| *c.address())
        .or_else(|| soc.ok().map(|s| *s.address()))
        .ok_or_else(|| ChunkError::invalid_format("no structural parse"))?;
    let result = AnyChunk::from_wire_bytes(&address, bytes);
    if let Ok(chunk) = &result {
        let _ = chunk.address();
    }
    result
}

/// Wire round trip of a valid content chunk: the encoding must decode to a
/// chunk with the same identity (address), span and payload.
pub fn content_chunk_round_trip<const BODY_SIZE: usize>(
    chunk: &ContentChunk<BODY_SIZE>,
) -> Result<(), Violation> {
    let encoded: Bytes = chunk.clone().into();
    let Ok(decoded) = ContentChunk::<BODY_SIZE>::try_from(encoded.as_ref()) else {
        return Err(Violation::new("encoded content chunks must decode"));
    };
    if decoded != *chunk {
        return Err(Violation::new("decoded CAC must equal the original"));
    }
    if decoded.span() != chunk.span() {
        return Err(Violation::new("span must round-trip"));
    }
    if decoded.data() != chunk.data() {
        return Err(Violation::new("data must round-trip"));
    }
    Ok(())
}

/// Wire round trip of a valid single-owner chunk: the encoding must decode
/// to a chunk with the same identity (id plus recovered owner), signature,
/// payload and derived address.
pub fn single_owner_chunk_round_trip<const BODY_SIZE: usize>(
    chunk: &SingleOwnerChunk<BODY_SIZE>,
) -> Result<(), Violation> {
    let encoded: Bytes = chunk.clone().into();
    let Ok(decoded) = SingleOwnerChunk::<BODY_SIZE>::try_from(encoded.as_ref()) else {
        return Err(Violation::new("encoded single-owner chunks must decode"));
    };
    if decoded != *chunk {
        return Err(Violation::new("decoded SOC must equal the original"));
    }
    if decoded.signature() != chunk.signature() {
        return Err(Violation::new("signature must round-trip"));
    }
    if decoded.data() != chunk.data() {
        return Err(Violation::new("data must round-trip"));
    }
    if decoded.address() != chunk.address() {
        return Err(Violation::new("address must round-trip"));
    }
    Ok(())
}

/// Dispatch a valid chunk of either kind to its wire round-trip oracle.
pub fn any_chunk_round_trip<const BODY_SIZE: usize>(
    chunk: &AnyChunk<BODY_SIZE>,
) -> Result<(), Violation> {
    match chunk {
        AnyChunk::Content(chunk) => content_chunk_round_trip(chunk),
        AnyChunk::SingleOwner(chunk) => single_owner_chunk_round_trip(chunk),
    }
}
