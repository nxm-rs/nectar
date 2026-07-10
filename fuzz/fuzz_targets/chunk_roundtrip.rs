//! Structured round-trip fuzz of the chunk wire codecs.
//!
//! The `Arbitrary` impls for `ContentChunk` (content.rs) and
//! `SingleOwnerChunk` (single_owner.rs) generate only valid chunks — the SOC
//! impl signs the id/body with a fresh key — so the oracle is stronger than
//! "no panic": the wire encoding (`Bytes: From<chunk>`) must decode
//! (`TryFrom<&[u8]>`), and the decoded chunk must reproduce the original's
//! identity (address), payload (span/data), and, for SOCs, the signature and
//! recovered owner. Any failure is a codec bug.
//!
//! The same properties are pinned on stable by the `test_chunk_properties`
//! proptests in `crates/primitives/src/chunk/{content,single_owner}.rs`.

#![no_main]

use libfuzzer_sys::fuzz_target;
use nectar_primitives::{
    BmtChunk, Chunk, ContentChunk, DEFAULT_BODY_SIZE, SingleOwnerChunk, bytes::Bytes,
};

/// One structured input: either chunk kind, so a single corpus drives both
/// codecs (the SOC arm pays an ECDSA sign per exec, the CAC arm stays cheap).
#[derive(Debug, arbitrary::Arbitrary)]
enum ChunkInput {
    Content(ContentChunk<DEFAULT_BODY_SIZE>),
    SingleOwner(SingleOwnerChunk<DEFAULT_BODY_SIZE>),
}

fuzz_target!(|input: ChunkInput| {
    match input {
        ChunkInput::Content(chunk) => {
            let encoded: Bytes = chunk.clone().into();
            let decoded = ContentChunk::<DEFAULT_BODY_SIZE>::try_from(encoded.as_ref())
                .expect("encoded content chunks must decode");
            // ContentChunk equality is BMT-address equality; also pin the
            // payload fields the address is derived from.
            assert_eq!(decoded, chunk, "decoded CAC must equal the original");
            assert_eq!(decoded.span(), chunk.span(), "span must round-trip");
            assert_eq!(decoded.data(), chunk.data(), "data must round-trip");
        }
        ChunkInput::SingleOwner(chunk) => {
            let encoded: Bytes = chunk.clone().into();
            let decoded = SingleOwnerChunk::<DEFAULT_BODY_SIZE>::try_from(encoded.as_ref())
                .expect("encoded single-owner chunks must decode");
            // SOC equality is id + recovered owner; also pin the raw
            // signature, payload, and the derived chunk address.
            assert_eq!(decoded, chunk, "decoded SOC must equal the original");
            assert_eq!(
                decoded.signature(),
                chunk.signature(),
                "signature must round-trip"
            );
            assert_eq!(decoded.data(), chunk.data(), "data must round-trip");
            assert_eq!(
                decoded.address(),
                chunk.address(),
                "address must round-trip"
            );
        }
    }
});
