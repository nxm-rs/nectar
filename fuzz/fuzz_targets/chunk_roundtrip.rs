//! Structured round-trip fuzz of the chunk wire codecs.
//!
//! Inputs come from `nectar_primitives::generators::any_chunk`, so they are
//! valid by construction: CACs need no signature, SOCs are signed by a
//! signer drawn from the same input, so ownership recovery and `verify`
//! succeed. The oracle is therefore stronger than "no panic": the wire
//! encoding (`Bytes: From<chunk>`) must decode (`TryFrom<&[u8]>`), and the
//! decoded chunk must reproduce the original's identity (address), payload
//! (span/data), and, for SOCs, the signature and recovered owner. Any
//! failure is a codec bug.
//!
//! The raw `Arbitrary` impls (unconstrained SOC signature) are deliberately
//! not used here; adversarial inputs belong to the decode targets.
//!
//! The same properties are pinned on stable by the `test_chunk_properties`
//! proptests in `crates/primitives/src/chunk/{content,single_owner}.rs`.

#![no_main]

use arbitrary::Unstructured;
use libfuzzer_sys::fuzz_target;
use nectar_primitives::{
    AnyChunk, ChunkOps, ContentChunk, DEFAULT_BODY_SIZE, SingleOwnerChunk, bytes::Bytes, generators,
};

fuzz_target!(|data: &[u8]| {
    let mut u = Unstructured::new(data);
    let Ok(chunk) = generators::any_chunk::<DEFAULT_BODY_SIZE>(&mut u) else {
        return;
    };
    match chunk {
        AnyChunk::Content(chunk) => {
            let encoded: Bytes = chunk.clone().into();
            let decoded = ContentChunk::<DEFAULT_BODY_SIZE>::try_from(encoded.as_ref())
                .expect("encoded content chunks must decode");
            // ContentChunk equality is BMT-address equality; also pin the
            // payload fields the address is derived from.
            assert_eq!(decoded, chunk, "decoded CAC must equal the original");
            assert_eq!(decoded.span(), chunk.span(), "span must round-trip");
            assert_eq!(decoded.data(), chunk.data(), "data must round-trip");
        }
        AnyChunk::SingleOwner(chunk) => {
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
