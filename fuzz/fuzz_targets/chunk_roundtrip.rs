//! Structured round-trip fuzz of the chunk wire codecs.
//!
//! Inputs come from `nectar_primitives::generators::any_chunk`, so they are
//! valid by construction: CACs need no signature, SOCs are signed by a
//! signer drawn from the same input, so ownership recovery and `verify`
//! succeed. The oracle is therefore stronger than "no panic": the shared
//! `nectar_primitives::oracles::any_chunk_round_trip` oracle requires the
//! wire encoding (`Bytes: From<chunk>`) to decode (`TryFrom<&[u8]>`) to a
//! chunk reproducing the original's identity (address), payload (span/data)
//! and, for SOCs, the signature and recovered owner. Any failure is a codec
//! bug.
//!
//! The raw `Arbitrary` impls (unconstrained SOC signature) are deliberately
//! not used here; adversarial inputs belong to the decode targets.
//!
//! The same oracle is pinned on stable by the `test_chunk_properties` and
//! `test_signed_chunk_verifies` proptests in
//! `crates/primitives/src/chunk/{content,single_owner}.rs`.

#![no_main]

use arbitrary::Unstructured;
use libfuzzer_sys::fuzz_target;
use nectar_primitives::{DEFAULT_BODY_SIZE, generators, oracles};

fuzz_target!(|data: &[u8]| {
    let mut u = Unstructured::new(data);
    let Ok(chunk) = generators::any_chunk::<DEFAULT_BODY_SIZE>(&mut u) else {
        return;
    };
    oracles::any_chunk_round_trip(&chunk).expect("valid chunks must round-trip the wire codec");
});
