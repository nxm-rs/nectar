//! Structured round-trip fuzz of the postage stamp wire codec.
//!
//! The `Arbitrary` impl for `Stamp` (crates/postage/src/stamp.rs) builds a
//! stamp from arbitrary batch/index/timestamp fields and an arbitrary (r, s,
//! v) signature, so the oracle is stronger than "no panic": the 113-byte
//! encoding (`to_bytes`) must decode (`from_bytes`), the decoded stamp must
//! equal the original, and re-encoding must be byte-identical (canonical
//! form). Any failure is a codec bug.
//!
//! The same property is pinned on stable by
//! `arbitrary_stamp_encode_decode_round_trip` in `crates/postage/src/stamp.rs`
//! (run with `--features arbitrary`).

#![no_main]

use libfuzzer_sys::fuzz_target;
use nectar_postage::Stamp;

fuzz_target!(|stamp: Stamp| {
    let encoded = stamp.to_bytes();
    let decoded = Stamp::from_bytes(&encoded).expect("encoded stamps must decode");
    assert_eq!(
        decoded, stamp,
        "decode(encode(stamp)) must reproduce the stamp"
    );

    // Canonical form: re-encoding the decoded stamp must be byte-identical.
    assert_eq!(decoded.to_bytes(), encoded, "encoding must be canonical");
});
