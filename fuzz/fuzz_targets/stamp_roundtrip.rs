//! Structured round-trip fuzz of the postage stamp wire codec.
//!
//! The `Arbitrary` impl for `Stamp` (crates/postage/src/stamp.rs) builds a
//! stamp from arbitrary batch/index/timestamp fields and an arbitrary (r, s,
//! v) signature, so the oracle is stronger than "no panic": the shared
//! `nectar_postage::oracles::stamp_round_trip` oracle requires the 113-byte
//! encoding to decode to an equal stamp and to re-encode byte-identically
//! (canonical form). Any failure is a codec bug.
//!
//! The same oracle is pinned on stable by the
//! `stamp_encode_decode_round_trip` proptest in
//! `crates/postage/src/stamp.rs`.

#![no_main]

use libfuzzer_sys::fuzz_target;
use nectar_postage::{Stamp, oracles};

fuzz_target!(|stamp: Stamp| {
    oracles::stamp_round_trip(&stamp).expect("the wire codec must round-trip arbitrary stamps");
});
