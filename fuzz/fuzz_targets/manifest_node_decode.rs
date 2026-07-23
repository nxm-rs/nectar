//! Fuzz the mantaray 1.0 manifest node decoder with raw attacker-controlled
//! bytes.
//!
//! `Node::decode` is the entry point through which untrusted manifest chunks
//! from the network are parsed. The shared
//! `nectar_manifest::oracles::node_decode_canonical` oracle holds the
//! reject-or-accept contract on every input: no panic, no OOM, no hang, and
//! an accepted image re-encodes byte-exactly and decodes back to the same
//! node. The decode -> encode bijection proven here on arbitrary input
//! complements the structured round-trip target.
//!
//! Seeds live in `fuzz/seeds/manifest_node_decode/` and are replayed on
//! stable by `seed_replay_manifest_node_decode` in
//! `crates/manifest/src/codec.rs`, through the same oracle.

#![no_main]

use libfuzzer_sys::fuzz_target;
use nectar_manifest::oracles;

fuzz_target!(|data: &[u8]| {
    let _ = oracles::node_decode_canonical(data).expect("an accepted image must be canonical");
});
