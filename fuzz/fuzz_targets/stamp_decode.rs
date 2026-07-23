//! Fuzz the postage stamp wire decoder and signer recovery.
//!
//! The shared `nectar_postage::oracles::stamp_decode` oracle drives
//! `Stamp::try_from_slice`, which parses the 113-byte wire encoding (batch
//! id, bucket/index, timestamp, 65-byte ECDSA signature). When the input
//! carries a stamp plus 32 further bytes, those bytes are used as the chunk
//! address and EIP-191 signer recovery and owner verification run over the
//! arbitrary stamp fields; the ECDSA recovery must be panic-free. Any
//! returned `Err` is success; the oracle is "no panic, no OOM, no hang".
//!
//! Seeds live in `fuzz/seeds/stamp_decode/` and are replayed on stable by
//! `seed_replay_stamp_decode` in `crates/postage/src/stamp.rs`, through the
//! same oracle.

#![no_main]

use libfuzzer_sys::fuzz_target;
use nectar_postage::oracles;

fuzz_target!(|data: &[u8]| {
    let _ = oracles::stamp_decode(data);
});
