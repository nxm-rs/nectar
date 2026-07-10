//! Fuzz the SBU1 usage-snapshot root decoder with raw bytes.
//!
//! `RootInfo::parse` recovers the bit-packed snapshot root: a 66-byte header
//! (magic, batch id, geometry, flags, delta width, sequence numbers) followed
//! by exception pairs, slot indices, and either leaf digests or an inline
//! packed counter bitstream. The `capacity = 1 << (depth - bucket_depth)`
//! geometry and the packed-length arithmetic are the interesting attack
//! surface. Any returned `Err` is success; the oracle is "no panic, no OOM,
//! no hang".
//!
//! Seeds live in `fuzz/seeds/usage_snapshot_decode/` and are replayed on
//! stable by `seed_replay_usage_snapshot_decode` in
//! `crates/postage-usage/src/codec.rs`.

#![no_main]

use libfuzzer_sys::fuzz_target;
use nectar_postage_usage::RootInfo;

fuzz_target!(|data: &[u8]| {
    let _ = RootInfo::parse(data);
});
