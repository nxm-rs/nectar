//! Fuzz the postage stamp wire decoder and signer recovery.
//!
//! `Stamp::try_from_slice` parses the 113-byte wire encoding (batch id,
//! bucket/index, timestamp, 65-byte ECDSA signature). When the input carries
//! a stamp plus 32 further bytes, those bytes are used as the chunk address
//! and EIP-191 signer recovery runs over the arbitrary stamp fields — the
//! ECDSA recovery must be panic-free. Any returned `Err` is success; the
//! oracle is "no panic, no OOM, no hang".
//!
//! Seeds live in `fuzz/seeds/stamp_decode/` and are replayed on stable by
//! `seed_replay_stamp_decode` in `crates/postage/src/stamp.rs`.

#![no_main]

use libfuzzer_sys::fuzz_target;
use nectar_postage::{STAMP_SIZE, Stamp};
use nectar_primitives::ChunkAddress;

fuzz_target!(|data: &[u8]| {
    // Whole-input decode covers the exact-length check path.
    let _ = Stamp::try_from_slice(data);

    // Structured split: a 113-byte stamp followed by a 32-byte chunk address,
    // so signer recovery runs over arbitrary stamp fields.
    if data.len() >= STAMP_SIZE + 32
        && let Ok(stamp) = Stamp::try_from_slice(&data[..STAMP_SIZE])
        && let Ok(address) = ChunkAddress::from_slice(&data[STAMP_SIZE..STAMP_SIZE + 32])
    {
        let _ = stamp.recover_signer(&address);
    }
});
