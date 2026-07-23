//! Shared fuzz and test oracles for the stamp wire codec.
//!
//! One oracle per invariant: the fuzz target and the stable pins call the
//! same body, so the rungs cannot drift. Oracles return `Err` instead of
//! panicking; call sites assert.

use alloy_primitives::Address;
use nectar_primitives::ChunkAddress;
use nectar_primitives::oracles::Violation;

use crate::{STAMP_SIZE, Stamp, StampError};

/// Drive the stamp decode surface over one input: decode the whole slice,
/// decode the leading 113 bytes when present, and run EIP-191 signer
/// recovery and owner verification over whatever parsed. `Err` is an
/// acceptable outcome for arbitrary bytes; the invariant is that no path
/// panics. Returns the primary decode: the first `STAMP_SIZE` bytes when the
/// input is long enough, the whole slice otherwise.
pub fn stamp_decode(data: &[u8]) -> Result<Stamp, StampError> {
    let _ = Stamp::try_from_slice(data);

    let primary = Stamp::try_from_slice(data.get(..STAMP_SIZE).unwrap_or(data));
    if let Ok(stamp) = &primary {
        // Trailing bytes, when present, act as the chunk address the stamp
        // is recovered against; ECDSA recovery over arbitrary stamp fields
        // must not panic. The zero-owner verify keeps the mismatch arm
        // exercised too.
        let address = data
            .get(STAMP_SIZE..)
            .and_then(|tail| tail.get(..32))
            .and_then(|tail| ChunkAddress::from_slice(tail).ok())
            .unwrap_or_else(ChunkAddress::zero);
        let _ = stamp.recover_signer(&address);
        let _ = stamp.verify(&address, Address::ZERO);
    }
    primary
}

/// Wire round trip of one stamp: the 113-byte encoding must decode to an
/// equal stamp, and re-encoding must be byte-identical (canonical form).
pub fn stamp_round_trip(stamp: &Stamp) -> Result<(), Violation> {
    let encoded = stamp.to_bytes();
    let Ok(decoded) = Stamp::from_bytes(&encoded) else {
        return Err(Violation::new("encoded stamps must decode"));
    };
    if decoded != *stamp {
        return Err(Violation::new(
            "decode(encode(stamp)) must reproduce the stamp",
        ));
    }
    if decoded.to_bytes() != encoded {
        return Err(Violation::new("encoding must be canonical"));
    }
    Ok(())
}
