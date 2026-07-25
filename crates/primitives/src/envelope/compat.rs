//! Thin adapter over the frozen [`crate::ecies`] construction.
//!
//! Wire-frozen: hint, key derivation and ciphertext are byte-for-byte the
//! compat baseline; only the frame around them is shared with the envelope.

use k256::SecretKey;

use crate::ecies::{self, Hint, Salt};

use super::{Compat, OpenError, Opened, Recipient, Record, SealedRecord, hint_matches};

#[cfg(feature = "encryption")]
pub(super) fn seal(
    recipient: &Recipient<Compat>,
    salt: Salt<'_>,
    plaintext: &[u8],
    ct_len: usize,
) -> Result<SealedRecord<Compat>, ecies::EciesError> {
    let encrypted = ecies::encrypt(recipient.key(), salt, plaintext, Some(ct_len))?;
    let hint = *Hint::derive(&encrypted.key, salt).as_bytes();
    let (enc_x, parity) = super::x_and_parity(&encrypted.ephemeral);
    Ok(SealedRecord::from_parts(
        hint,
        parity,
        enc_x,
        encrypted.ciphertext,
        encrypted.key,
    ))
}

pub(super) fn open(
    secret: &SecretKey,
    salt: Salt<'_>,
    record: &Record<'_>,
) -> Result<Option<Opened<Compat>>, OpenError> {
    // Compat ECDH only uses the shared x, so either parity reconstructs the
    // same key; use the carried bit for symmetry with the envelope.
    let Some(ephemeral) = super::reconstruct(record.enc_x(), record.parity()) else {
        return Ok(None);
    };
    let key = ecies::shared_key(secret, &ephemeral, salt);
    if !hint_matches(Hint::derive(&key, salt).as_bytes(), record.hint()) {
        return Ok(None);
    }
    let plaintext = ecies::decrypt(&key, record.ciphertext());
    Ok(Some(Opened {
        plaintext,
        extra: key,
    }))
}
