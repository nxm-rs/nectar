//! Thin adapter over the frozen [`crate::ecies`] construction.
//!
//! Wire-frozen: hint, key derivation and ciphertext are byte-for-byte the
//! compat baseline; only the frame around them is shared with HPKE.

use k256::SecretKey;

use crate::ecies::{self, Hint, Salt, SharedX};

use super::{Compat, Envelope, OpenError, Opened, ecdh, hint_matches};
#[cfg(any(test, feature = "encryption"))]
use super::{Recipient, SealedEnvelope};

#[cfg(any(test, feature = "encryption"))]
pub(super) fn seal(
    recipient: &Recipient<Compat>,
    salt: Salt<'_>,
    plaintext: &[u8],
    ct_len: usize,
) -> Result<SealedEnvelope<Compat>, ecies::EciesError> {
    let encrypted = ecies::encrypt(recipient.key(), salt, plaintext, Some(ct_len))?;
    let hint = *Hint::derive(&encrypted.key, salt).as_bytes();
    let (tail, nonce_bits) = ecdh::tail(&encrypted.ephemeral, &encrypted.ciphertext);
    Ok(SealedEnvelope::from_parts(
        hint,
        nonce_bits,
        tail,
        encrypted.key,
    ))
}

/// The ECDH is salt-independent, so one run serves every candidate topic.
pub(super) fn decap(
    secret: &SecretKey,
    envelope: &Envelope<'_>,
) -> Result<Option<SharedX>, OpenError> {
    // Compat ECDH only uses the shared x, so either parity reconstructs the
    // same key; use the carried bit for symmetry with HPKE.
    let Some((ephemeral, _)) = ecdh::split(envelope) else {
        return Ok(None);
    };
    #[cfg(test)]
    crate::decaps::note(super::SchemeId::Compat);
    Ok(Some(ecies::shared_x(secret, &ephemeral)))
}

pub(super) fn probe(
    shared: &SharedX,
    salt: Salt<'_>,
    envelope: &Envelope<'_>,
) -> Result<Option<Opened<Compat>>, OpenError> {
    let key = shared.derive(salt);
    if !hint_matches(Hint::derive(&key, salt).as_bytes(), envelope.hint()) {
        return Ok(None);
    }
    let plaintext = ecies::decrypt(&key, ecdh::ciphertext(envelope));
    Ok(Some(Opened {
        plaintext,
        extra: key,
    }))
}
