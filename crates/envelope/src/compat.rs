//! Thin adapter over the frozen [`crate::ecies`] construction.
//!
//! Wire-frozen: hint, key derivation and ciphertext are byte-for-byte the
//! compat baseline; only the frame around them is shared with HPKE.

use k256::SecretKey;
use thiserror::Error;

use crate::ecdh::ENC_X_SIZE;
use crate::ecies::{self, EciesError, Hint, Salt, SharedX};

use super::{Compat, Envelope, OpenError, Opened, ecdh, hint_matches};
#[cfg(any(test, feature = "encryption"))]
use super::{Recipient, SealedEnvelope};

/// Errors from a compat seal.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum CompatSealError {
    /// The tail cannot hold the encapsulation.
    #[error("tail too small: {tail_len} bytes, minimum {ENC_X_SIZE}")]
    TailTooSmall {
        /// Requested tail length.
        tail_len: usize,
    },
    /// The ciphertext region cannot hold the plaintext.
    #[error(transparent)]
    Ecies(#[from] EciesError),
}

#[cfg(any(test, feature = "encryption"))]
pub(super) fn seal(
    recipient: &Recipient<Compat>,
    salt: Salt<'_>,
    plaintext: &[u8],
    tail_len: usize,
) -> Result<SealedEnvelope<Compat>, CompatSealError> {
    let ct_len = tail_len
        .checked_sub(ENC_X_SIZE)
        .ok_or(CompatSealError::TailTooSmall { tail_len })?;
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
