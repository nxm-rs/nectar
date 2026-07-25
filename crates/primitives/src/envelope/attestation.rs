//! Envelope-capability attestation.
//!
//! A peer pins its envelope capability by signing its encryption key with
//! its long-term secp256k1 identity key (EIP-191 personal sign, matching
//! the handshake). Verification is the only mint of [`Recipient<Hpke>`];
//! provenance, the pinned-peer registry and fail-closed enforcement live
//! with the node, which must treat a missing token for a pinned peer as an
//! attack, never as licence to fall back to compat.

use alloy_primitives::{Address, Signature};
use k256::PublicKey;
use k256::elliptic_curve::sec1::ToEncodedPoint;
use thiserror::Error;

use crate::error::WrongLength;

use super::{Hpke, Recipient};

/// Domain prefix of the attestation sign-data.
pub const ATTESTATION_PREFIX: &[u8] = b"nectar/env/v1 attest";

/// Byte length of a serialized attestation: compressed key plus signature.
const SIZE: usize = 33 + 65;

/// Build the attestation sign-data: `prefix || compressed SEC1 key`.
///
/// Signing is not wrapped: the identity key personal-signs (EIP-191) this
/// buffer, matching the handshake convention.
#[must_use]
pub fn sign_data(key: &PublicKey) -> Vec<u8> {
    let point = key.to_encoded_point(true);
    let mut out = Vec::new();
    out.extend_from_slice(ATTESTATION_PREFIX);
    out.extend_from_slice(point.as_bytes());
    out
}

/// Errors from decoding or verifying an attestation.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum AttestationError {
    /// The serialized token has the wrong length.
    #[error(transparent)]
    Length(#[from] WrongLength),
    /// The attested key is not a valid curve point.
    #[error("attested key is not a valid curve point")]
    Key,
    /// The signature is malformed or unrecoverable.
    #[error("signature is malformed or unrecoverable")]
    Signature,
    /// The recovered signer is not the pinned identity.
    #[error("recovered signer {recovered} does not match identity {expected}")]
    Signer {
        /// The pinned identity.
        expected: Address,
        /// The address the signature recovered to.
        recovered: Address,
    },
}

/// Signed statement that an identity receives under the envelope key.
#[derive(Debug, Clone)]
pub struct EnvelopeAttestation {
    key: PublicKey,
    signature: Signature,
}

impl EnvelopeAttestation {
    /// Assemble a token from its parts; [`Self::verify`] does the checking.
    #[must_use]
    pub const fn new(key: PublicKey, signature: Signature) -> Self {
        Self { key, signature }
    }

    /// The attested envelope key.
    #[must_use]
    pub const fn key(&self) -> &PublicKey {
        &self.key
    }

    /// The identity signature over [`sign_data`].
    #[must_use]
    pub const fn signature(&self) -> &Signature {
        &self.signature
    }

    /// Verify against the pinned identity and mint the sealing handle.
    pub fn verify(&self, identity: Address) -> Result<Recipient<Hpke>, AttestationError> {
        let recovered = self
            .signature
            .recover_address_from_msg(sign_data(&self.key))
            .map_err(|_| AttestationError::Signature)?;
        if recovered != identity {
            return Err(AttestationError::Signer {
                expected: identity,
                recovered,
            });
        }
        Ok(Recipient::attested(self.key))
    }

    /// Serialize: `compressed key || 65-byte signature`.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(self.key.to_encoded_point(true).as_bytes());
        out.extend_from_slice(&self.signature.as_bytes());
        out
    }
}

impl TryFrom<&[u8]> for EnvelopeAttestation {
    type Error = AttestationError;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        let wrong_length = WrongLength {
            expected: SIZE,
            got: bytes.len(),
        };
        let Some((key_bytes, rest)) = bytes.split_first_chunk::<33>() else {
            return Err(wrong_length.into());
        };
        let signature_bytes: &[u8; 65] = rest.try_into().map_err(|_| wrong_length)?;
        let key = PublicKey::from_sec1_bytes(key_bytes).map_err(|_| AttestationError::Key)?;
        let signature =
            Signature::from_raw_array(signature_bytes).map_err(|_| AttestationError::Signature)?;
        Ok(Self::new(key, signature))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_signer::SignerSync;
    use alloy_signer_local::LocalSigner;

    use crate::ecies::generate_secret;

    fn attested() -> (LocalSigner<k256::ecdsa::SigningKey>, EnvelopeAttestation) {
        let identity = LocalSigner::random();
        let envelope_key = generate_secret().public_key();
        let signature = identity
            .sign_message_sync(&sign_data(&envelope_key))
            .unwrap();
        let attestation = EnvelopeAttestation::new(envelope_key, signature);
        (identity, attestation)
    }

    #[test]
    fn verify_mints_the_recipient() {
        let (identity, attestation) = attested();
        let recipient = attestation.verify(identity.address()).unwrap();
        assert_eq!(recipient.key(), attestation.key());
    }

    #[test]
    fn wrong_identity_is_rejected() {
        let (identity, attestation) = attested();
        let other = LocalSigner::random().address();
        let err = attestation.verify(other).unwrap_err();
        assert!(matches!(
            err,
            AttestationError::Signer { expected, recovered }
                if expected == other && recovered == identity.address()
        ));
    }

    #[test]
    fn swapped_key_breaks_the_signature() {
        let (identity, attestation) = attested();
        let forged =
            EnvelopeAttestation::new(generate_secret().public_key(), *attestation.signature());
        assert!(forged.verify(identity.address()).is_err());
    }

    #[test]
    fn bytes_roundtrip() {
        let (identity, attestation) = attested();
        let bytes = attestation.to_bytes();
        assert_eq!(bytes.len(), SIZE);
        let decoded = EnvelopeAttestation::try_from(bytes.as_slice()).unwrap();
        assert_eq!(decoded.key(), attestation.key());
        let recipient = decoded.verify(identity.address()).unwrap();
        assert_eq!(recipient.key(), attestation.key());
    }

    #[test]
    fn short_token_is_rejected() {
        let err = EnvelopeAttestation::try_from([0u8; 10].as_slice()).unwrap_err();
        assert!(matches!(err, AttestationError::Length(_)));
    }

    #[test]
    fn sign_data_layout() {
        let key = generate_secret().public_key();
        let data = sign_data(&key);
        assert_eq!(&data[..ATTESTATION_PREFIX.len()], ATTESTATION_PREFIX);
        assert_eq!(data.len(), ATTESTATION_PREFIX.len() + 33);
    }
}
