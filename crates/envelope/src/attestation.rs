//! Attested scheme sets.
//!
//! A peer pins the schemes it can receive under by signing the whole set
//! with its long-term secp256k1 identity key (EIP-191 personal sign,
//! matching the handshake). Verification is the only mint of a
//! [`Recipient<Hpke>`]; provenance, the pinned-peer registry and fail-closed
//! enforcement live with the node, which must treat a missing token for a
//! pinned peer as an attack, never as licence to fall back to compat.
//!
//! Compat is unrepresentable in [`AttestedScheme`], so no verified set can
//! yield a compat recipient: the reference client publishes no attestations,
//! and anyone who can verify a set can speak HPKE.

use alloc::vec::Vec;
use core::fmt;

use alloy_primitives::{Address, Signature};
use k256::PublicKey;
use k256::elliptic_curve::sec1::ToEncodedPoint;
use thiserror::Error;

use super::{Hpke, Recipient};

/// Domain prefix of the attestation sign-data.
pub const ATTESTATION_PREFIX: &[u8] = b"nectar/env/v2 attest";

/// Byte length of a serialized signature.
const SIGNATURE_SIZE: usize = 65;

/// A scheme that may be advertised in an attested set.
///
/// Compat has no code here by construction, so it cannot be attested.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum AttestedScheme {
    /// RFC 9180 HPKE under the pinned suite.
    Hpke,
}

impl AttestedScheme {
    /// Sender preference, strongest first; crate-owned, not caller-supplied.
    const STRENGTH: &'static [Self] = &[Self::Hpke];

    /// Wire code of this scheme.
    #[must_use]
    pub const fn code(self) -> u16 {
        match self {
            Self::Hpke => 0x0001,
        }
    }

    /// The scheme a wire code names, if this build knows it.
    #[must_use]
    pub const fn from_code(code: u16) -> Option<Self> {
        match code {
            0x0001 => Some(Self::Hpke),
            _ => None,
        }
    }
}

/// Proof that a [`Recipient`] came from a verified attestation; not
/// constructible outside this crate.
#[derive(Clone)]
pub struct Attested {
    scheme: AttestedScheme,
    counter: u64,
}

impl Attested {
    pub(crate) const fn new(scheme: AttestedScheme, counter: u64) -> Self {
        Self { scheme, counter }
    }

    /// The attested scheme.
    #[must_use]
    pub const fn scheme(&self) -> AttestedScheme {
        self.scheme
    }

    /// The counter of the set this recipient came from.
    #[must_use]
    pub const fn counter(&self) -> u64 {
        self.counter
    }
}

impl fmt::Debug for Attested {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Attested")
            .field("scheme", &self.scheme)
            .field("counter", &self.counter)
            .finish()
    }
}

/// One advertised scheme and the key it receives under.
///
/// An unknown code is carried verbatim so the signed bytes survive a
/// round-trip through a build that does not know the scheme.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SchemeEntry {
    code: u16,
    key: Vec<u8>,
}

impl SchemeEntry {
    /// Advertise `key` under `scheme`.
    #[must_use]
    pub fn new(scheme: AttestedScheme, key: &PublicKey) -> Self {
        Self {
            code: scheme.code(),
            key: key.to_encoded_point(true).as_bytes().to_vec(),
        }
    }

    #[cfg(test)]
    pub(crate) const fn raw(code: u16, key: Vec<u8>) -> Self {
        Self { code, key }
    }

    /// The wire code.
    #[must_use]
    pub const fn code(&self) -> u16 {
        self.code
    }

    /// The scheme, if this build knows the code.
    #[must_use]
    pub const fn scheme(&self) -> Option<AttestedScheme> {
        AttestedScheme::from_code(self.code)
    }

    /// The serialized key.
    #[must_use]
    pub fn key_bytes(&self) -> &[u8] {
        &self.key
    }

    fn encode(&self, out: &mut Vec<u8>) {
        let len = u16::try_from(self.key.len()).unwrap_or(u16::MAX);
        out.extend_from_slice(&self.code.to_be_bytes());
        out.extend_from_slice(&len.to_be_bytes());
        out.extend_from_slice(&self.key);
    }
}

/// Errors from decoding or verifying an attestation.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum AttestationError {
    /// The serialized token is malformed.
    #[error("attestation encoding is malformed")]
    Encoding,
    /// The entry list is empty or not strictly ascending.
    #[error("entries are empty or not strictly ascending")]
    Order,
    /// A key for a known scheme is not a valid curve point.
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

/// The signed body: `counter || entry count || entries`.
fn body(counter: u64, entries: &[SchemeEntry]) -> Vec<u8> {
    let count = u16::try_from(entries.len()).unwrap_or(u16::MAX);
    let mut out = Vec::new();
    out.extend_from_slice(&counter.to_be_bytes());
    out.extend_from_slice(&count.to_be_bytes());
    for entry in entries {
        entry.encode(&mut out);
    }
    out
}

/// Build the attestation sign-data: `prefix || counter || sorted entries`.
///
/// Signing is not wrapped: the identity key personal-signs (EIP-191) this
/// buffer, matching the handshake convention.
#[must_use]
pub fn sign_data(counter: u64, entries: &[SchemeEntry]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(ATTESTATION_PREFIX);
    out.extend_from_slice(&body(counter, entries));
    out
}

/// Signed statement of the schemes an identity receives under.
#[derive(Debug, Clone)]
pub struct RecipientAttestation {
    counter: u64,
    entries: Vec<SchemeEntry>,
    signature: Signature,
}

impl RecipientAttestation {
    /// Assemble a token from its parts; [`Self::verify`] does the checking.
    #[must_use]
    pub const fn new(counter: u64, entries: Vec<SchemeEntry>, signature: Signature) -> Self {
        Self {
            counter,
            entries,
            signature,
        }
    }

    /// The monotonic counter.
    #[must_use]
    pub const fn counter(&self) -> u64 {
        self.counter
    }

    /// The advertised entries, as carried.
    #[must_use]
    pub fn entries(&self) -> &[SchemeEntry] {
        &self.entries
    }

    /// The identity signature over [`sign_data`].
    #[must_use]
    pub const fn signature(&self) -> &Signature {
        &self.signature
    }

    /// Verify against the pinned identity.
    pub fn verify(&self, identity: Address) -> Result<VerifiedAttestation, AttestationError> {
        if !strictly_ascending(&self.entries) {
            return Err(AttestationError::Order);
        }
        let recovered = self
            .signature
            .recover_address_from_msg(sign_data(self.counter, &self.entries))
            .map_err(|_| AttestationError::Signature)?;
        if recovered != identity {
            return Err(AttestationError::Signer {
                expected: identity,
                recovered,
            });
        }
        for entry in &self.entries {
            if entry.scheme().is_some() && PublicKey::from_sec1_bytes(&entry.key).is_err() {
                return Err(AttestationError::Key);
            }
        }
        Ok(VerifiedAttestation {
            counter: self.counter,
            entries: self.entries.clone(),
        })
    }

    /// Serialize: `counter || entry count || entries || 65-byte signature`.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = body(self.counter, &self.entries);
        out.extend_from_slice(&self.signature.as_bytes());
        out
    }
}

fn strictly_ascending(entries: &[SchemeEntry]) -> bool {
    !entries.is_empty()
        && entries.windows(2).all(|pair| match pair {
            [left, right] => left < right,
            _ => false,
        })
}

impl TryFrom<&[u8]> for RecipientAttestation {
    type Error = AttestationError;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        let (counter, rest) = bytes
            .split_first_chunk::<8>()
            .ok_or(AttestationError::Encoding)?;
        let (count, mut rest) = rest
            .split_first_chunk::<2>()
            .ok_or(AttestationError::Encoding)?;
        let count = usize::from(u16::from_be_bytes(*count));
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            let (code, tail) = rest
                .split_first_chunk::<2>()
                .ok_or(AttestationError::Encoding)?;
            let (len, tail) = tail
                .split_first_chunk::<2>()
                .ok_or(AttestationError::Encoding)?;
            let (key, tail) = tail
                .split_at_checked(usize::from(u16::from_be_bytes(*len)))
                .ok_or(AttestationError::Encoding)?;
            entries.push(SchemeEntry {
                code: u16::from_be_bytes(*code),
                key: key.to_vec(),
            });
            rest = tail;
        }
        let signature_bytes: &[u8; SIGNATURE_SIZE] =
            rest.try_into().map_err(|_| AttestationError::Encoding)?;
        let signature =
            Signature::from_raw_array(signature_bytes).map_err(|_| AttestationError::Signature)?;
        Ok(Self::new(u64::from_be_bytes(*counter), entries, signature))
    }
}

/// A verified set; the only source of attested recipients.
#[derive(Debug, Clone)]
pub struct VerifiedAttestation {
    counter: u64,
    entries: Vec<SchemeEntry>,
}

impl VerifiedAttestation {
    /// The monotonic counter; the node holds the last accepted value, so
    /// verification alone does not reject a replayed set.
    #[must_use]
    pub const fn counter(&self) -> u64 {
        self.counter
    }

    /// The verified entries.
    #[must_use]
    pub fn entries(&self) -> &[SchemeEntry] {
        &self.entries
    }

    /// Mint a recipient for the strongest scheme the set offers.
    #[must_use]
    pub fn select(&self) -> Option<AttestedRecipient> {
        for scheme in AttestedScheme::STRENGTH {
            let Some(entry) = self.entries.iter().find(|e| e.code == scheme.code()) else {
                continue;
            };
            let key = PublicKey::from_sec1_bytes(&entry.key).ok()?;
            return Some(match *scheme {
                AttestedScheme::Hpke => AttestedRecipient::Hpke(Recipient::attested(
                    key,
                    Attested::new(AttestedScheme::Hpke, self.counter),
                )),
            });
        }
        None
    }
}

/// A recipient minted from a verified set.
///
/// Compat has no variant here, and no conversion exists, so a verified
/// attestation can never produce a `Recipient<Compat>`:
///
/// ```compile_fail
/// use nectar_envelope::{AttestedRecipient, Compat, Recipient};
///
/// fn downgrade(attested: AttestedRecipient) -> Recipient<Compat> {
///     match attested {
///         AttestedRecipient::Compat(recipient) => recipient,
///         _ => unreachable!(),
///     }
/// }
/// ```
#[non_exhaustive]
#[derive(Debug, Clone)]
pub enum AttestedRecipient {
    /// An HPKE recipient.
    Hpke(Recipient<Hpke>),
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_signer::SignerSync;
    use alloy_signer_local::LocalSigner;

    use crate::ecies::generate_secret;

    type Signer = LocalSigner<k256::ecdsa::SigningKey>;

    fn sign(identity: &Signer, counter: u64, entries: Vec<SchemeEntry>) -> RecipientAttestation {
        let signature = identity
            .sign_message_sync(&sign_data(counter, &entries))
            .unwrap();
        RecipientAttestation::new(counter, entries, signature)
    }

    fn attested() -> (Signer, PublicKey, RecipientAttestation) {
        let identity = LocalSigner::random();
        let key = generate_secret().public_key();
        let attestation = sign(
            &identity,
            7,
            alloc::vec![SchemeEntry::new(AttestedScheme::Hpke, &key)],
        );
        (identity, key, attestation)
    }

    /// An entry for a scheme this build does not know, sorting after HPKE.
    fn unknown() -> SchemeEntry {
        SchemeEntry::raw(0xffff, alloc::vec![0xab; 33])
    }

    #[test]
    fn verify_selects_the_hpke_recipient() {
        let (identity, key, attestation) = attested();
        let verified = attestation.verify(identity.address()).unwrap();
        assert_eq!(verified.counter(), 7);
        let AttestedRecipient::Hpke(recipient) = verified.select().unwrap();
        assert_eq!(recipient.key(), &key);
        assert_eq!(recipient.provenance().counter(), 7);
        assert_eq!(recipient.provenance().scheme(), AttestedScheme::Hpke);
    }

    #[test]
    fn wrong_identity_is_rejected() {
        let (identity, _, attestation) = attested();
        let other = LocalSigner::random().address();
        let err = attestation.verify(other).unwrap_err();
        assert!(matches!(
            err,
            AttestationError::Signer { expected, recovered }
                if expected == other && recovered == identity.address()
        ));
    }

    #[test]
    fn dropping_the_strong_scheme_breaks_the_signature() {
        let identity = LocalSigner::random();
        let key = generate_secret().public_key();
        let full = alloc::vec![SchemeEntry::new(AttestedScheme::Hpke, &key), unknown()];
        let attestation = sign(&identity, 1, full);
        assert!(attestation.verify(identity.address()).is_ok());

        let stripped =
            RecipientAttestation::new(1, alloc::vec![unknown()], *attestation.signature());
        assert!(stripped.verify(identity.address()).is_err());
    }

    #[test]
    fn the_counter_is_signed() {
        let (identity, _, attestation) = attested();
        let replayed = RecipientAttestation::new(
            attestation.counter() + 1,
            attestation.entries().to_vec(),
            *attestation.signature(),
        );
        assert!(replayed.verify(identity.address()).is_err());
    }

    #[test]
    fn selection_ignores_unknown_codes() {
        let identity = LocalSigner::random();
        let key = generate_secret().public_key();
        let entries = alloc::vec![SchemeEntry::new(AttestedScheme::Hpke, &key), unknown()];
        let verified = sign(&identity, 0, entries)
            .verify(identity.address())
            .unwrap();
        let AttestedRecipient::Hpke(recipient) = verified.select().unwrap();
        assert_eq!(recipient.key(), &key);
    }

    #[test]
    fn unsorted_or_duplicate_entries_are_rejected() {
        let identity = LocalSigner::random();
        let key = generate_secret().public_key();
        let entry = SchemeEntry::new(AttestedScheme::Hpke, &key);
        for entries in [
            alloc::vec![unknown(), entry.clone()],
            alloc::vec![entry.clone(), entry],
            Vec::new(),
        ] {
            let attestation = sign(&identity, 0, entries);
            assert!(matches!(
                attestation.verify(identity.address()).unwrap_err(),
                AttestationError::Order
            ));
        }
    }

    #[test]
    fn a_bad_key_for_a_known_scheme_is_rejected() {
        let identity = LocalSigner::random();
        let attestation = sign(
            &identity,
            0,
            alloc::vec![SchemeEntry::raw(
                AttestedScheme::Hpke.code(),
                alloc::vec![0u8; 33]
            )],
        );
        assert!(matches!(
            attestation.verify(identity.address()).unwrap_err(),
            AttestationError::Key
        ));
    }

    #[test]
    fn bytes_roundtrip() {
        let (identity, key, attestation) = attested();
        let bytes = attestation.to_bytes();
        let decoded = RecipientAttestation::try_from(bytes.as_slice()).unwrap();
        assert_eq!(decoded.counter(), 7);
        assert_eq!(decoded.entries(), attestation.entries());
        let verified = decoded.verify(identity.address()).unwrap();
        let AttestedRecipient::Hpke(recipient) = verified.select().unwrap();
        assert_eq!(recipient.key(), &key);
    }

    #[test]
    fn truncated_tokens_are_rejected() {
        let (_, _, attestation) = attested();
        let bytes = attestation.to_bytes();
        for len in [0, 9, bytes.len() - 1] {
            assert!(matches!(
                RecipientAttestation::try_from(&bytes[..len]).unwrap_err(),
                AttestationError::Encoding
            ));
        }
    }

    #[test]
    fn sign_data_layout() {
        let key = generate_secret().public_key();
        let entries = alloc::vec![SchemeEntry::new(AttestedScheme::Hpke, &key)];
        let data = sign_data(3, &entries);
        assert_eq!(&data[..ATTESTATION_PREFIX.len()], ATTESTATION_PREFIX);
        assert_eq!(&data[ATTESTATION_PREFIX.len()..][..8], &3u64.to_be_bytes());
        assert_eq!(data.len(), ATTESTATION_PREFIX.len() + 8 + 2 + 4 + 33);
    }
}
