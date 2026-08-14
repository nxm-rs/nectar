//! The HPKE suite: RFC 9180 base mode over the draft secp256k1 KEM.
//!
//! The KEM is composed directly from k256 and HKDF-SHA256 per
//! draft-wahby-cfrg-hpke-kem-secp256k1 (codepoint 0x0016): x-only ECDH is
//! rejected for zero output, `enc` enters `kem_context` as uncompressed
//! SEC1, and every secret intermediate is zeroized on drop. Auth and psk
//! modes arrive with their consumers.

use alloc::vec::Vec;
use chacha20poly1305::aead::AeadInPlace;
use chacha20poly1305::{ChaCha20Poly1305, KeyInit, Nonce, Tag};
use hkdf::Hkdf;
use k256::elliptic_curve::sec1::ToEncodedPoint;
use k256::{PublicKey, SecretKey};
use sha2::Sha256;
use subtle::ConstantTimeEq;
use thiserror::Error;
use zeroize::Zeroizing;

use super::{Envelope, Hpke, LABEL, OpenError, Opened, Topic, hint_matches};
#[cfg(any(test, feature = "encryption"))]
use super::{Recipient, SealedEnvelope};

/// `"KEM"` plus the registered codepoint 0x0016.
const KEM_SUITE_ID: &[u8] = b"KEM\x00\x16";
/// `"HPKE"` plus kem 0x0016, kdf 0x0001, aead 0x0003.
const HPKE_SUITE_ID: &[u8] = b"HPKE\x00\x16\x00\x01\x00\x03";
/// mode_base.
const MODE_BASE: u8 = 0x00;
/// Exporter context of the HPKE hint.
const HINT_CONTEXT: &[u8] = b"nectar/env/v1 hint";

/// Poly1305 tag length.
const TAG_SIZE: usize = 16;
/// Big-endian length prefix inside the sealed plaintext.
const LEN_PREFIX: usize = 2;
/// Smallest admissible ciphertext region.
const MIN_CT_LEN: usize = TAG_SIZE + LEN_PREFIX;

/// Key-derivation context: the domain label followed by the topic.
#[derive(Debug, Clone, Copy)]
pub struct Info<'a> {
    topic: &'a Topic,
}

impl<'a> Info<'a> {
    /// The bound topic.
    #[must_use]
    pub const fn topic(&self) -> &'a Topic {
        self.topic
    }
}

impl<'a> From<&'a Topic> for Info<'a> {
    fn from(topic: &'a Topic) -> Self {
        Self { topic }
    }
}

/// An export length exceeded the HKDF expand bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("requested length exceeds the hkdf expand bound")]
pub struct ExportError;

/// Keypair derivation exhausted the candidate loop; probability 2^-256 per
/// step, unreachable in practice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("keypair derivation exhausted its candidate space")]
pub struct DeriveKeyPairError;

/// Errors from an HPKE seal.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum HpkeSealError {
    /// The message does not fit the ciphertext region.
    #[error("plaintext too long: {len} bytes, capacity {max}")]
    MessageTooLong {
        /// Plaintext length.
        len: usize,
        /// Region capacity after tag and length prefix.
        max: usize,
    },
    /// The region cannot hold the tag and length prefix.
    #[error("ciphertext region too small: {ct_len} bytes, minimum {MIN_CT_LEN}")]
    RegionTooSmall {
        /// Requested region length.
        ct_len: usize,
    },
    /// A fixed-size derivation failed; unreachable for the pinned suite.
    #[error("suite derivation invariant violated")]
    Internal,
}

/// Exporter interface over the context's `exporter_secret`.
pub struct Exporter {
    secret: Zeroizing<[u8; 32]>,
}

impl Exporter {
    /// Fill `okm` with `LabeledExpand(exporter_secret, "sec", context, L)`.
    pub fn export(&self, context: &[u8], okm: &mut [u8]) -> Result<(), ExportError> {
        labeled_expand(HPKE_SUITE_ID, &self.secret, b"sec", &[context], okm)
    }
}

impl core::fmt::Debug for Exporter {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("Exporter(..)")
    }
}

/// `LabeledExtract(salt, label, ikm)` with `ikm` given in parts.
fn labeled_extract(
    suite_id: &[u8],
    salt: &[u8],
    label: &[u8],
    ikm_parts: &[&[u8]],
) -> Zeroizing<[u8; 32]> {
    let mut ikm = Zeroizing::new(Vec::new());
    ikm.extend_from_slice(b"HPKE-v1");
    ikm.extend_from_slice(suite_id);
    ikm.extend_from_slice(label);
    for part in ikm_parts {
        ikm.extend_from_slice(part);
    }
    let (prk, _) = Hkdf::<Sha256>::extract(Some(salt), &ikm);
    Zeroizing::new(prk.into())
}

/// `LabeledExpand(prk, label, info, okm.len())` with `info` given in parts.
fn labeled_expand(
    suite_id: &[u8],
    prk: &[u8; 32],
    label: &[u8],
    info_parts: &[&[u8]],
    okm: &mut [u8],
) -> Result<(), ExportError> {
    let hk = Hkdf::<Sha256>::from_prk(prk).map_err(|_| ExportError)?;
    let length = u16::try_from(okm.len()).map_err(|_| ExportError)?;
    let length_bytes = length.to_be_bytes();
    let mut info: Vec<&[u8]> = alloc::vec![&length_bytes, b"HPKE-v1", suite_id, label];
    info.extend_from_slice(info_parts);
    hk.expand_multi_info(&info, okm).map_err(|_| ExportError)
}

/// DHKEM ExtractAndExpand; `Ok(None)` rejects a zero ECDH output.
fn extract_and_expand(
    dh: &[u8],
    enc: &[u8],
    pkr: &[u8],
) -> Result<Option<Zeroizing<[u8; 32]>>, ExportError> {
    if bool::from(dh.ct_eq(&[0u8; 32])) {
        return Ok(None);
    }
    let eae_prk = labeled_extract(KEM_SUITE_ID, b"", b"eae_prk", &[dh]);
    let mut shared = Zeroizing::new([0u8; 32]);
    labeled_expand(
        KEM_SUITE_ID,
        &eae_prk,
        b"shared_secret",
        &[enc, pkr],
        shared.as_mut_slice(),
    )?;
    Ok(Some(shared))
}

/// DeriveKeyPair per the draft; the secp256k1 bitmask is 0xff.
pub fn derive_key_pair(ikm: &[u8]) -> Result<(SecretKey, PublicKey), DeriveKeyPairError> {
    let dkp_prk = labeled_extract(KEM_SUITE_ID, b"", b"dkp_prk", &[ikm]);
    for counter in 0u8..=255 {
        let mut candidate = Zeroizing::new([0u8; 32]);
        labeled_expand(
            KEM_SUITE_ID,
            &dkp_prk,
            b"candidate",
            &[&[counter]],
            candidate.as_mut_slice(),
        )
        .map_err(|_| DeriveKeyPairError)?;
        if let Ok(secret) = SecretKey::from_slice(candidate.as_slice()) {
            let public = secret.public_key();
            return Ok((secret, public));
        }
    }
    Err(DeriveKeyPairError)
}

/// Derived per-message key material.
struct Schedule {
    key: Zeroizing<[u8; 32]>,
    base_nonce: [u8; 12],
    exporter: Exporter,
}

impl Schedule {
    /// The HPKE hint: `export("nectar/env/v1 hint", 8)`.
    fn hint(&self) -> Result<[u8; 8], ExportError> {
        let mut hint = [0u8; 8];
        self.exporter.export(HINT_CONTEXT, &mut hint)?;
        Ok(hint)
    }
}

/// KeySchedule for mode_base with `info = LABEL || topic`.
fn key_schedule(shared: &[u8; 32], topic: &Topic) -> Result<Schedule, ExportError> {
    let psk_id_hash = labeled_extract(HPKE_SUITE_ID, b"", b"psk_id_hash", &[]);
    let info_hash = labeled_extract(HPKE_SUITE_ID, b"", b"info_hash", &[LABEL, topic.as_bytes()]);
    let mut context = Vec::new();
    context.push(MODE_BASE);
    context.extend_from_slice(psk_id_hash.as_slice());
    context.extend_from_slice(info_hash.as_slice());

    let secret = labeled_extract(HPKE_SUITE_ID, shared, b"secret", &[]);
    let mut key = Zeroizing::new([0u8; 32]);
    labeled_expand(
        HPKE_SUITE_ID,
        &secret,
        b"key",
        &[&context],
        key.as_mut_slice(),
    )?;
    let mut base_nonce = [0u8; 12];
    labeled_expand(
        HPKE_SUITE_ID,
        &secret,
        b"base_nonce",
        &[&context],
        &mut base_nonce,
    )?;
    let mut exporter_secret = Zeroizing::new([0u8; 32]);
    labeled_expand(
        HPKE_SUITE_ID,
        &secret,
        b"exp",
        &[&context],
        exporter_secret.as_mut_slice(),
    )?;
    Ok(Schedule {
        key,
        base_nonce,
        exporter: Exporter {
            secret: exporter_secret,
        },
    })
}

/// Decap once per envelope; `Ok(None)` when `enc_x` is off-curve or the ECDH
/// output is rejected.
pub(super) fn decap(
    secret: &SecretKey,
    envelope: &Envelope<'_>,
) -> Result<Option<Zeroizing<[u8; 32]>>, OpenError> {
    let Some(enc) = super::reconstruct(envelope.enc_x(), envelope.parity()) else {
        return Ok(None);
    };
    let dh = k256::ecdh::diffie_hellman(secret.to_nonzero_scalar(), enc.as_affine());
    let dh_bytes: &[u8] = dh.raw_secret_bytes().as_ref();
    let enc_point = enc.to_encoded_point(false);
    let pkr_point = secret.public_key().to_encoded_point(false);
    extract_and_expand(dh_bytes, enc_point.as_bytes(), pkr_point.as_bytes())
        .map_err(|_| OpenError::Internal)
}

/// Hint probe plus AEAD open for one topic over an established shared
/// secret; a hint collision fails the tag and falls through as `Ok(None)`.
pub(super) fn open_with_shared(
    shared: &Zeroizing<[u8; 32]>,
    topic: &Topic,
    envelope: &Envelope<'_>,
) -> Result<Option<Opened<Hpke>>, OpenError> {
    let schedule = key_schedule(shared, topic).map_err(|_| OpenError::Internal)?;
    let hint = schedule.hint().map_err(|_| OpenError::Internal)?;
    if !hint_matches(&hint, envelope.hint()) {
        return Ok(None);
    }
    let Some((body, tag)) = envelope.ciphertext().split_last_chunk::<TAG_SIZE>() else {
        return Ok(None);
    };
    let cipher = ChaCha20Poly1305::new_from_slice(schedule.key.as_slice())
        .map_err(|_| OpenError::Internal)?;
    let nonce = Nonce::from(schedule.base_nonce);
    let mut buf = Zeroizing::new(body.to_vec());
    if cipher
        .decrypt_in_place_detached(&nonce, b"", &mut buf, &Tag::from(*tag))
        .is_err()
    {
        return Ok(None);
    }
    let Some((length_bytes, rest)) = buf.split_first_chunk::<LEN_PREFIX>() else {
        return Err(OpenError::MalformedMessage);
    };
    let length = usize::from(u16::from_be_bytes(*length_bytes));
    let message = rest.get(..length).ok_or(OpenError::MalformedMessage)?;
    Ok(Some(Opened {
        plaintext: message.to_vec(),
        extra: schedule.exporter,
    }))
}

pub(super) fn open(
    secret: &SecretKey,
    ctx: Info<'_>,
    envelope: &Envelope<'_>,
) -> Result<Option<Opened<Hpke>>, OpenError> {
    let Some(shared) = decap(secret, envelope)? else {
        return Ok(None);
    };
    open_with_shared(&shared, ctx.topic(), envelope)
}

#[cfg(any(test, feature = "encryption"))]
pub(super) fn seal(
    recipient: &Recipient<Hpke>,
    ctx: Info<'_>,
    plaintext: &[u8],
    ct_len: usize,
) -> Result<SealedEnvelope<Hpke>, HpkeSealError> {
    seal_with_ephemeral(
        &crate::ecies::generate_secret(),
        recipient.key(),
        ctx.topic(),
        plaintext,
        ct_len,
    )
}

/// Deterministic apart from the ephemeral; reached from [`seal`] and, for
/// the pinned vectors, from the KAT tests only.
#[cfg(any(test, feature = "encryption"))]
fn seal_with_ephemeral(
    ephemeral: &SecretKey,
    recipient: &PublicKey,
    topic: &Topic,
    plaintext: &[u8],
    ct_len: usize,
) -> Result<SealedEnvelope<Hpke>, HpkeSealError> {
    let capacity = ct_len
        .checked_sub(MIN_CT_LEN)
        .ok_or(HpkeSealError::RegionTooSmall { ct_len })?;
    let max = capacity.min(usize::from(u16::MAX));
    if plaintext.len() > max {
        return Err(HpkeSealError::MessageTooLong {
            len: plaintext.len(),
            max,
        });
    }
    let length = u16::try_from(plaintext.len()).map_err(|_| HpkeSealError::Internal)?;

    // Encap toward the recipient.
    let dh = k256::ecdh::diffie_hellman(ephemeral.to_nonzero_scalar(), recipient.as_affine());
    let dh_bytes: &[u8] = dh.raw_secret_bytes().as_ref();
    let enc = ephemeral.public_key();
    let enc_point = enc.to_encoded_point(false);
    let pkr_point = recipient.to_encoded_point(false);
    let shared = extract_and_expand(dh_bytes, enc_point.as_bytes(), pkr_point.as_bytes())
        .map_err(|_| HpkeSealError::Internal)?
        .ok_or(HpkeSealError::Internal)?;

    let schedule = key_schedule(&shared, topic).map_err(|_| HpkeSealError::Internal)?;
    let hint = schedule.hint().map_err(|_| HpkeSealError::Internal)?;

    // `len || msg || pad`, sealed to fill the region exactly.
    let inner_len = ct_len
        .checked_sub(TAG_SIZE)
        .ok_or(HpkeSealError::Internal)?;
    let mut buf = Vec::with_capacity(ct_len);
    buf.extend_from_slice(&length.to_be_bytes());
    buf.extend_from_slice(plaintext);
    buf.resize(inner_len, 0);

    let cipher = ChaCha20Poly1305::new_from_slice(schedule.key.as_slice())
        .map_err(|_| HpkeSealError::Internal)?;
    let nonce = Nonce::from(schedule.base_nonce);
    let tag = cipher
        .encrypt_in_place_detached(&nonce, b"", &mut buf)
        .map_err(|_| HpkeSealError::Internal)?;
    buf.extend_from_slice(&tag);

    let (enc_x, parity) = super::x_and_parity(&enc);
    Ok(SealedEnvelope::from_parts(
        hint,
        parity,
        enc_x,
        buf,
        schedule.exporter,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{hex, keccak256};
    use hpke_rs::{Hpke as RefHpke, HpkePrivateKey, HpkePublicKey, Mode};
    use hpke_rs_crypto::types::{AeadAlgorithm, KdfAlgorithm, KemAlgorithm};
    use hpke_rs_rust_crypto::HpkeRustCrypto;

    fn reference() -> RefHpke<HpkeRustCrypto> {
        RefHpke::new(
            Mode::Base,
            KemAlgorithm::DhKemK256,
            KdfAlgorithm::HkdfSha256,
            AeadAlgorithm::ChaCha20Poly1305,
        )
    }

    fn info_bytes(topic: &Topic) -> Vec<u8> {
        let mut info = LABEL.to_vec();
        info.extend_from_slice(topic.as_bytes());
        info
    }

    fn topic() -> Topic {
        Topic::new(keccak256(b"nectar envelope kat topic").0)
    }

    /// Deterministic keys: the scalars are keccak256 of the ASCII labels.
    fn recipient_keys() -> (SecretKey, PublicKey) {
        let secret =
            SecretKey::from_slice(keccak256(b"nectar envelope kat recipient").as_slice()).unwrap();
        let public = secret.public_key();
        (secret, public)
    }

    fn ephemeral_key() -> SecretKey {
        SecretKey::from_slice(keccak256(b"nectar envelope kat ephemeral").as_slice()).unwrap()
    }

    fn envelope_from(bytes: &[u8]) -> Envelope<'_> {
        Envelope::parse(bytes).unwrap()
    }

    #[test]
    fn differential_our_seal_reference_open() {
        let (recipient_sk, recipient_pk) = recipient_keys();
        let topic = topic();
        let msg = b"differential message";
        let sealed = seal_with_ephemeral(&ephemeral_key(), &recipient_pk, &topic, msg, 64).unwrap();

        let enc = ephemeral_key().public_key().to_encoded_point(false);
        let bytes = sealed.to_bytes();
        let envelope = envelope_from(&bytes);
        let inner = reference()
            .open(
                enc.as_bytes(),
                &recipient_sk.to_bytes().to_vec().into(),
                &info_bytes(&topic),
                b"",
                envelope.ciphertext(),
                None,
                None,
                None,
            )
            .unwrap();
        // The reference sees our inner framing: len || msg || pad.
        let (length, rest) = inner.split_first_chunk::<2>().unwrap();
        let length = usize::from(u16::from_be_bytes(*length));
        assert_eq!(length, msg.len());
        assert_eq!(&rest[..length], msg);
        assert_eq!(inner.len(), 64 - TAG_SIZE);
    }

    #[test]
    fn differential_reference_seal_our_open() {
        let (recipient_sk, recipient_pk) = recipient_keys();
        let topic = topic();
        let msg = b"reference sealed this";
        let pk_r = HpkePublicKey::new(recipient_pk.to_encoded_point(false).as_bytes().to_vec());
        let (enc, ct) = reference()
            .seal(&pk_r, &info_bytes(&topic), b"", msg, None, None, None)
            .unwrap();

        // Rebuild the shared secret from their enc and open their ct raw.
        let enc_pub = PublicKey::from_sec1_bytes(&enc).unwrap();
        let dh = k256::ecdh::diffie_hellman(recipient_sk.to_nonzero_scalar(), enc_pub.as_affine());
        let dh_bytes: &[u8] = dh.raw_secret_bytes().as_ref();
        let enc_point = enc_pub.to_encoded_point(false);
        assert_eq!(enc_point.as_bytes(), &enc[..]);
        let pkr_point = recipient_pk.to_encoded_point(false);
        let shared = extract_and_expand(dh_bytes, enc_point.as_bytes(), pkr_point.as_bytes())
            .unwrap()
            .unwrap();
        let schedule = key_schedule(&shared, &topic).unwrap();
        let (body, tag) = ct.split_last_chunk::<TAG_SIZE>().unwrap();
        let cipher = ChaCha20Poly1305::new_from_slice(schedule.key.as_slice()).unwrap();
        let mut buf = body.to_vec();
        cipher
            .decrypt_in_place_detached(
                &Nonce::from(schedule.base_nonce),
                b"",
                &mut buf,
                &Tag::from(*tag),
            )
            .unwrap();
        assert_eq!(buf, msg);
    }

    #[test]
    fn differential_export_and_hint() {
        let (recipient_sk, recipient_pk) = recipient_keys();
        let topic = topic();
        let sealed =
            seal_with_ephemeral(&ephemeral_key(), &recipient_pk, &topic, b"x", 64).unwrap();
        let enc = ephemeral_key().public_key().to_encoded_point(false);

        let theirs = reference()
            .receiver_export(
                enc.as_bytes(),
                &recipient_sk.to_bytes().to_vec().into(),
                &info_bytes(&topic),
                None,
                None,
                None,
                HINT_CONTEXT,
                8,
            )
            .unwrap();
        let bytes = sealed.to_bytes();
        let envelope = envelope_from(&bytes);
        assert_eq!(&theirs[..], envelope.hint());

        let mut ours = [0u8; 8];
        sealed.extra().export(HINT_CONTEXT, &mut ours).unwrap();
        assert_eq!(&theirs[..], &ours);
    }

    #[test]
    fn differential_derive_key_pair() {
        let ikm = keccak256(b"nectar envelope kat ikm").0;
        let (secret, public) = derive_key_pair(&ikm).unwrap();
        let theirs = reference().derive_key_pair(&ikm).unwrap();
        let (their_sk, their_pk) = theirs.into_keys();
        assert_eq!(their_sk, HpkePrivateKey::from(secret.to_bytes().to_vec()));
        assert_eq!(
            their_pk.as_slice(),
            public.to_encoded_point(false).as_bytes()
        );
    }

    #[test]
    fn region_too_small() {
        let (_, recipient_pk) = recipient_keys();
        let err =
            seal_with_ephemeral(&ephemeral_key(), &recipient_pk, &topic(), b"", 17).unwrap_err();
        assert_eq!(err, HpkeSealError::RegionTooSmall { ct_len: 17 });
    }

    #[test]
    fn message_too_long() {
        let (_, recipient_pk) = recipient_keys();
        let err = seal_with_ephemeral(&ephemeral_key(), &recipient_pk, &topic(), &[0u8; 47], 64)
            .unwrap_err();
        assert_eq!(err, HpkeSealError::MessageTooLong { len: 47, max: 46 });
    }

    #[test]
    fn off_curve_enc_rejects_cheaply() {
        let (recipient_sk, recipient_pk) = recipient_keys();
        let sealed =
            seal_with_ephemeral(&ephemeral_key(), &recipient_pk, &topic(), b"msg", 64).unwrap();
        let mut bytes = sealed.to_bytes();
        // Find an x that is off-curve for both parities, deterministically.
        let mut x = [0u8; 32];
        while super::super::reconstruct(&x, false).is_some()
            || super::super::reconstruct(&x, true).is_some()
        {
            x[31] = x[31].wrapping_add(1);
        }
        bytes[40..72].copy_from_slice(&x);
        let envelope = envelope_from(&bytes);
        assert!(decap(&recipient_sk, &envelope).unwrap().is_none());
        assert!(
            open(&recipient_sk, Info::from(&topic()), &envelope)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn exporter_length_bound() {
        let (_, recipient_pk) = recipient_keys();
        let sealed =
            seal_with_ephemeral(&ephemeral_key(), &recipient_pk, &topic(), b"x", 64).unwrap();
        let mut too_long = vec![0u8; 32 * 256];
        assert_eq!(
            sealed.extra().export(b"ctx", &mut too_long).unwrap_err(),
            ExportError
        );
    }

    /// Nectar-owned conformance vector in the RFC 9180 appendix shape:
    /// mode_base, kem 0x0016, kdf 0x0001, aead 0x0003, `ct_len` 64. The
    /// draft KEM has no registry vectors; these pin the whole chain from
    /// scalars to the serialized frame.
    #[test]
    fn kat_pinned() {
        const SK_RM: [u8; 32] =
            hex!("92f8eb1624814af780f0057c70922fbcfc11c7c4d660621f8ace98e8d1b74c0f");
        const PK_RM: [u8; 65] = hex!(
            "044bcc9cf73bde3b2b8cd50ccbe1121045d68c154673bc770f3120f1054b3667a528eb4c4d8cf27b88bc708a79cf46699500c293f51652ac06d6ec81df3e5be420"
        );
        const SK_EM: [u8; 32] =
            hex!("ccfdec3c5c5e08d13dc3e92ab3b60fca31a794ef089bd549d77ec0cc2a7fcb4f");
        const ENC: [u8; 65] = hex!(
            "04059ad0913b3994ee1124c74c717ef77105853d98d035f104eb59abb843c284a1f64be32b1705005723b677b6443d2b3e328f9a5d74f1526c4df9dece28197d14"
        );
        const TOPIC: [u8; 32] =
            hex!("495d20024ea5c3b8b0c1a496340482662f282f72a3d54914c0836b5eb9aff574");
        const SHARED_SECRET: [u8; 32] =
            hex!("a0828bc0a22e17da6114de7be55825fc3984f13b701abb0b777d3eb9876555c8");
        const KEY: [u8; 32] =
            hex!("38baadc102a68013e1149955e36c3feb0ba28aeacb923e6de32fda54c087d5db");
        const BASE_NONCE: [u8; 12] = hex!("6b806eaf22c4319f6cc50878");
        const HINT: [u8; 8] = hex!("e53a0642aa151e15");
        const FRAME: [u8; 136] = hex!(
            "e53a0642aa151e150000000000000000000000000000000000000000000000000000000000000000059ad0913b3994ee1124c74c717ef77105853d98d035f104eb59abb843c284a1dcc31c32e025943268d6d7b68f80c9c3dd6ad02e1769ad7824722ec2be1963e0a9f7b48940a16dd464f6e13b6cf52e5a4a21b2407c7b661e494efc9f76eb8ad2"
        );
        const MSG: &[u8] = b"nectar envelope kat message";

        let recipient_sk = SecretKey::from_slice(&SK_RM).unwrap();
        let recipient_pk = recipient_sk.public_key();
        assert_eq!(recipient_pk.to_encoded_point(false).as_bytes(), PK_RM);
        let eph = SecretKey::from_slice(&SK_EM).unwrap();
        assert_eq!(eph.public_key().to_encoded_point(false).as_bytes(), ENC);
        let topic = Topic::new(TOPIC);

        let dh = k256::ecdh::diffie_hellman(eph.to_nonzero_scalar(), recipient_pk.as_affine());
        let dh_bytes: &[u8] = dh.raw_secret_bytes().as_ref();
        let shared = extract_and_expand(dh_bytes, &ENC, &PK_RM).unwrap().unwrap();
        assert_eq!(*shared, SHARED_SECRET);
        let schedule = key_schedule(&shared, &topic).unwrap();
        assert_eq!(*schedule.key, KEY);
        assert_eq!(schedule.base_nonce, BASE_NONCE);
        assert_eq!(schedule.hint().unwrap(), HINT);

        let sealed = seal_with_ephemeral(&eph, &recipient_pk, &topic, MSG, 64).unwrap();
        assert_eq!(sealed.to_bytes(), FRAME);

        let envelope = envelope_from(&FRAME);
        let opened = open(&recipient_sk, Info::from(&topic), &envelope)
            .unwrap()
            .unwrap();
        assert_eq!(opened.plaintext, MSG);
    }

    /// Pinned DeriveKeyPair vector for the draft KEM.
    #[test]
    fn kat_derive_key_pair_pinned() {
        const IKM: [u8; 32] =
            hex!("a19f0627fc71bc68959c8dba381a3415a149ffa17711b63084e53a4eae030add");
        const SK: [u8; 32] =
            hex!("356863ac16c2d7d1c30e8f7c5b63bf4759e315a0d88bed24408ecfc13959c99e");
        const PK: [u8; 65] = hex!(
            "043148e5388b2676c8d8e05da2a918125e33e07dd2c57f7c401623b20785c419c84ce65c28c9585f7150b08040b3f06d849fc2bf414700875757aa6bc23e412af3"
        );
        let (secret, public) = derive_key_pair(&IKM).unwrap();
        assert_eq!(secret.to_bytes().to_vec(), SK.to_vec());
        assert_eq!(public.to_encoded_point(false).as_bytes(), PK);
    }
}
