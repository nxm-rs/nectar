//! Versioned modern envelope over the frozen ecies compat baseline.
//!
//! Two sealed schemes share one frozen frame inside the chunk payload:
//! `hint[0..8] || nonce[8..40] || enc_x[40..72] || ct[72..]`. Compat is the
//! byte-frozen [`crate::ecies`] construction; the envelope is RFC 9180 HPKE
//! with the single suite DHKEM(secp256k1, HKDF-SHA256) + HKDF-SHA256 +
//! ChaCha20-Poly1305 under the domain label `nectar/env/v1`.
//!
//! The KEM is draft-wahby-cfrg-hpke-kem-secp256k1 (requested codepoint
//! 0x0016): an expired individual CFRG draft, absent from the IANA HPKE
//! registry. The codepoint is ecosystem convention only, so the conformance
//! vectors carried by the test suite are nectar-owned and normative here.
//!
//! The version discriminant is cryptographic, not syntactic: compat keeps
//! `keccak256(key || salt)[..8]` in the hint slot, the envelope hint is
//! `export("nectar/env/v1 hint", 8)`; the labelled transcripts are
//! domain-disjoint, so only the recipient can tell the schemes apart. The
//! hint is mandatory for envelope records: ChaCha20-Poly1305 is not
//! key-committing, and trial-opening one ciphertext against many per-topic
//! key schedules without the hint gate is a partitioning oracle.
//!
//! `enc` travels x-only; the low bit of `nonce[0]` carries the ephemeral y
//! parity in both schemes (miners hold it fixed). The envelope reconstructs
//! canonical uncompressed SEC1 before `kem_context`; compat ECDH ignores
//! parity, so a flipped bit is an authenticated decap failure (DoS only) for
//! the envelope and a no-op for compat. Records of both schemes are
//! curve-membership-detectable against uniform bytes: the anonymity set is
//! trojan chunks, at exact parity with the deployed compat path.
//!
//! Trial order: decap once per record (after chunk validity and
//! proof-of-work checks; that per-valid-chunk cost is inherent), one hint
//! probe per candidate topic, AEAD open only on a hint match. HKDF-SHA256
//! deliberately adds SHA-256 beside the otherwise keccak-only proving
//! surface.

use alloc::vec::Vec;
use core::fmt;
use core::marker::PhantomData;

use k256::{PublicKey, SecretKey};
use subtle::ConstantTimeEq;
use thiserror::Error;

use crate::ecies::Salt;

mod attestation;
mod compat;
mod hpke;

pub use attestation::{ATTESTATION_PREFIX, AttestationError, EnvelopeAttestation, sign_data};
pub use hpke::{DeriveKeyPairError, ExportError, Exporter, HpkeSealError, Info, derive_key_pair};

use crate::chunk::encryption::EncryptionKey;
use crate::ecies::EciesError;

/// Domain label of the envelope suite.
pub const LABEL: &[u8] = b"nectar/env/v1";

/// Byte length of the hint slot.
pub const HINT_SIZE: usize = 8;
/// Byte length of the mining nonce slot.
pub const NONCE_SIZE: usize = 32;
/// Byte length of the x-only ephemeral slot.
pub const ENC_X_SIZE: usize = 32;
/// Byte length of the frame header preceding the ciphertext region.
pub const HEADER_SIZE: usize = HINT_SIZE + NONCE_SIZE + ENC_X_SIZE;

mod sealed {
    pub trait Sealed {}
    impl Sealed for super::Compat {}
    impl Sealed for super::Hpke {}
    impl Sealed for super::EnvelopeOnly {}
    impl Sealed for super::EnvelopeThenCompat {}
}

/// 32-byte topic a record is addressed under.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Topic([u8; 32]);

impl Topic {
    /// Wrap raw topic bytes.
    #[must_use]
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Access the raw topic bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl From<[u8; 32]> for Topic {
    fn from(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}

impl<'a> From<&'a Topic> for Salt<'a> {
    fn from(topic: &'a Topic) -> Self {
        Self::raw(&topic.0)
    }
}

/// Scheme label reported in open results; never a dispatcher.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemeId {
    /// The frozen reference-client construction.
    Compat,
    /// The HPKE envelope.
    Envelope,
}

/// One of the two envelope schemes; sealed, dispatch rides the type.
pub trait Scheme: sealed::Sealed + Sized {
    /// Per-scheme key-derivation context built from a topic.
    type Context<'a>: From<&'a Topic>;
    /// Scheme-specific by-product of a seal or open.
    type Extra: fmt::Debug;
    /// Proof carried by a [`Recipient`] of how it was minted.
    type Provenance: Clone + fmt::Debug;
    /// Errors from sealing.
    type SealError: core::error::Error;

    /// AEAD tag bytes spent inside the ciphertext region.
    const TAG: usize;
    /// Report label for open results.
    const ID: SchemeId;

    /// Seal `plaintext` toward `recipient` into a `ct_len`-byte ciphertext
    /// region.
    #[cfg(feature = "encryption")]
    fn seal(
        recipient: &Recipient<Self>,
        ctx: Self::Context<'_>,
        plaintext: &[u8],
        ct_len: usize,
    ) -> Result<SealedRecord<Self>, Self::SealError>;

    /// Try to open `record` under `secret` for one context. `Ok(None)` means
    /// the record is not recognized under this scheme and context.
    fn open(
        secret: &SecretKey,
        ctx: Self::Context<'_>,
        record: &Record<'_>,
    ) -> Result<Option<Opened<Self>>, OpenError>;
}

/// The frozen reference-client construction, adapted over [`crate::ecies`].
#[derive(Debug, Clone, Copy)]
pub enum Compat {}

/// The HPKE envelope suite; one sealed type, no agility.
#[derive(Debug, Clone, Copy)]
pub enum Hpke {}

/// Loud, greppable justification for sealing toward a compat recipient.
#[derive(Debug, Clone, Copy)]
pub struct InteropReason(&'static str);

impl InteropReason {
    /// Record why compat interop is unavoidable here.
    #[must_use]
    pub const fn new(reason: &'static str) -> Self {
        Self(reason)
    }

    /// The recorded justification.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        self.0
    }
}

/// Proof that a [`Recipient<Hpke>`] came from a verified
/// [`EnvelopeAttestation`]; not constructible outside this crate.
#[derive(Clone)]
pub struct Attested(());

impl fmt::Debug for Attested {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Attested")
    }
}

/// Sealing handle for one recipient key under one scheme.
///
/// Upgrade is one-way: no conversion between schemes exists in either
/// direction.
///
/// ```compile_fail
/// use nectar_primitives::envelope::{Compat, Hpke, Recipient, Scheme, Topic};
///
/// fn downgrade(recipient: &Recipient<Hpke>, topic: &Topic) {
///     // compat seal toward an envelope recipient must not typecheck
///     let _ = Compat::seal(recipient, topic.into(), b"msg", 64);
/// }
/// ```
#[derive(Clone)]
pub struct Recipient<S: Scheme> {
    key: PublicKey,
    provenance: S::Provenance,
}

impl<S: Scheme> Recipient<S> {
    /// The recipient public key.
    #[must_use]
    pub const fn key(&self) -> &PublicKey {
        &self.key
    }
}

impl<S: Scheme> fmt::Debug for Recipient<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Recipient")
            .field("scheme", &S::ID)
            .field("key", &self.key)
            .finish_non_exhaustive()
    }
}

impl Recipient<Compat> {
    /// The one sanctioned downgrade path: address a reader that only speaks
    /// the reference construction.
    #[must_use]
    pub const fn assume_reference(key: PublicKey, reason: InteropReason) -> Self {
        Self {
            key,
            provenance: reason,
        }
    }

    /// Why this recipient is compat.
    #[must_use]
    pub const fn reason(&self) -> InteropReason {
        self.provenance
    }
}

impl Recipient<Hpke> {
    /// Mint from a verified attestation; the only construction path.
    pub(crate) const fn attested(key: PublicKey) -> Self {
        Self {
            key,
            provenance: Attested(()),
        }
    }
}

/// A record payload was shorter than the frame header.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("record too short: {got} bytes, header is {HEADER_SIZE}")]
pub struct RecordTooShort {
    /// Observed payload length.
    pub got: usize,
}

/// Parsed view of the frozen frame.
#[derive(Debug, Clone, Copy)]
pub struct Record<'a> {
    hint: &'a [u8; HINT_SIZE],
    nonce: &'a [u8; NONCE_SIZE],
    enc_x: &'a [u8; ENC_X_SIZE],
    ct: &'a [u8],
}

impl<'a> Record<'a> {
    /// Split a chunk payload into the frame fields.
    pub const fn parse(payload: &'a [u8]) -> Result<Self, RecordTooShort> {
        let got = payload.len();
        let Some((hint, rest)) = payload.split_first_chunk::<HINT_SIZE>() else {
            return Err(RecordTooShort { got });
        };
        let Some((nonce, rest)) = rest.split_first_chunk::<NONCE_SIZE>() else {
            return Err(RecordTooShort { got });
        };
        let Some((enc_x, ct)) = rest.split_first_chunk::<ENC_X_SIZE>() else {
            return Err(RecordTooShort { got });
        };
        Ok(Self {
            hint,
            nonce,
            enc_x,
            ct,
        })
    }

    /// The hint slot.
    #[must_use]
    pub const fn hint(&self) -> &[u8; HINT_SIZE] {
        self.hint
    }

    /// The mining nonce slot.
    #[must_use]
    pub const fn nonce(&self) -> &[u8; NONCE_SIZE] {
        self.nonce
    }

    /// The x-only ephemeral slot.
    #[must_use]
    pub const fn enc_x(&self) -> &[u8; ENC_X_SIZE] {
        self.enc_x
    }

    /// The ciphertext region.
    #[must_use]
    pub const fn ciphertext(&self) -> &'a [u8] {
        self.ct
    }

    /// Ephemeral y parity carried in the low bit of `nonce[0]`.
    #[must_use]
    pub const fn parity(&self) -> bool {
        let [first, ..] = self.nonce;
        *first & 1 == 1
    }
}

/// Output of a successful open.
pub struct Opened<S: Scheme> {
    /// Recovered plaintext. Envelope: the exact message; compat: the whole
    /// decrypted ciphertext region, framing left to the caller.
    pub plaintext: Vec<u8>,
    /// Scheme-specific by-product.
    pub extra: S::Extra,
}

impl<S: Scheme> fmt::Debug for Opened<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Opened")
            .field("scheme", &S::ID)
            .field("plaintext_len", &self.plaintext.len())
            .field("extra", &self.extra)
            .finish()
    }
}

/// Errors from opening a record that was recognized as ours.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum OpenError {
    /// The authenticated inner framing is inconsistent.
    #[error("authenticated inner framing is malformed")]
    MalformedMessage,
    /// A fixed-size derivation failed; unreachable for the pinned suite.
    #[error("suite derivation invariant violated")]
    Internal,
}

/// A sealed record ready for framing into a chunk payload.
pub struct SealedRecord<S: Scheme> {
    hint: [u8; HINT_SIZE],
    nonce: [u8; NONCE_SIZE],
    enc_x: [u8; ENC_X_SIZE],
    ct: Vec<u8>,
    extra: S::Extra,
}

impl<S: Scheme> SealedRecord<S> {
    pub(crate) fn from_parts(
        hint: [u8; HINT_SIZE],
        parity: bool,
        enc_x: [u8; ENC_X_SIZE],
        ct: Vec<u8>,
        extra: S::Extra,
    ) -> Self {
        let mut nonce = [0u8; NONCE_SIZE];
        let [first, ..] = &mut nonce;
        *first = u8::from(parity);
        Self {
            hint,
            nonce,
            enc_x,
            ct,
            extra,
        }
    }

    /// Replace the mining nonce; the parity bit is reasserted.
    pub const fn set_nonce(&mut self, mut nonce: [u8; NONCE_SIZE]) {
        let [current, ..] = &self.nonce;
        let parity = *current & 1;
        let [first, ..] = &mut nonce;
        *first = (*first & 0xfe) | parity;
        self.nonce = nonce;
    }

    /// Serialize the frozen frame.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(HEADER_SIZE.saturating_add(self.ct.len()));
        out.extend_from_slice(&self.hint);
        out.extend_from_slice(&self.nonce);
        out.extend_from_slice(&self.enc_x);
        out.extend_from_slice(&self.ct);
        out
    }

    /// Scheme-specific by-product of the seal.
    #[must_use]
    pub const fn extra(&self) -> &S::Extra {
        &self.extra
    }

    /// Consume into the scheme-specific by-product.
    #[must_use]
    pub fn into_extra(self) -> S::Extra {
        self.extra
    }
}

impl<S: Scheme> fmt::Debug for SealedRecord<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SealedRecord")
            .field("scheme", &S::ID)
            .field("ct_len", &self.ct.len())
            .finish_non_exhaustive()
    }
}

/// Constant-time hint comparison.
pub(crate) fn hint_matches(derived: &[u8; HINT_SIZE], carried: &[u8; HINT_SIZE]) -> bool {
    derived.ct_eq(carried).into()
}

/// Delivery policy of an [`Inbox`]; sealed.
pub trait Policy: sealed::Sealed {
    /// Whether compat probes are admitted after the envelope pass.
    const COMPAT: bool;
}

/// Envelope-only delivery: compat hints are never computed, so compat
/// records are undeliverable outright.
#[derive(Debug, Clone, Copy)]
pub enum EnvelopeOnly {}

impl Policy for EnvelopeOnly {
    const COMPAT: bool = false;
}

/// Strictly transitional delivery: envelope first, compat probes after.
/// Forbidden for peers pinned envelope-capable; compat has no integrity or
/// sender authentication.
#[derive(Debug, Clone, Copy)]
pub enum EnvelopeThenCompat {}

impl Policy for EnvelopeThenCompat {
    const COMPAT: bool = true;
}

/// Trial-decrypt driver for one recipient secret under a delivery policy.
pub struct Inbox<P: Policy> {
    secret: SecretKey,
    _policy: PhantomData<P>,
}

impl<P: Policy> fmt::Debug for Inbox<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Inbox")
            .field("compat", &P::COMPAT)
            .finish_non_exhaustive()
    }
}

impl Inbox<EnvelopeOnly> {
    /// Envelope-only inbox for `secret`.
    #[must_use]
    pub const fn new(secret: SecretKey) -> Self {
        Self {
            secret,
            _policy: PhantomData,
        }
    }
}

impl Inbox<EnvelopeThenCompat> {
    /// Transitional inbox for `secret`; flip to [`EnvelopeOnly`] the moment
    /// the peer set is confirmed envelope-capable.
    #[must_use]
    pub const fn transitional(secret: SecretKey) -> Self {
        Self {
            secret,
            _policy: PhantomData,
        }
    }
}

impl<P: Policy> Inbox<P> {
    /// Trial-open `record` against `topics`: one decap, an envelope hint
    /// probe per topic, then compat probes where the policy admits them.
    pub fn open(
        &self,
        topics: &[Topic],
        record: &Record<'_>,
    ) -> Result<Option<(Topic, InboxOpened)>, OpenError> {
        if let Some(shared) = hpke::decap(&self.secret, record)? {
            for topic in topics {
                if let Some(opened) = hpke::open_with_shared(&shared, topic, record)? {
                    return Ok(Some((*topic, InboxOpened::Envelope(opened))));
                }
            }
        }
        if P::COMPAT {
            for topic in topics {
                if let Some(opened) = Compat::open(&self.secret, topic.into(), record)? {
                    return Ok(Some((*topic, InboxOpened::Compat(opened))));
                }
            }
        }
        Ok(None)
    }
}

/// Erased inbox delivery; match once at the edge.
#[derive(Debug)]
pub enum InboxOpened {
    /// Opened under the envelope.
    Envelope(Opened<Hpke>),
    /// Opened under compat.
    Compat(Opened<Compat>),
}

impl InboxOpened {
    /// Which scheme delivered.
    #[must_use]
    pub const fn scheme(&self) -> SchemeId {
        match self {
            Self::Envelope(_) => SchemeId::Envelope,
            Self::Compat(_) => SchemeId::Compat,
        }
    }
}

/// Edge-only recipient union for heterogeneous collections; matches once
/// and calls the monomorphized path, never enters the seam.
#[derive(Debug, Clone)]
pub enum AnyRecipient {
    /// An attested envelope recipient.
    Envelope(Recipient<Hpke>),
    /// A justified compat recipient.
    Compat(Recipient<Compat>),
}

/// Erased sealed record from [`AnyRecipient::seal`].
#[derive(Debug)]
pub enum AnySealed {
    /// Sealed under the envelope.
    Envelope(SealedRecord<Hpke>),
    /// Sealed under compat.
    Compat(SealedRecord<Compat>),
}

impl AnySealed {
    /// Which scheme sealed.
    #[must_use]
    pub const fn scheme(&self) -> SchemeId {
        match self {
            Self::Envelope(_) => SchemeId::Envelope,
            Self::Compat(_) => SchemeId::Compat,
        }
    }

    /// Serialize the frozen frame.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Self::Envelope(record) => record.to_bytes(),
            Self::Compat(record) => record.to_bytes(),
        }
    }
}

/// Errors from [`AnyRecipient::seal`].
#[derive(Debug, Error)]
pub enum AnySealError {
    /// The envelope seal failed.
    #[error(transparent)]
    Envelope(#[from] HpkeSealError),
    /// The compat seal failed.
    #[error(transparent)]
    Compat(#[from] EciesError),
}

#[cfg(feature = "encryption")]
impl AnyRecipient {
    /// Seal toward this recipient, scheme inferred from the handle.
    pub fn seal(
        &self,
        topic: &Topic,
        plaintext: &[u8],
        ct_len: usize,
    ) -> Result<AnySealed, AnySealError> {
        match self {
            Self::Envelope(recipient) => Ok(AnySealed::Envelope(Hpke::seal(
                recipient,
                topic.into(),
                plaintext,
                ct_len,
            )?)),
            Self::Compat(recipient) => Ok(AnySealed::Compat(Compat::seal(
                recipient,
                topic.into(),
                plaintext,
                ct_len,
            )?)),
        }
    }
}

/// Split a public key into its x coordinate and y parity.
pub(crate) fn x_and_parity(key: &PublicKey) -> ([u8; 32], bool) {
    use k256::elliptic_curve::point::AffineCoordinates;
    let affine = key.as_affine();
    (affine.x().into(), affine.y_is_odd().into())
}

/// Reconstruct a public key from an x-only slot and a parity bit.
pub(crate) fn reconstruct(enc_x: &[u8; ENC_X_SIZE], parity: bool) -> Option<PublicKey> {
    let mut sec1 = [0u8; 33];
    let [tag, x @ ..] = &mut sec1;
    *tag = if parity { 0x03 } else { 0x02 };
    *x = *enc_x;
    PublicKey::from_sec1_bytes(&sec1).ok()
}

// Compat has no extra beyond the derived key; keep the association explicit.
impl Scheme for Compat {
    type Context<'a> = Salt<'a>;
    type Extra = EncryptionKey;
    type Provenance = InteropReason;
    type SealError = EciesError;

    const TAG: usize = 0;
    const ID: SchemeId = SchemeId::Compat;

    #[cfg(feature = "encryption")]
    fn seal(
        recipient: &Recipient<Self>,
        ctx: Self::Context<'_>,
        plaintext: &[u8],
        ct_len: usize,
    ) -> Result<SealedRecord<Self>, Self::SealError> {
        compat::seal(recipient, ctx, plaintext, ct_len)
    }

    fn open(
        secret: &SecretKey,
        ctx: Self::Context<'_>,
        record: &Record<'_>,
    ) -> Result<Option<Opened<Self>>, OpenError> {
        compat::open(secret, ctx, record)
    }
}

impl Scheme for Hpke {
    type Context<'a> = Info<'a>;
    type Extra = Exporter;
    type Provenance = Attested;
    type SealError = HpkeSealError;

    const TAG: usize = 16;
    const ID: SchemeId = SchemeId::Envelope;

    #[cfg(feature = "encryption")]
    fn seal(
        recipient: &Recipient<Self>,
        ctx: Self::Context<'_>,
        plaintext: &[u8],
        ct_len: usize,
    ) -> Result<SealedRecord<Self>, Self::SealError> {
        hpke::seal(recipient, ctx, plaintext, ct_len)
    }

    fn open(
        secret: &SecretKey,
        ctx: Self::Context<'_>,
        record: &Record<'_>,
    ) -> Result<Option<Opened<Self>>, OpenError> {
        hpke::open(secret, ctx, record)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::keccak256;

    use crate::ecies;

    fn keys(label: &[u8]) -> (SecretKey, PublicKey) {
        let secret = SecretKey::from_slice(keccak256(label).as_slice()).unwrap();
        let public = secret.public_key();
        (secret, public)
    }

    fn topic() -> Topic {
        Topic::new(keccak256(b"nectar envelope seam topic").0)
    }

    fn envelope_recipient(key: PublicKey) -> Recipient<Hpke> {
        Recipient::attested(key)
    }

    fn compat_recipient(key: PublicKey) -> Recipient<Compat> {
        Recipient::assume_reference(key, InteropReason::new("seam test"))
    }

    #[test]
    fn envelope_roundtrip_via_seam() {
        let (secret, public) = keys(b"nectar envelope seam recipient");
        let topic = topic();
        let msg = b"seam roundtrip";
        let sealed = Hpke::seal(&envelope_recipient(public), (&topic).into(), msg, 4024).unwrap();
        let bytes = sealed.to_bytes();
        assert_eq!(bytes.len(), HEADER_SIZE + 4024);

        let record = Record::parse(&bytes).unwrap();
        let opened = Hpke::open(&secret, (&topic).into(), &record)
            .unwrap()
            .unwrap();
        assert_eq!(opened.plaintext, msg);

        // Both sides export identical bytes for the same context.
        let mut sender = [0u8; 16];
        sealed.extra().export(b"reply", &mut sender).unwrap();
        let mut receiver = [0u8; 16];
        opened.extra.export(b"reply", &mut receiver).unwrap();
        assert_eq!(sender, receiver);
    }

    #[test]
    fn envelope_wrong_topic_is_undeliverable() {
        let (secret, public) = keys(b"nectar envelope seam recipient");
        let sealed = Hpke::seal(&envelope_recipient(public), (&topic()).into(), b"m", 64).unwrap();
        let bytes = sealed.to_bytes();
        let record = Record::parse(&bytes).unwrap();
        let other = Topic::new(keccak256(b"other topic").0);
        assert!(
            Hpke::open(&secret, (&other).into(), &record)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn compat_roundtrip_matches_ecies() {
        let (secret, public) = keys(b"nectar envelope seam recipient");
        let topic = topic();
        let msg = b"compat roundtrip";
        let sealed = Compat::seal(&compat_recipient(public), (&topic).into(), msg, 100).unwrap();
        let bytes = sealed.to_bytes();
        assert_eq!(bytes.len(), HEADER_SIZE + 100);

        let record = Record::parse(&bytes).unwrap();
        let opened = Compat::open(&secret, (&topic).into(), &record)
            .unwrap()
            .unwrap();
        assert_eq!(&opened.plaintext[..msg.len()], msg);
        assert_eq!(opened.plaintext.len(), 100);

        // The adapter is byte-for-byte the frozen construction.
        let ephemeral = reconstruct(record.enc_x(), record.parity()).unwrap();
        let key = ecies::shared_key(&secret, &ephemeral, (&topic).into());
        assert_eq!(key, opened.extra);
        assert_eq!(
            ecies::Hint::derive(&key, (&topic).into()).as_bytes(),
            record.hint()
        );
        assert_eq!(ecies::decrypt(&key, record.ciphertext()), opened.plaintext);
    }

    #[test]
    fn schemes_never_cross_open() {
        let (secret, public) = keys(b"nectar envelope seam recipient");
        let topic = topic();

        let envelope = Hpke::seal(&envelope_recipient(public), (&topic).into(), b"e", 64).unwrap();
        let bytes = envelope.to_bytes();
        let record = Record::parse(&bytes).unwrap();
        assert!(
            Compat::open(&secret, (&topic).into(), &record)
                .unwrap()
                .is_none()
        );

        let compat = Compat::seal(&compat_recipient(public), (&topic).into(), b"c", 64).unwrap();
        let bytes = compat.to_bytes();
        let record = Record::parse(&bytes).unwrap();
        assert!(
            Hpke::open(&secret, (&topic).into(), &record)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn envelope_only_inbox_rejects_compat_forgeries() {
        let (secret, public) = keys(b"nectar envelope seam recipient");
        let topic = topic();
        let compat = Compat::seal(&compat_recipient(public), (&topic).into(), b"f", 64).unwrap();
        let bytes = compat.to_bytes();
        let record = Record::parse(&bytes).unwrap();

        let inbox = Inbox::<EnvelopeOnly>::new(secret.clone());
        assert!(inbox.open(&[topic], &record).unwrap().is_none());

        // The transitional inbox still delivers it, reported as compat.
        let inbox = Inbox::<EnvelopeThenCompat>::transitional(secret);
        let (delivered_topic, opened) = inbox.open(&[topic], &record).unwrap().unwrap();
        assert_eq!(delivered_topic, topic);
        assert_eq!(opened.scheme(), SchemeId::Compat);
    }

    #[test]
    fn inbox_delivers_envelope_on_the_right_topic() {
        let (secret, public) = keys(b"nectar envelope seam recipient");
        let topic = topic();
        let decoy = Topic::new(keccak256(b"decoy topic").0);
        let sealed = Hpke::seal(&envelope_recipient(public), (&topic).into(), b"m", 64).unwrap();
        let bytes = sealed.to_bytes();
        let record = Record::parse(&bytes).unwrap();

        let inbox = Inbox::<EnvelopeOnly>::new(secret);
        let (delivered_topic, opened) = inbox.open(&[decoy, topic], &record).unwrap().unwrap();
        assert_eq!(delivered_topic, topic);
        assert_eq!(opened.scheme(), SchemeId::Envelope);
        let InboxOpened::Envelope(opened) = opened else {
            panic!("wrong scheme");
        };
        assert_eq!(opened.plaintext, b"m");
    }

    #[test]
    fn remining_the_nonce_keeps_the_record_open() {
        let (secret, public) = keys(b"nectar envelope seam recipient");
        let topic = topic();
        let mut sealed =
            Hpke::seal(&envelope_recipient(public), (&topic).into(), b"m", 64).unwrap();
        let parity_before = {
            let bytes = sealed.to_bytes();
            bytes[8] & 1
        };
        sealed.set_nonce([0xff; 32]);
        let bytes = sealed.to_bytes();
        assert_eq!(bytes[8] & 1, parity_before);
        assert_eq!(&bytes[9..40], &[0xff; 31]);

        let record = Record::parse(&bytes).unwrap();
        assert!(
            Hpke::open(&secret, (&topic).into(), &record)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn flipped_parity_is_dos_only_for_the_envelope() {
        let (secret, public) = keys(b"nectar envelope seam recipient");
        let topic = topic();

        let sealed = Hpke::seal(&envelope_recipient(public), (&topic).into(), b"m", 64).unwrap();
        let mut bytes = sealed.to_bytes();
        bytes[8] ^= 1;
        let record = Record::parse(&bytes).unwrap();
        assert!(
            Hpke::open(&secret, (&topic).into(), &record)
                .unwrap()
                .is_none()
        );

        // Compat ECDH ignores parity: the flipped record still opens.
        let sealed = Compat::seal(&compat_recipient(public), (&topic).into(), b"m", 64).unwrap();
        let mut bytes = sealed.to_bytes();
        bytes[8] ^= 1;
        let record = Record::parse(&bytes).unwrap();
        assert!(
            Compat::open(&secret, (&topic).into(), &record)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn record_parse_rejects_short_payloads() {
        assert_eq!(
            Record::parse(&[0u8; HEADER_SIZE - 1]).unwrap_err(),
            RecordTooShort {
                got: HEADER_SIZE - 1
            }
        );
        let record = Record::parse(&[0u8; HEADER_SIZE]).unwrap();
        assert!(record.ciphertext().is_empty());
    }

    #[test]
    fn any_recipient_seals_via_the_monomorphized_paths() {
        let (secret, public) = keys(b"nectar envelope seam recipient");
        let topic = topic();

        let any = AnyRecipient::Envelope(envelope_recipient(public));
        let sealed = any.seal(&topic, b"m", 64).unwrap();
        assert_eq!(sealed.scheme(), SchemeId::Envelope);
        let bytes = sealed.to_bytes();
        let record = Record::parse(&bytes).unwrap();
        assert!(
            Hpke::open(&secret, (&topic).into(), &record)
                .unwrap()
                .is_some()
        );

        let any = AnyRecipient::Compat(compat_recipient(public));
        let sealed = any.seal(&topic, b"m", 64).unwrap();
        assert_eq!(sealed.scheme(), SchemeId::Compat);
        let bytes = sealed.to_bytes();
        let record = Record::parse(&bytes).unwrap();
        assert!(
            Compat::open(&secret, (&topic).into(), &record)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn compat_reason_is_carried() {
        let (_, public) = keys(b"nectar envelope seam recipient");
        let recipient =
            Recipient::assume_reference(public, InteropReason::new("legacy gateway peer"));
        assert_eq!(recipient.reason().as_str(), "legacy gateway peer");
        assert_eq!(recipient.key(), &public);
    }

    #[test]
    fn scheme_constants() {
        assert_eq!(Compat::TAG, 0);
        assert_eq!(Hpke::TAG, 16);
        assert_eq!(Compat::ID, SchemeId::Compat);
        assert_eq!(Hpke::ID, SchemeId::Envelope);
    }
}
