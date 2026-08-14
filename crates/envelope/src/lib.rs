//! Sealed envelopes over the frozen ecies compat baseline.
//!
//! Two sealed schemes share one frozen frame inside the chunk payload:
//! `hint[0..8] || nonce[8..40] || enc_x[40..72] || ct[72..]`. Compat is the
//! byte-frozen [`crate::ecies`] construction; HPKE is RFC 9180 with the
//! single suite DHKEM(secp256k1, HKDF-SHA256) + HKDF-SHA256 +
//! ChaCha20-Poly1305 under the domain label `nectar/env/v1`.
//!
//! No nectar crate reads or writes this frame yet, so the crate is unstable:
//! the KEM, the frame and this API may change without a major version until a
//! consumer pins them.
//!
//! The KEM is registered with IANA as 0x0016, DHKEM(secp256k1, HKDF-SHA256),
//! under Specification Required; its defining draft
//! (draft-wahby-cfrg-hpke-kem-secp256k1) has expired and publishes no test
//! vectors. The conformance vectors carried by the test suite are therefore
//! nectar-owned, and validated differentially against `hpke-rs`.
//!
//! The version discriminant is cryptographic, not syntactic: compat keeps
//! `keccak256(key || salt)[..8]` in the hint slot, the HPKE hint is
//! `export("nectar/env/v1 hint", 8)`; the labelled transcripts are
//! domain-disjoint, so only the recipient can tell the schemes apart. The
//! hint is mandatory under HPKE: ChaCha20-Poly1305 is not key-committing,
//! and trial-opening one ciphertext against many per-topic key schedules
//! without the hint gate is a partitioning oracle.
//!
//! `enc` travels x-only; the low bit of `nonce[0]` carries the ephemeral y
//! parity in both schemes (miners hold it fixed). HPKE reconstructs
//! canonical uncompressed SEC1 before `kem_context`; compat ECDH ignores
//! parity, so a flipped bit is an authenticated decap failure (DoS only)
//! under HPKE and a no-op for compat. Envelopes of both schemes are
//! curve-membership-detectable against uniform bytes: the anonymity set is
//! trojan chunks, at exact parity with the deployed compat path.
//!
//! The reference client places that bit elsewhere: it mines `nonce[28]` and
//! reads it back as `chunkData[36]`. Neither side breaks, because compat ECDH
//! is x-only, and its reader forces odd regardless (`chunkData[36] | 0x1 != 0`
//! is a bitwise or, so always true). It matters only to a future scheme that
//! makes parity load-bearing on the compat frame.
//!
//! Trial order: decap once per envelope (after chunk validity and
//! proof-of-work checks; that per-valid-chunk cost is inherent), one hint
//! probe per candidate topic, AEAD open only on a hint match. HKDF-SHA256
//! deliberately adds SHA-256 beside the otherwise keccak-only proving
//! surface.

#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::get_unwrap,
        clippy::indexing_slicing,
        clippy::string_slice,
        clippy::arithmetic_side_effects,
        clippy::panic,
        clippy::unreachable,
        clippy::panic_in_result_fn,
        clippy::as_conversions
    )
)]
extern crate alloc;

use alloc::vec::Vec;
use core::fmt;
use core::marker::PhantomData;

use k256::{PublicKey, SecretKey};
use nectar_primitives::chunk::encryption::EncryptionKey;
use subtle::ConstantTimeEq;
use thiserror::Error;

use crate::ecies::{EciesError, Salt};

mod attestation;
mod compat;
pub mod ecies;
mod hpke;

pub use attestation::{ATTESTATION_PREFIX, AttestationError, RecipientAttestation, sign_data};
pub use hpke::{DeriveKeyPairError, ExportError, Exporter, HpkeSealError, Info, derive_key_pair};

/// Domain label of the HPKE suite.
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
    impl Sealed for super::HpkeOnly {}
    impl Sealed for super::HpkeThenCompat {}
}

/// 32-byte topic an envelope is addressed under.
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
    /// RFC 9180 HPKE.
    Hpke,
}

/// One of the two sealing schemes; sealed, dispatch rides the type.
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
    #[cfg(any(test, feature = "encryption"))]
    fn seal(
        recipient: &Recipient<Self>,
        ctx: Self::Context<'_>,
        plaintext: &[u8],
        ct_len: usize,
    ) -> Result<SealedEnvelope<Self>, Self::SealError>;

    /// Try to open `envelope` under `secret` for one context. `Ok(None)`
    /// means it is not recognized under this scheme and context.
    fn open(
        secret: &SecretKey,
        ctx: Self::Context<'_>,
        envelope: &Envelope<'_>,
    ) -> Result<Option<Opened<Self>>, OpenError>;
}

/// The frozen reference-client construction, adapted over [`crate::ecies`].
#[derive(Debug, Clone, Copy)]
pub enum Compat {}

/// The HPKE suite; one sealed type, no agility.
#[derive(Debug, Clone, Copy)]
pub enum Hpke {}

/// Loud, greppable justification for sealing toward a compat recipient.
#[derive(Debug, Clone, Copy)]
pub struct InteropReason(&'static str);

impl InteropReason {
    /// State why compat interop is unavoidable here.
    #[must_use]
    pub const fn new(reason: &'static str) -> Self {
        Self(reason)
    }

    /// The stated justification.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        self.0
    }
}

/// Proof that a [`Recipient<Hpke>`] came from a verified
/// [`RecipientAttestation`]; not constructible outside this crate.
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
/// use nectar_envelope::{Compat, Hpke, Recipient, Scheme, Topic};
///
/// fn downgrade(recipient: &Recipient<Hpke>, topic: &Topic) {
///     // compat seal toward an hpke recipient must not typecheck
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

/// An envelope payload was shorter than the frame header.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("envelope too short: {got} bytes, header is {HEADER_SIZE}")]
pub struct EnvelopeTooShort {
    /// Observed payload length.
    pub got: usize,
}

/// Parsed view of the frozen frame.
#[derive(Debug, Clone, Copy)]
pub struct Envelope<'a> {
    hint: &'a [u8; HINT_SIZE],
    nonce: &'a [u8; NONCE_SIZE],
    enc_x: &'a [u8; ENC_X_SIZE],
    ct: &'a [u8],
}

impl<'a> Envelope<'a> {
    /// Split a chunk payload into the frame fields.
    pub const fn parse(payload: &'a [u8]) -> Result<Self, EnvelopeTooShort> {
        let got = payload.len();
        let Some((hint, rest)) = payload.split_first_chunk::<HINT_SIZE>() else {
            return Err(EnvelopeTooShort { got });
        };
        let Some((nonce, rest)) = rest.split_first_chunk::<NONCE_SIZE>() else {
            return Err(EnvelopeTooShort { got });
        };
        let Some((enc_x, ct)) = rest.split_first_chunk::<ENC_X_SIZE>() else {
            return Err(EnvelopeTooShort { got });
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
    /// Recovered plaintext. HPKE: the exact message; compat: the whole
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

/// Errors from opening an envelope that was recognized as ours.
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

/// A sealed envelope ready for framing into a chunk payload.
pub struct SealedEnvelope<S: Scheme> {
    hint: [u8; HINT_SIZE],
    nonce: [u8; NONCE_SIZE],
    enc_x: [u8; ENC_X_SIZE],
    ct: Vec<u8>,
    extra: S::Extra,
}

impl<S: Scheme> SealedEnvelope<S> {
    #[cfg(any(test, feature = "encryption"))]
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

impl<S: Scheme> fmt::Debug for SealedEnvelope<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SealedEnvelope")
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
    /// Whether compat probes are admitted after the HPKE pass.
    const COMPAT: bool;
}

/// HPKE-only delivery: compat hints are never computed, so compat envelopes
/// are undeliverable outright.
#[derive(Debug, Clone, Copy)]
pub enum HpkeOnly {}

impl Policy for HpkeOnly {
    const COMPAT: bool = false;
}

/// Strictly transitional delivery: HPKE first, compat probes after.
/// Forbidden for peers pinned HPKE-capable: compat is an unauthenticated
/// stream cipher. Neither scheme authenticates the sender: HPKE runs base mode.
#[derive(Debug, Clone, Copy)]
pub enum HpkeThenCompat {}

impl Policy for HpkeThenCompat {
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

impl Inbox<HpkeOnly> {
    /// HPKE-only inbox for `secret`.
    #[must_use]
    pub const fn new(secret: SecretKey) -> Self {
        Self {
            secret,
            _policy: PhantomData,
        }
    }
}

impl Inbox<HpkeThenCompat> {
    /// Transitional inbox for `secret`; flip to [`HpkeOnly`] the moment
    /// the peer set is confirmed HPKE-capable.
    #[must_use]
    pub const fn transitional(secret: SecretKey) -> Self {
        Self {
            secret,
            _policy: PhantomData,
        }
    }
}

impl<P: Policy> Inbox<P> {
    /// Trial-open `envelope` against `topics`: one decap, an HPKE hint
    /// probe per topic, then compat probes where the policy admits them.
    pub fn open(
        &self,
        topics: &[Topic],
        envelope: &Envelope<'_>,
    ) -> Result<Option<(Topic, InboxOpened)>, OpenError> {
        if let Some(shared) = hpke::decap(&self.secret, envelope)? {
            for topic in topics {
                if let Some(opened) = hpke::open_with_shared(&shared, topic, envelope)? {
                    return Ok(Some((*topic, InboxOpened::Hpke(opened))));
                }
            }
        }
        if P::COMPAT {
            for topic in topics {
                if let Some(opened) = Compat::open(&self.secret, topic.into(), envelope)? {
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
    /// Opened under HPKE.
    Hpke(Opened<Hpke>),
    /// Opened under compat.
    Compat(Opened<Compat>),
}

impl InboxOpened {
    /// Which scheme delivered.
    #[must_use]
    pub const fn scheme(&self) -> SchemeId {
        match self {
            Self::Hpke(_) => SchemeId::Hpke,
            Self::Compat(_) => SchemeId::Compat,
        }
    }
}

/// Edge-only recipient union for heterogeneous collections; matches once
/// and calls the monomorphized path, never enters the seam.
#[derive(Debug, Clone)]
pub enum AnyRecipient {
    /// An attested HPKE recipient.
    Hpke(Recipient<Hpke>),
    /// A justified compat recipient.
    Compat(Recipient<Compat>),
}

/// Erased sealed envelope from [`AnyRecipient::seal`].
#[derive(Debug)]
pub enum AnySealed {
    /// Sealed under HPKE.
    Hpke(SealedEnvelope<Hpke>),
    /// Sealed under compat.
    Compat(SealedEnvelope<Compat>),
}

impl AnySealed {
    /// Which scheme sealed.
    #[must_use]
    pub const fn scheme(&self) -> SchemeId {
        match self {
            Self::Hpke(_) => SchemeId::Hpke,
            Self::Compat(_) => SchemeId::Compat,
        }
    }

    /// Serialize the frozen frame.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Self::Hpke(envelope) => envelope.to_bytes(),
            Self::Compat(envelope) => envelope.to_bytes(),
        }
    }
}

/// Errors from [`AnyRecipient::seal`].
#[derive(Debug, Error)]
pub enum AnySealError {
    /// The HPKE seal failed.
    #[error(transparent)]
    Hpke(#[from] HpkeSealError),
    /// The compat seal failed.
    #[error(transparent)]
    Compat(#[from] EciesError),
}

#[cfg(any(test, feature = "encryption"))]
impl AnyRecipient {
    /// Seal toward this recipient, scheme inferred from the handle.
    pub fn seal(
        &self,
        topic: &Topic,
        plaintext: &[u8],
        ct_len: usize,
    ) -> Result<AnySealed, AnySealError> {
        match self {
            Self::Hpke(recipient) => Ok(AnySealed::Hpke(Hpke::seal(
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
#[cfg(any(test, feature = "encryption"))]
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

    #[cfg(any(test, feature = "encryption"))]
    fn seal(
        recipient: &Recipient<Self>,
        ctx: Self::Context<'_>,
        plaintext: &[u8],
        ct_len: usize,
    ) -> Result<SealedEnvelope<Self>, Self::SealError> {
        compat::seal(recipient, ctx, plaintext, ct_len)
    }

    fn open(
        secret: &SecretKey,
        ctx: Self::Context<'_>,
        envelope: &Envelope<'_>,
    ) -> Result<Option<Opened<Self>>, OpenError> {
        compat::open(secret, ctx, envelope)
    }
}

impl Scheme for Hpke {
    type Context<'a> = Info<'a>;
    type Extra = Exporter;
    type Provenance = Attested;
    type SealError = HpkeSealError;

    const TAG: usize = 16;
    const ID: SchemeId = SchemeId::Hpke;

    #[cfg(any(test, feature = "encryption"))]
    fn seal(
        recipient: &Recipient<Self>,
        ctx: Self::Context<'_>,
        plaintext: &[u8],
        ct_len: usize,
    ) -> Result<SealedEnvelope<Self>, Self::SealError> {
        hpke::seal(recipient, ctx, plaintext, ct_len)
    }

    fn open(
        secret: &SecretKey,
        ctx: Self::Context<'_>,
        envelope: &Envelope<'_>,
    ) -> Result<Option<Opened<Self>>, OpenError> {
        hpke::open(secret, ctx, envelope)
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

    fn hpke_recipient(key: PublicKey) -> Recipient<Hpke> {
        Recipient::attested(key)
    }

    fn compat_recipient(key: PublicKey) -> Recipient<Compat> {
        Recipient::assume_reference(key, InteropReason::new("seam test"))
    }

    #[test]
    fn hpke_roundtrip_via_seam() {
        let (secret, public) = keys(b"nectar envelope seam recipient");
        let topic = topic();
        let msg = b"seam roundtrip";
        let sealed = Hpke::seal(&hpke_recipient(public), (&topic).into(), msg, 4024).unwrap();
        let bytes = sealed.to_bytes();
        assert_eq!(bytes.len(), HEADER_SIZE + 4024);

        let envelope = Envelope::parse(&bytes).unwrap();
        let opened = Hpke::open(&secret, (&topic).into(), &envelope)
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
    fn hpke_wrong_topic_is_undeliverable() {
        let (secret, public) = keys(b"nectar envelope seam recipient");
        let sealed = Hpke::seal(&hpke_recipient(public), (&topic()).into(), b"m", 64).unwrap();
        let bytes = sealed.to_bytes();
        let envelope = Envelope::parse(&bytes).unwrap();
        let other = Topic::new(keccak256(b"other topic").0);
        assert!(
            Hpke::open(&secret, (&other).into(), &envelope)
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

        let envelope = Envelope::parse(&bytes).unwrap();
        let opened = Compat::open(&secret, (&topic).into(), &envelope)
            .unwrap()
            .unwrap();
        assert_eq!(&opened.plaintext[..msg.len()], msg);
        assert_eq!(opened.plaintext.len(), 100);

        // The adapter is byte-for-byte the frozen construction.
        let ephemeral = reconstruct(envelope.enc_x(), envelope.parity()).unwrap();
        let key = ecies::shared_key(&secret, &ephemeral, (&topic).into());
        assert_eq!(key, opened.extra);
        assert_eq!(
            ecies::Hint::derive(&key, (&topic).into()).as_bytes(),
            envelope.hint()
        );
        assert_eq!(
            ecies::decrypt(&key, envelope.ciphertext()),
            opened.plaintext
        );
    }

    #[test]
    fn schemes_never_cross_open() {
        let (secret, public) = keys(b"nectar envelope seam recipient");
        let topic = topic();

        let sealed = Hpke::seal(&hpke_recipient(public), (&topic).into(), b"e", 64).unwrap();
        let bytes = sealed.to_bytes();
        let envelope = Envelope::parse(&bytes).unwrap();
        assert!(
            Compat::open(&secret, (&topic).into(), &envelope)
                .unwrap()
                .is_none()
        );

        let sealed = Compat::seal(&compat_recipient(public), (&topic).into(), b"c", 64).unwrap();
        let bytes = sealed.to_bytes();
        let envelope = Envelope::parse(&bytes).unwrap();
        assert!(
            Hpke::open(&secret, (&topic).into(), &envelope)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn hpke_only_inbox_rejects_compat_forgeries() {
        let (secret, public) = keys(b"nectar envelope seam recipient");
        let topic = topic();
        let sealed = Compat::seal(&compat_recipient(public), (&topic).into(), b"f", 64).unwrap();
        let bytes = sealed.to_bytes();
        let envelope = Envelope::parse(&bytes).unwrap();

        let inbox = Inbox::<HpkeOnly>::new(secret.clone());
        assert!(inbox.open(&[topic], &envelope).unwrap().is_none());

        // The transitional inbox still delivers it, reported as compat.
        let inbox = Inbox::<HpkeThenCompat>::transitional(secret);
        let (delivered_topic, opened) = inbox.open(&[topic], &envelope).unwrap().unwrap();
        assert_eq!(delivered_topic, topic);
        assert_eq!(opened.scheme(), SchemeId::Compat);
    }

    #[test]
    fn inbox_delivers_hpke_on_the_right_topic() {
        let (secret, public) = keys(b"nectar envelope seam recipient");
        let topic = topic();
        let decoy = Topic::new(keccak256(b"decoy topic").0);
        let sealed = Hpke::seal(&hpke_recipient(public), (&topic).into(), b"m", 64).unwrap();
        let bytes = sealed.to_bytes();
        let envelope = Envelope::parse(&bytes).unwrap();

        let inbox = Inbox::<HpkeOnly>::new(secret);
        let (delivered_topic, opened) = inbox.open(&[decoy, topic], &envelope).unwrap().unwrap();
        assert_eq!(delivered_topic, topic);
        assert_eq!(opened.scheme(), SchemeId::Hpke);
        let InboxOpened::Hpke(opened) = opened else {
            panic!("wrong scheme");
        };
        assert_eq!(opened.plaintext, b"m");
    }

    #[test]
    fn remining_the_nonce_keeps_the_envelope_open() {
        let (secret, public) = keys(b"nectar envelope seam recipient");
        let topic = topic();
        let mut sealed = Hpke::seal(&hpke_recipient(public), (&topic).into(), b"m", 64).unwrap();
        let parity_before = {
            let bytes = sealed.to_bytes();
            bytes[8] & 1
        };
        sealed.set_nonce([0xff; 32]);
        let bytes = sealed.to_bytes();
        assert_eq!(bytes[8] & 1, parity_before);
        assert_eq!(&bytes[9..40], &[0xff; 31]);

        let envelope = Envelope::parse(&bytes).unwrap();
        assert!(
            Hpke::open(&secret, (&topic).into(), &envelope)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn flipped_parity_is_dos_only_for_hpke() {
        let (secret, public) = keys(b"nectar envelope seam recipient");
        let topic = topic();

        let sealed = Hpke::seal(&hpke_recipient(public), (&topic).into(), b"m", 64).unwrap();
        let mut bytes = sealed.to_bytes();
        bytes[8] ^= 1;
        let envelope = Envelope::parse(&bytes).unwrap();
        assert!(
            Hpke::open(&secret, (&topic).into(), &envelope)
                .unwrap()
                .is_none()
        );

        // Compat ECDH ignores parity: the flipped envelope still opens.
        let sealed = Compat::seal(&compat_recipient(public), (&topic).into(), b"m", 64).unwrap();
        let mut bytes = sealed.to_bytes();
        bytes[8] ^= 1;
        let envelope = Envelope::parse(&bytes).unwrap();
        assert!(
            Compat::open(&secret, (&topic).into(), &envelope)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn envelope_parse_rejects_short_payloads() {
        assert_eq!(
            Envelope::parse(&[0u8; HEADER_SIZE - 1]).unwrap_err(),
            EnvelopeTooShort {
                got: HEADER_SIZE - 1
            }
        );
        let envelope = Envelope::parse(&[0u8; HEADER_SIZE]).unwrap();
        assert!(envelope.ciphertext().is_empty());
    }

    #[test]
    fn any_recipient_seals_via_the_monomorphized_paths() {
        let (secret, public) = keys(b"nectar envelope seam recipient");
        let topic = topic();

        let any = AnyRecipient::Hpke(hpke_recipient(public));
        let sealed = any.seal(&topic, b"m", 64).unwrap();
        assert_eq!(sealed.scheme(), SchemeId::Hpke);
        let bytes = sealed.to_bytes();
        let envelope = Envelope::parse(&bytes).unwrap();
        assert!(
            Hpke::open(&secret, (&topic).into(), &envelope)
                .unwrap()
                .is_some()
        );

        let any = AnyRecipient::Compat(compat_recipient(public));
        let sealed = any.seal(&topic, b"m", 64).unwrap();
        assert_eq!(sealed.scheme(), SchemeId::Compat);
        let bytes = sealed.to_bytes();
        let envelope = Envelope::parse(&bytes).unwrap();
        assert!(
            Compat::open(&secret, (&topic).into(), &envelope)
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

    /// Frozen wire: the crate name must never leak into the format.
    #[test]
    fn frozen_wire_constants() {
        assert_eq!(LABEL, b"nectar/env/v1");
        assert_eq!(ATTESTATION_PREFIX, b"nectar/env/v1 attest");
        assert_eq!((HINT_SIZE, NONCE_SIZE, ENC_X_SIZE), (8, 32, 32));
        assert_eq!(HEADER_SIZE, 72);
    }

    #[test]
    fn scheme_constants() {
        assert_eq!(Compat::TAG, 0);
        assert_eq!(Hpke::TAG, 16);
        assert_eq!(Compat::ID, SchemeId::Compat);
        assert_eq!(Hpke::ID, SchemeId::Hpke);
    }
}
