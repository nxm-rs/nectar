//! Sealed envelopes over the frozen ecies compat baseline.
//!
//! Two sealed schemes share one frozen frame inside the chunk payload:
//! `hint[0..8] || nonce[8..40] || tail[40..]`. Only the hint and the nonce
//! are frozen, because they are the fields the network itself touches: topic
//! gating and mining. The tail is scheme-owned opaque bytes, and both
//! shipped schemes lay it out as `enc_x || ct`. Compat is the byte-frozen
//! [`crate::ecies`] construction; HPKE is RFC 9180 with the single suite
//! DHKEM(secp256k1, HKDF-SHA256) + HKDF-SHA256 + ChaCha20-Poly1305 under the
//! domain label `nectar/env/v1`.
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
//! parity in both schemes (miners hold it fixed, see [`Scheme::NONCE_KEEP`]).
//! HPKE reconstructs canonical uncompressed SEC1 before `kem_context`;
//! compat ECDH ignores parity, so a flipped bit is an authenticated decap
//! failure (DoS only) under HPKE and a no-op for compat. Envelopes of both
//! schemes are curve-membership-detectable against uniform bytes: the
//! anonymity set is trojan chunks, at exact parity with the deployed compat
//! path.
//!
//! The reference client places that bit elsewhere: it mines `nonce[28]` and
//! reads it back as `chunkData[36]`. Neither side breaks, because compat ECDH
//! is x-only, and its reader forces odd regardless (`chunkData[36] | 0x1 != 0`
//! is a bitwise or, so always true). It matters only to a future scheme that
//! makes parity load-bearing on the compat frame.
//!
//! Trial order: [`Scheme::decap`] once per envelope per scheme (after chunk
//! validity and proof-of-work checks; that per-valid-chunk cost is
//! inherent), one [`Scheme::probe`] per candidate topic, AEAD open only on a
//! hint match. HKDF-SHA256 deliberately adds SHA-256 beside the otherwise
//! keccak-only proving surface.

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

use k256::SecretKey;
use nectar_primitives::chunk::encryption::EncryptionKey;
use subtle::ConstantTimeEq;
use thiserror::Error;

use crate::ecies::Salt;

mod attestation;
mod compat;
mod ecdh;
pub mod ecies;
mod hpke;

pub use attestation::{
    ATTESTATION_PREFIX, AttestationError, Attested, AttestedRecipient, AttestedScheme,
    RecipientAttestation, SchemeEntry, VerifiedAttestation, sign_data,
};
pub use compat::CompatSealError;
pub use hpke::{
    DeriveKeyPairError, ExportError, Exporter, HpkeSealError, Info, Shared, derive_key_pair,
};

/// Domain label of the HPKE suite.
pub const LABEL: &[u8] = b"nectar/env/v1";

/// Byte length of the hint slot.
pub const HINT_SIZE: usize = 8;
/// Byte length of the mining nonce slot.
pub const NONCE_SIZE: usize = 32;
/// Byte length of the frozen header preceding the scheme-owned tail.
pub const HEADER_SIZE: usize = HINT_SIZE + NONCE_SIZE;

mod sealed {
    pub trait Sealed {}
    impl Sealed for super::Refuse {}
    impl<S: super::Scheme, Rest: super::Policy> Sealed for super::Try<S, Rest> {}
}

/// Declare the scheme list once; the edge unions follow from it.
macro_rules! schemes {
    ($($scheme:ident => $seal_error:ty),+ $(,)?) => {
        /// Scheme label reported in open results; never a dispatcher.
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum SchemeId {
            $(
                #[doc = concat!("[`", stringify!($scheme), "`].")]
                $scheme,
            )+
        }

        $(impl sealed::Sealed for $scheme {})+

        /// Erased inbox delivery; match once at the edge.
        #[derive(Debug)]
        pub enum InboxOpened {
            $(
                #[doc = concat!("Opened under [`", stringify!($scheme), "`].")]
                $scheme(Opened<$scheme>),
            )+
        }

        impl InboxOpened {
            /// Which scheme delivered.
            #[must_use]
            pub const fn scheme(&self) -> SchemeId {
                match self {
                    $(Self::$scheme(_) => SchemeId::$scheme,)+
                }
            }
        }

        /// Edge-only recipient union for heterogeneous collections; matches
        /// once and calls the monomorphized path, never enters the seam.
        #[derive(Debug, Clone)]
        pub enum AnyRecipient {
            $(
                #[doc = concat!("A [`", stringify!($scheme), "`] recipient.")]
                $scheme(Recipient<$scheme>),
            )+
        }

        /// Erased sealed envelope from [`AnyRecipient::seal`].
        #[derive(Debug)]
        pub enum AnySealed {
            $(
                #[doc = concat!("Sealed under [`", stringify!($scheme), "`].")]
                $scheme(SealedEnvelope<$scheme>),
            )+
        }

        impl AnySealed {
            /// Which scheme sealed.
            #[must_use]
            pub const fn scheme(&self) -> SchemeId {
                match self {
                    $(Self::$scheme(_) => SchemeId::$scheme,)+
                }
            }

            /// Serialize the frozen frame.
            #[must_use]
            pub fn to_bytes(&self) -> Vec<u8> {
                match self {
                    $(Self::$scheme(envelope) => envelope.to_bytes(),)+
                }
            }
        }

        /// Errors from [`AnyRecipient::seal`].
        #[non_exhaustive]
        #[derive(Debug, Error)]
        pub enum AnySealError {
            $(
                #[doc = concat!("The [`", stringify!($scheme), "`] seal failed.")]
                #[error(transparent)]
                $scheme(#[from] $seal_error),
            )+
        }

        #[cfg(any(test, feature = "encryption"))]
        impl AnyRecipient {
            /// Seal toward this recipient, scheme inferred from the handle.
            pub fn seal(
                &self,
                topic: &Topic,
                plaintext: &[u8],
                tail_len: usize,
            ) -> Result<AnySealed, AnySealError> {
                match self {
                    $(Self::$scheme(recipient) => Ok(AnySealed::$scheme(
                        $scheme::seal(recipient, topic.into(), plaintext, tail_len)?,
                    )),)+
                }
            }
        }
    };
}

schemes! {
    Hpke => HpkeSealError,
    Compat => CompatSealError,
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

/// One of the sealing schemes; sealed, dispatch rides the type.
///
/// Admission rule for a new scheme: its tail must be indistinguishable from
/// the existing tail distribution to an observer without the recipient key,
/// and the hint stays 8 bytes, or the anonymity set forks.
pub trait Scheme: sealed::Sealed + Sized {
    /// Key an envelope is sealed toward.
    type PublicKey: Clone + fmt::Debug;
    /// Key an envelope is opened with.
    type SecretKey;
    /// Per-scheme key-derivation context built from a topic.
    type Context<'a>: From<&'a Topic>;
    /// Topic-independent product of one decapsulation.
    type Decap;
    /// Scheme-specific by-product of a seal or open.
    type Extra: fmt::Debug;
    /// Proof carried by a [`Recipient`] of how it was minted.
    type Provenance: Clone + fmt::Debug;
    /// Errors from sealing.
    type SealError: core::error::Error;

    /// Report label for open results.
    const ID: SchemeId;
    /// Tail bytes that carry no payload: encapsulation plus any framing.
    const OVERHEAD: usize;
    /// Bits of `nonce[0]` the scheme owns; a miner keeps them fixed.
    const NONCE_KEEP: u8;

    /// Seal `plaintext` into a `tail_len`-byte tail, which carries at most
    /// `tail_len - OVERHEAD` message bytes.
    #[cfg(any(test, feature = "encryption"))]
    fn seal(
        recipient: &Recipient<Self>,
        ctx: Self::Context<'_>,
        plaintext: &[u8],
        tail_len: usize,
    ) -> Result<SealedEnvelope<Self>, Self::SealError>;

    /// Decapsulate once per envelope. `Ok(None)` rejects the tail outright.
    fn decap(
        secret: &Self::SecretKey,
        envelope: &Envelope<'_>,
    ) -> Result<Option<Self::Decap>, OpenError>;

    /// Probe one context against an established decapsulation. `Ok(None)`
    /// means the envelope is not addressed under this scheme and context.
    fn probe(
        decap: &Self::Decap,
        ctx: Self::Context<'_>,
        envelope: &Envelope<'_>,
    ) -> Result<Option<Opened<Self>>, OpenError>;

    /// Inject into the edge union.
    fn erase(opened: Opened<Self>) -> InboxOpened;

    /// Decap then probe for a single context.
    fn open(
        secret: &Self::SecretKey,
        ctx: Self::Context<'_>,
        envelope: &Envelope<'_>,
    ) -> Result<Option<Opened<Self>>, OpenError> {
        let Some(decap) = Self::decap(secret, envelope)? else {
            return Ok(None);
        };
        Self::probe(&decap, ctx, envelope)
    }
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

/// Sealing handle for one recipient key under one scheme.
///
/// Upgrade is one-way: no conversion between schemes exists in either
/// direction.
///
/// ```compile_fail
/// use nectar_envelope::{Compat, Hpke, Recipient};
///
/// fn downgrade(recipient: Recipient<Hpke>) -> Recipient<Compat> {
///     recipient
/// }
/// ```
#[derive(Clone)]
pub struct Recipient<S: Scheme> {
    key: S::PublicKey,
    provenance: S::Provenance,
}

impl<S: Scheme> Recipient<S> {
    /// The recipient public key.
    #[must_use]
    pub const fn key(&self) -> &S::PublicKey {
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
    pub const fn assume_reference(key: k256::PublicKey, reason: InteropReason) -> Self {
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
    pub(crate) const fn attested(key: k256::PublicKey, provenance: Attested) -> Self {
        Self { key, provenance }
    }

    /// How this recipient was attested.
    #[must_use]
    pub const fn provenance(&self) -> &Attested {
        &self.provenance
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
    tail: &'a [u8],
}

impl<'a> Envelope<'a> {
    /// Split a chunk payload into the frozen header and the scheme tail.
    pub const fn parse(payload: &'a [u8]) -> Result<Self, EnvelopeTooShort> {
        let got = payload.len();
        let Some((hint, rest)) = payload.split_first_chunk::<HINT_SIZE>() else {
            return Err(EnvelopeTooShort { got });
        };
        let Some((nonce, tail)) = rest.split_first_chunk::<NONCE_SIZE>() else {
            return Err(EnvelopeTooShort { got });
        };
        Ok(Self { hint, nonce, tail })
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

    /// The scheme-owned tail.
    #[must_use]
    pub const fn tail(&self) -> &'a [u8] {
        self.tail
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
    tail: Vec<u8>,
    extra: S::Extra,
}

impl<S: Scheme> SealedEnvelope<S> {
    #[cfg(any(test, feature = "encryption"))]
    pub(crate) const fn from_parts(
        hint: [u8; HINT_SIZE],
        nonce_bits: u8,
        tail: Vec<u8>,
        extra: S::Extra,
    ) -> Self {
        let mut nonce = [0u8; NONCE_SIZE];
        let [first, ..] = &mut nonce;
        *first = nonce_bits & S::NONCE_KEEP;
        Self {
            hint,
            nonce,
            tail,
            extra,
        }
    }

    /// Replace the mining nonce; the scheme-owned bits are reasserted.
    pub const fn set_nonce(&mut self, mut nonce: [u8; NONCE_SIZE]) {
        let [current, ..] = &self.nonce;
        let kept = *current & S::NONCE_KEEP;
        let [first, ..] = &mut nonce;
        *first = (*first & !S::NONCE_KEEP) | kept;
        self.nonce = nonce;
    }

    /// Serialize the frozen frame.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(HEADER_SIZE.saturating_add(self.tail.len()));
        out.extend_from_slice(&self.hint);
        out.extend_from_slice(&self.nonce);
        out.extend_from_slice(&self.tail);
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
            .field("tail_len", &self.tail.len())
            .finish_non_exhaustive()
    }
}

/// Constant-time hint comparison.
pub(crate) fn hint_matches(derived: &[u8; HINT_SIZE], carried: &[u8; HINT_SIZE]) -> bool {
    derived.ct_eq(carried).into()
}

/// Delivery policy of an [`Inbox`]: a keyed cons list of schemes to try, in
/// order, each cell owning its own scheme's secret.
pub trait Policy: sealed::Sealed + fmt::Debug {
    /// Try each cell in turn; at most one decapsulation per scheme.
    fn deliver(
        &self,
        topics: &[Topic],
        envelope: &Envelope<'_>,
    ) -> Result<Option<(Topic, InboxOpened)>, OpenError>;
}

/// Terminal cell: matches nothing.
#[derive(Debug, Clone, Copy)]
pub struct Refuse;

impl Policy for Refuse {
    fn deliver(
        &self,
        _topics: &[Topic],
        _envelope: &Envelope<'_>,
    ) -> Result<Option<(Topic, InboxOpened)>, OpenError> {
        Ok(None)
    }
}

/// Cons cell: try `S` under its own secret, then `Rest`.
pub struct Try<S: Scheme, Rest: Policy = Refuse> {
    secret: S::SecretKey,
    rest: Rest,
}

impl<S: Scheme> Try<S> {
    /// A single-scheme policy.
    #[must_use]
    pub const fn new(secret: S::SecretKey) -> Self {
        Self {
            secret,
            rest: Refuse,
        }
    }
}

impl<S: Scheme, Rest: Policy> Try<S, Rest> {
    /// Prepend `S` to an existing policy.
    #[must_use]
    pub const fn with(secret: S::SecretKey, rest: Rest) -> Self {
        Self { secret, rest }
    }
}

impl Try<Hpke> {
    /// Append the compat cell.
    #[must_use]
    pub fn then_compat(self, secret: SecretKey) -> HpkeThenCompat {
        Try::with(self.secret, Try::new(secret))
    }
}

impl<S: Scheme, Rest: Policy> fmt::Debug for Try<S, Rest> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Try")
            .field("scheme", &S::ID)
            .field("rest", &self.rest)
            .finish_non_exhaustive()
    }
}

impl<S: Scheme, Rest: Policy> Policy for Try<S, Rest> {
    fn deliver(
        &self,
        topics: &[Topic],
        envelope: &Envelope<'_>,
    ) -> Result<Option<(Topic, InboxOpened)>, OpenError> {
        if let Some(decap) = S::decap(&self.secret, envelope)? {
            for topic in topics {
                if let Some(opened) = S::probe(&decap, topic.into(), envelope)? {
                    return Ok(Some((*topic, S::erase(opened))));
                }
            }
        }
        self.rest.deliver(topics, envelope)
    }
}

/// HPKE-only delivery: compat hints are never computed, so compat envelopes
/// are undeliverable outright.
pub type HpkeOnly = Try<Hpke>;

/// Strictly transitional delivery: HPKE first, compat probes after.
/// Forbidden for peers pinned HPKE-capable: compat is an unauthenticated
/// stream cipher. Neither scheme authenticates the sender: HPKE runs base mode.
pub type HpkeThenCompat = Try<Hpke, Try<Compat>>;

/// Trial-decrypt driver for one delivery policy.
#[derive(Debug)]
pub struct Inbox<P: Policy> {
    policy: P,
}

impl Inbox<HpkeOnly> {
    /// HPKE-only inbox for `secret`.
    #[must_use]
    pub const fn new(secret: SecretKey) -> Self {
        Self {
            policy: Try::new(secret),
        }
    }
}

impl Inbox<HpkeThenCompat> {
    /// Transitional inbox; flip to [`HpkeOnly`] the moment the peer set is
    /// confirmed HPKE-capable.
    #[must_use]
    pub fn transitional(hpke: SecretKey, compat: SecretKey) -> Self {
        Self {
            policy: Try::new(hpke).then_compat(compat),
        }
    }
}

impl<P: Policy> Inbox<P> {
    /// Drive an arbitrary policy.
    #[must_use]
    pub const fn with_policy(policy: P) -> Self {
        Self { policy }
    }

    /// Trial-open `envelope` against `topics` under the policy order.
    pub fn open(
        &self,
        topics: &[Topic],
        envelope: &Envelope<'_>,
    ) -> Result<Option<(Topic, InboxOpened)>, OpenError> {
        self.policy.deliver(topics, envelope)
    }
}

/// Decapsulation counter behind the decap-once law.
#[cfg(test)]
pub(crate) mod decaps {
    use super::SchemeId;
    use core::cell::Cell;

    std::thread_local! {
        static COUNT: Cell<(usize, usize)> = const { Cell::new((0, 0)) };
    }

    pub(crate) fn note(id: SchemeId) {
        COUNT.with(|count| {
            let (hpke, compat) = count.get();
            count.set(match id {
                SchemeId::Hpke => (hpke + 1, compat),
                SchemeId::Compat => (hpke, compat + 1),
            });
        });
    }

    /// Counts since the last call, as `(hpke, compat)`.
    pub(crate) fn take() -> (usize, usize) {
        COUNT.with(|count| count.replace((0, 0)))
    }
}

impl Scheme for Compat {
    type PublicKey = k256::PublicKey;
    type SecretKey = SecretKey;
    type Context<'a> = Salt<'a>;
    type Decap = ecies::SharedX;
    type Extra = EncryptionKey;
    type Provenance = InteropReason;
    type SealError = CompatSealError;

    const ID: SchemeId = SchemeId::Compat;
    const OVERHEAD: usize = ecdh::ENC_X_SIZE;
    const NONCE_KEEP: u8 = ecdh::PARITY;

    #[cfg(any(test, feature = "encryption"))]
    fn seal(
        recipient: &Recipient<Self>,
        ctx: Self::Context<'_>,
        plaintext: &[u8],
        tail_len: usize,
    ) -> Result<SealedEnvelope<Self>, Self::SealError> {
        compat::seal(recipient, ctx, plaintext, tail_len)
    }

    fn decap(
        secret: &Self::SecretKey,
        envelope: &Envelope<'_>,
    ) -> Result<Option<Self::Decap>, OpenError> {
        compat::decap(secret, envelope)
    }

    fn probe(
        decap: &Self::Decap,
        ctx: Self::Context<'_>,
        envelope: &Envelope<'_>,
    ) -> Result<Option<Opened<Self>>, OpenError> {
        compat::probe(decap, ctx, envelope)
    }

    fn erase(opened: Opened<Self>) -> InboxOpened {
        InboxOpened::Compat(opened)
    }
}

impl Scheme for Hpke {
    type PublicKey = k256::PublicKey;
    type SecretKey = SecretKey;
    type Context<'a> = Info<'a>;
    type Decap = Shared;
    type Extra = Exporter;
    type Provenance = Attested;
    type SealError = HpkeSealError;

    const ID: SchemeId = SchemeId::Hpke;
    const OVERHEAD: usize = hpke::OVERHEAD;
    const NONCE_KEEP: u8 = ecdh::PARITY;

    #[cfg(any(test, feature = "encryption"))]
    fn seal(
        recipient: &Recipient<Self>,
        ctx: Self::Context<'_>,
        plaintext: &[u8],
        tail_len: usize,
    ) -> Result<SealedEnvelope<Self>, Self::SealError> {
        hpke::seal(recipient, ctx, plaintext, tail_len)
    }

    fn decap(
        secret: &Self::SecretKey,
        envelope: &Envelope<'_>,
    ) -> Result<Option<Self::Decap>, OpenError> {
        hpke::decap(secret, envelope)
    }

    fn probe(
        decap: &Self::Decap,
        ctx: Self::Context<'_>,
        envelope: &Envelope<'_>,
    ) -> Result<Option<Opened<Self>>, OpenError> {
        hpke::probe(decap, ctx, envelope)
    }

    fn erase(opened: Opened<Self>) -> InboxOpened {
        InboxOpened::Hpke(opened)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::keccak256;
    use k256::PublicKey;

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
        Recipient::attested(key, Attested::new(AttestedScheme::Hpke, 0))
    }

    fn compat_recipient(key: PublicKey) -> Recipient<Compat> {
        Recipient::assume_reference(key, InteropReason::new("seam test"))
    }

    #[test]
    fn hpke_roundtrip_via_seam() {
        let (secret, public) = keys(b"nectar envelope seam recipient");
        let topic = topic();
        let msg = b"seam roundtrip";
        let sealed = Hpke::seal(&hpke_recipient(public), (&topic).into(), msg, 4056).unwrap();
        let bytes = sealed.to_bytes();
        assert_eq!(bytes.len(), HEADER_SIZE + 4056);

        let envelope = Envelope::parse(&bytes).unwrap();
        let opened = Hpke::open(&secret, (&topic).into(), &envelope)
            .unwrap()
            .unwrap();
        assert_eq!(opened.plaintext, msg);

        let mut sender = [0u8; 16];
        sealed.extra().export(b"reply", &mut sender).unwrap();
        let mut receiver = [0u8; 16];
        opened.extra.export(b"reply", &mut receiver).unwrap();
        assert_eq!(sender, receiver);
    }

    /// `OVERHEAD` is the whole capacity handle: a tail carries exactly
    /// `tail_len - OVERHEAD` message bytes, one more is refused.
    #[test]
    fn overhead_bounds_the_tail_capacity() {
        let (_, public) = keys(b"nectar envelope seam recipient");
        let topic = topic();
        for tail_len in [96usize, 512, 4056] {
            let capacity = tail_len - Hpke::OVERHEAD;
            let sealed = Hpke::seal(
                &hpke_recipient(public),
                (&topic).into(),
                &vec![7u8; capacity],
                tail_len,
            )
            .unwrap();
            assert_eq!(sealed.to_bytes().len(), HEADER_SIZE + tail_len);
            assert!(
                Hpke::seal(
                    &hpke_recipient(public),
                    (&topic).into(),
                    &vec![7u8; capacity + 1],
                    tail_len
                )
                .is_err()
            );

            let capacity = tail_len - Compat::OVERHEAD;
            let sealed = Compat::seal(
                &compat_recipient(public),
                (&topic).into(),
                &vec![7u8; capacity],
                tail_len,
            )
            .unwrap();
            assert_eq!(sealed.to_bytes().len(), HEADER_SIZE + tail_len);
            assert!(
                Compat::seal(
                    &compat_recipient(public),
                    (&topic).into(),
                    &vec![7u8; capacity + 1],
                    tail_len
                )
                .is_err()
            );
        }
    }

    /// A tail below the scheme overhead is refused, never truncated.
    #[test]
    fn a_tail_below_the_overhead_is_refused() {
        let (_, public) = keys(b"nectar envelope seam recipient");
        let topic = topic();
        assert!(Hpke::seal(&hpke_recipient(public), (&topic).into(), b"", 49).is_err());
        assert!(Compat::seal(&compat_recipient(public), (&topic).into(), b"", 31).is_err());
    }

    #[test]
    fn hpke_wrong_topic_is_undeliverable() {
        let (secret, public) = keys(b"nectar envelope seam recipient");
        let sealed = Hpke::seal(&hpke_recipient(public), (&topic()).into(), b"m", 96).unwrap();
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
        let sealed = Compat::seal(&compat_recipient(public), (&topic).into(), msg, 132).unwrap();
        let bytes = sealed.to_bytes();
        assert_eq!(bytes.len(), HEADER_SIZE + 132);

        let envelope = Envelope::parse(&bytes).unwrap();
        let opened = Compat::open(&secret, (&topic).into(), &envelope)
            .unwrap()
            .unwrap();
        assert_eq!(&opened.plaintext[..msg.len()], msg);
        assert_eq!(opened.plaintext.len(), 132 - Compat::OVERHEAD);

        // The adapter is byte-for-byte the frozen construction.
        let (ephemeral, ct) = ecdh::split(&envelope).unwrap();
        let key = ecies::shared_key(&secret, &ephemeral, (&topic).into());
        assert_eq!(key, opened.extra);
        assert_eq!(
            ecies::Hint::derive(&key, (&topic).into()).as_bytes(),
            envelope.hint()
        );
        assert_eq!(ecies::decrypt(&key, ct), opened.plaintext);
    }

    #[test]
    fn schemes_never_cross_open() {
        let (secret, public) = keys(b"nectar envelope seam recipient");
        let topic = topic();

        let sealed = Hpke::seal(&hpke_recipient(public), (&topic).into(), b"e", 96).unwrap();
        let bytes = sealed.to_bytes();
        let envelope = Envelope::parse(&bytes).unwrap();
        assert!(
            Compat::open(&secret, (&topic).into(), &envelope)
                .unwrap()
                .is_none()
        );

        let sealed = Compat::seal(&compat_recipient(public), (&topic).into(), b"c", 96).unwrap();
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
        let sealed = Compat::seal(&compat_recipient(public), (&topic).into(), b"f", 96).unwrap();
        let bytes = sealed.to_bytes();
        let envelope = Envelope::parse(&bytes).unwrap();

        let inbox = Inbox::<HpkeOnly>::new(secret.clone());
        assert!(inbox.open(&[topic], &envelope).unwrap().is_none());

        let inbox = Inbox::<HpkeThenCompat>::transitional(secret.clone(), secret);
        let (delivered_topic, opened) = inbox.open(&[topic], &envelope).unwrap().unwrap();
        assert_eq!(delivered_topic, topic);
        assert_eq!(opened.scheme(), SchemeId::Compat);
    }

    #[test]
    fn inbox_delivers_hpke_on_the_right_topic() {
        let (secret, public) = keys(b"nectar envelope seam recipient");
        let topic = topic();
        let decoy = Topic::new(keccak256(b"decoy topic").0);
        let sealed = Hpke::seal(&hpke_recipient(public), (&topic).into(), b"m", 96).unwrap();
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

    /// The decap-once law: candidate topics cost probes, never decaps.
    #[test]
    fn decap_runs_once_per_scheme_regardless_of_topic_count() {
        let (secret, public) = keys(b"nectar envelope seam recipient");
        let topic = topic();
        let mut topics: Vec<Topic> = (0u8..7).map(|i| Topic::new(keccak256([i]).0)).collect();
        topics.push(topic);

        let sealed = Compat::seal(&compat_recipient(public), (&topic).into(), b"c", 96).unwrap();
        let bytes = sealed.to_bytes();
        let envelope = Envelope::parse(&bytes).unwrap();
        let inbox = Inbox::<HpkeThenCompat>::transitional(secret.clone(), secret);
        let _ = decaps::take();
        let (delivered, opened) = inbox.open(&topics, &envelope).unwrap().unwrap();
        assert_eq!(delivered, topic);
        assert_eq!(opened.scheme(), SchemeId::Compat);
        assert_eq!(decaps::take(), (1, 1));

        let sealed = Hpke::seal(&hpke_recipient(public), (&topic).into(), b"h", 96).unwrap();
        let bytes = sealed.to_bytes();
        let envelope = Envelope::parse(&bytes).unwrap();
        let _ = decaps::take();
        assert!(inbox.open(&topics, &envelope).unwrap().is_some());
        assert_eq!(decaps::take(), (1, 0));
    }

    #[test]
    fn remining_the_nonce_keeps_the_envelope_open() {
        let (secret, public) = keys(b"nectar envelope seam recipient");
        let topic = topic();
        let mut sealed = Hpke::seal(&hpke_recipient(public), (&topic).into(), b"m", 96).unwrap();
        let parity_before = {
            let bytes = sealed.to_bytes();
            bytes[8] & 1
        };
        sealed.set_nonce([0xff; 32]);
        let bytes = sealed.to_bytes();
        assert_eq!(bytes[8] & 1, parity_before);
        assert_eq!(bytes[8] & 0xfe, 0xfe);
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

        let sealed = Hpke::seal(&hpke_recipient(public), (&topic).into(), b"m", 96).unwrap();
        let mut bytes = sealed.to_bytes();
        bytes[8] ^= 1;
        let envelope = Envelope::parse(&bytes).unwrap();
        assert!(
            Hpke::open(&secret, (&topic).into(), &envelope)
                .unwrap()
                .is_none()
        );

        // Compat ECDH ignores parity: the flipped envelope still opens.
        let sealed = Compat::seal(&compat_recipient(public), (&topic).into(), b"m", 96).unwrap();
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
        assert!(envelope.tail().is_empty());
    }

    /// A tail too short for the ecdh slot is a decap miss, not a parse error.
    #[test]
    fn short_tail_is_rejected_by_the_scheme() {
        let (secret, _) = keys(b"nectar envelope seam recipient");
        let bytes = [0u8; HEADER_SIZE + ecdh::ENC_X_SIZE - 1];
        let envelope = Envelope::parse(&bytes).unwrap();
        assert!(Hpke::decap(&secret, &envelope).unwrap().is_none());
        assert!(Compat::decap(&secret, &envelope).unwrap().is_none());
    }

    #[test]
    fn any_recipient_seals_via_the_monomorphized_paths() {
        let (secret, public) = keys(b"nectar envelope seam recipient");
        let topic = topic();

        let any = AnyRecipient::Hpke(hpke_recipient(public));
        let sealed = any.seal(&topic, b"m", 96).unwrap();
        assert_eq!(sealed.scheme(), SchemeId::Hpke);
        let bytes = sealed.to_bytes();
        let envelope = Envelope::parse(&bytes).unwrap();
        assert!(
            Hpke::open(&secret, (&topic).into(), &envelope)
                .unwrap()
                .is_some()
        );

        let any = AnyRecipient::Compat(compat_recipient(public));
        let sealed = any.seal(&topic, b"m", 96).unwrap();
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
        assert_eq!(ATTESTATION_PREFIX, b"nectar/env/v2 attest");
        assert_eq!((HINT_SIZE, NONCE_SIZE), (8, 32));
        assert_eq!(HEADER_SIZE, 40);
        assert_eq!(HEADER_SIZE + ecdh::ENC_X_SIZE, 72);
    }

    #[test]
    fn scheme_constants() {
        assert_eq!(Compat::OVERHEAD, 32);
        assert_eq!(Hpke::OVERHEAD, 50);
        assert_eq!((Compat::NONCE_KEEP, Hpke::NONCE_KEEP), (1, 1));
        assert_eq!(Compat::ID, SchemeId::Compat);
        assert_eq!(Hpke::ID, SchemeId::Hpke);
    }
}
