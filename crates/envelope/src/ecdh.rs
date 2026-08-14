//! Tail layout shared by the two secp256k1 ECDH schemes: `enc_x || ct`.
//!
//! `enc` travels x-only; the low bit of `nonce[0]` carries its y parity.

use k256::PublicKey;

use super::Envelope;
#[cfg(any(test, feature = "encryption"))]
use alloc::vec::Vec;

/// Byte length of the x-only ephemeral slot at the head of the tail.
pub(crate) const ENC_X_SIZE: usize = 32;

pub(crate) const PARITY: u8 = 0x01;

pub(crate) fn ciphertext<'a>(envelope: &Envelope<'a>) -> &'a [u8] {
    envelope.tail().get(ENC_X_SIZE..).unwrap_or(&[])
}

pub(crate) fn reconstruct(enc_x: &[u8; ENC_X_SIZE], parity: bool) -> Option<PublicKey> {
    let mut sec1 = [0u8; 33];
    let [tag, x @ ..] = &mut sec1;
    *tag = if parity { 0x03 } else { 0x02 };
    *x = *enc_x;
    PublicKey::from_sec1_bytes(&sec1).ok()
}

/// Recover the ephemeral key and the ciphertext region; `None` for a short
/// tail or an off-curve x.
pub(crate) fn split<'a>(envelope: &Envelope<'a>) -> Option<(PublicKey, &'a [u8])> {
    let (enc_x, ct) = envelope.tail().split_first_chunk::<ENC_X_SIZE>()?;
    let [nonce_0, ..] = envelope.nonce();
    let key = reconstruct(enc_x, *nonce_0 & PARITY == PARITY)?;
    Some((key, ct))
}

/// Lay `enc_x || ct` down and report the parity bit for `nonce[0]`.
#[cfg(any(test, feature = "encryption"))]
pub(crate) fn tail(ephemeral: &PublicKey, ct: &[u8]) -> (Vec<u8>, u8) {
    use k256::elliptic_curve::point::AffineCoordinates;

    let affine = ephemeral.as_affine();
    let x: [u8; ENC_X_SIZE] = affine.x().into();
    let mut tail = Vec::with_capacity(ENC_X_SIZE.saturating_add(ct.len()));
    tail.extend_from_slice(&x);
    tail.extend_from_slice(ct);
    (tail, u8::from(bool::from(affine.y_is_odd())))
}
