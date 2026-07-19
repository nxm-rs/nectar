//! Reference grammar seam: how a walk reads child references and node
//! bodies.

use core::fmt::Debug;

use bytes::{Bytes, BytesMut};
use nectar_primitives::chunk::ChunkAddress;
use nectar_primitives::chunk::encryption::{EncryptedChunkRef, EncryptionKey, transcrypt_in_place};
use nectar_primitives::store::{MaybeSend, MaybeSync};

use super::error::DecodeError;
use crate::geometry::Mode;

/// Reference grammar of one tree profile.
///
/// The engine stays mode-blind: a mode contributes its [`Mode`] geometry, the
/// parser for one wire reference, and the body decoder; the descent itself is
/// shared.
pub trait WalkMode: MaybeSend + MaybeSync + 'static {
    /// Reference layout this mode walks.
    const MODE: Mode;

    /// Companion data a reference carries beyond the address, threaded to
    /// every fetched node (an encrypted reference's decryption key).
    type Context: Clone + Debug + MaybeSend + MaybeSync + 'static;

    /// Read one reference off the front of `input`, advancing it past the
    /// consumed bytes; `None` when fewer than one reference remains.
    fn take_ref(input: &mut &[u8]) -> Option<(ChunkAddress, Self::Context)>;

    /// Decode one fetched body into the plaintext the tree grammar reads:
    /// `take` bytes (a leaf's span, or an intermediate's packed references)
    /// out of a `body_size`-byte profile.
    ///
    /// `scratch` is the walk's staging buffer, reused across nodes; a
    /// transforming mode writes its output there and splits it off, a
    /// pass-through mode leaves it untouched.
    fn decode_body(
        context: &Self::Context,
        body_size: usize,
        take: usize,
        data: Bytes,
        scratch: &mut BytesMut,
    ) -> Result<Bytes, DecodeError>;
}

/// Plain mode: a reference is a bare chunk address and bodies arrive as
/// plaintext.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Plain;

impl WalkMode for Plain {
    const MODE: Mode = Mode::Plain;

    type Context = ();

    fn take_ref(input: &mut &[u8]) -> Option<(ChunkAddress, ())> {
        let (address, rest) = input.split_first_chunk::<{ ChunkAddress::SIZE }>()?;
        *input = rest;
        Some((ChunkAddress::new(*address), ()))
    }

    fn decode_body(
        _context: &(),
        _body_size: usize,
        _take: usize,
        data: Bytes,
        _scratch: &mut BytesMut,
    ) -> Result<Bytes, DecodeError> {
        Ok(data)
    }
}

/// Encrypted mode: a reference carries an address plus the decryption key of
/// a keccak counter-mode ciphertext body.
///
/// Joining needs no keys beyond the references, so the default `K = ()`
/// serves every read path; the split side instantiates `K` with a key
/// source behind the `encryption` feature.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Encrypted<K = ()> {
    source: K,
}

impl<K> Encrypted<K> {
    /// Wrap a split-side key source; `Encrypted::default()` covers reads.
    pub const fn new(source: K) -> Self {
        Self { source }
    }

    /// The wrapped key source.
    pub const fn source(&self) -> &K {
        &self.source
    }
}

impl<K: MaybeSend + MaybeSync + 'static> WalkMode for Encrypted<K> {
    const MODE: Mode = Mode::Encrypted;

    type Context = EncryptionKey;

    fn take_ref(input: &mut &[u8]) -> Option<(ChunkAddress, EncryptionKey)> {
        let (raw, rest) = input.split_first_chunk::<{ EncryptedChunkRef::SIZE }>()?;
        *input = rest;
        Some(EncryptedChunkRef::from_bytes(raw).into_parts())
    }

    /// A ciphertext body is always full-size (short leaves are padded), so
    /// only the first `take` bytes are decrypted and returned.
    ///
    /// The plaintext is staged in `scratch` and split off as the frame; the
    /// shared ciphertext is never touched, and the staging allocation is
    /// reclaimed once earlier frames drop.
    fn decode_body(
        context: &EncryptionKey,
        body_size: usize,
        take: usize,
        mut data: Bytes,
        scratch: &mut BytesMut,
    ) -> Result<Bytes, DecodeError> {
        if data.len() != body_size {
            return Err(DecodeError::CiphertextLength {
                len: data.len(),
                expected: body_size,
            });
        }
        data.truncate(take.min(body_size));
        scratch.clear();
        scratch.extend_from_slice(data.as_ref());
        transcrypt_in_place(context, 0, scratch.as_mut());
        Ok(scratch.split().freeze())
    }
}
