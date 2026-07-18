//! Reference grammar seam: how a walk reads child references.

use core::fmt::Debug;

use nectar_primitives::chunk::ChunkAddress;
use nectar_primitives::store::{MaybeSend, MaybeSync};

use crate::geometry::Mode;

/// Reference grammar of one tree profile.
///
/// The engine stays mode-blind: a mode contributes its [`Mode`] geometry and
/// the parser for one wire reference; the descent itself is shared.
pub trait WalkMode: MaybeSend + MaybeSync + 'static {
    /// Reference layout this mode walks.
    const MODE: Mode;

    /// Companion data a reference carries beyond the address, threaded to
    /// every fetched node (an encrypted reference's decryption key).
    type Context: Clone + Debug + MaybeSend + MaybeSync + 'static;

    /// Read one reference off the front of `input`, advancing it past the
    /// consumed bytes; `None` when fewer than one reference remains.
    fn take_ref(input: &mut &[u8]) -> Option<(ChunkAddress, Self::Context)>;
}

/// Plain mode: a reference is a bare chunk address.
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
}
