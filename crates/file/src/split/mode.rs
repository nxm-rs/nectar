//! Reference grammar seam: how a split writes child references.

use alloc::vec::Vec;
use core::fmt::Debug;

use bytes::Bytes;
use nectar_primitives::PrimitivesError;
use nectar_primitives::chunk::{AnyChunkSet, Chunk, ChunkAddress, ContentChunk, Verified};
use nectar_primitives::store::{MaybeSend, MaybeSync};

use crate::geometry::Mode;
use crate::walk::Plain;

/// A sealed chunk and the reference that names it.
pub type Sealed<M, const B: usize> = (Chunk<Verified, AnyChunkSet<B>>, <M as SplitMode>::Ref);

/// Write-side reference grammar of one tree profile.
///
/// The engine stays mode-blind: a mode contributes its [`Mode`] geometry,
/// the per-intermediate data-slot count (the parity retrofit seam), and the
/// sealer for one payload; the ascent itself is shared.
pub trait SplitMode: MaybeSend + MaybeSync + 'static {
    /// Reference layout this mode writes.
    const MODE: Mode;

    /// Reference to a sealed subtree, embedded in its parent's body.
    type Ref: Clone + Debug + MaybeSend + MaybeSync + 'static;

    /// Reference naming the finished tree.
    type Root: Clone + Debug + MaybeSend + MaybeSync + 'static;

    /// Data-carrying reference slots per intermediate body. Plain modes use
    /// every slot; a parity variant reserves trailing slots instead.
    fn data_slots(branches: u64) -> u64;

    /// Seal one payload into a chunk and the reference that names it.
    ///
    /// The payload is the chunk's wire body: a little-endian `u64` span
    /// followed by the spanned content (leaf bytes or packed references).
    fn seal<const B: usize>(payload: Bytes) -> Result<Sealed<Self, B>, PrimitivesError>;

    /// Append one reference's wire bytes to a parent payload under build.
    fn write_ref(reference: &Self::Ref, out: &mut Vec<u8>);

    /// The root of a finished tree from its sole surviving reference.
    fn into_root(reference: Self::Ref) -> Self::Root;
}

/// Plain split: a reference is the bare chunk address. One marker serves
/// both engines.
impl SplitMode for Plain {
    const MODE: Mode = Mode::Plain;

    type Ref = ChunkAddress;
    type Root = ChunkAddress;

    fn data_slots(branches: u64) -> u64 {
        branches
    }

    fn seal<const B: usize>(payload: Bytes) -> Result<Sealed<Self, B>, PrimitivesError> {
        let chunk = ContentChunk::<B>::try_from(payload)?.seal::<AnyChunkSet<B>>();
        let address = *chunk.address();
        Ok((chunk, address))
    }

    fn write_ref(reference: &ChunkAddress, out: &mut Vec<u8>) {
        out.extend_from_slice(reference.as_bytes());
    }

    fn into_root(reference: ChunkAddress) -> ChunkAddress {
        reference
    }
}
