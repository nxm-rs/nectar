//! Typed chunk references.
//!
//! A reference names a chunk by its 32-byte address; the width is a fact of the
//! reference type, not a runtime byte count. [`RefKind`] names the two widths
//! and [`Reference`] carries them at the type level, so every wire-width
//! constant in the crate derives from this single statement of the fact.

use std::mem::size_of;

use crate::chunk::ChunkAddress;
use crate::entry_ref::EntryRef;
use crate::error::WrongLength;
use crate::wire::{Cursor, FromCursor, ToWriter, Underrun, Writer};

pub(crate) mod sealed {
    pub trait Sealed {}
}

/// An [`EntryRef`] did not carry the width its target reference type requires.
///
/// Raised where a manifest keyed by one [`RefKind`] is handed an entry of the
/// other; the typed replacement for a laundered byte-length mismatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("wrong reference kind: expected {expected:?}, got {got:?}")]
pub struct WrongRefKind {
    /// The reference kind the target type requires.
    pub expected: RefKind,
    /// The reference kind the entry actually carried.
    pub got: RefKind,
}

/// The two reference widths: a plain address, or an address plus a key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RefKind {
    /// A plain reference ([`ChunkRef`]): a 32-byte address.
    Plain,
    /// An encrypted reference
    /// ([`EncryptedChunkRef`](crate::chunk::encryption::EncryptedChunkRef)):
    /// the same address plus the chunk's decryption key.
    Encrypted,
}

impl RefKind {
    /// Wire width in bytes of a reference of this kind.
    pub const fn size(self) -> usize {
        match self {
            Self::Plain => ChunkRef::SIZE,
            Self::Encrypted => crate::chunk::encryption::EncryptedChunkRef::SIZE,
        }
    }
}

/// A chunk reference whose width is a compile-time fact.
///
/// Sealed: the only references are [`ChunkRef`] and
/// [`EncryptedChunkRef`](crate::chunk::encryption::EncryptedChunkRef). The width
/// fact is stated once, on [`Self::KIND`]; wire serialization derives from it,
/// so no caller restates 32 or 64.
pub trait Reference: sealed::Sealed + Sized + Clone + Eq + core::fmt::Debug + 'static {
    /// Which width this reference carries.
    const KIND: RefKind;

    /// Wire width in bytes; the width fact, derived from [`Self::KIND`].
    const SIZE: usize = Self::KIND.size();

    /// The chunk address this reference names.
    fn address(&self) -> &ChunkAddress;

    /// This reference as a width-typed [`EntryRef`], carrying its address (and
    /// key) without a wire round-trip.
    fn into_entry_ref(self) -> EntryRef;

    /// Recover a typed reference from an [`EntryRef`], or report the kind found
    /// when it is not [`KIND`](Self::KIND).
    fn from_entry_ref(entry: EntryRef) -> Result<Self, WrongRefKind>;

    /// Append this reference's [`SIZE`](Self::SIZE) wire bytes to `out`.
    fn write_to(&self, out: &mut Vec<u8>);

    /// Reconstruct from exactly [`SIZE`](Self::SIZE) wire bytes; `None` on any
    /// other length.
    fn from_wire_bytes(bytes: &[u8]) -> Option<Self>;

    /// This reference's [`SIZE`](Self::SIZE) wire bytes as an owned buffer.
    fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(Self::SIZE);
        self.write_to(&mut out);
        out
    }

    /// Read a reference from `cursor`, or `None` when the slot is the all-zero
    /// sentinel. Each impl reads its own typed fields; the cursor is the sole
    /// fallible read.
    fn read_optional(cursor: &mut Cursor<'_>) -> Result<Option<Self>, Underrun>;
}

/// A 32-byte reference to a chunk.
///
/// The chunk may be content-addressed or single-owner; a reference is identical
/// either way, and which kind it is is resolved on fetch and validation, never
/// from the reference.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ChunkRef(ChunkAddress);

impl ChunkRef {
    /// Wire width in bytes.
    pub const SIZE: usize = size_of::<ChunkAddress>();

    /// Wrap an address as a reference.
    pub const fn new(address: ChunkAddress) -> Self {
        Self(address)
    }

    /// The referenced chunk address.
    pub const fn address(&self) -> &ChunkAddress {
        &self.0
    }

    /// Consume the reference, returning its address.
    pub const fn into_address(self) -> ChunkAddress {
        self.0
    }
}

/// Reads the 32 address bytes.
impl FromCursor for ChunkRef {
    type Error = Underrun;

    fn take_from(cur: &mut Cursor<'_>) -> Result<Self, Underrun> {
        cur.take::<[u8; ChunkAddress::SIZE]>()
            .map(|bytes| Self::new(ChunkAddress::new(bytes)))
    }
}

/// Writes the 32 address bytes, the mirror of the `FromCursor` impl above.
impl ToWriter for ChunkRef {
    fn put_into(&self, w: &mut Writer<'_>) {
        w.put(self.address().as_bytes());
    }
}

impl From<ChunkAddress> for ChunkRef {
    fn from(address: ChunkAddress) -> Self {
        Self(address)
    }
}

impl TryFrom<&[u8]> for ChunkRef {
    type Error = WrongLength;

    fn try_from(slice: &[u8]) -> Result<Self, Self::Error> {
        let bytes: [u8; Self::SIZE] = slice.try_into().map_err(|_| WrongLength {
            expected: Self::SIZE,
            got: slice.len(),
        })?;
        Ok(Self::new(ChunkAddress::from(bytes)))
    }
}

impl sealed::Sealed for ChunkRef {}

impl Reference for ChunkRef {
    const KIND: RefKind = RefKind::Plain;

    fn address(&self) -> &ChunkAddress {
        &self.0
    }

    fn into_entry_ref(self) -> EntryRef {
        EntryRef::Plain(self.into_address())
    }

    fn from_entry_ref(entry: EntryRef) -> Result<Self, WrongRefKind> {
        match entry {
            EntryRef::Plain(address) => Ok(Self::new(address)),
            EntryRef::Encrypted(_) => Err(WrongRefKind {
                expected: Self::KIND,
                got: RefKind::Encrypted,
            }),
        }
    }

    fn write_to(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(self.0.as_bytes());
    }

    fn from_wire_bytes(bytes: &[u8]) -> Option<Self> {
        let bytes: [u8; Self::SIZE] = bytes.try_into().ok()?;
        Some(Self::new(ChunkAddress::from(bytes)))
    }

    fn read_optional(cursor: &mut Cursor<'_>) -> Result<Option<Self>, Underrun> {
        let addr = cursor.take::<[u8; ChunkAddress::SIZE]>()?;
        if addr.iter().all(|&b| b == 0) {
            Ok(None)
        } else {
            Ok(Some(Self(ChunkAddress::new(addr))))
        }
    }
}

#[cfg(feature = "arbitrary")]
impl<'a> arbitrary::Arbitrary<'a> for ChunkRef {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(Self::new(ChunkAddress::arbitrary(u)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunk::encryption::{EncryptedChunkRef, EncryptionKey};
    use alloy_primitives::B256;

    #[test]
    fn chunk_ref_is_address_width() {
        assert_eq!(ChunkRef::SIZE, 32);
        assert_eq!(<ChunkRef as Reference>::SIZE, ChunkRef::SIZE);
        assert_eq!(ChunkRef::KIND, RefKind::Plain);
        assert_eq!(RefKind::Plain.size(), ChunkRef::SIZE);
    }

    #[test]
    fn encrypted_ref_is_address_plus_key() {
        assert_eq!(EncryptedChunkRef::SIZE, 64);
        assert_eq!(
            <EncryptedChunkRef as Reference>::SIZE,
            EncryptedChunkRef::SIZE
        );
        assert_eq!(EncryptedChunkRef::KIND, RefKind::Encrypted);
        assert_eq!(RefKind::Encrypted.size(), EncryptedChunkRef::SIZE);
    }

    #[test]
    fn entry_ref_round_trips_plain() {
        let addr = ChunkAddress::from(B256::repeat_byte(0x11));
        let reference = ChunkRef::new(addr);
        let entry = reference.into_entry_ref();
        assert_eq!(entry, EntryRef::Plain(addr));
        assert_eq!(ChunkRef::from_entry_ref(entry).unwrap(), reference);
    }

    #[test]
    fn entry_ref_round_trips_encrypted() {
        let addr = ChunkAddress::from(B256::repeat_byte(0x22));
        let reference = EncryptedChunkRef::new(addr, EncryptionKey::from([0x33; 32]));
        let entry = reference.clone().into_entry_ref();
        assert_eq!(entry, EntryRef::Encrypted(reference.clone()));
        assert_eq!(EncryptedChunkRef::from_entry_ref(entry).unwrap(), reference);
    }

    #[test]
    fn from_entry_ref_rejects_wrong_kind() {
        let addr = ChunkAddress::from(B256::repeat_byte(0x44));
        let plain = ChunkRef::new(addr).into_entry_ref();
        let encrypted =
            EncryptedChunkRef::new(addr, EncryptionKey::from([0x55; 32])).into_entry_ref();

        assert_eq!(
            EncryptedChunkRef::from_entry_ref(plain).unwrap_err(),
            WrongRefKind {
                expected: RefKind::Encrypted,
                got: RefKind::Plain,
            }
        );
        assert_eq!(
            ChunkRef::from_entry_ref(encrypted).unwrap_err(),
            WrongRefKind {
                expected: RefKind::Plain,
                got: RefKind::Encrypted,
            }
        );
    }

    #[test]
    fn try_from_slice_round_trips() {
        let addr = ChunkAddress::from(B256::repeat_byte(0x66));
        let reference = ChunkRef::new(addr);
        assert_eq!(ChunkRef::try_from(addr.as_bytes()).unwrap(), reference);
    }

    #[test]
    fn try_from_slice_wrong_length() {
        let short = [0u8; 31];
        assert_eq!(
            ChunkRef::try_from(short.as_slice()).unwrap_err(),
            WrongLength {
                expected: ChunkRef::SIZE,
                got: 31
            }
        );

        let long = [0u8; 64];
        assert_eq!(
            ChunkRef::try_from(long.as_slice()).unwrap_err(),
            WrongLength {
                expected: ChunkRef::SIZE,
                got: 64
            }
        );
    }

    #[test]
    fn round_trips_through_address() {
        let addr = ChunkAddress::from(B256::repeat_byte(0x7f));
        let reference = ChunkRef::new(addr);
        assert_eq!(reference.address(), &addr);
        assert_eq!(reference.into_address(), addr);
        assert_eq!(ChunkRef::from(addr), reference);
    }

    #[test]
    fn read_optional_maps_the_zero_slot_to_none() {
        let zeros = [0u8; ChunkRef::SIZE];
        let mut cur = Cursor::new(&zeros);
        assert_eq!(ChunkRef::read_optional(&mut cur).unwrap(), None);
        assert!(cur.is_empty());

        let bytes = [0x7fu8; ChunkRef::SIZE];
        let mut cur = Cursor::new(&bytes);
        assert_eq!(
            ChunkRef::read_optional(&mut cur).unwrap(),
            Some(ChunkRef::new(ChunkAddress::new(bytes)))
        );
        assert!(cur.is_empty());

        let short = [0x7fu8; ChunkRef::SIZE - 1];
        let mut cur = Cursor::new(&short);
        assert!(ChunkRef::read_optional(&mut cur).is_err());
    }
}
