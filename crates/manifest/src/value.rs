//! Keys and entry values: what the map stores against each key.

use core::marker::PhantomData;

use bytes::Bytes;
use nectar_primitives::{ChunkAddress, ChunkRef, EncryptedChunkRef, EntryRef};

use crate::error::{NotAReference, ValueTooLong};
use crate::format::{Format, V1};

/// An arbitrary-byte map key, ordered bytewise.
///
/// Unbounded: long keys chain through trie nodes on the wire, so no format
/// bound applies to the key itself. The empty key is legal; its value lives
/// in the root extension.
#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Key(Bytes);

impl Key {
    /// The empty key.
    #[must_use]
    pub const fn empty() -> Self {
        Self(Bytes::new())
    }

    /// Wrap `bytes` as a key.
    #[must_use]
    pub const fn new(bytes: Bytes) -> Self {
        Self(bytes)
    }

    /// Key length in bytes.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `true` for the empty key.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The key bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// The key bytes as shared [`Bytes`].
    #[must_use]
    pub fn into_bytes(self) -> Bytes {
        self.0
    }
}

impl AsRef<[u8]> for Key {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<Bytes> for Key {
    fn from(bytes: Bytes) -> Self {
        Self(bytes)
    }
}

impl From<Vec<u8>> for Key {
    fn from(bytes: Vec<u8>) -> Self {
        Self(Bytes::from(bytes))
    }
}

impl From<&[u8]> for Key {
    fn from(bytes: &[u8]) -> Self {
        Self(Bytes::copy_from_slice(bytes))
    }
}

/// An inline value: at most `F::VINLINE_MAX` bytes, checked once here.
///
/// The empty value is legal and distinct from an absent entry, which is
/// `Option::None` at the use site.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct InlineValue<F: Format = V1> {
    bytes: Bytes,
    _format: PhantomData<F>,
}

impl<F: Format> InlineValue<F> {
    /// The empty value.
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            bytes: Bytes::new(),
            _format: PhantomData,
        }
    }

    /// Wrap `bytes` as an inline value, rejecting lengths above
    /// `F::VINLINE_MAX`.
    pub fn new(bytes: Bytes) -> Result<Self, ValueTooLong> {
        check_vlen::<F>(bytes.len())?;
        Ok(Self {
            bytes,
            _format: PhantomData,
        })
    }

    /// Value length in bytes; always at most `F::VINLINE_MAX`.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Returns `true` for the empty value.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// The value bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// The value bytes as shared [`Bytes`].
    #[must_use]
    pub fn into_bytes(self) -> Bytes {
        self.bytes
    }
}

impl<F: Format> Default for InlineValue<F> {
    fn default() -> Self {
        Self::empty()
    }
}

/// Length gate shared by the owned and copying constructors, checked before
/// any copy so an over-long slice never allocates.
const fn check_vlen<F: Format>(actual: usize) -> Result<(), ValueTooLong> {
    if actual > F::VINLINE_MAX {
        return Err(ValueTooLong {
            actual,
            max: F::VINLINE_MAX,
        });
    }
    Ok(())
}

impl<F: Format> AsRef<[u8]> for InlineValue<F> {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl<F: Format> TryFrom<Bytes> for InlineValue<F> {
    type Error = ValueTooLong;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        Self::new(bytes)
    }
}

impl<F: Format> TryFrom<&[u8]> for InlineValue<F> {
    type Error = ValueTooLong;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        check_vlen::<F>(bytes.len())?;
        Ok(Self {
            bytes: Bytes::copy_from_slice(bytes),
            _format: PhantomData,
        })
    }
}

/// A key's value: a chunk reference or inline bytes.
///
/// Absence is `Option<Entry>` at the use site; the wire carries a presence
/// discriminant, never an in-band null sentinel. Traversal follows the
/// reference variants only; inline bytes are opaque. A ref64 asserts the
/// target is an encrypted chunk and transports its decryption key in-band.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Entry<F: Format = V1> {
    /// A plain 32-byte reference.
    Ref32(ChunkRef),
    /// An encrypted 64-byte reference: address plus decryption key.
    Ref64(EncryptedChunkRef),
    /// Opaque inline bytes, at most `F::VINLINE_MAX`.
    Inline(InlineValue<F>),
}

impl<F: Format> Entry<F> {
    /// Wrap `bytes` as an inline entry, rejecting lengths above
    /// `F::VINLINE_MAX`. Longer byte values belong in content chunks behind
    /// a reference variant.
    pub fn inline(bytes: Bytes) -> Result<Self, ValueTooLong> {
        Ok(Self::Inline(InlineValue::new(bytes)?))
    }

    /// The referenced chunk address; `None` for an inline value.
    #[must_use]
    pub const fn address(&self) -> Option<&ChunkAddress> {
        match self {
            Self::Ref32(r) => Some(r.address()),
            Self::Ref64(r) => Some(r.address()),
            Self::Inline(_) => None,
        }
    }

    /// Returns `true` when traversal follows this entry to a chunk.
    #[must_use]
    pub const fn is_reference(&self) -> bool {
        matches!(self, Self::Ref32(_) | Self::Ref64(_))
    }
}

impl<F: Format> From<ChunkRef> for Entry<F> {
    fn from(reference: ChunkRef) -> Self {
        Self::Ref32(reference)
    }
}

impl<F: Format> From<EncryptedChunkRef> for Entry<F> {
    fn from(reference: EncryptedChunkRef) -> Self {
        Self::Ref64(reference)
    }
}

impl<F: Format> From<InlineValue<F>> for Entry<F> {
    fn from(value: InlineValue<F>) -> Self {
        Self::Inline(value)
    }
}

impl<F: Format> From<EntryRef> for Entry<F> {
    fn from(reference: EntryRef) -> Self {
        match reference {
            EntryRef::Plain(r) => Self::Ref32(r),
            EntryRef::Encrypted(r) => Self::Ref64(r),
        }
    }
}

impl<F: Format> TryFrom<Entry<F>> for EntryRef {
    type Error = NotAReference;

    fn try_from(entry: Entry<F>) -> Result<Self, Self::Error> {
        match entry {
            Entry::Ref32(r) => Ok(Self::Plain(r)),
            Entry::Ref64(r) => Ok(Self::Encrypted(r)),
            Entry::Inline(value) => Err(NotAReference {
                len: value.as_ref().len(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use nectar_primitives::EncryptionKey;

    use super::*;

    fn address(byte: u8) -> ChunkAddress {
        ChunkAddress::new([byte; 32])
    }

    #[test]
    fn key_orders_bytewise() {
        let a = Key::from(&b"a"[..]);
        let ab = Key::from(&b"ab"[..]);
        let b = Key::from(&b"b"[..]);
        assert!(Key::empty() < a);
        assert!(a < ab);
        assert!(ab < b);
    }

    #[test]
    fn key_empty_is_default() {
        let key = Key::default();
        assert!(key.is_empty());
        assert_eq!(key.len(), 0);
        assert_eq!(key, Key::empty());
    }

    #[test]
    fn key_round_trips_bytes() {
        let key = Key::from(vec![0x00, 0xFF]);
        assert_eq!(key.as_bytes(), &[0x00, 0xFF]);
        assert_eq!(key.clone().into_bytes(), Bytes::from(vec![0x00, 0xFF]));
        assert_eq!(Key::new(Bytes::from(vec![0x00, 0xFF])), key);
    }

    #[test]
    fn inline_value_accepts_up_to_vinline_max() {
        let bytes = vec![0xAB; V1::VINLINE_MAX];
        let value = InlineValue::<V1>::try_from(bytes.as_slice()).unwrap();
        assert_eq!(value.len(), V1::VINLINE_MAX);
        assert_eq!(value.as_bytes(), bytes.as_slice());
    }

    #[test]
    fn inline_value_rejects_over_vinline_max() {
        let bytes = vec![0xAB; V1::VINLINE_MAX + 1];
        let err = InlineValue::<V1>::new(Bytes::from(bytes)).unwrap_err();
        assert_eq!(
            err,
            ValueTooLong {
                actual: V1::VINLINE_MAX + 1,
                max: V1::VINLINE_MAX
            }
        );
    }

    #[test]
    fn inline_value_empty_is_legal_and_distinct_from_absent() {
        let value = InlineValue::<V1>::empty();
        assert!(value.is_empty());
        assert_eq!(value, InlineValue::default());

        let entry: Option<Entry> = Some(Entry::inline(Bytes::new()).unwrap());
        assert_ne!(entry, None);
    }

    #[test]
    fn entry_address_follows_references_only() {
        let addr = address(0x11);
        let plain: Entry = ChunkRef::new(addr).into();
        assert_eq!(plain.address(), Some(&addr));
        assert!(plain.is_reference());

        let encrypted: Entry = EncryptedChunkRef::new(addr, EncryptionKey::from([0x22; 32])).into();
        assert_eq!(encrypted.address(), Some(&addr));
        assert!(encrypted.is_reference());

        let inline: Entry = InlineValue::empty().into();
        assert_eq!(inline.address(), None);
        assert!(!inline.is_reference());
    }

    #[test]
    fn entry_inline_rejects_over_vinline_max() {
        let err = Entry::<V1>::inline(Bytes::from(vec![0; V1::VINLINE_MAX + 1])).unwrap_err();
        assert_eq!(err.max, V1::VINLINE_MAX);
    }

    #[test]
    fn entry_round_trips_entry_ref_both_widths() {
        let plain = EntryRef::Plain(ChunkRef::new(address(0x33)));
        let entry = Entry::<V1>::from(plain.clone());
        assert_eq!(entry, Entry::Ref32(ChunkRef::new(address(0x33))));
        assert_eq!(EntryRef::try_from(entry).unwrap(), plain);

        let encrypted = EntryRef::Encrypted(EncryptedChunkRef::new(
            address(0x44),
            EncryptionKey::from([0x55; 32]),
        ));
        let entry = Entry::<V1>::from(encrypted.clone());
        assert_eq!(EntryRef::try_from(entry).unwrap(), encrypted);
    }

    #[test]
    fn inline_entry_is_not_a_reference() {
        let entry: Entry = Entry::inline(Bytes::from(vec![0xAA; 3])).unwrap();
        let err = EntryRef::try_from(entry).unwrap_err();
        assert_eq!(err, NotAReference { len: 3 });
    }
}
