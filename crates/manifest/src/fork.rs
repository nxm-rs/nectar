//! Fork records and the per-node fork table.
//!
//! The table is keyed on the first prefix byte, so sorted-unique order and
//! the radix-256 fork bound are structural facts, not runtime checks. Wire
//! flag bits carry no independent state and are never stored: presence is
//! derived from the structure at encode time.

use std::collections::BTreeMap;

use nectar_primitives::{ChunkAddress, ChunkRef, EncryptedChunkRef};

use crate::bounded::Prefix;
use crate::error::ForkPrefixEmpty;
use crate::format::{Format, V1};
use crate::meta::Metadata;
use crate::value::Entry;

/// A fork's child: the trie continuation below the fork's prefix.
///
/// An embedded child is its fork table alone: an embedded body carries no
/// root extension and is never segmented, so nothing else survives the
/// type. Its encoded size bound (`F::INLINE_MAX`) and non-emptiness are the
/// packing and codec cars' checks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Child<F: Format = V1> {
    /// A plain 32-byte reference to the child chunk.
    Ref32(ChunkRef),
    /// An encrypted 64-byte reference: address plus decryption key.
    Ref64(EncryptedChunkRef),
    /// The child's body held in the parent instead of a chunk of its own.
    Embedded(ForkTable<F>),
}

impl<F: Format> Child<F> {
    /// The referenced chunk address; `None` for an embedded child.
    #[must_use]
    pub const fn address(&self) -> Option<&ChunkAddress> {
        match self {
            Self::Ref32(r) => Some(r.address()),
            Self::Ref64(r) => Some(r.address()),
            Self::Embedded(_) => None,
        }
    }

    /// Returns `true` when the child lives in a chunk of its own.
    #[must_use]
    pub const fn is_reference(&self) -> bool {
        matches!(self, Self::Ref32(_) | Self::Ref64(_))
    }
}

impl<F: Format> From<ChunkRef> for Child<F> {
    fn from(reference: ChunkRef) -> Self {
        Self::Ref32(reference)
    }
}

impl<F: Format> From<EncryptedChunkRef> for Child<F> {
    fn from(reference: EncryptedChunkRef) -> Self {
        Self::Ref64(reference)
    }
}

impl<F: Format> From<ForkTable<F>> for Child<F> {
    fn from(forks: ForkTable<F>) -> Self {
        Self::Embedded(forks)
    }
}

/// What a fork holds: it terminates a key, continues the trie, or both.
///
/// Neither is unrepresentable: a fork with no entry and no child has no
/// meaning.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ForkPayload<F: Format = V1> {
    /// The accumulated path ending in this prefix is a key with this value.
    Entry(Entry<F>),
    /// The trie continues below this prefix.
    Child(Child<F>),
    /// A key terminates here and the trie continues below it.
    Both {
        /// The value at the accumulated path.
        entry: Entry<F>,
        /// The continuation below the prefix.
        child: Child<F>,
    },
}

impl<F: Format> ForkPayload<F> {
    /// Assemble from parts; `None` when both are absent.
    #[must_use]
    pub fn new(entry: Option<Entry<F>>, child: Option<Child<F>>) -> Option<Self> {
        match (entry, child) {
            (Some(entry), Some(child)) => Some(Self::Both { entry, child }),
            (Some(entry), None) => Some(Self::Entry(entry)),
            (None, Some(child)) => Some(Self::Child(child)),
            (None, None) => None,
        }
    }

    /// The value terminating a key at this fork.
    #[must_use]
    pub const fn entry(&self) -> Option<&Entry<F>> {
        match self {
            Self::Entry(entry) | Self::Both { entry, .. } => Some(entry),
            Self::Child(_) => None,
        }
    }

    /// The trie continuation below this fork.
    #[must_use]
    pub const fn child(&self) -> Option<&Child<F>> {
        match self {
            Self::Child(child) | Self::Both { child, .. } => Some(child),
            Self::Entry(_) => None,
        }
    }
}

impl<F: Format> From<Entry<F>> for ForkPayload<F> {
    fn from(entry: Entry<F>) -> Self {
        Self::Entry(entry)
    }
}

impl<F: Format> From<Child<F>> for ForkPayload<F> {
    fn from(child: Child<F>) -> Self {
        Self::Child(child)
    }
}

/// One fork: the prefix tail plus what hangs off it.
///
/// The prefix's first byte is not stored here: it is the fork-table key, so
/// a record cannot disagree with its index position, and the full prefix
/// length stays in `1..=F::PLEN_MAX` by construction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ForkRecord<F: Format = V1> {
    tail: Prefix<F>,
    payload: ForkPayload<F>,
    metadata: Option<Metadata<F>>,
}

impl<F: Format> ForkRecord<F> {
    /// Split `prefix` into its first byte (the table key) and a record
    /// holding the tail. Rejects the empty prefix: a fork consumes at least
    /// the byte it is indexed under.
    pub fn new(
        prefix: Prefix<F>,
        payload: ForkPayload<F>,
        metadata: Option<Metadata<F>>,
    ) -> Result<(u8, Self), ForkPrefixEmpty> {
        let (first, tail) = prefix.split_first().ok_or(ForkPrefixEmpty)?;
        Ok((
            first,
            Self {
                tail,
                payload,
                metadata,
            },
        ))
    }

    /// A record from its decoded wire parts; the tail arrives without its
    /// fork-table key byte, so no split is needed.
    pub(crate) const fn from_tail_parts(
        tail: Prefix<F>,
        payload: ForkPayload<F>,
        metadata: Option<Metadata<F>>,
    ) -> Self {
        Self {
            tail,
            payload,
            metadata,
        }
    }

    /// The prefix tail: everything after the fork-table key byte.
    #[must_use]
    pub const fn tail(&self) -> &Prefix<F> {
        &self.tail
    }

    /// Full prefix length in bytes; always in `1..=F::PLEN_MAX`.
    #[must_use]
    pub const fn prefix_len(&self) -> usize {
        self.tail.len().saturating_add(1)
    }

    /// What the fork holds.
    #[must_use]
    pub const fn payload(&self) -> &ForkPayload<F> {
        &self.payload
    }

    /// Mutable access to what the fork holds.
    pub const fn payload_mut(&mut self) -> &mut ForkPayload<F> {
        &mut self.payload
    }

    /// The value terminating a key at this fork.
    #[must_use]
    pub const fn entry(&self) -> Option<&Entry<F>> {
        self.payload.entry()
    }

    /// The trie continuation below this fork.
    #[must_use]
    pub const fn child(&self) -> Option<&Child<F>> {
        self.payload.child()
    }

    /// The metadata of the key terminating at this fork.
    #[must_use]
    pub const fn metadata(&self) -> Option<&Metadata<F>> {
        self.metadata.as_ref()
    }

    /// Mutable access to the fork's metadata slot.
    pub const fn metadata_mut(&mut self) -> &mut Option<Metadata<F>> {
        &mut self.metadata
    }
}

/// The per-node fork table, keyed on the first prefix byte.
///
/// Radix-256: the `u8` key makes the table sorted-unique and caps it at
/// `F::FORKS_MAX` records structurally. Iteration order is the wire order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ForkTable<F: Format = V1> {
    records: BTreeMap<u8, ForkRecord<F>>,
}

impl<F: Format> ForkTable<F> {
    /// The empty table.
    #[must_use]
    pub const fn new() -> Self {
        // The structural cap is the key type's cardinality.
        const { assert!(F::FORKS_MAX == 256) };
        Self {
            records: BTreeMap::new(),
        }
    }

    /// Insert the fork for `prefix`, replacing and returning any record
    /// already indexed under its first byte. Rejects the empty prefix.
    pub fn insert(
        &mut self,
        prefix: Prefix<F>,
        payload: ForkPayload<F>,
        metadata: Option<Metadata<F>>,
    ) -> Result<Option<ForkRecord<F>>, ForkPrefixEmpty> {
        let (first, record) = ForkRecord::new(prefix, payload, metadata)?;
        Ok(self.records.insert(first, record))
    }

    /// Insert a decoded record directly under its fork-table key byte,
    /// replacing and returning any record already indexed there.
    pub(crate) fn insert_record(
        &mut self,
        first: u8,
        record: ForkRecord<F>,
    ) -> Option<ForkRecord<F>> {
        self.records.insert(first, record)
    }

    /// The record indexed under `first`.
    #[must_use]
    pub fn get(&self, first: u8) -> Option<&ForkRecord<F>> {
        self.records.get(&first)
    }

    /// Mutable access to the record indexed under `first`.
    pub fn get_mut(&mut self, first: u8) -> Option<&mut ForkRecord<F>> {
        self.records.get_mut(&first)
    }

    /// Remove and return the record indexed under `first`.
    pub fn remove(&mut self, first: u8) -> Option<ForkRecord<F>> {
        self.records.remove(&first)
    }

    /// The forks in wire order: ascending first byte.
    pub fn iter(&self) -> impl Iterator<Item = (u8, &ForkRecord<F>)> {
        self.records.iter().map(|(first, record)| (*first, record))
    }

    /// Consume the table into its records in ascending first-byte order.
    pub(crate) fn into_records(self) -> impl Iterator<Item = (u8, ForkRecord<F>)> {
        self.records.into_iter()
    }

    /// Number of forks; always at most `F::FORKS_MAX`.
    #[must_use]
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Returns `true` when the table holds no forks.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

impl<F: Format> Default for ForkTable<F> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    fn entry(byte: u8) -> Entry {
        ChunkRef::new(ChunkAddress::new([byte; 32])).into()
    }

    fn prefix(bytes: &[u8]) -> Prefix {
        Prefix::try_from(bytes).unwrap()
    }

    #[test]
    fn insert_splits_the_prefix_and_iterates_in_wire_order() {
        let mut table = ForkTable::new();
        table
            .insert(prefix(b"beta"), entry(1).into(), None)
            .unwrap();
        table
            .insert(prefix(b"alpha"), entry(2).into(), None)
            .unwrap();

        let forks: Vec<_> = table
            .iter()
            .map(|(first, record)| (first, record.tail().as_bytes().to_vec()))
            .collect();
        assert_eq!(
            forks,
            vec![(b'a', b"lpha".to_vec()), (b'b', b"eta".to_vec())]
        );
    }

    #[test]
    fn empty_prefix_is_rejected() {
        let mut table = ForkTable::new();
        let err = table
            .insert(Prefix::empty(), entry(1).into(), None)
            .unwrap_err();
        assert_eq!(err, ForkPrefixEmpty);
        assert!(table.is_empty());
    }

    #[test]
    fn full_prefix_length_is_bounded_by_plen_max() {
        let bytes = vec![0x61; V1::PLEN_MAX];
        let (first, record) = ForkRecord::new(prefix(&bytes), entry(1).into(), None).unwrap();
        assert_eq!(first, 0x61);
        assert_eq!(record.tail().len(), V1::PLEN_MAX - 1);
        assert_eq!(record.prefix_len(), V1::PLEN_MAX);
    }

    #[test]
    fn single_byte_prefix_has_an_empty_tail() {
        let (first, record) = ForkRecord::new(prefix(b"x"), entry(1).into(), None).unwrap();
        assert_eq!(first, b'x');
        assert!(record.tail().is_empty());
        assert_eq!(record.prefix_len(), 1);
    }

    #[test]
    fn a_shared_first_byte_replaces_the_record() {
        let mut table = ForkTable::new();
        table.insert(prefix(b"aa"), entry(1).into(), None).unwrap();
        let replaced = table
            .insert(prefix(b"ab"), entry(2).into(), None)
            .unwrap()
            .unwrap();
        assert_eq!(replaced.tail().as_bytes(), b"a");
        assert_eq!(table.len(), 1);
        assert_eq!(table.get(b'a').unwrap().tail().as_bytes(), b"b");
    }

    #[test]
    fn the_table_saturates_at_forks_max() {
        let mut table = ForkTable::new();
        for first in u8::MIN..=u8::MAX {
            table
                .insert(prefix(&[first]), entry(first).into(), None)
                .unwrap();
        }
        assert_eq!(table.len(), V1::FORKS_MAX);

        table.insert(prefix(&[0]), entry(1).into(), None).unwrap();
        assert_eq!(table.len(), V1::FORKS_MAX);
    }

    #[test]
    fn payload_requires_an_entry_or_a_child() {
        assert_eq!(ForkPayload::<V1>::new(None, None), None);

        let only_entry = ForkPayload::new(Some(entry(1)), None).unwrap();
        assert_eq!(only_entry.entry(), Some(&entry(1)));
        assert_eq!(only_entry.child(), None);

        let child = Child::from(ChunkRef::new(ChunkAddress::new([2; 32])));
        let only_child = ForkPayload::new(None, Some(child.clone())).unwrap();
        assert_eq!(only_child.entry(), None);
        assert_eq!(only_child.child(), Some(&child));

        let both = ForkPayload::new(Some(entry(1)), Some(child.clone())).unwrap();
        assert_eq!(both.entry(), Some(&entry(1)));
        assert_eq!(both.child(), Some(&child));
    }

    #[test]
    fn an_embedded_child_is_a_fork_table() {
        let mut inner = ForkTable::new();
        inner
            .insert(prefix(b"leaf"), entry(3).into(), None)
            .unwrap();

        let embedded = Child::from(inner.clone());
        assert_eq!(embedded.address(), None);
        assert!(!embedded.is_reference());

        let mut outer = ForkTable::new();
        outer
            .insert(prefix(b"dir/"), embedded.into(), None)
            .unwrap();
        let child = outer.get(b'd').unwrap().child().unwrap();
        assert_eq!(child, &Child::Embedded(inner));
    }

    #[test]
    fn record_carries_metadata() {
        let meta = Metadata::new(
            crate::meta::KeyId::ContentType,
            Bytes::from_static(b"text/html"),
        )
        .unwrap();
        let (_, record) =
            ForkRecord::new(prefix(b"index.html"), entry(1).into(), Some(meta.clone())).unwrap();
        assert_eq!(record.metadata(), Some(&meta));
    }

    #[test]
    fn removal_and_mutation_address_the_first_byte() {
        let mut table = ForkTable::new();
        table.insert(prefix(b"aa"), entry(1).into(), None).unwrap();
        table.insert(prefix(b"bb"), entry(2).into(), None).unwrap();

        *table.get_mut(b'a').unwrap().payload_mut() = entry(9).into();
        assert_eq!(table.get(b'a').unwrap().entry(), Some(&entry(9)));

        let removed = table.remove(b'b').unwrap();
        assert_eq!(removed.tail().as_bytes(), b"b");
        assert_eq!(table.len(), 1);
        assert_eq!(table.remove(b'b'), None);
    }
}
