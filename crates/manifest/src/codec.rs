//! Wire codec: the node grammar over the primitives cursor and writer.
//!
//! Decode is reject-or-accept: every rule checkable from the bytes alone
//! (preamble, flag positions, index order, cumulative offsets, exact spans,
//! metadata order, bounds) rejects here, so an accepted image is canonical
//! as far as one chunk can tell. The cursor is the only fallible byte
//! access; every multi-byte integer is little-endian and states so once, on
//! [`U16Le`]. Emission is total, so [`Node::encode`] validates the packing
//! bounds the types do not carry before any byte is written.

use core::mem::size_of;

use bytes::Bytes;
use nectar_primitives::wire::{Cursor, FromCursor, ToWriter, Underrun, Writer};
use nectar_primitives::{ChunkAddress, ChunkRef, EncryptedChunkRef, EncryptionKey};

use crate::bounded::{MetadataLen, Prefix};
use crate::count::{CountError, SubtreeCount};
use crate::error::{CustomKeyError, ForkPrefixEmpty, MetadataTooLong, PrefixTooLong, ValueTooLong};
use crate::fork::{Child, ForkPayload, ForkRecord, ForkTable};
use crate::format::Format;
use crate::meta::{CustomKey, KeyId, Metadata, MetadataKey};
use crate::node::{Node, RootExtension};
use crate::value::{Entry, InlineValue};

/// Grammar facts of the shared flags byte and the metadata key escape.
///
/// These are structure, not tunable parameters: changing any of them is a
/// new grammar, so they live here rather than on [`Format`].
struct Wire;

impl Wire {
    /// Bits 0-1: the entry presence and width discriminant.
    const ENTRY_MASK: u8 = 0b0000_0011;
    /// Bits 2-3: the child presence and width discriminant (forks only).
    const CHILD_MASK: u8 = 0b0000_1100;
    /// Bit 4: a metadata block follows.
    const HAS_META: u8 = 0b0001_0000;
    /// Bit 5: the fork table is a segment directory (nodes only).
    const SEGMENTED: u8 = 0b0010_0000;
    /// Bit 6: the body is a segment, not a node (nodes only).
    const SEGMENT: u8 = 0b0100_0000;
    /// Bit 7: reserved, never set.
    const RESERVED: u8 = 0b1000_0000;
    /// The metadata key byte introducing an unregistered key.
    const META_ESCAPE: u8 = 0xFF;
}

/// The two-bit entry/child format discriminant of the flags byte.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WireFmt {
    /// Absent field.
    None,
    /// A plain 32-byte reference.
    Ref32,
    /// An encrypted 64-byte reference.
    Ref64,
    /// Inline bytes: a length-prefixed value, or an embedded child body.
    Inline,
}

impl WireFmt {
    /// The discriminant read from the entry position (bits 0-1).
    const fn from_entry(flags: u8) -> Self {
        match flags & Wire::ENTRY_MASK {
            0b01 => Self::Ref32,
            0b10 => Self::Ref64,
            0b11 => Self::Inline,
            _ => Self::None,
        }
    }

    /// The discriminant read from the child position (bits 2-3).
    const fn from_child(flags: u8) -> Self {
        match flags & Wire::CHILD_MASK {
            0b0100 => Self::Ref32,
            0b1000 => Self::Ref64,
            0b1100 => Self::Inline,
            _ => Self::None,
        }
    }

    /// The discriminant bits in the entry position.
    const fn entry_bits(self) -> u8 {
        match self {
            Self::None => 0b00,
            Self::Ref32 => 0b01,
            Self::Ref64 => 0b10,
            Self::Inline => 0b11,
        }
    }

    /// The discriminant bits in the child position.
    const fn child_bits(self) -> u8 {
        match self {
            Self::None => 0b0000,
            Self::Ref32 => 0b0100,
            Self::Ref64 => 0b1000,
            Self::Inline => 0b1100,
        }
    }

    /// The discriminant an entry encodes as.
    const fn of_entry<F: Format>(entry: Option<&Entry<F>>) -> Self {
        match entry {
            None => Self::None,
            Some(Entry::Ref32(_)) => Self::Ref32,
            Some(Entry::Ref64(_)) => Self::Ref64,
            Some(Entry::Inline(_)) => Self::Inline,
        }
    }

    /// The discriminant a child encodes as.
    const fn of_child<F: Format>(child: Option<&Child<F>>) -> Self {
        match child {
            None => Self::None,
            Some(Child::Ref32(_)) => Self::Ref32,
            Some(Child::Ref64(_)) => Self::Ref64,
            Some(Child::Embedded(_)) => Self::Inline,
        }
    }
}

/// A little-endian u16 wire field: the format's only multi-byte integer
/// width, so every count, offset and length states its byte order here.
#[derive(Clone, Copy, Debug)]
struct U16Le(u16);

impl U16Le {
    /// The value widened for length arithmetic.
    fn get(self) -> usize {
        usize::from(self.0)
    }

    /// Narrows `len` for emission. Every caller's value is held within u16
    /// by a type invariant or by [`Node::encode`]'s validation; the
    /// saturating fallback keeps emission total and fails the decode span
    /// checks instead of panicking.
    fn of(len: usize) -> Self {
        Self(u16::try_from(len).unwrap_or(u16::MAX))
    }
}

impl FromCursor for U16Le {
    type Error = Underrun;

    fn take_from(cur: &mut Cursor<'_>) -> Result<Self, Underrun> {
        cur.take::<[u8; size_of::<u16>()]>()
            .map(|bytes| Self(u16::from_le_bytes(bytes)))
    }
}

impl ToWriter for U16Le {
    fn put_into(&self, w: &mut Writer<'_>) {
        w.put(&self.0.to_le_bytes());
    }
}

/// Narrows `len` for a one-byte wire length. Callers hold the bound by a
/// type invariant; the saturating fallback keeps emission total and fails
/// the decode span checks instead of panicking.
fn len_byte(len: usize) -> u8 {
    u8::try_from(len).unwrap_or(u8::MAX)
}

/// Rejections from [`Node::decode`]: the image violates the grammar or a
/// canonical-form rule checkable from the bytes alone.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DecodeError {
    /// The buffer ended inside a field.
    #[error(transparent)]
    Underrun(#[from] Underrun),
    /// The payload does not open with this format's magic and version; no
    /// other format is co-decoded.
    #[error("not a mantaray 1.0 manifest: preamble {found:02X?}")]
    NotAManifest {
        /// The two bytes found in place of the preamble.
        found: [u8; 2],
    },
    /// A node flags byte sets reserved or position-illegal bits.
    #[error("illegal node flags {0:#04x}")]
    NodeFlags(u8),
    /// The flags declare a segment or segment-directory body; those carry
    /// the packing grammar, not the plain node grammar decoded here.
    #[error("segmented body: flags {0:#04x}")]
    Segmented(u8),
    /// A fork flags byte sets node-only or reserved bits, or declares
    /// neither entry nor child.
    #[error("illegal fork flags {0:#04x}")]
    ForkFlags(u8),
    /// The fork count exceeds the format's `FORKS_MAX`.
    #[error("fork count {0} exceeds the format maximum")]
    ForkCount(usize),
    /// Fork index keys are not strictly ascending.
    #[error("fork index keys not strictly ascending")]
    ForkIndexOrder,
    /// Fork record offsets are not cumulative from zero.
    #[error("fork record offsets not cumulative from zero")]
    ForkOffsets,
    /// A fork record did not consume exactly its indexed span.
    #[error("fork record consumed {consumed} bytes of its {span}-byte span")]
    RecordSpan {
        /// The span the index assigns to the record.
        span: usize,
        /// The bytes the record parse consumed.
        consumed: usize,
    },
    /// A fork prefix length byte of zero.
    #[error(transparent)]
    EmptyPrefix(#[from] ForkPrefixEmpty),
    /// A prefix length over the format's `PLEN_MAX`.
    #[error(transparent)]
    PrefixTooLong(#[from] PrefixTooLong),
    /// An inline value length over the format's `VINLINE_MAX`.
    #[error(transparent)]
    ValueTooLong(#[from] ValueTooLong),
    /// A metadata length of zero: absence travels as an unset flag bit,
    /// never as an in-band empty block.
    #[error("metadata block is empty")]
    MetadataEmpty,
    /// A metadata length over the format's `META_MAX`.
    #[error(transparent)]
    MetadataTooLong(#[from] MetadataTooLong),
    /// Metadata pairs out of wire order or duplicated.
    #[error("metadata pairs not strictly ascending")]
    MetadataOrder,
    /// A reserved or unassigned metadata key id.
    #[error("reserved metadata key id {0:#04x}")]
    MetadataKeyId(u8),
    /// A malformed custom metadata key.
    #[error(transparent)]
    CustomKey(#[from] CustomKeyError),
    /// An embedded child length of zero or over the format's `INLINE_MAX`.
    #[error("embedded child length {0} outside the format bounds")]
    EmbeddedLen(usize),
    /// An embedded child body with nonzero flags: no root extension and no
    /// segmentation can ride inside a parent.
    #[error("embedded child flags {0:#04x}, expected 0x00")]
    EmbeddedFlags(u8),
    /// An embedded child with no forks: empty nodes are never referenced or
    /// embedded.
    #[error("embedded child has no forks")]
    EmbeddedEmpty,
    /// Bytes remain after the body's last record.
    #[error("{0} trailing bytes after the body")]
    Trailing(usize),
    /// A segment directory sflags byte sets a reserved bit.
    #[error("illegal segment directory flags {0:#04x}")]
    SegmentFlags(u8),
    /// A segment directory descriptor count of zero or over `FORKS_MAX`.
    #[error("segment descriptor count {0} outside the format bounds")]
    SegmentCount(usize),
    /// Segment directory first keys are not strictly ascending.
    #[error("segment directory keys not strictly ascending")]
    SegmentOrder,
    /// A reference reached a bare segment, or a directory nested past depth two;
    /// segments are plumbing, never a node a fork points at (spec 5.3).
    #[error("segment reached out of directory context")]
    SegmentContext,
    /// A counted-grammar subtree count was malformed: overlong, over-wide, or
    /// short.
    #[error(transparent)]
    Count(#[from] CountError),
}

/// Rejections from [`Node::encode`]: the in-memory tree cannot form a legal
/// wire image.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum EncodeError {
    /// The node body exceeds the format's `BUDGET`; an oversized node is
    /// the packing layer's input, never a wire image.
    #[error("node body {len} exceeds the format budget {max}")]
    OverBudget {
        /// The body length in bytes.
        len: usize,
        /// The format's `BUDGET`.
        max: usize,
    },
    /// An embedded child body exceeds the format's `INLINE_MAX`.
    #[error("embedded child body {len} exceeds the format maximum {max}")]
    EmbeddedOversize {
        /// The embedded body length in bytes.
        len: usize,
        /// The format's `INLINE_MAX`.
        max: usize,
    },
    /// An embedded child has no forks; an empty table has no wire form
    /// inside a parent.
    #[error("embedded child has no forks")]
    EmbeddedEmpty,
}

/// Exact encoded bytes of an entry in its declared format.
const fn entry_len<F: Format>(entry: &Entry<F>) -> usize {
    match entry {
        Entry::Ref32(_) => ChunkRef::SIZE,
        Entry::Ref64(_) => EncryptedChunkRef::SIZE,
        Entry::Inline(value) => size_of::<u8>().saturating_add(value.len()),
    }
}

/// Exact encoded bytes of a metadata block: the length field plus the pairs.
const fn meta_len<F: Format>(metadata: &Metadata<F>) -> usize {
    size_of::<u16>().saturating_add(metadata.encoded_len().get())
}

/// Exact encoded bytes of a child in its declared format.
fn child_len<F: Format>(child: &Child<F>) -> usize {
    match child {
        Child::Ref32(_) => ChunkRef::SIZE,
        Child::Ref64(_) => EncryptedChunkRef::SIZE,
        Child::Embedded(table) => size_of::<u16>().saturating_add(embedded_len(table)),
    }
}

/// Exact encoded bytes of an embedded child body: its forced zero flags
/// byte plus its fork table.
fn embedded_len<F: Format>(table: &ForkTable<F>) -> usize {
    size_of::<u8>().saturating_add(table_len(table))
}

/// Exact encoded bytes of one fork record, including any trailing count.
fn record_len<F: Format>(record: &ForkRecord<F>) -> usize {
    let mut len = size_of::<u8>() // fflags
        .saturating_add(size_of::<u8>()) // plen
        .saturating_add(record.tail().len());
    if let Some(entry) = record.entry() {
        len = len.saturating_add(entry_len(entry));
    }
    if let Some(child) = record.child() {
        len = len.saturating_add(child_len(child));
    }
    if let Some(metadata) = record.metadata() {
        len = len.saturating_add(meta_len(metadata));
    }
    len.saturating_add(child_count_len(record))
}

/// The trailing count bytes a fork record carries: a referenced child's
/// `child_count`; an embedded or leaf fork carries none.
fn child_count_len<F: Format>(record: &ForkRecord<F>) -> usize {
    if record.child().is_some_and(Child::is_reference) {
        record.child_count().unwrap_or_default().wire_len()
    } else {
        0
    }
}

/// The subtree key-count of one fork: its terminal key, plus its child's count.
/// A referenced child's count is the stored annotation; an embedded child's is
/// walked in place, so a node's total is the in-buffer sum of its fork counts.
pub(crate) fn fork_count<F: Format>(record: &ForkRecord<F>) -> u64 {
    let entry = u64::from(record.entry().is_some());
    let child = match record.child() {
        None => 0,
        Some(Child::Embedded(table)) => table_count(table),
        Some(Child::Ref32(_) | Child::Ref64(_)) => {
            record.child_count().map_or(0, SubtreeCount::get)
        }
    };
    entry.saturating_add(child)
}

/// The subtree key-count of a fork table: the sum of its forks' counts.
pub(crate) fn table_count<F: Format>(table: &ForkTable<F>) -> u64 {
    table
        .iter()
        .map(|(_, record)| fork_count(record))
        .fold(0, u64::saturating_add)
}

/// Exact encoded bytes of a fork table: count, index, records.
fn table_len<F: Format>(table: &ForkTable<F>) -> usize {
    let slot = size_of::<u8>().saturating_add(size_of::<u16>());
    let mut len = size_of::<u16>().saturating_add(table.len().saturating_mul(slot));
    for (_, record) in table.iter() {
        len = len.saturating_add(record_len(record));
    }
    len
}

/// The packing weight of one fork: its record bytes plus the three-byte fork
/// index slot the record sits behind (spec 5.2).
pub(crate) fn record_weight<F: Format>(record: &ForkRecord<F>) -> usize {
    let slot = size_of::<u8>().saturating_add(size_of::<u16>());
    slot.saturating_add(record_len(record))
}

/// Exact encoded bytes of a node body: flags, root extension, fork table.
pub(crate) fn body_len<F: Format>(node: &Node<F>) -> usize {
    let mut len = size_of::<u8>();
    if let Some(entry) = node.entry() {
        len = len.saturating_add(entry_len(entry));
    }
    if let Some(metadata) = node.metadata() {
        len = len.saturating_add(meta_len(metadata));
    }
    len.saturating_add(table_len(node.forks()))
}

/// Walks every embedded child, rejecting the packing bounds the data model
/// cannot rule out: an empty table and a body over `INLINE_MAX`.
fn validate_tables<F: Format>(table: &ForkTable<F>) -> Result<(), EncodeError> {
    for (_, record) in table.iter() {
        if let Some(Child::Embedded(inner)) = record.child() {
            if inner.is_empty() {
                return Err(EncodeError::EmbeddedEmpty);
            }
            validate_tables(inner)?;
            let len = embedded_len(inner);
            if len > F::INLINE_MAX {
                return Err(EncodeError::EmbeddedOversize {
                    len,
                    max: F::INLINE_MAX,
                });
            }
        }
    }
    Ok(())
}

impl<F: Format> Node<F> {
    /// Exact encoded payload length: the preamble plus the body.
    #[must_use]
    pub fn encoded_len(&self) -> usize {
        F::PREAMBLE.len().saturating_add(body_len(self))
    }

    /// Encode this node into its chunk payload.
    ///
    /// Validates the packing bounds the types do not carry (body within
    /// `F::BUDGET`, embedded children non-empty and within `F::INLINE_MAX`),
    /// then writes into a buffer pre-sized to
    /// [`encoded_len`](Self::encoded_len).
    ///
    /// ```
    /// use nectar_manifest::Node;
    ///
    /// let node: Node = Node::empty();
    /// assert_eq!(node.encode()?, [0x6D, 0x01, 0x00, 0x00, 0x00]);
    /// # Ok::<(), nectar_manifest::EncodeError>(())
    /// ```
    pub fn encode(&self) -> Result<Vec<u8>, EncodeError> {
        validate_tables(self.forks())?;
        let body = body_len(self);
        if body > F::BUDGET {
            return Err(EncodeError::OverBudget {
                len: body,
                max: F::BUDGET,
            });
        }
        let mut payload = Vec::with_capacity(self.encoded_len());
        Writer::new(&mut payload).put(self);
        Ok(payload)
    }

    /// Decode a chunk payload, rejecting any non-canonical image.
    ///
    /// Dispatches on the two-byte preamble and fails loud on anything but
    /// `F::PREAMBLE`.
    ///
    /// ```
    /// use nectar_manifest::{DecodeError, Node, V1};
    ///
    /// let node: Node = Node::decode(&[0x6D, 0x01, 0x00, 0x00, 0x00])?;
    /// assert!(node.is_empty());
    /// assert!(matches!(
    ///     Node::<V1>::decode(&[0x6D, 0x02]),
    ///     Err(DecodeError::NotAManifest { .. })
    /// ));
    /// # Ok::<(), DecodeError>(())
    /// ```
    pub fn decode(payload: &[u8]) -> Result<Self, DecodeError> {
        let mut cur = Cursor::new(payload);
        let node = cur.take::<Self>()?;
        if !cur.is_empty() {
            return Err(DecodeError::Trailing(cur.remaining().len()));
        }
        Ok(node)
    }
}

/// Validates a node-position flags byte, passing it through.
const fn node_flags_checked(flags: u8) -> Result<u8, DecodeError> {
    if flags & Wire::RESERVED != 0 {
        return Err(DecodeError::NodeFlags(flags));
    }
    if flags & Wire::SEGMENT != 0 {
        // The only legal segment bodies are exactly the leaf and directory
        // markers; anything else riding the segment bit is illegal outright.
        return if flags == Wire::SEGMENT || flags == (Wire::SEGMENT | Wire::SEGMENTED) {
            Err(DecodeError::Segmented(flags))
        } else {
            Err(DecodeError::NodeFlags(flags))
        };
    }
    if flags & Wire::CHILD_MASK != 0 {
        return Err(DecodeError::NodeFlags(flags));
    }
    if flags & Wire::SEGMENTED != 0 {
        return Err(DecodeError::Segmented(flags));
    }
    Ok(flags)
}

impl<F: Format> FromCursor for Node<F> {
    type Error = DecodeError;

    /// Reads the preamble and the whole node body; the body extends to the
    /// end of the buffer, so a successful read consumes the cursor.
    fn take_from(cur: &mut Cursor<'_>) -> Result<Self, DecodeError> {
        let found = cur.take::<[u8; 2]>()?;
        if found != F::PREAMBLE {
            return Err(DecodeError::NotAManifest { found });
        }
        let flags = node_flags_checked(cur.take::<u8>()?)?;
        take_plain_body(cur, flags)
    }
}

/// Reads a plain node body (root extension then fork table) behind an already
/// validated, already consumed flags byte.
fn take_plain_body<F: Format>(cur: &mut Cursor<'_>, flags: u8) -> Result<Node<F>, DecodeError> {
    let entry = take_entry(cur, WireFmt::from_entry(flags))?;
    let metadata = if flags & Wire::HAS_META != 0 {
        Some(cur.take::<Metadata<F>>()?)
    } else {
        None
    };
    let forks = take_fork_table(cur)?;
    Ok(Node::new(RootExtension::new(entry, metadata), forks))
}

impl<F: Format> ToWriter for Node<F> {
    /// Emits the full payload: preamble, flags, root extension, fork table.
    /// The presence bits are derived from the structure, never stored.
    fn put_into(&self, w: &mut Writer<'_>) {
        w.put(&F::PREAMBLE);
        w.put(&node_flag_bits(self));
        if let Some(entry) = self.entry() {
            w.put(entry);
        }
        if let Some(metadata) = self.metadata() {
            w.put(metadata);
        }
        put_fork_table(w, self.forks());
    }
}

/// The node flags byte, derived from the structure.
const fn node_flag_bits<F: Format>(node: &Node<F>) -> u8 {
    let mut flags = WireFmt::of_entry(node.entry()).entry_bits();
    if node.metadata().is_some() {
        flags |= Wire::HAS_META;
    }
    flags
}

/// The fork flags byte, derived from the structure.
const fn fork_flag_bits<F: Format>(record: &ForkRecord<F>) -> u8 {
    let mut flags = WireFmt::of_entry(record.entry()).entry_bits()
        | WireFmt::of_child(record.child()).child_bits();
    if record.metadata().is_some() {
        flags |= Wire::HAS_META;
    }
    flags
}

/// Reads a fork table that extends to the end of the cursor: the count, the
/// index, then each record cut to exactly its indexed span.
fn take_fork_table<F: Format>(cur: &mut Cursor<'_>) -> Result<ForkTable<F>, DecodeError> {
    let fcount = cur.take::<U16Le>()?.get();
    if fcount > F::FORKS_MAX {
        return Err(DecodeError::ForkCount(fcount));
    }
    let mut index = Vec::with_capacity(fcount);
    let mut previous: Option<u8> = None;
    for _ in 0..fcount {
        let key = cur.take::<u8>()?;
        let off = cur.take::<U16Le>()?.get();
        if previous.is_some_and(|p| p >= key) {
            return Err(DecodeError::ForkIndexOrder);
        }
        previous = Some(key);
        index.push((key, off));
    }

    let region = cur.remaining().len();
    let mut table = ForkTable::new();
    for (i, &(key, off)) in index.iter().enumerate() {
        if i == 0 && off != 0 {
            return Err(DecodeError::ForkOffsets);
        }
        let end = index
            .get(i.saturating_add(1))
            .map_or(region, |&(_, next)| next);
        let span = match end.checked_sub(off) {
            Some(span) if span > 0 => span,
            _ => return Err(DecodeError::ForkOffsets),
        };
        let mut body = Cursor::new(cur.take_slice(span)?);
        let record = body.take::<ForkRecord<F>>()?;
        if !body.is_empty() {
            return Err(DecodeError::RecordSpan {
                span,
                consumed: span.saturating_sub(body.remaining().len()),
            });
        }
        if table.insert_record(key, record).is_some() {
            return Err(DecodeError::ForkIndexOrder);
        }
    }
    if !cur.is_empty() {
        return Err(DecodeError::Trailing(cur.remaining().len()));
    }
    Ok(table)
}

/// Emits the count, the index with offsets cumulative from zero, then the
/// records in ascending key order.
fn put_fork_table<F: Format>(w: &mut Writer<'_>, table: &ForkTable<F>) {
    w.put(&U16Le::of(table.len()));
    let mut off = 0usize;
    for (key, record) in table.iter() {
        w.put(&key);
        w.put(&U16Le::of(off));
        off = off.saturating_add(record_len(record));
    }
    for (_, record) in table.iter() {
        w.put(record);
    }
}

impl<F: Format> FromCursor for ForkRecord<F> {
    type Error = DecodeError;

    /// Reads one record; the fork-table key byte is not part of the record,
    /// so the produced prefix is the tail alone.
    fn take_from(cur: &mut Cursor<'_>) -> Result<Self, DecodeError> {
        let flags = cur.take::<u8>()?;
        if flags & (Wire::RESERVED | Wire::SEGMENTED | Wire::SEGMENT) != 0 {
            return Err(DecodeError::ForkFlags(flags));
        }
        let entry_fmt = WireFmt::from_entry(flags);
        let child_fmt = WireFmt::from_child(flags);
        if entry_fmt == WireFmt::None && child_fmt == WireFmt::None {
            return Err(DecodeError::ForkFlags(flags));
        }
        let tail = cur.take::<Prefix<F>>()?;
        let entry = take_entry(cur, entry_fmt)?;
        let child = take_child(cur, child_fmt)?;
        let metadata = if flags & Wire::HAS_META != 0 {
            Some(cur.take::<Metadata<F>>()?)
        } else {
            None
        };
        let referenced_child = matches!(child_fmt, WireFmt::Ref32 | WireFmt::Ref64);
        let payload = ForkPayload::new(entry, child).ok_or(DecodeError::ForkFlags(flags))?;
        let mut record = Self::from_tail_parts(tail, payload, metadata);
        // The trailing count rides only a referenced child; an embedded or
        // leaf fork recomputes it in place.
        if referenced_child {
            record.set_child_count(Some(cur.take::<SubtreeCount>()?));
        }
        Ok(record)
    }
}

impl<F: Format> ToWriter for ForkRecord<F> {
    /// Emits the flags, the tail behind its count, then the flag-gated
    /// fields, and last the trailing referenced-child count.
    fn put_into(&self, w: &mut Writer<'_>) {
        w.put(&fork_flag_bits(self));
        w.put(self.tail());
        if let Some(entry) = self.entry() {
            w.put(entry);
        }
        if let Some(child) = self.child() {
            put_child(w, child);
        }
        if let Some(metadata) = self.metadata() {
            w.put(metadata);
        }
        if self.child().is_some_and(Child::is_reference) {
            w.put(&self.child_count().unwrap_or_default());
        }
    }
}

impl<F: Format> FromCursor for Prefix<F> {
    type Error = DecodeError;

    /// Reads a fork tail behind its full-prefix count: `plen` counts the
    /// fork-table key byte, so the tail is one byte shorter.
    fn take_from(cur: &mut Cursor<'_>) -> Result<Self, DecodeError> {
        let plen = cur.take::<u8>()?;
        let Some(tail_len) = plen.checked_sub(1) else {
            return Err(ForkPrefixEmpty.into());
        };
        if usize::from(plen) > F::PLEN_MAX {
            return Err(PrefixTooLong {
                actual: usize::from(plen),
                max: F::PLEN_MAX,
            }
            .into());
        }
        Ok(Self::try_from(cur.take_slice(usize::from(tail_len))?)?)
    }
}

impl<F: Format> ToWriter for Prefix<F> {
    /// Emits the fork-tail wire form: the full-prefix count, then the tail
    /// bytes. A record tail is at most `PLEN_MAX - 1`, so the count fits
    /// its byte.
    fn put_into(&self, w: &mut Writer<'_>) {
        w.put(&len_byte(self.len().saturating_add(1)));
        w.put(self.as_bytes());
    }
}

/// Reads an entry in the format its flags declared.
fn take_entry<F: Format>(
    cur: &mut Cursor<'_>,
    fmt: WireFmt,
) -> Result<Option<Entry<F>>, DecodeError> {
    Ok(match fmt {
        WireFmt::None => None,
        WireFmt::Ref32 => Some(Entry::Ref32(cur.take::<ChunkRef>()?)),
        WireFmt::Ref64 => Some(Entry::Ref64(cur.take::<EncryptedChunkRef>()?)),
        WireFmt::Inline => {
            let vlen = usize::from(cur.take::<u8>()?);
            Some(Entry::Inline(InlineValue::try_from(cur.take_slice(vlen)?)?))
        }
    })
}

/// Reads a child in the format its flags declared; an embedded child is an
/// exactly `ilen`-delimited node body with zero flags and at least one fork.
fn take_child<F: Format>(
    cur: &mut Cursor<'_>,
    fmt: WireFmt,
) -> Result<Option<Child<F>>, DecodeError> {
    Ok(match fmt {
        WireFmt::None => None,
        WireFmt::Ref32 => Some(Child::Ref32(cur.take::<ChunkRef>()?)),
        WireFmt::Ref64 => Some(Child::Ref64(cur.take::<EncryptedChunkRef>()?)),
        WireFmt::Inline => {
            let ilen = cur.take::<U16Le>()?.get();
            if ilen == 0 || ilen > F::INLINE_MAX {
                return Err(DecodeError::EmbeddedLen(ilen));
            }
            let mut body = Cursor::new(cur.take_slice(ilen)?);
            let flags = body.take::<u8>()?;
            if flags != 0 {
                return Err(DecodeError::EmbeddedFlags(flags));
            }
            let table = take_fork_table(&mut body)?;
            if table.is_empty() {
                return Err(DecodeError::EmbeddedEmpty);
            }
            Some(Child::Embedded(table))
        }
    })
}

/// Emits a child in its declared format; an embedded child carries its
/// exact length, its forced zero flags byte, then its fork table.
fn put_child<F: Format>(w: &mut Writer<'_>, child: &Child<F>) {
    match child {
        Child::Ref32(reference) => w.put(reference),
        Child::Ref64(reference) => w.put(reference),
        Child::Embedded(table) => {
            w.put(&U16Le::of(embedded_len(table)));
            // An embedded body cannot carry a root extension or
            // segmentation, so its flags byte is always zero.
            w.put(&0u8);
            put_fork_table(w, table);
        }
    }
}

impl<F: Format> ToWriter for Entry<F> {
    /// Emits the entry bytes alone; the format discriminant travels in the
    /// owning flags byte.
    fn put_into(&self, w: &mut Writer<'_>) {
        match self {
            Self::Ref32(reference) => w.put(reference),
            Self::Ref64(reference) => w.put(reference),
            Self::Inline(value) => {
                w.put(&len_byte(value.len()));
                w.put(value.as_bytes());
            }
        }
    }
}

/// Reads one pair key: a registered id, or the escape then a custom key.
fn take_metadata_key<F: Format>(cur: &mut Cursor<'_>) -> Result<MetadataKey<F>, DecodeError> {
    let id = cur.take::<u8>()?;
    if id == Wire::META_ESCAPE {
        let klen = usize::from(cur.take::<u8>()?);
        let key = CustomKey::try_from(cur.take_slice(klen)?)?;
        return Ok(MetadataKey::Custom(key));
    }
    KeyId::from_id(id)
        .map(MetadataKey::Known)
        .ok_or(DecodeError::MetadataKeyId(id))
}

/// Reads one metadata pair: the key, then the length-prefixed value.
fn take_metadata_pair<F: Format>(
    cur: &mut Cursor<'_>,
) -> Result<(MetadataKey<F>, Bytes), DecodeError> {
    let key = take_metadata_key(cur)?;
    let vlen = cur.take::<U16Le>()?.get();
    let value = Bytes::copy_from_slice(cur.take_slice(vlen)?);
    Ok((key, value))
}

impl<F: Format> FromCursor for Metadata<F> {
    type Error = DecodeError;

    /// Reads an `mlen`-delimited block, enforcing sorted-unique pairs and
    /// exact consumption.
    fn take_from(cur: &mut Cursor<'_>) -> Result<Self, DecodeError> {
        let mlen = cur.take::<U16Le>()?.get();
        if mlen == 0 {
            return Err(DecodeError::MetadataEmpty);
        }
        MetadataLen::<F>::new(mlen)?;
        let mut pairs = Cursor::new(cur.take_slice(mlen)?);

        let (key, value) = take_metadata_pair(&mut pairs)?;
        let mut block = Self::new(key.clone(), value)?;
        let mut previous = key;
        while !pairs.is_empty() {
            let (key, value) = take_metadata_pair(&mut pairs)?;
            if previous >= key {
                return Err(DecodeError::MetadataOrder);
            }
            block.insert(key.clone(), value)?;
            previous = key;
        }
        Ok(block)
    }
}

impl<F: Format> ToWriter for Metadata<F> {
    /// Emits `mlen` then the pairs in iteration (wire) order; the block
    /// bound keeps every length within its wire field.
    fn put_into(&self, w: &mut Writer<'_>) {
        w.put(&U16Le::of(self.encoded_len().get()));
        for (key, value) in self.iter() {
            match key {
                MetadataKey::Known(id) => w.put(&id.id()),
                MetadataKey::Custom(custom) => {
                    w.put(&Wire::META_ESCAPE);
                    w.put(&len_byte(custom.len()));
                    w.put(custom.as_bytes());
                }
            }
            w.put(&U16Le::of(value.len()));
            w.put(value.as_ref());
        }
    }
}

// ---------------------------------------------------------------------------
// Segment directories (spec 5.2, 5.3): the wire form an oversized node spills
// into. A leaf segment carries a fork-table fragment; a directory segment
// routes a first-byte range to its children; a SEGMENTED node holds the top
// directory in place of its fork table. Widths are uniform per directory tree.
// ---------------------------------------------------------------------------

/// Node/segment flags for a leaf segment body: the `SEGMENT` bit alone.
const SEG_LEAF: u8 = Wire::SEGMENT;
/// Node/segment flags for a directory segment body: `SEGMENT | SEGMENTED`.
const SEG_DIR: u8 = Wire::SEGMENT | Wire::SEGMENTED;
/// Segment-directory sflags bit 0: descriptors carry ref64, not ref32.
const WIDE_REFS: u8 = 0b0000_0001;

/// One segment-directory descriptor: the first fork key of the child segment
/// it routes to, and the reference that reaches that segment chunk.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SegDesc {
    /// The key of the child segment's first fork; the segment covers keys from
    /// here up to the next descriptor's key.
    pub(crate) first_key: u8,
    /// The child segment chunk address.
    pub(crate) address: ChunkAddress,
    /// The child's decryption key, present iff the directory is wide.
    pub(crate) key: Option<EncryptionKey>,
    /// The sum of the covered forks' subtree counts, so a spilled node routes a
    /// whole segment by rank without fetching it.
    pub(crate) seg_count: SubtreeCount,
}

/// A decoded segment directory: uniform-width descriptors in ascending key
/// order. Directory depth never exceeds two by the frozen bounds (spec 5.4).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SegmentDir {
    /// Whether the descriptors carry ref64; set iff the node's chunks are
    /// encrypted, so a tree's descriptor widths stay uniform.
    pub(crate) wide: bool,
    /// The descriptors, strictly ascending by `first_key`.
    pub(crate) descriptors: Vec<SegDesc>,
}

impl SegmentDir {
    /// A plaintext directory over `(first_key, address)` descriptors, with the
    /// subtree count each descriptor routes.
    pub(crate) fn plain(descriptors: Vec<(u8, ChunkAddress, SubtreeCount)>) -> Self {
        Self {
            wide: false,
            descriptors: descriptors
                .into_iter()
                .map(|(first_key, address, seg_count)| SegDesc {
                    first_key,
                    address,
                    key: None,
                    seg_count,
                })
                .collect(),
        }
    }
}

/// A decoded manifest chunk: a plain node, a segmented node, or a segment.
///
/// Every kind round-trips through [`reencode`](Self::reencode) to its exact
/// bytes, so canonical-form validation is decode-then-re-encode-and-compare
/// (spec 6.2) whatever the chunk kind.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum DecodedChunk<F: Format> {
    /// A one-chunk node: root extension over a fork table.
    Node(Node<F>),
    /// An oversized node: root extension over the top segment directory.
    Segmented(Option<RootExtension<F>>, SegmentDir),
    /// A leaf segment: a fork-table fragment of a spilled node.
    Leaf(ForkTable<F>),
    /// A directory segment: an inner level of a spilled node's directory.
    Directory(SegmentDir),
}

impl<F: Format> DecodedChunk<F> {
    /// Re-encode to the exact chunk bytes; total, so it never rejects a value
    /// decode already accepted.
    pub(crate) fn reencode(&self) -> Vec<u8> {
        match self {
            Self::Node(node) => {
                let mut payload = Vec::with_capacity(node.encoded_len());
                Writer::new(&mut payload).put(node);
                payload
            }
            Self::Segmented(root, dir) => encode_segmented_node::<F>(root.as_ref(), dir),
            Self::Leaf(table) => encode_leaf_segment(table),
            Self::Directory(dir) => encode_dir_segment::<F>(dir),
        }
    }
}

/// Emit a segment directory body: sflags, count, the ascending keys, then the
/// uniform-width references, each trailed by its `seg_count`.
fn put_segment_dir(w: &mut Writer<'_>, dir: &SegmentDir) {
    w.put(&(if dir.wide { WIDE_REFS } else { 0u8 }));
    w.put(&U16Le::of(dir.descriptors.len()));
    for desc in &dir.descriptors {
        w.put(&desc.first_key);
    }
    for desc in &dir.descriptors {
        w.put(desc.address.as_bytes());
        if let Some(key) = &desc.key {
            w.put(key.as_bytes());
        }
        w.put(&desc.seg_count);
    }
}

/// Encode a leaf segment chunk: preamble, the leaf marker, then the fragment's
/// fork table.
pub(crate) fn encode_leaf_segment<F: Format>(table: &ForkTable<F>) -> Vec<u8> {
    let mut payload = Vec::new();
    let mut w = Writer::new(&mut payload);
    w.put(&F::PREAMBLE);
    w.put(&SEG_LEAF);
    put_fork_table(&mut w, table);
    payload
}

/// Encode a directory segment chunk: preamble, the directory marker, then the
/// segment directory.
pub(crate) fn encode_dir_segment<F: Format>(dir: &SegmentDir) -> Vec<u8> {
    let mut payload = Vec::new();
    let mut w = Writer::new(&mut payload);
    w.put(&F::PREAMBLE);
    w.put(&SEG_DIR);
    put_segment_dir(&mut w, dir);
    payload
}

/// Encode a segmented node chunk: preamble, `SEGMENTED` flags with any root
/// extension bits, the root extension, then the top segment directory.
pub(crate) fn encode_segmented_node<F: Format>(
    root: Option<&RootExtension<F>>,
    dir: &SegmentDir,
) -> Vec<u8> {
    let entry = root.and_then(RootExtension::entry);
    let metadata = root.and_then(RootExtension::metadata);
    let mut flags = WireFmt::of_entry(entry).entry_bits() | Wire::SEGMENTED;
    if metadata.is_some() {
        flags |= Wire::HAS_META;
    }
    let mut payload = Vec::new();
    let mut w = Writer::new(&mut payload);
    w.put(&F::PREAMBLE);
    w.put(&flags);
    if let Some(entry) = entry {
        w.put(entry);
    }
    if let Some(metadata) = metadata {
        w.put(metadata);
    }
    put_segment_dir(&mut w, dir);
    payload
}

/// Read a segment directory body behind an already consumed marker.
fn take_segment_dir<F: Format>(cur: &mut Cursor<'_>) -> Result<SegmentDir, DecodeError> {
    let sflags = cur.take::<u8>()?;
    if sflags & !WIDE_REFS != 0 {
        return Err(DecodeError::SegmentFlags(sflags));
    }
    let wide = sflags & WIDE_REFS != 0;
    let scount = cur.take::<U16Le>()?.get();
    if scount == 0 || scount > F::FORKS_MAX {
        return Err(DecodeError::SegmentCount(scount));
    }
    let mut keys = Vec::with_capacity(scount);
    let mut previous: Option<u8> = None;
    for _ in 0..scount {
        let key = cur.take::<u8>()?;
        if previous.is_some_and(|p| p >= key) {
            return Err(DecodeError::SegmentOrder);
        }
        previous = Some(key);
        keys.push(key);
    }
    let mut descriptors = Vec::with_capacity(scount);
    for &first_key in &keys {
        let address = ChunkAddress::new(cur.take::<[u8; ChunkAddress::SIZE]>()?);
        let key = if wide {
            Some(EncryptionKey::from(
                cur.take::<[u8; EncryptionKey::SIZE]>()?,
            ))
        } else {
            None
        };
        let seg_count = cur.take::<SubtreeCount>()?;
        descriptors.push(SegDesc {
            first_key,
            address,
            key,
            seg_count,
        });
    }
    Ok(SegmentDir { wide, descriptors })
}

/// A leaf segment carries a fork table that must hold at least one fork; the
/// empty-map root is never a segment.
fn take_leaf_segment<F: Format>(cur: &mut Cursor<'_>) -> Result<ForkTable<F>, DecodeError> {
    let table = take_fork_table(cur)?;
    if table.is_empty() {
        return Err(DecodeError::EmbeddedEmpty);
    }
    Ok(table)
}

impl<F: Format> Node<F> {
    /// Decode any manifest chunk: a plain node, a segmented node, or a segment.
    ///
    /// Dispatches on the flags byte after the preamble. A plain node decodes
    /// through [`decode`](Self::decode); the segmented and segment bodies carry
    /// the packing grammar and decode here into a [`DecodedChunk`].
    pub(crate) fn decode_chunk(payload: &[u8]) -> Result<DecodedChunk<F>, DecodeError> {
        let mut cur = Cursor::new(payload);
        let found = cur.take::<[u8; 2]>()?;
        if found != F::PREAMBLE {
            return Err(DecodeError::NotAManifest { found });
        }
        let flags = cur.take::<u8>()?;
        let decoded = if flags & Wire::SEGMENT != 0 {
            match flags {
                SEG_LEAF => DecodedChunk::Leaf(take_leaf_segment(&mut cur)?),
                SEG_DIR => DecodedChunk::Directory(take_segment_dir::<F>(&mut cur)?),
                _ => return Err(DecodeError::NodeFlags(flags)),
            }
        } else if flags & Wire::SEGMENTED != 0 {
            if flags & (Wire::RESERVED | Wire::CHILD_MASK) != 0 {
                return Err(DecodeError::NodeFlags(flags));
            }
            let entry = take_entry(&mut cur, WireFmt::from_entry(flags))?;
            let metadata = if flags & Wire::HAS_META != 0 {
                Some(cur.take::<Metadata<F>>()?)
            } else {
                None
            };
            let dir = take_segment_dir::<F>(&mut cur)?;
            DecodedChunk::Segmented(RootExtension::new(entry, metadata), dir)
        } else {
            DecodedChunk::Node(take_plain_body(&mut cur, node_flags_checked(flags)?)?)
        };
        if !cur.is_empty() {
            return Err(DecodeError::Trailing(cur.remaining().len()));
        }
        Ok(decoded)
    }
}

/// Re-encode a manifest chunk to its canonical bytes, whatever its kind.
///
/// The spec's canonical-form check (`encode(decode(bytes)) == bytes`, spec 6.2)
/// for a whole stored chunk set, including the segmented nodes and segments a
/// spilled node produces.
pub fn recanonicalize<F: Format>(payload: &[u8]) -> Result<Vec<u8>, DecodeError> {
    Ok(Node::<F>::decode_chunk(payload)?.reencode())
}

#[cfg(test)]
mod tests {
    use nectar_primitives::EncryptionKey;

    use crate::format::V1;

    use super::*;

    fn addr(byte: u8) -> ChunkAddress {
        ChunkAddress::new([byte; 32])
    }

    fn ref32(byte: u8) -> ChunkRef {
        ChunkRef::new(addr(byte))
    }

    fn ref64(byte: u8) -> EncryptedChunkRef {
        EncryptedChunkRef::new(addr(byte), EncryptionKey::from([byte ^ 0xFF; 32]))
    }

    fn prefix(bytes: &[u8]) -> Prefix {
        Prefix::try_from(bytes).unwrap()
    }

    fn meta(id: KeyId, value: &'static [u8]) -> Metadata {
        Metadata::new(id, Bytes::from_static(value)).unwrap()
    }

    /// A payload from node flags and the bytes that follow them.
    fn body(flags: u8, rest: &[u8]) -> Vec<u8> {
        let mut payload = vec![0x6D, 0x01, flags];
        payload.extend_from_slice(rest);
        payload
    }

    /// A payload with node flags 0x00 and the given fork-table bytes.
    fn with_table(table: &[u8]) -> Vec<u8> {
        body(0x00, table)
    }

    /// A payload with node flags HAS_META, the given metadata bytes, and an
    /// empty fork table.
    fn with_meta(meta_bytes: &[u8]) -> Vec<u8> {
        let mut rest = meta_bytes.to_vec();
        rest.extend_from_slice(&[0x00, 0x00]);
        body(0x10, &rest)
    }

    /// A payload with one fork under `'a'` (plen 1, CHILD_FMT inline) whose
    /// child bytes are given.
    fn with_embedded_child(child_bytes: &[u8]) -> Vec<u8> {
        let mut table = vec![0x01, 0x00, b'a', 0x00, 0x00, 0x0C, 0x01];
        table.extend_from_slice(child_bytes);
        with_table(&table)
    }

    /// A minimal 34-byte record: an entry-only fork with an empty tail and
    /// a ref32 entry of `byte`.
    fn record32(byte: u8) -> Vec<u8> {
        let mut record = vec![0x01, 0x01];
        record.extend_from_slice(&[byte; 32]);
        record
    }

    #[test]
    fn empty_map_root_is_the_five_byte_payload() {
        let node: Node = Node::empty();
        assert_eq!(node.encoded_len(), 5);

        let payload = node.encode().unwrap();
        assert_eq!(payload, [0x6D, 0x01, 0x00, 0x00, 0x00]);

        let decoded: Node = Node::decode(&payload).unwrap();
        assert!(decoded.is_empty());
    }

    // The spec's worked example: a two-file website with an embedded shared
    // child, one 150-byte payload.
    #[test]
    fn worked_example_layout_is_byte_exact() {
        let ref_a = ref32(0xAA); // "index.html"
        let ref_b = ref32(0xBB); // "img/logo.png"

        let mut child = ForkTable::new();
        child
            .insert(
                prefix(b"mg/logo.png"),
                Entry::from(ref_b).into(),
                Some(meta(KeyId::ContentType, b"image/png")),
            )
            .unwrap();
        child
            .insert(
                prefix(b"ndex.html"),
                Entry::from(ref_a).into(),
                Some(meta(KeyId::ContentType, b"text/html")),
            )
            .unwrap();

        let mut forks = ForkTable::new();
        forks
            .insert(prefix(b"i"), Child::Embedded(child).into(), None)
            .unwrap();

        let node: Node = Node::new(
            RootExtension::new(None, Some(meta(KeyId::WebsiteIndexDocument, b"index.html"))),
            forks,
        );

        let payload = node.encode().unwrap();
        assert_eq!(payload.len(), 150);
        assert_eq!(node.encoded_len(), 150);

        let mut expected = vec![0x6D, 0x01, 0x10, 0x0D, 0x00, 0x03, 0x0A, 0x00];
        expected.extend_from_slice(b"index.html");
        expected.extend_from_slice(&[0x01, 0x00, b'i', 0x00, 0x00, 0x0C, 0x01, 0x7B, 0x00]);
        expected.extend_from_slice(&[0x00, 0x02, 0x00, b'm', 0x00, 0x00, b'n', 0x3A, 0x00]);
        expected.extend_from_slice(&[0x11, 0x0B]);
        expected.extend_from_slice(b"g/logo.png");
        expected.extend_from_slice(&[0xBB; 32]);
        expected.extend_from_slice(&[0x0C, 0x00, 0x01, 0x09, 0x00]);
        expected.extend_from_slice(b"image/png");
        expected.extend_from_slice(&[0x11, 0x09]);
        expected.extend_from_slice(b"dex.html");
        expected.extend_from_slice(&[0xAA; 32]);
        expected.extend_from_slice(&[0x0C, 0x00, 0x01, 0x09, 0x00]);
        expected.extend_from_slice(b"text/html");
        assert_eq!(payload, expected);

        let decoded: Node = Node::decode(&payload).unwrap();
        assert_eq!(decoded, node);
        assert_eq!(decoded.encode().unwrap(), payload);
    }

    #[test]
    fn round_trips_every_entry_and_child_format() {
        let mut sub = ForkTable::new();
        sub.insert(prefix(b"x"), Entry::from(ref32(3)).into(), None)
            .unwrap();

        let mut forks = ForkTable::new();
        forks
            .insert(prefix(b"a32"), Entry::from(ref32(1)).into(), None)
            .unwrap();
        forks
            .insert(
                prefix(b"b64"),
                Entry::from(ref64(2)).into(),
                Some(meta(KeyId::Filename, b"b")),
            )
            .unwrap();
        forks
            .insert(
                prefix(b"cin"),
                Entry::inline(Bytes::from_static(b"inline value"))
                    .unwrap()
                    .into(),
                None,
            )
            .unwrap();
        forks
            .insert(
                prefix(b"dboth"),
                ForkPayload::new(Some(Entry::from(ref32(4))), Some(Child::from(ref32(5)))).unwrap(),
                None,
            )
            .unwrap();
        forks
            .insert(prefix(b"e"), Child::from(ref64(6)).into(), None)
            .unwrap();
        forks
            .insert(prefix(b"f"), Child::Embedded(sub).into(), None)
            .unwrap();

        let node: Node = Node::new(
            RootExtension::new(
                Some(Entry::inline(Bytes::from_static(b"root")).unwrap()),
                Some(meta(KeyId::WebsiteErrorDocument, b"404.html")),
            ),
            forks,
        );

        let payload = node.encode().unwrap();
        assert_eq!(payload.len(), node.encoded_len());

        let decoded: Node = Node::decode(&payload).unwrap();
        assert_eq!(decoded, node);
        assert_eq!(decoded.encode().unwrap(), payload);
    }

    #[test]
    fn round_trips_custom_metadata_keys_in_wire_order() {
        let mut block = Metadata::new(
            CustomKey::try_from(&b"note"[..]).unwrap(),
            Bytes::from_static(b"hi"),
        )
        .unwrap();
        block
            .insert(KeyId::ContentType, Bytes::from_static(b"text/plain"))
            .unwrap();
        block.insert(KeyId::Filename, Bytes::new()).unwrap();

        let mut forks = ForkTable::new();
        forks
            .insert(prefix(b"k"), Entry::from(ref32(1)).into(), Some(block))
            .unwrap();
        let node: Node = Node::new(None, forks);

        let payload = node.encode().unwrap();
        let decoded: Node = Node::decode(&payload).unwrap();
        assert_eq!(decoded, node);
    }

    #[test]
    fn a_full_radix_table_round_trips() {
        let mut forks = ForkTable::new();
        for first in u8::MIN..=u8::MAX {
            forks
                .insert(
                    Prefix::try_from(&[first][..]).unwrap(),
                    Entry::inline(Bytes::new()).unwrap().into(),
                    None,
                )
                .unwrap();
        }
        let node: Node = Node::new(None, forks);

        let payload = node.encode().unwrap();
        assert_eq!(payload.len(), node.encoded_len());

        let decoded: Node = Node::decode(&payload).unwrap();
        assert_eq!(decoded.forks().len(), V1::FORKS_MAX);
        assert_eq!(decoded, node);
    }

    #[test]
    fn non_v1_preambles_fail_loud() {
        for bad in [
            [0x00, 0x01],
            [0x6D, 0x00],
            [0x6D, 0x02],
            [0x4E, 0x01],
            [0xFF, 0xFF],
        ] {
            let mut payload = bad.to_vec();
            payload.extend_from_slice(&[0x00, 0x00, 0x00]);
            assert!(matches!(
                Node::<V1>::decode(&payload),
                Err(DecodeError::NotAManifest { found }) if found == bad
            ));
        }
    }

    #[test]
    fn every_truncation_of_a_valid_payload_rejects() {
        let mut forks = ForkTable::new();
        forks
            .insert(
                prefix(b"index.html"),
                Entry::from(ref32(1)).into(),
                Some(meta(KeyId::ContentType, b"text/html")),
            )
            .unwrap();
        let node: Node = Node::new(RootExtension::new(Some(Entry::from(ref64(2))), None), forks);
        let payload = node.encode().unwrap();

        for len in 0..payload.len() {
            assert!(
                Node::<V1>::decode(&payload[..len]).is_err(),
                "length {len} must reject"
            );
        }
    }

    #[test]
    fn node_flag_positions_reject() {
        // Reserved bit.
        assert!(matches!(
            Node::<V1>::decode(&body(0x80, &[0x00, 0x00])),
            Err(DecodeError::NodeFlags(0x80))
        ));
        // Child bits are fork-only.
        assert!(matches!(
            Node::<V1>::decode(&body(0x04, &[0x00, 0x00])),
            Err(DecodeError::NodeFlags(0x04))
        ));
        // Segment markers and the segment directory carry the packing
        // grammar.
        for flags in [0x40, 0x60, 0x20] {
            assert!(matches!(
                Node::<V1>::decode(&body(flags, &[0x00, 0x00])),
                Err(DecodeError::Segmented(found)) if found == flags
            ));
        }
        // The segment bit with any other bit is illegal outright.
        assert!(matches!(
            Node::<V1>::decode(&body(0x41, &[0x00, 0x00])),
            Err(DecodeError::NodeFlags(0x41))
        ));
    }

    #[test]
    fn fork_count_over_max_rejects() {
        assert!(matches!(
            Node::<V1>::decode(&with_table(&[0x01, 0x01])),
            Err(DecodeError::ForkCount(257))
        ));
    }

    #[test]
    fn fork_index_keys_must_strictly_ascend() {
        for (first, second) in [(b'b', b'a'), (b'a', b'a')] {
            let mut table = vec![0x02, 0x00, first, 0x00, 0x00, second, 0x22, 0x00];
            table.extend(record32(1));
            table.extend(record32(2));
            assert!(matches!(
                Node::<V1>::decode(&with_table(&table)),
                Err(DecodeError::ForkIndexOrder)
            ));
        }
    }

    #[test]
    fn fork_offsets_must_be_cumulative_from_zero() {
        // off[0] != 0.
        let mut table = vec![0x01, 0x00, b'a', 0x01, 0x00];
        table.extend(record32(1));
        assert!(matches!(
            Node::<V1>::decode(&with_table(&table)),
            Err(DecodeError::ForkOffsets)
        ));

        // Non-increasing offsets.
        let mut table = vec![0x02, 0x00, b'a', 0x00, 0x00, b'b', 0x00, 0x00];
        table.extend(record32(1));
        table.extend(record32(2));
        assert!(matches!(
            Node::<V1>::decode(&with_table(&table)),
            Err(DecodeError::ForkOffsets)
        ));
    }

    #[test]
    fn record_spans_must_be_exact() {
        // One padding byte after a 34-byte record: span 35, consumed 34.
        let mut table = vec![0x01, 0x00, b'a', 0x00, 0x00];
        table.extend(record32(1));
        table.push(0x00);
        assert!(matches!(
            Node::<V1>::decode(&with_table(&table)),
            Err(DecodeError::RecordSpan {
                span: 35,
                consumed: 34
            })
        ));

        // A record cut one byte short of its needs underruns its span.
        let mut table = vec![0x01, 0x00, b'a', 0x00, 0x00];
        let mut record = record32(1);
        record.pop();
        table.extend(record);
        assert!(matches!(
            Node::<V1>::decode(&with_table(&table)),
            Err(DecodeError::Underrun(_))
        ));
    }

    #[test]
    fn fork_flag_positions_reject() {
        for fflags in [0x81, 0x41, 0x21] {
            let table = [0x01, 0x00, b'a', 0x00, 0x00, fflags, 0x01];
            assert!(matches!(
                Node::<V1>::decode(&with_table(&table)),
                Err(DecodeError::ForkFlags(found)) if found == fflags
            ));
        }
        // Neither entry nor child (metadata alone is not a fork).
        let table = [0x01, 0x00, b'a', 0x00, 0x00, 0x10, 0x01];
        assert!(matches!(
            Node::<V1>::decode(&with_table(&table)),
            Err(DecodeError::ForkFlags(0x10))
        ));
    }

    #[test]
    fn zero_plen_rejects() {
        let mut table = vec![0x01, 0x00, b'a', 0x00, 0x00, 0x01, 0x00];
        table.extend_from_slice(&[0x22; 32]);
        assert!(matches!(
            Node::<V1>::decode(&with_table(&table)),
            Err(DecodeError::EmptyPrefix(_))
        ));
    }

    #[test]
    fn inline_value_over_vinline_max_rejects() {
        let mut table = vec![0x01, 0x00, b'a', 0x00, 0x00, 0x03, 0x01, 0x81];
        table.extend_from_slice(&[0x61; 129]);
        assert!(matches!(
            Node::<V1>::decode(&with_table(&table)),
            Err(DecodeError::ValueTooLong(_))
        ));
    }

    #[test]
    fn metadata_length_bounds_reject() {
        assert!(matches!(
            Node::<V1>::decode(&with_meta(&[0x00, 0x00])),
            Err(DecodeError::MetadataEmpty)
        ));
        // mlen 1025 exceeds META_MAX.
        assert!(matches!(
            Node::<V1>::decode(&with_meta(&[0x01, 0x04])),
            Err(DecodeError::MetadataTooLong(_))
        ));
    }

    #[test]
    fn metadata_reserved_ids_reject() {
        for id in [0x00, 0x08, 0x7F, 0x80, 0xFE] {
            let block = [0x04, 0x00, id, 0x01, 0x00, 0x61];
            assert!(matches!(
                Node::<V1>::decode(&with_meta(&block)),
                Err(DecodeError::MetadataKeyId(found)) if found == id
            ));
        }
    }

    #[test]
    fn metadata_pairs_must_strictly_ascend() {
        // Known ids out of order, then duplicated.
        for (first, second) in [(0x02, 0x01), (0x01, 0x01)] {
            let block = [0x06, 0x00, first, 0x00, 0x00, second, 0x00, 0x00];
            assert!(matches!(
                Node::<V1>::decode(&with_meta(&block)),
                Err(DecodeError::MetadataOrder)
            ));
        }
    }

    #[test]
    fn metadata_custom_key_violations_reject() {
        // A registered name behind the escape.
        let mut block = vec![0x0D, 0x00, 0xFF, 0x08];
        block.extend_from_slice(b"filename");
        block.extend_from_slice(&[0x00, 0x00]);
        assert!(matches!(
            Node::<V1>::decode(&with_meta(&block)),
            Err(DecodeError::CustomKey(CustomKeyError::Registered(
                KeyId::Filename
            )))
        ));

        // The empty custom key.
        let block = [0x04, 0x00, 0xFF, 0x00, 0x00, 0x00];
        assert!(matches!(
            Node::<V1>::decode(&with_meta(&block)),
            Err(DecodeError::CustomKey(CustomKeyError::Empty))
        ));
    }

    #[test]
    fn metadata_pair_overrunning_its_block_rejects() {
        // The pair's value length reaches past mlen.
        let block = [0x04, 0x00, 0x01, 0x05, 0x00, 0x61];
        assert!(matches!(
            Node::<V1>::decode(&with_meta(&block)),
            Err(DecodeError::Underrun(_))
        ));
    }

    #[test]
    fn embedded_child_bounds_reject() {
        // ilen 0.
        assert!(matches!(
            Node::<V1>::decode(&with_embedded_child(&[0x00, 0x00])),
            Err(DecodeError::EmbeddedLen(0))
        ));
        // ilen 1537 exceeds INLINE_MAX.
        assert!(matches!(
            Node::<V1>::decode(&with_embedded_child(&[0x01, 0x06])),
            Err(DecodeError::EmbeddedLen(1537))
        ));
        // Nonzero embedded flags.
        assert!(matches!(
            Node::<V1>::decode(&with_embedded_child(&[0x03, 0x00, 0x01, 0x00, 0x00])),
            Err(DecodeError::EmbeddedFlags(0x01))
        ));
        // An embedded child with no forks.
        assert!(matches!(
            Node::<V1>::decode(&with_embedded_child(&[0x03, 0x00, 0x00, 0x00, 0x00])),
            Err(DecodeError::EmbeddedEmpty)
        ));
        // Trailing bytes inside the embedded region.
        assert!(matches!(
            Node::<V1>::decode(&with_embedded_child(&[0x04, 0x00, 0x00, 0x00, 0x00, 0xAA])),
            Err(DecodeError::Trailing(1))
        ));
    }

    #[test]
    fn trailing_bytes_after_the_body_reject() {
        assert!(matches!(
            Node::<V1>::decode(&[0x6D, 0x01, 0x00, 0x00, 0x00, 0xFF]),
            Err(DecodeError::Trailing(1))
        ));
    }

    #[test]
    fn encode_rejects_an_empty_embedded_child() {
        let mut forks = ForkTable::new();
        forks
            .insert(prefix(b"a"), Child::Embedded(ForkTable::new()).into(), None)
            .unwrap();
        let node: Node = Node::new(None, forks);
        assert_eq!(node.encode().unwrap_err(), EncodeError::EmbeddedEmpty);
    }

    #[test]
    fn encode_rejects_an_oversized_embedded_child() {
        let mut inner = ForkTable::new();
        for first in 0..2u8 {
            inner
                .insert(
                    Prefix::try_from(&[first][..]).unwrap(),
                    Entry::from(ref32(first)).into(),
                    Some(Metadata::new(KeyId::ContentType, Bytes::from(vec![0x61; 900])).unwrap()),
                )
                .unwrap();
        }
        let mut forks = ForkTable::new();
        forks
            .insert(prefix(b"a"), Child::Embedded(inner).into(), None)
            .unwrap();
        let node: Node = Node::new(None, forks);
        assert!(matches!(
            node.encode().unwrap_err(),
            EncodeError::EmbeddedOversize { len, max }
                if len > V1::INLINE_MAX && max == V1::INLINE_MAX
        ));
    }

    #[test]
    fn encode_rejects_an_over_budget_body() {
        // Four forks, each with a META_MAX metadata block, overrun BUDGET.
        let mut forks = ForkTable::new();
        for first in 0..4u8 {
            forks
                .insert(
                    Prefix::try_from(&[first][..]).unwrap(),
                    Entry::from(ref32(first)).into(),
                    Some(Metadata::new(KeyId::ContentType, Bytes::from(vec![0x61; 1021])).unwrap()),
                )
                .unwrap();
        }
        let node: Node = Node::new(None, forks);
        assert!(matches!(
            node.encode().unwrap_err(),
            EncodeError::OverBudget { len, max } if len > V1::BUDGET && max == V1::BUDGET
        ));
    }

    /// A valid image exercising every entry width, an embedded child, a
    /// nested embedded child, and metadata: the mutation seed below.
    fn rich_payload() -> Vec<u8> {
        let mut inner = ForkTable::new();
        inner
            .insert(prefix(b"deep"), Entry::from(ref32(9)).into(), None)
            .unwrap();

        let mut child = ForkTable::new();
        child
            .insert(
                prefix(b"eaf"),
                ForkPayload::new(Some(Entry::from(ref64(7))), Some(Child::Embedded(inner)))
                    .unwrap(),
                Some(meta(KeyId::Filename, b"leaf")),
            )
            .unwrap();

        let mut forks = ForkTable::new();
        forks
            .insert(prefix(b"a"), Entry::from(ref32(1)).into(), None)
            .unwrap();
        forks
            .insert(
                prefix(b"binline"),
                Entry::inline(Bytes::from_static(b"v")).unwrap().into(),
                None,
            )
            .unwrap();
        forks
            .insert(prefix(b"l"), Child::Embedded(child).into(), None)
            .unwrap();

        let node: Node = Node::new(
            RootExtension::new(
                Some(Entry::from(ref64(2))),
                Some(meta(KeyId::WebsiteIndexDocument, b"index.html")),
            ),
            forks,
        );
        node.encode().unwrap()
    }

    // The decoder parses untrusted network bytes, so no input may panic: the
    // reject-or-accept contract holds for every corruption, not just the
    // canonical images the round-trip tests build. A future edit that trades
    // a cursor read for a bare index or a saturating narrowing for a raw one
    // is caught here rather than in production.
    #[test]
    fn no_adversarial_input_panics() {
        let base = rich_payload();

        // Every single-byte substitution, and the prefix truncation at that
        // point, over the whole image.
        for i in 0..base.len() {
            let _ = Node::<V1>::decode(&base[..i]);
            for byte in 0..=u8::MAX {
                let mut image = base.clone();
                image[i] = byte;
                let _ = Node::<V1>::decode(&image);
            }
        }

        // Every adjacent-pair overwrite with a wide length field: drives the
        // count, offset, and length readers toward their bounds.
        for i in 0..base.len().saturating_sub(1) {
            for hi in [0x00u8, 0x01, 0x02, 0x04, 0x06, 0x10, 0x40, 0xFF] {
                let mut image = base.clone();
                image[i] = 0xFF;
                image[i + 1] = hi;
                let _ = Node::<V1>::decode(&image);
            }
        }

        // Deterministic pseudorandom images, half carrying the real preamble.
        let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for _ in 0..100_000 {
            let len = usize::try_from(next() % 300).unwrap();
            let mut image: Vec<u8> = (0..len)
                .map(|_| u8::try_from(next() & 0xFF).unwrap())
                .collect();
            if len >= 2 && next() & 1 == 0 {
                image[0] = V1::MAGIC;
                image[1] = V1::VERSION;
            }
            let _ = Node::<V1>::decode(&image);
        }
    }

    #[test]
    fn segment_chunks_round_trip_and_classify() {
        let mut leaf_table = ForkTable::new();
        leaf_table
            .insert(prefix(b"ab"), Entry::from(ref32(1)).into(), None)
            .unwrap();
        leaf_table
            .insert(prefix(b"cd"), Entry::from(ref32(2)).into(), None)
            .unwrap();
        let leaf = encode_leaf_segment::<V1>(&leaf_table);
        assert!(matches!(
            Node::<V1>::decode_chunk(&leaf).unwrap(),
            DecodedChunk::Leaf(_)
        ));
        assert_eq!(recanonicalize::<V1>(&leaf).unwrap(), leaf);

        let dir = SegmentDir::plain(vec![
            (b'a', addr(1), SubtreeCount::default()),
            (b'c', addr(2), SubtreeCount::default()),
        ]);
        let directory = encode_dir_segment::<V1>(&dir);
        assert!(matches!(
            Node::<V1>::decode_chunk(&directory).unwrap(),
            DecodedChunk::Directory(_)
        ));
        assert_eq!(recanonicalize::<V1>(&directory).unwrap(), directory);

        let root = RootExtension::new(Some(Entry::from(ref32(9))), None);
        let segmented = encode_segmented_node::<V1>(root.as_ref(), &dir);
        assert!(matches!(
            Node::<V1>::decode_chunk(&segmented).unwrap(),
            DecodedChunk::Segmented(Some(_), _)
        ));
        assert_eq!(recanonicalize::<V1>(&segmented).unwrap(), segmented);
    }

    // A node carrying one referenced-child fork: the child count rides as a
    // trailing uleb128 after the reference.
    #[test]
    fn counted_referenced_child_carries_a_trailing_count() {
        let mut forks: ForkTable<V1> = ForkTable::new();
        forks
            .insert(
                Prefix::try_from(&b"a"[..]).unwrap(),
                Child::Ref32(ChunkRef::new(addr(0x11))).into(),
                None,
            )
            .unwrap();
        forks
            .get_mut(b'a')
            .unwrap()
            .set_child_count(Some(SubtreeCount::new(5)));
        let node = Node::new(None, forks);

        let mut expected = vec![0x6D, 0x01, 0x00, 0x01, 0x00, b'a', 0x00, 0x00, 0x04, 0x01];
        expected.extend_from_slice(&[0x11; 32]);
        expected.push(0x05);
        let payload = node.encode().unwrap();
        assert_eq!(payload, expected);
        assert_eq!(node.encoded_len(), payload.len());

        let decoded = Node::<V1>::decode(&payload).unwrap();
        assert_eq!(decoded, node);
        assert_eq!(
            decoded.forks().get(b'a').unwrap().child_count(),
            Some(SubtreeCount::new(5))
        );
        assert_eq!(fork_count(decoded.forks().get(b'a').unwrap()), 5);
    }

    // A non-canonical (overlong) child count is rejected: canonical minimal
    // length only.
    #[test]
    fn an_overlong_count_rejects() {
        let mut image = vec![0x6D, 0x01, 0x00, 0x01, 0x00, b'a', 0x00, 0x00, 0x04, 0x01];
        image.extend_from_slice(&[0x11; 32]);
        // 0x80 0x00 encodes zero in two bytes: a non-minimal run.
        image.extend_from_slice(&[0x80, 0x00]);
        assert!(matches!(
            Node::<V1>::decode(&image),
            Err(DecodeError::Count(CountError::Overlong))
        ));
    }

    // A segment directory carries a seg_count after every descriptor, and the
    // whole segment chunk set round-trips through recanonicalize.
    #[test]
    fn counted_segments_carry_seg_counts_and_round_trip() {
        let mut leaf: ForkTable<V1> = ForkTable::new();
        leaf.insert(
            Prefix::<V1>::try_from(&b"ab"[..]).unwrap(),
            Entry::from(ref32(1)).into(),
            None,
        )
        .unwrap();
        leaf.insert(
            Prefix::<V1>::try_from(&b"cd"[..]).unwrap(),
            Entry::from(ref32(2)).into(),
            None,
        )
        .unwrap();
        let leaf_chunk = encode_leaf_segment::<V1>(&leaf);
        assert_eq!(recanonicalize::<V1>(&leaf_chunk).unwrap(), leaf_chunk);

        let dir = SegmentDir::plain(vec![
            (b'a', addr(1), SubtreeCount::new(3)),
            (b'c', addr(2), SubtreeCount::new(4)),
        ]);
        let dir_chunk = encode_dir_segment::<V1>(&dir);
        // The descriptor's seg_count follows its address: ...addr(32) 03, addr(32) 04.
        assert_eq!(dir_chunk.last(), Some(&0x04));
        match Node::<V1>::decode_chunk(&dir_chunk).unwrap() {
            DecodedChunk::Directory(decoded) => {
                assert_eq!(decoded.descriptors[0].seg_count, SubtreeCount::new(3));
                assert_eq!(decoded.descriptors[1].seg_count, SubtreeCount::new(4));
            }
            _ => panic!("expected a directory"),
        }
        assert_eq!(recanonicalize::<V1>(&dir_chunk).unwrap(), dir_chunk);
    }

    #[test]
    fn segment_bodies_reject_malformed_images() {
        // A leaf segment with no forks: an empty node never rides a segment.
        assert!(matches!(
            Node::<V1>::decode_chunk(&[0x6D, 0x01, 0x40, 0x00, 0x00]),
            Err(DecodeError::EmbeddedEmpty)
        ));
        // A segment directory with a reserved sflags bit set.
        assert!(matches!(
            Node::<V1>::decode_chunk(&[0x6D, 0x01, 0x60, 0x02]),
            Err(DecodeError::SegmentFlags(0x02))
        ));
        // A segment directory of zero descriptors.
        assert!(matches!(
            Node::<V1>::decode_chunk(&[0x6D, 0x01, 0x60, 0x00, 0x00, 0x00]),
            Err(DecodeError::SegmentCount(0))
        ));
        // The segment bit with an illegal extra bit is neither leaf nor
        // directory.
        assert!(matches!(
            Node::<V1>::decode_chunk(&[0x6D, 0x01, 0x41]),
            Err(DecodeError::NodeFlags(0x41))
        ));
        // Non-ascending directory keys.
        let mut image = vec![0x6D, 0x01, 0x60, 0x00, 0x02, 0x00, b'b', b'a'];
        image.extend_from_slice(&[0x11; 32]);
        image.extend_from_slice(&[0x22; 32]);
        assert!(matches!(
            Node::<V1>::decode_chunk(&image),
            Err(DecodeError::SegmentOrder)
        ));
    }
}
