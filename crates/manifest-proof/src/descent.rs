//! Authenticated descent over a node's raw payload bytes.
//!
//! One engine drives both proof granularities: the verifier hands it the bytes
//! it has authenticated for a node (a whole chunk, or a contiguous run of
//! BMT-authenticated leading segments) and it re-derives the single next step
//! the trie takes for the key. Embedded children live in the parent's bytes, so
//! the walk crosses them without a step; only a referenced edge yields a hop.
//!
//! The parse restates just the node-body grammar the descent reads (preamble,
//! flags, fork index, the followed record); soundness rests on canonicalness
//! (spec 6.2): an authenticated node's fork set is complete and edges maximal,
//! so the index entry for a byte is the whole story for that byte.

use nectar_manifest::{Entry, Format, InlineValue};
use nectar_primitives::wire::{Cursor, Underrun};
use nectar_primitives::{ChunkAddress, ChunkRef, EncryptedChunkRef};

/// Flags-byte facts of the node body grammar (spec 5.1). Restated here because
/// they are wire structure, not a tunable the manifest crate exports.
mod flag {
    /// Bits 0-1: the entry presence and width discriminant.
    pub(super) const ENTRY_MASK: u8 = 0b0000_0011;
    /// Bits 2-3: the child presence and width discriminant.
    pub(super) const CHILD_MASK: u8 = 0b0000_1100;
    /// Bit 4: a metadata block follows.
    pub(super) const HAS_META: u8 = 0b0001_0000;
    /// Bit 5: the fork table is a segment directory.
    pub(super) const SEGMENTED: u8 = 0b0010_0000;
    /// Bit 6: the body is a segment, not a node.
    pub(super) const SEGMENT: u8 = 0b0100_0000;
}

/// A two-bit reference/value width discriminant.
#[derive(Clone, Copy)]
enum Width {
    /// Absent field.
    None,
    /// A plain 32-byte reference.
    Ref32,
    /// An encrypted 64-byte reference.
    Ref64,
    /// Length-prefixed inline bytes, or an embedded child body.
    Inline,
}

/// The entry-position (bits 0-1) width of a flags byte.
const fn entry_width(flags: u8) -> Width {
    match flags & flag::ENTRY_MASK {
        0b01 => Width::Ref32,
        0b10 => Width::Ref64,
        0b11 => Width::Inline,
        _ => Width::None,
    }
}

/// The child-position (bits 2-3) width of a flags byte.
const fn child_width(flags: u8) -> Width {
    match flags & flag::CHILD_MASK {
        0b0100 => Width::Ref32,
        0b1000 => Width::Ref64,
        0b1100 => Width::Inline,
        _ => Width::None,
    }
}

/// A rejection while reading a node's authenticated bytes.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum DescentError {
    /// The authenticated bytes ended inside a field the descent had to read;
    /// for a segment proof this is a short segment run, not a malformed node.
    #[error(transparent)]
    Underrun(#[from] Underrun),
    /// The payload does not open with this format's preamble.
    #[error("not a mantaray 1.0 manifest")]
    NotAManifest,
    /// The node is a spilled segment or segment directory; a proof over a
    /// spilled node is out of this crate's scope.
    #[error("spilled node on the proof path")]
    Spilled,
    /// A fork prefix length of zero.
    #[error("empty fork prefix")]
    EmptyPrefix,
    /// An inline value longer than the format admits.
    #[error("inline value over the format bound")]
    ValueTooLong,
}

/// Where the descent through one node lands for the key.
#[derive(Debug)]
pub(crate) enum Step<F: Format> {
    /// The key terminates in this node with this value.
    Found(Entry<F>),
    /// The key continues into a referenced child at this address, with this
    /// many key bytes consumed.
    Follow(ChunkAddress, usize),
    /// No authenticated continuation reaches the key from this node.
    Absent,
    /// The key funnels into an encrypted child; following it needs a key this
    /// plain descent does not carry.
    Encrypted,
}

/// A fresh cursor over `bytes` advanced past its first `offset` bytes.
fn seek(bytes: &[u8], offset: usize) -> Result<Cursor<'_>, Underrun> {
    let mut cur = Cursor::new(bytes);
    let _ = cur.take_slice(offset)?;
    Ok(cur)
}

/// The little-endian u16 the node grammar uses for every count, offset and
/// length, widened for arithmetic.
fn take_u16(cur: &mut Cursor<'_>) -> Result<usize, Underrun> {
    cur.take::<[u8; 2]>()
        .map(|b| usize::from(u16::from_le_bytes(b)))
}

/// The absolute offset a cursor over the whole buffer has reached.
const fn offset(bytes: &[u8], cur: &Cursor<'_>) -> usize {
    bytes.len().saturating_sub(cur.remaining().len())
}

/// Raise the high-water mark to `to`.
fn bump(reached: &mut usize, to: usize) {
    *reached = (*reached).max(to);
}

/// Read a width-tagged reference or value, advancing past it. Returns the value
/// only when the caller may terminate on it; the bytes are consumed regardless
/// so the cursor lands on whatever follows.
fn take_value<F: Format>(
    cur: &mut Cursor<'_>,
    width: Width,
) -> Result<Option<Entry<F>>, DescentError> {
    Ok(match width {
        Width::None => None,
        Width::Ref32 => {
            let bytes = cur.take::<[u8; ChunkRef::SIZE]>()?;
            Some(Entry::Ref32(ChunkRef::new(ChunkAddress::new(bytes))))
        }
        Width::Ref64 => {
            let bytes = cur.take::<[u8; EncryptedChunkRef::SIZE]>()?;
            Some(Entry::Ref64(EncryptedChunkRef::from(bytes)))
        }
        Width::Inline => {
            let len = usize::from(cur.take::<u8>()?);
            let bytes = cur.take_slice(len)?;
            let value = InlineValue::try_from(bytes).map_err(|_| DescentError::ValueTooLong)?;
            Some(Entry::Inline(value))
        }
    })
}

/// Descend one node for `key` from key position `pos`, over bytes already
/// authenticated against the node's address.
///
/// `reached` records the highest byte offset the descent read, so a prover can
/// ship exactly the covering leading segments; a verifier passes a scratch it
/// ignores.
pub(crate) fn descend<F: Format>(
    bytes: &[u8],
    key: &[u8],
    pos: usize,
    reached: &mut usize,
) -> Result<Step<F>, DescentError> {
    let mut cur = Cursor::new(bytes);
    if cur.take::<[u8; 2]>()? != F::PREAMBLE {
        return Err(DescentError::NotAManifest);
    }
    let flags = cur.take::<u8>()?;
    if flags & (flag::SEGMENT | flag::SEGMENTED) != 0 {
        return Err(DescentError::Spilled);
    }
    // The root extension: an entry then a metadata block, each gated by the
    // flags. A fork-child node carries none, so its flags leave both absent.
    let root_entry = take_value::<F>(&mut cur, entry_width(flags))?;
    if flags & flag::HAS_META != 0 {
        let len = take_u16(&mut cur)?;
        let _ = cur.take_slice(len)?;
    }
    let table_start = offset(bytes, &cur);
    bump(reached, table_start);
    // The empty key, or a key wholly consumed before this node, reads the
    // node's own value.
    if pos >= key.len() {
        return Ok(root_entry.map_or(Step::Absent, Step::Found));
    }
    descend_table::<F>(bytes, table_start, key, pos, reached)
}

/// Walk `key` from `pos` down the fork table rooted at `table_start`, crossing
/// embedded child tables in place and stopping at the first terminal, dead end,
/// or referenced hop.
fn descend_table<F: Format>(
    bytes: &[u8],
    table_start: usize,
    key: &[u8],
    pos: usize,
    reached: &mut usize,
) -> Result<Step<F>, DescentError> {
    let Some(&byte) = key.get(pos) else {
        return Ok(Step::Absent);
    };
    // The fork index: a count then `(first_byte, offset)` slots. Absence of a
    // slot for `byte` is the compact gap that proves no fork continues the key.
    let mut cur = seek(bytes, table_start)?;
    let fcount = take_u16(&mut cur)?;
    let mut record_off = None;
    for _ in 0..fcount {
        let first = cur.take::<u8>()?;
        let off = take_u16(&mut cur)?;
        if first == byte {
            record_off = Some(off);
        }
    }
    let records_start = offset(bytes, &cur);
    bump(reached, records_start);
    let Some(off) = record_off else {
        return Ok(Step::Absent);
    };

    // The record for `byte`: flags, the tail behind its full-prefix length,
    // then the flag-gated entry and child.
    let mut rec = seek(bytes, records_start.saturating_add(off))?;
    let flags = rec.take::<u8>()?;
    let plen = usize::from(rec.take::<u8>()?);
    let Some(tail_len) = plen.checked_sub(1) else {
        return Err(DescentError::EmptyPrefix);
    };
    let tail = rec.take_slice(tail_len)?;
    let start = pos.saturating_add(1);
    let end = start.saturating_add(tail_len);
    // The compacted edge must match byte for byte; a divergence proves the key
    // leaves the trie here.
    match key.get(start..end) {
        Some(matched) if matched == tail => {}
        _ => {
            bump(reached, offset(bytes, &rec));
            return Ok(Step::Absent);
        }
    }
    let newpos = end;
    let entry = take_value::<F>(&mut rec, entry_width(flags))?;
    if newpos >= key.len() {
        // The key ends at this fork: its own value, or nothing (a fork that
        // only branches holds no value at its prefix).
        bump(reached, offset(bytes, &rec));
        return Ok(entry.map_or(Step::Absent, Step::Found));
    }
    let step = match child_width(flags) {
        Width::None => Step::Absent,
        Width::Ref32 => {
            let address = ChunkAddress::new(rec.take::<[u8; ChunkRef::SIZE]>()?);
            Step::Follow(address, newpos)
        }
        Width::Ref64 => Step::Encrypted,
        Width::Inline => {
            // An embedded child body: its length, a forced zero flags byte,
            // then its own fork table, all in this node's bytes.
            let _len = take_u16(&mut rec)?;
            let body = offset(bytes, &rec);
            return descend_table::<F>(bytes, body.saturating_add(1), key, newpos, reached);
        }
    };
    bump(reached, offset(bytes, &rec));
    Ok(step)
}
