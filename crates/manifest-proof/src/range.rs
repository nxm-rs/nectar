//! Range-completeness: prove a listing is every key in a half-open range.
//!
//! A single-key proof authenticates one descent; a complete listing needs the
//! whole frontier. This proof ships every trie node whose subtree overlaps
//! `[lo, hi)`, each self-authenticated by content address and chained from the
//! trusted root, and the verifier re-walks that frontier: it emits each present
//! key in order and, at every referenced child that overlaps the range, demands
//! the node that continues it. A withheld node leaves an overlapping edge with
//! no witness, so a listing that omits a key cannot verify, and the gaps are
//! provably empty without a per-gap exclusion proof.
//!
//! The walk needs the whole fork table of each frontier node, so this proof is
//! chunk-granularity throughout; the segment form authenticates a single edge,
//! not a table. Enumeration mirrors the ordered reader (spec 8): a value rides
//! in its fork record, embedded children fold in place, and a referenced child
//! is a hop, so the emitted order is the trie's total key order.

use std::collections::{BTreeMap, BTreeSet};

use nectar_manifest::{Child, Entry, ForkTable, Format, Key, Node};
use nectar_primitives::{ChunkAddress, ChunkOps, ContentChunk, DEFAULT_BODY_SIZE};

use crate::error::{ProveError, VerifyError};
use crate::prove::NodeSource;

/// A complete authenticated listing over a key range: the frontier of trie
/// nodes whose subtrees overlap the range, each addressed by its content hash.
///
/// Carries no key list of its own; [`verify_range`] re-derives the listing from
/// the authenticated nodes, so the proof cannot assert a key its nodes omit.
#[derive(Clone, Debug)]
pub struct RangeProof {
    nodes: Vec<Vec<u8>>,
}

impl RangeProof {
    /// Assemble a proof from its frontier node payloads.
    #[must_use]
    pub const fn new(nodes: Vec<Vec<u8>>) -> Self {
        Self { nodes }
    }

    /// The frontier node payloads, each a content-addressed chunk.
    #[must_use]
    pub fn nodes(&self) -> &[Vec<u8>] {
        &self.nodes
    }

    /// The number of frontier nodes carried.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Whether the proof carries no nodes; never true for one `verify` accepts,
    /// which always authenticates at least the root.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
}

/// One resolved position in a node's ordered contents, keyed by its full key.
enum Item<F: Format> {
    /// A key terminates here with this value.
    Value(Vec<u8>, Entry<F>),
    /// The trie continues into a referenced child at this key prefix.
    Ref(Vec<u8>, ChunkAddress),
    /// The trie continues into an encrypted child the plain walk cannot open.
    Encrypted(Vec<u8>),
}

/// Prove that the keys with `lo <= key < hi` are exactly the listing a
/// [`verify_range`] of this proof yields, under `root`.
///
/// Collects every frontier node the range walk reaches; errors with
/// [`ProveError::Encrypted`] when the range spans an encrypted subtree the
/// plain prover cannot enumerate.
pub fn prove_range_complete<F, S>(
    source: &S,
    root: &ChunkAddress,
    lo: &Key,
    hi: &Key,
) -> Result<RangeProof, ProveError>
where
    F: Format,
    S: NodeSource<F>,
{
    let mut collector = Collector {
        source,
        lo: lo.as_bytes(),
        hi: hi.as_bytes(),
        nodes: Vec::new(),
        seen: BTreeSet::new(),
    };
    collector.collect(root, &[], true)?;
    Ok(RangeProof::new(collector.nodes))
}

/// Verify `proof` over `[lo, hi)` under `root`, returning the authenticated
/// complete listing in ascending key order.
///
/// Every referenced child whose subtree overlaps the range must be carried, so
/// an accepted listing is provably total: a returned `Ok` is the whole range,
/// not a prefix of it.
pub fn verify_range<F: Format>(
    root: &ChunkAddress,
    lo: &Key,
    hi: &Key,
    proof: &RangeProof,
) -> Result<Vec<(Key, Entry<F>)>, VerifyError> {
    let mut map: BTreeMap<ChunkAddress, Node<F>> = BTreeMap::new();
    for payload in proof.nodes() {
        let chunk =
            ContentChunk::<DEFAULT_BODY_SIZE>::new(payload.clone()).map_err(VerifyError::Seal)?;
        let address = *chunk.address();
        let node = Node::<F>::decode(payload).map_err(VerifyError::Decode)?;
        map.insert(address, node);
    }
    let mut walker = Walker {
        map: &map,
        lo: lo.as_bytes(),
        hi: hi.as_bytes(),
        out: Vec::new(),
    };
    walker.walk(root, &[], true)?;
    Ok(walker.out)
}

/// The prove-side frontier walk state: the node source, the range bounds and the
/// nodes gathered so far, keyed to skip a subtree reached twice.
struct Collector<'a, S> {
    source: &'a S,
    lo: &'a [u8],
    hi: &'a [u8],
    nodes: Vec<Vec<u8>>,
    seen: BTreeSet<ChunkAddress>,
}

impl<S> Collector<'_, S> {
    /// Descend from `address`, pushing its node and following every referenced
    /// child whose subtree overlaps the range.
    fn collect<F>(
        &mut self,
        address: &ChunkAddress,
        base: &[u8],
        is_root: bool,
    ) -> Result<(), ProveError>
    where
        F: Format,
        S: NodeSource<F>,
    {
        if !self.seen.insert(*address) {
            return Ok(());
        }
        let node = self
            .source
            .node(address)
            .ok_or(ProveError::NodeMissing(*address))?;
        self.nodes.push(node.encode()?);
        let mut items = Vec::new();
        items_of(&node, base, is_root, &mut items);
        for item in &items {
            match item {
                Item::Ref(prefix, child) if overlaps(prefix, self.lo, self.hi) => {
                    self.collect::<F>(child, prefix, false)?;
                }
                Item::Encrypted(prefix) if overlaps(prefix, self.lo, self.hi) => {
                    return Err(ProveError::Encrypted);
                }
                _ => {}
            }
        }
        Ok(())
    }
}

/// The verify-side frontier walk state: the authenticated node map keyed by
/// content address, the range bounds and the listing built so far.
struct Walker<'a, F: Format> {
    map: &'a BTreeMap<ChunkAddress, Node<F>>,
    lo: &'a [u8],
    hi: &'a [u8],
    out: Vec<(Key, Entry<F>)>,
}

impl<F: Format> Walker<'_, F> {
    /// Walk from `address`, emitting each in-range key and demanding a node for
    /// every overlapping referenced child.
    fn walk(
        &mut self,
        address: &ChunkAddress,
        base: &[u8],
        is_root: bool,
    ) -> Result<(), VerifyError> {
        let node = self
            .map
            .get(address)
            .ok_or(VerifyError::NodeAbsent(*address))?;
        let mut items = Vec::new();
        items_of(node, base, is_root, &mut items);
        for item in items {
            match item {
                Item::Value(key, entry) => {
                    if in_range(&key, self.lo, self.hi) {
                        self.out.push((Key::from(key), entry));
                    }
                }
                Item::Ref(prefix, child) => {
                    if overlaps(&prefix, self.lo, self.hi) {
                        self.walk(&child, &prefix, false)?;
                    }
                }
                Item::Encrypted(prefix) => {
                    if overlaps(&prefix, self.lo, self.hi) {
                        return Err(VerifyError::Encrypted);
                    }
                }
            }
        }
        Ok(())
    }
}

/// A node's ordered contents as full-key items. The root's own value is the
/// empty-key extension, the least key, and leads the list; a fork child node
/// carries no such value of its own.
fn items_of<F: Format>(node: &Node<F>, base: &[u8], is_root: bool, out: &mut Vec<Item<F>>) {
    if is_root && let Some(entry) = node.entry() {
        out.push(Item::Value(base.to_vec(), entry.clone()));
    }
    let mut prefix = base.to_vec();
    table_items(node.forks(), &mut prefix, out);
}

/// Append each fork's value and continuation in wire order, folding embedded
/// children in place so a whole node flattens without a fetch.
fn table_items<F: Format>(table: &ForkTable<F>, prefix: &mut Vec<u8>, out: &mut Vec<Item<F>>) {
    for (first, record) in table.iter() {
        let mark = prefix.len();
        prefix.push(first);
        prefix.extend_from_slice(record.tail().as_bytes());
        if let Some(entry) = record.entry() {
            out.push(Item::Value(prefix.clone(), entry.clone()));
        }
        match record.child() {
            Some(Child::Embedded(inner)) => table_items(inner, prefix, out),
            Some(Child::Ref32(reference)) => {
                out.push(Item::Ref(prefix.clone(), *reference.address()));
            }
            Some(Child::Ref64(_)) => out.push(Item::Encrypted(prefix.clone())),
            None => {}
        }
        prefix.truncate(mark);
    }
}

/// Whether the subtree under `prefix` can hold a key in `[lo, hi)`: its keys span
/// `[prefix, successor(prefix))`, so it overlaps unless it sits wholly at or past
/// `hi` or wholly below `lo`.
fn overlaps(prefix: &[u8], lo: &[u8], hi: &[u8]) -> bool {
    prefix < hi && successor(prefix).is_none_or(|end| end.as_slice() > lo)
}

/// Whether `key` falls in the half-open range `[lo, hi)`.
fn in_range(key: &[u8], lo: &[u8], hi: &[u8]) -> bool {
    lo <= key && key < hi
}

/// The least byte string strictly greater than every string starting with
/// `prefix`: increment the last sub-`0xFF` byte after dropping the trailing
/// `0xFF` run. `None` when the prefix is empty or all `0xFF`, i.e. unbounded.
fn successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut bytes = prefix.to_vec();
    loop {
        match bytes.last() {
            None => return None,
            Some(&0xFF) => {
                bytes.pop();
            }
            Some(&last) => {
                if let Some(slot) = bytes.last_mut() {
                    *slot = last.saturating_add(1);
                }
                return Some(bytes);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn successor_bounds_the_prefix_subtree() {
        assert_eq!(successor(b"ab").as_deref(), Some(&b"ac"[..]));
        assert_eq!(successor(b"a\xff").as_deref(), Some(&b"b"[..]));
        assert_eq!(successor(b"\xff\xff"), None);
        assert_eq!(successor(b""), None);
    }

    #[test]
    fn overlap_and_range_are_half_open() {
        assert!(overlaps(b"a", b"a", b"b"));
        assert!(!overlaps(b"b", b"a", b"b"));
        assert!(!overlaps(b"\x00", b"a", b"b"));
        assert!(in_range(b"a", b"a", b"b"));
        assert!(!in_range(b"b", b"a", b"b"));
        assert!(!in_range(b"\x00", b"a", b"b"));
    }
}
