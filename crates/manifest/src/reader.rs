//! Memory-bounded streaming reader: lazy, fetch-on-demand descent of the trie.
//!
//! A lookup follows one fork per node down the radix-256 trie, fetching only
//! the single child on the key's path at each referenced hop; a whole level is
//! never materialized, so peak retained state is O(depth), not O(level width).
//! The reader holds nothing but the store trait, so a caching store composes
//! beneath it without the reader's knowledge.

use core::marker::PhantomData;

use nectar_primitives::store::MaybeSync;
use nectar_primitives::{ChunkAddress, ChunkOps, ChunkRef};

use crate::codec::{DecodedChunk, SegmentDir};
use crate::fork::{Child, ForkTable};
use crate::format::{Format, V1};
use crate::node::{Node, RootExtension};
use crate::store::{NodeGet, StoreError};
use crate::value::{Entry, Key};

/// A lookup failure.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum ReaderError {
    /// Loading or decoding a node across the store seam failed.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// Descent reached an encrypted subtree; following it needs the
    /// decryption key the plain reader does not carry.
    #[error("descent reached an encrypted subtree")]
    EncryptedChild,
}

/// Lazy trie reader over a trusted node store of format `F`.
///
/// Descent is serial: each referenced hop is one fetch, so a lookup costs
/// O(depth) round trips and retains one node at a time, never a whole level.
#[derive(Clone, Copy, Debug)]
pub struct Reader<S, F: Format = V1> {
    store: S,
    _format: PhantomData<F>,
}

impl<S, F: Format> Reader<S, F> {
    /// Wrap `store` as a reader; compose a caching store here to cache hops.
    #[must_use]
    pub const fn new(store: S) -> Self {
        Self {
            store,
            _format: PhantomData,
        }
    }

    /// The backing store.
    #[must_use]
    pub const fn store(&self) -> &S {
        &self.store
    }

    /// Unwrap the backing store.
    #[must_use]
    pub fn into_store(self) -> S {
        self.store
    }
}

impl<S, F: Format> Reader<S, F>
where
    S: NodeGet + MaybeSync,
{
    /// The value bound to `key` under the manifest rooted at `root`, or `None`
    /// when the key is absent.
    ///
    /// The empty key reads the root extension's own value; every other key
    /// descends the trie, matching each compacted edge byte for byte and
    /// fetching one node per referenced hop.
    pub async fn get(
        &self,
        root: &ChunkAddress,
        key: &Key,
    ) -> Result<Option<Entry<F>>, ReaderError> {
        let key = key.as_bytes();
        let mut address = *root;
        let mut pos = 0usize;
        let mut is_root = true;
        loop {
            let chunk = self.store.get(&address).await.map_err(StoreError::store)?;
            let decoded =
                Node::<F>::decode_chunk(chunk.envelope().data()).map_err(StoreError::Decode)?;
            // The empty key reads the root's own value; a spilled root carries
            // it in the segmented node's bytes just as a plain root does.
            if is_root && key.is_empty() {
                return Ok(root_entry(&decoded));
            }
            let descent = match &decoded {
                DecodedChunk::Node(node) => descend(node.forks(), key, pos),
                DecodedChunk::Segmented(_, dir) => {
                    covering_leaf::<S, F>(&self.store, dir, key, pos)
                        .await?
                        .map_or(Descent::Absent, |table| descend(&table, key, pos))
                }
                DecodedChunk::Leaf(_) | DecodedChunk::Directory(_) => {
                    return Err(ReaderError::Store(StoreError::Decode(
                        crate::codec::DecodeError::SegmentContext,
                    )));
                }
            };
            match descent {
                Descent::Absent => return Ok(None),
                Descent::Found(entry) => return Ok(Some(entry)),
                Descent::Encrypted => return Err(ReaderError::EncryptedChild),
                Descent::Follow(next_address, next) => {
                    address = next_address;
                    pos = next;
                    is_root = false;
                }
            }
        }
    }

    /// The reference of the single chunk that holds exactly the keys carrying
    /// `prefix`, so a folder or prefix listing can be handed off in one
    /// delegation rather than walked.
    ///
    /// The empty prefix selects the whole manifest and returns the root
    /// reference. The result is `None` when no single chunk's key set is
    /// exactly the prefix's: the prefix selects nothing, ends inside an embedded
    /// child, or ends at a fork that also terminates a key. Descent into an
    /// encrypted subtree surfaces as [`ReaderError::EncryptedChild`], since a
    /// plain reference cannot carry it.
    pub async fn subtree(
        &self,
        root: &ChunkAddress,
        prefix: &Key,
    ) -> Result<Option<ChunkRef>, ReaderError> {
        Ok(self
            .descend_subtree(root, prefix.as_bytes())
            .await?
            .map(|found| found.reference))
    }

    /// Descend `prefix` to the referenced chunk whose key set it is, tracking
    /// the key bytes consumed to reach that chunk's root.
    ///
    /// Each referenced hop is one fetch, and the boundary chunk itself is never
    /// fetched: its reference is the answer, so the cost is O(depth) fetches
    /// down to the boundary and nothing below it.
    pub(crate) async fn descend_subtree(
        &self,
        root: &ChunkAddress,
        prefix: &[u8],
    ) -> Result<Option<Subtree>, ReaderError> {
        if prefix.is_empty() {
            return Ok(Some(Subtree {
                reference: ChunkRef::new(*root),
                base: 0,
            }));
        }
        let mut address = *root;
        let mut base = 0usize;
        loop {
            let node = self.store.get_node::<F>(&address).await?;
            match subtree_step(node.forks(), prefix, base) {
                SubtreeStep::Absent => return Ok(None),
                SubtreeStep::Encrypted => return Err(ReaderError::EncryptedChild),
                SubtreeStep::Boundary(reference, base) => {
                    return Ok(Some(Subtree { reference, base }));
                }
                SubtreeStep::Descend(next_address, next) => {
                    address = next_address;
                    base = next;
                }
            }
        }
    }
}

/// A prefix's subtree: the chunk holding exactly the prefix's keys, and the key
/// bytes consumed to reach that chunk's root.
pub(crate) struct Subtree {
    /// The subtree chunk's reference.
    pub(crate) reference: ChunkRef,
    /// Key bytes consumed to reach the subtree chunk's root; at least the
    /// prefix length, and equal to it when the prefix ends at the chunk root.
    pub(crate) base: usize,
}

/// Where following `prefix` from `pos` through a chunk's embedded fork tables
/// lands, stopping at the first boundary, referenced hop, or dead end.
enum SubtreeStep {
    /// No single chunk's key set is exactly the prefix's below here.
    Absent,
    /// The prefix funnels into an encrypted child the plain reader cannot open.
    Encrypted,
    /// The prefix's key set is exactly this referenced child's; its reference
    /// and the key bytes consumed to reach the child's root.
    Boundary(ChunkRef, usize),
    /// The prefix continues past this edge into a referenced child at the given
    /// address, with this many key bytes consumed.
    Descend(ChunkAddress, usize),
}

/// Follow `prefix` from `pos` down a node's fork table and its embedded
/// children, stopping where the prefix's key set is a lone referenced child,
/// crosses a referenced edge, or has no single-chunk boundary.
///
/// Stays within one chunk: an embedded child lives in the parent's bytes, so
/// the walk crosses embedded tables without a fetch and only a referenced edge
/// bubbles up as a hop or a boundary.
fn subtree_step<F: Format>(table: &ForkTable<F>, prefix: &[u8], pos: usize) -> SubtreeStep {
    let mut table = table;
    let mut pos = pos;
    loop {
        let Some(&byte) = prefix.get(pos) else {
            return SubtreeStep::Absent;
        };
        let Some(record) = table.get(byte) else {
            return SubtreeStep::Absent;
        };
        let tail = record.tail().as_bytes();
        let Some(start) = pos.checked_add(1) else {
            return SubtreeStep::Absent;
        };
        let Some(end) = start.checked_add(tail.len()) else {
            return SubtreeStep::Absent;
        };
        // The prefix bytes past the fork byte, up to whatever the prefix holds.
        let rest = prefix.get(start..).unwrap_or(&[]);
        if prefix.len() <= end {
            // The prefix ends at or within this edge: a boundary only when the
            // fork is a lone reference whose child holds exactly its key set.
            match tail.get(..rest.len()) {
                Some(head) if head == rest => {}
                _ => return SubtreeStep::Absent,
            }
            if record.entry().is_some() {
                return SubtreeStep::Absent;
            }
            return match record.child() {
                Some(Child::Ref32(reference)) => SubtreeStep::Boundary(*reference, end),
                Some(Child::Ref64(_)) => SubtreeStep::Encrypted,
                Some(Child::Embedded(_)) | None => SubtreeStep::Absent,
            };
        }
        // The prefix passes the edge end: the whole edge must match, then the
        // walk continues into the child.
        match prefix.get(start..end) {
            Some(matched) if matched == tail => {}
            _ => return SubtreeStep::Absent,
        }
        match record.child() {
            Some(Child::Embedded(inner)) => {
                table = inner;
                pos = end;
            }
            Some(Child::Ref32(reference)) => {
                return SubtreeStep::Descend(*reference.address(), end);
            }
            Some(Child::Ref64(_)) => return SubtreeStep::Encrypted,
            None => return SubtreeStep::Absent,
        }
    }
}

/// The empty-key value a decoded root carries: its root extension entry.
fn root_entry<F: Format>(decoded: &DecodedChunk<F>) -> Option<Entry<F>> {
    match decoded {
        DecodedChunk::Node(node) => node.entry().cloned(),
        DecodedChunk::Segmented(root, _) => root.as_ref().and_then(RootExtension::entry).cloned(),
        DecodedChunk::Leaf(_) | DecodedChunk::Directory(_) => None,
    }
}

/// The descriptor covering `byte`: the one with the greatest first key not past
/// it. `None` when `byte` precedes the first fork, so the key is absent.
fn covering_desc(dir: &SegmentDir, byte: u8) -> Option<&crate::codec::SegDesc> {
    dir.descriptors
        .iter()
        .take_while(|desc| desc.first_key <= byte)
        .last()
}

/// Fetch the one leaf segment of a spilled node that covers `key[pos]`,
/// descending at most one intermediate directory level (spec 5.4).
///
/// One segment per directory level, never the whole node, so a lookup through a
/// spilled node stays O(depth) in fetches, not O(node width).
async fn covering_leaf<S, F>(
    store: &S,
    top: &SegmentDir,
    key: &[u8],
    pos: usize,
) -> Result<Option<ForkTable<F>>, ReaderError>
where
    S: NodeGet + MaybeSync,
    F: Format,
{
    let Some(&byte) = key.get(pos) else {
        return Ok(None);
    };
    // Route through the top directory, then, when it points at another
    // directory, once more; a leaf ends the descent.
    let mut current = match covering_desc(top, byte) {
        Some(desc) => desc.clone(),
        None => return Ok(None),
    };
    for _ in 0..2 {
        if current.key.is_some() {
            return Err(ReaderError::EncryptedChild);
        }
        let chunk = store
            .get(&current.address)
            .await
            .map_err(StoreError::store)?;
        match Node::<F>::decode_chunk(chunk.envelope().data()).map_err(StoreError::Decode)? {
            DecodedChunk::Leaf(table) => return Ok(Some(table)),
            DecodedChunk::Directory(dir) => match covering_desc(&dir, byte) {
                Some(desc) => current = desc.clone(),
                None => return Ok(None),
            },
            _ => {
                return Err(ReaderError::Store(StoreError::Decode(
                    crate::codec::DecodeError::SegmentContext,
                )));
            }
        }
    }
    Err(ReaderError::Store(StoreError::Decode(
        crate::codec::DecodeError::SegmentContext,
    )))
}

/// The outcome of walking one node's embedded tables as far as they reach.
enum Descent<F: Format> {
    /// No fork matches the key below this node: the key is absent.
    Absent,
    /// The key terminates here with this value.
    Found(Entry<F>),
    /// The key continues into a referenced child at the given address, with
    /// this many key bytes already consumed.
    Follow(ChunkAddress, usize),
    /// The key continues into an encrypted child the plain reader cannot open.
    Encrypted,
}

/// Walk `key` from `pos` down a node's fork table and its embedded children,
/// stopping at the first terminal, absent branch, or referenced child.
///
/// Stays within one chunk: an embedded child lives in the parent's bytes, so
/// the walk crosses embedded tables without a fetch and only a referenced edge
/// bubbles up as a hop.
fn descend<F: Format>(table: &ForkTable<F>, key: &[u8], pos: usize) -> Descent<F> {
    let mut table = table;
    let mut pos = pos;
    loop {
        let Some(&byte) = key.get(pos) else {
            return Descent::Absent;
        };
        let Some(record) = table.get(byte) else {
            return Descent::Absent;
        };
        // Match the compacted edge: the fork-table byte plus the record tail.
        let tail = record.tail().as_bytes();
        let Some(start) = pos.checked_add(1) else {
            return Descent::Absent;
        };
        let Some(end) = start.checked_add(tail.len()) else {
            return Descent::Absent;
        };
        match key.get(start..end) {
            Some(rest) if rest == tail => {}
            _ => return Descent::Absent,
        }
        pos = end;
        if pos == key.len() {
            return record
                .entry()
                .map_or(Descent::Absent, |entry| Descent::Found(entry.clone()));
        }
        match record.child() {
            Some(Child::Embedded(inner)) => table = inner,
            Some(Child::Ref32(reference)) => return Descent::Follow(*reference.address(), pos),
            Some(Child::Ref64(_)) => return Descent::Encrypted,
            None => return Descent::Absent,
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use nectar_primitives::store::MemoryStore;
    use nectar_primitives::{ChunkAddress, ChunkRef, EncryptedChunkRef, EncryptionKey};
    use nectar_testing::run;

    use crate::bounded::Prefix;
    use crate::fork::{Child, ForkPayload, ForkTable};
    use crate::node::Node;
    use crate::store::NodePut;
    use crate::value::{Entry, Key};

    use super::*;

    fn entry(byte: u8) -> Entry {
        ChunkRef::new(ChunkAddress::new([byte; 32])).into()
    }

    fn prefix(bytes: &[u8]) -> Prefix {
        Prefix::try_from(bytes).unwrap()
    }

    #[test]
    fn descends_embedded_and_referenced_children_alike() {
        let store = MemoryStore::default();

        // A leaf reached by reference, holding the key "img/logo.png".
        let mut leaf = ForkTable::new();
        leaf.insert(prefix(b"logo.png"), entry(0xBB).into(), None)
            .unwrap();
        let leaf_ref = run(store.put_node(&Node::new(None, leaf))).unwrap();

        // The root: "index.html" behind an embedded child, "mg/" behind the
        // referenced leaf.
        let mut embedded = ForkTable::new();
        embedded
            .insert(prefix(b"ndex.html"), entry(0xAA).into(), None)
            .unwrap();
        let mut forks = ForkTable::new();
        forks
            .insert(prefix(b"i"), Child::Embedded(embedded).into(), None)
            .unwrap();
        forks
            .insert(
                prefix(b"mg/"),
                Child::Ref32(ChunkRef::new(leaf_ref)).into(),
                None,
            )
            .unwrap();
        let root = run(store.put_node(&Node::new(None, forks))).unwrap();

        let reader: Reader<_> = Reader::new(&store);
        assert_eq!(
            run(reader.get(&root, &Key::from(&b"index.html"[..]))).unwrap(),
            Some(entry(0xAA)),
        );
        assert_eq!(
            run(reader.get(&root, &Key::from(&b"mg/logo.png"[..]))).unwrap(),
            Some(entry(0xBB)),
        );
        // A key that prefixes an edge without reaching its end is absent.
        assert_eq!(
            run(reader.get(&root, &Key::from(&b"mg/logo"[..]))).unwrap(),
            None,
        );
        // A key past a fork with no matching continuation is absent.
        assert_eq!(
            run(reader.get(&root, &Key::from(&b"other"[..]))).unwrap(),
            None,
        );
    }

    #[test]
    fn a_fork_with_only_a_child_holds_no_value_at_its_own_prefix() {
        let store = MemoryStore::default();
        let mut child = ForkTable::new();
        child.insert(prefix(b"b"), entry(1).into(), None).unwrap();
        let mut forks = ForkTable::new();
        forks
            .insert(prefix(b"a"), Child::Embedded(child).into(), None)
            .unwrap();
        let root = run(store.put_node(&Node::new(None, forks))).unwrap();

        let reader: Reader<_> = Reader::new(&store);
        // "ab" terminates, "a" is only a branch.
        assert_eq!(
            run(reader.get(&root, &Key::from(&b"ab"[..]))).unwrap(),
            Some(entry(1)),
        );
        assert_eq!(run(reader.get(&root, &Key::from(&b"a"[..]))).unwrap(), None,);
    }

    #[test]
    fn the_empty_key_reads_the_root_extension_value() {
        let store = MemoryStore::default();
        let root_ext = crate::node::RootExtension::new(Some(entry(9)), None);
        let root = run(store.put_node(&Node::new(root_ext, ForkTable::new()))).unwrap();

        let reader: Reader<_> = Reader::new(&store);
        assert_eq!(
            run(reader.get(&root, &Key::empty())).unwrap(),
            Some(entry(9)),
        );
    }

    #[test]
    fn inline_values_read_back_whole() {
        let store = MemoryStore::default();
        let value = Entry::inline(Bytes::from_static(b"hi")).unwrap();
        let mut forks = ForkTable::new();
        forks
            .insert(prefix(b"k"), ForkPayload::Entry(value.clone()), None)
            .unwrap();
        let root = run(store.put_node(&Node::new(None, forks))).unwrap();

        let reader: Reader<_> = Reader::new(&store);
        assert_eq!(
            run(reader.get(&root, &Key::from(&b"k"[..]))).unwrap(),
            Some(value),
        );
    }

    // A manifest whose "mg/" directory is a referenced subtree holding one key
    // "mg/logo.png", with "index.html" embedded in the root under "i".
    fn subtree_sample(store: &MemoryStore) -> (ChunkAddress, ChunkAddress) {
        let mut leaf = ForkTable::new();
        leaf.insert(prefix(b"logo.png"), entry(0xBB).into(), None)
            .unwrap();
        let leaf_ref = run(store.put_node(&Node::new(None, leaf))).unwrap();

        let mut embedded = ForkTable::new();
        embedded
            .insert(prefix(b"ndex.html"), entry(0xAA).into(), None)
            .unwrap();
        let mut forks = ForkTable::new();
        forks
            .insert(prefix(b"i"), Child::Embedded(embedded).into(), None)
            .unwrap();
        forks
            .insert(
                prefix(b"mg/"),
                Child::Ref32(ChunkRef::new(leaf_ref)).into(),
                None,
            )
            .unwrap();
        let root = run(store.put_node(&Node::new(None, forks))).unwrap();
        (root, leaf_ref)
    }

    #[test]
    fn subtree_of_the_empty_prefix_is_the_root() {
        let store = MemoryStore::default();
        let (root, _) = subtree_sample(&store);
        let reader: Reader<_> = Reader::new(&store);
        assert_eq!(
            run(reader.subtree(&root, &Key::empty())).unwrap(),
            Some(ChunkRef::new(root)),
        );
    }

    #[test]
    fn subtree_returns_the_referenced_child_covering_the_prefix() {
        let store = MemoryStore::default();
        let (root, leaf_ref) = subtree_sample(&store);
        let reader: Reader<_> = Reader::new(&store);
        // The prefix ends exactly at the referenced edge: the child is the
        // subtree root, and its key set is exactly the "mg/" keys.
        assert_eq!(
            run(reader.subtree(&root, &Key::from(&b"mg/"[..]))).unwrap(),
            Some(ChunkRef::new(leaf_ref)),
        );
        // A shorter prefix funnels into the same lone child with no branch or
        // key between: still one node boundary, still the child.
        assert_eq!(
            run(reader.subtree(&root, &Key::from(&b"m"[..]))).unwrap(),
            Some(ChunkRef::new(leaf_ref)),
        );
    }

    #[test]
    fn subtree_of_a_mid_edge_prefix_with_no_boundary_is_none() {
        let store = MemoryStore::default();
        let (root, _) = subtree_sample(&store);
        let reader: Reader<_> = Reader::new(&store);
        // "mg/logo" lands within the leaf's "logo.png" edge, which terminates a
        // key rather than referencing a child: no chunk holds exactly its keys.
        assert_eq!(
            run(reader.subtree(&root, &Key::from(&b"mg/logo"[..]))).unwrap(),
            None,
        );
    }

    #[test]
    fn subtree_of_an_embedded_prefix_is_none() {
        let store = MemoryStore::default();
        let (root, _) = subtree_sample(&store);
        let reader: Reader<_> = Reader::new(&store);
        // "index.html" lives embedded in the root chunk, with no chunk of its
        // own to hand off.
        assert_eq!(
            run(reader.subtree(&root, &Key::from(&b"i"[..]))).unwrap(),
            None,
        );
    }

    #[test]
    fn subtree_of_an_absent_prefix_is_none() {
        let store = MemoryStore::default();
        let (root, _) = subtree_sample(&store);
        let reader: Reader<_> = Reader::new(&store);
        assert_eq!(
            run(reader.subtree(&root, &Key::from(&b"zzz"[..]))).unwrap(),
            None,
        );
    }

    #[test]
    fn subtree_through_an_encrypted_child_is_an_error() {
        let store = MemoryStore::default();
        // A "sec/" directory referenced through an encrypted (ref64) child: the
        // plain reader cannot open it, and a 32-byte reference cannot carry it.
        let mut forks = ForkTable::new();
        forks
            .insert(
                prefix(b"sec/"),
                Child::Ref64(EncryptedChunkRef::new(
                    ChunkAddress::new([0x5E; 32]),
                    EncryptionKey::from([0xA1; 32]),
                ))
                .into(),
                None,
            )
            .unwrap();
        let root = run(store.put_node(&Node::new(None, forks))).unwrap();
        let reader: Reader<_> = Reader::new(&store);
        // The boundary lands exactly on the encrypted edge.
        assert!(matches!(
            run(reader.subtree(&root, &Key::from(&b"sec/"[..]))),
            Err(ReaderError::EncryptedChild),
        ));
        // A shorter prefix funnelling into the same encrypted child errs alike.
        assert!(matches!(
            run(reader.subtree(&root, &Key::from(&b"s"[..]))),
            Err(ReaderError::EncryptedChild),
        ));
    }

    #[test]
    fn subtree_covers_exactly_the_prefix_key_set() {
        let store = MemoryStore::default();
        let (root, leaf_ref) = subtree_sample(&store);
        let reader: Reader<_> = Reader::new(&store);
        let sub = run(reader.subtree(&root, &Key::from(&b"mg/"[..])))
            .unwrap()
            .unwrap();
        assert_eq!(sub.address(), &leaf_ref);

        // The delegated subtree, walked from its own root, yields the same keys
        // as the prefix range walked from the manifest root.
        let mut delegated = Vec::new();
        let mut cursor = run(reader.iter(sub.address())).unwrap();
        while let Some((key, value)) = run(cursor.next()).unwrap() {
            let mut full = b"mg/".to_vec();
            full.extend_from_slice(key.as_bytes());
            delegated.push((full, value));
        }

        let mut walked = Vec::new();
        let mut cursor = run(reader.prefix(&root, &Key::from(&b"mg/"[..]))).unwrap();
        while let Some((key, value)) = run(cursor.next()).unwrap() {
            walked.push((key.as_bytes().to_vec(), value));
        }
        assert_eq!(delegated, walked);
    }

    #[test]
    fn a_missing_root_is_a_store_error() {
        let store = MemoryStore::default();
        let reader: Reader<_> = Reader::new(&store);
        let err = run(reader.get(&ChunkAddress::new([0; 32]), &Key::from(&b"x"[..]))).unwrap_err();
        assert!(matches!(err, ReaderError::Store(StoreError::Store(_))));
    }
}
