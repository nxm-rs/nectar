//! Memory-bounded streaming reader: lazy, fetch-on-demand descent of the trie.
//!
//! A lookup follows one fork per node down the radix-256 trie, fetching only
//! the single child on the key's path at each referenced hop; a whole level is
//! never materialized, so peak retained state is O(depth), not O(level width).
//! The reader holds nothing but the store trait, so a caching store composes
//! beneath it without the reader's knowledge.

use core::marker::PhantomData;

use nectar_primitives::store::MaybeSync;
use nectar_primitives::{ChunkAddress, ChunkOps};

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
    use futures::executor::block_on;
    use nectar_primitives::store::MemoryStore;
    use nectar_primitives::{ChunkAddress, ChunkRef};

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
        let leaf_ref = block_on(store.put_node(&Node::new(None, leaf))).unwrap();

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
        let root = block_on(store.put_node(&Node::new(None, forks))).unwrap();

        let reader: Reader<_> = Reader::new(&store);
        assert_eq!(
            block_on(reader.get(&root, &Key::from(&b"index.html"[..]))).unwrap(),
            Some(entry(0xAA)),
        );
        assert_eq!(
            block_on(reader.get(&root, &Key::from(&b"mg/logo.png"[..]))).unwrap(),
            Some(entry(0xBB)),
        );
        // A key that prefixes an edge without reaching its end is absent.
        assert_eq!(
            block_on(reader.get(&root, &Key::from(&b"mg/logo"[..]))).unwrap(),
            None,
        );
        // A key past a fork with no matching continuation is absent.
        assert_eq!(
            block_on(reader.get(&root, &Key::from(&b"other"[..]))).unwrap(),
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
        let root = block_on(store.put_node(&Node::new(None, forks))).unwrap();

        let reader: Reader<_> = Reader::new(&store);
        // "ab" terminates, "a" is only a branch.
        assert_eq!(
            block_on(reader.get(&root, &Key::from(&b"ab"[..]))).unwrap(),
            Some(entry(1)),
        );
        assert_eq!(
            block_on(reader.get(&root, &Key::from(&b"a"[..]))).unwrap(),
            None,
        );
    }

    #[test]
    fn the_empty_key_reads_the_root_extension_value() {
        let store = MemoryStore::default();
        let root_ext = crate::node::RootExtension::new(Some(entry(9)), None);
        let root = block_on(store.put_node(&Node::new(root_ext, ForkTable::new()))).unwrap();

        let reader: Reader<_> = Reader::new(&store);
        assert_eq!(
            block_on(reader.get(&root, &Key::empty())).unwrap(),
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
        let root = block_on(store.put_node(&Node::new(None, forks))).unwrap();

        let reader: Reader<_> = Reader::new(&store);
        assert_eq!(
            block_on(reader.get(&root, &Key::from(&b"k"[..]))).unwrap(),
            Some(value),
        );
    }

    #[test]
    fn a_missing_root_is_a_store_error() {
        let store = MemoryStore::default();
        let reader: Reader<_> = Reader::new(&store);
        let err =
            block_on(reader.get(&ChunkAddress::new([0; 32]), &Key::from(&b"x"[..]))).unwrap_err();
        assert!(matches!(err, ReaderError::Store(StoreError::Store(_))));
    }
}
