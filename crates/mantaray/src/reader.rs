//! Depth-guarded path reader over persisted mantaray tries.
//!
//! Each lookup descends from a root address one [`NodeView`] per hop, fetching
//! only the nodes on the path, so it costs O(depth) store round trips under a
//! caller-set fetch budget. A node's obfuscation key and reference width
//! travel in its own bytes, so one reader serves plain, encrypted and
//! mixed-width tries by address alone.

use alloc::sync::Arc;

use nectar_primitives::AnyChunkSet;
use nectar_primitives::bmt::DEFAULT_BODY_SIZE;
use nectar_primitives::chunk::{ChunkAddress, ChunkOps};
use nectar_primitives::store::TrustedGet;

use crate::entry::Entry;
use crate::error::ReaderError;
use crate::node::NodeType;
use crate::view::NodeView;

/// Default per-lookup node-fetch budget.
///
/// A lookup fetches the root plus one node per edge, and every edge consumes
/// at least one path byte, so this covers any path up to 255 bytes.
pub const DEFAULT_MAX_DEPTH: usize = 256;

/// Depth-guarded reader over a trusted chunk store.
///
/// Stateless between calls: each lookup starts from the root address it is
/// given, so one reader serves any number of tries in the same store.
#[derive(Clone, Copy, Debug)]
pub struct Reader<S, const BS: usize = DEFAULT_BODY_SIZE> {
    store: S,
    max_depth: usize,
}

impl<S, const BS: usize> Reader<S, BS> {
    /// Reader with the [`DEFAULT_MAX_DEPTH`] fetch budget.
    #[must_use]
    pub const fn new(store: S) -> Self {
        Self::with_max_depth(store, DEFAULT_MAX_DEPTH)
    }

    /// Reader with an explicit per-lookup fetch budget.
    #[must_use]
    pub const fn with_max_depth(store: S, max_depth: usize) -> Self {
        Self { store, max_depth }
    }

    /// The per-lookup node-fetch budget.
    #[must_use]
    pub const fn max_depth(&self) -> usize {
        self.max_depth
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

impl<S: TrustedGet<AnyChunkSet<BS>>, const BS: usize> Reader<S, BS> {
    /// The entry at `path` under the trie rooted at `root`, or `None` when
    /// the path is absent or names a bare edge. A metadata-carrying edge
    /// (the root documents node) reads back as an entry with no reference.
    ///
    /// Fetches the root, one node per matched edge, and the terminal value
    /// node; a bare-edge terminal is decided from its parent's fork record
    /// without being fetched.
    pub async fn get(
        &self,
        root: &ChunkAddress,
        path: &[u8],
    ) -> Result<Option<Entry>, ReaderError> {
        let mut budget = self.max_depth;
        let mut view = self.fetch(&mut budget, root).await?;
        let mut rest = path;
        loop {
            let Some((first, _)) = rest.split_first() else {
                // The root has no arriving fork record to flag it as a value.
                return Ok(None);
            };
            let (child, terminal) = {
                let Some(fork) = view.fork(*first) else {
                    return Ok(None);
                };
                let Some(next) = rest.strip_prefix(fork.prefix()) else {
                    return Ok(None);
                };
                let terminal = if next.is_empty() {
                    if !fork
                        .node_type()
                        .intersects(NodeType::VALUE | NodeType::METADATA)
                    {
                        return Ok(None);
                    }
                    Some(fork.metadata().cloned().unwrap_or_default())
                } else {
                    None
                };
                rest = next;
                (*fork.reference().address(), terminal)
            };
            view = self.fetch(&mut budget, &child).await?;
            if let Some(metadata) = terminal {
                return Ok(Some(Entry {
                    path: path.to_vec(),
                    reference: view.entry().cloned(),
                    metadata,
                }));
            }
        }
    }

    /// Whether any stored path equals or extends `prefix`.
    ///
    /// The boundary node is never fetched: a prefix ending inside or exactly
    /// at an edge is answered from the parent's fork record, so the cost is
    /// at most one fetch per prefix byte. The empty prefix is trivially
    /// present and costs no fetch.
    pub async fn has_prefix(
        &self,
        root: &ChunkAddress,
        prefix: &[u8],
    ) -> Result<bool, ReaderError> {
        if prefix.is_empty() {
            return Ok(true);
        }
        let mut budget = self.max_depth;
        let mut view = self.fetch(&mut budget, root).await?;
        let mut rest = prefix;
        loop {
            let Some((first, _)) = rest.split_first() else {
                return Ok(true);
            };
            let child = {
                let Some(fork) = view.fork(*first) else {
                    return Ok(false);
                };
                let Some(next) = rest.strip_prefix(fork.prefix()) else {
                    return Ok(fork.prefix().starts_with(rest));
                };
                rest = next;
                *fork.reference().address()
            };
            if rest.is_empty() {
                return Ok(true);
            }
            view = self.fetch(&mut budget, &child).await?;
        }
    }

    /// Fetch and decode one node, spending one unit of the lookup's budget.
    async fn fetch(
        &self,
        budget: &mut usize,
        address: &ChunkAddress,
    ) -> Result<NodeView, ReaderError> {
        *budget = budget.checked_sub(1).ok_or(ReaderError::MaxDepth {
            max_depth: self.max_depth,
        })?;
        let chunk = self
            .store
            .get(address)
            .await
            .map_err(|e| ReaderError::Store {
                address: *address,
                source: Arc::new(e),
            })?;
        NodeView::try_from(chunk.envelope().data().as_ref()).map_err(|source| {
            ReaderError::Corrupt {
                address: *address,
                source,
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::collections::BTreeMap;
    use core::sync::atomic::{AtomicUsize, Ordering};

    use bytes::Bytes;
    use futures::executor::block_on;
    use nectar_primitives::chunk::{ChunkOps, ContentChunk};
    use nectar_primitives::store::{ChunkGet, ChunkPut, MemoryStore};
    use nectar_primitives::{
        Chunk, EncryptedChunkRef, EncryptionKey, EntryRef, StandardChunkSet, Verified,
    };

    use crate::{EncryptedManifest, PlainManifest};

    type Store = MemoryStore<StandardChunkSet>;
    type Manifest = PlainManifest<Store>;

    /// A ChunkAddress from a string, right-padded with zeroes.
    fn make_addr(s: &str) -> ChunkAddress {
        let bytes = s.as_bytes();
        let mut buf = [0u8; 32];
        let len = bytes.len().min(32);
        buf[..len].copy_from_slice(&bytes[..len]);
        ChunkAddress::from(buf)
    }

    /// Trie shapes covering prefix splits, nested folders, one-byte edge
    /// chains, and edges longer than the 30-byte prefix limit.
    fn corpora() -> Vec<Vec<&'static str>> {
        vec![
            vec!["a"],
            vec![
                "aa", "b", "aaaaaa", "aaaaab", "abbbb", "abbba", "bbbbba", "bbbaaa", "bbbaab",
            ],
            vec!["index.html", "img/1.png", "img/2.png", "robots.txt"],
            vec![
                "a/b/c/d/e/f/g/h/file00.dat",
                "a/b/c/d/e/f/g/h/file01.dat",
                "a/b/c/x.txt",
            ],
            vec!["a", "ab", "abc", "abcd", "abcde"],
            vec!["oneverylongpathsegmentthatexceedsthethirtybyteprefixlimitforsure"],
        ]
    }

    /// Probe set for a corpus: every stored path, proper prefixes, extensions
    /// past leaves, the empty path, and absent first bytes.
    fn probes(paths: &[&str]) -> Vec<String> {
        let mut out = vec![String::new(), "zzz-absent".to_string()];
        for p in paths {
            out.push((*p).to_string());
            if p.len() > 1 {
                out.push(p[..1].to_string());
                out.push(p[..p.len() - 1].to_string());
            }
            out.push(format!("{p}x"));
            out.push(format!("{p}/deeper"));
        }
        out
    }

    /// Build a persisted manifest, record the legacy answers for every probe,
    /// then compare the reader against them over the same store.
    fn assert_differential(paths: &[&str]) {
        let mut m = Manifest::new(Store::new());
        for &p in paths {
            block_on(m.add(p, make_addr(p))).unwrap();
        }
        let root = block_on(m.save()).unwrap();

        let probes = probes(paths);
        let expected: Vec<_> = probes
            .iter()
            .map(|p| {
                (
                    block_on(m.get(p)).unwrap(),
                    block_on(m.has_prefix(p)).unwrap(),
                )
            })
            .collect();

        let (_, store) = m.into_parts();
        let reader = Reader::new(store);
        for (probe, (want_get, want_has)) in probes.iter().zip(expected) {
            let got = block_on(reader.get(&root, probe.as_bytes())).unwrap();
            assert_eq!(got, want_get, "get({probe:?}) diverges from legacy");
            let has = block_on(reader.has_prefix(&root, probe.as_bytes())).unwrap();
            assert_eq!(has, want_has, "has_prefix({probe:?}) diverges from legacy");
        }
    }

    #[test]
    fn differential_get_and_has_prefix_vs_legacy() {
        for paths in corpora() {
            assert_differential(&paths);
        }
    }

    #[test]
    #[allow(deprecated)]
    fn differential_vs_deprecated_lookup() {
        let paths = ["index.html", "img/1.png", "img/2.png", "robots.txt"];
        let mut m = Manifest::new(Store::new());
        for p in paths {
            block_on(m.add(p, make_addr(p))).unwrap();
        }
        let root = block_on(m.save()).unwrap();

        let hits: Vec<_> = paths
            .iter()
            .map(|p| block_on(m.lookup(p)).unwrap())
            .collect();
        assert!(block_on(m.lookup("absent.txt")).is_err());

        let (_, store) = m.into_parts();
        let reader = Reader::new(store);
        for (p, want) in paths.iter().zip(hits) {
            let got = block_on(reader.get(&root, p.as_bytes())).unwrap();
            assert_eq!(got, Some(want), "get({p:?}) diverges from legacy lookup");
        }
        // Where the legacy lookup errs on a miss, the reader reports Ok(None).
        assert_eq!(block_on(reader.get(&root, b"absent.txt")).unwrap(), None);
    }

    #[test]
    fn encrypted_trie_differential() {
        let mut m = EncryptedManifest::new_encrypted(Store::new());
        let paths = ["secret/a.txt", "secret/b.txt", "top.txt"];
        for p in paths {
            let r = EncryptedChunkRef::new(make_addr(p), EncryptionKey::from([0x5a; 32]));
            block_on(m.add(p, r)).unwrap();
        }
        let manifest_ref = block_on(m.save()).unwrap();
        let (root, _key) = manifest_ref.into_parts();

        let expected: Vec<_> = paths.iter().map(|p| block_on(m.get(p)).unwrap()).collect();

        let (_, store) = m.into_parts();
        let reader = Reader::new(store);
        for (p, want) in paths.iter().zip(expected) {
            let got = block_on(reader.get(&root, p.as_bytes())).unwrap();
            assert_eq!(got, want, "encrypted get({p:?}) diverges from legacy");
            assert!(matches!(
                got.as_ref().and_then(Entry::reference),
                Some(EntryRef::Encrypted(_))
            ));
        }
        assert!(block_on(reader.has_prefix(&root, b"secret/")).unwrap());
        assert!(!block_on(reader.has_prefix(&root, b"secrets")).unwrap());
        assert_eq!(block_on(reader.get(&root, b"secret/")).unwrap(), None);
    }

    #[test]
    fn metadata_differential() {
        let mut m = Manifest::new(Store::new());
        block_on(m.add("plain.txt", make_addr("plain"))).unwrap();
        let meta: BTreeMap<String, String> =
            [("Content-Type".to_string(), "image/png".to_string())].into();
        block_on(m.add_with_metadata("logo.png", make_addr("logo"), meta.clone())).unwrap();
        block_on(m.set_index_document("index.html")).unwrap();
        let root = block_on(m.save()).unwrap();

        let expected: Vec<_> = ["plain.txt", "logo.png", "/"]
            .iter()
            .map(|p| block_on(m.get(p)).unwrap())
            .collect();

        let (_, store) = m.into_parts();
        let reader = Reader::new(store);
        for (p, want) in ["plain.txt", "logo.png", "/"].iter().zip(expected) {
            let got = block_on(reader.get(&root, p.as_bytes())).unwrap();
            assert_eq!(got, want, "get({p:?}) diverges from legacy");
        }
        let logo = block_on(reader.get(&root, b"logo.png")).unwrap().unwrap();
        assert_eq!(logo.metadata(), &meta);
        // The root path node carries metadata but no reference.
        let root_entry = block_on(reader.get(&root, b"/")).unwrap().unwrap();
        assert!(root_entry.reference().is_none());
        assert_eq!(
            root_entry.metadata().get("website-index-document").cloned(),
            Some("index.html".to_string())
        );
    }

    /// Store wrapper counting `get` calls, pinning the reader's fetch costs.
    struct CountingStore {
        inner: Store,
        gets: AtomicUsize,
    }

    impl CountingStore {
        fn new(inner: Store) -> Self {
            Self {
                inner,
                gets: AtomicUsize::new(0),
            }
        }

        fn take(&self) -> usize {
            self.gets.swap(0, Ordering::SeqCst)
        }
    }

    impl ChunkGet<StandardChunkSet> for CountingStore {
        type Trust = Verified;
        type Error = <Store as ChunkGet<StandardChunkSet>>::Error;

        async fn get(&self, address: &ChunkAddress) -> Result<Chunk, Self::Error> {
            self.gets.fetch_add(1, Ordering::SeqCst);
            ChunkGet::get(&self.inner, address).await
        }
    }

    #[test]
    fn fetch_costs_are_depth_bounded() {
        let mut m = Manifest::new(Store::new());
        block_on(m.add("abc", make_addr("abc"))).unwrap();
        let root = block_on(m.save()).unwrap();
        let (_, store) = m.into_parts();
        let reader = Reader::new(CountingStore::new(store));

        // Value hit: root plus the terminal node.
        assert!(block_on(reader.get(&root, b"abc")).unwrap().is_some());
        assert_eq!(reader.store().take(), 2);
        // Mid-edge miss: decided at the root.
        assert!(block_on(reader.get(&root, b"ab")).unwrap().is_none());
        assert_eq!(reader.store().take(), 1);
        // Prefix probes never fetch the boundary node.
        assert!(block_on(reader.has_prefix(&root, b"abc")).unwrap());
        assert_eq!(reader.store().take(), 1);
        assert!(block_on(reader.has_prefix(&root, b"ab")).unwrap());
        assert_eq!(reader.store().take(), 1);
        // The empty prefix is answered without touching the store.
        assert!(block_on(reader.has_prefix(&root, b"")).unwrap());
        assert_eq!(reader.store().take(), 0);
    }

    #[test]
    fn fetch_costs_stay_linear_in_path_length() {
        let paths = ["a", "ab", "abc", "abcd", "abcde"];
        let mut m = Manifest::new(Store::new());
        for p in paths {
            block_on(m.add(p, make_addr(p))).unwrap();
        }
        let root = block_on(m.save()).unwrap();
        let (_, store) = m.into_parts();
        let reader = Reader::new(CountingStore::new(store));

        for p in paths {
            assert!(block_on(reader.get(&root, p.as_bytes())).unwrap().is_some());
            assert!(
                reader.store().take() <= p.len() + 1,
                "get({p:?}) exceeded the depth bound"
            );
            assert!(block_on(reader.has_prefix(&root, p.as_bytes())).unwrap());
            assert!(
                reader.store().take() <= p.len(),
                "has_prefix({p:?}) exceeded the depth bound"
            );
        }
    }

    #[test]
    fn max_depth_is_a_typed_error() {
        // One-byte edge chain: get("abcde") costs 6 fetches, has_prefix 5.
        let paths = ["a", "ab", "abc", "abcd", "abcde"];
        let mut m = Manifest::new(Store::new());
        for p in paths {
            block_on(m.add(p, make_addr(p))).unwrap();
        }
        let root = block_on(m.save()).unwrap();
        let (_, store) = m.into_parts();

        let exact = Reader::with_max_depth(store, 6);
        assert!(block_on(exact.get(&root, b"abcde")).unwrap().is_some());
        assert!(block_on(exact.has_prefix(&root, b"abcde")).unwrap());

        let short = Reader::with_max_depth(exact.into_store(), 5);
        assert!(matches!(
            block_on(short.get(&root, b"abcde")),
            Err(ReaderError::MaxDepth { max_depth: 5 })
        ));
        assert!(block_on(short.has_prefix(&root, b"abcde")).unwrap());

        let shorter = Reader::with_max_depth(short.into_store(), 4);
        assert!(matches!(
            block_on(shorter.has_prefix(&root, b"abcde")),
            Err(ReaderError::MaxDepth { max_depth: 4 })
        ));

        // A zero budget rejects even the root fetch, but the empty prefix
        // needs none.
        let zero = Reader::with_max_depth(shorter.into_store(), 0);
        assert!(matches!(
            block_on(zero.get(&root, b"")),
            Err(ReaderError::MaxDepth { max_depth: 0 })
        ));
        assert!(block_on(zero.has_prefix(&root, b"")).unwrap());
    }

    #[test]
    fn empty_path_is_not_a_value() {
        let mut m = Manifest::new(Store::new());
        block_on(m.add("a", make_addr("a"))).unwrap();
        let root = block_on(m.save()).unwrap();
        let (_, store) = m.into_parts();
        let reader = Reader::new(store);
        assert_eq!(block_on(reader.get(&root, b"")).unwrap(), None);
    }

    #[test]
    fn missing_root_is_a_store_error() {
        let reader: Reader<Store> = Reader::new(Store::new());
        let root = make_addr("nowhere");
        assert!(matches!(
            block_on(reader.get(&root, b"x")),
            Err(ReaderError::Store { address, .. }) if address == root
        ));
        assert!(matches!(
            block_on(reader.has_prefix(&root, b"x")),
            Err(ReaderError::Store { address, .. }) if address == root
        ));
    }

    #[test]
    fn non_node_chunk_is_a_corrupt_error() {
        let store = Store::new();
        let chunk = ContentChunk::<{ nectar_primitives::bmt::DEFAULT_BODY_SIZE }>::new(
            Bytes::from_static(b"not a mantaray node"),
        )
        .unwrap();
        let root = *chunk.address();
        let sealed: Chunk = Chunk::from_envelope(chunk.into()).unwrap();
        block_on(store.put(sealed)).unwrap();

        let reader = Reader::new(store);
        assert!(matches!(
            block_on(reader.get(&root, b"x")),
            Err(ReaderError::Corrupt { address, .. }) if address == root
        ));
    }
}
