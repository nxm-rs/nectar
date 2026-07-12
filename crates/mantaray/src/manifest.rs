//! High-level mantaray manifest and lazy iterator.

use std::collections::BTreeMap;

use futures::{Stream, StreamExt, TryStreamExt, stream};
use nectar_primitives::bmt::DEFAULT_BODY_SIZE;
use nectar_primitives::chunk::{ChunkAddress, ChunkRef, Reference};
use nectar_primitives::store::{ChunkGet, ChunkPut, MaybeSend};

use crate::entry::Entry;
use crate::node::Node;
use crate::{MantarayError, Result, metadata};

/// Default fan-out width for [`Manifest::entries_concurrent`].
///
/// Matches the file joiner's async width, balancing round-trip overlap against
/// peak in-flight store load.
pub const DEFAULT_LIST_CONCURRENCY: usize = 8;

/// High-level mantaray manifest backed by a typed chunk store.
///
/// The entry type parameter `R` determines:
/// - What reference types `add()` accepts (compile-time enforcement)
/// - The reference byte size via `R::SIZE`
/// - What `save()` returns (specialized per entry type)
#[derive(Debug)]
pub struct Manifest<S, R: Reference = ChunkRef, const BS: usize = DEFAULT_BODY_SIZE> {
    trie: Node<R>,
    store: S,
}

impl<S, const BS: usize> Manifest<S, ChunkRef, BS> {
    /// Create a new plain manifest (no obfuscation, 32-byte refs).
    pub fn new(store: S) -> Self {
        let trie = Node::new_unencrypted();
        Self { trie, store }
    }

    /// Load a plain manifest from storage by its root chunk address.
    pub fn open(root: ChunkAddress, store: S) -> Self {
        let trie = Node::from_reference(root);
        Self { trie, store }
    }
}

#[cfg(feature = "encryption")]
impl<S, const BS: usize> Manifest<S, nectar_primitives::EncryptedChunkRef, BS> {
    /// Create a new encrypted manifest (random obfuscation key, 64-byte refs).
    pub fn new_encrypted(store: S) -> Self {
        use crate::obfuscation::ObfuscationKey;
        let trie = Node {
            obfuscation_key: ObfuscationKey::generate(),
            ..Node::default()
        };
        Self { trie, store }
    }

    /// Load an encrypted manifest from storage by its manifest reference.
    pub fn open_encrypted(root: crate::ManifestRef, store: S) -> Self {
        let (addr, key) = root.into_parts();
        let mut trie = Node::from_reference(addr);
        trie.obfuscation_key = key;
        Self { trie, store }
    }
}

impl<S, R: Reference, const BS: usize> Manifest<S, R, BS> {
    /// Access the underlying chunk store.
    pub const fn store(&self) -> &S {
        &self.store
    }

    /// Access the root trie node.
    pub const fn root(&self) -> &Node<R> {
        &self.trie
    }

    /// Mutable access to the root trie node.
    pub const fn root_mut(&mut self) -> &mut Node<R> {
        &mut self.trie
    }

    /// Consume the manifest and return its parts.
    pub fn into_parts(self) -> (Node<R>, S) {
        (self.trie, self.store)
    }

    /// Get the root reference (`None` if not yet saved).
    pub const fn reference(&self) -> Option<&ChunkAddress> {
        self.trie.reference()
    }
}

impl<S: ChunkGet<BS>, R: Reference + MaybeSend, const BS: usize> Manifest<S, R, BS> {
    /// Add a path with a typed reference (compile-time enforced by entry type).
    pub async fn add(&mut self, path: &str, reference: impl Into<R>) -> Result<()> {
        let entry = reference.into();
        self.trie
            .add::<S, BS>(path.as_bytes(), Some(entry), BTreeMap::new(), &self.store)
            .await
    }

    /// Add a path with a typed reference and metadata.
    pub async fn add_with_metadata(
        &mut self,
        path: &str,
        reference: impl Into<R>,
        metadata: BTreeMap<String, String>,
    ) -> Result<()> {
        let entry = reference.into();
        self.trie
            .add::<S, BS>(path.as_bytes(), Some(entry), metadata, &self.store)
            .await
    }

    /// Add a path with a pre-built [`Entry`] (metadata + reference).
    pub async fn add_entry(&mut self, path: &str, entry: Entry) -> Result<()> {
        let e = entry.reference.map(R::from_entry_ref).transpose()?;
        self.trie
            .add::<S, BS>(path.as_bytes(), e, entry.metadata, &self.store)
            .await
    }

    /// Remove a path from the manifest.
    pub async fn remove(&mut self, path: &str) -> Result<()> {
        self.trie
            .remove::<S, BS>(path.as_bytes(), &self.store)
            .await
    }

    /// Look up a path in the manifest.
    pub async fn lookup(&mut self, path: &str) -> Result<Entry> {
        let node = self
            .trie
            .lookup_node::<S, BS>(path.as_bytes(), &self.store)
            .await?;

        if !node.is_value() {
            return Err(MantarayError::NotValueType);
        }

        Ok(Entry::from_node(path.as_bytes(), node))
    }

    /// Test whether the manifest contains a prefix.
    pub async fn has_prefix(&mut self, prefix: &str) -> Result<bool> {
        self.trie
            .has_prefix::<S, BS>(prefix.as_bytes(), &self.store)
            .await
    }

    /// Walk all nodes depth-first, calling `f` for each node with its path.
    pub async fn walk<F>(&mut self, f: &mut F) -> Result<()>
    where
        F: FnMut(&[u8], &Node<R>) -> Result<()>,
    {
        self.trie.walk::<S, BS, _>(&self.store, f).await
    }

    /// Walk the subtree rooted at `root`, calling `f` for each node with its path.
    pub async fn walk_from<F>(&mut self, root: &str, f: &mut F) -> Result<()>
    where
        F: FnMut(&[u8], &Node<R>) -> Result<()>,
    {
        self.trie
            .walk_from::<S, BS, _>(root.as_bytes(), &self.store, f)
            .await
    }

    /// Collect all value entries from the manifest.
    ///
    /// Convenience wrapper around the [`stream`](Self::stream) accessor.
    pub async fn entries(&mut self) -> Result<Vec<Entry>> {
        let mut iter = self.iter();
        let mut out = Vec::new();
        while let Some(item) = iter.next().await {
            out.push(item?);
        }
        Ok(out)
    }

    /// Collect all value entries, fetching sibling forks concurrently.
    ///
    /// Walks the trie level by level, keeping up to `concurrency` node loads in
    /// flight through the shared [`ChunkGet`]. Where [`entries`](Self::entries)
    /// fetches one node per `await` in depth-first order, this fans out each
    /// level's sibling forks at once, collapsing a folder's N sequential round
    /// trips into ceil(N / concurrency) batched ones.
    ///
    /// Entries arrive in completion order, not path order. Sort by
    /// [`path`](Entry::path) if a stable order is required; use
    /// [`entries`](Self::entries) for the serial depth-first ordering.
    ///
    /// `concurrency` is clamped to at least 1; pass [`DEFAULT_LIST_CONCURRENCY`]
    /// for the default width. Takes `&self`: the manifest's own trie is left
    /// untouched (traversal runs over owned clones), so unlike `entries` no
    /// nodes are cached back into it.
    pub async fn entries_concurrent(&self, concurrency: usize) -> Result<Vec<Entry>> {
        let width = concurrency.max(1);
        let store = &self.store;
        let mut out = Vec::new();

        // Owned, loaded root. Cloning leaves the manifest trie untouched and
        // gives each per-node load future disjoint state to own across the
        // fan-out. For a persisted manifest the cloned child forks are
        // reference-only, so the clone is shallow.
        let mut root = self.trie.clone();
        if !root.loaded {
            root.load::<S, BS>(store).await?;
        }
        let mut frontier: Vec<(Vec<u8>, Node<R>)> = vec![(Vec::new(), root)];

        while !frontier.is_empty() {
            let mut pending: Vec<(Vec<u8>, Node<R>)> = Vec::new();
            for (path, node) in &frontier {
                if node.is_value() {
                    out.push(Entry::from_node(path, node));
                }
                for fork in node.forks.values() {
                    let mut child_path = path.clone();
                    child_path.extend_from_slice(&fork.prefix);
                    pending.push((child_path, fork.node.clone()));
                }
            }

            frontier = stream::iter(pending)
                .map(move |(path, mut node)| async move {
                    if !node.loaded {
                        node.load::<S, BS>(store).await?;
                    }
                    Ok::<_, MantarayError>((path, node))
                })
                .buffer_unordered(width)
                .try_collect()
                .await?;
        }

        Ok(out)
    }

    /// Set the website index document on the root path metadata.
    pub async fn set_index_document(&mut self, filename: &str) -> Result<()> {
        self.set_root_metadata(metadata::WEBSITE_INDEX_DOCUMENT, filename)
            .await
    }

    /// Set the website error document on the root path metadata.
    pub async fn set_error_document(&mut self, path: &str) -> Result<()> {
        self.set_root_metadata(metadata::WEBSITE_ERROR_DOCUMENT, path)
            .await
    }

    /// Get the website index document from root path metadata.
    pub async fn index_document(&mut self) -> Result<Option<String>> {
        self.get_root_metadata(metadata::WEBSITE_INDEX_DOCUMENT)
            .await
    }

    /// Get the website error document from root path metadata.
    pub async fn error_document(&mut self) -> Result<Option<String>> {
        self.get_root_metadata(metadata::WEBSITE_ERROR_DOCUMENT)
            .await
    }

    /// Walk all nodes, yielding both node references and entry references.
    ///
    /// Enumerates every chunk address the manifest depends on, for garbage
    /// collection and pinning.
    pub async fn iterate_addresses<F>(&mut self, mut f: F) -> Result<()>
    where
        F: FnMut(&[u8]) -> Result<()>,
    {
        self.walk(&mut |_path, node| {
            if let Some(addr) = node.reference() {
                f(addr.as_bytes())?;
            }

            if let Some(entry) = node.entry()
                && node.is_value()
            {
                let entry_bytes = entry.to_bytes();
                f(&entry_bytes)?;
            }

            Ok(())
        })
        .await
    }

    /// Create a lazy depth-first stream over all entries in the manifest.
    ///
    /// Nodes are loaded from storage on demand, so the entire trie does not
    /// need to be in memory at once. Drive it with [`ManifestIter::next`] or
    /// the [`Stream`] impl.
    pub const fn iter(&mut self) -> ManifestIter<'_, S, R, BS> {
        ManifestIter::new(&mut self.trie, &self.store)
    }

    /// Lazy depth-first stream over all entries in the manifest.
    pub fn stream(&mut self) -> impl Stream<Item = Result<Entry>> + '_ {
        futures::stream::unfold(self.iter(), |mut iter| async move {
            iter.next().await.map(|item| (item, iter))
        })
    }

    async fn set_root_metadata(&mut self, key: &str, value: &str) -> Result<()> {
        // Ensure the root path node exists.
        match self
            .trie
            .lookup_node::<S, BS>(metadata::ROOT_PATH.as_bytes(), &self.store)
            .await
        {
            Ok(node) => {
                // Node exists; mutate metadata in place (no clone).
                node.metadata_mut().insert(key.into(), value.into());
                node.make_with_metadata();
                node.mark_dirty();
                Ok(())
            }
            Err(MantarayError::NoForkFound { .. }) => {
                // Root path doesn't exist yet; create it with the metadata.
                let mut meta = BTreeMap::new();
                meta.insert(key.into(), value.into());
                self.trie
                    .add::<S, BS>(metadata::ROOT_PATH.as_bytes(), None, meta, &self.store)
                    .await
            }
            Err(e) => Err(e),
        }
    }

    async fn get_root_metadata(&mut self, key: &str) -> Result<Option<String>> {
        match self
            .trie
            .lookup_node::<S, BS>(metadata::ROOT_PATH.as_bytes(), &self.store)
            .await
        {
            Ok(node) => Ok(node.metadata().get(key).cloned()),
            Err(MantarayError::NoForkFound { .. }) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

impl<S: ChunkGet<BS> + ChunkPut<BS>, const BS: usize> Manifest<S, ChunkRef, BS> {
    /// Persist the plain manifest trie to storage, returning the root chunk address.
    pub async fn save(&mut self) -> Result<ChunkAddress> {
        self.trie.save::<S, BS>(&self.store).await?;
        Ok(*self
            .trie
            .reference()
            .ok_or(MantarayError::MissingReference)?)
    }
}

#[cfg(feature = "encryption")]
impl<S: ChunkGet<BS> + ChunkPut<BS>, const BS: usize>
    Manifest<S, nectar_primitives::EncryptedChunkRef, BS>
{
    /// Persist the encrypted manifest trie, returning a [`ManifestRef`](crate::ManifestRef).
    pub async fn save(&mut self) -> Result<crate::ManifestRef> {
        self.trie.save::<S, BS>(&self.store).await?;
        let addr = *self
            .trie
            .reference()
            .ok_or(MantarayError::MissingReference)?;
        Ok(crate::ManifestRef::new(addr, self.trie.obfuscation_key))
    }
}

/// Lazy depth-first iterator over manifest entries.
///
/// Loads nodes from storage on demand. Each call to `next()` may perform
/// storage reads as it traverses unloaded parts of the trie.
///
/// Uses raw node pointers for O(1) per-step traversal. This is sound because
/// the trie is exclusively borrowed (`&'a mut Node`) for the iterator's
/// lifetime, and `BTreeMap` values are stable (we never insert into or remove
/// from a parent's fork map during iteration).
pub struct ManifestIter<'a, S, R: Reference = ChunkRef, const BS: usize = DEFAULT_BODY_SIZE> {
    trie: &'a mut Node<R>,
    store: &'a S,
    stack: Vec<IterFrame<R>>,
    /// Running path buffer; extended when pushing frames, truncated when popping.
    path_buf: Vec<u8>,
    root_visited: bool,
}

impl<S, R: Reference, const BS: usize> std::fmt::Debug for ManifestIter<'_, S, R, BS> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManifestIter")
            .field("stack_depth", &self.stack.len())
            .field("root_visited", &self.root_visited)
            .finish_non_exhaustive()
    }
}

struct IterFrame<R: Reference> {
    /// Pointer to the node at this stack level.
    ///
    /// # Safety
    /// Valid for the iterator's `'a` lifetime. Points into the exclusively
    /// borrowed trie. Derived from `&mut Node` references obtained via
    /// `BTreeMap::get_mut`, whose values are stable across unrelated mutations.
    node: *mut Node<R>,
    /// Length of `path_buf` before this frame's prefix was appended.
    path_len_before: usize,
    /// This node's sorted fork keys.
    keys: Vec<u8>,
    /// Index into `keys` for the next fork to visit.
    key_idx: usize,
}

impl<'a, S: ChunkGet<BS>, R: Reference, const BS: usize> ManifestIter<'a, S, R, BS> {
    pub(crate) const fn new(trie: &'a mut Node<R>, store: &'a S) -> Self {
        Self {
            trie,
            store,
            stack: Vec::new(),
            path_buf: Vec::new(),
            root_visited: false,
        }
    }

    /// Advance the lazy traversal, returning the next entry (or `None` when done).
    ///
    /// Loads unvisited nodes from storage on demand.
    #[allow(clippy::arithmetic_side_effects)] // the only arithmetic is the fork-cursor `key_idx += 1`, bounded by keys.len() <= 256
    pub async fn next(&mut self) -> Option<Result<Entry>> {
        loop {
            if !self.root_visited {
                self.root_visited = true;

                if !self.trie.loaded
                    && let Err(e) = self.trie.load::<S, BS>(self.store).await
                {
                    return Some(Err(e));
                }

                let keys: Vec<u8> = self.trie.forks.keys().copied().collect();
                let entry = if self.trie.is_value() {
                    Some(Entry::from_node(&[], self.trie))
                } else {
                    None
                };

                self.stack.push(IterFrame {
                    node: std::ptr::from_mut(self.trie),
                    path_len_before: 0,
                    keys,
                    key_idx: 0,
                });

                if let Some(entry) = entry {
                    return Some(Ok(entry));
                }
                continue;
            }

            // Pop exhausted frames, truncating path_buf as we go.
            while let Some(frame) = self.stack.pop_if(|f| f.key_idx >= f.keys.len()) {
                self.path_buf.truncate(frame.path_len_before);
            }

            // Advance: get the next fork key and parent pointer from the top frame.
            let (key, parent_node) = {
                let frame = self.stack.last_mut()?;
                #[allow(clippy::indexing_slicing)] // the pop_if loop above removed every frame with key_idx >= keys.len()
                let key = frame.keys[frame.key_idx];
                frame.key_idx += 1;
                (key, frame.node)
            };

            // SAFETY: parent_node points into the exclusively borrowed trie.
            // No other mutable reference to this node exists; frames only hold
            // pointers to ancestors, which we do not dereference simultaneously.
            let parent = unsafe { &mut *parent_node };
            let fork = match parent.forks.get_mut(&key) {
                Some(f) => f,
                None => {
                    return Some(Err(MantarayError::NoForkFound {
                        reference: parent.reference,
                    }));
                }
            };

            let child = std::ptr::from_mut(&mut fork.node);

            // SAFETY: child is a descendant of the exclusively borrowed trie.
            let child_ref = unsafe { &mut *child };
            if !child_ref.loaded
                && let Err(e) = child_ref.load::<S, BS>(self.store).await
            {
                return Some(Err(e));
            }

            let child_keys: Vec<u8> = child_ref.forks.keys().copied().collect();
            let is_value = child_ref.is_value();

            // Extend path_buf with this fork's prefix, record restore point.
            let path_len_before = self.path_buf.len();
            self.path_buf.extend_from_slice(&fork.prefix);

            self.stack.push(IterFrame {
                node: child,
                path_len_before,
                keys: child_keys,
                key_idx: 0,
            });

            if is_value {
                return Some(Ok(Entry::from_node(&self.path_buf, child_ref)));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::executor::block_on;
    use nectar_primitives::bmt::DEFAULT_BODY_SIZE;
    use nectar_primitives::chunk::ChunkAddress;
    use nectar_primitives::store::MemoryStore;

    use super::*;

    type Store = MemoryStore<DEFAULT_BODY_SIZE>;
    type PlainManifest<S, const BS: usize = DEFAULT_BODY_SIZE> = super::Manifest<S, ChunkRef, BS>;

    /// Drain an async manifest iterator into a `Vec`, propagating the first error.
    fn drain<S: ChunkGet<BS>, R: Reference, const BS: usize>(
        mut iter: ManifestIter<'_, S, R, BS>,
    ) -> Result<Vec<Entry>> {
        block_on(async move {
            let mut out = Vec::new();
            while let Some(item) = iter.next().await {
                out.push(item?);
            }
            Ok(out)
        })
    }

    /// Create a ChunkAddress from a string, right-padded with zeroes.
    fn make_addr(s: &str) -> ChunkAddress {
        let bytes = s.as_bytes();
        let mut buf = [0u8; 32];
        let len = bytes.len().min(32);
        buf[..len].copy_from_slice(&bytes[..len]);
        ChunkAddress::from(buf)
    }

    /// Create a ChunkAddress from a u32, left-padded with zeroes.
    fn make_addr_u32(i: u32) -> ChunkAddress {
        let mut buf = [0u8; 32];
        buf[28..].copy_from_slice(&i.to_be_bytes());
        ChunkAddress::from(buf)
    }

    #[test]
    fn persist_idempotence() {
        let store = Store::new();

        let mut m = PlainManifest::new(store);

        let paths = &[
            "aa", "b", "aaaaaa", "aaaaab", "abbbb", "abbba", "bbbbba", "bbbaaa", "bbbaab",
        ];

        for &path in paths {
            block_on(m.save()).unwrap();
            let addr = make_addr(path);
            block_on(m.add(path, addr)).unwrap();
        }

        block_on(m.save()).unwrap();

        for &path in paths {
            let entry = block_on(m.lookup(path)).unwrap();
            let expected = make_addr(path);
            assert_eq!(entry.address(), Some(&expected));
        }
    }

    #[test]
    fn manifest_entries() {
        let store = Store::new();
        let mut m = PlainManifest::new(store);

        let paths = &["index.html", "img/1.png", "img/2.png", "robots.txt"];
        for &path in paths {
            let addr = make_addr(path);
            block_on(m.add(path, addr)).unwrap();
        }

        let entries = block_on(m.entries()).unwrap();
        assert_eq!(entries.len(), paths.len());

        for &path in paths {
            assert!(
                entries.iter().any(|e| e.path() == path.as_bytes()),
                "path {path} not found in entries"
            );
        }
    }

    #[test]
    fn website_document_helpers() {
        let store = Store::new();
        let mut m = PlainManifest::new(store);

        // Add a dummy entry so the root "/" path has an entry
        block_on(m.add("/", ChunkAddress::from([0u8; 32]))).unwrap();

        block_on(m.set_index_document("index.html")).unwrap();
        block_on(m.set_error_document("404.html")).unwrap();

        assert_eq!(
            block_on(m.index_document()).unwrap(),
            Some("index.html".to_string())
        );
        assert_eq!(
            block_on(m.error_document()).unwrap(),
            Some("404.html".to_string())
        );
    }

    #[test]
    fn website_document_helpers_merge_metadata() {
        let store = Store::new();
        let mut m = PlainManifest::new(store);

        block_on(m.set_index_document("index.html")).unwrap();
        block_on(m.set_error_document("404.html")).unwrap();

        assert_eq!(
            block_on(m.index_document()).unwrap(),
            Some("index.html".to_string())
        );
        assert_eq!(
            block_on(m.error_document()).unwrap(),
            Some("404.html".to_string())
        );
    }

    #[test]
    fn website_document_helpers_none_when_missing() {
        let store = Store::new();
        let mut m = PlainManifest::new(store);

        assert_eq!(block_on(m.index_document()).unwrap(), None);
        assert_eq!(block_on(m.error_document()).unwrap(), None);
    }

    #[test]
    fn iterate_addresses_yields_all_refs() {
        let store = Store::new();
        let mut m = PlainManifest::new(store);

        let paths = &["index.html", "img/1.png", "img/2.png", "robots.txt"];
        for &path in paths {
            let addr = make_addr(path);
            block_on(m.add(path, addr)).unwrap();
        }

        let root_ref = block_on(m.save()).unwrap();

        let (_, store) = m.into_parts();
        let mut m2 = PlainManifest::open(root_ref, store);
        let mut addresses = Vec::new();
        block_on(m2.iterate_addresses(|addr| {
            addresses.push(addr.to_vec());
            Ok(())
        }))
        .unwrap();

        assert!(!addresses.is_empty());

        for &path in paths {
            let expected = make_addr(path);
            assert!(
                addresses.iter().any(|a| a == expected.as_bytes()),
                "entry ref for {path} not found in addresses"
            );
        }
    }

    #[test]
    fn partial_update_workflow() {
        let store = Store::new();
        let mut m = PlainManifest::new(store);

        // Build a manifest with 100 entries
        for i in 0..100u32 {
            let path = format!("dir{}/file{}.txt", i / 10, i);
            let addr = make_addr_u32(i);
            block_on(m.add(&path, addr)).unwrap();
        }
        let root_ref_1 = block_on(m.save()).unwrap();

        // Update a single path
        let updated_addr = make_addr_u32(999);
        block_on(m.add("dir0/file0.txt", updated_addr)).unwrap();
        let root_ref_2 = block_on(m.save()).unwrap();

        assert_ne!(root_ref_1, root_ref_2);

        let entry = block_on(m.lookup("dir0/file0.txt")).unwrap();
        assert_eq!(entry.address(), Some(&updated_addr));

        for i in 1..100u32 {
            let path = format!("dir{}/file{}.txt", i / 10, i);
            let entry = block_on(m.lookup(&path)).unwrap();
            let expected = make_addr_u32(i);
            assert_eq!(
                entry.address(),
                Some(&expected),
                "entry at {path} was corrupted"
            );
        }
    }

    #[test]
    fn stream_yields_all_entries() {
        use futures::StreamExt;

        let store = Store::new();
        let mut m = PlainManifest::new(store);

        let paths = &["index.html", "img/1.png", "img/2.png", "robots.txt"];
        for &path in paths {
            let addr = make_addr(path);
            block_on(m.add(path, addr)).unwrap();
        }

        let all_entries: Vec<_> =
            block_on(async { m.stream().map(|r| r.unwrap()).collect::<Vec<_>>().await });

        assert_eq!(all_entries.len(), paths.len());
        for &path in paths {
            assert!(
                all_entries.iter().any(|e| e.path() == path.as_bytes()),
                "path {path} not found via stream"
            );
        }
    }

    #[test]
    fn manifest_iter_lazy() {
        let store = Store::new();
        let mut m = PlainManifest::new(store);

        let paths = &["index.html", "img/1.png", "img/2.png", "robots.txt"];
        for &path in paths {
            let addr = make_addr(path);
            block_on(m.add(path, addr)).unwrap();
        }

        // Save and reload to exercise lazy loading
        let root_ref = block_on(m.save()).unwrap();

        let (_, store) = m.into_parts();
        let mut m2 = PlainManifest::open(root_ref, store);

        let mut visited = Vec::new();
        if let Some(result) = block_on(m2.iter().next()) {
            let entry = result.unwrap();
            visited.push(entry.path);
        }
        assert_eq!(visited.len(), 1);

        // Full iteration
        let (_, store) = m2.into_parts();
        let mut m3 = PlainManifest::open(root_ref, store);
        let all_entries = drain(m3.iter()).unwrap();

        assert_eq!(all_entries.len(), paths.len());
        for &path in paths {
            assert!(
                all_entries.iter().any(|e| e.path() == path.as_bytes()),
                "path {path} not found via iterator"
            );
        }
    }

    #[test]
    fn iter_empty_manifest() {
        let store = Store::new();
        let mut m = PlainManifest::new(store);
        let entries = drain(m.iter()).unwrap();
        assert!(entries.is_empty(), "empty manifest should yield no entries");
    }

    #[test]
    fn iter_single_entry() {
        let store = Store::new();
        let mut m = PlainManifest::new(store);
        let addr = make_addr("only");
        block_on(m.add("only.txt", addr)).unwrap();

        let entries = drain(m.iter()).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].path(), b"only.txt");
        assert_eq!(entries[0].address(), Some(&addr));
    }

    #[test]
    fn iter_deep_trie_with_lazy_loading() {
        let store = Store::new();
        let mut m = PlainManifest::new(store);

        // Build a deep trie: paths share long prefixes, forcing multiple
        // trie levels. After save+reload, iteration must lazily load each
        // intermediate node via raw-pointer traversal.
        let deep_paths: Vec<String> = (0..20)
            .map(|i| format!("a/b/c/d/e/f/g/h/file{i:02}.dat"))
            .collect();
        for path in &deep_paths {
            block_on(m.add(path, make_addr(path))).unwrap();
        }

        let root_ref = block_on(m.save()).unwrap();
        let (_, store) = m.into_parts();
        let mut m2 = PlainManifest::open(root_ref, store);

        let entries = drain(m2.iter()).unwrap();
        assert_eq!(entries.len(), deep_paths.len());
        for path in &deep_paths {
            assert!(
                entries.iter().any(|e| e.path() == path.as_bytes()),
                "deep path {path} not found via iterator"
            );
        }
    }

    #[test]
    fn iter_partial_then_reiterate() {
        let store = Store::new();
        let mut m = PlainManifest::new(store);

        let paths = &["a.txt", "b.txt", "c.txt", "d.txt", "e.txt"];
        for &path in paths {
            block_on(m.add(path, make_addr(path))).unwrap();
        }

        // Partial iteration: take only 2 entries, then drop iterator.
        {
            let mut iter = m.iter();
            let _first = block_on(iter.next()).unwrap().unwrap();
            let _second = block_on(iter.next()).unwrap().unwrap();
            // Iterator dropped here; must not corrupt trie state.
        }

        // Full re-iteration should still yield all entries.
        let all = drain(m.iter()).unwrap();
        assert_eq!(all.len(), paths.len());
        for &path in paths {
            assert!(
                all.iter().any(|e| e.path() == path.as_bytes()),
                "path {path} missing after partial iteration + re-iteration"
            );
        }
    }

    /// A `ChunkGet` wrapper that records the peak number of concurrent
    /// in-flight `get` calls, proving the concurrent listing fans out and
    /// stays bounded by the chosen width.
    struct TrackingStore {
        inner: Store,
        inflight: std::sync::atomic::AtomicUsize,
        max_inflight: std::sync::atomic::AtomicUsize,
        gets: std::sync::atomic::AtomicUsize,
    }

    impl TrackingStore {
        fn new(inner: Store) -> Self {
            Self {
                inner,
                inflight: std::sync::atomic::AtomicUsize::new(0),
                max_inflight: std::sync::atomic::AtomicUsize::new(0),
                gets: std::sync::atomic::AtomicUsize::new(0),
            }
        }

        fn max_inflight(&self) -> usize {
            self.max_inflight.load(std::sync::atomic::Ordering::SeqCst)
        }

        fn gets(&self) -> usize {
            self.gets.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    /// Yield once so sibling fetches in the same `buffer_unordered` batch can
    /// ramp their in-flight count before any single fetch resolves.
    async fn yield_once() {
        use std::task::Poll;
        let mut yielded = false;
        futures::future::poll_fn(|cx| {
            if yielded {
                Poll::Ready(())
            } else {
                yielded = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        })
        .await;
    }

    impl ChunkGet<DEFAULT_BODY_SIZE> for TrackingStore {
        type Error = <Store as ChunkGet<DEFAULT_BODY_SIZE>>::Error;

        async fn get(
            &self,
            address: &ChunkAddress,
        ) -> std::result::Result<nectar_primitives::chunk::AnyChunk<DEFAULT_BODY_SIZE>, Self::Error>
        {
            use std::sync::atomic::Ordering::SeqCst;
            self.gets.fetch_add(1, SeqCst);
            let cur = self.inflight.fetch_add(1, SeqCst) + 1;
            self.max_inflight.fetch_max(cur, SeqCst);
            yield_once().await;
            let r = ChunkGet::get(&self.inner, address).await;
            self.inflight.fetch_sub(1, SeqCst);
            r
        }
    }

    /// Build a saved manifest, reopen it over a `TrackingStore`, and return it
    /// alongside the recorded paths.
    fn saved_tracking_manifest(paths: &[&str]) -> (PlainManifest<TrackingStore>, Vec<Vec<u8>>) {
        let store = Store::new();
        let mut m = PlainManifest::new(store);
        for &path in paths {
            block_on(m.add(path, make_addr(path))).unwrap();
        }
        let root_ref = block_on(m.save()).unwrap();
        let (_, store) = m.into_parts();
        let expected = paths.iter().map(|p| p.as_bytes().to_vec()).collect();
        (
            PlainManifest::open(root_ref, TrackingStore::new(store)),
            expected,
        )
    }

    fn sorted_paths(entries: &[Entry]) -> Vec<Vec<u8>> {
        let mut v: Vec<Vec<u8>> = entries.iter().map(|e| e.path().to_vec()).collect();
        v.sort();
        v
    }

    #[test]
    fn entries_concurrent_matches_serial() {
        let paths = &[
            "index.html",
            "img/1.png",
            "img/2.png",
            "img/sub/deep.png",
            "robots.txt",
            "css/main.css",
        ];

        // Serial reference set.
        let store = Store::new();
        let mut serial = PlainManifest::new(store);
        for &path in paths {
            block_on(serial.add(path, make_addr(path))).unwrap();
        }
        let serial_entries = block_on(serial.entries()).unwrap();

        let (m, _) = saved_tracking_manifest(paths);
        let conc = block_on(m.entries_concurrent(DEFAULT_LIST_CONCURRENCY)).unwrap();

        assert_eq!(
            sorted_paths(&serial_entries),
            sorted_paths(&conc),
            "concurrent listing must yield the same entry set as serial"
        );
        assert_eq!(conc.len(), paths.len());
    }

    #[test]
    fn entries_concurrent_is_bounded_and_parallel() {
        // Twenty sibling files share the "file" prefix, so the widest trie
        // level has many forks fetched in one batch.
        let owned: Vec<String> = (0..20).map(|i| format!("file{i:02}.dat")).collect();
        let paths: Vec<&str> = owned.iter().map(String::as_str).collect();

        let (m, expected) = saved_tracking_manifest(&paths);
        let width = 4;
        let entries = block_on(m.entries_concurrent(width)).unwrap();

        let mut got = sorted_paths(&entries);
        let mut want = expected;
        want.sort();
        assert_eq!(got.len(), paths.len());
        got.dedup();
        assert_eq!(got, want, "all sibling files must be listed exactly once");

        let store = m.store();
        assert!(store.gets() > 1, "listing must perform multiple fetches");
        assert!(
            store.max_inflight() > 1,
            "concurrent listing must overlap fetches (got {})",
            store.max_inflight()
        );
        assert!(
            store.max_inflight() <= width,
            "in-flight fetches must stay bounded by width {width} (got {})",
            store.max_inflight()
        );
    }

    #[test]
    fn entries_concurrent_width_one_is_serial() {
        let owned: Vec<String> = (0..12).map(|i| format!("file{i:02}.dat")).collect();
        let paths: Vec<&str> = owned.iter().map(String::as_str).collect();

        let (m, _) = saved_tracking_manifest(&paths);
        let entries = block_on(m.entries_concurrent(1)).unwrap();

        assert_eq!(entries.len(), paths.len());
        assert_eq!(
            m.store().max_inflight(),
            1,
            "width 1 must never overlap fetches"
        );
    }

    #[test]
    fn entries_concurrent_clamps_zero_width() {
        let (m, _) = saved_tracking_manifest(&["a.txt", "b.txt"]);
        let entries = block_on(m.entries_concurrent(0)).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(m.store().max_inflight(), 1, "zero width clamps to serial");
    }

    #[test]
    fn entries_concurrent_empty_manifest() {
        let store = Store::new();
        let m = PlainManifest::new(store);
        let entries = block_on(m.entries_concurrent(DEFAULT_LIST_CONCURRENCY)).unwrap();
        assert!(entries.is_empty(), "empty manifest yields no entries");
    }

    #[test]
    fn entries_concurrent_single_entry() {
        let store = Store::new();
        let mut m = PlainManifest::new(store);
        let addr = make_addr("only");
        block_on(m.add("only.txt", addr)).unwrap();

        let entries = block_on(m.entries_concurrent(DEFAULT_LIST_CONCURRENCY)).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].path(), b"only.txt");
        assert_eq!(entries[0].address(), Some(&addr));
    }

    #[test]
    fn entries_concurrent_deep_trie() {
        let deep_paths: Vec<String> = (0..20)
            .map(|i| format!("a/b/c/d/e/f/g/h/file{i:02}.dat"))
            .collect();
        let paths: Vec<&str> = deep_paths.iter().map(String::as_str).collect();

        let (m, _) = saved_tracking_manifest(&paths);
        let entries = block_on(m.entries_concurrent(DEFAULT_LIST_CONCURRENCY)).unwrap();

        assert_eq!(entries.len(), deep_paths.len());
        let got = sorted_paths(&entries);
        for path in &deep_paths {
            assert!(
                got.iter().any(|p| p == path.as_bytes()),
                "deep path {path} missing from concurrent listing"
            );
        }
    }

    #[test]
    fn entries_concurrent_shared_prefix_branches() {
        // Shared-prefix branches force the trie to split mid-prefix, exercising
        // sibling fan-out at several levels.
        let paths = &[
            "aaaaaa", "aaaaab", "aaabbb", "abbbbb", "abbbba", "bbbbba", "bbbaaa", "bbbaab",
        ];
        let (m, _) = saved_tracking_manifest(paths);
        let entries = block_on(m.entries_concurrent(DEFAULT_LIST_CONCURRENCY)).unwrap();

        assert_eq!(sorted_paths(&entries), {
            let mut v: Vec<Vec<u8>> = paths.iter().map(|p| p.as_bytes().to_vec()).collect();
            v.sort();
            v
        });
    }

    #[test]
    fn iter_partial_then_reiterate_lazy() {
        let store = Store::new();
        let mut m = PlainManifest::new(store);

        let paths = &["x/1.txt", "x/2.txt", "y/1.txt", "y/2.txt", "z.txt"];
        for &path in paths {
            block_on(m.add(path, make_addr(path))).unwrap();
        }

        let root_ref = block_on(m.save()).unwrap();
        let (_, store) = m.into_parts();
        let mut m2 = PlainManifest::open(root_ref, store);

        // Partial iteration on a lazy-loaded manifest.
        {
            let mut iter = m2.iter();
            let _first = block_on(iter.next()).unwrap().unwrap();
        }

        // Re-iterate: previously loaded nodes stay loaded, the rest
        // are lazily fetched again through the raw-pointer path.
        let all = drain(m2.iter()).unwrap();
        assert_eq!(all.len(), paths.len());
        for &path in paths {
            assert!(
                all.iter().any(|e| e.path() == path.as_bytes()),
                "path {path} missing after partial lazy iteration"
            );
        }
    }
}
