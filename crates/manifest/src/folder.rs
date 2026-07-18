//! Folder and website views: a path interpretation layered over the flat
//! byte-keyed KV core.
//!
//! The separator is `F::SEPARATOR`, derived from the key bytes at read time and
//! never stored: a folder is an interpretation of one flat trie, not a second
//! structure. A directory listing is an ordered prefix scan that collapses
//! deeper keys at the next separator and seeks past each subtree it names, so a
//! level lists in O(depth) retained state and never fetches a value chunk.
//! Website serving reads the index- and error-document conventions from the
//! root's typed metadata, so the conventions travel as registered metadata,
//! never magic keys.

use core::mem::size_of;

use bytes::Bytes;
use nectar_primitives::ChunkAddress;
use nectar_primitives::store::MaybeSync;

use crate::format::{Format, V1};
use crate::meta::{KeyId, MetadataKey};
use crate::node::Node;
use crate::reader::{Reader, ReaderError};
use crate::scan::{Cursor, successor};
use crate::store::NodeGet;
use crate::value::{Entry, Key};

/// One immediate child of a listed directory.
///
/// A subdirectory collapses every key beneath it into a single entry whose key
/// ends in the separator; a file is a key with no further separator below the
/// directory.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DirEntry<F: Format = V1> {
    /// A file directly in the directory: its full key and bound value.
    File {
        /// The file's full key.
        key: Key,
        /// The bound value.
        entry: Entry<F>,
    },
    /// A subdirectory: its full key path including the trailing separator.
    Dir {
        /// The subdirectory path, ending in the separator.
        key: Key,
    },
}

impl<F: Format> DirEntry<F> {
    /// The entry's full key path.
    #[must_use]
    pub const fn key(&self) -> &Key {
        match self {
            Self::File { key, .. } | Self::Dir { key } => key,
        }
    }

    /// Whether the entry names a subdirectory.
    #[must_use]
    pub const fn is_dir(&self) -> bool {
        matches!(self, Self::Dir { .. })
    }
}

/// A streaming listing of one directory's immediate children in key order.
///
/// Deeper keys collapse at the next separator into one `Dir` entry, and the
/// walk seeks past each named subtree rather than descending it, so a directory
/// of any width or depth lists in O(depth) retained state and no value fetch.
#[derive(Debug)]
pub struct Listing<'a, S, F: Format = V1> {
    store: &'a S,
    root: ChunkAddress,
    /// The exclusive bound of the directory's prefix range.
    end: Option<Bytes>,
    /// Key bytes the cursor walks below; prepended to each cursor key to
    /// recover the full key. Empty unless the listing delegated to a subtree
    /// root, where the cursor walks the subtree's own key space.
    base: Bytes,
    /// Key bytes consumed by the directory prefix; a child's segment starts
    /// here.
    dir_len: usize,
    /// Set once a named subtree has no successor, so the walk stops.
    done: bool,
    cursor: Cursor<'a, S, F>,
}

impl<S, F> Listing<'_, S, F>
where
    S: NodeGet + MaybeSync,
    F: Format,
{
    /// The next immediate child of the directory in key order, or `None` at its
    /// end.
    ///
    /// Naming a subdirectory reseeks the walk to the least key past that
    /// subtree, so the collapsed keys are never revisited and the value chunks
    /// are never pulled.
    pub async fn next(&mut self) -> Result<Option<DirEntry<F>>, ReaderError> {
        loop {
            if self.done {
                return Ok(None);
            }
            let Some((key, entry)) = self.cursor.next().await? else {
                return Ok(None);
            };
            let full = self.full_key(key);
            let bytes = full.as_bytes();
            let suffix = bytes.get(self.dir_len..).unwrap_or(&[]);
            // The directory path itself is not one of its own children.
            if suffix.is_empty() {
                continue;
            }
            match suffix.iter().position(|&byte| byte == F::SEPARATOR) {
                None => return Ok(Some(DirEntry::File { key: full, entry })),
                Some(cut) => {
                    let through = self.dir_len.saturating_add(cut).saturating_add(1);
                    let dir = bytes.get(..through).unwrap_or(bytes);
                    let dir_key = Key::from(dir);
                    match successor(dir) {
                        Some(start) => {
                            // The cursor walks below `base`, so a reseek strips
                            // the shared base the delegated subtree omits.
                            let rel = start.get(self.base.len()..).unwrap_or(&[]);
                            self.cursor =
                                Cursor::seek(self.store, &self.root, rel, self.end.clone()).await?;
                        }
                        None => self.done = true,
                    }
                    return Ok(Some(DirEntry::Dir { key: dir_key }));
                }
            }
        }
    }

    /// The full key of a cursor step: the delegated base followed by the cursor
    /// key, which is the cursor key itself when the walk is rooted at the
    /// manifest and no base was stripped.
    fn full_key(&self, key: Key) -> Key {
        if self.base.is_empty() {
            return key;
        }
        let mut bytes = Vec::with_capacity(self.base.len().saturating_add(key.len()));
        bytes.extend_from_slice(&self.base);
        bytes.extend_from_slice(key.as_bytes());
        Key::from(bytes)
    }
}

/// The site-level document conventions read from a manifest's root metadata.
///
/// Both are optional and root-scope: an index document is served for a
/// directory path, an error document for an otherwise unresolved path.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Website {
    index: Option<Bytes>,
    error: Option<Bytes>,
}

impl Website {
    /// The index-document key bytes, if the manifest declares one.
    #[must_use]
    pub fn index(&self) -> Option<&[u8]> {
        self.index.as_deref()
    }

    /// The error-document key bytes, if the manifest declares one.
    #[must_use]
    pub fn error(&self) -> Option<&[u8]> {
        self.error.as_deref()
    }
}

/// What serving a request path resolves to under the website conventions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Served<F: Format = V1> {
    /// The path matched a key exactly.
    Exact {
        /// The matched key.
        key: Key,
        /// The bound value.
        entry: Entry<F>,
    },
    /// No exact key matched; the index document for the directory path did.
    Index {
        /// The index-document key that matched.
        key: Key,
        /// The bound value.
        entry: Entry<F>,
    },
    /// Neither the path nor its index document matched; the error document did.
    Error {
        /// The error-document key that matched.
        key: Key,
        /// The bound value.
        entry: Entry<F>,
    },
    /// No key, index document, or error document matched.
    Missing,
}

impl<F: Format> Served<F> {
    /// The resolved value, or `None` when nothing matched.
    #[must_use]
    pub const fn entry(&self) -> Option<&Entry<F>> {
        match self {
            Self::Exact { entry, .. } | Self::Index { entry, .. } | Self::Error { entry, .. } => {
                Some(entry)
            }
            Self::Missing => None,
        }
    }

    /// The resolved key, or `None` when nothing matched.
    #[must_use]
    pub const fn key(&self) -> Option<&Key> {
        match self {
            Self::Exact { key, .. } | Self::Index { key, .. } | Self::Error { key, .. } => {
                Some(key)
            }
            Self::Missing => None,
        }
    }
}

impl<S, F> Reader<S, F>
where
    S: NodeGet + MaybeSync,
    F: Format,
{
    /// List the immediate children of the directory named by `dir` in key
    /// order, collapsing deeper keys at the next separator.
    ///
    /// `dir` is used as a key prefix: the root is the empty key and a nested
    /// directory conventionally ends with the separator. Only the trie nodes on
    /// the frontier are fetched; the value chunks a listing names are not.
    pub async fn list(
        &self,
        root: &ChunkAddress,
        dir: &Key,
    ) -> Result<Listing<'_, S, F>, ReaderError> {
        let prefix = dir.as_bytes();
        // When the directory's keys are exactly one referenced chunk reached at
        // the prefix boundary, delegate the walk to that subtree root: it holds
        // precisely the directory's keys, so the walk starts there and needs no
        // upper bound. A boundary deeper than the prefix, or none, walks from
        // the manifest root as before.
        if let Some(found) = self.descend_subtree(root, prefix).await?
            && found.base == prefix.len()
        {
            let subtree = *found.reference.address();
            let cursor = Cursor::seek(self.store(), &subtree, &[], None).await?;
            return Ok(Listing {
                store: self.store(),
                root: subtree,
                end: None,
                base: Bytes::copy_from_slice(prefix),
                dir_len: prefix.len(),
                done: false,
                cursor,
            });
        }
        let end = successor(prefix);
        let cursor = Cursor::seek(self.store(), root, prefix, end.clone()).await?;
        Ok(Listing {
            store: self.store(),
            root: *root,
            end,
            base: Bytes::new(),
            dir_len: prefix.len(),
            done: false,
            cursor,
        })
    }

    /// The manifest's site-level document conventions, read from the root's
    /// typed metadata.
    pub async fn website(&self, root: &ChunkAddress) -> Result<Website, ReaderError> {
        let node = self.store().get_node::<F>(root).await?;
        Ok(Website {
            index: document(&node, KeyId::WebsiteIndexDocument),
            error: document(&node, KeyId::WebsiteErrorDocument),
        })
    }

    /// Resolve a request path to the entry a website server would return.
    ///
    /// An exact key wins; otherwise the path is read as a directory and its
    /// index document is tried, then the error document. The empty path and a
    /// path already ending in the separator name a directory directly; any
    /// other path is read as a directory by inserting a separator before the
    /// index document.
    pub async fn serve(&self, root: &ChunkAddress, path: &Key) -> Result<Served<F>, ReaderError> {
        if let Some(entry) = self.get(root, path).await? {
            return Ok(Served::Exact {
                key: path.clone(),
                entry,
            });
        }
        let site = self.website(root).await?;
        if let Some(index) = site.index {
            let key = directory_index::<F>(path.as_bytes(), &index);
            if let Some(entry) = self.get(root, &key).await? {
                return Ok(Served::Index { key, entry });
            }
        }
        if let Some(error) = site.error {
            let key = Key::from(&error[..]);
            if let Some(entry) = self.get(root, &key).await? {
                return Ok(Served::Error { key, entry });
            }
        }
        Ok(Served::Missing)
    }
}

/// A root-scope metadata document value, cloned out of the node's metadata.
fn document<F: Format>(node: &Node<F>, id: KeyId) -> Option<Bytes> {
    node.metadata()?.get(&MetadataKey::from(id)).cloned()
}

/// The index-document key for `path` read as a directory: `path`, a separator
/// unless `path` is empty or already ends with one, then the index document.
fn directory_index<F: Format>(path: &[u8], index: &[u8]) -> Key {
    let mut out = Vec::with_capacity(
        path.len()
            .saturating_add(index.len())
            .saturating_add(size_of::<u8>()),
    );
    out.extend_from_slice(path);
    if !path.is_empty() && path.last() != Some(&F::SEPARATOR) {
        out.push(F::SEPARATOR);
    }
    out.extend_from_slice(index);
    Key::from(out)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use nectar_primitives::store::MemoryStore;
    use nectar_primitives::{ChunkAddress, ChunkRef};
    use nectar_testing::run;

    use crate::builder::Builder;
    use crate::meta::{KeyId, Metadata};

    use super::*;

    fn entry(byte: u8) -> Entry {
        ChunkRef::new(ChunkAddress::new([byte; 32])).into()
    }

    /// Build a manifest from `(key, value)` pairs and return its root address.
    async fn manifest(store: &MemoryStore, pairs: &[(&[u8], u8)]) -> ChunkAddress {
        let mut builder = Builder::new();
        for (key, byte) in pairs {
            builder.insert(Key::from(&key[..]), entry(*byte), None);
        }
        *builder.build(store).await.unwrap().root()
    }

    /// Drain a listing into its entries.
    async fn entries(mut listing: Listing<'_, &MemoryStore>) -> Vec<DirEntry> {
        let mut out = Vec::new();
        while let Some(item) = listing.next().await.unwrap() {
            out.push(item);
        }
        out
    }

    fn file(key: &[u8], byte: u8) -> DirEntry {
        DirEntry::File {
            key: Key::from(key),
            entry: entry(byte),
        }
    }

    fn dir(key: &[u8]) -> DirEntry {
        DirEntry::Dir {
            key: Key::from(key),
        }
    }

    #[test]
    fn list_collapses_subdirectories_at_the_separator() {
        run(async {
            let store = MemoryStore::default();
            let root = manifest(
                &store,
                &[
                    (b"index.html", 0x01),
                    (b"img/logo.png", 0x02),
                    (b"img/icons/a.png", 0x03),
                    (b"style.css", 0x04),
                ],
            )
            .await;
            let reader: Reader<_> = Reader::new(&store);

            // The root lists two files and one collapsed directory, in key order.
            let got = entries(reader.list(&root, &Key::empty()).await.unwrap()).await;
            assert_eq!(
                got,
                vec![
                    dir(b"img/"),
                    file(b"index.html", 0x01),
                    file(b"style.css", 0x04),
                ]
            );
        })
    }

    #[test]
    fn list_of_a_nested_directory_reads_one_level() {
        run(async {
            let store = MemoryStore::default();
            let root = manifest(
                &store,
                &[
                    (b"img/logo.png", 0x02),
                    (b"img/icons/a.png", 0x03),
                    (b"img/icons/b.png", 0x05),
                    (b"other.txt", 0x06),
                ],
            )
            .await;
            let reader: Reader<_> = Reader::new(&store);

            let got = entries(reader.list(&root, &Key::from(&b"img/"[..])).await.unwrap()).await;
            assert_eq!(got, vec![dir(b"img/icons/"), file(b"img/logo.png", 0x02)]);
        })
    }

    #[test]
    fn list_collapses_consecutive_subdirectories() {
        run(async {
            let store = MemoryStore::default();
            // Two subdirectories in a row exercise the reseek-then-collapse-again
            // path: each named subtree must be skipped without swallowing the next.
            let root = manifest(
                &store,
                &[
                    (b"a/1", 0x01),
                    (b"a/2", 0x02),
                    (b"b/1", 0x03),
                    (b"c.txt", 0x04),
                ],
            )
            .await;
            let reader: Reader<_> = Reader::new(&store);

            let got = entries(reader.list(&root, &Key::empty()).await.unwrap()).await;
            assert_eq!(got, vec![dir(b"a/"), dir(b"b/"), file(b"c.txt", 0x04)]);
        })
    }

    #[test]
    fn list_skips_the_directory_key_itself() {
        run(async {
            let store = MemoryStore::default();
            // A key exactly equal to the listed directory path is the directory
            // itself, not a child.
            let root = manifest(&store, &[(b"dir/", 0x01), (b"dir/a", 0x02)]).await;
            let reader: Reader<_> = Reader::new(&store);

            let got = entries(reader.list(&root, &Key::from(&b"dir/"[..])).await.unwrap()).await;
            assert_eq!(got, vec![file(b"dir/a", 0x02)]);
        })
    }

    #[test]
    fn list_does_not_fetch_deeper_subtrees() {
        run(async {
            let store = CountingStore::default();
            // A wide, deep subdirectory the listing must not descend into.
            let mut pairs: Vec<(Vec<u8>, u8)> = Vec::new();
            for i in 0u8..64 {
                pairs.push((format!("deep/{i:03}/leaf").into_bytes(), i));
            }
            pairs.push((b"top.txt".to_vec(), 0xFF));
            let refs: Vec<(&[u8], u8)> = pairs.iter().map(|(k, v)| (k.as_slice(), *v)).collect();
            let root = manifest(&store.inner, &refs).await;
            store.reset();

            let reader: Reader<_> = Reader::new(&store);
            let got = entries_counting(reader.list(&root, &Key::empty()).await.unwrap()).await;
            assert_eq!(got, vec![dir(b"deep/"), file(b"top.txt", 0xFF)]);
            // Seeking past the subtree keeps the fetch count to the frontier, far
            // below the 64 leaves under it.
            assert!(store.gets() < 16, "fetched {} nodes", store.gets());
        })
    }

    #[test]
    fn list_delegates_a_referenced_subtree() {
        run(async {
            use crate::bounded::Prefix;
            use crate::fork::{Child, ForkTable};
            use crate::node::Node;
            use crate::store::NodePut;

            let store = MemoryStore::default();
            // A referenced "mg/" subtree holding a nested subdirectory and a file,
            // so listing it delegates to the subtree root and still collapses and
            // reseeks in the subtree's own key space.
            let mut inner = ForkTable::new();
            inner
                .insert(
                    Prefix::try_from(&b"1"[..]).unwrap(),
                    entry(0x01).into(),
                    None,
                )
                .unwrap();
            inner
                .insert(
                    Prefix::try_from(&b"2"[..]).unwrap(),
                    entry(0x02).into(),
                    None,
                )
                .unwrap();
            let mut leaf = ForkTable::new();
            leaf.insert(
                Prefix::try_from(&b"a/"[..]).unwrap(),
                Child::Embedded(inner).into(),
                None,
            )
            .unwrap();
            leaf.insert(
                Prefix::try_from(&b"logo.png"[..]).unwrap(),
                entry(0xBB).into(),
                None,
            )
            .unwrap();
            let leaf_ref = store.put_node(&Node::new(None, leaf)).await.unwrap();

            let mut forks: ForkTable = ForkTable::new();
            forks
                .insert(
                    Prefix::try_from(&b"mg/"[..]).unwrap(),
                    Child::Ref32(ChunkRef::new(leaf_ref)).into(),
                    None,
                )
                .unwrap();
            let root = store.put_node(&Node::new(None, forks)).await.unwrap();

            let reader: Reader<_> = Reader::new(&store);
            let got = entries(reader.list(&root, &Key::from(&b"mg/"[..])).await.unwrap()).await;
            assert_eq!(got, vec![dir(b"mg/a/"), file(b"mg/logo.png", 0xBB)]);
        })
    }

    #[test]
    fn serve_prefers_an_exact_key() {
        run(async {
            let store = MemoryStore::default();
            let root = manifest(&store, &[(b"a.html", 0x01)]).await;
            let reader: Reader<_> = Reader::new(&store);
            assert_eq!(
                reader
                    .serve(&root, &Key::from(&b"a.html"[..]))
                    .await
                    .unwrap(),
                Served::Exact {
                    key: Key::from(&b"a.html"[..]),
                    entry: entry(0x01),
                }
            );
        })
    }

    #[test]
    fn serve_falls_back_to_the_index_document() {
        run(async {
            let store = MemoryStore::default();
            let mut builder = Builder::new();
            builder.insert(Key::from(&b"index.html"[..]), entry(0x01), None);
            builder.insert(Key::from(&b"docs/index.html"[..]), entry(0x02), None);
            builder.manifest_metadata(
                Metadata::new(
                    KeyId::WebsiteIndexDocument,
                    Bytes::from_static(b"index.html"),
                )
                .unwrap(),
            );
            let root = *builder.build(&store).await.unwrap().root();
            let reader: Reader<_> = Reader::new(&store);

            // The root path resolves to the top-level index document.
            assert_eq!(
                reader.serve(&root, &Key::empty()).await.unwrap(),
                Served::Index {
                    key: Key::from(&b"index.html"[..]),
                    entry: entry(0x01),
                }
            );
            // A directory path (trailing separator) resolves to its index document.
            assert_eq!(
                reader
                    .serve(&root, &Key::from(&b"docs/"[..]))
                    .await
                    .unwrap(),
                Served::Index {
                    key: Key::from(&b"docs/index.html"[..]),
                    entry: entry(0x02),
                }
            );
            // A directory path without a trailing separator is read as one too.
            assert_eq!(
                reader.serve(&root, &Key::from(&b"docs"[..])).await.unwrap(),
                Served::Index {
                    key: Key::from(&b"docs/index.html"[..]),
                    entry: entry(0x02),
                }
            );
        })
    }

    #[test]
    fn serve_falls_back_to_the_error_document() {
        run(async {
            let store = MemoryStore::default();
            let mut builder = Builder::new();
            builder.insert(Key::from(&b"404.html"[..]), entry(0x09), None);
            builder.manifest_metadata(
                Metadata::new(KeyId::WebsiteErrorDocument, Bytes::from_static(b"404.html"))
                    .unwrap(),
            );
            let root = *builder.build(&store).await.unwrap().root();
            let reader: Reader<_> = Reader::new(&store);

            assert_eq!(
                reader
                    .serve(&root, &Key::from(&b"missing"[..]))
                    .await
                    .unwrap(),
                Served::Error {
                    key: Key::from(&b"404.html"[..]),
                    entry: entry(0x09),
                }
            );
        })
    }

    #[test]
    fn serve_missing_without_conventions_is_missing() {
        run(async {
            let store = MemoryStore::default();
            let root = manifest(&store, &[(b"a.html", 0x01)]).await;
            let reader: Reader<_> = Reader::new(&store);
            assert_eq!(
                reader.serve(&root, &Key::from(&b"nope"[..])).await.unwrap(),
                Served::Missing
            );
        })
    }

    #[test]
    fn website_reads_the_root_conventions() {
        run(async {
            let store = MemoryStore::default();
            let mut builder = Builder::new();
            builder.insert(Key::from(&b"index.html"[..]), entry(0x01), None);
            let mut meta = Metadata::new(
                KeyId::WebsiteIndexDocument,
                Bytes::from_static(b"index.html"),
            )
            .unwrap();
            meta.insert(KeyId::WebsiteErrorDocument, Bytes::from_static(b"404.html"))
                .unwrap();
            builder.manifest_metadata(meta);
            let root = *builder.build(&store).await.unwrap().root();
            let reader: Reader<_> = Reader::new(&store);

            let site = reader.website(&root).await.unwrap();
            assert_eq!(site.index(), Some(&b"index.html"[..]));
            assert_eq!(site.error(), Some(&b"404.html"[..]));
        })
    }

    // A trusted store that counts each fetch, so a test can read off how many
    // nodes a listing pulled.
    use core::sync::atomic::{AtomicUsize, Ordering};

    use nectar_primitives::store::ChunkGet;
    use nectar_primitives::{Chunk, StandardChunkSet, Verified};

    #[derive(Debug, Default)]
    struct CountingStore {
        inner: MemoryStore,
        gets: AtomicUsize,
    }

    impl CountingStore {
        fn gets(&self) -> usize {
            self.gets.load(Ordering::Relaxed)
        }

        fn reset(&self) {
            self.gets.store(0, Ordering::Relaxed);
        }
    }

    impl ChunkGet<StandardChunkSet> for CountingStore {
        type Trust = Verified;
        type Error = <MemoryStore as ChunkGet>::Error;

        async fn get(
            &self,
            address: &ChunkAddress,
        ) -> Result<Chunk<Verified, StandardChunkSet>, Self::Error> {
            self.gets.fetch_add(1, Ordering::Relaxed);
            ChunkGet::get(&self.inner, address).await
        }
    }

    async fn entries_counting(mut listing: Listing<'_, &CountingStore>) -> Vec<DirEntry> {
        let mut out = Vec::new();
        while let Some(item) = listing.next().await.unwrap() {
            out.push(item);
        }
        out
    }
}
