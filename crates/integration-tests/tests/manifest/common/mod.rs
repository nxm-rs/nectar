//! Shared fixtures for the manifest seam tests.
#![allow(dead_code)]

use std::ops::Bound;
use std::sync::Arc;

use nectar_file::{DataSink, File, MemSink, Policy};
use nectar_ldb::Database;
use nectar_manifest::{
    Batch, Manifest, ManifestCursor, ManifestError, ManifestPath, ManifestView, MapEntry,
};
use nectar_mantaray::{MantarayManifest, NodeLoadSaver};
use nectar_primitives::store::{ContentGet, MemoryStore};
use nectar_primitives::{ChunkAddress, ChunkRef, DEFAULT_BODY_SIZE, StandardChunkSet};

/// Shared chunk store: `MemoryStore` clones its contents, so every handle in
/// one test has to reach the same map.
pub type Raw = Arc<MemoryStore<StandardChunkSet>>;
pub type Store = ContentGet<Raw>;
pub type Nodes = NodeLoadSaver<Raw>;
pub type Trie = MantarayManifest<Nodes, Store, DEFAULT_BODY_SIZE>;
pub type Kv = Database<Store>;

/// A fresh raw store and its content-addressed read handle.
pub fn stores() -> (Raw, Store) {
    let raw: Raw = Arc::new(MemoryStore::new());
    let store = ContentGet::new(Arc::clone(&raw));
    (raw, store)
}

/// Both formats over one raw store, each at its freshly persisted empty root.
pub async fn both_formats(raw: &Raw, store: &Store) -> ((Trie, ChunkRef), (Kv, ChunkRef)) {
    let trie: Trie = MantarayManifest::new(NodeLoadSaver::new(Arc::clone(raw)), store.clone());
    let trie_empty = trie.empty().await.unwrap();
    let kv = Database::<_>::plain(store.clone());
    let kv_empty = kv.empty().await.unwrap();
    ((trie, trie_empty), (kv, kv_empty))
}

/// `data` saved as a plain file tree, as the reference an entry binds.
pub async fn save_file(raw: &Raw, data: &[u8]) -> ChunkRef {
    let saver = File::<_, DEFAULT_BODY_SIZE>::new(raw, Policy::DEFAULT);
    ChunkRef::new(saver.save(data).await.unwrap())
}

/// A reference standing in for a file root; no chunk behind it is read.
pub fn reference(byte: u8) -> ChunkRef {
    ChunkRef::new(ChunkAddress::new([byte; 32]))
}

/// A path as text, for a readable assertion message.
pub fn text(path: &ManifestPath) -> String {
    String::from_utf8(path.as_bytes().to_vec()).unwrap()
}

/// `path` as a [`ManifestPath`]; the tests' one spelling.
pub fn p(path: &str) -> ManifestPath {
    ManifestPath::from(path)
}

/// One-shot `remove`, unwrapped.
pub async fn removed<M: Manifest<ChunkRef>>(manifest: &M, root: ChunkRef, path: &str) -> ChunkRef {
    manifest.remove(root, p(path)).await.unwrap()
}

/// A document-delta batch applied alone: `Some(Some(_))` sets a document,
/// `Some(None)` clears it, `None` leaves it unstaged.
pub async fn doc_delta<M: Manifest<ChunkRef>>(
    manifest: &M,
    base: &ChunkRef,
    index: Option<Option<&str>>,
    error: Option<Option<&str>>,
) -> ChunkRef {
    applied(manifest, base, |batch| {
        if let Some(index) = index {
            batch.set_index_document(index.map(ManifestPath::from));
        }
        if let Some(error) = error {
            batch.set_error_document(error.map(ManifestPath::from));
        }
    })
    .await
}

/// One batch staged by `stage` and applied to `base`, unwrapped.
pub async fn applied<M: Manifest<ChunkRef>>(
    manifest: &M,
    base: &ChunkRef,
    stage: impl FnOnce(&mut Batch<ChunkRef, M::Metadata>),
) -> ChunkRef {
    let mut batch = Batch::new();
    stage(&mut batch);
    manifest.apply(*base, batch).await.unwrap()
}

/// `keys` written through the seam in one batch, each bound to a distinct
/// reference: the key at `index` gets `reference(index + 1)`.
pub async fn write_keys<M: Manifest<ChunkRef>>(
    manifest: &M,
    empty: &ChunkRef,
    keys: &[&str],
) -> ChunkRef {
    applied(manifest, empty, |batch| {
        for (index, key) in keys.iter().enumerate() {
            let byte = u8::try_from(index).unwrap().saturating_add(1);
            batch.insert(ManifestPath::from(*key), reference(byte));
        }
    })
    .await
}

/// Build a manifest holding exactly `keys`, in the order given.
pub async fn build<M: Manifest<ChunkRef>>(
    manifest: &M,
    empty: &ChunkRef,
    keys: &[(ManifestPath, u8)],
) -> ChunkRef {
    applied(manifest, empty, |batch| {
        for (path, byte) in keys {
            batch.insert(path.clone(), reference(*byte));
        }
    })
    .await
}

/// The reserved path a refused write names, structurally over every format.
pub fn refused<T, F>(result: &Result<T, ManifestError<F>>) -> Option<ManifestPath> {
    result
        .as_ref()
        .err()?
        .as_reserved()
        .map(|reserved| reserved.path().clone())
}

/// Every path a manifest holds, in walk order.
pub async fn key_set<M: Manifest<ChunkRef>>(manifest: &M, root: &ChunkRef) -> Vec<String> {
    drain(&manifest.at(*root), (Bound::Unbounded, Bound::Unbounded)).await
}

/// Every path a bounded walk yields, in order.
pub async fn drain<V: ManifestView<ChunkRef>>(
    view: &V,
    bounds: (Bound<ManifestPath>, Bound<ManifestPath>),
) -> Vec<String> {
    let mut out = Vec::new();
    let mut cursor = view.range(bounds).await.unwrap();
    while let Some((path, _)) = cursor.next().await.unwrap() {
        out.push(text(&path));
    }
    out
}

/// Every path one directory level lists, in order.
pub async fn listing<V: ManifestView<ChunkRef>>(view: &V, dir: &ManifestPath) -> Vec<String> {
    view.dir(dir)
        .await
        .unwrap()
        .entries()
        .iter()
        .map(|entry| text(entry.path()))
        .collect()
}

/// Assert the two site documents on one root.
pub async fn docs<M>(
    manifest: &M,
    root: &ChunkRef,
    index: Option<&str>,
    error: Option<&str>,
    hint: &str,
) where
    M: Manifest<ChunkRef>,
{
    let view = manifest.at(*root);
    let index_doc = view.index_document().await.unwrap().map(|p| text(&p));
    assert_eq!(index_doc, index.map(String::from), "{hint}: index document");
    let error_doc = view.error_document().await.unwrap().map(|p| text(&p));
    assert_eq!(error_doc, error.map(String::from), "{hint}: error document");
}

/// Assert `get` over a table of (path, bound reference byte or absent).
pub async fn refs<M: Manifest<ChunkRef>>(
    manifest: &M,
    root: &ChunkRef,
    cases: &[(&str, Option<u8>)],
    hint: &str,
) {
    let view = manifest.at(*root);
    for (path, byte) in cases {
        let got = view.get(&ManifestPath::from(*path)).await.unwrap();
        let want = byte.map(|byte| MapEntry::Reference(reference(byte)));
        assert_eq!(got, want, "{hint}: {path:?}");
    }
}

/// The paths every scenario probes: the two reserved ones, a top-level file, a
/// directory key and a file below it, an interior key with siblings past it,
/// and one absent path.
pub fn probed() -> Vec<ManifestPath> {
    let a = ["", "/", "!", "404.html", "a", "ab"];
    let b = ["ac", "img/", "img/logo.png", "index.html", "zz"];
    a.into_iter().chain(b).map(ManifestPath::from).collect()
}

/// The removal scenarios' key set: every shape a removal can leave behind: a
/// mid-edge split, a shared directory, a top-level key sharing one byte with a
/// directory, a lone leaf, and pairs past the trie's 30-byte edge bound.
pub fn removal_keys() -> Vec<(ManifestPath, u8)> {
    let deep = |tail: &str| ManifestPath::from(format!("deep/{}{tail}", "a".repeat(30)).as_str());
    let under =
        |dir: &str, len: usize| ManifestPath::from(format!("{dir}{}", "e".repeat(len)).as_str());
    [
        (ManifestPath::from("alpha"), 1u8),
        (ManifestPath::from("alpine"), 2),
        (ManifestPath::from("beta"), 3),
        (ManifestPath::from("img/icon.png"), 4),
        (ManifestPath::from("img/logo.png"), 5),
        (ManifestPath::from("index.html"), 6),
        (deep("one"), 7),
        (deep("two"), 8),
        // 5 + 28 bytes: what a removal of "abcde" leaves joins to 33, past the
        // bound; 5 + 31 under a directory runs through two nodes.
        (ManifestPath::from("abcde"), 9),
        (under("abcde", 28), 10),
        (ManifestPath::from("wiki/"), 11),
        (under("wiki/", 31), 12),
    ]
    .into_iter()
    .collect()
}

/// `keys` with the key at `index` removed: what an exact-key remove leaves.
pub fn survivors(keys: &[(ManifestPath, u8)], index: usize) -> Vec<(ManifestPath, u8)> {
    keys.iter()
        .enumerate()
        .filter(|(other, _)| *other != index)
        .map(|(_, key)| key.clone())
        .collect()
}

/// One format's answers to one scenario, in a format-independent shape: every
/// walk, listing and document read, plus one [`Probe`] row per probed path.
#[derive(Debug, PartialEq, Eq)]
pub struct Observed {
    pub keys: Vec<String>,
    pub ranged: Vec<String>,
    pub listed: Vec<String>,
    pub prefixed: Vec<String>,
    pub index_document: Option<String>,
    pub error_document: Option<String>,
    pub probes: Vec<Probe>,
}

/// Every point read at one path: the entry, presence, bare metadata, the
/// floor, and whether a load reached the sink.
#[derive(Debug, PartialEq, Eq)]
pub struct Probe {
    pub path: String,
    pub entry: Option<MapEntry<ChunkRef>>,
    pub present: bool,
    pub bare: bool,
    pub floor: Option<String>,
    pub loaded: bool,
}

/// Read every verb over `root` at each of `paths`, in a format-independent
/// shape.
pub async fn observe<M>(manifest: &M, root: &ChunkRef, paths: &[ManifestPath]) -> Observed
where
    M: Manifest<ChunkRef>,
    M::Metadata: Clone + PartialEq + std::fmt::Debug,
{
    let view = manifest.at(*root);
    let mut probes = Vec::new();
    for path in paths {
        let meta = view.metadata(path).await.unwrap();
        let mut sink = MemSink::new();
        probes.push(Probe {
            path: text(path),
            entry: view.get(path).await.unwrap(),
            present: view.contains_key(path).await.unwrap(),
            bare: meta == M::Metadata::default(),
            floor: view.floor(path).await.unwrap().map(|(path, _)| text(&path)),
            loaded: view.load(path, &mut sink).await.is_ok(),
        });
    }
    Observed {
        keys: key_set(manifest, root).await,
        ranged: drain(&view, (Bound::Unbounded, Bound::Unbounded)).await,
        listed: listing(&view, &ManifestPath::default()).await,
        prefixed: listing(&view, &ManifestPath::from("a")).await,
        index_document: view.index_document().await.unwrap().map(|p| text(&p)),
        error_document: view.error_document().await.unwrap().map(|p| text(&p)),
        probes,
    }
}

/// Every root observed in turn, at the standard [`probed`] paths.
pub async fn observed<M>(manifest: &M, roots: &[&ChunkRef]) -> Vec<Observed>
where
    M: Manifest<ChunkRef>,
    M::Metadata: Clone + PartialEq + std::fmt::Debug,
{
    let paths = probed();
    let mut out = Vec::with_capacity(roots.len());
    for root in roots {
        out.push(observe(manifest, root, &paths).await);
    }
    out
}

/// A sink that refuses every write.
#[derive(Debug)]
pub struct RefusingSink;

/// The refusal [`RefusingSink`] reports; the seam has to keep it reachable.
#[derive(Debug, thiserror::Error)]
#[error("the sink refused the write")]
pub struct Refused;

impl DataSink for RefusingSink {
    type Error = Refused;

    fn write_at(&mut self, _offset: u64, _data: &[u8]) -> Result<(), Self::Error> {
        Err(Refused)
    }
}
