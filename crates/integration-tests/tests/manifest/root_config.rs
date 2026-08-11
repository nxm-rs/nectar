//! The convergence gate: content keys are bare, the manifest's own
//! configuration is an option and not a key, and `remove` is exact-key.
//! Identically on both manifest formats.
//!
//! Every scenario is generic over the [`Manifest`] seam and runs twice, once
//! per format. The assertions inside a scenario check the contract, and the
//! [`Observed`] comparison in each test checks that the two formats answer
//! alike.
//!
//! # The contract under test
//!
//! A content key is the path bytes verbatim, with nothing prepended.
//!
//! The site index and error documents are read as `Option<ManifestPath>` and
//! written through `Batch::set_index_document` and `Batch::set_error_document`.
//! Each lands in the format's own root slot, which is never a key. The two
//! reserved paths are the empty one and `"/"`: a read at either is absent and
//! a write at either refuses the whole batch as `ReservedKey`, in the seam,
//! before the format runs.
//!
//! `remove` is exact-key on both formats: the path's own value and metadata go,
//! and no other path does. A path with children keeps every one of them, a
//! childless leaf is pruned, and removing an absent path is a no-op.
//!
//! # History-independence is a key-value database guarantee only
//!
//! What crosses the formats after a removal is the key set, not the root.
//! `a_remove_is_history_independent_on_ldb` pins the root on the key-value
//! database alone, whose packing derives the whole shape from the key set.
//! mantaray 0.2 puts a node where the insert order first justified one and
//! never moves it, so it cannot have that without breaking the wire
//! `mantaray/bee_vectors.rs` pins.

use std::ops::Bound;
use std::sync::Arc;

use nectar_file::{DataSink, File, MemSink, Policy};
use nectar_ldb::{Builder, Database, Entry, Key, Plaintext, Reader as LdbReader, Served, V1, Website};
use nectar_loadsave::NodeLoadSaver;
use nectar_manifest::{
    Batch, Manifest, ManifestError, ManifestMeta, ManifestOp, ManifestPath, MapCursor, MapEntry,
    MapView, MetadataView, WellKnownKey,
};
use nectar_mantaray::{ManifestEditor, MantarayManifest};
use nectar_primitives::store::{ContentGet, MemoryStore};
use nectar_primitives::{ChunkAddress, ChunkRef, DEFAULT_BODY_SIZE, StandardChunkSet};
use nectar_testing::run;

/// Shared: `MemoryStore` clones its contents, so every handle in one test has
/// to reach the same map.
type Raw = Arc<MemoryStore<StandardChunkSet>>;
type Store = ContentGet<Raw>;
type Trie = MantarayManifest<NodeLoadSaver<Raw>, Store, DEFAULT_BODY_SIZE>;
type Kv = Database<Store>;

/// Both formats over one raw store, each at its freshly persisted empty root.
async fn both_formats(raw: &Raw, store: &Store) -> ((Trie, ChunkRef), (Kv, ChunkRef)) {
    let trie: Trie = MantarayManifest::new(NodeLoadSaver::new(Arc::clone(raw)), store.clone());
    let trie_empty = trie.empty().await.unwrap();
    let kv = Database::<_>::plain(store.clone());
    let kv_empty = kv.empty().await.unwrap();
    ((trie, trie_empty), (kv, kv_empty))
}

/// `keys` written through the seam in one batch, each bound to a distinct
/// reference.
async fn write_keys<M: Manifest<ChunkRef>>(
    manifest: &M,
    empty: &ChunkRef,
    keys: &[&str],
) -> ChunkRef {
    let mut batch = Batch::new();
    for (index, key) in keys.iter().enumerate() {
        let bound = reference(u8::try_from(index).unwrap().saturating_add(1));
        batch.insert(ManifestPath::from(*key), bound);
    }
    manifest.apply(*empty, batch).await.unwrap()
}

/// A filename joined below each directory, so relative on purpose.
const INDEX: &str = "index.html";
/// One whole content key.
const ERROR: &str = "404.html";

/// A reference standing in for a file root; no chunk behind it is read.
fn reference(byte: u8) -> ChunkRef {
    ChunkRef::new(ChunkAddress::new([byte; 32]))
}

/// A path as text, for a readable assertion message.
fn text(path: &ManifestPath) -> String {
    String::from_utf8(path.as_bytes().to_vec()).unwrap()
}

/// The reserved path a refused write names, structurally over every format.
fn refused<T, F>(result: &Result<T, ManifestError<F>>) -> Option<ManifestPath> {
    result
        .as_ref()
        .err()?
        .as_reserved()
        .map(|reserved| reserved.path().clone())
}

/// One format's answers to one scenario, in a format-independent shape.
#[derive(Debug, PartialEq, Eq)]
struct Observed {
    /// Every path `iter` yields, in order.
    keys: Vec<String>,
    /// Every path `range(..)` yields, in order.
    ranged: Vec<String>,
    /// The top level: every path `dir("")` lists, in order.
    listed: Vec<String>,
    /// Every path `dir("a")` lists: a prefix with no trailing separator.
    prefixed: Vec<String>,
    /// The index document the manifest declares.
    index_document: Option<String>,
    /// The error document the manifest declares.
    error_document: Option<String>,
    /// One row per probed path.
    probes: Vec<Probe>,
}

/// Every point read at one path.
#[derive(Debug, PartialEq, Eq)]
struct Probe {
    path: String,
    entry: Option<MapEntry<ChunkRef>>,
    present: bool,
    /// Whether the path carries the format's empty metadata.
    bare: bool,
    /// The greatest bound path at or below this one.
    floor: Option<String>,
    /// Whether a load of the path reached the sink.
    loaded: bool,
}

/// Read every verb over `root` at each of `paths`, in a format-independent
/// shape.
async fn observe<M>(manifest: &M, root: &ChunkRef, paths: &[ManifestPath]) -> Observed
where
    M: Manifest<ChunkRef>,
    M::Metadata: Clone + PartialEq + std::fmt::Debug,
{
    let view = manifest.at(root);

    let keys = key_set(manifest, root).await;
    let ranged = drain(&view, (Bound::Unbounded, Bound::Unbounded)).await;
    let listed = listing(&view, &ManifestPath::default()).await;
    let prefixed = listing(&view, &ManifestPath::from("a")).await;

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
        keys,
        ranged,
        listed,
        prefixed,
        index_document: view.index_document().await.unwrap().map(|p| text(&p)),
        error_document: view.error_document().await.unwrap().map(|p| text(&p)),
        probes,
    }
}

/// Every path a bounded walk yields, in order.
async fn drain<V: MapView<ChunkRef>>(
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
async fn listing<V: MapView<ChunkRef>>(view: &V, dir: &ManifestPath) -> Vec<String> {
    view.dir(dir)
        .await
        .unwrap()
        .entries()
        .iter()
        .map(|entry| text(entry.path()))
        .collect()
}

/// The paths every scenario probes: the two reserved ones, a top-level file, a
/// directory key and a file below it, an interior key with siblings past it,
/// and one absent path.
fn probed() -> Vec<ManifestPath> {
    [
        "",
        "/",
        "!",
        "404.html",
        "a",
        "ab",
        "ac",
        "img/",
        "img/logo.png",
        "index.html",
        "zz",
    ]
    .into_iter()
    .map(ManifestPath::from)
    .collect()
}

/// The site documents: set, read back, cleared, and never a key in the map.
async fn the_site_config_is_an_option_and_not_a_key<M>(
    manifest: &M,
    empty: &ChunkRef,
) -> Vec<Observed>
where
    M: Manifest<ChunkRef>,
    M::Metadata: Clone + PartialEq + std::fmt::Debug,
{
    let index = ManifestPath::from("index.html");
    let logo = ManifestPath::from("img/logo.png");
    let empty_path = ManifestPath::default();
    let separator = ManifestPath::from("/");

    // Content only: nothing declares a document yet.
    let mut batch = Batch::new();
    batch.insert(index.clone(), reference(1));
    batch.insert(logo.clone(), reference(2));
    let content = manifest.apply(*empty, batch).await.unwrap();
    let view = manifest.at(&content);
    assert_eq!(
        view.index_document().await.unwrap(),
        None,
        "an unset index document reads as None"
    );
    assert_eq!(
        view.error_document().await.unwrap(),
        None,
        "an unset error document reads as None"
    );

    // Both documents.
    let mut batch = Batch::new();
    batch.set_index_document(ManifestPath::from(INDEX));
    batch.set_error_document(ManifestPath::from(ERROR));
    let configured = manifest.apply(content, batch).await.unwrap();
    let view = manifest.at(&configured);
    assert_eq!(
        view.index_document().await.unwrap().map(|p| text(&p)),
        Some(String::from(INDEX)),
        "the index document reads back"
    );
    assert_eq!(
        view.error_document().await.unwrap().map(|p| text(&p)),
        Some(String::from(ERROR)),
        "the error document reads back"
    );
    assert_ne!(
        configured, content,
        "declaring a document moves the manifest root"
    );

    // The slot is not a key: no map verb reaches it.
    for path in [&empty_path, &separator] {
        assert_eq!(
            view.get(path).await.unwrap(),
            None,
            "{:?} binds nothing",
            text(path)
        );
        assert!(
            !view.contains_key(path).await.unwrap(),
            "{:?} is unbound",
            text(path)
        );
        assert_eq!(
            view.metadata(path).await.unwrap(),
            M::Metadata::default(),
            "{:?} carries no metadata",
            text(path)
        );
        assert!(
            view.load(path, &mut MemSink::new()).await.is_err(),
            "{:?} names no data",
            text(path)
        );
    }

    // No walk yields it either.
    assert_eq!(
        key_set(manifest, &configured).await,
        ["img/logo.png", "index.html"],
        "iter yields content keys, bare and in order"
    );
    assert_eq!(
        listing(&view, &empty_path).await,
        ["img/", "index.html"],
        "the top level lists content alone"
    );

    // A write at a reserved path is refused, and named.
    for path in [&empty_path, &separator] {
        assert_eq!(
            refused(
                &manifest
                    .insert(&configured, path.clone(), reference(9))
                    .await
            ),
            Some(path.clone()),
            "an insert at {:?} is refused as reserved",
            text(path)
        );
        assert_eq!(
            refused(&manifest.remove(&configured, path.clone()).await),
            Some(path.clone()),
            "a remove at {:?} is refused as reserved",
            text(path)
        );
    }

    // The refusal takes the whole batch with it.
    let mut batch = Batch::new();
    batch.insert(ManifestPath::from("landed.html"), reference(9));
    batch.insert(separator.clone(), reference(9));
    let mixed = manifest.apply(configured, batch).await;
    assert_eq!(
        refused(&mixed),
        Some(separator.clone()),
        "the batch is refused for the reserved path in it"
    );
    assert!(
        !manifest
            .at(&configured)
            .contains_key(&ManifestPath::from("landed.html"))
            .await
            .unwrap(),
        "and the op beside it did not land"
    );

    // The two documents are independent.
    let mut batch = Batch::new();
    batch.set_index_document(ManifestPath::from(INDEX));
    let index_only = manifest.apply(content, batch).await.unwrap();
    let view = manifest.at(&index_only);
    assert_eq!(
        view.index_document().await.unwrap().map(|p| text(&p)),
        Some(String::from(INDEX)),
        "the index document is declared"
    );
    assert_eq!(
        view.error_document().await.unwrap(),
        None,
        "and the error document is not"
    );

    // Clearing is the same setter with `None`.
    let mut batch = Batch::new();
    batch.set_error_document(None);
    let error_cleared = manifest.apply(configured, batch).await.unwrap();
    assert_eq!(
        error_cleared, index_only,
        "clearing the error document lands on the root that never declared one"
    );

    // Clearing the last document lands back on the content-only root.
    let mut batch = Batch::new();
    batch.set_index_document(None).set_error_document(None);
    let stripped = manifest.apply(configured, batch).await.unwrap();
    assert_eq!(
        stripped, content,
        "clearing both documents restores the content-only root"
    );
    let view = manifest.at(&stripped);
    assert_eq!(view.index_document().await.unwrap(), None, "index cleared");
    assert_eq!(view.error_document().await.unwrap(), None, "error cleared");

    // Clearing what was never declared is a no-op.
    let mut batch = Batch::new();
    batch.set_index_document(None).set_error_document(None);
    let noop = manifest.apply(content, batch).await.unwrap();
    assert_eq!(
        noop, content,
        "clearing an undeclared document changes nothing"
    );

    // Removing a content key does not touch the configuration.
    let child_gone = manifest.remove(&configured, logo.clone()).await.unwrap();
    let view = manifest.at(&child_gone);
    assert_eq!(
        view.index_document().await.unwrap().map(|p| text(&p)),
        Some(String::from(INDEX)),
        "the index document outlives a content key"
    );
    assert_eq!(
        view.error_document().await.unwrap().map(|p| text(&p)),
        Some(String::from(ERROR)),
        "so does the error document"
    );

    observed(
        manifest,
        &[&content, &configured, &index_only, &stripped, &child_gone],
    )
    .await
}

/// Every shape a removal can take, on the one manifest that holds all of them.
async fn every_remove_shape<M>(manifest: &M, empty: &ChunkRef) -> Vec<Observed>
where
    M: Manifest<ChunkRef>,
    M::Metadata: Clone + PartialEq + std::fmt::Debug,
{
    let a = ManifestPath::from("a");
    let ab = ManifestPath::from("ab");
    let ac = ManifestPath::from("ac");
    let img = ManifestPath::from("img/");
    let logo = ManifestPath::from("img/logo.png");
    let index = ManifestPath::from("index.html");

    let mut batch = Batch::new();
    for (path, byte) in [(&a, 1u8), (&ab, 2), (&ac, 3), (&logo, 4), (&index, 5)] {
        batch.insert(path.clone(), reference(byte));
    }
    let base = manifest.apply(*empty, batch).await.unwrap();

    // An absent path: nothing to remove, so nothing changes.
    assert_eq!(
        manifest
            .remove(&base, ManifestPath::from("nowhere"))
            .await
            .unwrap(),
        base,
        "removing an absent path changes nothing"
    );

    // An unbound directory key, with a path below it: still nothing to remove.
    assert!(
        !manifest.at(&base).contains_key(&img).await.unwrap(),
        "the directory key is unbound"
    );
    assert_eq!(
        manifest.remove(&base, img.clone()).await.unwrap(),
        base,
        "removing an unbound directory key changes nothing"
    );

    // An interior path with paths past it: only its own binding goes.
    let interior = manifest.remove(&base, a.clone()).await.unwrap();
    assert_ne!(
        interior, base,
        "clearing a bound interior path moves the root"
    );
    let view = manifest.at(&interior);
    assert_eq!(
        view.get(&a).await.unwrap(),
        None,
        "the interior path is gone"
    );
    assert_eq!(
        view.get(&ab).await.unwrap(),
        Some(MapEntry::Reference(reference(2))),
        "the path past it survives"
    );
    assert_eq!(
        view.get(&ac).await.unwrap(),
        Some(MapEntry::Reference(reference(3))),
        "so does its sibling"
    );

    // A childless leaf: pruned outright, and the paths around it untouched.
    let leaf = manifest.remove(&interior, ab.clone()).await.unwrap();
    let view = manifest.at(&leaf);
    assert_eq!(view.get(&ab).await.unwrap(), None, "the leaf is pruned");
    assert_eq!(
        view.get(&ac).await.unwrap(),
        Some(MapEntry::Reference(reference(3))),
        "its sibling is untouched"
    );

    // Removing every leaf below a directory leaves the listing empty.
    let emptied = manifest.remove(&leaf, logo.clone()).await.unwrap();
    let view = manifest.at(&emptied);
    assert!(
        view.dir(&img).await.unwrap().entries().is_empty(),
        "the emptied directory lists nothing"
    );
    assert_eq!(
        listing(&view, &ManifestPath::default()).await,
        ["ac", "index.html"],
        "the emptied directory is gone from the top level"
    );

    // A directory key that is bound keeps the paths below it.
    let mut batch = Batch::new();
    batch.insert(ManifestPath::from("d/"), reference(6));
    batch.insert(ManifestPath::from("d/x"), reference(7));
    let bound_dir = manifest.apply(emptied, batch).await.unwrap();
    let dir_cleared = manifest
        .remove(&bound_dir, ManifestPath::from("d/"))
        .await
        .unwrap();
    let view = manifest.at(&dir_cleared);
    assert_eq!(
        view.get(&ManifestPath::from("d/")).await.unwrap(),
        None,
        "the directory key's own binding is gone"
    );
    assert_eq!(
        view.get(&ManifestPath::from("d/x")).await.unwrap(),
        Some(MapEntry::Reference(reference(7))),
        "the path below it survives"
    );

    // Removing the last path leaves an empty manifest that still reads.
    let mut batch = Batch::new();
    for path in [&ac, &index, &ManifestPath::from("d/x")] {
        batch.remove(path.clone());
    }
    let stripped = manifest.apply(dir_cleared, batch).await.unwrap();
    let view = manifest.at(&stripped);
    assert!(
        view.iter().await.unwrap().next().await.unwrap().is_none(),
        "the stripped manifest holds no path"
    );
    assert!(
        view.dir(&ManifestPath::default())
            .await
            .unwrap()
            .entries()
            .is_empty(),
        "and lists none"
    );

    observed(
        manifest,
        &[
            &base,
            &interior,
            &leaf,
            &emptied,
            &bound_dir,
            &dir_cleared,
            &stripped,
        ],
    )
    .await
}

/// The key set the removal scenarios build.
///
/// It reaches every shape a removal can leave behind: a mid-edge split
/// (`alpha`/`alpine`), a shared directory (`img/`), a top-level key sharing one
/// byte with a directory (`index.html`), a lone leaf (`beta`), and pairs that
/// run past the trie's 30-byte edge bound.
fn removal_keys() -> Vec<(ManifestPath, u8)> {
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
        // bound.
        (ManifestPath::from("abcde"), 9),
        (under("abcde", 28), 10),
        // 5 + 31 bytes under a directory: the leftover runs through two nodes.
        (ManifestPath::from("wiki/"), 11),
        (under("wiki/", 31), 12),
    ]
    .into_iter()
    .collect()
}

/// `keys` with the key at `index` removed: what an exact-key remove leaves.
fn survivors(keys: &[(ManifestPath, u8)], index: usize) -> Vec<(ManifestPath, u8)> {
    keys.iter()
        .enumerate()
        .filter(|(other, _)| *other != index)
        .map(|(_, key)| key.clone())
        .collect()
}

/// Build a manifest holding exactly `keys`, in the order given.
async fn build<M: Manifest<ChunkRef>>(
    manifest: &M,
    empty: &ChunkRef,
    keys: &[(ManifestPath, u8)],
) -> ChunkRef {
    let mut batch = Batch::new();
    for (path, byte) in keys {
        batch.insert(path.clone(), reference(*byte));
    }
    manifest.apply(*empty, batch).await.unwrap()
}

/// Every path a manifest holds, in walk order.
async fn key_set<M: Manifest<ChunkRef>>(manifest: &M, root: &ChunkRef) -> Vec<String> {
    let view = manifest.at(root);
    let mut out = Vec::new();
    let mut cursor = view.iter().await.unwrap();
    while let Some((path, _)) = cursor.next().await.unwrap() {
        out.push(text(&path));
    }
    out
}

/// An exact-key remove leaves the surviving key set on both formats, and two
/// removals commute on it. The root is not asserted here; see the module note.
async fn a_remove_leaves_the_surviving_keys<M>(manifest: &M, empty: &ChunkRef) -> Vec<Observed>
where
    M: Manifest<ChunkRef>,
    M::Metadata: Clone + PartialEq + std::fmt::Debug,
{
    let keys = removal_keys();
    let base = build(manifest, empty, &keys).await;

    let mut roots = vec![base];
    for (index, (path, _)) in keys.iter().enumerate() {
        let removed = manifest.remove(&base, path.clone()).await.unwrap();
        assert_ne!(removed, base, "removing {:?} moves the root", text(path));

        let built = build(manifest, empty, &survivors(&keys, index)).await;
        assert_eq!(
            key_set(manifest, &removed).await,
            key_set(manifest, &built).await,
            "removing {:?} left a key set a build of the surviving keys does not hold",
            text(path)
        );
        roots.push(removed);
    }

    // The order the two keys go in does not reach the key set.
    let (first, second) = (keys[0].0.clone(), keys[4].0.clone());
    let forwards = manifest
        .remove(
            &manifest.remove(&base, first.clone()).await.unwrap(),
            second.clone(),
        )
        .await
        .unwrap();
    let backwards = manifest
        .remove(&manifest.remove(&base, second).await.unwrap(), first)
        .await
        .unwrap();
    assert_eq!(
        key_set(manifest, &forwards).await,
        key_set(manifest, &backwards).await,
        "two removals commute on the key set"
    );
    roots.push(forwards);
    roots.push(backwards);

    // An empty manifest has one shape on either format, so the root is the
    // assertion here.
    let mut batch = Batch::new();
    for (path, _) in &keys {
        batch.remove(path.clone());
    }
    let stripped = manifest.apply(base, batch).await.unwrap();
    assert_eq!(
        &stripped, empty,
        "removing every key restores the empty manifest"
    );
    roots.push(stripped);

    observed(manifest, &roots.iter().collect::<Vec<_>>()).await
}

/// Every root observed in turn.
async fn observed<M>(manifest: &M, roots: &[&ChunkRef]) -> Vec<Observed>
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

/// Both formats through one scenario, with the answers compared.
macro_rules! differential {
    ($name:ident, $scenario:ident) => {
        #[test]
        fn $name() {
            run(async {
                let raw: Raw = Arc::new(MemoryStore::new());
                let store: Store = ContentGet::new(Arc::clone(&raw));
                let ((trie, trie_empty), (kv, kv_empty)) = both_formats(&raw, &store).await;
                let from_trie = $scenario(&trie, &trie_empty).await;
                let from_kv = $scenario(&kv, &kv_empty).await;

                assert_eq!(
                    from_trie, from_kv,
                    "the trie and the key-value database answered differently"
                );
            });
        }
    };
}

differential!(
    the_site_config_agrees_across_formats,
    the_site_config_is_an_option_and_not_a_key
);
differential!(every_remove_shape_agrees_across_formats, every_remove_shape);
differential!(
    a_remove_leaves_the_surviving_keys_on_both_formats,
    a_remove_leaves_the_surviving_keys
);

/// The key-value database's removal is history-independent: the root a removal
/// lands on is the root a build of the surviving keys produces.
///
/// One format only, on purpose; see the module note.
#[test]
fn a_remove_is_history_independent_on_ldb() {
    run(async {
        let raw: Raw = Arc::new(MemoryStore::new());
        let store: Store = ContentGet::new(raw);
        let builder: Builder<V1> = Builder::new();
        let empty = *builder.build(&store, &Plaintext).await.unwrap().root();
        let kv = Database::<_>::plain(store.clone());

        let keys = removal_keys();
        let base = build(&kv, &empty, &keys).await;

        for (index, (path, _)) in keys.iter().enumerate() {
            let removed = Manifest::remove(&kv, &base, path.clone()).await.unwrap();
            assert_ne!(removed, base, "removing {:?} moves the root", text(path));

            assert_eq!(
                removed,
                build(&kv, &empty, &survivors(&keys, index)).await,
                "removing {:?} left a root a build of the surviving keys would not produce",
                text(path)
            );
        }

        // The order the two keys go in does not reach the root either.
        let (first, second) = (keys[0].0.clone(), keys[4].0.clone());
        let forwards = Manifest::remove(
            &kv,
            &Manifest::remove(&kv, &base, first.clone()).await.unwrap(),
            second.clone(),
        )
        .await
        .unwrap();
        let backwards = Manifest::remove(
            &kv,
            &Manifest::remove(&kv, &base, second).await.unwrap(),
            first,
        )
        .await
        .unwrap();
        assert_eq!(forwards, backwards, "two removals commute on the root");
    });
}

/// One batch folds in submission order, and an empty batch is the identity:
/// the last verb staged at one key is the one that lands.
async fn a_batch_folds_in_submission_order<M>(manifest: &M, empty: &ChunkRef)
where
    M: Manifest<ChunkRef>,
    M::Metadata: Clone + PartialEq + std::fmt::Debug,
{
    let page = ManifestPath::from("a.txt");
    let one = manifest
        .insert(empty, page.clone(), reference(1))
        .await
        .unwrap();
    let ins = |byte: u8| ManifestOp::Insert {
        path: page.clone(),
        reference: reference(byte),
        meta: M::Metadata::default(),
    };
    let rm = || ManifestOp::Remove { path: page.clone() };

    for (ops, want, hint) in [
        (vec![], empty, "the empty batch moved the root"),
        (vec![ins(1), rm()], empty, "the staged remove lost"),
        (vec![rm(), ins(1)], &one, "absent remove not a no-op"),
        (vec![ins(2), ins(1)], &one, "the first insert won"),
    ] {
        let mut batch = Batch::new();
        batch.extend(ops);
        let got = manifest.apply(*empty, batch).await.unwrap();
        assert_eq!(&got, want, "{hint}");
    }
}

#[test]
fn a_batch_folds_in_submission_order_on_both_formats() {
    run(async {
        let raw: Raw = Arc::new(MemoryStore::new());
        let store: Store = ContentGet::new(Arc::clone(&raw));
        let ((trie, trie_empty), (kv, kv_empty)) = both_formats(&raw, &store).await;
        a_batch_folds_in_submission_order(&trie, &trie_empty).await;
        a_batch_folds_in_submission_order(&kv, &kv_empty).await;
    });
}

/// The reserved refusal is settled in the seam, before the format runs: the
/// base root here resolves to nothing in the store, so any format work would
/// fail as `Format`, yet `apply` answers `Reserved` without reading it.
#[test]
fn the_reserved_refusal_precedes_the_format() {
    run(async {
        let raw: Raw = Arc::new(MemoryStore::new());
        let store: Store = ContentGet::new(Arc::clone(&raw));
        let ((trie, _), (kv, _)) = both_formats(&raw, &store).await;
        // No chunk lives behind this root.
        let garbage = reference(0xEE);
        let sep = ManifestPath::from("/");

        let mut batch = Batch::new();
        batch.insert(sep.clone(), reference(1));
        let got = refused(&trie.apply(garbage, batch).await);
        assert_eq!(got, Some(sep.clone()), "the trie ran before the refusal");

        // The refusal names the first reserved path staged.
        let mut batch = Batch::new();
        batch.remove(ManifestPath::default()).remove(sep);
        let got = refused(&kv.apply(garbage, batch).await);
        assert_eq!(got, Some(ManifestPath::default()), "not the first staged");
    });
}

/// The pure staging pins on `Batch`: submission order across every verb,
/// untouched documents as `None`, the delta shapes, `is_empty` until staged
/// into, and the reserved refusal naming the first reserved path staged.
#[test]
fn a_batch_stages_ops_and_documents_and_records_the_first_refusal() {
    let mut batch: Batch = Batch::new();
    assert!(batch.is_empty());
    batch
        .insert(ManifestPath::from("b"), reference(1))
        .remove(ManifestPath::from("a"))
        .extend([ManifestOp::Insert {
            path: ManifestPath::from("c"),
            reference: reference(2),
            meta: (),
        }])
        .insert_with(ManifestPath::from("b"), reference(3), ());
    assert!(!batch.is_empty());
    let checked = batch.into_checked().unwrap();
    let paths: Vec<&[u8]> = checked.ops.iter().map(|op| op.path().as_bytes()).collect();
    assert_eq!(paths, [&b"b"[..], b"a", b"c", b"b"]);
    assert!(checked.ops[1].is_remove());
    assert_eq!(checked.index_document, None);
    assert_eq!(checked.error_document, None);

    // The site documents are a delta, not ops, and a doc-only batch stages
    // no op: set is `Some(Some)`, clear is `Some(None)`.
    let mut batch: Batch = Batch::new();
    batch.set_index_document(ManifestPath::from("index.html"));
    batch.set_error_document(None);
    let checked = batch.into_checked().unwrap();
    assert!(checked.ops.is_empty());
    let set = Some(Some(ManifestPath::from("index.html")));
    assert_eq!(checked.index_document, set);
    assert_eq!(checked.error_document, Some(None));

    // A reserved path stages no op and refuses the whole batch with the
    // first one, through any verb; the refusal counts against `is_empty`.
    let mut batch: Batch = Batch::new();
    batch.insert(ManifestPath::from("landed.html"), reference(1));
    batch.remove(ManifestPath::from("/"));
    batch.insert(ManifestPath::default(), reference(2));
    assert!(!batch.is_empty());
    let refusal = batch.into_checked().unwrap_err();
    assert_eq!(refusal.path(), &ManifestPath::from("/"));

    let mut batch: Batch = Batch::new();
    batch.extend([ManifestOp::Remove {
        path: ManifestPath::default(),
    }]);
    assert!(!batch.is_empty());
    let refusal = batch.into_checked().unwrap_err();
    assert_eq!(refusal.path(), &ManifestPath::default());
}

/// Content keys that start with the separator list as the directory they are
/// under, identically on both formats.
///
/// `"/"` is reserved as a key, not as a prefix: `"/a.txt"` is ordinary content
/// one level down, listed under the directory `"/"`.
/// [`a_planted_separator_key_is_not_listed`] pins the other side.
#[test]
fn separator_prefixed_content_lists_alike_on_both_formats() {
    run(async {
        // Each case is a key set written through the seam and the top level it
        // has to list.
        let cases: [(&[&str], &[&str]); 3] = [
            (&["/a.txt"], &["/"]),
            (&["/a.txt", "/b.txt", "top.txt"], &["/", "top.txt"]),
            (&["//a"], &["/"]),
        ];
        for (keys, want) in cases {
            let (from_trie, from_kv) = both_top_levels(keys).await;
            assert_eq!(from_trie, want, "the trie listed {keys:?} wrong");
            assert_eq!(from_kv, want, "the database listed {keys:?} wrong");
        }
    });
}

/// One key set through both formats' `dir("")`, written through the seam.
async fn both_top_levels(keys: &[&str]) -> (Vec<String>, Vec<String>) {
    let raw: Raw = Arc::new(MemoryStore::new());
    let store: Store = ContentGet::new(Arc::clone(&raw));
    let ((trie, trie_empty), (kv, kv_empty)) = both_formats(&raw, &store).await;
    let trie_root = write_keys(&trie, &trie_empty, keys).await;
    let kv_root = write_keys(&kv, &kv_empty, keys).await;

    let top = ManifestPath::default();
    (
        listing(&trie.at(&trie_root), &top).await,
        listing(&kv.at(&kv_root), &top).await,
    )
}

/// An insert replaces the whole binding: a bare re-insert takes a new reference
/// and clears the metadata the path carried.
async fn insert_replaces_the_whole_binding<M>(manifest: &M, base: &ChunkRef)
where
    M: Manifest<ChunkRef>,
    M::Metadata: Clone + PartialEq + std::fmt::Debug,
{
    let page = ManifestPath::from("page.html");
    let meta =
        M::Metadata::from_source(&MetadataView::new().with(WellKnownKey::ContentType, "text/html"));
    assert_ne!(
        meta,
        M::Metadata::default(),
        "the format carries a content type"
    );

    let carried = {
        let mut batch = Batch::new();
        batch.insert_with(page.clone(), reference(1), meta.clone());
        manifest.apply(*base, batch).await.unwrap()
    };
    assert_eq!(manifest.at(&carried).metadata(&page).await.unwrap(), meta);

    let bare = {
        let mut batch = Batch::new();
        batch.insert(page.clone(), reference(2));
        manifest.apply(carried, batch).await.unwrap()
    };
    let view = manifest.at(&bare);
    assert_eq!(
        view.get(&page).await.unwrap(),
        Some(MapEntry::Reference(reference(2))),
        "the reference is replaced"
    );
    assert_eq!(
        view.metadata(&page).await.unwrap(),
        M::Metadata::default(),
        "a bare insert clears the metadata the path carried"
    );

    let one_shot = manifest
        .insert(&carried, page.clone(), reference(2))
        .await
        .unwrap();
    assert_eq!(one_shot, bare, "the one-shot is a one-op batch");
}

#[test]
fn an_insert_replaces_the_whole_binding_on_both_formats() {
    run(async {
        let raw: Raw = Arc::new(MemoryStore::new());
        let store: Store = ContentGet::new(Arc::clone(&raw));
        let ((trie, trie_empty), (kv, kv_empty)) = both_formats(&raw, &store).await;
        insert_replaces_the_whole_binding(&trie, &trie_empty).await;
        insert_replaces_the_whole_binding(&kv, &kv_empty).await;
    });
}

/// `empty` equals `native`, holds no path, and reads an absent path as
/// `ManifestError::NotFound`, structurally.
async fn assert_empty_bootstrap<M: Manifest<ChunkRef>>(manifest: &M, native: ChunkRef, f: &str) {
    let empty: ChunkRef = manifest.empty().await.unwrap();
    assert_eq!(empty, native, "{f}: bootstrap off the native empty root");
    let view = manifest.at(&empty);
    let first = view.iter().await.unwrap().next().await.unwrap();
    assert!(first.is_none(), "{f}: the empty manifest holds no path");
    let missing = view
        .load(&ManifestPath::from("missing.html"), &mut MemSink::new())
        .await
        .err();
    assert!(missing.is_some_and(|e| e.is_not_found()), "{f}: NotFound");
}

/// The seam bootstrap is the format's own: `empty` returns the root the
/// native builder produces.
#[test]
fn the_seam_bootstrap_matches_each_format() {
    run(async {
        let raw: Raw = Arc::new(MemoryStore::new());
        let store: Store = ContentGet::new(Arc::clone(&raw));

        let nodes = NodeLoadSaver::new(Arc::clone(&raw));
        let editor: ManifestEditor<_> = ManifestEditor::new(nodes.clone());
        let (native, _) = editor.commit().await.unwrap();
        let trie = MantarayManifest::<_, _, DEFAULT_BODY_SIZE>::new(nodes, store.clone());
        assert_empty_bootstrap(&trie, ChunkRef::new(native), "trie").await;

        let builder: Builder<V1> = Builder::new();
        let native = *builder.build(&store, &Plaintext).await.unwrap().root();
        assert_empty_bootstrap(&Database::<_>::plain(store.clone()), native, "database").await;
    });
}

/// Insert `file`, load it through a refusing sink, and check the seam
/// classified the refusal as `Sink` with the refusal kept as the source.
async fn assert_sink_refusal<M: Manifest<ChunkRef>>(manifest: &M, file: ChunkRef, f: &str) {
    let path = ManifestPath::from(INDEX);
    let empty = manifest.empty().await.unwrap();
    let root = manifest.insert(&empty, path.clone(), file).await.unwrap();
    let error = manifest
        .at(&root)
        .load(&path, &mut RefusingSink)
        .await
        .unwrap_err();
    let sink = matches!(error, ManifestError::Sink(_));
    assert!(sink, "{f}: wrong variant: {error:?}");
    let source = std::error::Error::source(&error);
    let kept = source.is_some_and(|s| s.downcast_ref::<Refused>().is_some());
    assert!(kept, "{f}: the sink's own error left the chain");
}

/// A sink refusal is the seam's own `Sink`, never `Data`, on both formats.
#[test]
fn a_refused_sink_write_is_sink_on_both_formats() {
    run(async {
        let raw: Raw = Arc::new(MemoryStore::new());
        let store: Store = ContentGet::new(Arc::clone(&raw));
        let data = vec![7u8; 20_000];
        let saver = File::<_, DEFAULT_BODY_SIZE>::new(&raw, Policy::DEFAULT);
        let file = ChunkRef::new(saver.save(&data[..]).await.unwrap());

        let ((trie, _), (kv, _)) = both_formats(&raw, &store).await;
        assert_sink_refusal(&trie, file, "trie").await;
        assert_sink_refusal(&kv, file, "database").await;
    });
}

/// A sink that refuses every write.
#[derive(Debug)]
struct RefusingSink;

/// The refusal [`RefusingSink`] reports; the seam has to keep it reachable.
#[derive(Debug, thiserror::Error)]
#[error("the sink refused the write")]
struct Refused;

impl DataSink for RefusingSink {
    type Error = Refused;

    fn write_at(&mut self, _offset: u64, _data: &[u8]) -> Result<(), Self::Error> {
        Err(Refused)
    }
}

/// A reserved key planted past the seam is not listed, whatever kind the
/// listing calls it.
///
/// The seam refuses a write at `"/"`, so only a database written through the
/// raw layer holds one. The folder view collapses it into a subdirectory entry,
/// and a subdirectory with nothing under it is that key alone. A subdirectory
/// with content under it is not, which
/// [`separator_prefixed_content_lists_alike_on_both_formats`] pins.
#[test]
fn a_planted_separator_key_is_not_listed() {
    run(async {
        let raw: Raw = Arc::new(MemoryStore::new());
        let store: Store = ContentGet::new(Arc::clone(&raw));
        let separator = ManifestPath::from("/");
        let content = ManifestPath::from("a.txt");

        // The raw layer knows no reserved key, so it plants what the seam
        // refuses.
        let builder: Builder<V1> = Builder::new();
        let empty = *builder.build(&store, &Plaintext).await.unwrap().root();
        let db: Database<_> = Database::plain(&store);
        let planted = {
            let mut editor = db.edit(&empty);
            editor.insert(Key::from(&b"/"[..]), Entry::from(reference(1)));
            editor.insert(Key::from(&b"a.txt"[..]), Entry::from(reference(2)));
            editor.commit().await.unwrap()
        };
        assert!(
            db.at(&planted)
                .get(&Key::from(&b"/"[..]))
                .await
                .unwrap()
                .is_some(),
            "the raw layer holds the planted key"
        );

        let kv = Database::<_>::plain(store.clone());
        let view = kv.at(&planted);
        assert_eq!(
            listing(&view, &ManifestPath::default()).await,
            ["a.txt"],
            "the top level lists content alone, and never the planted key"
        );
        assert_eq!(
            MapView::get(&view, &separator).await.unwrap(),
            None,
            "and no read reaches it either"
        );

        // The trie holds the same content and lists the same level.
        let nodes = NodeLoadSaver::new(Arc::clone(&raw));
        let editor: ManifestEditor<_> = ManifestEditor::new(nodes.clone());
        let (trie_empty, _) = editor.commit().await.unwrap();
        let trie = MantarayManifest::<_, _, DEFAULT_BODY_SIZE>::new(nodes, store.clone());
        let root = {
            let mut batch = Batch::new();
            batch.insert(content.clone(), reference(2));
            trie.apply(ChunkRef::new(trie_empty), batch).await.unwrap()
        };
        assert_eq!(
            listing(&trie.at(&root), &ManifestPath::default()).await,
            listing(&view, &ManifestPath::default()).await,
            "both formats list the same top level"
        );

        // Next to content one level under it, the listed entry is the
        // directory of that content, and the planted key still no key.
        let with_content = {
            let mut editor = db.edit(&empty);
            editor.insert(Key::from(&b"/"[..]), Entry::from(reference(1)));
            editor.insert(Key::from(&b"/a.txt"[..]), Entry::from(reference(2)));
            editor.commit().await.unwrap()
        };
        let view = kv.at(&with_content);
        assert_eq!(
            listing(&view, &ManifestPath::default()).await,
            ["/"],
            "the directory of the content below the separator is listed"
        );
        assert_eq!(
            MapView::get(&view, &separator).await.unwrap(),
            None,
            "and the planted key itself still reads absent"
        );
    });
}

/// The site documents resolve over bare keys: the index document is a filename
/// joined below each directory, and the error document is one whole key.
///
/// Written through the seam's setters, read through the database's own website
/// reader.
#[test]
fn website_documents_resolve_over_bare_keys() {
    run(async {
        let raw: Raw = Arc::new(MemoryStore::new());
        let store: Store = ContentGet::new(raw);
        let builder: Builder<V1> = Builder::new();
        let empty = *builder.build(&store, &Plaintext).await.unwrap().root();
        let kv = Database::<_>::plain(store.clone());

        let root = {
            let mut batch = Batch::new();
            batch.insert(ManifestPath::from("index.html"), reference(1));
            batch.insert(ManifestPath::from("docs/index.html"), reference(2));
            batch.insert(ManifestPath::from("docs/guide.html"), reference(3));
            batch.insert(ManifestPath::from("404.html"), reference(4));
            batch
                .set_index_document(ManifestPath::from(INDEX))
                .set_error_document(ManifestPath::from(ERROR));
            kv.apply(empty, batch).await.unwrap()
        };

        let reader: LdbReader<_> = LdbReader::new(&store);
        let site = reader.website(&root).await.unwrap();
        assert_eq!(
            site.index(),
            Some(INDEX.as_bytes()),
            "the index document is the relative filename it was set to"
        );
        assert_eq!(
            site.error(),
            Some(ERROR.as_bytes()),
            "the error document is the bare key it was set to"
        );

        /// The key a request path resolved to, and how.
        fn resolved(served: &Served<V1>) -> (&'static str, String) {
            let key = served
                .key()
                .map(|key| String::from_utf8(key.as_bytes().to_vec()).unwrap())
                .unwrap_or_default();
            let how = match served {
                Served::Exact { .. } => "exact",
                Served::Index { .. } => "index",
                Served::Error { .. } => "error",
                Served::Missing => "missing",
            };
            (how, key)
        }

        for (request, want) in [
            // An exact key wins.
            ("index.html", ("exact", "index.html")),
            // The top level is the empty path, which resolves its own index.
            ("", ("index", "index.html")),
            // The index document joins below each directory, per directory.
            ("docs/", ("index", "docs/index.html")),
            ("docs", ("index", "docs/index.html")),
            // Nothing resolves, so the error document does, as one whole key.
            ("missing.html", ("error", "404.html")),
            ("docs/missing.html", ("error", "404.html")),
        ] {
            let served = reader
                .serve(&root, &Key::from(request.as_bytes()))
                .await
                .unwrap();
            let (how, key) = resolved(&served);
            assert_eq!(
                (how, key.as_str()),
                want,
                "serving {request:?} resolved the wrong way"
            );
        }

        // With the documents cleared, nothing falls back at all.
        let cleared = {
            let mut batch = Batch::new();
            batch.set_index_document(None).set_error_document(None);
            kv.apply(root, batch).await.unwrap()
        };
        assert_eq!(
            reader.website(&cleared).await.unwrap(),
            Website::default(),
            "clearing the documents leaves no site conventions"
        );
        let served = reader
            .serve(&cleared, &Key::from(&b"missing.html"[..]))
            .await
            .unwrap();
        assert_eq!(
            resolved(&served),
            ("missing", String::new()),
            "no documents are declared, so nothing resolves"
        );
    });
}
