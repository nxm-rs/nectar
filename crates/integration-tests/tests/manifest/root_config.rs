//! The convergence gate: content keys are bare, the manifest's own
//! configuration is an option and not a key, and `remove` is exact-key.
//! Identically on both manifest formats.
//!
//! Every scenario below is generic over the [`Manifest`] seam and is run twice,
//! once per format. Two things therefore have to hold for it to pass: each
//! format has to answer what the contract says, which the assertions inside the
//! scenario check, and the two formats have to answer the same thing, which the
//! [`Observed`] comparison in each test checks. Either failing names the exact
//! verb and path that diverged.
//!
//! # The contract under test
//!
//! A content key is the path bytes verbatim, with nothing prepended. That is
//! what keeps the trie image byte-identical to the reference client's, and
//! `mantaray/legacy_differential.rs` pins the bytes themselves.
//!
//! The site index and error documents are read as `Option<ManifestPath>` and
//! written through `with_index_document` and `with_error_document`. Each lands in
//! the format's own root slot, which is never a key: no map verb reads it, no
//! walk yields it, and neither reserved path reaches anything at all. The two
//! reserved paths are the empty one and `"/"`: a read at either is absent on
//! both formats and a write at either is refused as `ReservedKey`, so the two
//! formats keep the same key space rather than one of them storing a slot key
//! as content.
//!
//! `remove` is exact-key on both formats, exactly as `HashMap::remove` is: the
//! path's own value and metadata go, and no other path does. A path with
//! children keeps every one of them, a childless leaf is pruned, and removing an
//! unbound or absent path is a no-op that leaves the root where it was.
//!
//! # History-independence is a key-value database guarantee only
//!
//! What crosses the formats after a removal is the key set: both hold exactly
//! the survivors, and two removals commute on that set. The root does not cross
//! them, and this file does not ask it to.
//!
//! The key-value database is history-independent by construction: its packing
//! derives the whole shape from the key set, so the root a removal lands on is
//! the root a build of the survivors produces. `a_remove_is_history_independent_on_ldb`
//! pins that, on that format alone.
//!
//! The trie is not, by design. mantaray 0.2 puts a node where the insert order
//! first justified one and never moves it, so past the 30-byte edge bound even a
//! plain `add` of one key set in two orders lands on two roots. The pinned
//! legacy oracle confirms that, and `mantaray/legacy_differential.rs` pins those
//! bytes. History-independence is a mantaray-1.0 guarantee (whitepaper
//! capability matrix, row 20), which the key-value database is the
//! implementation of. Asking 0.2 for it would mean rewriting the surviving
//! edges, which would break the wire the differential pins.

use std::ops::Bound;
use std::sync::Arc;

use nectar_file::MemSink;
use nectar_ldb::{
    Builder, Database, Entry, Key, LdbManifest, Plaintext, Reader as LdbReader, Served, V1,
};
use nectar_loadsave::NodeLoadSaver;
use nectar_manifest::{
    Manifest, ManifestPath, MapCursor, MapEntry, MapView, MapWriter, MetadataView, WellKnownKey,
    reserved_key,
};
use nectar_mantaray::{ManifestEditor, MantarayManifest};
use nectar_primitives::store::{ContentGet, MemoryStore};
use nectar_primitives::{ChunkAddress, ChunkRef, DEFAULT_BODY_SIZE, StandardChunkSet};
use nectar_testing::run;

/// The chunk store, shared: `MemoryStore` clones its contents, so every handle
/// in one test has to reach the same map.
type Raw = Arc<MemoryStore<StandardChunkSet>>;
type Store = ContentGet<Raw>;

/// The site index document: a filename joined below each directory, so it is
/// relative on purpose.
const INDEX: &str = "index.html";
/// The site error document: one whole content key, so it is bare like any path.
const ERROR: &str = "404.html";

/// A reference standing in for a file root; no chunk behind it is read.
fn reference(byte: u8) -> ChunkRef {
    ChunkRef::new(ChunkAddress::new([byte; 32]))
}

/// A path as text, for an assertion message a human can read.
fn text(path: &ManifestPath) -> String {
    String::from_utf8(path.as_bytes().to_vec()).unwrap()
}

/// The reserved path a refused write names, or `None` when the write was not
/// refused for that reason.
///
/// One matcher over every format: the seam's own, walking the source chain of
/// whatever error type the format reports.
fn refused<T, E: std::error::Error + 'static>(result: &Result<T, E>) -> Option<ManifestPath> {
    let error = result.as_ref().err()?;
    reserved_key(error).map(|reserved| reserved.path().clone())
}

/// One format's answers to one scenario, in a shape that names no
/// format-specific type, so the two formats compare directly.
#[derive(Debug, PartialEq, Eq)]
struct Observed {
    /// Every path `iter` yields, in order.
    keys: Vec<String>,
    /// Every path `range(..)` yields, in order.
    ranged: Vec<String>,
    /// Every path `dir("")` lists, in order: the top level.
    listed: Vec<String>,
    /// Every path `dir("a")` lists: a prefix with no trailing separator, so it
    /// matches the paths starting with it rather than a directory.
    prefixed: Vec<String>,
    /// The index document the manifest declares.
    index_document: Option<String>,
    /// The error document the manifest declares.
    error_document: Option<String>,
    /// One row per probed path.
    probes: Vec<Probe>,
}

/// Every point read at one path, with the metadata answer reduced to the one
/// fact that crosses formats.
#[derive(Debug, PartialEq, Eq)]
struct Probe {
    path: String,
    entry: Option<MapEntry<ChunkRef>>,
    present: bool,
    /// Whether the path carries the format's empty metadata.
    bare: bool,
    /// The greatest bound path at or below this one.
    floor: Option<String>,
    /// Whether a load of the path reached the sink; the references here name no
    /// stored chunk, so this is the verb reaching storage, not a naming answer.
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

    let mut keys = Vec::new();
    let mut cursor = view.iter().await.unwrap();
    while let Some((path, _)) = cursor.next().await.unwrap() {
        keys.push(text(&path));
    }

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

/// The paths every scenario probes: the empty path and the separator alone,
/// which name no content key on either format, a top-level file, a directory key
/// and a file below it, an interior key with siblings past it, and one absent
/// path.
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

/// The site documents through their option-typed API: set, read back, cleared,
/// and never a key in the map.
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
    let content = {
        let mut writer = manifest.edit(empty);
        writer.insert(index.clone(), reference(1));
        writer.insert(logo.clone(), reference(2));
        writer.commit().await.unwrap()
    };
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

    // Both documents, set through the chainable option-typed setters.
    let configured = {
        let mut writer = manifest.edit(&content);
        writer
            .with_index_document(ManifestPath::from(INDEX))
            .with_error_document(ManifestPath::from(ERROR));
        writer.commit().await.unwrap()
    };
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

    // The slot is not a key: no map verb reaches it, at the empty path or at the
    // separator the trie keys its own slot with.
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

    // No walk yields it either, and the listing is content alone.
    let mut walked = Vec::new();
    let mut cursor = view.iter().await.unwrap();
    while let Some((path, _)) = cursor.next().await.unwrap() {
        walked.push(text(&path));
    }
    assert_eq!(
        walked,
        ["img/logo.png", "index.html"],
        "iter yields content keys, bare and in order"
    );
    assert_eq!(
        listing(&view, &empty_path).await,
        ["img/", "index.html"],
        "the top level lists content alone"
    );

    // A write at a reserved path is refused, and named: it is a prefix or a
    // root slot, never a key, so neither format takes it silently.
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

    // The refusal takes the whole batch with it: a reserved path anywhere in it
    // means nothing lands, so no caller sees a half-applied root.
    let mixed = {
        let mut writer = manifest.edit(&configured);
        writer.insert(ManifestPath::from("landed.html"), reference(9));
        writer.insert(separator.clone(), reference(9));
        writer.commit().await
    };
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

    // The two documents are independent: setting one leaves the other alone.
    let index_only = {
        let mut writer = manifest.edit(&content);
        writer.with_index_document(ManifestPath::from(INDEX));
        writer.commit().await.unwrap()
    };
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

    // Clearing is the same setter with `None`, one document at a time.
    let error_cleared = {
        let mut writer = manifest.edit(&configured);
        writer.with_error_document(None);
        writer.commit().await.unwrap()
    };
    assert_eq!(
        error_cleared, index_only,
        "clearing the error document lands on the root that never declared one"
    );

    // Clearing the last document lands back on the content-only root, so the
    // configuration leaves no trace on the wire.
    let stripped = {
        let mut writer = manifest.edit(&configured);
        writer.with_index_document(None).with_error_document(None);
        writer.commit().await.unwrap()
    };
    assert_eq!(
        stripped, content,
        "clearing both documents restores the content-only root"
    );
    let view = manifest.at(&stripped);
    assert_eq!(view.index_document().await.unwrap(), None, "index cleared");
    assert_eq!(view.error_document().await.unwrap(), None, "error cleared");

    // Clearing what was never declared is a no-op.
    let noop = {
        let mut writer = manifest.edit(&content);
        writer.with_index_document(None).with_error_document(None);
        writer.commit().await.unwrap()
    };
    assert_eq!(
        noop, content,
        "clearing an undeclared document changes nothing"
    );

    // Removing a content key does not touch the configuration: the documents are
    // the manifest's own, not a property of the tree below it.
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

    let base = {
        let mut writer = manifest.edit(empty);
        for (path, byte) in [(&a, 1u8), (&ab, 2), (&ac, 3), (&logo, 4), (&index, 5)] {
            writer.insert(path.clone(), reference(byte));
        }
        writer.commit().await.unwrap()
    };

    // An absent path: nothing to remove, so nothing changes.
    assert_eq!(
        manifest
            .remove(&base, ManifestPath::from("nowhere"))
            .await
            .unwrap(),
        base,
        "removing an absent path changes nothing"
    );

    // An unbound directory key, with a path below it: still nothing bound at the
    // key itself, so still nothing to remove.
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

    // Removing every leaf below a directory leaves the directory listing empty
    // rather than leaving a stale entry behind.
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

    // A directory key that is bound keeps the paths below it, the same way any
    // other path with children does.
    let bound_dir = {
        let mut writer = manifest.edit(&emptied);
        writer.insert(ManifestPath::from("d/"), reference(6));
        writer.insert(ManifestPath::from("d/x"), reference(7));
        writer.commit().await.unwrap()
    };
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
    let stripped = {
        let mut writer = manifest.edit(&dir_cleared);
        for path in [&ac, &index, &ManifestPath::from("d/x")] {
            writer.remove(path.clone());
        }
        writer.commit().await.unwrap()
    };
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

/// The key set the removal scenarios build, with the reference each key binds.
///
/// Chosen to reach every shape a removal can leave behind: a mid-edge split
/// (`alpha`/`alpine`), a shared directory (`img/`), a top-level key sharing one
/// byte with a directory (`index.html`), a lone leaf (`beta`), and pairs that
/// run past the trie's 30-byte edge bound, where a removal leaves a chain no
/// build would have written.
///
/// The last two pairs are the edge bound itself: a short key with one long key
/// below it, so the trie splits the long key at the short one and what a
/// removal of the short one leaves behind runs past 30 bytes. Those are exactly
/// the shapes where the two formats' roots part company, and where the key set
/// still has to agree.
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
        // 5 + 28 bytes: what a removal of "abcde" leaves joins to 33, which is
        // past the bound and so is no edge a build writes.
        (ManifestPath::from("abcde"), 9),
        (under("abcde", 28), 10),
        // 5 + 31 bytes under a directory: the long key already chains through a
        // full-length edge, so the leftover runs through two nodes before it
        // ends.
        (ManifestPath::from("wiki/"), 11),
        (under("wiki/", 31), 12),
    ]
    .into_iter()
    .collect()
}

/// Build a manifest holding exactly `keys`, in the order given.
async fn build<M: Manifest<ChunkRef>>(
    manifest: &M,
    empty: &ChunkRef,
    keys: &[(ManifestPath, u8)],
) -> ChunkRef {
    let mut writer = manifest.edit(empty);
    for (path, byte) in keys {
        writer.insert(path.clone(), reference(*byte));
    }
    writer.commit().await.unwrap()
}

/// Every path a manifest holds, in walk order.
///
/// The key set is what a removal contracts for on both formats, and what the
/// two formats have to agree on. The root is a per-format matter; see the
/// module note.
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
/// removals commute on it.
///
/// What a manifest holds after a removal is decided by what it held and what
/// went, on either format and in whatever order. The root a removal lands on is
/// not asserted here: only the key-value database derives it from the key set,
/// and `a_remove_is_history_independent_on_ldb` pins that on that format alone.
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

        let survivors: Vec<(ManifestPath, u8)> = keys
            .iter()
            .enumerate()
            .filter(|(other, _)| *other != index)
            .map(|(_, key)| key.clone())
            .collect();
        let built = build(manifest, empty, &survivors).await;
        assert_eq!(
            key_set(manifest, &removed).await,
            key_set(manifest, &built).await,
            "removing {:?} left a key set a build of the surviving keys does not hold",
            text(path)
        );
        roots.push(removed);
    }

    // Two removals compose the same way, so the property is not a one-step
    // accident: the order the two keys go in does not reach the key set.
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

    // Removing every key lands back on the empty manifest, which is the build
    // of no keys at all. An empty manifest has one shape on either format, so
    // the root is the assertion here.
    let stripped = {
        let mut writer = manifest.edit(&base);
        for (path, _) in &keys {
            writer.remove(path.clone());
        }
        writer.commit().await.unwrap()
    };
    assert_eq!(
        &stripped, empty,
        "removing every key restores the empty manifest"
    );
    roots.push(stripped);

    observed(manifest, &roots.iter().collect::<Vec<_>>()).await
}

/// Every root observed in turn, so one comparison covers every state the
/// scenario passed through.
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

                let nodes = NodeLoadSaver::new(Arc::clone(&raw));
                let editor: ManifestEditor<_> = ManifestEditor::new(nodes.clone());
                let (empty, _) = editor.commit().await.unwrap();
                let trie = MantarayManifest::<_, _, DEFAULT_BODY_SIZE>::new(nodes, store.clone());
                let from_trie = $scenario(&trie, &ChunkRef::new(empty)).await;

                let builder: Builder<V1> = Builder::new();
                let empty = *builder.build(&store, &Plaintext).await.unwrap().root();
                let kv = LdbManifest::plain(store.clone());
                let from_kv = $scenario(&kv, &empty).await;

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
/// lands on is the root a build of the surviving keys produces, so a manifest's
/// address is what it holds rather than how it came to hold it.
///
/// One format only, on purpose. The packing derives the whole shape from the
/// key set, so the database gets this for free; the trie cannot have it, because
/// mantaray 0.2 leaves a node where the insert order put it and rewriting the
/// surviving edges would break the wire `mantaray/legacy_differential.rs` pins.
/// It is a mantaray-1.0 guarantee (whitepaper capability matrix, row 20), and
/// the database is what meets it. See the module note.
#[test]
fn a_remove_is_history_independent_on_ldb() {
    run(async {
        let raw: Raw = Arc::new(MemoryStore::new());
        let store: Store = ContentGet::new(raw);
        let builder: Builder<V1> = Builder::new();
        let empty = *builder.build(&store, &Plaintext).await.unwrap().root();
        let kv = LdbManifest::plain(store.clone());

        let keys = removal_keys();
        let base = build(&kv, &empty, &keys).await;

        for (index, (path, _)) in keys.iter().enumerate() {
            let removed = kv.remove(&base, path.clone()).await.unwrap();
            assert_ne!(removed, base, "removing {:?} moves the root", text(path));

            let survivors: Vec<(ManifestPath, u8)> = keys
                .iter()
                .enumerate()
                .filter(|(other, _)| *other != index)
                .map(|(_, key)| key.clone())
                .collect();
            assert_eq!(
                removed,
                build(&kv, &empty, &survivors).await,
                "removing {:?} left a root a build of the surviving keys would not produce",
                text(path)
            );
        }

        // Two removals compose the same way, so the property is not a one-step
        // accident: the order the two keys go in does not reach the root either.
        let (first, second) = (keys[0].0.clone(), keys[4].0.clone());
        let forwards = kv
            .remove(
                &kv.remove(&base, first.clone()).await.unwrap(),
                second.clone(),
            )
            .await
            .unwrap();
        let backwards = kv
            .remove(&kv.remove(&base, second).await.unwrap(), first)
            .await
            .unwrap();
        assert_eq!(forwards, backwards, "two removals commute on the root");
    });
}

/// Content keys that start with the separator list as the directory they are
/// under, identically on both formats.
///
/// `"/"` is reserved as a *key*, and it is not reserved as a *prefix*: a key
/// like `"/a.txt"` is ordinary content that happens to sit one level down, and
/// the top level names it by the directory it is in, which is `"/"`. Both
/// formats collapse it that way, so a listing must not confuse the directory
/// standing for that content with the reserved slot itself, which
/// [`a_planted_separator_key_is_not_listed`] pins from the other side.
#[test]
fn separator_prefixed_content_lists_alike_on_both_formats() {
    run(async {
        // Each case is a key set written through the seam and the top level it
        // has to list: the directory of separator-prefixed content, a mix of
        // that and a plain top-level key, and a doubled separator, which is one
        // more level down and so still lists as the same directory.
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
    let bound = |index: usize| reference(u8::try_from(index).unwrap().saturating_add(1));

    let nodes = NodeLoadSaver::new(Arc::clone(&raw));
    let editor: ManifestEditor<_> = ManifestEditor::new(nodes.clone());
    let (trie_empty, _) = editor.commit().await.unwrap();
    let trie = MantarayManifest::<_, _, DEFAULT_BODY_SIZE>::new(nodes, store.clone());
    let trie_root = {
        let mut writer = trie.edit(&ChunkRef::new(trie_empty));
        for (index, key) in keys.iter().enumerate() {
            writer.insert(ManifestPath::from(*key), bound(index));
        }
        writer.commit().await.unwrap()
    };

    let builder: Builder<V1> = Builder::new();
    let kv_empty = *builder.build(&store, &Plaintext).await.unwrap().root();
    let kv = LdbManifest::plain(store.clone());
    let kv_root = {
        let mut writer = kv.edit(&kv_empty);
        for (index, key) in keys.iter().enumerate() {
            writer.insert(ManifestPath::from(*key), bound(index));
        }
        writer.commit().await.unwrap()
    };

    let top = ManifestPath::default();
    (
        listing(&trie.at(&trie_root), &top).await,
        listing(&kv.at(&kv_root), &top).await,
    )
}

/// An insert replaces the whole binding: a bare re-insert takes a new reference
/// and clears the metadata the path carried, and the one-shot writes the same.
async fn insert_replaces_the_whole_binding<M>(manifest: &M, base: &ChunkRef)
where
    M: Manifest<ChunkRef>,
    M::Metadata: Clone + PartialEq + std::fmt::Debug,
{
    let page = ManifestPath::from("page.html");
    let meta = manifest
        .metadata_from_view(&MetadataView::new().with(WellKnownKey::ContentType, "text/html"))
        .unwrap();
    assert_ne!(
        meta,
        M::Metadata::default(),
        "the format carries a content type"
    );

    let carried = {
        let mut writer = manifest.edit(base);
        writer.insert(page.clone(), reference(1)).meta(meta.clone());
        writer.commit().await.unwrap()
    };
    assert_eq!(manifest.at(&carried).metadata(&page).await.unwrap(), meta);

    let bare = {
        let mut writer = manifest.edit(&carried);
        writer.insert(page.clone(), reference(2));
        writer.commit().await.unwrap()
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
    assert_eq!(one_shot, bare, "the one-shot is an edit of one insert");
}

#[test]
fn an_insert_replaces_the_whole_binding_on_both_formats() {
    run(async {
        let raw: Raw = Arc::new(MemoryStore::new());
        let store: Store = ContentGet::new(Arc::clone(&raw));

        let nodes = NodeLoadSaver::new(Arc::clone(&raw));
        let editor: ManifestEditor<_> = ManifestEditor::new(nodes.clone());
        let (trie_empty, _) = editor.commit().await.unwrap();
        let trie = MantarayManifest::<_, _, DEFAULT_BODY_SIZE>::new(nodes, store.clone());
        insert_replaces_the_whole_binding(&trie, &ChunkRef::new(trie_empty)).await;

        let builder: Builder<V1> = Builder::new();
        let kv_empty = *builder.build(&store, &Plaintext).await.unwrap().root();
        let kv = LdbManifest::plain(store.clone());
        insert_replaces_the_whole_binding(&kv, &kv_empty).await;
    });
}

/// A reserved key planted past the seam is not listed, whatever kind the
/// listing calls it.
///
/// The seam refuses a write at `"/"`, so only a database written through the
/// raw layer holds one. The folder view collapses it into a subdirectory entry,
/// because it ends in the separator, and a subdirectory with nothing under it
/// is that key and nothing else, so the listing steps over it. A subdirectory
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

        let kv = LdbManifest::plain(store.clone());
        let view = kv.at(&planted);
        assert_eq!(
            listing(&view, &ManifestPath::default()).await,
            ["a.txt"],
            "the top level lists content alone, and never the planted key"
        );
        assert_eq!(
            view.get(&separator).await.unwrap(),
            None,
            "and no read reaches it either"
        );

        // The trie holds the same content and lists the same level.
        let nodes = NodeLoadSaver::new(Arc::clone(&raw));
        let editor: ManifestEditor<_> = ManifestEditor::new(nodes.clone());
        let (trie_empty, _) = editor.commit().await.unwrap();
        let trie = MantarayManifest::<_, _, DEFAULT_BODY_SIZE>::new(nodes, store.clone());
        let root = {
            let mut writer = trie.edit(&ChunkRef::new(trie_empty));
            writer.insert(content.clone(), reference(2));
            writer.commit().await.unwrap()
        };
        assert_eq!(
            listing(&trie.at(&root), &ManifestPath::default()).await,
            listing(&view, &ManifestPath::default()).await,
            "both formats list the same top level"
        );

        // The planted key next to content one level under it: the entry the
        // listing yields is the directory of that content, so it stays, and the
        // planted key is still no key of its own.
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
            view.get(&separator).await.unwrap(),
            None,
            "and the planted key itself still reads absent"
        );
    });
}

/// The site documents resolve over bare keys: the index document is a filename
/// joined below each directory, and the error document is one whole key.
///
/// The manifest is written through the seam's option-typed setters and read
/// through the database's own website reader, so this pins the two halves against
/// each other.
#[test]
fn website_documents_resolve_over_bare_keys() {
    run(async {
        let raw: Raw = Arc::new(MemoryStore::new());
        let store: Store = ContentGet::new(raw);
        let builder: Builder<V1> = Builder::new();
        let empty = *builder.build(&store, &Plaintext).await.unwrap().root();
        let kv = LdbManifest::plain(store.clone());

        let root = {
            let mut writer = kv.edit(&empty);
            writer.insert(ManifestPath::from("index.html"), reference(1));
            writer.insert(ManifestPath::from("docs/index.html"), reference(2));
            writer.insert(ManifestPath::from("docs/guide.html"), reference(3));
            writer.insert(ManifestPath::from("404.html"), reference(4));
            writer
                .with_index_document(ManifestPath::from(INDEX))
                .with_error_document(ManifestPath::from(ERROR));
            writer.commit().await.unwrap()
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
            let mut writer = kv.edit(&root);
            writer.with_index_document(None).with_error_document(None);
            writer.commit().await.unwrap()
        };
        assert!(
            reader.website(&cleared).await.unwrap() == Default::default(),
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
