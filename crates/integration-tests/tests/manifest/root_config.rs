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
//! walk yields it, and the empty path reaches nothing at all.
//!
//! `remove` is exact-key on both formats, exactly as `HashMap::remove` is: the
//! path's own value and metadata go, and no other path does. A path with
//! children keeps every one of them, a childless leaf is pruned, and removing an
//! unbound or absent path is a no-op that leaves the root where it was.

use std::ops::Bound;
use std::sync::Arc;

use nectar_file::MemSink;
use nectar_ldb::{Builder, Key, LdbManifest, Plaintext, Reader as LdbReader, Served, V1};
use nectar_loadsave::NodeLoadSaver;
use nectar_manifest::{Manifest, ManifestPath, MapCursor, MapEntry, MapView, MapWriter};
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

    // A write at the empty path reaches nothing: it is a prefix, not a key.
    let ignored = manifest
        .insert(&configured, empty_path.clone(), reference(9))
        .await
        .unwrap();
    assert_eq!(
        ignored, configured,
        "an insert at the empty path changes nothing"
    );
    assert_eq!(
        manifest
            .remove(&configured, empty_path.clone())
            .await
            .unwrap(),
        configured,
        "a remove at the empty path changes nothing"
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
