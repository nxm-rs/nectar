//! The convergence gate: `"/"` is an ordinary key, and `remove` is exact-key,
//! identically on both manifest formats.
//!
//! Every scenario below is generic over the [`Manifest`] seam and is run twice,
//! once per format. Two things therefore have to hold for it to pass: each
//! format has to answer what the contract says, which the assertions inside the
//! scenario check, and the two formats have to answer the same thing, which the
//! [`Observed`] comparison in each test checks. Either failing names the exact
//! verb and path that diverged, so a root-key edge surfaces here rather than at
//! the gate.
//!
//! # The contract under test
//!
//! `"/"` is a key like any other. `get`, `contains_key`, `metadata`, `floor`,
//! `iter`, `range` and `load` treat it that way, an insert there replaces its
//! whole binding, and `dir("/")` omits it only because no path is a child of
//! itself.
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
use nectar_manifest::{
    Manifest, ManifestPath, MapCursor, MapEntry, MapView, MapWriter, MetadataView, WellKnownKey,
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
/// The site error document: one whole key, so it is absolute like any path.
const ERROR: &str = "/404.html";

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
    /// Every path a range excluding the root yields: the root is a bound like
    /// any other, so excluding it drops exactly itself.
    past_root: Vec<String>,
    /// Every path a range up to and including the root yields: at most the root.
    upto_root: Vec<String>,
    /// Every path `dir("/")` lists, in order.
    listed: Vec<String>,
    /// Every path `dir("/a")` lists: a prefix with no trailing separator, so it
    /// matches the paths starting with it rather than a directory.
    prefixed: Vec<String>,
    /// One row per probed path.
    probes: Vec<Probe>,
}

/// Every point read at one path, with the metadata answer reduced to the two
/// facts that cross formats.
#[derive(Debug, PartialEq, Eq)]
struct Probe {
    path: String,
    entry: Option<MapEntry<ChunkRef>>,
    present: bool,
    /// Whether the path carries the format's empty metadata.
    bare: bool,
    /// Whether the path carries exactly the site documents under test.
    documents: bool,
    /// The greatest bound path at or below this one.
    floor: Option<String>,
    /// Whether a load of the path reached the sink; the references here name no
    /// stored chunk, so this is the verb reaching storage, not a naming answer.
    loaded: bool,
}

/// The site documents in the format's own metadata vocabulary.
fn documents<M: Manifest<ChunkRef>>(manifest: &M) -> M::Metadata {
    manifest
        .metadata_from_view(
            &MetadataView::new()
                .with(WellKnownKey::IndexDocument, INDEX)
                .with(WellKnownKey::ErrorDocument, ERROR),
        )
        .unwrap()
}

/// Read every verb over `root` at each of `paths`, in a format-independent
/// shape.
async fn observe<M>(manifest: &M, root: &ChunkRef, paths: &[ManifestPath]) -> Observed
where
    M: Manifest<ChunkRef>,
    M::Metadata: Clone + PartialEq + std::fmt::Debug,
{
    let view = manifest.at(root);
    let want = documents(manifest);

    let mut keys = Vec::new();
    let mut cursor = view.iter().await.unwrap();
    while let Some((path, _)) = cursor.next().await.unwrap() {
        keys.push(text(&path));
    }

    let root_path = ManifestPath::root();
    let ranged = drain(&view, (Bound::Unbounded, Bound::Unbounded)).await;
    let past_root = drain(
        &view,
        (Bound::Excluded(root_path.clone()), Bound::Unbounded),
    )
    .await;
    let upto_root = drain(
        &view,
        (Bound::Unbounded, Bound::Included(root_path.clone())),
    )
    .await;

    let listed = listing(&view, &root_path).await;
    let prefixed = listing(&view, &ManifestPath::from("/a")).await;

    let mut probes = Vec::new();
    for path in paths {
        let meta = view.metadata(path).await.unwrap();
        let mut sink = MemSink::new();
        probes.push(Probe {
            path: text(path),
            entry: view.get(path).await.unwrap(),
            present: view.contains_key(path).await.unwrap(),
            bare: meta == M::Metadata::default(),
            documents: meta == want,
            floor: view.floor(path).await.unwrap().map(|(path, _)| text(&path)),
            loaded: view.load(path, &mut sink).await.is_ok(),
        });
    }

    Observed {
        keys,
        ranged,
        past_root,
        upto_root,
        listed,
        prefixed,
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

/// The paths every scenario probes: the root, a top-level file, a directory key
/// and a file below it, an interior key with siblings past it, and one absent
/// path.
fn probed() -> Vec<ManifestPath> {
    [
        "/",
        // Sorts immediately after the root and before every named path, so its
        // floor is the root itself when the root is bound and nothing when it is
        // not.
        "/!",
        "/404.html",
        "/a",
        "/ab",
        "/ac",
        "/img/",
        "/img/logo.png",
        "/index.html",
        "/zz",
    ]
    .into_iter()
    .map(ManifestPath::from)
    .collect()
}

/// Every verb at `"/"`, from the site documents on its entry through to the
/// listing that omits it.
async fn every_verb_at_the_root<M>(manifest: &M, empty: &ChunkRef) -> Vec<Observed>
where
    M: Manifest<ChunkRef>,
    M::Metadata: Clone + PartialEq + std::fmt::Debug,
{
    let root_path = ManifestPath::root();
    let index = ManifestPath::from("/index.html");
    let logo = ManifestPath::from("/img/logo.png");
    let img = ManifestPath::from("/img/");

    let bound = {
        let mut writer = manifest.edit(empty);
        writer.insert(index.clone(), reference(1));
        writer.insert(logo.clone(), reference(2));
        writer
            .insert(root_path.clone(), reference(9))
            .with_index_document(INDEX)
            .with_error_document(ERROR);
        writer.commit().await.unwrap()
    };

    let view = manifest.at(&bound);

    // get, contains_key and metadata answer at the root like anywhere else.
    assert_eq!(
        view.get(&root_path).await.unwrap(),
        Some(MapEntry::Reference(reference(9))),
        "get answers at the root"
    );
    assert!(
        view.contains_key(&root_path).await.unwrap(),
        "the root is bound"
    );
    assert_eq!(
        view.metadata(&root_path).await.unwrap(),
        documents(manifest),
        "metadata at the root reads the site documents back"
    );

    // floor: the root is the least path, so it is its own floor and the floor of
    // nothing else.
    assert_eq!(
        view.floor(&root_path).await.unwrap().map(|(p, _)| text(&p)),
        Some(String::from("/")),
        "a bound path is its own floor, the root included"
    );

    // iter and range surface it, sorted first, with nothing filtered.
    let mut walked = Vec::new();
    let mut cursor = view.iter().await.unwrap();
    while let Some((path, _)) = cursor.next().await.unwrap() {
        walked.push(text(&path));
    }
    assert_eq!(walked, ["/", "/img/logo.png", "/index.html"], "iter");

    let mut ranged = Vec::new();
    let mut cursor = view.range(root_path.clone()..).await.unwrap();
    while let Some((path, _)) = cursor.next().await.unwrap() {
        ranged.push(text(&path));
    }
    assert_eq!(ranged, walked, "a range from the root yields every path");

    // dir lists children, and no path is a child of itself: that alone is why
    // the root is absent from its own listing.
    let listed: Vec<String> = view
        .dir(&root_path)
        .await
        .unwrap()
        .entries()
        .iter()
        .map(|entry| text(entry.path()))
        .collect();
    assert_eq!(listed, ["/img/", "/index.html"], "dir(\"/\") omits itself");

    // load reaches storage at the root, so it fails on a reference that names no
    // stored chunk rather than answering from the manifest.
    let mut sink = MemSink::new();
    assert!(
        view.load(&root_path, &mut sink).await.is_err(),
        "a load at the root reaches storage"
    );

    // A bare insert at the root replaces the whole binding, so the site
    // documents go with the reference they were attached to.
    let bare = manifest
        .insert(&bound, root_path.clone(), reference(8))
        .await
        .unwrap();
    let view = manifest.at(&bare);
    assert_eq!(
        view.get(&root_path).await.unwrap(),
        Some(MapEntry::Reference(reference(8))),
        "the root reference is replaced"
    );
    assert_eq!(
        view.metadata(&root_path).await.unwrap(),
        M::Metadata::default(),
        "a bare insert at the root clears the site documents"
    );

    // A remove at the root clears that binding and leaves every child, because
    // the paths below it are its children rather than part of its value.
    let cleared = manifest.remove(&bound, root_path.clone()).await.unwrap();
    assert_ne!(cleared, bound, "clearing a bound root moves the root");
    let view = manifest.at(&cleared);
    assert_eq!(
        view.get(&root_path).await.unwrap(),
        None,
        "the root binding is gone"
    );
    assert!(
        !view.contains_key(&root_path).await.unwrap(),
        "the root is unbound"
    );
    assert_eq!(
        view.metadata(&root_path).await.unwrap(),
        M::Metadata::default(),
        "a remove at the root clears the site documents with the value"
    );
    for child in [&index, &logo] {
        assert_eq!(
            view.get(child).await.unwrap(),
            Some(MapEntry::Reference(if child == &index {
                reference(1)
            } else {
                reference(2)
            })),
            "the child at {} survives the removal of the root",
            text(child)
        );
    }
    let listed: Vec<String> = view
        .dir(&root_path)
        .await
        .unwrap()
        .entries()
        .iter()
        .map(|entry| text(entry.path()))
        .collect();
    assert_eq!(
        listed,
        ["/img/", "/index.html"],
        "the listing is untouched by the removal of the root"
    );
    assert!(
        !view.contains_key(&img).await.unwrap(),
        "a directory key nothing bound stays unbound"
    );

    // A second remove at the now-unbound root is a no-op, and so is one on a
    // root that was never bound.
    assert_eq!(
        manifest.remove(&cleared, root_path.clone()).await.unwrap(),
        cleared,
        "removing an unbound root changes nothing"
    );
    assert_eq!(
        manifest.remove(empty, root_path.clone()).await.unwrap(),
        *empty,
        "removing the root of an empty manifest changes nothing"
    );

    // A child going does not touch the root's binding: the site documents are
    // the root key's metadata, not a property of the tree below it.
    let child_gone = manifest.remove(&bound, logo.clone()).await.unwrap();
    let view = manifest.at(&child_gone);
    assert_eq!(
        view.get(&root_path).await.unwrap(),
        Some(MapEntry::Reference(reference(9))),
        "the root binding outlives a child"
    );
    assert_eq!(
        view.metadata(&root_path).await.unwrap(),
        documents(manifest),
        "so do the site documents"
    );

    // Every root this scenario produced is observed, so a divergence in any
    // state the root key can be in has to show up: bound with site documents,
    // rebound bare, unbound with its children intact, and bound with a child
    // taken out from under it.
    observed(manifest, &[&bound, &bare, &cleared, &child_gone]).await
}

/// Every shape a removal can take, on the one manifest that holds all of them.
async fn every_remove_shape<M>(manifest: &M, empty: &ChunkRef) -> Vec<Observed>
where
    M: Manifest<ChunkRef>,
    M::Metadata: Clone + PartialEq + std::fmt::Debug,
{
    let a = ManifestPath::from("/a");
    let ab = ManifestPath::from("/ab");
    let ac = ManifestPath::from("/ac");
    let img = ManifestPath::from("/img/");
    let logo = ManifestPath::from("/img/logo.png");
    let index = ManifestPath::from("/index.html");

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
            .remove(&base, ManifestPath::from("/nowhere"))
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
    let listed: Vec<String> = view
        .dir(&ManifestPath::root())
        .await
        .unwrap()
        .entries()
        .iter()
        .map(|entry| text(entry.path()))
        .collect();
    assert_eq!(
        listed,
        ["/ac", "/index.html"],
        "the emptied directory is gone from the top level"
    );

    // A directory key that is bound keeps the paths below it, the same way any
    // other path with children does.
    let bound_dir = {
        let mut writer = manifest.edit(&emptied);
        writer.insert(ManifestPath::from("/d/"), reference(6));
        writer.insert(ManifestPath::from("/d/x"), reference(7));
        writer.commit().await.unwrap()
    };
    let dir_cleared = manifest
        .remove(&bound_dir, ManifestPath::from("/d/"))
        .await
        .unwrap();
    let view = manifest.at(&dir_cleared);
    assert_eq!(
        view.get(&ManifestPath::from("/d/")).await.unwrap(),
        None,
        "the directory key's own binding is gone"
    );
    assert_eq!(
        view.get(&ManifestPath::from("/d/x")).await.unwrap(),
        Some(MapEntry::Reference(reference(7))),
        "the path below it survives"
    );

    // Removing the last path leaves an empty manifest that still reads.
    let stripped = {
        let mut writer = manifest.edit(&dir_cleared);
        for path in [&ac, &index, &ManifestPath::from("/d/x")] {
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
        view.dir(&ManifestPath::root())
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
    every_verb_at_the_root_agrees_across_formats,
    every_verb_at_the_root
);
differential!(every_remove_shape_agrees_across_formats, every_remove_shape);

/// The site documents resolve over rooted keys: the index document is a filename
/// joined below each directory, and the error document is one whole key.
///
/// The manifest is written through the seam's typed builders and read through the
/// database's own website reader, so this pins the two halves against each other.
#[test]
fn website_documents_resolve_over_rooted_keys() {
    run(async {
        let raw: Raw = Arc::new(MemoryStore::new());
        let store: Store = ContentGet::new(raw);
        let builder: Builder<V1> = Builder::new();
        let empty = *builder.build(&store, &Plaintext).await.unwrap().root();
        let kv = LdbManifest::plain(store.clone());

        let root = {
            let mut writer = kv.edit(&empty);
            writer.insert(ManifestPath::from("/index.html"), reference(1));
            writer.insert(ManifestPath::from("/docs/index.html"), reference(2));
            writer.insert(ManifestPath::from("/docs/guide.html"), reference(3));
            writer.insert(ManifestPath::from("/404.html"), reference(4));
            writer
                .insert(ManifestPath::root(), reference(9))
                .with_index_document(INDEX)
                .with_error_document(ERROR);
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
            "the error document is the absolute key it was set to"
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
            // An exact key wins, the root key included: it is bound here.
            ("/", ("exact", "/")),
            ("/index.html", ("exact", "/index.html")),
            // The index document joins below each directory, per directory.
            ("/docs/", ("index", "/docs/index.html")),
            ("/docs", ("index", "/docs/index.html")),
            // Nothing resolves, so the error document does, as one whole key.
            ("/missing.html", ("error", "/404.html")),
            ("/docs/missing.html", ("error", "/404.html")),
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

        // With the root unbound, the root request path falls back to its
        // directory index rather than to an exact key.
        let cleared = kv.remove(&root, ManifestPath::root()).await.unwrap();
        assert!(
            reader.website(&cleared).await.unwrap() == Default::default(),
            "clearing the root binding clears the site documents with it"
        );
        let served = reader.serve(&cleared, &Key::from(&b"/"[..])).await.unwrap();
        assert_eq!(
            resolved(&served),
            ("missing", String::new()),
            "no documents are declared, so nothing resolves for the root path"
        );
    });
}
