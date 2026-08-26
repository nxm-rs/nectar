//! The convergence gate: content keys are bare, the site configuration is an
//! option and not a key, and `remove` is exact-key, identically on both
//! formats. Each `differential!` test runs one scenario on both formats and
//! compares the [`Observed`] answers. Root history-independence is pinned on
//! the key-value database alone: mantaray 0.2 keeps insert-order node
//! placement, which the wire `mantaray/bee_vectors.rs` pins.

#![allow(clippy::indexing_slicing, clippy::unwrap_used)]

use std::ops::Bound;
use std::sync::Arc;

use bytes::Bytes;
use nectar_ldb::{
    Builder, Database, Entry, FolderServed, Key, Plaintext, Reader as LdbReader, V1, Website,
};
use nectar_manifest::{
    Batch, ListEntry, Manifest, ManifestCursor, ManifestError, ManifestMeta, ManifestOp,
    ManifestPath, ManifestView, MapEntry, MemSink, MetadataView, WellKnownKey,
};
use nectar_mantaray::{ManifestEditor, MantarayManifest, NodeLoadSaver};
use nectar_primitives::{ChunkRef, DEFAULT_BODY_SIZE, EncryptedChunkRef, EncryptionKey};
use nectar_testing::run;

mod common;
use common::{
    Observed, Refused, RefusingSink, applied, both_formats, build, doc_delta, docs, drain, key_set,
    listing, observed, p, reference, refs, refused, removal_keys, removed, survivors, text,
    write_keys,
};

/// A filename joined below each directory, so relative on purpose.
const INDEX: &str = "index.html";
/// One whole content key.
const ERROR: &str = "404.html";

/// The site documents: set, read back, cleared, and never a key in the map.
async fn site_config_scenario<M>(manifest: &M, empty: &ChunkRef) -> Vec<Observed>
where
    M: Manifest<ChunkRef>,
    M::Metadata: Clone + PartialEq + std::fmt::Debug,
{
    let empty_path = ManifestPath::default();
    let separator = p("/");

    // Content only: nothing declares a document yet.
    let content = applied(manifest, empty, |batch| {
        batch.insert(p(INDEX), reference(1));
        batch.insert(p("img/logo.png"), reference(2));
    })
    .await;
    docs(manifest, &content, None, None, "unset").await;

    // Both documents read back, and declaring them moves the root.
    let configured = doc_delta(manifest, &content, Some(Some(INDEX)), Some(Some(ERROR))).await;
    docs(manifest, &configured, Some(INDEX), Some(ERROR), "cfg").await;
    assert_ne!(configured, content, "declaring a document moves the root");

    // The slot is not a key: no map verb reaches a reserved path, and a write
    // at one is refused, and named.
    let view = manifest.at(configured);
    let nine = reference(9);
    for path in [&empty_path, &separator] {
        let hint = text(path);
        let got = view.get(path).await.unwrap();
        assert_eq!(got, None, "{hint:?} binds nothing");
        let bound = view.contains_key(path).await.unwrap();
        assert!(!bound, "{hint:?} is unbound");
        let meta = view.metadata(path).await.unwrap();
        assert_eq!(meta, M::Metadata::default(), "{hint:?} carries metadata");
        let load = view.load(path, &mut MemSink::new()).await;
        assert!(load.is_err(), "{hint:?} names no data");
        let insert = manifest.insert(configured, path.clone(), nine).await;
        assert_eq!(refused(&insert), Some(path.clone()), "insert at {hint:?}");
        let remove = manifest.remove(configured, path.clone()).await;
        assert_eq!(refused(&remove), Some(path.clone()), "remove at {hint:?}");
    }

    // No walk yields the slot either.
    let keys = key_set(manifest, &configured).await;
    assert_eq!(keys, ["img/logo.png", "index.html"], "bare keys in order");
    let top = listing(&view, &empty_path).await;
    assert_eq!(top, ["img/", "index.html"], "the top level lists content");

    // The refusal takes the whole batch with it.
    let mut batch = Batch::new();
    batch.insert(p("landed.html"), reference(9));
    batch.insert(separator.clone(), reference(9));
    let mixed = manifest.apply(configured, batch).await;
    assert_eq!(refused(&mixed), Some(separator), "refused whole");
    let landed = view.contains_key(&p("landed.html")).await.unwrap();
    assert!(!landed, "the op beside the refusal landed");

    // The two documents are independent; clearing is the same setter with
    // `None`, lands on the root that never declared, restores the content-only
    // root once both go, and is a no-op on an undeclared document.
    let index_only = doc_delta(manifest, &content, Some(Some(INDEX)), None).await;
    docs(manifest, &index_only, Some(INDEX), None, "index only").await;
    let error_cleared = doc_delta(manifest, &configured, None, Some(None)).await;
    assert_eq!(error_cleared, index_only, "clears onto the undeclared root");
    let stripped = doc_delta(manifest, &configured, Some(None), Some(None)).await;
    assert_eq!(stripped, content, "clearing both restores the content root");
    docs(manifest, &stripped, None, None, "stripped").await;
    let noop = doc_delta(manifest, &content, Some(None), Some(None)).await;
    assert_eq!(noop, content, "clearing an undeclared doc is a no-op");

    // Removing a content key does not touch the configuration.
    let child_gone = removed(manifest, configured, "img/logo.png").await;
    docs(manifest, &child_gone, Some(INDEX), Some(ERROR), "outlive").await;

    let roots = [content, configured, index_only, stripped, child_gone];
    observed(manifest, &roots.iter().collect::<Vec<_>>()).await
}

/// Every shape a removal can take, on the one manifest that holds all of them.
async fn every_remove_shape<M>(manifest: &M, empty: &ChunkRef) -> Vec<Observed>
where
    M: Manifest<ChunkRef>,
    M::Metadata: Clone + PartialEq + std::fmt::Debug,
{
    let img = p("img/");
    let keys = ["a", "ab", "ac", "img/logo.png", "index.html"];
    let base = write_keys(manifest, empty, &keys).await;

    // Nothing to remove: an absent path, then an unbound directory key with a
    // path below it. Neither changes the root.
    let same = removed(manifest, base, "nowhere").await;
    assert_eq!(same, base, "an absent path is a no-op");
    let bound = manifest.at(base).contains_key(&img).await.unwrap();
    assert!(!bound, "img/ is unbound");
    let same = removed(manifest, base, "img/").await;
    assert_eq!(same, base, "an unbound directory key is a no-op");

    // An interior path with paths past it: only its own binding goes.
    let interior = removed(manifest, base, "a").await;
    assert_ne!(interior, base, "a bound interior path moves the root");
    let cases = [("a", None), ("ab", Some(2)), ("ac", Some(3))];
    refs(manifest, &interior, &cases, "the paths past it survive").await;

    // A childless leaf: pruned outright, and the sibling untouched.
    let leaf = removed(manifest, interior, "ab").await;
    refs(manifest, &leaf, &[("ab", None), ("ac", Some(3))], "pruned").await;

    // Removing every leaf below a directory leaves the listing empty.
    let emptied = removed(manifest, leaf, "img/logo.png").await;
    let view = manifest.at(emptied);
    let entries = view.dir(&img).await.unwrap();
    assert!(entries.entries().is_empty(), "the emptied dir lists none");
    let top = listing(&view, &ManifestPath::default()).await;
    assert_eq!(top, ["ac", "index.html"], "the emptied dir leaves the top");

    // A directory key that is bound keeps the paths below it.
    let bound = applied(manifest, &emptied, |batch| {
        batch.insert(p("d/"), reference(6));
        batch.insert(p("d/x"), reference(7));
    })
    .await;
    let cleared = removed(manifest, bound, "d/").await;
    let cases = [("d/", None), ("d/x", Some(7))];
    refs(manifest, &cleared, &cases, "d/x survives").await;

    // Removing the last path leaves an empty manifest that still reads.
    let stripped = applied(manifest, &cleared, |batch| {
        for path in ["ac", "index.html", "d/x"] {
            batch.remove(p(path));
        }
    })
    .await;
    let view = manifest.at(stripped);
    let first = view.iter().await.unwrap().next().await.unwrap();
    assert!(first.is_none(), "the stripped manifest holds no path");
    let top = view.dir(&ManifestPath::default()).await.unwrap();
    assert!(top.entries().is_empty(), "and lists none");

    let roots = [base, interior, leaf, emptied, bound, cleared, stripped];
    observed(manifest, &roots.iter().collect::<Vec<_>>()).await
}

/// An exact-key remove leaves the surviving key set on both formats, and two
/// removals commute on it. The root is not asserted here; see the module doc.
async fn a_remove_leaves_the_surviving_keys<M>(manifest: &M, empty: &ChunkRef) -> Vec<Observed>
where
    M: Manifest<ChunkRef>,
    M::Metadata: Clone + PartialEq + std::fmt::Debug,
{
    let keys = removal_keys();
    let base = build(manifest, empty, &keys).await;

    let mut roots = vec![base];
    for (index, (path, _)) in keys.iter().enumerate() {
        let removed = manifest.remove(base, path.clone()).await.unwrap();
        assert_ne!(removed, base, "removing {:?} moves the root", text(path));

        let built = build(manifest, empty, &survivors(&keys, index)).await;
        let got = key_set(manifest, &removed).await;
        let want = key_set(manifest, &built).await;
        assert_eq!(got, want, "removing {:?} left the wrong keys", text(path));
        roots.push(removed);
    }

    // The order the two keys go in does not reach the key set.
    let (first, second) = (keys[0].0.clone(), keys[4].0.clone());
    let forwards = manifest.remove(base, first.clone()).await.unwrap();
    let forwards = manifest.remove(forwards, second.clone()).await.unwrap();
    let backwards = manifest.remove(base, second).await.unwrap();
    let backwards = manifest.remove(backwards, first).await.unwrap();
    let got = key_set(manifest, &forwards).await;
    let want = key_set(manifest, &backwards).await;
    assert_eq!(got, want, "two removals commute on the key set");
    roots.push(forwards);
    roots.push(backwards);

    // An empty manifest has one shape on either format, so the root is the
    // assertion here.
    let stripped = applied(manifest, &base, |batch| {
        for (path, _) in &keys {
            batch.remove(path.clone());
        }
    })
    .await;
    assert_eq!(&stripped, empty, "removing every key restores empty");
    roots.push(stripped);

    observed(manifest, &roots.iter().collect::<Vec<_>>()).await
}

/// Both formats through one scenario, with the answers compared.
macro_rules! differential {
    ($name:ident, $scenario:ident) => {
        #[test]
        fn $name() {
            run(async {
                let (raw, store) = common::stores();
                let ((trie, trie_empty), (kv, kv_empty)) = both_formats(&raw, &store).await;
                let from_trie = $scenario(&trie, &trie_empty).await;
                let from_kv = $scenario(&kv, &kv_empty).await;

                assert_eq!(from_trie, from_kv, "the formats answered differently");
            });
        }
    };
}

differential!(the_site_config_agrees_across_formats, site_config_scenario);
differential!(every_remove_shape_agrees_across_formats, every_remove_shape);
differential!(
    a_remove_leaves_the_surviving_keys_on_both_formats,
    a_remove_leaves_the_surviving_keys
);

/// The key-value database's removal is history-independent: the root a removal
/// lands on is the root a build of the surviving keys produces. One format
/// only, on purpose; see the module doc.
#[test]
fn a_remove_is_history_independent_on_ldb() {
    run(async {
        let (_raw, store) = common::stores();
        let kv = Database::<_>::plain(store.clone());
        let empty: ChunkRef = kv.empty().await.unwrap();
        let rm = |root, path: &ManifestPath| Manifest::remove(&kv, root, path.clone());

        let keys = removal_keys();
        let base = build(&kv, &empty, &keys).await;

        for (index, (path, _)) in keys.iter().enumerate() {
            let removed = rm(base, path).await.unwrap();
            assert_ne!(removed, base, "removing {:?} moves the root", text(path));

            let rebuilt = build(&kv, &empty, &survivors(&keys, index)).await;
            assert_eq!(removed, rebuilt, "{:?} is history-dependent", text(path));
        }

        // The order the two keys go in does not reach the root either.
        let forwards = rm(base, &keys[0].0).await.unwrap();
        let forwards = rm(forwards, &keys[4].0).await.unwrap();
        let backwards = rm(base, &keys[4].0).await.unwrap();
        let backwards = rm(backwards, &keys[0].0).await.unwrap();
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
    let page = p("a.txt");
    let one = manifest.insert(*empty, page.clone(), reference(1)).await;
    let one = one.unwrap();
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
        let (raw, store) = common::stores();
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
        let (raw, store) = common::stores();
        let ((trie, _), (kv, _)) = both_formats(&raw, &store).await;
        // No chunk lives behind this root.
        let garbage = reference(0xEE);
        let sep = p("/");

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
        .insert(p("b"), reference(1))
        .remove(p("a"))
        .extend([ManifestOp::Insert {
            path: p("c"),
            reference: reference(2),
            meta: (),
        }])
        .insert_with(p("b"), reference(3), ());
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
    batch.set_index_document(p(INDEX));
    batch.set_error_document(None);
    let checked = batch.into_checked().unwrap();
    assert!(checked.ops.is_empty());
    assert_eq!(checked.index_document, Some(Some(p(INDEX))));
    assert_eq!(checked.error_document, Some(None));

    // A reserved path stages no op and refuses the whole batch with the
    // first one, through any verb; the refusal counts against `is_empty`.
    let mut batch: Batch = Batch::new();
    batch.insert(p("landed.html"), reference(1));
    batch.remove(p("/"));
    batch.insert(ManifestPath::default(), reference(2));
    assert!(!batch.is_empty());
    assert_eq!(batch.into_checked().unwrap_err().path(), &p("/"));

    let mut batch: Batch = Batch::new();
    batch.extend([ManifestOp::Remove {
        path: ManifestPath::default(),
    }]);
    assert!(!batch.is_empty());
    let refusal = batch.into_checked().unwrap_err();
    assert_eq!(refusal.path(), &ManifestPath::default());
}

/// Content keys that start with the separator list as the directory they are
/// under, identically on both formats: `"/"` is reserved as a key, not as a
/// prefix. [`a_planted_separator_key_is_not_listed`] pins the other side.
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
            let (raw, store) = common::stores();
            let ((trie, trie_empty), (kv, kv_empty)) = both_formats(&raw, &store).await;
            let trie_root = write_keys(&trie, &trie_empty, keys).await;
            let kv_root = write_keys(&kv, &kv_empty, keys).await;
            let top = ManifestPath::default();
            let from_trie = listing(&trie.at(trie_root), &top).await;
            assert_eq!(from_trie, want, "trie {keys:?}");
            let from_kv = listing(&kv.at(&kv_root), &top).await;
            assert_eq!(from_kv, want, "database {keys:?}");
        }
    });
}

/// An insert replaces the whole binding: a bare re-insert takes a new reference
/// and clears the metadata the path carried.
async fn insert_replaces_the_whole_binding<M>(manifest: &M, base: &ChunkRef)
where
    M: Manifest<ChunkRef>,
    M::Metadata: Clone + PartialEq + std::fmt::Debug,
{
    let page = p("page.html");
    let meta =
        M::Metadata::from_source(&MetadataView::new().with(WellKnownKey::ContentType, "text/html"));
    assert_ne!(meta, M::Metadata::default(), "a content type is carried");

    let carried = applied(manifest, base, |batch| {
        batch.insert_with(page.clone(), reference(1), meta.clone());
    })
    .await;
    assert_eq!(manifest.at(carried).metadata(&page).await.unwrap(), meta);

    let bare = applied(manifest, &carried, |batch| {
        batch.insert(page.clone(), reference(2));
    })
    .await;
    let view = manifest.at(bare);
    let entry = view.get(&page).await.unwrap();
    assert_eq!(entry, Some(MapEntry::Reference(reference(2))), "replaced");
    let meta_after = view.metadata(&page).await.unwrap();
    assert_eq!(meta_after, M::Metadata::default(), "metadata cleared");

    let one_shot = manifest.insert(carried, page, reference(2)).await.unwrap();
    assert_eq!(one_shot, bare, "the one-shot is a one-op batch");
}

#[test]
fn an_insert_replaces_the_whole_binding_on_both_formats() {
    run(async {
        let (raw, store) = common::stores();
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
    let view = manifest.at(empty);
    let first = view.iter().await.unwrap().next().await.unwrap();
    assert!(first.is_none(), "{f}: the empty manifest holds no path");
    let mut sink = MemSink::new();
    let missing = view.load(&p("missing.html"), &mut sink).await.err();
    assert!(missing.is_some_and(|e| e.is_not_found()), "{f}: NotFound");
}

/// The seam bootstrap is the format's own: `empty` returns the root the
/// native builder produces.
#[test]
fn the_seam_bootstrap_matches_each_format() {
    run(async {
        let (raw, store) = common::stores();

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
    let path = p(INDEX);
    let empty = manifest.empty().await.unwrap();
    let root = manifest.insert(empty, path.clone(), file).await.unwrap();
    let view = manifest.at(root);
    let error = view.load(&path, &mut RefusingSink).await.unwrap_err();
    let sink = matches!(error, ManifestError::Sink(_));
    assert!(sink, "{f}: wrong variant: {error:?}");
    let source = core::error::Error::source(&error);
    let kept = source.is_some_and(|s| s.downcast_ref::<Refused>().is_some());
    assert!(kept, "{f}: the sink's own error left the chain");
}

/// A sink refusal is the seam's own `Sink`, never `Data`, on both formats.
#[test]
fn a_refused_sink_write_is_sink_on_both_formats() {
    run(async {
        let (raw, store) = common::stores();
        let data = vec![7u8; 20_000];
        let file = common::save_file(&raw, &data).await;

        let ((trie, _), (kv, _)) = both_formats(&raw, &store).await;
        assert_sink_refusal(&trie, file, "trie").await;
        assert_sink_refusal(&kv, file, "database").await;
    });
}

/// Raw entries planted below the seam, which knows no reserved key.
async fn plant(kv: &common::Kv, empty: &ChunkRef, keys: &[(&[u8], u8)]) -> ChunkRef {
    let mut editor = kv.edit(empty);
    for (key, byte) in keys {
        editor.insert(Key::from(*key), Entry::from(reference(*byte)));
    }
    editor.commit().await.unwrap()
}

/// A reserved key planted past the seam is not listed, whatever kind the
/// listing calls it: only a database written through the raw layer holds one,
/// the folder view collapses it into a subdirectory entry, and a subdirectory
/// with nothing under it is that key alone.
/// [`separator_prefixed_content_lists_alike_on_both_formats`] pins the other
/// side.
#[test]
fn a_planted_separator_key_is_not_listed() {
    run(async {
        let (raw, store) = common::stores();
        let separator = p("/");
        let kv = Database::<_>::plain(store.clone());
        let empty: ChunkRef = kv.empty().await.unwrap();

        let planted = plant(&kv, &empty, &[(b"/", 1), (b"a.txt", 2)]).await;
        let raw_read = kv.at(&planted).get(&Key::from(&b"/"[..])).await.unwrap();
        assert!(raw_read.is_some(), "the raw layer holds the planted key");

        let view = Manifest::<ChunkRef>::at(&kv, planted);
        let top = listing(&view, &ManifestPath::default()).await;
        assert_eq!(top, ["a.txt"], "content alone, never the planted key");
        let read = ManifestView::get(&view, &separator).await.unwrap();
        assert_eq!(read, None, "and no read reaches it either");

        // The trie holds the same content and lists the same level.
        let nodes = NodeLoadSaver::new(Arc::clone(&raw));
        let editor: ManifestEditor<_> = ManifestEditor::new(nodes.clone());
        let (trie_empty, _) = editor.commit().await.unwrap();
        let trie = MantarayManifest::<_, _, DEFAULT_BODY_SIZE>::new(nodes, store.clone());
        let root = applied(&trie, &ChunkRef::new(trie_empty), |batch| {
            batch.insert(p("a.txt"), reference(2));
        })
        .await;
        let from_trie = listing(&trie.at(root), &ManifestPath::default()).await;
        assert_eq!(from_trie, top, "both formats list the same top level");

        // Next to content one level under it, the listed entry is the
        // directory of that content, and the planted key still no key.
        let with_content = plant(&kv, &empty, &[(b"/", 1), (b"/a.txt", 2)]).await;
        let view = Manifest::<ChunkRef>::at(&kv, with_content);
        let top = listing(&view, &ManifestPath::default()).await;
        assert_eq!(top, ["/"], "the content's directory is listed");
        let read = ManifestView::get(&view, &separator).await.unwrap();
        assert_eq!(read, None, "the planted key itself still reads absent");
    });
}

/// The site documents resolve over bare keys: the index document is a filename
/// joined below each directory, and the error document is one whole key.
/// Written through the seam's setters, read through the database's own website
/// reader.
#[test]
fn website_documents_resolve_over_bare_keys() {
    run(async {
        let (_raw, store) = common::stores();
        let kv = Database::<_>::plain(store.clone());
        let empty: ChunkRef = kv.empty().await.unwrap();

        let root = applied(&kv, &empty, |batch| {
            batch
                .insert(p("index.html"), reference(1))
                .insert(p("docs/index.html"), reference(2))
                .insert(p("docs/guide.html"), reference(3))
                .insert(p("404.html"), reference(4))
                .set_index_document(p(INDEX))
                .set_error_document(p(ERROR));
        })
        .await;

        let reader: LdbReader<_> = LdbReader::new(&store);
        let site = reader.website(&root).await.unwrap();
        assert_eq!(site.index(), Some(INDEX.as_bytes()), "index reads back");
        assert_eq!(site.error(), Some(ERROR.as_bytes()), "error reads back");

        /// The key a request path resolved to, and how.
        fn resolved(served: &FolderServed<V1>) -> (&'static str, String) {
            let key = served
                .key()
                .map(|key| String::from_utf8(key.as_bytes().to_vec()).unwrap())
                .unwrap_or_default();
            let how = match served {
                FolderServed::Exact { .. } => "exact",
                FolderServed::Index { .. } => "index",
                FolderServed::Error { .. } => "error",
                FolderServed::Missing => "missing",
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
            let key = Key::from(request.as_bytes());
            let served = reader.serve(&root, &key).await.unwrap();
            let (how, key) = resolved(&served);
            assert_eq!((how, key.as_str()), want, "serving {request:?}");
        }

        // With the documents cleared, nothing falls back at all.
        let cleared = doc_delta(&kv, &root, Some(None), Some(None)).await;
        let site = reader.website(&cleared).await.unwrap();
        assert_eq!(site, Website::default(), "no conventions survive");
        let key = Key::from(&b"missing.html"[..]);
        let served = reader.serve(&cleared, &key).await.unwrap();
        assert_eq!(resolved(&served), ("missing", String::new()));
    });
}

/// The bytes an inline entry carries in the manifest itself.
const INLINE: &[u8] = b"carried by the manifest itself";
/// The bytes a plain reference stores behind the manifest.
const FILE_DATA: &[u8] = b"stored behind a reference";
/// The three probed keys, in path order.
const KINDS: [&str; 3] = ["docs/inline.txt", "docs/plain.bin", "docs/wide.bin"];

/// `MapEntry::Value` conformance on the key-value format: `get` answers
/// `Reference`, `Value` or `Opaque`, `load` serves exactly the loadable two,
/// and every walk carries the kind. An inline value loads from the manifest
/// itself; a reference the caller's width cannot hold reads as `Opaque` and
/// loads as `NoData`, before any store fetch.
#[test]
fn an_inline_entry_is_a_loadable_value_and_opaque_is_not() {
    run(async {
        let (raw, store) = common::stores();
        let file = common::save_file(&raw, FILE_DATA).await;
        let wide = EncryptedChunkRef::new(*file.address(), EncryptionKey::from([0x5a; 32]));
        let db = Database::plain(store.clone());
        let empty: ChunkRef = db.empty().await.unwrap();
        let inline = Entry::inline(Bytes::from_static(INLINE)).unwrap();
        let mut editor = db.edit(&empty);
        editor.insert(Key::from(KINDS[0].as_bytes()), inline);
        editor.insert(Key::from(KINDS[1].as_bytes()), Entry::from(file));
        editor.insert(Key::from(KINDS[2].as_bytes()), Entry::from(wide));
        let root = editor.commit().await.unwrap();
        let view = Manifest::<ChunkRef>::at(&db, root);

        // `get` discriminates the three entry kinds, `is_loadable` follows the
        // kind, and `load` serves exactly the loadable two: the opaque one
        // fails as `NoData`, before any store fetch, with nothing sunk.
        for (path, want, loads) in [
            (KINDS[0], MapEntry::Value, Some(INLINE)),
            (KINDS[1], MapEntry::Reference(file), Some(FILE_DATA)),
            (KINDS[2], MapEntry::Opaque, None),
        ] {
            let probe = p(path);
            let entry = ManifestView::get(&view, &probe).await.unwrap().unwrap();
            assert_eq!(entry, want, "{path}");
            assert_eq!(entry.is_loadable(), loads.is_some(), "{path}: is_loadable");
            let mut sink = MemSink::new();
            let outcome = ManifestView::load(&view, &probe, &mut sink).await;
            if let Some(bytes) = loads {
                outcome.unwrap();
                assert_eq!(sink.as_ref(), bytes, "{path}: the wrong bytes loaded");
            } else {
                let error = outcome.unwrap_err();
                let no_data =
                    matches!(&error, ManifestError::NoData(p) if p.as_bytes() == path.as_bytes());
                assert!(no_data, "{path}: wrong error: {error:?}");
                assert!(sink.as_ref().is_empty(), "{path}: bytes reached the sink");
            }
        }

        // The full walk yields the three kinds in path order, and a bounded
        // walk keeps the discrimination.
        let expect = |loadable: [bool; 3]| -> Vec<(String, bool)> {
            KINDS
                .iter()
                .zip(loadable)
                .map(|(k, l)| ((*k).to_owned(), l))
                .collect()
        };
        let mut kinds = Vec::new();
        let mut cursor = ManifestView::<ChunkRef>::iter(&view).await.unwrap();
        while let Some((path, entry)) = cursor.next().await.unwrap() {
            kinds.push((text(&path), entry.is_loadable()));
        }
        assert_eq!(kinds, expect([true, true, false]));
        let bounds = (Bound::Included(p(KINDS[0])), Bound::Excluded(p(KINDS[2])));
        assert_eq!(drain(&view, bounds).await, [KINDS[0], KINDS[1]]);

        // The folder view lists an inline value and an opaque reference both
        // as values, never fetching either.
        let dir = ManifestView::<ChunkRef>::dir(&view, &p("docs/")).await;
        let listed: Vec<(String, bool)> = dir
            .unwrap()
            .entries()
            .iter()
            .map(|entry| (text(entry.path()), matches!(entry, ListEntry::Value { .. })))
            .collect();
        assert_eq!(listed, expect([true, false, true]));

        // The ordered seek answers with the entry kind too.
        let probe = p("docs/inline.zzz");
        let floored = ManifestView::floor(&view, &probe).await.unwrap().unwrap();
        assert_eq!(floored.0.as_bytes(), KINDS[0].as_bytes());
        assert_eq!(floored.1, MapEntry::Value);
    });
}
