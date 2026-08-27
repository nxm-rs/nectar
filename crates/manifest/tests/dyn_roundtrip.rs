//! Both manifest formats behind one `Box<dyn ErasedManifest>`: the same list,
//! load and apply calls drive both, and the metadata a caller writes through
//! the erased view lands in each format's own native slot.

#![allow(
    clippy::as_conversions,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::unwrap_used
)]

use nectar_ldb::{Database, KeyLookup};
use nectar_manifest::{
    ErasedManifest, Listing, ManifestOp, ManifestPath, MapEntry, MetadataSource, MetadataView,
    SiteConfig, WellKnownKey,
};
use nectar_mantaray::{MantarayManifest, NodeLoadSaver, TrieLookup, metadata};
use nectar_primitives::{ChunkRef, DEFAULT_BODY_SIZE};
use nectar_testing::MemWriteAt;
use nectar_testing::run;
use std::sync::Arc;

mod common;
use common::{Nodes, p, save_file, stores};

/// The file bytes every manifest entry in this test points at.
fn payload() -> Vec<u8> {
    (0..20_000u32).map(|i| (i % 251) as u8).collect()
}

/// Content type written through the erased metadata view.
const CONTENT_TYPE: &str = "text/html";

/// Erased metadata carrying one content type.
fn content_type() -> Box<dyn MetadataSource> {
    Box::new(MetadataView::new().with(WellKnownKey::ContentType, CONTENT_TYPE))
}

/// The listed paths, as text.
fn paths(listing: &Listing) -> Vec<String> {
    listing
        .entries()
        .iter()
        .map(|entry| String::from_utf8_lossy(entry.path().as_bytes()).into_owned())
        .collect()
}

/// Drive one erased manifest through the whole seam: bootstrap the empty
/// root, apply a batch, list the root and a subdirectory, load an entry's
/// data, then remove it.
async fn exercise(manifest: &dyn ErasedManifest, base: &ChunkRef, file: &ChunkRef, data: &[u8]) {
    let top = ManifestPath::default();
    let index = p("index.html");
    let logo = p("img/logo.png");
    let ops = vec![
        ManifestOp::Insert {
            path: index.clone(),
            reference: *file,
            meta: content_type(),
        },
        ManifestOp::Insert {
            path: logo.clone(),
            reference: *file,
            meta: Box::new(()),
        },
    ];
    let root = manifest.dyn_apply(base, ops).await.unwrap();
    assert_ne!(&root, base, "the batch produced a new root");

    // The metadata written through the erased seam reads back through it, by
    // key and enumerably; a bare entry reads back empty.
    let meta = manifest.dyn_metadata(&root, &index).await.unwrap();
    let want = Some(CONTENT_TYPE.as_bytes());
    assert_eq!(meta.get(&WellKnownKey::ContentType), want);
    let mut enumerated = false;
    meta.for_each(&mut |key, value| {
        enumerated |= WellKnownKey::registered(key) == Some(WellKnownKey::ContentType)
            && value == CONTENT_TYPE.as_bytes();
    });
    assert!(enumerated, "the content type enumerates");
    let bare = manifest.dyn_metadata(&root, &logo).await.unwrap();
    assert_eq!(bare.get(&WellKnownKey::ContentType), None);
    bare.for_each(&mut |key, _| panic!("a bare entry enumerated {key}"));

    // One level: the deeper path collapses into a directory entry, in path
    // order, and neither format fetches the referenced data to list it.
    let listing = manifest.dyn_dir(&root, &top).await.unwrap();
    assert_eq!(paths(&listing), ["img/", "index.html"]);
    assert!(listing.entries()[0].is_dir());
    assert_eq!(listing.entries()[1].reference(), Some(file));

    // The subdirectory lists its one file under its full path.
    let nested = manifest.dyn_dir(&root, &p("img/")).await.unwrap();
    assert_eq!(paths(&nested), ["img/logo.png"]);

    // The erased floor is the greatest bound path at or below the probe.
    let floored = manifest.dyn_floor(&root, &p("index.zzz")).await.unwrap();
    let (path, entry) = floored.expect("a path at or below the probe");
    assert_eq!(path.as_bytes(), b"index.html");
    assert_eq!(entry.reference(), Some(file));
    let none = manifest.dyn_floor(&root, &p("aaa")).await.unwrap();
    assert!(none.is_none(), "no path is at or below the probe");

    // A load joins the whole chunk tree the entry names into the sink, and
    // reports the bytes it wrote.
    let mut sink = MemWriteAt::new();
    let written = manifest.dyn_load(&root, &index, &mut sink).await.unwrap();
    assert_eq!(written, u64::try_from(data.len()).unwrap());
    assert_eq!(sink.as_bytes(), data);

    // A removal is the same map vocabulary, and the removed path stops
    // resolving.
    let ops = vec![ManifestOp::Remove {
        path: index.clone(),
    }];
    let pruned = manifest.dyn_apply(&root, ops).await.unwrap();
    let listing = manifest.dyn_dir(&pruned, &top).await.unwrap();
    assert_eq!(paths(&listing), ["img/"]);

    let mut sink = MemWriteAt::new();
    let gone = manifest.dyn_load(&pruned, &index, &mut sink).await;
    assert!(gone.is_err(), "a removed path names no data");

    // The configuration crosses the erased seam as a value: what is set reads
    // back, and clearing it restores the root that declared nothing.
    let unset = manifest.dyn_site_config(base).await.unwrap();
    assert_eq!(unset, SiteConfig::new(), "neither document declared");
    let config = SiteConfig::new()
        .with_index_document(index.clone())
        .with_error_document(p("404.html"));
    let configured = manifest.dyn_set_site_config(&root, config.clone()).await;
    let configured = configured.unwrap();
    let read = manifest.dyn_site_config(&configured).await.unwrap();
    assert_eq!(read, config, "the documents read back");
    let none = SiteConfig::new();
    let cleared = manifest.dyn_set_site_config(&configured, none).await;
    assert_eq!(cleared.unwrap(), root, "clearing restores the content root");

    // The empty path is a listing prefix, not a key.
    let mut sink = MemWriteAt::new();
    let empty_load = manifest.dyn_load(&configured, &top, &mut sink).await;
    assert!(empty_load.is_err(), "the empty path names no data");
    let bound = manifest.dyn_contains_key(&configured, &top).await.unwrap();
    assert!(!bound, "the empty path binds nothing");

    // The taxonomy survives erasure: the seam variants still match structurally.
    let separator = p("/");
    let meta = content_type();
    let reserved = manifest
        .dyn_insert(&root, separator.clone(), *file, meta.as_ref())
        .await
        .err()
        .and_then(|e| e.as_reserved().map(|r| r.path().clone()));
    assert_eq!(reserved, Some(separator), "erased Reserved");
    let mut sink = MemWriteAt::new();
    let absent = p("absent.html");
    let missing = manifest.dyn_load(&root, &absent, &mut sink).await.err();
    assert!(missing.is_some_and(|e| e.is_not_found()), "erased NotFound");
}

#[test]
fn both_formats_round_trip_through_one_erased_handle() {
    run(async {
        let (raw, store) = stores();
        let data = payload();
        let file = save_file(&raw, &data).await;

        let nodes: Nodes = NodeLoadSaver::new(Arc::clone(&raw));
        let trie: Box<dyn ErasedManifest> = Box::new(
            MantarayManifest::<_, _, DEFAULT_BODY_SIZE>::new(nodes, store.clone()),
        );
        let trie_root = trie.dyn_empty().await.unwrap();
        let kv: Box<dyn ErasedManifest> = Box::new(Database::<_>::plain(store.clone()));
        let kv_root = kv.dyn_empty().await.unwrap();

        exercise(trie.as_ref(), &trie_root, &file, &data).await;
        exercise(kv.as_ref(), &kv_root, &file, &data).await;
    });
}

/// The erased site config lands in each format's native root slot.
#[test]
fn the_site_config_lands_in_each_format_native_slot() {
    run(async {
        let (raw, store) = stores();
        let config = SiteConfig::new().with_index_document(p("index.html"));

        // The trie keeps the site documents on its root path node, which
        // binds no entry.
        let nodes: Nodes = NodeLoadSaver::new(Arc::clone(&raw));
        let trie = MantarayManifest::<_, _, DEFAULT_BODY_SIZE>::new(nodes.clone(), store.clone());
        let trie_root = trie.dyn_empty().await.unwrap();
        let root = trie.dyn_set_site_config(&trie_root, config.clone()).await;
        let root = root.unwrap();
        let entry = TrieLookup::new(nodes)
            .get(root, metadata::ROOT_PATH.as_bytes())
            .await
            .unwrap()
            .expect("the root path node carries the site documents");
        let reference = entry.reference();
        assert!(reference.is_none(), "the site-config node binds no entry");
        let stored = entry.metadata().get(metadata::WEBSITE_INDEX_DOCUMENT);
        assert_eq!(stored, Some(&"index.html".to_owned()));
        // That slot is the trie's own, not a directory: a top-level listing must
        // not surface it as a child.
        let listing = trie.dyn_dir(&root, &ManifestPath::default()).await.unwrap();
        assert!(listing.is_empty(), "listed as {:?}", listing.entries());

        // The key-value database keeps them in the root's typed metadata,
        // which its website view reads.
        let kv = Database::<_>::plain(store.clone());
        let kv_root = kv.dyn_empty().await.unwrap();
        let root = kv.dyn_set_site_config(&kv_root, config).await.unwrap();
        let reader: KeyLookup<_> = KeyLookup::new(&store);
        let website = reader.website(&root).await.unwrap();
        assert_eq!(website.index(), Some(&b"index.html"[..]));
    });
}

/// The erased walk visits every content path in order and stops on `Break`.
#[test]
fn the_erased_walk_visits_in_order_and_breaks_early() {
    use std::ops::ControlFlow;

    run(async {
        let (raw, store) = stores();
        let file = save_file(&raw, &payload()).await;
        let ops = |paths: &[&str]| -> Vec<ManifestOp<ChunkRef, Box<dyn MetadataSource>>> {
            paths
                .iter()
                .map(|path| ManifestOp::Insert {
                    path: p(path),
                    reference: file,
                    meta: Box::new(()) as Box<dyn MetadataSource>,
                })
                .collect()
        };

        let nodes: Nodes = NodeLoadSaver::new(Arc::clone(&raw));
        let trie: Box<dyn ErasedManifest> = Box::new(
            MantarayManifest::<_, _, DEFAULT_BODY_SIZE>::new(nodes, store.clone()),
        );
        let kv: Box<dyn ErasedManifest> = Box::new(Database::<_>::plain(store.clone()));

        for (manifest, format) in [(&trie, "trie"), (&kv, "kv")] {
            let base = manifest.dyn_empty().await.unwrap();
            let root = manifest.dyn_apply(&base, ops(&["b/c", "a", "d"])).await;
            let root = root.unwrap();

            // The full walk arrives in path order, entries intact.
            let mut seen = Vec::new();
            manifest
                .dyn_for_each(&root, &mut |path: ManifestPath, entry: MapEntry| {
                    assert!(entry.is_loadable(), "{format}: {path:?}");
                    seen.push(String::from_utf8_lossy(path.as_bytes()).into_owned());
                    ControlFlow::Continue(())
                })
                .await
                .unwrap();
            assert_eq!(seen, ["a", "b/c", "d"], "{format}");

            // A break stops the walk after the first entry.
            let mut first = Vec::new();
            manifest
                .dyn_for_each(&root, &mut |path: ManifestPath, _| {
                    first.push(String::from_utf8_lossy(path.as_bytes()).into_owned());
                    ControlFlow::Break(())
                })
                .await
                .unwrap();
            assert_eq!(first, ["a"], "{format} breaks early");
        }
    });
}
