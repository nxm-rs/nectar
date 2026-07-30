//! Both manifest formats behind one `Box<dyn DynManifest>`.
//!
//! The point of the seam is that a consumer holding an erased handle cannot
//! tell the trie from the key-value database: the same list, load and apply
//! calls drive both, and the metadata a caller writes through the erased view
//! lands in each format's own native slot.

use nectar_file::{File, MemSink, Policy};
use nectar_ldb::{Builder, LdbManifest, Plaintext, Reader as LdbReader, V1};
use nectar_loadsave::NodeLoadSaver;
use nectar_manifest::{
    DynManifest, ManifestMetadata, ManifestOp, ManifestPath, MetadataView, SiteConfig, WellKnownKey,
};
use nectar_mantaray::{ManifestEditor, MantarayManifest, Reader as MantarayReader, metadata};
use nectar_primitives::store::{ContentGet, MemoryStore};
use nectar_primitives::{ChunkRef, DEFAULT_BODY_SIZE, StandardChunkSet};
use nectar_testing::run;
use std::sync::Arc;

/// The chunk store, shared: `MemoryStore` clones its contents, so every
/// handle in one test has to reach the same map.
type Raw = Arc<MemoryStore<StandardChunkSet>>;
type Store = ContentGet<Raw>;
type Nodes = NodeLoadSaver<Raw>;

/// The file bytes every manifest entry in this test points at.
fn payload() -> Vec<u8> {
    (0..20_000u32)
        .map(|i| u8::try_from(i % 251).unwrap())
        .collect()
}

/// Content type written through the erased metadata view.
const CONTENT_TYPE: &str = "text/html";

/// Erased metadata carrying one content type.
fn content_type() -> Box<dyn ManifestMetadata> {
    Box::new(MetadataView::new().with(WellKnownKey::ContentType, CONTENT_TYPE))
}

/// Drive one erased manifest through the whole seam: apply a batch, list the
/// root and a subdirectory, load an entry's data, then remove it.
async fn exercise(manifest: &dyn DynManifest, base: &ChunkRef, file: &ChunkRef, data: &[u8]) {
    let root = manifest
        .dyn_apply(
            base,
            vec![
                ManifestOp::Insert {
                    path: ManifestPath::from("index.html"),
                    reference: *file,
                    meta: content_type(),
                },
                ManifestOp::Insert {
                    path: ManifestPath::from("img/logo.png"),
                    reference: *file,
                    meta: Box::new(()),
                },
            ],
        )
        .await
        .unwrap();
    assert_ne!(&root, base, "the batch produced a new root");

    // One level: the deeper path collapses into a directory entry, in path
    // order, and neither format fetches the referenced data to list it.
    let listing = manifest
        .dyn_dir(&root, &ManifestPath::default())
        .await
        .unwrap();
    let paths: Vec<&[u8]> = listing
        .entries()
        .iter()
        .map(|entry| entry.path().as_bytes())
        .collect();
    assert_eq!(paths, vec![&b"img/"[..], &b"index.html"[..]]);
    assert!(listing.entries()[0].is_dir());
    assert_eq!(listing.entries()[1].reference(), Some(file));

    // The subdirectory lists its one file under its full path.
    let nested = manifest
        .dyn_dir(&root, &ManifestPath::from("img/"))
        .await
        .unwrap();
    let nested_paths: Vec<&[u8]> = nested
        .entries()
        .iter()
        .map(|entry| entry.path().as_bytes())
        .collect();
    assert_eq!(nested_paths, vec![&b"img/logo.png"[..]]);

    // The erased floor is the same ordered-map read on either format: the
    // greatest bound path at or below the probe.
    let (path, entry) = manifest
        .dyn_floor(&root, &ManifestPath::from("index.zzz"))
        .await
        .unwrap()
        .expect("a path at or below the probe");
    assert_eq!(path.as_bytes(), b"index.html");
    assert_eq!(entry.reference(), Some(file));
    assert!(
        manifest
            .dyn_floor(&root, &ManifestPath::from("aaa"))
            .await
            .unwrap()
            .is_none(),
        "no path is at or below the probe"
    );

    // A load joins the whole chunk tree the entry names into the sink.
    let mut sink = MemSink::new();
    manifest
        .dyn_load(&root, &ManifestPath::from("index.html"), &mut sink)
        .await
        .unwrap();
    assert_eq!(sink.as_ref(), data);

    // A removal is the same map vocabulary, and the removed path stops
    // resolving.
    let pruned = manifest
        .dyn_apply(
            &root,
            vec![ManifestOp::Remove {
                path: ManifestPath::from("index.html"),
            }],
        )
        .await
        .unwrap();
    let listing = manifest
        .dyn_dir(&pruned, &ManifestPath::default())
        .await
        .unwrap();
    let paths: Vec<&[u8]> = listing
        .entries()
        .iter()
        .map(|entry| entry.path().as_bytes())
        .collect();
    assert_eq!(paths, vec![&b"img/"[..]]);

    let mut sink = MemSink::new();
    assert!(
        manifest
            .dyn_load(&pruned, &ManifestPath::from("index.html"), &mut sink)
            .await
            .is_err(),
        "a removed path names no data"
    );

    // The manifest's own configuration is not a path, so it crosses the erased
    // seam as a value: what is set reads back, and clearing it restores the root
    // that declared nothing.
    assert_eq!(
        manifest.dyn_site_config(base).await.unwrap(),
        SiteConfig::new(),
        "an unconfigured manifest declares neither document"
    );
    let configured = manifest
        .dyn_set_site_config(
            &root,
            SiteConfig::new()
                .with_index_document(ManifestPath::from("index.html"))
                .with_error_document(ManifestPath::from("404.html")),
        )
        .await
        .unwrap();
    assert_eq!(
        manifest.dyn_site_config(&configured).await.unwrap(),
        SiteConfig::new()
            .with_index_document(ManifestPath::from("index.html"))
            .with_error_document(ManifestPath::from("404.html")),
        "the documents read back through the erased seam"
    );
    assert_eq!(
        manifest
            .dyn_set_site_config(&configured, SiteConfig::new())
            .await
            .unwrap(),
        root,
        "clearing the documents restores the content-only root"
    );

    // The empty path is a listing prefix, not a key: nothing is bound there and
    // nothing loads from it.
    let mut sink = MemSink::new();
    assert!(
        manifest
            .dyn_load(&configured, &ManifestPath::default(), &mut sink)
            .await
            .is_err(),
        "the empty path names no data"
    );
    assert!(
        !manifest
            .dyn_contains_key(&configured, &ManifestPath::default())
            .await
            .unwrap(),
        "the empty path binds nothing"
    );
}

/// An empty trie manifest, and the seams it is read and written through.
async fn mantaray(raw: &Raw) -> (Nodes, ChunkRef) {
    let nodes = NodeLoadSaver::new(Arc::clone(raw));
    let editor: ManifestEditor<_> = ManifestEditor::new(nodes.clone());
    let (root, _) = editor.commit().await.unwrap();
    (nodes, ChunkRef::new(root))
}

/// An empty key-value manifest.
async fn ldb(store: &Store) -> ChunkRef {
    let builder: Builder<V1> = Builder::new();
    *builder.build(store, &Plaintext).await.unwrap().root()
}

#[test]
fn both_formats_round_trip_through_one_erased_handle() {
    run(async {
        let raw: Raw = Arc::new(MemoryStore::new());
        let store = ContentGet::new(Arc::clone(&raw));
        let data = payload();
        let file = ChunkRef::new(
            File::<_, DEFAULT_BODY_SIZE>::new(&raw, Policy::DEFAULT)
                .save(&data[..])
                .await
                .unwrap(),
        );

        let (nodes, trie_root) = mantaray(&raw).await;
        let trie: Box<dyn DynManifest> = Box::new(
            MantarayManifest::<_, _, DEFAULT_BODY_SIZE>::new(nodes, store.clone()),
        );
        let kv_root = ldb(&store).await;
        let kv: Box<dyn DynManifest> = Box::new(LdbManifest::plain(store.clone()));

        exercise(trie.as_ref(), &trie_root, &file, &data).await;
        exercise(kv.as_ref(), &kv_root, &file, &data).await;
    });
}

/// The erased site config lands in each format's native root slot, so a reader
/// of either format below the seam finds it where its own convention says.
#[test]
fn the_site_config_lands_in_each_format_native_slot() {
    run(async {
        let raw: Raw = Arc::new(MemoryStore::new());
        let store = ContentGet::new(Arc::clone(&raw));
        let config = SiteConfig::new().with_index_document(ManifestPath::from("index.html"));

        // The trie keeps the site documents on its root path node, which binds
        // no entry: the reference client's zero address is the empty slot.
        let (nodes, trie_root) = mantaray(&raw).await;
        let trie = MantarayManifest::<_, _, DEFAULT_BODY_SIZE>::new(nodes.clone(), store.clone());
        let root = trie
            .dyn_set_site_config(&trie_root, config.clone())
            .await
            .unwrap();
        let entry = MantarayReader::new(nodes)
            .get(root, metadata::ROOT_PATH.as_bytes())
            .await
            .unwrap()
            .expect("the root path node carries the site documents");
        assert!(
            entry.reference().is_none(),
            "the site-config node binds no entry"
        );
        assert_eq!(
            entry.metadata().get(metadata::WEBSITE_INDEX_DOCUMENT),
            Some(&"index.html".to_owned())
        );
        // That slot is the trie's own, not a directory: a top-level listing must
        // not surface it as a child.
        let listing = trie.dyn_dir(&root, &ManifestPath::default()).await.unwrap();
        assert!(
            listing.is_empty(),
            "the site-config node listed as {:?}",
            listing.entries()
        );

        // The key-value database keeps them in the root's typed metadata,
        // which its website view reads.
        let kv_root = ldb(&store).await;
        let kv = LdbManifest::plain(store.clone());
        let root = kv.dyn_set_site_config(&kv_root, config).await.unwrap();
        let reader: LdbReader<_> = LdbReader::new(&store);
        let website = reader.website(&root).await.unwrap();
        assert_eq!(website.index(), Some(&b"index.html"[..]));
    });
}
