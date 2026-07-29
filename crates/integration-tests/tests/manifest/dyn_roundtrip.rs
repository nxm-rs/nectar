//! Both manifest formats behind one `Box<dyn DynManifest>`.
//!
//! The point of the seam is that a consumer holding an erased handle cannot
//! tell the trie from the key-value database: the same list, load and apply
//! calls drive both, and the metadata a caller writes through the erased view
//! lands in each format's own native slot.

use nectar_file::split::collect_into;
use nectar_file::{MemSink, Plain, PutWindow};
use nectar_ldb::{Builder, LdbManifest, Plaintext, Reader as LdbReader, V1};
use nectar_loadsave::NodeLoadSaver;
use nectar_manifest::{
    DynManifest, ManifestMetadata, ManifestOp, ManifestPath, MetadataView, WellKnownKey,
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
    (0..20_000u32).map(|i| u8::try_from(i % 251).unwrap()).collect()
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
        .dyn_apply(base, vec![
            ManifestOp::Put {
                path: ManifestPath::from("index.html"),
                reference: *file,
                meta: content_type(),
            },
            ManifestOp::Put {
                path: ManifestPath::from("img/logo.png"),
                reference: *file,
                meta: Box::new(()),
            },
        ])
        .await
        .unwrap();
    assert_ne!(&root, base, "the batch produced a new root");

    // One level: the deeper path collapses into a directory entry, in path
    // order, and neither format fetches the referenced data to list it.
    let listing = manifest
        .dyn_list(&root, &ManifestPath::root())
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
        .dyn_list(&root, &ManifestPath::from("img/"))
        .await
        .unwrap();
    let nested_paths: Vec<&[u8]> = nested
        .entries()
        .iter()
        .map(|entry| entry.path().as_bytes())
        .collect();
    assert_eq!(nested_paths, vec![&b"img/logo.png"[..]]);

    // A load joins the whole chunk tree the entry names into the sink.
    let mut sink = MemSink::new();
    manifest
        .dyn_load(&root, &ManifestPath::from("index.html"), &mut sink)
        .await
        .unwrap();
    assert_eq!(sink.as_ref(), data);

    // A removal is the same batch vocabulary, and the removed path stops
    // resolving.
    let pruned = manifest
        .dyn_apply(&root, vec![ManifestOp::Remove {
            path: ManifestPath::from("index.html"),
        }])
        .await
        .unwrap();
    let listing = manifest
        .dyn_list(&pruned, &ManifestPath::root())
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
            collect_into::<_, Plain, DEFAULT_BODY_SIZE>(&raw, PutWindow::DEFAULT, &data)
                .await
                .unwrap(),
        );

        let (nodes, trie_root) = mantaray(&raw).await;
        let trie: Box<dyn DynManifest> =
            Box::new(MantarayManifest::<_, _, DEFAULT_BODY_SIZE>::new(
                nodes,
                store.clone(),
            ));
        let kv_root = ldb(&store).await;
        let kv: Box<dyn DynManifest> = Box::new(LdbManifest::plain(store.clone()));

        exercise(trie.as_ref(), &trie_root, &file, &data).await;
        exercise(kv.as_ref(), &kv_root, &file, &data).await;
    });
}

#[test]
fn root_scope_metadata_lands_in_each_format_native_slot() {
    run(async {
        let raw: Raw = Arc::new(MemoryStore::new());
        let store = ContentGet::new(Arc::clone(&raw));
        let data = payload();
        let file = ChunkRef::new(
            collect_into::<_, Plain, DEFAULT_BODY_SIZE>(&raw, PutWindow::DEFAULT, &data)
                .await
                .unwrap(),
        );
        let index: Box<dyn ManifestMetadata> =
            Box::new(MetadataView::new().with(WellKnownKey::IndexDocument, "index.html"));

        // The trie keeps the site documents on its root path node.
        let (nodes, trie_root) = mantaray(&raw).await;
        let trie = MantarayManifest::<_, _, DEFAULT_BODY_SIZE>::new(nodes.clone(), store.clone());
        let root = trie
            .dyn_apply(&trie_root, vec![ManifestOp::Put {
                path: ManifestPath::root(),
                reference: file,
                meta: index,
            }])
            .await
            .unwrap();
        let entry = MantarayReader::new(nodes)
            .get(root, metadata::ROOT_PATH.as_bytes())
            .await
            .unwrap()
            .expect("the root path node carries the site documents");
        assert_eq!(
            entry.metadata().get(metadata::WEBSITE_INDEX_DOCUMENT),
            Some(&"index.html".to_owned())
        );
        // That slot is the trie's own, not a directory: a root listing must
        // not surface it as a child.
        let listing = trie.dyn_list(&root, &ManifestPath::root()).await.unwrap();
        assert!(
            listing.is_empty(),
            "the root metadata slot listed as {:?}",
            listing.entries()
        );

        // The key-value database keeps them in the root's typed metadata,
        // which its website view reads.
        let index: Box<dyn ManifestMetadata> =
            Box::new(MetadataView::new().with(WellKnownKey::IndexDocument, "index.html"));
        let kv_root = ldb(&store).await;
        let kv = LdbManifest::plain(store.clone());
        let root = kv
            .dyn_apply(&kv_root, vec![ManifestOp::Put {
                path: ManifestPath::root(),
                reference: file,
                meta: index,
            }])
            .await
            .unwrap();
        let reader: LdbReader<_> = LdbReader::new(&store);
        let website = reader.website(&root).await.unwrap();
        assert_eq!(website.index(), Some(&b"index.html"[..]));
    });
}
