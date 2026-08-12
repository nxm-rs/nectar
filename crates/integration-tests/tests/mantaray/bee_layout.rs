//! The mantaray v0.2 layout, asserted against what the reference client writes.
//!
//! `bee_vectors.rs` pins the bytes; this file pins the shape they
//! carry:
//!
//! - A content path is stored bare and verbatim. Nothing is prepended.
//! - The site-level documents are metadata on the `"/"` node, a one-byte fork
//!   under the structural root. That node binds no entry, so it is a
//!   metadata-only value.
//! - The structural root is the empty path. It carries no site config, and the
//!   manifest seam never exposes it.
//!
//! The reference sites are `bee/pkg/manifest/manifest.go` (`RootPath = "/"`),
//! `bee/pkg/api/bzz.go` (`m.Add(RootPath, NewEntry(swarm.ZeroAddress, meta))`
//! and the three `Lookup(RootPath)` reads), and
//! `bee/pkg/manifest/mantaray.go` (the path passed straight to `trie.Add`).

use std::sync::Arc;

use nectar_manifest::{Batch, Manifest, ManifestCursor, ManifestPath, ManifestView};
use nectar_mantaray::{ManifestEditor, MantarayManifest, NodeLoadSaver, Reader, metadata};
use nectar_primitives::store::{ContentGet, MemoryStore};
use nectar_primitives::{ChunkAddress, ChunkRef, DEFAULT_BODY_SIZE, StandardChunkSet};
use nectar_testing::run;

type Raw = Arc<MemoryStore<StandardChunkSet>>;

/// A reference standing in for a file root; no chunk behind it is read.
fn reference(byte: u8) -> ChunkRef {
    ChunkRef::new(ChunkAddress::new([byte; 32]))
}

/// A path as text, for a readable assertion message.
fn text(path: &ManifestPath) -> String {
    String::from_utf8(path.as_bytes().to_vec()).unwrap()
}

/// A website manifest written through the seam stores content bare and the site
/// documents as metadata on the `"/"` node, with no entry bound there.
#[test]
fn a_website_manifest_matches_the_reference_layout() {
    run(async {
        let raw: Raw = Arc::new(MemoryStore::new());
        let nodes = NodeLoadSaver::new(Arc::clone(&raw));
        let data = ContentGet::new(Arc::clone(&raw));

        let editor: ManifestEditor<_> = ManifestEditor::new(nodes.clone());
        let (empty, _) = editor.commit().await.unwrap();
        let trie = MantarayManifest::<_, _, DEFAULT_BODY_SIZE>::new(nodes.clone(), data);

        let root = {
            let mut batch = Batch::new();
            batch.insert(ManifestPath::from("index.html"), reference(1));
            batch.insert(ManifestPath::from("css/style.css"), reference(2));
            batch
                .set_index_document(ManifestPath::from("index.html"))
                .set_error_document(ManifestPath::from("404.html"));
            trie.apply(ChunkRef::new(empty), batch).await.unwrap()
        };

        // The trie is read below the seam, so this asserts the stored image.
        let reader = Reader::new(nodes);

        // Content keys are the bare bytes, byte for byte.
        for (key, byte) in [(&b"index.html"[..], 1u8), (&b"css/style.css"[..], 2)] {
            let entry = reader
                .get(*root.address(), key)
                .await
                .unwrap()
                .expect("the bare content key is bound");
            assert_eq!(
                entry.reference().map(|r| *r.address()),
                Some(*reference(byte).address()),
                "the content key {:?} is stored verbatim",
                String::from_utf8_lossy(key)
            );
            assert!(
                entry.metadata().is_empty(),
                "a content entry carries no site metadata"
            );
        }

        // Nothing is stored under a rooted spelling of a content key.
        assert!(
            reader
                .get(*root.address(), b"/index.html")
                .await
                .unwrap()
                .is_none(),
            "no content key is rooted at the separator"
        );

        // The site documents live on the "/" node, which binds no entry.
        let site = reader
            .get(*root.address(), metadata::ROOT_PATH.as_bytes())
            .await
            .unwrap()
            .expect("the site-config node exists");
        assert!(
            site.reference().is_none(),
            "the site-config node binds the zero address, which is no entry"
        );
        assert_eq!(
            site.metadata()
                .get(metadata::WEBSITE_INDEX_DOCUMENT)
                .map(String::as_str),
            Some("index.html"),
            "the index document is metadata on the \"/\" node"
        );
        assert_eq!(
            site.metadata()
                .get(metadata::WEBSITE_ERROR_DOCUMENT)
                .map(String::as_str),
            Some("404.html"),
            "so is the error document"
        );

        // The structural root carries nothing.
        let structural = reader.get(*root.address(), b"").await.unwrap();
        assert!(structural.is_none(), "the empty path binds nothing at all");

        // Read back through the seam: content plus two options.
        let view = trie.at(root);
        let mut walked = Vec::new();
        let mut cursor = view.iter().await.unwrap();
        while let Some((path, _)) = cursor.next().await.unwrap() {
            walked.push(text(&path));
        }
        assert_eq!(
            walked,
            ["css/style.css", "index.html"],
            "the seam walks content keys alone"
        );
        assert_eq!(
            view.index_document().await.unwrap().as_ref().map(text),
            Some(String::from("index.html")),
            "the seam reads the index document as an option"
        );
        assert_eq!(
            view.error_document().await.unwrap().as_ref().map(text),
            Some(String::from("404.html")),
            "and the error document too"
        );
    });
}

/// The editor's own verbs write the same layout as the seam's setters.
#[test]
fn the_editor_verbs_write_the_same_site_config_node() {
    run(async {
        let raw: Raw = Arc::new(MemoryStore::new());
        let nodes = NodeLoadSaver::new(Arc::clone(&raw));
        let data = ContentGet::new(Arc::clone(&raw));

        // Below the seam: the trie's own verbs.
        let mut editor: ManifestEditor<_> = ManifestEditor::new(nodes.clone());
        editor.insert("index.html", *reference(1).address());
        editor.set_index_document("index.html");
        editor.set_error_document("404.html");
        let (below, _) = editor.commit().await.unwrap();

        // Above the seam: the option-typed setters.
        let editor: ManifestEditor<_> = ManifestEditor::new(nodes.clone());
        let (empty, _) = editor.commit().await.unwrap();
        let trie = MantarayManifest::<_, _, DEFAULT_BODY_SIZE>::new(nodes, data);
        let above = {
            let mut batch = Batch::new();
            batch.insert(ManifestPath::from("index.html"), reference(1));
            batch
                .set_index_document(ManifestPath::from("index.html"))
                .set_error_document(ManifestPath::from("404.html"));
            trie.apply(ChunkRef::new(empty), batch).await.unwrap()
        };

        assert_eq!(
            *above.address(),
            below,
            "the seam and the trie's own verbs produce one image"
        );
    });
}
