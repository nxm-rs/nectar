//! Integration test: unified store for file splitting and manifest creation.

// Bench, example, and integration-test code: unwraps, direct indexing,
// casts, and assertions are setup and illustration, not shipped surface.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::panic,
    clippy::panic_in_result_fn,
    clippy::as_conversions,
    clippy::missing_panics_doc
)]
use nectar_mantaray::PlainManifest;
use nectar_primitives::StandardChunkSet;
use nectar_primitives::chunk::ChunkAddress;
use nectar_primitives::file::{ChunkPutExt, join};
use nectar_primitives::store::MemoryStore;
use nectar_testing::run;

type Store = MemoryStore<StandardChunkSet>;

/// Split files into a MemoryStore, create a manifest in the same store,
/// then verify lookup and round-trip.
#[test]
fn unified_store_workflow() {
    run(async {
        let store = Store::new();

        // Seed file chunks into the store.
        let root_a = store.write_file(b"file A contents".to_vec()).await.unwrap();
        let root_b = store.write_file(b"file B contents".to_vec()).await.unwrap();

        let files_chunk_count = store.len();
        assert!(files_chunk_count > 0);

        // Create manifest in the same store
        let mut manifest = PlainManifest::new(store);
        manifest
            .add_with_metadata(
                "a.txt",
                root_a,
                [("Content-Type".to_string(), "text/plain".to_string())].into(),
            )
            .await
            .unwrap();
        manifest
            .add_with_metadata(
                "b.txt",
                root_b,
                [("Content-Type".to_string(), "text/plain".to_string())].into(),
            )
            .await
            .unwrap();

        let root_ref = manifest.save().await.unwrap();

        // Now the store contains both file chunks AND manifest trie nodes
        let (_, store) = manifest.into_parts();
        assert!(store.len() > files_chunk_count);

        // Reload manifest from the store
        let manifest2 = PlainManifest::open(root_ref, store.clone());

        // Verify lookup
        let entry_a = manifest2.get("a.txt").await.unwrap().unwrap();
        assert_eq!(entry_a.address(), Some(&root_a));
        assert_eq!(entry_a.content_type(), Some("text/plain"));

        let entry_b = manifest2.get("b.txt").await.unwrap().unwrap();
        assert_eq!(entry_b.address(), Some(&root_b));

        // Verify file data round-trip through the same store
        let recovered_a = join(store.clone(), root_a).await.unwrap();
        assert_eq!(recovered_a, b"file A contents");

        let recovered_b = join(store, root_b).await.unwrap();
        assert_eq!(recovered_b, b"file B contents");
    })
}

/// Test that entries() collects all items from a saved manifest.
#[test]
fn entries_round_trip() {
    run(async {
        let store = Store::new();
        let mut manifest = PlainManifest::new(store);

        let paths = &["index.html", "css/style.css", "js/app.js", "img/logo.png"];

        for &path in paths {
            let addr = make_addr(path);
            manifest
                .add_with_metadata(
                    path,
                    addr,
                    [(
                        "Content-Type".to_string(),
                        "application/octet-stream".to_string(),
                    )]
                    .into(),
                )
                .await
                .unwrap();
        }

        let root_ref = manifest.save().await.unwrap();

        let (_, store) = manifest.into_parts();
        let manifest2 = PlainManifest::open(root_ref, store);

        let entries = manifest2.entries().await.unwrap();
        assert_eq!(entries.len(), paths.len());

        for &path in paths {
            let entry = entries
                .iter()
                .find(|e| e.path() == path.as_bytes())
                .unwrap_or_else(|| panic!("missing entry for {path}"));
            assert_eq!(
                entry.content_type(),
                Some("application/octet-stream"),
                "wrong content type for {path}"
            );
        }
    })
}

/// Iterator over a manifest yields all added entries.
#[test]
fn iterator_yields_all_entries() {
    run(async {
        let store = Store::new();
        let mut manifest = PlainManifest::new(store);

        let paths = &["a/1", "a/2", "b/1", "c"];
        for &path in paths {
            let addr = make_addr(path);
            manifest.add(path, addr).await.unwrap();
        }

        let entries = {
            let mut out = Vec::new();
            let mut iter = manifest.iter();
            while let Some(item) = iter.next().await {
                out.push(item.unwrap());
            }
            out
        };
        assert_eq!(entries.len(), paths.len());

        for &path in paths {
            assert!(
                entries.iter().any(|e| e.path() == path.as_bytes()),
                "entry {path:?} not found in iterator",
            );
        }
    })
}

/// Ergonomic API: write_file/read_file, PlainManifest::open, Entry convenience methods.
#[test]
fn ergonomic_api_workflow() {
    run(async {
        use nectar_mantaray::DefaultMemoryStore;
        use nectar_primitives::file::{ChunkGetExt, ChunkPutExt};

        let store = DefaultMemoryStore::new();

        // Seed file chunks.
        let root_a = store.write_file(b"file A contents".to_vec()).await.unwrap();
        let root_b = store.write_file(b"file B contents".to_vec()).await.unwrap();

        // Create manifest in the same store
        let mut manifest = PlainManifest::new(store);
        manifest
            .add_with_metadata(
                "a.txt",
                root_a,
                [("Content-Type".to_string(), "text/plain".to_string())].into(),
            )
            .await
            .unwrap();
        manifest
            .add_with_metadata(
                "b.txt",
                root_b,
                [("Content-Type".to_string(), "text/plain".to_string())].into(),
            )
            .await
            .unwrap();

        let root_addr = manifest.save().await.unwrap();

        let (_, store) = manifest.into_parts();

        // Reopen using PlainManifest::open (takes ChunkAddress)
        let mut manifest2 = PlainManifest::open(root_addr, store);

        // Iterate entries using convenience methods
        let entries = {
            let mut out = Vec::new();
            let mut iter = manifest2.iter();
            while let Some(item) = iter.next().await {
                out.push(item.unwrap());
            }
            out
        };

        assert_eq!(entries.len(), 2);

        for entry in &entries {
            // path_str() convenience
            let path = entry.path_str().unwrap();
            assert!(path == "a.txt" || path == "b.txt");

            // content_type() from metadata
            assert_eq!(entry.content_type(), Some("text/plain"));

            // address() extracts ChunkAddress from reference
            let addr = entry.address().expect("32-byte reference yields address");

            let data = manifest2.store().clone().read_file(*addr).await.unwrap();
            if path == "a.txt" {
                assert_eq!(data, b"file A contents");
            } else {
                assert_eq!(data, b"file B contents");
            }
        }
    })
}

/// Fused seam: `ManifestBuilder::put_file` splits, stores, and stages in one
/// mode; `Manifest::read` looks up and joins, collapsing the manual
/// write_file + add and get + join dance the earlier tests spell out.
#[test]
fn fused_put_file_read_workflow() {
    run(async {
        use nectar_mantaray::ManifestBuilder;

        let mut builder: ManifestBuilder<Store> = ManifestBuilder::new(Store::new());
        builder
            .put_file("a.txt", b"file A contents".to_vec())
            .await
            .unwrap();
        builder
            .put_file("nested/b.txt", b"file B contents".to_vec())
            .await
            .unwrap();

        let (root, manifest) = builder.save().await.unwrap();

        // Read back through the handle the save returned.
        assert_eq!(
            manifest.read("a.txt").await.unwrap().unwrap(),
            b"file A contents"
        );
        assert_eq!(
            manifest.read("nested/b.txt").await.unwrap().unwrap(),
            b"file B contents"
        );
        assert!(manifest.read("missing").await.unwrap().is_none());

        // Reopen from storage and read again, exercising the lazy-load path.
        let (_, store) = manifest.into_parts();
        let reopened = PlainManifest::open(root, store);
        assert_eq!(
            reopened.read("nested/b.txt").await.unwrap().unwrap(),
            b"file B contents"
        );
    })
}

/// Create a ChunkAddress from a string, right-padded with zeroes.
fn make_addr(s: &str) -> ChunkAddress {
    let bytes = s.as_bytes();
    let mut buf = [0u8; 32];
    let len = bytes.len().min(32);
    buf[..len].copy_from_slice(&bytes[..len]);
    ChunkAddress::from(buf)
}
