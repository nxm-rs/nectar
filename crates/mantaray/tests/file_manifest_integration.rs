//! Integration test: unified store for file splitting and manifest creation.

use futures::executor::block_on;
use nectar_mantaray::PlainManifest;
use nectar_primitives::bmt::DEFAULT_BODY_SIZE;
use nectar_primitives::chunk::ChunkAddress;
use nectar_primitives::file::{ChunkPutExt, join};
use nectar_primitives::store::MemoryStore;

type Store = MemoryStore<DEFAULT_BODY_SIZE>;

/// Split files into a MemoryStore, create a manifest in the same store,
/// then verify lookup and round-trip.
#[test]
fn unified_store_workflow() {
    let store = Store::new();

    // Seed file chunks into the store.
    let root_a = block_on(store.write_file(b"file A contents".to_vec())).unwrap();
    let root_b = block_on(store.write_file(b"file B contents".to_vec())).unwrap();

    let files_chunk_count = store.len();
    assert!(files_chunk_count > 0);

    // Create manifest in the same store
    let mut manifest = PlainManifest::new(store);
    block_on(manifest.add_with_metadata(
        "a.txt",
        root_a,
        [("Content-Type".to_string(), "text/plain".to_string())].into(),
    ))
    .unwrap();
    block_on(manifest.add_with_metadata(
        "b.txt",
        root_b,
        [("Content-Type".to_string(), "text/plain".to_string())].into(),
    ))
    .unwrap();

    let root_ref = block_on(manifest.save()).unwrap();

    // Now the store contains both file chunks AND manifest trie nodes
    let (_, store) = manifest.into_parts();
    assert!(store.len() > files_chunk_count);

    // Reload manifest from the store
    let mut manifest2 = PlainManifest::open(root_ref, store.clone());

    // Verify lookup
    let entry_a = block_on(manifest2.lookup("a.txt")).unwrap();
    assert_eq!(entry_a.address(), Some(&root_a));
    assert_eq!(entry_a.content_type(), Some("text/plain"));

    let entry_b = block_on(manifest2.lookup("b.txt")).unwrap();
    assert_eq!(entry_b.address(), Some(&root_b));

    // Verify file data round-trip through the same store
    let recovered_a = block_on(join(store.clone(), root_a)).unwrap();
    assert_eq!(recovered_a, b"file A contents");

    let recovered_b = block_on(join(store, root_b)).unwrap();
    assert_eq!(recovered_b, b"file B contents");
}

/// Test that entries() collects all items from a saved manifest.
#[test]
fn entries_round_trip() {
    let store = Store::new();
    let mut manifest = PlainManifest::new(store);

    let paths = &["index.html", "css/style.css", "js/app.js", "img/logo.png"];

    for &path in paths {
        let addr = make_addr(path);
        block_on(
            manifest.add_with_metadata(
                path,
                addr,
                [(
                    "Content-Type".to_string(),
                    "application/octet-stream".to_string(),
                )]
                .into(),
            ),
        )
        .unwrap();
    }

    let root_ref = block_on(manifest.save()).unwrap();

    let (_, store) = manifest.into_parts();
    let mut manifest2 = PlainManifest::open(root_ref, store);

    let entries = block_on(manifest2.entries()).unwrap();
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
}

/// Iterator over a manifest yields all added entries.
#[test]
fn iterator_yields_all_entries() {
    let store = Store::new();
    let mut manifest = PlainManifest::new(store);

    let paths = &["a/1", "a/2", "b/1", "c"];
    for &path in paths {
        let addr = make_addr(path);
        block_on(manifest.add(path, addr)).unwrap();
    }

    let entries = block_on(async {
        let mut out = Vec::new();
        let mut iter = manifest.iter();
        while let Some(item) = iter.next().await {
            out.push(item.unwrap());
        }
        out
    });
    assert_eq!(entries.len(), paths.len());

    for &path in paths {
        assert!(
            entries.iter().any(|e| e.path() == path.as_bytes()),
            "entry {path:?} not found in iterator",
        );
    }
}

/// Ergonomic API: write_file/read_file, PlainManifest::open, Entry convenience methods.
#[test]
fn ergonomic_api_workflow() {
    use nectar_mantaray::DefaultMemoryStore;
    use nectar_primitives::file::{ChunkGetExt, ChunkPutExt};

    let store = DefaultMemoryStore::new();

    // Seed file chunks.
    let root_a = block_on(store.write_file(b"file A contents".to_vec())).unwrap();
    let root_b = block_on(store.write_file(b"file B contents".to_vec())).unwrap();

    // Create manifest in the same store
    let mut manifest = PlainManifest::new(store);
    block_on(manifest.add_with_metadata(
        "a.txt",
        root_a,
        [("Content-Type".to_string(), "text/plain".to_string())].into(),
    ))
    .unwrap();
    block_on(manifest.add_with_metadata(
        "b.txt",
        root_b,
        [("Content-Type".to_string(), "text/plain".to_string())].into(),
    ))
    .unwrap();

    let root_addr = block_on(manifest.save()).unwrap();

    let (_, store) = manifest.into_parts();

    // Reopen using PlainManifest::open (takes ChunkAddress)
    let mut manifest2 = PlainManifest::open(root_addr, store);

    // Iterate entries using convenience methods
    let entries = block_on(async {
        let mut out = Vec::new();
        let mut iter = manifest2.iter();
        while let Some(item) = iter.next().await {
            out.push(item.unwrap());
        }
        out
    });

    assert_eq!(entries.len(), 2);

    for entry in &entries {
        // path_str() convenience
        let path = entry.path_str().unwrap();
        assert!(path == "a.txt" || path == "b.txt");

        // content_type() from metadata
        assert_eq!(entry.content_type(), Some("text/plain"));

        // address() extracts ChunkAddress from reference
        let addr = entry.address().expect("32-byte reference yields address");

        let data = block_on(manifest2.store().clone().read_file(*addr)).unwrap();
        if path == "a.txt" {
            assert_eq!(data, b"file A contents");
        } else {
            assert_eq!(data, b"file B contents");
        }
    }
}

/// Create a ChunkAddress from a string, right-padded with zeroes.
fn make_addr(s: &str) -> ChunkAddress {
    let bytes = s.as_bytes();
    let mut buf = [0u8; 32];
    let len = bytes.len().min(32);
    buf[..len].copy_from_slice(&bytes[..len]);
    ChunkAddress::from(buf)
}
