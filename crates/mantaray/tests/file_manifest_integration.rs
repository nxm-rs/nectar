//! Integration test: unified store for file splitting and manifest creation.

use nectar_mantaray::{Entry, Manifest};
use nectar_primitives::bmt::DEFAULT_BODY_SIZE;
use nectar_primitives::file::{join, split_reader};
use nectar_primitives::store::MemorySink;

type Store = MemorySink<DEFAULT_BODY_SIZE>;

/// Split files into a MemorySink, create a manifest in the same store,
/// then verify lookup and round-trip.
#[test]
fn unified_store_workflow() {
    let store = Store::new();

    // Split files into the store
    let (root_a, store) = split_reader(b"file A contents".as_slice(), 15, store).unwrap();
    let (root_b, store) = split_reader(b"file B contents".as_slice(), 15, store).unwrap();

    let files_chunk_count = store.len();
    assert!(files_chunk_count > 0);

    // Create manifest in the same store
    let mut manifest = Manifest::new(store, false);
    manifest
        .add(
            "a.txt",
            Entry::new(root_a.as_bytes().to_vec()).with_content_type("text/plain"),
        )
        .unwrap();
    manifest
        .add(
            "b.txt",
            Entry::new(root_b.as_bytes().to_vec()).with_content_type("text/plain"),
        )
        .unwrap();

    manifest.save().unwrap();
    let root_ref = manifest.reference().to_vec();

    // Now the store contains both file chunks AND manifest trie nodes
    let (_, store) = manifest.into_parts();
    assert!(store.len() > files_chunk_count);

    // Reload manifest from the store
    let mut manifest2 = Manifest::new_manifest_reference(&root_ref, store.clone());

    // Verify lookup
    let entry_a = manifest2.lookup("a.txt").unwrap();
    assert_eq!(entry_a.reference, root_a.as_bytes());
    assert_eq!(entry_a.content_type(), Some("text/plain"));

    let entry_b = manifest2.lookup("b.txt").unwrap();
    assert_eq!(entry_b.reference, root_b.as_bytes());

    // Verify file data round-trip through the same store
    let recovered_a = join(&store, root_a).unwrap();
    assert_eq!(recovered_a, b"file A contents");

    let recovered_b = join(&store, root_b).unwrap();
    assert_eq!(recovered_b, b"file B contents");
}

/// Test that entries() collects all items from a saved manifest.
#[test]
fn entries_round_trip() {
    let store = Store::new();
    let mut manifest = Manifest::new(store, false);

    let paths = &[
        "index.html",
        "css/style.css",
        "js/app.js",
        "img/logo.png",
    ];

    for &path in paths {
        let mut v = path.as_bytes().to_vec();
        v.resize(32, 0);
        manifest
            .add(path, Entry::new(v).with_content_type("application/octet-stream"))
            .unwrap();
    }

    manifest.save().unwrap();
    let root_ref = manifest.reference().to_vec();

    let (_, store) = manifest.into_parts();
    let mut manifest2 = Manifest::new_manifest_reference(&root_ref, store);

    let entries = manifest2.entries().unwrap();
    assert_eq!(entries.len(), paths.len());

    for &path in paths {
        let entry = entries
            .iter()
            .find(|e| e.path == path.as_bytes())
            .unwrap_or_else(|| panic!("missing entry for {path}"));
        assert_eq!(
            entry.content_type(),
            Some("application/octet-stream"),
            "wrong content type for {path}"
        );
    }
}

/// Iterator over a manifest yields the same entries as entries().
#[test]
fn iterator_matches_entries() {
    let store = Store::new();
    let mut manifest = Manifest::new(store, false);

    let paths = &["a/1", "a/2", "b/1", "c"];
    for &path in paths {
        let mut v = path.as_bytes().to_vec();
        v.resize(32, 0);
        manifest.add(path, Entry::new(v)).unwrap();
    }

    let entries_via_method = manifest.entries().unwrap();

    let mut entries_via_iter = Vec::new();
    let mut iter = manifest.iter();
    while let Some(result) = iter.next() {
        entries_via_iter.push(result.unwrap());
    }

    assert_eq!(entries_via_method.len(), entries_via_iter.len());

    for method_entry in &entries_via_method {
        assert!(
            entries_via_iter
                .iter()
                .any(|e| e.path == method_entry.path && e.reference == method_entry.reference),
            "entry {:?} from entries() not found in iterator",
            String::from_utf8_lossy(&method_entry.path)
        );
    }
}

/// Ergonomic API: write_file/read_file, Manifest::open, Entry convenience methods.
#[test]
fn ergonomic_api_workflow() {
    use nectar_mantaray::DefaultMemorySink;
    use nectar_primitives::file::{ChunkGetExt, SplitExt};

    // Split files using SplitExt (no turbofish)
    let (root_a, store) = b"file A contents"
        .as_slice()
        .split_into(DefaultMemorySink::new())
        .unwrap();
    let (root_b, store) = b"file B contents"
        .as_slice()
        .split_into(store)
        .unwrap();

    // Create manifest in the same store
    let mut manifest = Manifest::new(store, false);
    manifest
        .add(
            "a.txt",
            Entry::new(root_a.as_bytes().to_vec()).with_content_type("text/plain"),
        )
        .unwrap();
    manifest
        .add(
            "b.txt",
            Entry::new(root_b.as_bytes().to_vec()).with_content_type("text/plain"),
        )
        .unwrap();

    manifest.save().unwrap();
    let root_addr =
        nectar_primitives::ChunkAddress::from_slice(manifest.reference()).unwrap();

    let (_, store) = manifest.into_parts();

    // Reopen using Manifest::open (takes ChunkAddress)
    let mut manifest2 = Manifest::open(root_addr, store);

    // Iterate entries using convenience methods
    let entries: Vec<_> = manifest2
        .iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(entries.len(), 2);

    for entry in &entries {
        // path_str() convenience
        let path = entry.path_str();
        assert!(path == "a.txt" || path == "b.txt");

        // content_type() from metadata
        assert_eq!(entry.content_type(), Some("text/plain"));

        // address() extracts ChunkAddress from reference
        let addr = entry.address().expect("32-byte reference yields address");

        // Use manifest.store() to read the file
        let data = manifest2.store().read_file(addr).unwrap();
        if path == "a.txt" {
            assert_eq!(data, b"file A contents");
        } else {
            assert_eq!(data, b"file B contents");
        }
    }
}
