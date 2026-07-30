//! The vocabulary gate: a map is named like a map, and structured content is
//! not.
//!
//! Every verb below is called by name, so this test is a compile-time pin as
//! much as a runtime one: renaming `get`, `insert` or `remove` on a map
//! surface, or `save`, `load` or `publish` on structured content, fails to
//! build here before any behaviour changes.
//!
//! The three map surfaces are the `Manifest` seam, the `nectar-ldb` database
//! and the mantaray trie. The two content surfaces are `nectar-file` and
//! `nectar-feeds`. The chunk store keeps `put` on purpose: its key is the hash
//! of the value, so a caller supplies no key to insert under.
//!
//! This file pins the verbs that must be present. The absence of the content
//! verbs on a map surface is pinned next door, in `manifest/absence.rs`.

use std::sync::Arc;

use alloy_signer_local::PrivateKeySigner;
use bytes::Bytes;
use nectar_feeds::{Feed, Publisher, Topic};
use nectar_file::{File, MemSink, Policy};
use nectar_ldb::{
    Builder, Database, Entry, Key, KeyId, LdbManifest, Metadata, Plaintext, Reader as LdbReader, V1,
};
use nectar_loadsave::NodeLoadSaver;
use nectar_manifest::{
    Manifest, ManifestPath, MapCursor, MapEntry, MapView, MapWriter, MetadataView, WellKnownKey,
};
use nectar_mantaray::{ManifestEditor, MantarayManifest, Reader as MantarayReader};
use nectar_primitives::store::{ContentGet, MemoryStore};
use nectar_primitives::{
    ChunkAddress, ChunkRef, DEFAULT_BODY_SIZE, EntryRef, SingleOwnerOnlyChunkSet, StandardChunkSet,
};
use nectar_testing::run;

/// The chunk store, shared: `MemoryStore` clones its contents, so every handle
/// in one test has to reach the same map.
type Raw = Arc<MemoryStore<StandardChunkSet>>;
type Store = ContentGet<Raw>;

/// A reference standing in for a file root; no chunk behind it is read.
fn reference(byte: u8) -> ChunkRef {
    ChunkRef::new(ChunkAddress::new([byte; 32]))
}

/// Drive one manifest through the whole map vocabulary and hand back the root
/// the last op produced.
///
/// The seam's read verbs come off the root-bound view, the write verbs off the
/// base-bound writer, and the one-shots off the manifest itself.
async fn map_vocabulary<M: Manifest<ChunkRef>>(manifest: &M, base: &ChunkRef) -> ChunkRef {
    let index = ManifestPath::from("index.html");
    let logo = ManifestPath::from("img/logo.png");

    // edit + insert + remove + commit: a write yields a new root.
    let stale = ManifestPath::from("stale.txt");
    let root = {
        let mut writer = manifest.edit(base);
        writer.insert(index.clone(), reference(1));
        writer
            .insert(logo.clone(), reference(2))
            .meta(Default::default());
        writer.insert(stale.clone(), reference(4));
        writer.remove(stale.clone());
        writer.commit().await.unwrap()
    };
    assert_ne!(&root, base, "the batch produced a new root");

    // at + get + contains_key + metadata + dir + iter + range + load.
    let view = manifest.at(&root);
    assert_eq!(
        view.get(&index).await.unwrap(),
        Some(MapEntry::Reference(reference(1)))
    );
    assert!(view.contains_key(&index).await.unwrap());
    assert!(
        !view.contains_key(&stale).await.unwrap(),
        "the staged removal took the path out of the committed root"
    );
    let _ = view.metadata(&index).await.unwrap();
    assert_eq!(view.dir(&ManifestPath::default()).await.unwrap().len(), 2);

    let mut walked = Vec::new();
    let mut cursor = view.iter().await.unwrap();
    while let Some((path, _)) = cursor.next().await.unwrap() {
        walked.push(path);
    }
    assert_eq!(walked, vec![logo.clone(), index.clone()]);

    let mut ranged = Vec::new();
    let mut cursor = view
        .range(index.clone()..ManifestPath::from("z"))
        .await
        .unwrap();
    while let Some((path, _)) = cursor.next().await.unwrap() {
        ranged.push(path);
    }
    assert_eq!(ranged, vec![index.clone()]);

    // floor: the greatest bound path at or below the probe, which every format
    // answers, natively or by walking the range.
    let indexed = Some((index.clone(), MapEntry::Reference(reference(1))));
    assert_eq!(
        view.floor(&ManifestPath::from("index.zzz")).await.unwrap(),
        indexed,
        "the floor of a path past the last one is the last one"
    );
    assert_eq!(
        view.floor(&index).await.unwrap(),
        indexed,
        "a bound path is its own floor"
    );
    assert_eq!(
        view.floor(&ManifestPath::from("img/logo.pnh"))
            .await
            .unwrap()
            .map(|(path, _)| path),
        Some(logo.clone()),
        "the floor stops below the greater path"
    );
    assert!(
        view.floor(&ManifestPath::from("aaa"))
            .await
            .unwrap()
            .is_none(),
        "no path is at or below the probe"
    );

    // A load of a reference that names no stored chunk fails, which is the
    // verb reaching storage rather than a naming answer.
    let mut sink = MemSink::new();
    assert!(view.load(&index, &mut sink).await.is_err());

    // The store-level one-shots: insert and remove, each a new root.
    let extra = ManifestPath::from("extra.txt");
    let added = manifest
        .insert(&root, extra.clone(), reference(3))
        .await
        .unwrap();
    assert!(manifest.at(&added).contains_key(&extra).await.unwrap());
    let pruned = manifest.remove(&added, extra.clone()).await.unwrap();
    assert!(!manifest.at(&pruned).contains_key(&extra).await.unwrap());
    pruned
}

/// A bare insert replaces the whole binding on either format: the metadata the
/// path carried is cleared, exactly as a `HashMap` insert replaces the value it
/// holds under a key.
async fn insert_replaces_the_whole_binding<M>(manifest: &M, base: &ChunkRef)
where
    M: Manifest<ChunkRef>,
    M::Metadata: Clone + PartialEq + std::fmt::Debug,
{
    let page = ManifestPath::from("page.html");
    let meta = manifest
        .metadata_from_view(&MetadataView::new().with(WellKnownKey::ContentType, "text/html"))
        .unwrap();
    assert_ne!(
        meta,
        M::Metadata::default(),
        "the format carries a content type"
    );

    let carried = {
        let mut writer = manifest.edit(base);
        writer.insert(page.clone(), reference(1)).meta(meta.clone());
        writer.commit().await.unwrap()
    };
    assert_eq!(manifest.at(&carried).metadata(&page).await.unwrap(), meta);

    // The bare re-insert: a new reference, and no metadata to carry.
    let bare = {
        let mut writer = manifest.edit(&carried);
        writer.insert(page.clone(), reference(2));
        writer.commit().await.unwrap()
    };
    let view = manifest.at(&bare);
    assert_eq!(
        view.get(&page).await.unwrap(),
        Some(MapEntry::Reference(reference(2))),
        "the reference is replaced"
    );
    assert_eq!(
        view.metadata(&page).await.unwrap(),
        M::Metadata::default(),
        "a bare insert clears the metadata the path carried"
    );

    // The one-shot is the same write, so it clears the same way.
    let one_shot = manifest
        .insert(&carried, page.clone(), reference(2))
        .await
        .unwrap();
    assert_eq!(one_shot, bare, "the one-shot is an edit of one insert");
}

#[test]
fn both_manifest_formats_speak_the_map_vocabulary() {
    run(async {
        let raw: Raw = Arc::new(MemoryStore::new());
        let store: Store = ContentGet::new(Arc::clone(&raw));

        let nodes = NodeLoadSaver::new(Arc::clone(&raw));
        let editor: ManifestEditor<_> = ManifestEditor::new(nodes.clone());
        let (empty, _) = editor.commit().await.unwrap();
        let trie = MantarayManifest::<_, _, DEFAULT_BODY_SIZE>::new(nodes, store.clone());
        let trie_empty = ChunkRef::new(empty);
        let _ = map_vocabulary(&trie, &trie_empty).await;
        insert_replaces_the_whole_binding(&trie, &trie_empty).await;

        let builder: Builder<V1> = Builder::new();
        let empty = *builder.build(&store, &Plaintext).await.unwrap().root();
        let kv = LdbManifest::plain(store.clone());
        let _ = map_vocabulary(&kv, &empty).await;
        insert_replaces_the_whole_binding(&kv, &empty).await;
    });
}

#[test]
fn the_key_value_database_speaks_the_map_vocabulary() {
    run(async {
        let store: Store = ContentGet::new(Arc::new(MemoryStore::new()));
        let builder: Builder<V1> = Builder::new();
        let empty = *builder.build(&store, &Plaintext).await.unwrap().root();
        let db: Database<_> = Database::plain(&store);
        let key = Key::from(&b"index.html"[..]);

        // edit + insert + meta + remove + commit.
        let root = {
            let mut editor = db.edit(&empty);
            editor
                .insert(key.clone(), Entry::from(reference(1)))
                .meta(Metadata::new(KeyId::ContentType, Bytes::from_static(b"text/html")).unwrap());
            editor.remove(Key::from(&b"absent"[..]));
            editor.commit().await.unwrap()
        };

        // at + get + contains_key + metadata + floor + iter + range + dir.
        let view = db.at(&root);
        assert_eq!(
            view.get(&key).await.unwrap(),
            Some(Entry::from(reference(1)))
        );
        assert!(view.contains_key(&key).await.unwrap());
        assert!(view.metadata(&key).await.unwrap().is_some());
        assert!(view.floor(&Key::from(&b"z"[..])).await.unwrap().is_some());
        assert!(view.iter().await.unwrap().next().await.unwrap().is_some());
        assert!(
            view.range(key.clone()..Key::from(&b"z"[..]))
                .await
                .unwrap()
                .next()
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            view.dir(&Key::empty())
                .await
                .unwrap()
                .next()
                .await
                .unwrap()
                .is_some()
        );

        // The one-shots, and the reader the handle is built on.
        let added = db
            .insert(&root, Key::from(&b"extra"[..]), Entry::from(reference(2)))
            .await
            .unwrap();
        let pruned = db.remove(&added, Key::from(&b"extra"[..])).await.unwrap();
        assert_eq!(pruned, root, "a removal is history-independent");
        let reader: LdbReader<_> = LdbReader::new(&store);
        assert!(reader.contains_key(&root, &key).await.unwrap());
    });
}

#[test]
fn the_trie_editor_speaks_the_map_vocabulary() {
    run(async {
        let raw: Raw = Arc::new(MemoryStore::new());
        let nodes = NodeLoadSaver::new(Arc::clone(&raw));

        let mut editor: ManifestEditor<_> = ManifestEditor::new(nodes.clone());
        editor.insert("index.html", ChunkAddress::from([1u8; 32]));
        editor.insert("img/logo.png", ChunkAddress::from([2u8; 32]));
        editor.remove("img/logo.png");
        let (root, nodes) = editor.commit().await.unwrap();

        let reader = MantarayReader::new(nodes);
        assert!(reader.get(root, b"index.html").await.unwrap().is_some());
        assert!(reader.get(root, b"img/logo.png").await.unwrap().is_none());
    });
}

#[test]
fn structured_content_keeps_its_own_verbs() {
    run(async {
        let raw: Raw = Arc::new(MemoryStore::new());
        let data: Vec<u8> = (0..5_000u32)
            .map(|i| u8::try_from(i % 251).unwrap())
            .collect();

        // A file saves and loads; it is content, not a map, so it names no key
        // to insert under.
        let root = File::<_, DEFAULT_BODY_SIZE>::new(Arc::clone(&raw), Policy::DEFAULT)
            .save(&data[..])
            .await
            .unwrap();
        let mut sink = MemSink::new();
        let store: Store = ContentGet::new(Arc::clone(&raw));
        File::<_, DEFAULT_BODY_SIZE>::new(store, Policy::DEFAULT)
            .load(EntryRef::Plain(ChunkRef::new(root)), &mut sink)
            .await
            .unwrap();
        assert_eq!(sink.as_ref(), &data[..]);

        // A feed publishes; it is an append-only log, not a map.
        let signer = PrivateKeySigner::random();
        let soc_store: MemoryStore<SingleOwnerOnlyChunkSet> = MemoryStore::new();
        let feed: Feed = Feed::new(Topic::from_label("vocabulary"), signer.address());
        let mut publisher = Publisher::new(feed, &soc_store, signer);
        publisher.publish(&b"first"[..]).await.unwrap();
        assert!(
            publisher.next_index().is_some(),
            "a publish advances the sequence"
        );
    });
}
