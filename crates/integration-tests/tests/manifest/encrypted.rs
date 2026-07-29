//! The seam is generic over the reference width.
//!
//! Both formats are reference-generic, so the same static `Manifest` code
//! drives an encrypted manifest: the ops carry 64-byte references, the roots
//! come back encrypted, and a load joins an encrypted chunk tree.

#![cfg(feature = "encryption")]

use std::sync::Arc;

use nectar_file::{File, MemSink, Policy};
use nectar_ldb::{Builder, Encrypted as EncryptedSeal, LdbManifest, V1};
use nectar_loadsave::NodeLoadSaver;
use nectar_manifest::{ListEntry, Manifest, ManifestPath, MetadataView, WellKnownKey};
use nectar_mantaray::{ManifestEditor, MantarayManifest};
use nectar_primitives::store::{ContentGet, MemoryStore};
use nectar_primitives::{DEFAULT_BODY_SIZE, EncryptedChunkRef, StandardChunkSet};
use nectar_testing::run;

/// The chunk store, shared: `MemoryStore` clones its contents, so every handle
/// in one test has to reach the same map.
type Raw = Arc<MemoryStore<StandardChunkSet>>;
type Store = ContentGet<Raw>;

/// The secret an encrypted key-value database derives its keys from.
const SECRET: &[u8] = b"an encrypted database secret";

/// Save one entry, list it back, and load its data, all at the encrypted
/// reference width.
async fn exercise<M: Manifest<EncryptedChunkRef>>(
    manifest: &M,
    base: &EncryptedChunkRef,
    file: &EncryptedChunkRef,
    data: &[u8],
) {
    let meta = manifest
        .metadata_from_view(&MetadataView::new().with(WellKnownKey::ContentType, "text/plain"))
        .unwrap();
    let root = manifest
        .save(
            base,
            ManifestPath::from("data.bin"),
            file.clone(),
            meta,
        )
        .await
        .unwrap();

    let listing = manifest.list(&root, &ManifestPath::root()).await.unwrap();
    assert_eq!(listing.entries(), [ListEntry::File {
        path: ManifestPath::from("data.bin"),
        reference: file.clone(),
    }]);

    let mut sink = MemSink::new();
    manifest
        .load(&root, &ManifestPath::from("data.bin"), &mut sink)
        .await
        .unwrap();
    assert_eq!(sink.as_ref(), data);
}

#[test]
fn both_formats_drive_an_encrypted_manifest() {
    run(async {
        let raw: Raw = Arc::new(MemoryStore::new());
        let store = ContentGet::new(Arc::clone(&raw));
        let data: Vec<u8> = (0..9_000u32)
            .map(|i| u8::try_from(i % 241).unwrap())
            .collect();
        // The entry's own data is an encrypted chunk tree, so its reference
        // carries the key that opens it.
        let file = File::<_, DEFAULT_BODY_SIZE>::new(&raw, Policy::DEFAULT)
            .save_encrypted(&data[..])
            .await
            .unwrap();

        let nodes = NodeLoadSaver::new(Arc::clone(&raw));
        let editor: ManifestEditor<_, EncryptedChunkRef> =
            ManifestEditor::new_encrypted(nodes.clone());
        let (trie_root, _) = editor.commit().await.unwrap();
        let trie: MantarayManifest<_, Store, DEFAULT_BODY_SIZE> =
            MantarayManifest::new(nodes, store.clone());
        exercise(&trie, &trie_root, &file, &data).await;

        let seal: EncryptedSeal<'_, V1> = EncryptedSeal::new(SECRET);
        let builder: Builder<V1> = Builder::new();
        let kv_root = builder.build(&store, &seal).await.unwrap().root().clone();
        let kv = LdbManifest::new(store.clone(), seal);
        exercise(&kv, &kv_root, &file, &data).await;
    });
}
