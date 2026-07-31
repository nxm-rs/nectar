//! The seam is generic over the reference width.
//!
//! Both formats are reference-generic, so the same static `Manifest` code
//! drives an encrypted manifest: the ops carry 64-byte references, the roots
//! come back encrypted, and a load joins an encrypted chunk tree.

#![cfg(feature = "encryption")]

use std::sync::Arc;

use nectar_file::{File, MemSink, Policy};
use nectar_ldb::{Database, Encrypted as EncryptedSeal, V1};
use nectar_loadsave::NodeLoadSaver;
use nectar_manifest::{
    Batch, ListEntry, Manifest, ManifestMeta, ManifestPath, ManifestView, MapEntry, MetadataView,
    WellKnownKey,
};
use nectar_mantaray::MantarayManifest;
use nectar_primitives::{DEFAULT_BODY_SIZE, EncryptedChunkRef};
use nectar_testing::run;

mod common;
use common::{Store, stores};

/// The secret an encrypted key-value database derives its keys from.
const SECRET: &[u8] = b"an encrypted database secret";

/// Insert, read back and load one entry at the encrypted reference width.
async fn exercise<M: Manifest<EncryptedChunkRef>>(
    manifest: &M,
    base: &EncryptedChunkRef,
    file: &EncryptedChunkRef,
    data: &[u8],
) {
    let meta = M::Metadata::from_source(
        &MetadataView::new().with(WellKnownKey::ContentType, "text/plain"),
    );
    let root = {
        let mut batch = Batch::new();
        batch.insert_with(ManifestPath::from("data.bin"), file.clone(), meta);
        manifest.apply(base.clone(), batch).await.unwrap()
    };

    let view = manifest.at(root.clone());
    assert_eq!(
        view.get(&ManifestPath::from("data.bin")).await.unwrap(),
        Some(MapEntry::Reference(file.clone())),
    );
    assert!(
        view.contains_key(&ManifestPath::from("data.bin"))
            .await
            .unwrap()
    );
    let listing = view.dir(&ManifestPath::default()).await.unwrap();
    assert_eq!(
        listing.entries(),
        [ListEntry::File {
            path: ManifestPath::from("data.bin"),
            reference: file.clone(),
        }]
    );

    let mut sink = MemSink::new();
    view.load(&ManifestPath::from("data.bin"), &mut sink)
        .await
        .unwrap();
    assert_eq!(sink.as_ref(), data);

    // The one-shot removal leaves the entry unreachable under its new root.
    let pruned = manifest
        .remove(root, ManifestPath::from("data.bin"))
        .await
        .unwrap();
    assert!(
        !manifest
            .at(pruned)
            .contains_key(&ManifestPath::from("data.bin"))
            .await
            .unwrap()
    );
}

#[test]
fn both_formats_drive_an_encrypted_manifest() {
    run(async {
        let (raw, store) = stores();
        let data: Vec<u8> = (0..9_000u32)
            .map(|i| u8::try_from(i % 241).unwrap())
            .collect();
        // The entry's own data is an encrypted chunk tree, so its reference
        // carries the key that opens it.
        let file = File::<_, DEFAULT_BODY_SIZE>::new(&raw, Policy::DEFAULT)
            .save_encrypted(&data[..])
            .await
            .unwrap();

        // The seam bootstrap works at the encrypted width too.
        let nodes = NodeLoadSaver::new(Arc::clone(&raw));
        let trie: MantarayManifest<_, Store, DEFAULT_BODY_SIZE> =
            MantarayManifest::new(nodes, store.clone());
        let trie_root: EncryptedChunkRef = trie.empty().await.unwrap();
        exercise(&trie, &trie_root, &file, &data).await;

        let seal: EncryptedSeal<'_, V1> = EncryptedSeal::new(SECRET);
        let kv: Database<_, _> = Database::new(store.clone(), seal);
        let kv_root: EncryptedChunkRef = kv.empty().await.unwrap();
        exercise(&kv, &kv_root, &file, &data).await;
    });
}
