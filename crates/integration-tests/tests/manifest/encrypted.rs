//! The seam is generic over the reference width.
//!
//! Both formats are reference-generic, so the same static `Manifest` code
//! drives an encrypted manifest: the ops carry 64-byte references, the roots
//! come back encrypted, and a load joins an encrypted chunk tree.

#![cfg(feature = "encryption")]

use std::sync::Arc;

use nectar_file::{File, MemSink, Policy};
use nectar_ldb::{Database, Encrypted as EncryptedSeal, V1};
use nectar_manifest::{
    Batch, ListEntry, Manifest, ManifestMeta, ManifestPath, ManifestView, MapEntry, MetadataView,
    WellKnownKey,
};
use nectar_mantaray::{MantarayManifest, NodeLoadSaver};
use nectar_primitives::{DEFAULT_BODY_SIZE, EncryptedChunkRef};
use nectar_testing::run;

mod common;
use common::{Store, p, stores};

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
    let bin = p("data.bin");
    let root = {
        let mut batch = Batch::new();
        batch.insert_with(bin.clone(), file.clone(), meta);
        manifest.apply(base.clone(), batch).await.unwrap()
    };

    let view = manifest.at(root.clone());
    let got = view.get(&bin).await.unwrap();
    assert_eq!(got, Some(MapEntry::Reference(file.clone())));
    assert!(view.contains_key(&bin).await.unwrap());
    let listing = view.dir(&ManifestPath::default()).await.unwrap();
    let want = [ListEntry::File {
        path: bin.clone(),
        reference: file.clone(),
    }];
    assert_eq!(listing.entries(), want);

    let mut sink = MemSink::new();
    view.load(&bin, &mut sink).await.unwrap();
    assert_eq!(sink.as_ref(), data);

    // The one-shot removal leaves the entry unreachable under its new root.
    let pruned = manifest.remove(root, bin.clone()).await.unwrap();
    let still = manifest.at(pruned).contains_key(&bin).await.unwrap();
    assert!(!still);
}

#[test]
fn both_formats_drive_an_encrypted_manifest() {
    run(async {
        let (raw, store) = stores();
        let data: Vec<u8> = (0..9_000u32).map(|i| (i % 241) as u8).collect();
        // The entry's own data is an encrypted chunk tree, so its reference
        // carries the key that opens it.
        let saver = File::<_, DEFAULT_BODY_SIZE>::new(&raw, Policy::DEFAULT);
        let file = saver.save_encrypted(&data[..]).await.unwrap();

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
