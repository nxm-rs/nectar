//! The node persistence seam over both formats.
//!
//! The unit of transfer differs (a whole image for the trie, a decoded node
//! for the database), so the laws are stated once per adapter: a save round
//! trips through a load at both reference widths, and a traced load agrees
//! with a plain one over a non-empty root-first address list.

#![allow(clippy::as_conversions, clippy::unwrap_used)]

use std::sync::Arc;

use nectar_ldb::{Builder, Database, Entry, ForkTable, Key, Node, NodeRef, Prefix, Seal, V1};
use nectar_manifest::{NodeLoader, NodeSaver};
use nectar_mantaray::NodeLoadSaver;
use nectar_primitives::{ChunkAddress, ChunkRef, EntryRef};
use nectar_testing::run;

mod common;
use common::{Store, stores};

/// A node image spanning several chunks, so the trie's traced load has more
/// than a root to report.
fn image() -> Vec<u8> {
    (0..20_000u32).map(|i| (i % 251) as u8).collect()
}

/// A reference standing in for a bound value.
fn entry(byte: u8) -> Entry {
    Entry::from(ChunkRef::new(ChunkAddress::new([byte; 32])))
}

/// A node small enough to seal into one chunk, which is all a per-node save
/// can express; spilling is the builder's, not the seam's.
fn plain_node<R: NodeRef>() -> Node<V1, R> {
    let mut forks = ForkTable::new();
    for byte in 0u8..4 {
        forks
            .insert(
                Prefix::try_from(&[byte][..]).unwrap(),
                entry(byte).into(),
                None,
            )
            .unwrap();
    }
    Node::new(None, forks)
}

/// A 256-fork root built natively, so its stored shape spills across segment
/// chunks and no single image exists for it.
async fn spilled_root<R: NodeRef, K: Seal<R>>(store: &Store, seal: &K) -> R {
    let mut builder = Builder::<V1>::new();
    for byte in 0u8..=255 {
        builder.insert(Key::from(&[byte][..]), entry(byte), None);
    }
    builder.build(store, seal).await.unwrap().root().clone()
}

#[test]
fn the_trie_adapter_round_trips_a_node_image() {
    run(async {
        let (raw, _) = stores();
        let nodes = NodeLoadSaver::new(Arc::clone(&raw));
        let data = image();

        let root = NodeSaver::<[u8], ChunkRef>::save(&nodes, data.as_slice())
            .await
            .unwrap();
        let reference = EntryRef::from(root);
        assert_eq!(
            NodeLoader::<Vec<u8>>::load(&nodes, &reference)
                .await
                .unwrap(),
            data
        );

        let (traced, addresses) = nodes.load_traced(&reference).await.unwrap();
        assert_eq!(traced, data);
        assert_eq!(addresses.first(), Some(reference.address()));
        assert!(
            addresses.len() > 1,
            "a multi-chunk image reports every chunk it occupies"
        );
    });
}

#[cfg(feature = "test-encryption")]
#[test]
fn the_trie_adapter_round_trips_at_the_encrypted_width() {
    use nectar_primitives::EncryptedChunkRef;

    run(async {
        let (raw, _) = stores();
        let nodes = NodeLoadSaver::new(Arc::clone(&raw));
        let data = image();

        let root = NodeSaver::<[u8], EncryptedChunkRef>::save(&nodes, data.as_slice())
            .await
            .unwrap();
        let reference = EntryRef::from(root);
        assert_eq!(
            NodeLoader::<Vec<u8>>::load(&nodes, &reference)
                .await
                .unwrap(),
            data
        );

        let (traced, addresses) = nodes.load_traced(&reference).await.unwrap();
        assert_eq!(traced, data);
        assert_eq!(addresses.first(), Some(reference.address()));
        assert!(addresses.len() > 1);
    });
}

#[test]
fn the_database_round_trips_a_decoded_node() {
    run(async {
        let (_, store) = stores();
        let db: Database<_> = Database::plain(store);
        let node = plain_node::<ChunkRef>();

        let root: ChunkRef = NodeSaver::save(&db, &node).await.unwrap();
        let reference = EntryRef::from(root);
        let loaded: Node<V1, ChunkRef> = db.load(&reference).await.unwrap();
        assert_eq!(loaded, node);

        let (traced, addresses) = NodeLoader::<Node<V1, ChunkRef>>::load_traced(&db, &reference)
            .await
            .unwrap();
        assert_eq!(traced, loaded);
        assert_eq!(addresses, vec![*reference.address()]);
    });
}

#[test]
fn the_database_traces_a_spilled_node_root_first() {
    run(async {
        let (_, store) = stores();
        let root: ChunkRef = spilled_root(&store, &nectar_ldb::Plaintext).await;
        let db: Database<_> = Database::plain(store);
        let reference = EntryRef::from(root);

        let loaded: Node<V1, ChunkRef> = db.load(&reference).await.unwrap();
        assert_eq!(loaded.forks().len(), 256);

        let (traced, addresses) = NodeLoader::<Node<V1, ChunkRef>>::load_traced(&db, &reference)
            .await
            .unwrap();
        assert_eq!(traced, loaded);
        assert_eq!(addresses.first(), Some(reference.address()));
        assert!(
            addresses.len() > 1,
            "a spilled node reports its segment chunks after the root"
        );
    });
}

/// The runtime read reference is the one thing the typed native path checks at
/// compile time, so a width mismatch has to surface as an error rather than a
/// mis-parse.
#[cfg(feature = "test-encryption")]
#[test]
fn the_database_rejects_a_reference_of_the_wrong_width() {
    use nectar_primitives::{EncryptedChunkRef, EncryptionKey};

    run(async {
        let (_, store) = stores();
        let db: Database<_> = Database::plain(store);
        let root: ChunkRef = NodeSaver::save(&db, &plain_node::<ChunkRef>())
            .await
            .unwrap();
        let widened = EntryRef::from(EncryptedChunkRef::new(
            *root.address(),
            EncryptionKey::from([0u8; EncryptionKey::SIZE]),
        ));

        let error = NodeLoader::<Node<V1, ChunkRef>>::load(&db, &widened)
            .await
            .unwrap_err();
        assert!(matches!(error, nectar_ldb::StoreError::Width(_)));
    });
}

/// The seal is the write-side secret, so an encrypted database mints encrypted
/// references from the same seam call.
#[cfg(feature = "test-encryption")]
#[test]
fn the_database_round_trips_at_the_encrypted_width() {
    use nectar_ldb::Encrypted as EncryptedSeal;
    use nectar_primitives::EncryptedChunkRef;

    run(async {
        let (_, store) = stores();
        let seal: EncryptedSeal<'_, V1> = EncryptedSeal::new(b"a node seam secret");
        let spilled: EncryptedChunkRef = spilled_root(&store, &seal).await;
        let db: Database<_, _> = Database::new(store, seal);

        let node = plain_node::<EncryptedChunkRef>();
        let root: EncryptedChunkRef = NodeSaver::save(&db, &node).await.unwrap();
        let reference = EntryRef::from(root);
        let loaded: Node<V1, EncryptedChunkRef> = db.load(&reference).await.unwrap();
        assert_eq!(loaded, node);

        let reference = EntryRef::from(spilled);
        let (traced, addresses) =
            NodeLoader::<Node<V1, EncryptedChunkRef>>::load_traced(&db, &reference)
                .await
                .unwrap();
        assert_eq!(traced.forks().len(), 256);
        assert_eq!(addresses.first(), Some(reference.address()));
        assert!(addresses.len() > 1);
    });
}

/// A trie fork may reference a node of either width, so the read side takes
/// the runtime union rather than a width parameter.
#[test]
fn the_null_loader_answers_not_found_at_any_unit() {
    use nectar_primitives::store::NullLoader;

    run(async {
        let reference = EntryRef::from(ChunkRef::new(ChunkAddress::new([4; 32])));
        assert!(
            NodeLoader::<Vec<u8>>::load(&NullLoader, &reference)
                .await
                .is_err()
        );
        assert!(
            NodeLoader::<Node<V1, ChunkRef>>::load(&NullLoader, &reference)
                .await
                .is_err()
        );
    });
}
