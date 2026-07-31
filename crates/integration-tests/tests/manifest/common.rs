//! Shared fixtures for the manifest seam tests.
#![allow(dead_code)]

use std::sync::Arc;

use nectar_file::{File, Policy};
use nectar_ldb::Database;
use nectar_loadsave::NodeLoadSaver;
use nectar_manifest::{Manifest, ManifestPath};
use nectar_mantaray::MantarayManifest;
use nectar_primitives::store::{ContentGet, MemoryStore};
use nectar_primitives::{ChunkAddress, ChunkRef, DEFAULT_BODY_SIZE, StandardChunkSet};

/// Shared chunk store: `MemoryStore` clones its contents, so every handle in
/// one test has to reach the same map.
pub type Raw = Arc<MemoryStore<StandardChunkSet>>;
pub type Store = ContentGet<Raw>;
pub type Nodes = NodeLoadSaver<Raw>;
pub type Trie = MantarayManifest<Nodes, Store, DEFAULT_BODY_SIZE>;
pub type Kv = Database<Store>;

/// A fresh raw store and its content-addressed read handle.
pub fn stores() -> (Raw, Store) {
    let raw: Raw = Arc::new(MemoryStore::new());
    let store = ContentGet::new(Arc::clone(&raw));
    (raw, store)
}

/// Both formats over one raw store, each at its freshly persisted empty root.
pub async fn both_formats(raw: &Raw, store: &Store) -> ((Trie, ChunkRef), (Kv, ChunkRef)) {
    let trie: Trie = MantarayManifest::new(NodeLoadSaver::new(Arc::clone(raw)), store.clone());
    let trie_empty = trie.empty().await.unwrap();
    let kv = Database::<_>::plain(store.clone());
    let kv_empty = kv.empty().await.unwrap();
    ((trie, trie_empty), (kv, kv_empty))
}

/// `data` saved as a plain file tree, as the reference an entry binds.
pub async fn save_file(raw: &Raw, data: &[u8]) -> ChunkRef {
    ChunkRef::new(
        File::<_, DEFAULT_BODY_SIZE>::new(raw, Policy::DEFAULT)
            .save(data)
            .await
            .unwrap(),
    )
}

/// A reference standing in for a file root; no chunk behind it is read.
pub fn reference(byte: u8) -> ChunkRef {
    ChunkRef::new(ChunkAddress::new([byte; 32]))
}

/// A path as text, for a readable assertion message.
pub fn text(path: &ManifestPath) -> String {
    String::from_utf8(path.as_bytes().to_vec()).unwrap()
}
