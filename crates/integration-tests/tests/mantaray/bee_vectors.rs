//! Fixed root vectors from bee, the reference client.
//!
//! Each root is the exact `swarm.Address` bee's own mantaray produces for the
//! same plain manifest, obfuscated with zeros. nectar must land on the same
//! root byte for byte: a bare content key, the website index document on the
//! `"/"` node, and a fat root node whose image spans several chunks through the
//! file pipeline.
//!
//! Generated against `github.com/ethersphere/bee/v2`'s
//! `manifest.NewMantarayManifest(ls, false)` with an in-memory store, a plain
//! pipeline and `redundancy.NONE`.

use std::sync::Arc;

use nectar_loadsave::NodeLoadSaver;
use nectar_manifest::{Batch, Manifest, ManifestPath};
use nectar_mantaray::{ManifestEditor, MantarayManifest};
use nectar_primitives::store::{ContentGet, MemoryStore};
use nectar_primitives::{ChunkAddress, ChunkRef, DEFAULT_BODY_SIZE, StandardChunkSet};
use nectar_testing::run;

type Raw = Arc<MemoryStore<StandardChunkSet>>;

/// A reference whose first byte is `i` and rest zero, matching the generator.
fn reference(i: u8) -> ChunkRef {
    let mut bytes = [0u8; 32];
    bytes[0] = i;
    ChunkRef::new(ChunkAddress::new(bytes))
}

/// A root address parsed from its 64-hex-char vector.
fn address(hex: &str) -> ChunkAddress {
    let mut out = [0u8; 32];
    for (i, pair) in hex.as_bytes().chunks(2).enumerate() {
        out[i] = u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap();
    }
    ChunkAddress::new(out)
}

/// Store `entries` as a plain manifest, with an optional index document, and
/// return the root.
async fn root(entries: &[(ManifestPath, ChunkRef)], index_document: Option<&str>) -> ChunkRef {
    let raw: Raw = Arc::new(MemoryStore::new());
    let nodes = NodeLoadSaver::new(Arc::clone(&raw));
    let data = ContentGet::new(Arc::clone(&raw));
    let editor: ManifestEditor<_> = ManifestEditor::new(nodes.clone());
    let (empty, _) = editor.commit().await.unwrap();
    let trie = MantarayManifest::<_, _, DEFAULT_BODY_SIZE>::new(nodes, data);

    let mut batch = Batch::new();
    for (path, reference) in entries {
        batch.insert(path.clone(), *reference);
    }
    if let Some(document) = index_document {
        batch.set_index_document(ManifestPath::from(document));
    }
    trie.apply(ChunkRef::new(empty), batch).await.unwrap()
}

#[test]
fn one_bare_content_key_matches_bee() {
    run(async {
        let root = root(&[(ManifestPath::from("index.html"), reference(7))], None).await;
        assert_eq!(
            *root.address(),
            address("135e08fe3256ffb32a2abeadc66d335568372ee4ebed35cdf9b40fdcbb31263a"),
        );
    });
}

#[test]
fn a_website_index_document_matches_bee() {
    run(async {
        let root = root(
            &[(ManifestPath::from("index.html"), reference(7))],
            Some("index.html"),
        )
        .await;
        assert_eq!(
            *root.address(),
            address("71679aa2e389c9ae87aac980f73b34c15843db48824e7b2957f7e11f6dc18c44"),
        );
    });
}

#[test]
fn a_multi_chunk_root_node_matches_bee() {
    run(async {
        let mut entries = Vec::new();
        for i in 1u16..=200 {
            let byte = i as u8;
            if byte == b'/' {
                continue;
            }
            let mut key = vec![byte];
            key.extend_from_slice(b"-content-file");
            entries.push((ManifestPath::from(key.as_slice()), reference(byte)));
        }
        let root = root(&entries, None).await;
        assert_eq!(
            *root.address(),
            address("73351dd770c0d3205d46f2954f99502112f77b91db56fed72f245f4331724010"),
        );
    });
}
