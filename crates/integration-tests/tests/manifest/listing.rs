//! Both formats list one directory level the same way, and the same way a
//! model of the key set does.
//!
//! A listing is where the two formats are furthest apart internally: the
//! key-value database seeks past each named subtree, while the trie walks the
//! whole subtree under the prefix and collapses it in the adapter. The seam is
//! only worth holding if those two arrive at the same answer, so the answer is
//! pinned against a model rather than against either implementation.

use std::collections::BTreeSet;
use std::sync::Arc;

use nectar_file::{File, Policy};
use nectar_ldb::{Builder, LdbManifest, Plaintext, V1};
use nectar_loadsave::NodeLoadSaver;
use nectar_manifest::{DynManifest, ManifestMetadata, ManifestOp, ManifestPath};
use nectar_mantaray::{ManifestEditor, MantarayManifest};
use nectar_primitives::store::{ContentGet, MemoryStore};
use nectar_primitives::{ChunkRef, DEFAULT_BODY_SIZE, StandardChunkSet};
use nectar_testing::run;

/// The chunk store, shared: `MemoryStore` clones its contents, so every handle
/// in one test has to reach the same map.
type Raw = Arc<MemoryStore<StandardChunkSet>>;
type Store = ContentGet<Raw>;

/// Key sets that put a byte either side of the separator next to it, name a
/// directory that is also a file, and nest deeper than one level.
const SETS: &[&[&str]] = &[
    &["a", "a/b", "a/c/d", "a-x", "a.x", "a0x", "ab/c", "b"],
    &["img/logo.png", "imgx.png", "img/a/b/c", "img/a/d", "img/z"],
    &["x/", "x/a", "x/b/", "x/b/c", "y"],
    &["deep/1/2/3/4/5", "deep/1/2/3/4/6", "deep/2", "deep2"],
    &["s p a c e/f", "s p a c e/g", "s p a c f"],
];

/// Directories to list, including prefixes that name nothing and ones that
/// stop short of the separator.
const DIRS: &[&str] = &[
    "",
    "a/",
    "a",
    "img/",
    "img",
    "x/",
    "x/b/",
    "deep/",
    "deep/1/",
    "missing/",
    "s p a c e/",
];

/// The immediate children of `dir` over the key set, `dir` taken as a byte
/// prefix and deeper keys collapsed at the next separator.
fn model(keys: &[&str], dir: &str) -> Vec<Vec<u8>> {
    let mut sorted: Vec<&&str> = keys.iter().collect();
    sorted.sort_unstable();
    let mut seen: BTreeSet<Vec<u8>> = BTreeSet::new();
    let mut out: Vec<Vec<u8>> = Vec::new();
    for key in sorted {
        let bytes = key.as_bytes();
        let Some(suffix) = bytes.strip_prefix(dir.as_bytes()) else {
            continue;
        };
        // The directory path itself is not one of its own children.
        if suffix.is_empty() {
            continue;
        }
        let entry = match suffix.iter().position(|&byte| byte == b'/') {
            Some(cut) => bytes[..dir.len() + cut + 1].to_vec(),
            None => bytes.to_vec(),
        };
        if seen.insert(entry.clone()) {
            out.push(entry);
        }
    }
    out
}

/// One insert per key, with no metadata.
fn inserts(keys: &[&str], file: &ChunkRef) -> Vec<ManifestOp<ChunkRef, Box<dyn ManifestMetadata>>> {
    keys.iter()
        .map(|key| ManifestOp::Insert {
            path: ManifestPath::from(*key),
            reference: *file,
            meta: Box::new(()) as Box<dyn ManifestMetadata>,
        })
        .collect()
}

/// The listed paths, as text for a readable failure.
async fn listed(manifest: &dyn DynManifest, root: &ChunkRef, dir: &str) -> Vec<String> {
    manifest
        .dyn_dir(root, &ManifestPath::from(dir))
        .await
        .unwrap()
        .entries()
        .iter()
        .map(|entry| String::from_utf8_lossy(entry.path().as_bytes()).into_owned())
        .collect()
}

fn expected(keys: &[&str], dir: &str) -> Vec<String> {
    model(keys, dir)
        .iter()
        .map(|path| String::from_utf8_lossy(path).into_owned())
        .collect()
}

#[test]
fn both_formats_list_a_level_the_way_the_model_does() {
    run(async {
        for keys in SETS {
            let raw: Raw = Arc::new(MemoryStore::new());
            let store: Store = ContentGet::new(Arc::clone(&raw));
            let file = ChunkRef::new(
                File::<_, DEFAULT_BODY_SIZE>::new(&raw, Policy::DEFAULT)
                    .save(&b"payload"[..])
                    .await
                    .unwrap(),
            );

            let nodes = NodeLoadSaver::new(Arc::clone(&raw));
            let editor: ManifestEditor<_> = ManifestEditor::new(nodes.clone());
            let (empty, _) = editor.commit().await.unwrap();
            let trie = MantarayManifest::<_, _, DEFAULT_BODY_SIZE>::new(nodes, store.clone());
            let trie_root = trie
                .dyn_apply(&ChunkRef::new(empty), inserts(keys, &file))
                .await
                .unwrap();

            let builder: Builder<V1> = Builder::new();
            let empty = *builder.build(&store, &Plaintext).await.unwrap().root();
            let kv = LdbManifest::plain(store.clone());
            let kv_root = kv.dyn_apply(&empty, inserts(keys, &file)).await.unwrap();

            for dir in DIRS {
                let want = expected(keys, dir);
                assert_eq!(listed(&trie, &trie_root, dir).await, want, "trie {dir:?}");
                assert_eq!(listed(&kv, &kv_root, dir).await, want, "kv {dir:?}");
            }
        }
    });
}
