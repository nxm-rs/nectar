//! Both formats list one directory level the same way, and the same way a
//! model of the key set does.
//!
//! A listing is where the two formats are furthest apart internally: the
//! key-value database seeks past each named subtree, while the trie walks the
//! whole subtree under the prefix and collapses it in the seam. The seam is
//! only worth holding if those two arrive at the same answer, so the answer is
//! pinned against a model rather than against either implementation.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use nectar_loadsave::NodeLoadSaver;
use nectar_manifest::{
    Batch, ErasedManifest, Manifest, ManifestCursor, ManifestOp, ManifestPath, ManifestView,
    MetadataSource,
};
use nectar_mantaray::{MantarayManifest, NodeLoader, NodeSaver};
use nectar_primitives::{ChunkRef, DEFAULT_BODY_SIZE, EntryRef};
use nectar_testing::run;

mod common;
use common::{Nodes, both_formats, reference, save_file, stores};

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
fn inserts(keys: &[&str], file: &ChunkRef) -> Vec<ManifestOp<ChunkRef, Box<dyn MetadataSource>>> {
    keys.iter()
        .map(|key| ManifestOp::Insert {
            path: ManifestPath::from(*key),
            reference: *file,
            meta: Box::new(()) as Box<dyn MetadataSource>,
        })
        .collect()
}

/// The listed paths, as text for a readable failure.
async fn listed(manifest: &dyn ErasedManifest, root: &ChunkRef, dir: &str) -> Vec<String> {
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
            let (raw, store) = stores();
            let file = save_file(&raw, b"payload").await;
            let ((trie, trie_empty), (kv, kv_empty)) = both_formats(&raw, &store).await;
            let trie_root = trie
                .dyn_apply(&trie_empty, inserts(keys, &file))
                .await
                .unwrap();
            let kv_root = kv.dyn_apply(&kv_empty, inserts(keys, &file)).await.unwrap();

            for dir in DIRS {
                let want = expected(keys, dir);
                assert_eq!(listed(&trie, &trie_root, dir).await, want, "trie {dir:?}");
                assert_eq!(listed(&kv, &kv_root, dir).await, want, "kv {dir:?}");
            }
        }
    });
}

/// The trie's node seam, counting every load a walk touches.
#[derive(Clone)]
struct Counting {
    inner: Nodes,
    loads: Arc<AtomicUsize>,
}

impl Counting {
    /// The loads since the last take.
    fn take(&self) -> usize {
        self.loads.swap(0, Ordering::SeqCst)
    }
}

impl NodeLoader for Counting {
    type Error = <Nodes as NodeLoader>::Error;

    async fn load(&self, reference: &EntryRef) -> Result<Vec<u8>, Self::Error> {
        self.loads.fetch_add(1, Ordering::SeqCst);
        self.inner.load(reference).await
    }
}

impl NodeSaver<ChunkRef> for Counting {
    type Error = <Nodes as NodeSaver<ChunkRef>>::Error;

    async fn save(&self, data: Vec<u8>) -> Result<ChunkRef, Self::Error> {
        <Nodes as NodeSaver<ChunkRef>>::save(&self.inner, data).await
    }
}

/// Listing one directory level costs its own subtree, not the whole map: the
/// trie seeks to the listed prefix instead of the seam's walking default.
#[test]
fn listing_one_level_costs_that_subtree_alone() {
    run(async {
        let (raw, store) = stores();
        let nodes = Counting {
            inner: NodeLoadSaver::new(Arc::clone(&raw)),
            loads: Arc::default(),
        };
        let trie: MantarayManifest<_, _, DEFAULT_BODY_SIZE> =
            MantarayManifest::new(nodes.clone(), store);

        // Twenty-six top-level folders, so a walk that starts at the trie
        // root pays every folder before the listed one.
        let mut batch: Batch<ChunkRef, _> = Batch::new();
        for letter in b'a'..=b'z' {
            for index in 0..8u32 {
                let path = format!("{0}{0}{0}/{index}.bin", char::from(letter));
                batch.insert(ManifestPath::from(path.as_str()), reference(letter));
            }
        }
        let root = trie.empty().await.unwrap();
        let root = trie.apply(root, batch).await.unwrap();

        let view = trie.at(root);
        nodes.take();
        let listing = view.dir(&ManifestPath::from("zzz/")).await.unwrap();
        let level = nodes.take();
        assert_eq!(listing.entries().len(), 8);

        let mut cursor = view.iter().await.unwrap();
        let mut keys = 0;
        while cursor.next().await.unwrap().is_some() {
            keys += 1;
        }
        let whole = nodes.take();
        assert_eq!(keys, 26 * 8);

        // The last folder is a twenty-sixth of the map, so a pruned seek is
        // an order below the full walk. A filtering walk would tie.
        assert!(
            level * 4 < whole,
            "listing loaded {level} nodes, the whole trie {whole}"
        );
    });
}
