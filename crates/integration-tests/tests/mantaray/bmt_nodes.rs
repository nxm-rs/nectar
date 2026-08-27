//! Manifest nodes through the BMT file layer via the loadsave adapter.
//!
//! Pins the two wave-one guarantees: a node at or below one chunk keeps the
//! pre-adapter content-chunk address (so existing roots are unchanged), and
//! a node larger than one chunk commits, reloads and edits through the
//! adapter end to end.

use std::collections::BTreeSet;

use bytes::Bytes;
use nectar_file::{File, Policy};
use nectar_mantaray::{
    ManifestEditor, NodeLoadSaver, NodeLoader, TrieAddressStream, TrieListing, TrieLookup,
};
use nectar_primitives::chunk::{ChunkAddress, ChunkOps, ContentChunk};
use nectar_primitives::store::{ContentGet, MemoryStore};
use nectar_primitives::{EntryRef, StandardChunkSet};
use nectar_testing::run;

type Store = MemoryStore<StandardChunkSet>;
type LoadSaver = NodeLoadSaver<Store>;
type Editor = ManifestEditor<LoadSaver>;

const BODY: usize = 4096;

/// A ChunkAddress from a string, right-padded with zeroes.
fn make_addr(s: &str) -> ChunkAddress {
    let bytes = s.as_bytes();
    let mut buf = [0u8; 32];
    let len = bytes.len().min(32);
    buf[..len].copy_from_slice(&bytes[..len]);
    ChunkAddress::from(buf)
}

/// A nonzero per-byte entry address.
fn byte_addr(b: u8) -> ChunkAddress {
    let mut buf = [1u8; 32];
    buf[31] = b;
    ChunkAddress::from(buf)
}

/// Walk every persisted node from `root` and assert each single-chunk
/// node's reference equals its bytes' content-chunk address: the adapter
/// changes nothing at or below one chunk.
async fn assert_node_parity(loadsaver: &LoadSaver, root: EntryRef) {
    let mut stack = vec![root];
    let mut visited = 0usize;
    while let Some(reference) = stack.pop() {
        let bytes = loadsaver.load(&reference).await.unwrap();
        if bytes.len() <= BODY {
            let chunk = ContentChunk::<BODY>::new(Bytes::from(bytes.clone())).unwrap();
            assert_eq!(
                chunk.address(),
                reference.address(),
                "single-chunk node diverged from the content-chunk address"
            );
        }
        let view = nectar_mantaray::view::NodeView::try_from(bytes.as_slice()).unwrap();
        for fork in view.forks() {
            stack.push(fork.reference().clone());
        }
        visited += 1;
    }
    assert!(visited > 0);
}

#[test]
fn small_node_roots_keep_address_parity() {
    let corpora: &[&[&str]] = &[
        &["a"],
        &["index.html", "img/1.png", "img/2.png", "robots.txt"],
        &["a/b/c/d/e/f/g/h/file00.dat", "a/b/c/x.txt"],
        &["app.js", "app.js.map"],
    ];
    run(async {
        for paths in corpora {
            let mut editor = Editor::new(LoadSaver::new(Store::new()));
            for &p in *paths {
                editor.insert(p, make_addr(p));
            }
            let (root, loadsaver) = editor.commit().await.unwrap();
            assert_node_parity(&loadsaver, EntryRef::from(root)).await;
        }
    });
}

/// One manifest whose root node exceeds one chunk: 256 one-byte fanout
/// paths make a 256-fork root of roughly 16 KiB.
fn build_wide() -> (ChunkAddress, LoadSaver) {
    let mut editor = Editor::new(LoadSaver::new(Store::new()));
    for b in 0..=u8::MAX {
        editor.insert([b], byte_addr(b));
    }
    run(editor.commit()).unwrap()
}

#[test]
fn multi_chunk_root_commits_and_reads_back() {
    let (root, loadsaver) = build_wide();
    run(async {
        // The root node image genuinely spans chunks.
        let bytes = loadsaver.load(&EntryRef::from(root)).await.unwrap();
        assert!(bytes.len() > BODY, "root image is {} bytes", bytes.len());

        // Every entry reads back through the adapter.
        let reader = TrieLookup::new(loadsaver.clone());
        for b in 0..=u8::MAX {
            let entry = reader.get(root, &[b]).await.unwrap().unwrap();
            assert_eq!(entry.reference().map(|r| *r.address()), Some(byte_addr(b)));
        }

        // The listing yields all 256 paths in order.
        let mut cursor = TrieListing::new(loadsaver.clone(), root);
        let mut listed = Vec::new();
        while let Some(entry) = cursor.next().await {
            listed.push(entry.unwrap().path().to_vec());
        }
        assert_eq!(listed, (0..=u8::MAX).map(|b| vec![b]).collect::<Vec<_>>());

        // Sub-chunk parity still holds for the leaf nodes below the root.
        assert_node_parity(&loadsaver, EntryRef::from(root)).await;
    });
}

#[test]
fn multi_chunk_root_edits_through_the_adapter() {
    let (root, loadsaver) = build_wide();
    let mut editor = Editor::open(root, loadsaver);
    editor.remove([7u8]);
    editor.insert("added.txt", make_addr("added"));
    let (root, loadsaver) = run(editor.commit()).unwrap();

    let reader = TrieLookup::new(loadsaver);
    run(async {
        assert!(reader.get(root, &[7u8]).await.unwrap().is_none());
        assert!(reader.get(root, &[8u8]).await.unwrap().is_some());
        assert!(reader.get(root, b"added.txt").await.unwrap().is_some());
    });
}

#[test]
fn address_stream_covers_every_chunk_of_a_multi_chunk_node() {
    let (root, loadsaver) = build_wide();
    run(async {
        let mut stream = TrieAddressStream::new(loadsaver.clone(), root);
        let mut got = BTreeSet::new();
        while let Some(address) = stream.next().await {
            got.insert(address.unwrap());
        }
        // Every stored chunk (root spine and leaves included) plus every
        // entry address: nothing a pinner needs is missing.
        let mut want: BTreeSet<ChunkAddress> = loadsaver
            .store()
            .clone()
            .into_chunks()
            .keys()
            .copied()
            .collect();
        want.extend((0..=u8::MAX).map(byte_addr));
        assert_eq!(got, want);
    });
}

/// Publishing a file root under a manifest path: the exhibit formerly on
/// the file crate, now on the manifest side of the layering.
#[test]
fn publish_root_under_path() {
    run(async {
        let data = b"hello swarm".to_vec();
        let store = Store::new();
        let root = File::<_, BODY>::new(&store, Policy::DEFAULT)
            .save(&data[..])
            .await
            .unwrap();

        let mut editor = Editor::new(LoadSaver::new(store));
        editor.insert("hello.txt", root);
        editor.insert("stale.txt", root);
        editor.remove("stale.txt");
        let (manifest_root, loadsaver) = editor.commit().await.unwrap();

        let reader = TrieLookup::new(loadsaver.clone());
        assert!(
            reader
                .get(manifest_root, b"stale.txt")
                .await
                .unwrap()
                .is_none()
        );
        let entry = reader
            .get(manifest_root, b"hello.txt")
            .await
            .unwrap()
            .unwrap();
        let file = File::<_, BODY>::new(ContentGet::new(loadsaver.into_store()), Policy::DEFAULT);
        let bytes = file
            .collect(entry.reference().unwrap().clone(), u64::MAX)
            .await
            .unwrap();
        assert_eq!(bytes, data);
    });
}

#[cfg(feature = "encryption")]
mod encrypted {
    use super::*;
    use nectar_primitives::{EncryptedChunkRef, EncryptionKey};

    /// An encrypted manifest commits through the encrypted split and reads
    /// back from its full-width root reference alone.
    #[test]
    fn encrypted_manifest_round_trips_through_the_adapter() {
        let paths = ["secret/a.txt", "secret/b.txt", "top.txt"];
        let key = EncryptionKey::from([0x5a; 32]);
        let mut editor: ManifestEditor<LoadSaver, EncryptedChunkRef> =
            ManifestEditor::new_encrypted(LoadSaver::new(Store::new()));
        for p in paths {
            editor.insert(p, EncryptedChunkRef::new(make_addr(p), key.clone()));
        }
        let (root, loadsaver) = run(editor.commit()).unwrap();

        let reader = TrieLookup::new(loadsaver.clone());
        run(async {
            for p in paths {
                let entry = reader
                    .get(root.clone(), p.as_bytes())
                    .await
                    .unwrap()
                    .unwrap();
                match entry.reference() {
                    Some(EntryRef::Encrypted(reference)) => {
                        assert_eq!(reference.address(), &make_addr(p));
                        assert_eq!(reference.key(), &key);
                    }
                    other => panic!("expected an encrypted reference, got {other:?}"),
                }
            }
        });

        // Reopen and extend from the root reference alone.
        let mut editor: ManifestEditor<LoadSaver, EncryptedChunkRef> =
            ManifestEditor::open_encrypted(root, loadsaver);
        editor.insert("secret/c.txt", EncryptedChunkRef::new(make_addr("c"), key));
        let (root, loadsaver) = run(editor.commit()).unwrap();
        let reader = TrieLookup::new(loadsaver);
        assert!(run(reader.get(root, b"secret/c.txt")).unwrap().is_some());
    }
}
