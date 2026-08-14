//! The streaming reader through the public API: descent follows one fork per
//! node, so a lookup down a wide manifest fetches O(depth) nodes and never a
//! whole level. A counting store witnesses the bound directly.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::{Result, ensure};
use bytes::Bytes;
use nectar_ldb::{
    Builder, Child, Database, Entry, ForkTable, Key, KeyId, Metadata, Node, Plaintext, Prefix,
    Reader, V1, save_node,
};
use nectar_manifest::{Batch, Manifest, ManifestPath, ManifestView};
use nectar_primitives::store::{ChunkGet, ContentGet, MemoryStore};
use nectar_primitives::{Chunk, ChunkAddress, ChunkRef, ContentOnlyChunkSet, Verified};
use nectar_testing::run;

/// A trusted store that counts every `get`, so a test can read off how many
/// nodes a lookup fetched.
#[derive(Debug, Default)]
struct CountingStore {
    inner: ContentGet<MemoryStore>,
    gets: AtomicUsize,
}

impl CountingStore {
    fn gets(&self) -> usize {
        self.gets.load(Ordering::Relaxed)
    }
}

impl ChunkGet<ContentOnlyChunkSet> for CountingStore {
    type Trust = Verified;
    type Error = <ContentGet<MemoryStore> as ChunkGet<ContentOnlyChunkSet>>::Error;

    async fn get(
        &self,
        address: &ChunkAddress,
    ) -> Result<Chunk<Verified, ContentOnlyChunkSet>, Self::Error> {
        self.gets.fetch_add(1, Ordering::Relaxed);
        ChunkGet::get(&self.inner, address).await
    }
}

fn entry(byte: u8) -> Entry {
    ChunkRef::new(ChunkAddress::new([byte; 32])).into()
}

/// Build a deliberately wide two-level manifest: a root whose fork table holds
/// one referenced leaf per first byte, each leaf terminating a single key. The
/// second level is `width` chunks wide, but any one key sits two nodes deep.
async fn wide_manifest(store: &MemoryStore, width: u16) -> Result<ChunkRef> {
    let mut forks = ForkTable::<V1>::new();
    for first in 0..width {
        let first = u8::try_from(first)?;
        let mut leaf = ForkTable::new();
        leaf.insert(Prefix::try_from(&[0xFFu8][..])?, entry(first).into(), None)?;
        let leaf_ref = save_node(store, &Node::new(None, leaf), &Plaintext).await?;
        forks.insert(
            Prefix::try_from(&[first][..])?,
            Child::Ref(leaf_ref).into(),
            None,
        )?;
    }
    Ok(save_node(store, &Node::new(None, forks), &Plaintext).await?)
}

#[test]
fn a_lookup_fetches_depth_nodes_not_the_wide_level() -> Result<()> {
    // Wide enough to dwarf the depth-2 path, yet inside one root chunk (a
    // radix-full root spills to a directory, which is a later car's concern).
    let width = 100u16;
    let memory = MemoryStore::default();
    let root = run(wide_manifest(&memory, width))?;

    // The whole manifest is one root plus `width` leaves.
    ensure!(memory.len() == usize::from(width) + 1, "stored node count");

    let store = CountingStore {
        inner: ContentGet::new(memory),
        gets: AtomicUsize::new(0),
    };
    let reader: Reader<_> = Reader::new(&store);

    // Look up one key: first byte 0x2A, then the leaf's 0xFF fork.
    let key = Key::from(&[0x2Au8, 0xFF][..]);
    let value = run(reader.get(&root, &key))?;
    ensure!(value == Some(entry(0x2A)), "looked-up value");

    // Two hops: the root and the single leaf on the path. Never the wide
    // sibling level, so fetches track depth, not width.
    ensure!(store.gets() == 2, "fetches equal path depth");
    ensure!(
        store.gets() < usize::from(width),
        "fetches below level width"
    );

    // A second lookup on a different branch is again two hops: the frontier is
    // never widened, each key pays only its own path.
    store.gets.store(0, Ordering::Relaxed);
    let value = run(reader.get(&root, &Key::from(&[0x50u8, 0xFF][..])))?;
    ensure!(value == Some(entry(0x50)), "second value");
    ensure!(store.gets() == 2, "second lookup is also depth-bounded");
    Ok(())
}

#[test]
fn every_builder_key_reads_back_through_referenced_hops() -> Result<()> {
    // Build a real manifest through the builder so the reader is exercised
    // against the wire structure it exists to read, not a hand-laid table: the
    // shared-prefix grid forces compacted edges, embedded subtrees and, once a
    // subtree outgrows the inline bound, referenced children the reader must
    // fetch and descend.
    let memory = MemoryStore::default();
    let mut builder = Builder::new();
    let mut expected: Vec<(String, Entry)> = Vec::new();
    for dir in 0u8..16 {
        for file in 0u8..40 {
            let key = format!("dir{dir:02}/file{file:04}.txt");
            let value = entry((dir ^ file).wrapping_add(1));
            builder.insert(Key::from(key.clone().into_bytes()), value.clone(), None);
            expected.push((key, value));
        }
    }
    // A key that is a strict prefix of another exercises a fork carrying both a
    // terminal value and a continuation.
    builder.insert(Key::from(&b"pre"[..]), entry(0x11), None);
    expected.push(("pre".to_owned(), entry(0x11)));
    builder.insert(Key::from(&b"prefix"[..]), entry(0x22), None);
    expected.push(("prefix".to_owned(), entry(0x22)));
    // An inline value must read back whole, not as a reference.
    let inline = Entry::inline(Bytes::from_static(b"hello"))?;
    builder.insert(Key::from(&b"inline.txt"[..]), inline.clone(), None);
    expected.push(("inline.txt".to_owned(), inline));
    // The empty key sets the manifest's own value in the root extension.
    builder.insert(Key::empty(), entry(0x99), None);

    let built = run(builder.build(&memory, &Plaintext))?;
    let root = *built.root();
    // More than one node spilled: the builder emitted referenced children, so
    // reading keys beneath them genuinely drives the fetch-and-descend path.
    ensure!(
        built.stats().nodes_written() > 1,
        "builder spilled referenced children",
    );

    let store = CountingStore {
        inner: ContentGet::new(memory),
        gets: AtomicUsize::new(0),
    };
    let reader: Reader<_> = Reader::new(&store);

    // The root extension answers the empty key.
    ensure!(
        run(reader.get(&root, &Key::empty()))? == Some(entry(0x99)),
        "empty key reads the root value",
    );

    // Every key reads back its exact value, and no single lookup ever fetches a
    // whole level: the deepest path stays a small multiple of the tree depth,
    // far below the spilled node count.
    let mut deepest = 0usize;
    run(async {
        for (key, value) in &expected {
            store.gets.store(0, Ordering::Relaxed);
            let got = reader
                .get(&root, &Key::from(key.clone().into_bytes()))
                .await?;
            ensure!(got.as_ref() == Some(value), "{key}");
            deepest = deepest.max(store.gets());
        }
        anyhow::Ok(())
    })?;
    ensure!(deepest >= 2, "at least one key descends a referenced hop");
    ensure!(
        deepest < built.stats().nodes_written(),
        "no lookup fetches the whole tree",
    );

    // Keys that diverge from, fall short of, or overrun a stored key are absent.
    run(async {
        for absent in [
            "nope",
            "dir99/file0000.txt",
            "dir00/file9999.txt",
            "pr",
            "prefixed",
        ] {
            ensure!(
                reader
                    .get(&root, &Key::from(absent.as_bytes()))
                    .await?
                    .is_none(),
                "{absent}",
            );
        }
        Ok(())
    })
}

#[test]
fn an_absent_key_stops_at_the_first_unmatched_fork() -> Result<()> {
    let width = 64u16;
    let memory = MemoryStore::default();
    let root = run(wide_manifest(&memory, width))?;

    let store = CountingStore {
        inner: ContentGet::new(memory),
        gets: AtomicUsize::new(0),
    };
    let reader: Reader<_> = Reader::new(&store);

    // A first byte no root fork carries: the walk stops at the root without
    // fetching any leaf.
    let value = run(reader.get(&root, &Key::from(&[0xFFu8, 0x00][..])))?;
    ensure!(value.is_none(), "absent value");
    ensure!(store.gets() == 1, "only the root is fetched");
    Ok(())
}

/// A website root carries the site documents in its manifest metadata and binds
/// no root entry, so every read of that slot has to answer with it.
///
/// The three surfaces are the streaming reader, the root-bound view, and the
/// `Manifest` seam's view, which reaches the slot through its option-typed
/// accessors rather than through a key.
#[test]
fn root_metadata_reads_back_without_a_root_entry() -> Result<()> {
    let store = ContentGet::new(Arc::new(MemoryStore::default()));
    let mut meta = Metadata::<V1>::new(
        KeyId::WebsiteIndexDocument,
        Bytes::from_static(b"index.html"),
    )?;
    meta.insert(KeyId::WebsiteErrorDocument, Bytes::from_static(b"404.html"))?;

    let mut builder: Builder<V1> = Builder::new();
    builder.insert(Key::from(&b"index.html"[..]), entry(0x01), None);
    builder.manifest_metadata(meta.clone());
    let root = *run(builder.build(&store, &Plaintext))?.root();

    run(async {
        let reader: Reader<_> = Reader::new(&store);
        let site = reader.website(&root).await?;
        ensure!(site.index() == Some(&b"index.html"[..]), "index document");
        ensure!(site.error() == Some(&b"404.html"[..]), "error document");

        let read = reader.metadata(&root, &Key::empty()).await?;
        ensure!(
            read.as_ref() == Some(&meta),
            "the reader reads the metadata"
        );

        let db: Database<_> = Database::plain(&store);
        let view = db.at(&root);
        ensure!(
            view.metadata(&Key::empty()).await? == read,
            "the view agrees"
        );
        ensure!(
            view.website().await?.index() == Some(&b"index.html"[..]),
            "the view's website agrees",
        );

        let seam = Database::<_>::plain(store.clone());
        let seam_view = Manifest::at(&seam, root);
        ensure!(
            ManifestView::index_document(&seam_view)
                .await?
                .as_ref()
                .map(ManifestPath::as_bytes)
                == Some(&b"index.html"[..]),
            "the index document survives the seam",
        );
        ensure!(
            ManifestView::error_document(&seam_view)
                .await?
                .as_ref()
                .map(ManifestPath::as_bytes)
                == Some(&b"404.html"[..]),
            "the error document survives the seam",
        );
        // The slot is not a content key.
        let empty = ManifestPath::default();
        ensure!(
            ManifestView::metadata(&seam_view, &empty).await?.is_none(),
            "the empty path carries no metadata",
        );
        ensure!(
            ManifestView::get(&seam_view, &empty).await?.is_none(),
            "the empty path binds nothing",
        );

        // An absent root entry still reads as absent.
        ensure!(view.get(&Key::empty()).await?.is_none(), "no root entry");
        ensure!(!view.contains_key(&Key::empty()).await?, "no root binding");
        Ok(())
    })
}

/// One `Database` value serves both contracts: the inherent methods keep the
/// native surface, and the `Manifest` bound reaches the seam.
///
/// The seam's `apply` folds the whole batch through one native changeset, so
/// its root is byte-identical to the one the native editor lands on. A
/// delegation shell would not compile here, and a drift in the fold moves the
/// root.
#[test]
fn one_database_serves_the_native_and_the_seam_contract() -> Result<()> {
    let store = ContentGet::new(Arc::new(MemoryStore::default()));
    run(async {
        let db: Database<_> = Database::plain(store.clone());
        let empty: ChunkRef = Manifest::empty(&db).await?;

        let key = Key::from(&b"index.html"[..]);
        let path = ManifestPath::from("index.html");
        let reference = ChunkRef::new(ChunkAddress::new([0x11; 32]));
        let meta = Metadata::<V1>::new(KeyId::ContentType, Bytes::from_static(b"text/html"))?;

        // The seam's write: one batch with an entry, its metadata, and a site
        // document.
        let mut batch = Batch::new();
        batch
            .insert_with(path.clone(), reference, Some(meta.clone()))
            .set_index_document(path.clone());
        let seam_root = Manifest::apply(&db, empty, batch).await?;

        // The native write of the same ops, on the same value.
        let mut editor = db.edit(&empty);
        editor.insert_with(key.clone(), Entry::from(reference), meta);
        editor.set_root_metadata(
            KeyId::WebsiteIndexDocument,
            Some(Bytes::from_static(b"index.html")),
        );
        let native_root: ChunkRef = editor.commit().await?;
        ensure!(
            seam_root == native_root,
            "the seam's apply and the native editor land on one root"
        );

        // The inherent `at` still answers the native contract over keys.
        ensure!(
            db.at(&seam_root).get(&key).await?.is_some(),
            "the native view answers over keys"
        );

        // The trait's `at` answers the seam contract over paths, with the
        // reserved slots filtered.
        let view = Manifest::at(&db, seam_root);
        ensure!(
            ManifestView::get(&view, &path).await?.is_some(),
            "the seam view answers over paths"
        );
        ensure!(
            ManifestView::get(&view, &ManifestPath::default())
                .await?
                .is_none(),
            "the seam view keeps the root slot filtered"
        );
        Ok(())
    })
}
