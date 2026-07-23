//! The streaming builder through the public API: the worked example is
//! reproduced byte-for-byte from a key set, the published root is a pure
//! function of the keys whatever order they arrive in, peak retained node
//! buffers track the trie depth rather than the key count, and the files path
//! splits through BMT and references the stored roots.

use anyhow::{Context, Result, anyhow, ensure};
use bytes::Bytes;
use nectar_manifest::{
    BuildStats, Builder, Child, Entry, ForkPayload, ForkTable, Key, KeyId, Metadata, Node, NodeGet,
    Prefix, RootExtension, V1, build_files,
};
use nectar_primitives::{ChunkAddress, ChunkOps, ChunkRef, MemoryStore};
use nectar_testing::run;

mod common;
use common::split_whole;

const fn ref32(byte: u8) -> ChunkRef {
    ChunkRef::new(ChunkAddress::new([byte; 32]))
}

fn content_type(value: &'static [u8]) -> Result<Metadata> {
    Ok(Metadata::new(
        KeyId::ContentType,
        Bytes::from_static(value),
    )?)
}

fn website_index() -> Result<Metadata> {
    Ok(Metadata::new(
        KeyId::WebsiteIndexDocument,
        Bytes::from_static(b"index.html"),
    )?)
}

// The spec's worked example, built manually: a two-file website behind one
// shared embedded child, the same node the codec conformance pins to 150 bytes.
fn worked_example_node() -> Result<Node> {
    let mut child = ForkTable::new();
    child.insert(
        Prefix::try_from(&b"mg/logo.png"[..])?,
        Entry::from(ref32(0xBB)).into(),
        Some(content_type(b"image/png")?),
    )?;
    child.insert(
        Prefix::try_from(&b"ndex.html"[..])?,
        Entry::from(ref32(0xAA)).into(),
        Some(content_type(b"text/html")?),
    )?;

    let mut forks = ForkTable::new();
    forks.insert(
        Prefix::try_from(&b"i"[..])?,
        Child::Embedded(child).into(),
        None,
    )?;

    Ok(Node::new(
        RootExtension::new(None, Some(website_index()?)),
        forks,
    ))
}

#[test]
fn builds_the_worked_example_byte_for_byte() -> Result<()> {
    let store = MemoryStore::default();
    let mut builder: Builder = Builder::new();
    builder
        .insert(
            Key::from(&b"index.html"[..]),
            Entry::from(ref32(0xAA)),
            Some(content_type(b"text/html")?),
        )
        .insert(
            Key::from(&b"img/logo.png"[..]),
            Entry::from(ref32(0xBB)),
            Some(content_type(b"image/png")?),
        )
        .manifest_metadata(website_index()?);

    let built = run(builder.build(&store))?;

    // The shared child is inlined, so the whole manifest is one chunk.
    ensure!(built.stats().nodes_written() == 1, "one node written");
    ensure!(built.stats().nodes_embedded() == 1, "one child embedded");

    let chunk = store.get(built.root()).context("root not stored")?;
    let expected = worked_example_node()?.encode()?;
    ensure!(expected.len() == 150, "worked example is 150 bytes");
    ensure!(
        chunk.envelope().data().as_ref() == expected.as_slice(),
        "root payload",
    );

    let decoded: Node = run(store.get_node(built.root()))?;
    ensure!(decoded == worked_example_node()?, "decoded root");
    Ok(())
}

fn root_of(order: &[(&[u8], u8)]) -> Result<ChunkAddress> {
    let store = MemoryStore::default();
    let mut builder: Builder = Builder::new();
    for (key, fill) in order {
        builder.insert(Key::from(*key), Entry::from(ref32(*fill)), None);
    }
    Ok(*run(builder.build(&store))?.root())
}

#[test]
fn the_published_root_is_history_independent() -> Result<()> {
    // A key set that exercises a terminating-and-continuing fork ("a" under
    // "about"), a nested shared edge ("about-us"), and a shared directory
    // ("img/").
    let order: [(&[u8], u8); 6] = [
        (b"a", 1),
        (b"about", 2),
        (b"about-us", 3),
        (b"img/logo.png", 4),
        (b"img/icon.svg", 5),
        (b"index.html", 6),
    ];
    let forward = root_of(&order)?;

    let mut reversed = order;
    reversed.reverse();
    ensure!(root_of(&reversed)? == forward, "reversed order");

    let mut rotated = order;
    rotated.rotate_left(3);
    ensure!(root_of(&rotated)? == forward, "rotated order");
    Ok(())
}

fn two_level_stats(store: &MemoryStore, fan: u16, width: u8) -> Result<BuildStats> {
    let mut builder: Builder<V1> = Builder::new();
    for hi in 0..fan {
        let hi = u8::try_from(hi)?;
        for lo in 0..width {
            builder.insert(Key::from(&[hi, lo][..]), Entry::from(ref32(hi)), None);
        }
    }
    Ok(*run(builder.build(store))?.stats())
}

#[test]
fn peak_node_buffers_track_depth_not_key_count() -> Result<()> {
    // Two two-level manifests whose second level is wide enough that each child
    // spills to its own chunk. The narrow build has 8 subtrees, the wide one 64;
    // the wide build stores many more nodes, yet both keep the same tiny number
    // of nodes open at once, so peak memory follows depth, not key count.
    let narrow = MemoryStore::default();
    let wide = MemoryStore::default();
    let narrow_stats = two_level_stats(&narrow, 8, 80)?;
    let wide_stats = two_level_stats(&wide, 64, 80)?;

    // Root plus one open child: never a whole level or frontier.
    ensure!(narrow_stats.peak_open_nodes() == 2, "narrow peak");
    ensure!(wide_stats.peak_open_nodes() == 2, "wide peak");

    // The children are spilled, not embedded, so work scales with the fan.
    ensure!(narrow_stats.nodes_embedded() == 0, "narrow spills all");
    ensure!(narrow_stats.nodes_written() == 9, "narrow node count");
    ensure!(wide_stats.nodes_written() == 65, "wide node count");

    // Eight times the keys, eight times the stored nodes, identical peak.
    ensure!(
        wide_stats.nodes_written() > narrow_stats.nodes_written().saturating_mul(7),
        "work scales with keys",
    );
    ensure!(
        narrow_stats.peak_open_nodes() == wide_stats.peak_open_nodes(),
        "peak is key-count independent",
    );
    Ok(())
}

#[test]
fn the_empty_builder_publishes_the_empty_root() -> Result<()> {
    let store = MemoryStore::default();
    let builder: Builder = Builder::new();
    let built = run(builder.build(&store))?;

    ensure!(built.stats().peak_open_nodes() == 1, "one open node");
    ensure!(built.stats().nodes_written() == 1, "one node written");

    let node: Node = run(store.get_node(built.root()))?;
    ensure!(node.is_empty(), "root is the empty map");
    Ok(())
}

#[test]
fn build_files_splits_through_bmt_and_references_the_stored_roots() -> Result<()> {
    let store = MemoryStore::default();
    let logo = Bytes::from(vec![0x42u8; 12_000]); // several chunks
    let page = Bytes::from_static(b"<h1>hello</h1>");
    let files = [
        (Key::from(&b"index.html"[..]), page.clone()),
        (Key::from(&b"logo.png"[..]), logo.clone()),
    ];

    let built = run(build_files(&store, files))?;
    let node: Node = run(store.get_node(built.root()))?;

    // Each file's manifest entry is its independent BMT root, and every file
    // chunk is present in the same store.
    for (first, tail, data) in [
        (b'i', &b"ndex.html"[..], page),
        (b'l', &b"ogo.png"[..], logo),
    ] {
        let record = node.forks().get(first).context("missing fork")?;
        ensure!(record.tail().as_bytes() == tail, "fork tail");
        let address = record
            .entry()
            .context("fork has no entry")?
            .address()
            .context("entry is not a reference")?;

        let (expected_root, _) = run(split_whole(&data)).map_err(|e| anyhow!("{e}"))?;
        ensure!(address == &expected_root, "file root reference");
        ensure!(store.get(address).is_some(), "file root stored");
    }
    Ok(())
}

#[test]
fn a_key_that_prefixes_another_shares_a_fork() -> Result<()> {
    let store = MemoryStore::default();
    let mut builder: Builder = Builder::new();
    builder
        .insert(Key::from(&b"a"[..]), Entry::from(ref32(1)), None)
        .insert(Key::from(&b"ab"[..]), Entry::from(ref32(2)), None);
    let built = run(builder.build(&store))?;

    let node: Node = run(store.get_node(built.root()))?;
    let record = node.forks().get(b'a').context("missing fork a")?;
    ensure!(record.tail().is_empty(), "single-byte edge");
    // "a" terminates here and the trie continues to "ab".
    ensure!(
        matches!(record.payload(), ForkPayload::Both { .. }),
        "fork is entry-and-child",
    );
    ensure!(
        record.entry() == Some(&Entry::from(ref32(1))),
        "terminating value",
    );
    Ok(())
}
