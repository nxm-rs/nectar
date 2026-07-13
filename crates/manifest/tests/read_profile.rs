//! The read-optimised profile through the public API: the whole build, read
//! and apply path is generic over the format, and a heavier embedding budget
//! inlines a subtree that V1 would reference, so the same key set resolves
//! through fewer chunks under `V1Read`.

use std::error::Error;

use bytes::Bytes;
use futures::executor::block_on;
use nectar_manifest::{Builder, Changeset, Entry, Key, Reader, V1, V1Read, apply};
use nectar_primitives::{ChunkAddress, ChunkRef, MemoryStore};

type TestResult = Result<(), Box<dyn Error>>;

fn ensure(cond: bool, what: &str) -> TestResult {
    if cond { Ok(()) } else { Err(what.into()) }
}

fn ref_entry<F: nectar_manifest::Format>(fill: u8) -> Entry<F> {
    Entry::from(ChunkRef::new(ChunkAddress::new([fill; 32])))
}

/// Keys sharing a single first byte and diverging at the second, so they hang
/// off one root fork as a single sub-node. Forty-eight ref32 records size that
/// sub-node body into the window between the two embedding budgets: V1
/// references it, the read profile embeds it.
fn windowed_keys() -> Vec<(Key, u8)> {
    (0u8..48).map(|x| (Key::from(&[b'a', x][..]), x)).collect()
}

/// Build the windowed key set under `F`, returning the root and the number of
/// distinct chunks the build stored.
fn build_windowed<F: nectar_manifest::Format>() -> Result<(ChunkAddress, usize), Box<dyn Error>> {
    let store = MemoryStore::default();
    let mut builder = Builder::<F>::new();
    for (key, fill) in windowed_keys() {
        builder.insert(key, ref_entry::<F>(fill), None);
    }
    let built = block_on(builder.build(&store))?;
    Ok((*built.root(), store.len()))
}

#[test]
fn the_read_profile_stores_fewer_chunks_for_a_windowed_subtree() -> TestResult {
    let (_, v1_chunks) = build_windowed::<V1>()?;
    let (_, read_chunks) = build_windowed::<V1Read>()?;
    // The sub-node is referenced under V1 (its own chunk) but embedded under
    // the read profile (folded into the root), so the read build stores fewer.
    ensure(
        read_chunks < v1_chunks,
        "read profile must store fewer chunks than V1 for the windowed subtree",
    )?;
    Ok(())
}

#[test]
fn the_read_profile_and_v1_produce_distinct_roots() -> TestResult {
    let (v1_root, _) = build_windowed::<V1>()?;
    let (read_root, _) = build_windowed::<V1Read>()?;
    // A distinct wire version and a distinct shape: byte-distinct manifests.
    ensure(
        v1_root != read_root,
        "the two profiles must root differently",
    )?;
    Ok(())
}

#[test]
fn build_then_read_round_trips_every_key_under_the_read_profile() -> TestResult {
    let store = MemoryStore::default();
    let mut builder = Builder::<V1Read>::new();
    for (key, fill) in windowed_keys() {
        builder.insert(key, ref_entry::<V1Read>(fill), None);
    }
    let root = *block_on(builder.build(&store))?.root();

    let reader = Reader::<MemoryStore, V1Read>::new(store);
    for (key, fill) in windowed_keys() {
        let got = block_on(reader.get(&root, &key))?;
        ensure(
            got == Some(ref_entry::<V1Read>(fill)),
            "read value mismatch",
        )?;
    }
    // An absent key still reads as absent under the profile.
    ensure(
        block_on(reader.get(&root, &Key::from(&b"absent"[..])))?.is_none(),
        "absent key must read as None",
    )?;
    Ok(())
}

#[test]
fn apply_matches_a_from_scratch_build_under_the_read_profile() -> TestResult {
    let all = windowed_keys();
    let split = 40usize;

    // A base of the first keys, then a changeset staging the rest.
    let store = MemoryStore::default();
    let mut base = Builder::<V1Read>::new();
    for (key, fill) in all.iter().take(split) {
        base.insert(key.clone(), ref_entry::<V1Read>(*fill), None);
    }
    let base_root = *block_on(base.build(&store))?.root();

    let mut changeset = Changeset::<V1Read>::new();
    for (key, fill) in all.iter().skip(split) {
        changeset.put(key.clone(), ref_entry::<V1Read>(*fill), None);
    }
    let applied = block_on(apply(&store, &base_root, &changeset))?;

    // A from-scratch build of the merged key set lands on the same root, byte
    // for byte: history independence holds under the read profile too.
    let (scratch_root, _) = build_windowed::<V1Read>()?;
    ensure(
        applied == scratch_root,
        "apply must match a from-scratch build under the read profile",
    )?;

    // The applied manifest reads back the full key set.
    let reader = Reader::<MemoryStore, V1Read>::new(store);
    for (key, fill) in &all {
        let got = block_on(reader.get(&applied, key))?;
        ensure(
            got == Some(ref_entry::<V1Read>(*fill)),
            "applied value mismatch",
        )?;
    }
    Ok(())
}

#[test]
fn an_inline_value_round_trips_under_the_read_profile() -> TestResult {
    let store = MemoryStore::default();
    let mut builder = Builder::<V1Read>::new();
    let value = Entry::<V1Read>::inline(Bytes::from_static(b"<h1>hi</h1>"))?;
    builder.insert(Key::from(&b"index.html"[..]), value.clone(), None);
    let root = *block_on(builder.build(&store))?.root();

    let reader = Reader::<MemoryStore, V1Read>::new(store);
    let got = block_on(reader.get(&root, &Key::from(&b"index.html"[..])))?;
    ensure(got == Some(value), "inline value must round-trip")?;
    Ok(())
}
