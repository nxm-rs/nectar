//! Order-statistic counts through the public API: a manifest round-trips its
//! keys, stays canonical under shuffled build order, and folds a changeset to
//! the same root a from-scratch build produces, with counts maintained
//! throughout.

use std::error::Error;

use bytes::Bytes;
use futures::executor::block_on;
use nectar_manifest::{Builder, Changeset, Entry, Key, Reader, V1, apply};
use nectar_primitives::{ChunkAddress, ChunkRef, MemoryStore};

type TestResult = Result<(), Box<dyn Error>>;

fn ensure(cond: bool, what: &str) -> TestResult {
    if cond { Ok(()) } else { Err(what.into()) }
}

fn ref_entry<F: nectar_manifest::Format>(fill: u8) -> Entry<F> {
    Entry::from(ChunkRef::new(ChunkAddress::new([fill; 32])))
}

/// A key set wide and deep enough to reference sub-nodes and spill the root:
/// many first-byte groups, each holding a windowed sub-tree of its own.
fn keys() -> Vec<(Key, u8)> {
    let mut out = Vec::new();
    for p in 0u8..96 {
        for x in 0u8..44 {
            out.push((Key::from(&[p, x][..]), x));
        }
    }
    out
}

fn build<F: nectar_manifest::Format>(order: &[(Key, u8)]) -> Result<ChunkAddress, Box<dyn Error>> {
    let store = MemoryStore::default();
    let mut builder = Builder::<F>::new();
    for (key, fill) in order {
        builder.insert(key.clone(), ref_entry::<F>(*fill), None);
    }
    Ok(*block_on(builder.build(&store))?.root())
}

#[test]
fn counted_canonical_bytes_are_stable_under_shuffled_build_order() -> TestResult {
    let forward = keys();
    let mut reversed = forward.clone();
    reversed.reverse();
    let a = build::<V1>(&forward)?;
    let b = build::<V1>(&reversed)?;
    ensure(a == b, "counted root must not depend on insertion order")?;
    Ok(())
}

#[test]
fn build_then_read_round_trips_every_key() -> TestResult {
    let store = MemoryStore::default();
    let all = keys();
    let mut builder = Builder::<V1>::new();
    for (key, fill) in &all {
        builder.insert(key.clone(), ref_entry::<V1>(*fill), None);
    }
    let root = *block_on(builder.build(&store))?.root();

    let reader = Reader::<MemoryStore, V1>::new(store);
    for (key, fill) in &all {
        let got = block_on(reader.get(&root, key))?;
        ensure(got == Some(ref_entry::<V1>(*fill)), "read value mismatch")?;
    }
    ensure(
        block_on(reader.get(&root, &Key::from(&b"absent"[..])))?.is_none(),
        "absent key must read as None",
    )?;
    Ok(())
}

#[test]
fn apply_matches_a_from_scratch_build() -> TestResult {
    let all = keys();
    let split = all.len() * 3 / 4;

    let store = MemoryStore::default();
    let mut base = Builder::<V1>::new();
    for (key, fill) in all.iter().take(split) {
        base.insert(key.clone(), ref_entry::<V1>(*fill), None);
    }
    let base_root = *block_on(base.build(&store))?.root();

    let mut changeset = Changeset::<V1>::new();
    for (key, fill) in all.iter().skip(split) {
        changeset.put(key.clone(), ref_entry::<V1>(*fill), None);
    }
    let applied = block_on(apply(&store, &base_root, &changeset))?;

    let scratch = build::<V1>(&all)?;
    ensure(applied == scratch, "apply must match a from-scratch build")?;
    Ok(())
}

#[test]
fn an_inline_value_round_trips() -> TestResult {
    let store = MemoryStore::default();
    let mut builder = Builder::<V1>::new();
    let value = Entry::<V1>::inline(Bytes::from_static(b"<h1>hi</h1>"))?;
    builder.insert(Key::from(&b"index.html"[..]), value.clone(), None);
    let root = *block_on(builder.build(&store))?.root();

    let reader = Reader::<MemoryStore, V1>::new(store);
    let got = block_on(reader.get(&root, &Key::from(&b"index.html"[..])))?;
    ensure(got == Some(value), "inline value must round-trip")?;
    Ok(())
}
