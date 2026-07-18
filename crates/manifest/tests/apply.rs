//! History-independent apply through the public API: folding a changeset into a
//! manifest lands on the exact root a from-scratch build of the merged key set
//! produces, byte for byte, over random bases and random update batches.
//!
//! This is the conformance equation `apply(root(M), D) == root(M + D)`, the
//! guarantee that content addressing and dedup survive an update stream.

use std::collections::BTreeMap;

use bytes::Bytes;
use futures::executor::block_on;
use nectar_manifest::{Builder, Changeset, Entry, Key, KeyId, Metadata, V1, apply};
use nectar_primitives::{ChunkAddress, ChunkRef, MemoryStore};
use proptest::prelude::*;

/// A key's value plus optional metadata, the payload both paths carry.
type Value = (Entry<V1>, Option<Metadata<V1>>);

/// One staged update: a value to bind, or `None` to delete.
type Update = (Vec<u8>, Option<Value>);

/// The generated parts of a value: inline flag, fill byte, blob, metadata flag.
type Parts = (bool, u8, Vec<u8>, bool);

fn ref32(fill: u8) -> Entry<V1> {
    Entry::from(ChunkRef::new(ChunkAddress::new([fill; 32])))
}

/// A value from its generated parts: a short inline blob or a plain reference,
/// with metadata on request.
fn value(inline: bool, fill: u8, blob: &[u8], meta: bool) -> Result<Value, TestCaseError> {
    let entry = if inline {
        Entry::inline(Bytes::copy_from_slice(blob))
            .map_err(|e| TestCaseError::fail(e.to_string()))?
    } else {
        ref32(fill)
    };
    let metadata = if meta {
        Some(
            Metadata::new(KeyId::ContentType, Bytes::from_static(b"text/html"))
                .map_err(|e| TestCaseError::fail(e.to_string()))?,
        )
    } else {
        None
    };
    Ok((entry, metadata))
}

/// The root a from-scratch build of `map` produces in a fresh store, so the
/// address depends on the merged bytes alone.
fn rebuild(map: &BTreeMap<Vec<u8>, Value>) -> Result<ChunkAddress, TestCaseError> {
    let store = MemoryStore::default();
    let mut builder = Builder::<V1>::new();
    for (key, (entry, meta)) in map {
        builder.insert(Key::from(key.clone()), entry.clone(), meta.clone());
    }
    let built = block_on(builder.build(&store)).map_err(|e| TestCaseError::fail(e.to_string()))?;
    Ok(*built.root())
}

/// A short key over a five-symbol alphabet, so keys share prefixes and the trie
/// branches, compacts and spills.
fn key_bytes() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(0u8..5, 1..=8)
}

/// A generated value: inline-or-reference, a fill byte, a short blob, metadata.
fn value_parts() -> impl Strategy<Value = Parts> {
    (
        any::<bool>(),
        any::<u8>(),
        prop::collection::vec(any::<u8>(), 0..=6),
        any::<bool>(),
    )
}

/// A base key set: distinct keys collapse in a map, so the build is well-defined.
fn base_set() -> impl Strategy<Value = Vec<(Vec<u8>, Parts)>> {
    prop::collection::vec((key_bytes(), value_parts()), 0..=60)
}

/// A changeset: each entry binds a value or, on `None`, deletes the key.
fn change_set() -> impl Strategy<Value = Vec<(Vec<u8>, Option<Parts>)>> {
    prop::collection::vec((key_bytes(), prop::option::of(value_parts())), 0..=40)
}

/// A long, low-entropy key: a binary alphabet over up to 400 bytes, so pairs
/// share prefixes past the 255-byte bound while the two-way fanout keeps every
/// node within budget. Chains form, and a split above a chain boundary
/// re-compacts into a `PLEN_MAX`-capped chain rather than one over-long edge.
fn long_key_bytes() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(0u8..2, 1..=400)
}

/// A base key set of long keys.
fn long_base_set() -> impl Strategy<Value = Vec<(Vec<u8>, Parts)>> {
    prop::collection::vec((long_key_bytes(), value_parts()), 0..=8)
}

/// A changeset over long keys.
fn long_change_set() -> impl Strategy<Value = Vec<(Vec<u8>, Option<Parts>)>> {
    prop::collection::vec((long_key_bytes(), prop::option::of(value_parts())), 0..=6)
}

/// Fold `changes` into a manifest built from `base` and assert the applied root
/// is byte-identical to a from-scratch build of the merged key set.
fn assert_apply_equals_rebuild(
    base: &[(Vec<u8>, Parts)],
    changes: &[(Vec<u8>, Option<Parts>)],
) -> Result<(), TestCaseError> {
    // The base map and its published manifest.
    let mut map: BTreeMap<Vec<u8>, Value> = BTreeMap::new();
    for (key, (inline, fill, blob, meta)) in base {
        map.insert(key.clone(), value(*inline, *fill, blob, *meta)?);
    }
    let store = MemoryStore::default();
    let mut builder = Builder::<V1>::new();
    for (key, val) in &map {
        builder.insert(Key::from(key.clone()), val.0.clone(), val.1.clone());
    }
    let root = *block_on(builder.build(&store))
        .map_err(|e| TestCaseError::fail(e.to_string()))?
        .root();

    // Stage the batch into a changeset and, in the same order, into the
    // expected merged map so the last update per key wins in both.
    let mut changeset = Changeset::<V1>::new();
    let mut updates: Vec<Update> = Vec::new();
    for (key, op) in changes {
        match op {
            Some((inline, fill, blob, meta)) => {
                let val = value(*inline, *fill, blob, *meta)?;
                changeset.put(Key::from(key.clone()), val.0.clone(), val.1.clone());
                updates.push((key.clone(), Some(val)));
            }
            None => {
                changeset.remove(Key::from(key.clone()));
                updates.push((key.clone(), None));
            }
        }
    }
    for (key, op) in updates {
        match op {
            Some(val) => {
                map.insert(key, val);
            }
            None => {
                map.remove(&key);
            }
        }
    }

    let applied = block_on(apply(&store, &root, &changeset))
        .map_err(|e| TestCaseError::fail(e.to_string()))?;
    let expected = rebuild(&map)?;
    prop_assert_eq!(applied, expected);
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // apply(root(M), D) == root(M + D), byte-identical: the applied root equals a
    // from-scratch build of the merged key set. Random base, random batch of
    // inserts, updates and deletes, some overlapping near the root.
    #[test]
    fn apply_equals_rebuild(base in base_set(), changes in change_set()) {
        assert_apply_equals_rebuild(&base, &changes)?;
    }

    // The same equation over long, low-entropy keys, so collapses cross the
    // 255-byte prefix bound and exercise the chain-compaction path.
    #[test]
    fn apply_equals_rebuild_over_long_keys(base in long_base_set(), changes in long_change_set()) {
        assert_apply_equals_rebuild(&base, &changes)?;
    }
}
