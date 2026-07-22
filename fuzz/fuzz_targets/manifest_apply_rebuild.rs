//! Fuzz the mantaray 1.0 batch-apply history-independence property.
//!
//! A base key set is built, then a fuzzed changeset of puts and deletes is
//! folded in with `apply`. The oracle is that folding a changeset into a
//! manifest lands on the exact same root as building the merged key set from
//! scratch: `apply(build(base), delta) == build(base <+ delta)`. The order the
//! updates were staged in never reaches the root. A build or apply that cannot
//! fit a node returns a typed error rather than panicking; equality is asserted
//! only when both the apply and the rebuild succeed.

#![no_main]

use bytes::Bytes;
use futures::executor::block_on;
use libfuzzer_sys::fuzz_target;
use nectar_manifest::{Builder, Changeset, Entry, Key, V1, apply};
use nectar_primitives::store::MemoryStore;
use nectar_primitives::{ChunkAddress, ChunkRef};

use arbitrary::Arbitrary;
use std::collections::BTreeMap;

/// One fuzzed value: a plain reference or an inline byte string.
#[derive(Arbitrary, Debug, Clone)]
enum Val {
    /// A 32-byte reference synthesised from one byte.
    Ref(u8),
    /// An inline value; capped to the format bound before insertion.
    Inline(Vec<u8>),
}

/// Turn a fuzzed value into an entry, capping an inline value at the bound.
fn entry(val: Val) -> Entry<V1> {
    match val {
        Val::Ref(byte) => Entry::from(ChunkRef::new(ChunkAddress::new([byte; 32]))),
        Val::Inline(mut bytes) => {
            bytes.truncate(128);
            Entry::inline(Bytes::from(bytes))
                .unwrap_or_else(|_| Entry::from(ChunkRef::new(ChunkAddress::new([0; 32]))))
        }
    }
}

/// Build a key set into a fresh store, returning the store and root.
fn build(pairs: &BTreeMap<Vec<u8>, Entry<V1>>) -> Option<(MemoryStore, ChunkAddress)> {
    let store = MemoryStore::default();
    let mut builder = Builder::<V1>::new();
    for (key, entry) in pairs {
        builder.insert(Key::from(key.clone()), entry.clone(), None);
    }
    let built = block_on(builder.build(&store)).ok()?;
    Some((store, *built.root()))
}

fuzz_target!(
    |input: (Vec<(Vec<u8>, Val)>, Vec<(Vec<u8>, Option<Val>)>)| {
        let (base, delta) = input;

        // The base state, deduplicated the way the builder's sorted map would.
        let mut merged: BTreeMap<Vec<u8>, Entry<V1>> =
            base.into_iter().map(|(k, v)| (k, entry(v))).collect();

        let Some((store, root)) = build(&merged) else {
            return;
        };

        // Stage the changeset and fold the same ops into the expected merged state.
        let mut changeset = Changeset::<V1>::new();
        for (key, op) in delta {
            match op {
                Some(val) => {
                    let e = entry(val);
                    changeset.put(Key::from(key.clone()), e.clone(), None);
                    merged.insert(key, e);
                }
                None => {
                    changeset.remove(Key::from(key.clone()));
                    merged.remove(&key);
                }
            }
        }

        let applied = block_on(apply(&store, &root, &changeset));
        let rebuilt = build(&merged).map(|(_, root)| root);

        if let (Ok(applied), Some(rebuilt)) = (applied, rebuilt) {
            assert_eq!(applied, rebuilt, "apply must equal a from-scratch rebuild");
        }
    }
);
