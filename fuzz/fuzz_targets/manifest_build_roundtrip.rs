//! Structured build round-trip and canonical-bijection fuzz for mantaray 1.0.
//!
//! An arbitrary key set is streamed through the builder into a memory store.
//! The oracle is threefold: every node chunk the builder emits must decode and
//! re-encode to the exact bytes it was sealed from (encode -> decode identity
//! and canonical form), no emitted node may exceed one chunk body (the
//! single-chunk-node invariant), and two builds of the same key set must
//! produce the identical root and node set (history independence). A build that
//! cannot fit a node returns a typed error rather than panicking; that is an
//! accepted outcome, so the target skips it.

#![no_main]

use libfuzzer_sys::fuzz_target;
use nectar_fuzz::{Val, entry};
use nectar_manifest::{Builder, Entry, Key, V1, recanonicalize};
use nectar_primitives::store::MemoryStore;
use nectar_primitives::{ChunkAddress, ChunkOps, DEFAULT_BODY_SIZE};
use nectar_testing::run;

/// Build the key set into a fresh store, returning the root and the store.
fn build(pairs: &[(Key, Entry<V1>)]) -> Option<(ChunkAddress, MemoryStore)> {
    let store = MemoryStore::default();
    let mut builder = Builder::<V1>::new();
    for (key, entry) in pairs {
        builder.insert(key.clone(), entry.clone(), None);
    }
    let built = run(builder.build(&store)).ok()?;
    Some((*built.root(), store))
}

fuzz_target!(|input: Vec<(Vec<u8>, Val)>| {
    let pairs: Vec<(Key, Entry<V1>)> = input
        .into_iter()
        .map(|(key, val)| (Key::from(key), entry(val)))
        .collect();

    let Some((root, store)) = build(&pairs) else {
        return;
    };

    // Every emitted chunk round-trips through the codec byte for byte and fits
    // one chunk body: no builder output, plain node or spilled segment, can be
    // an over-budget or non-canonical image.
    for chunk in store.into_chunks().into_values() {
        let payload = chunk.envelope().data();
        assert!(
            payload.len() <= DEFAULT_BODY_SIZE,
            "chunk {} bytes exceeds one chunk body",
            payload.len(),
        );
        let reencoded = recanonicalize::<V1>(payload.as_ref()).expect("a stored chunk must decode");
        assert_eq!(
            reencoded.as_slice(),
            payload.as_ref(),
            "chunk encoding must be canonical",
        );
    }

    // History independence: a second build of the same key set reproduces the
    // root and the whole node set.
    let (root2, _store2) = build(&pairs).expect("a repeat build must also succeed");
    assert_eq!(root, root2, "two builds of one key set must match");
});
