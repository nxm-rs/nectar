//! Round-trip, tamper-rejection and exclusion-soundness properties, checked
//! against the streaming reader as the oracle. Tests report failures as errors
//! rather than panicking, so the runtime-safety lints hold in test code too.

use std::collections::BTreeMap;
use std::error::Error;

use alloy_primitives::B256;
use futures::executor::block_on;
use nectar_manifest::{Builder, Child, Entry, ForkTable, Key, Node, Reader, V1};
use nectar_manifest_proof::{
    ForkPathProof, Granularity, PathStep, Verdict, prove_exclusion, prove_inclusion, verify,
};
use nectar_primitives::store::{ChunkGet, MemoryStore};
use nectar_primitives::{ChunkAddress, ChunkOps, ChunkRef};
use proptest::prelude::*;

type TestResult = Result<(), Box<dyn Error>>;
type Map = BTreeMap<ChunkAddress, Node<V1>>;

/// A fallible assertion.
fn ensure(cond: bool, what: &str) -> TestResult {
    if cond { Ok(()) } else { Err(what.into()) }
}

/// A fallible equality assertion.
fn ensure_eq<T: PartialEq + core::fmt::Debug>(left: &T, right: &T, what: &str) -> TestResult {
    if left == right {
        Ok(())
    } else {
        Err(format!("{what}: {left:?} != {right:?}").into())
    }
}

/// A ref32 entry keyed on a value byte, distinct per value so equality is
/// meaningful.
fn entry(byte: u8) -> Entry {
    ChunkRef::new(ChunkAddress::new([byte; 32])).into()
}

/// Build a manifest from `pairs` into a fresh store and return its root plus a
/// map of every plain node reachable from the root. Returns `None` when a node
/// spilled (out of scope here), so a case can skip it.
fn build(pairs: &[(Vec<u8>, u8)]) -> Option<(MemoryStore, ChunkAddress, Map)> {
    let store = MemoryStore::default();
    let mut builder = Builder::<V1>::new();
    for (key, value) in pairs {
        builder.insert(Key::from(key.as_slice()), entry(*value), None);
    }
    let built = block_on(builder.build(&store)).ok()?;
    let root = *built.root();
    let mut map = Map::new();
    collect(&store, &root, &mut map)?;
    Some((store, root, map))
}

/// Fetch and decode the node at `address`, recording it and every plain child
/// it references. Returns `None` on a spilled node.
fn collect(store: &MemoryStore, address: &ChunkAddress, map: &mut Map) -> Option<()> {
    if map.contains_key(address) {
        return Some(());
    }
    let chunk = block_on(ChunkGet::get(store, address)).ok()?;
    let node = Node::<V1>::decode(chunk.envelope().data()).ok()?;
    map.insert(*address, node.clone());
    let mut children = Vec::new();
    gather(node.forks(), &mut children);
    for child in children {
        collect(store, &child, map)?;
    }
    Some(())
}

/// Append every referenced child address under `table`, descending embedded
/// tables in place.
fn gather(table: &ForkTable<V1>, out: &mut Vec<ChunkAddress>) {
    for (_, record) in table.iter() {
        match record.child() {
            Some(Child::Ref32(reference)) => out.push(*reference.address()),
            Some(Child::Embedded(inner)) => gather(inner, out),
            _ => {}
        }
    }
}

/// The reader's answer for `key`, the oracle a proof must agree with.
fn oracle(
    store: &MemoryStore,
    root: &ChunkAddress,
    key: &Key,
) -> Result<Option<Entry>, Box<dyn Error>> {
    let reader: Reader<_> = Reader::new(store);
    Ok(block_on(reader.get(root, key))?)
}

/// A source closure over a node map.
fn source(map: &Map) -> impl Fn(&ChunkAddress) -> Option<Node<V1>> + '_ {
    move |address: &ChunkAddress| map.get(address).cloned()
}

/// Prove and verify `key` at both granularities, asserting each verdict matches
/// the oracle and that the opposite proof is refused.
fn check_key(store: &MemoryStore, root: &ChunkAddress, map: &Map, key: &Key) -> TestResult {
    let want = oracle(store, root, key)?;
    let src = source(map);
    for granularity in [Granularity::Chunk, Granularity::Segment] {
        match &want {
            Some(value) => {
                let proof = prove_inclusion(&src, root, key, granularity)?;
                ensure_eq(
                    &verify(root, key, &proof)?,
                    &Verdict::Present(value.clone()),
                    "inclusion verdict",
                )?;
                ensure(
                    prove_exclusion(&src, root, key, granularity).is_err(),
                    "a present key must not admit an exclusion proof",
                )?;
            }
            None => {
                let proof = prove_exclusion(&src, root, key, granularity)?;
                ensure_eq(
                    &verify::<V1>(root, key, &proof)?,
                    &Verdict::Absent,
                    "exclusion verdict",
                )?;
                ensure(
                    prove_inclusion(&src, root, key, granularity).is_err(),
                    "an absent key must not admit an inclusion proof",
                )?;
            }
        }
    }
    Ok(())
}

/// Flip a byte inside the proof so no node authenticates, whatever the
/// granularity.
fn tamper(proof: &ForkPathProof) -> ForkPathProof {
    let mut steps: Vec<PathStep> = proof.steps().to_vec();
    if let Some(last) = steps.last_mut() {
        match last {
            PathStep::Chunk { payload } => {
                if let Some(byte) = payload.last_mut() {
                    *byte ^= 0xFF;
                }
            }
            PathStep::Segment { segments } => {
                if let Some(seg) = segments.first_mut() {
                    let mut bytes = seg.segment.0;
                    if let Some(byte) = bytes.first_mut() {
                        *byte ^= 0xFF;
                    }
                    seg.segment = B256::from(bytes);
                }
            }
            _ => {}
        }
    }
    ForkPathProof::new(steps)
}

#[test]
fn present_and_absent_keys_round_trip_at_both_granularities() -> TestResult {
    let pairs = vec![
        (b"index.html".to_vec(), 0xA1),
        (b"img/logo.png".to_vec(), 0xB2),
        (b"img/icon.svg".to_vec(), 0xC3),
        (b"about".to_vec(), 0xD4),
        (b"about/team".to_vec(), 0xE5),
    ];
    let (store, root, map) = build(&pairs).ok_or("unexpected spill")?;

    for (key, _) in &pairs {
        check_key(&store, &root, &map, &Key::from(key.as_slice()))?;
    }
    // Absent keys covering each exclusion shape: no fork, a key ending inside an
    // edge, a diverging edge, the empty key, and a lone branch byte.
    for absent in [
        &b"missing"[..],
        &b"img/logo"[..],
        &b"index.htmlx"[..],
        &b""[..],
        &b"i"[..],
    ] {
        check_key(&store, &root, &map, &Key::from(absent))?;
    }
    Ok(())
}

#[test]
fn a_tampered_proof_and_a_wrong_root_are_rejected() -> TestResult {
    let pairs = vec![(b"alpha".to_vec(), 0x11), (b"alpha/beta".to_vec(), 0x22)];
    let (_store, root, map) = build(&pairs).ok_or("unexpected spill")?;
    let src = source(&map);
    let key = Key::from(&b"alpha/beta"[..]);

    for granularity in [Granularity::Chunk, Granularity::Segment] {
        let proof = prove_inclusion(&src, &root, &key, granularity)?;
        ensure(
            matches!(verify::<V1>(&root, &key, &proof), Ok(Verdict::Present(_))),
            "the intact proof verifies present",
        )?;
        // Tampered bytes fail to authenticate.
        ensure(
            verify::<V1>(&root, &key, &tamper(&proof)).is_err(),
            "a tampered proof is rejected",
        )?;
        // A wrong root breaks the very first hop.
        let wrong = ChunkAddress::new([0x99; 32]);
        ensure(
            verify::<V1>(&wrong, &key, &proof).is_err(),
            "a wrong root is rejected",
        )?;
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    #[test]
    fn proofs_agree_with_the_reader(
        pairs in proptest::collection::vec(
            (proptest::collection::vec(any::<u8>(), 1..6), any::<u8>()),
            1..24,
        ),
    ) {
        let dedup: BTreeMap<Vec<u8>, u8> = pairs.into_iter().collect();
        let pairs: Vec<(Vec<u8>, u8)> = dedup.into_iter().collect();
        if let Some((store, root, map)) = build(&pairs) {
            let run = || -> TestResult {
                for (key, _) in &pairs {
                    check_key(&store, &root, &map, &Key::from(key.as_slice()))?;
                    // An absent probe derived from each present key.
                    let mut extended = key.clone();
                    extended.push(0x00);
                    check_key(&store, &root, &map, &Key::from(extended.as_slice()))?;
                }
                check_key(&store, &root, &map, &Key::from(&b"\xff\xff\xff"[..]))?;
                Ok(())
            };
            run().map_err(|e| TestCaseError::fail(e.to_string()))?;
        }
    }
}
