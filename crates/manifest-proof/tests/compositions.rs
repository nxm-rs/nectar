//! Range-completeness and state-transition properties, checked against the
//! streaming reader as the oracle. Tests report failures as errors rather than
//! panicking, so the runtime-safety lints hold in test code too.

use std::collections::BTreeMap;
use std::error::Error;

use futures::executor::block_on;
use nectar_manifest::{Builder, Child, Entry, ForkTable, Key, Node, Reader, V1};
use nectar_manifest_proof::{
    Granularity, RangeProof, Transition, prove_deletion, prove_range_complete, prove_transition,
    prove_update, verify_range, verify_transition,
};
use nectar_primitives::store::{ChunkGet, MemoryStore};
use nectar_primitives::{ChunkAddress, ChunkOps, ChunkRef};

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
/// map of every plain node reachable from the root. `None` when a node spilled
/// (out of scope here).
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

/// Fetch and decode the node at `address`, recording it and every plain child it
/// references. `None` on a spilled node.
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

/// A source closure over a node map.
fn source(map: &Map) -> impl Fn(&ChunkAddress) -> Option<Node<V1>> + '_ {
    move |address: &ChunkAddress| map.get(address).cloned()
}

/// The reader's listing for `[lo, hi)`, the oracle a range proof must match.
fn oracle(
    store: &MemoryStore,
    root: &ChunkAddress,
    lo: &Key,
    hi: &Key,
) -> Result<Vec<(Key, Entry)>, Box<dyn Error>> {
    let reader: Reader<_> = Reader::new(store);
    let mut cursor = block_on(reader.range(root, lo, hi))?;
    let mut out = Vec::new();
    while let Some(pair) = block_on(cursor.next())? {
        out.push(pair);
    }
    Ok(out)
}

#[test]
fn a_range_proof_lists_every_key_and_matches_the_reader() -> TestResult {
    let pairs = vec![
        (b"about".to_vec(), 0xD4),
        (b"about/team".to_vec(), 0xE5),
        (b"img/icon.svg".to_vec(), 0xC3),
        (b"img/logo.png".to_vec(), 0xB2),
        (b"index.html".to_vec(), 0xA1),
    ];
    let (store, root, map) = build(&pairs).ok_or("unexpected spill")?;
    let src = source(&map);

    // A spread of half-open windows, including empty and whole-map bounds.
    for (lo, hi) in [
        (&b""[..], &b"\xff\xff"[..]),
        (&b"about"[..], &b"index.html"[..]),
        (&b"img/"[..], &b"img/\xff"[..]),
        (&b"about/team"[..], &b"about/team"[..]),
        (&b"z"[..], &b"zz"[..]),
    ] {
        let lo = Key::from(lo);
        let hi = Key::from(hi);
        let want = oracle(&store, &root, &lo, &hi)?;
        let proof = prove_range_complete::<V1, _>(&src, &root, &lo, &hi)?;
        let got = verify_range::<V1>(&root, &lo, &hi, &proof)?;
        ensure_eq(&got, &want, "range listing")?;
    }
    Ok(())
}

#[test]
fn an_omitted_frontier_node_is_rejected() -> TestResult {
    // Enough keys under a shared first byte that the shared subtree spills into
    // its own chunk, so the frontier has more than the root and a referenced
    // child node can be withheld.
    let pairs: Vec<(Vec<u8>, u8)> = (0u8..55).map(|i| (vec![b'a', i], i)).collect();
    let (store, root, map) = build(&pairs).ok_or("unexpected spill")?;
    let src = source(&map);
    let lo = Key::from(&b""[..]);
    let hi = Key::from(&b"\xff\xff"[..]);

    let proof = prove_range_complete::<V1, _>(&src, &root, &lo, &hi)?;
    ensure(
        proof.len() >= 2,
        "a referenced child must yield a multi-node frontier",
    )?;
    // The intact proof lists every key.
    let want = oracle(&store, &root, &lo, &hi)?;
    ensure_eq(
        &verify_range::<V1>(&root, &lo, &hi, &proof)?,
        &want,
        "intact range listing",
    )?;

    // Dropping any non-root frontier node leaves an in-range edge with no
    // witness, so the listing can no longer be shown complete.
    for drop in 1..proof.len() {
        let kept: Vec<Vec<u8>> = proof
            .nodes()
            .iter()
            .enumerate()
            .filter(|&(i, _)| i != drop)
            .map(|(_, node)| node.clone())
            .collect();
        ensure(
            verify_range::<V1>(&root, &lo, &hi, &RangeProof::new(kept)).is_err(),
            "an omitted frontier node must be rejected",
        )?;
    }
    Ok(())
}

#[test]
fn a_tampered_frontier_node_is_rejected() -> TestResult {
    let pairs = vec![(b"alpha".to_vec(), 0x11), (b"beta".to_vec(), 0x22)];
    let (_store, root, map) = build(&pairs).ok_or("unexpected spill")?;
    let src = source(&map);
    let lo = Key::from(&b""[..]);
    let hi = Key::from(&b"\xff"[..]);

    let proof = prove_range_complete::<V1, _>(&src, &root, &lo, &hi)?;
    let mut nodes: Vec<Vec<u8>> = proof.nodes().to_vec();
    // Flip a byte in the root node: its content address no longer matches the
    // trusted root, so the walk cannot start.
    if let Some(node) = nodes.first_mut()
        && let Some(byte) = node.last_mut()
    {
        *byte ^= 0xFF;
    }
    ensure(
        verify_range::<V1>(&root, &lo, &hi, &RangeProof::new(nodes)).is_err(),
        "a tampered frontier node must be rejected",
    )?;
    Ok(())
}

#[test]
fn boundary_windows_over_a_multi_node_frontier_match_the_reader() -> TestResult {
    // Two spilled letter-subtrees give the root two referenced children, so a
    // window whose bound falls inside one subtree exercises the overlap test at
    // a real referenced edge rather than only over the whole-map frontier.
    let mut pairs: Vec<(Vec<u8>, u8)> = Vec::new();
    for i in 0u8..60 {
        pairs.push((vec![b'a', i], i));
    }
    for i in 0u8..60 {
        pairs.push((vec![b'b', i], 100u8.wrapping_add(i)));
    }
    let (store, root, map) = build(&pairs).ok_or("unexpected spill")?;
    let src = source(&map);

    let full = prove_range_complete::<V1, _>(
        &src,
        &root,
        &Key::from(&b""[..]),
        &Key::from(&b"\xff\xff"[..]),
    )?;
    ensure(
        full.len() >= 3,
        "two spilled subtrees must give a root plus two child nodes",
    )?;

    // Bounds that cut through a subtree, span both, or land in the empty gap
    // between them: each descends a referenced child on the boundary.
    for (lo, hi) in [
        (&b""[..], &b"b\x10"[..]),
        (&b"a\x14"[..], &b"c"[..]),
        (&b"a\x30"[..], &b"b\x05"[..]),
        (&b"a\xff"[..], &b"b\x00"[..]),
        (&b""[..], &b"a\x00"[..]),
    ] {
        let lo = Key::from(lo);
        let hi = Key::from(hi);
        let want = oracle(&store, &root, &lo, &hi)?;
        let proof = prove_range_complete::<V1, _>(&src, &root, &lo, &hi)?;
        let got = verify_range::<V1>(&root, &lo, &hi, &proof)?;
        ensure_eq(&got, &want, "boundary window listing")?;
    }
    Ok(())
}

#[test]
fn adversarial_frontier_bytes_are_rejected_not_panicked() -> TestResult {
    // verify_range decodes untrusted node bytes; truncated or malformed payloads
    // must return an error, never panic or index out of bounds.
    let root = ChunkAddress::new([7u8; 32]);
    let lo = Key::from(&b""[..]);
    let hi = Key::from(&b"\xff\xff"[..]);
    let cases: Vec<Vec<u8>> = vec![
        vec![],
        vec![0],
        vec![0x9a, 0x01],
        vec![0x9a, 0x01, 0x00],
        vec![0x40],
        vec![0x20],
        vec![1, 2, 3, 4, 5, 6, 7, 8],
        vec![0xff; 5000],
    ];
    for bytes in &cases {
        let proof = RangeProof::new(vec![bytes.clone()]);
        ensure(
            verify_range::<V1>(&root, &lo, &hi, &proof).is_err(),
            "adversarial frontier bytes must be rejected",
        )?;
    }
    Ok(())
}

#[test]
fn a_range_with_lo_above_hi_lists_nothing() -> TestResult {
    let pairs = vec![
        (b"a".to_vec(), 0x01),
        (b"b".to_vec(), 0x02),
        (b"c".to_vec(), 0x03),
    ];
    let (_store, root, map) = build(&pairs).ok_or("unexpected spill")?;
    let src = source(&map);
    let lo = Key::from(&b"z"[..]);
    let hi = Key::from(&b"a"[..]);
    let proof = prove_range_complete::<V1, _>(&src, &root, &lo, &hi)?;
    let got = verify_range::<V1>(&root, &lo, &hi, &proof)?;
    ensure(got.is_empty(), "a range with lo above hi lists nothing")?;
    Ok(())
}

/// The two roots of a manifest with `key` inserted, plus their node sources.
fn before_after(
    base: &[(Vec<u8>, u8)],
    key: &[u8],
    value: u8,
) -> Option<(ChunkAddress, Map, ChunkAddress, Map)> {
    let (_s1, root_before, map_before) = build(base)?;
    let mut after = base.to_vec();
    after.push((key.to_vec(), value));
    let (_s2, root_after, map_after) = build(&after)?;
    Some((root_before, map_before, root_after, map_after))
}

#[test]
fn an_insertion_transition_verifies_and_a_false_one_is_rejected() -> TestResult {
    let base = vec![(b"a".to_vec(), 0x01), (b"c".to_vec(), 0x03)];
    let key = Key::from(&b"b"[..]);
    let (r1, m1, r2, m2) = before_after(&base, b"b", 0x02).ok_or("unexpected spill")?;
    let (s1, s2) = (source(&m1), source(&m2));

    for granularity in [Granularity::Chunk, Granularity::Segment] {
        let proof = prove_transition::<V1, _, _>(&s1, &r1, &s2, &r2, &key, granularity)?;
        ensure_eq(
            &verify_transition::<V1>(&r1, &r2, &key, &proof)?,
            &Transition::Insertion(entry(0x02)),
            "insertion",
        )?;
        // Swapping the roots claims the key was inserted the other way: it is
        // present under r2 and absent under r1, so neither half fits the shape.
        ensure(
            verify_transition::<V1>(&r2, &r1, &key, &proof).is_err(),
            "a reversed-root insertion must be rejected",
        )?;
    }

    // A key already present under the first root has no insertion: the exclusion
    // half cannot be built at all.
    let present = Key::from(&b"a"[..]);
    ensure(
        prove_transition::<V1, _, _>(&s1, &r1, &s2, &r2, &present, Granularity::Chunk).is_err(),
        "a present key must not admit an insertion proof",
    )?;
    Ok(())
}

#[test]
fn deletion_and_update_duals_verify() -> TestResult {
    let base = vec![(b"a".to_vec(), 0x01), (b"b".to_vec(), 0x02)];
    let (_s0, root_with, map_with) = build(&base).ok_or("unexpected spill")?;
    let without = vec![(b"a".to_vec(), 0x01)];
    let (_s1, root_without, map_without) = build(&without).ok_or("unexpected spill")?;
    let updated = vec![(b"a".to_vec(), 0x01), (b"b".to_vec(), 0x2B)];
    let (_s2, root_updated, map_updated) = build(&updated).ok_or("unexpected spill")?;

    let with = source(&map_with);
    let missing = source(&map_without);
    let changed = source(&map_updated);
    let key = Key::from(&b"b"[..]);

    // Deletion: present under the first root, absent under the second.
    let deletion = prove_deletion::<V1, _, _>(
        &with,
        &root_with,
        &missing,
        &root_without,
        &key,
        Granularity::Segment,
    )?;
    ensure_eq(
        &verify_transition::<V1>(&root_with, &root_without, &key, &deletion)?,
        &Transition::Deletion(entry(0x02)),
        "deletion",
    )?;

    // Update: present under both roots with a changed value.
    let update = prove_update::<V1, _, _>(
        &with,
        &root_with,
        &changed,
        &root_updated,
        &key,
        Granularity::Chunk,
    )?;
    ensure_eq(
        &verify_transition::<V1>(&root_with, &root_updated, &key, &update)?,
        &Transition::Update {
            before: entry(0x02),
            after: entry(0x2B),
        },
        "update",
    )?;

    // An update whose value did not change is a no-op, not a transition.
    let noop = prove_update::<V1, _, _>(
        &with,
        &root_with,
        &with,
        &root_with,
        &key,
        Granularity::Chunk,
    )?;
    ensure(
        verify_transition::<V1>(&root_with, &root_with, &key, &noop).is_err(),
        "an unchanged value must not verify as an update",
    )?;
    Ok(())
}
