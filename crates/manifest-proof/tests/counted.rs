//! Authenticated rank, count, select and pagination proofs, checked against
//! the streaming reader as the oracle. Tests report
//! failures as errors rather than panicking, so the runtime-safety lints hold in
//! test code too.
//!
//! Counted proofs are honest-builder sound: the oracle here builds honest
//! manifests, so a proof must agree with it, and a hand-inflated on-path count
//! must be rejected by the cross-check.

use std::collections::BTreeMap;
use std::error::Error;

use futures::executor::block_on;
use nectar_manifest::{Builder, Child, Entry, ForkTable, Key, Node, Reader, V1};
use nectar_manifest_proof::{
    CountedPath, PageProof, RankProof, VerifyError, prove_count, prove_page, prove_page_prefix,
    prove_rank, prove_select, verify_count, verify_page, verify_page_prefix, verify_rank,
    verify_select,
};
use nectar_primitives::store::{ChunkGet, MemoryStore};
use nectar_primitives::{ChunkAddress, ChunkOps, ChunkRef, ContentChunk, DEFAULT_BODY_SIZE};
use proptest::prelude::*;

type TestResult = Result<(), Box<dyn Error>>;
type Map = BTreeMap<ChunkAddress, Node<V1>>;
type Listing = Vec<(Key, Entry<V1>)>;

/// A `usize` widened to the `u64` the rank API speaks, saturating rather than
/// wrapping so the runtime-safety lints stay green in test code.
fn as_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

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
fn entry(byte: u8) -> Entry<V1> {
    ChunkRef::new(ChunkAddress::new([byte; 32])).into()
}

/// Build a counted manifest from `pairs` into a fresh store and return its root
/// plus a map of every plain node reachable from the root. `None` when a node
/// spilled (out of scope here).
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

/// The reader over a store, the oracle every proof must agree with.
const fn reader(store: &MemoryStore) -> Reader<&MemoryStore, V1> {
    Reader::new(store)
}

/// The reader's whole ordered listing, the ground truth for select and page.
fn listing(store: &MemoryStore, root: &ChunkAddress) -> Result<Listing, Box<dyn Error>> {
    let reader = reader(store);
    let mut cursor = block_on(reader.range(root, &Key::empty(), &Key::from(&[0xFFu8; 8][..])))?;
    let mut out = Vec::new();
    while let Some(pair) = block_on(cursor.next())? {
        out.push(pair);
    }
    Ok(out)
}

/// Every rank probe verifies against the reader, and count over a window is the
/// difference of the two.
fn check_rank_and_count(
    store: &MemoryStore,
    root: &ChunkAddress,
    map: &Map,
    probes: &[&[u8]],
) -> TestResult {
    let reader = reader(store);
    let src = source(map);
    for probe in probes {
        let key = Key::from(*probe);
        let want = block_on(reader.rank(root, &key))?;
        let proof = prove_rank::<V1, _>(&src, root, &key)?;
        ensure_eq(&verify_rank::<V1>(root, &key, &proof)?, &want, "rank")?;
    }
    // Count over each ordered pair of probes matches rank(hi) - rank(lo).
    for lo in probes {
        for hi in probes {
            let lo = Key::from(*lo);
            let hi = Key::from(*hi);
            let want = block_on(reader.count(root, &lo, &hi))?;
            let proof = prove_count::<V1, _>(&src, root, &lo, &hi)?;
            ensure_eq(&verify_count::<V1>(root, &lo, &hi, &proof)?, &want, "count")?;
        }
    }
    Ok(())
}

/// Every in-range index selects the reader's key, and an out-of-range index is
/// refused a proof.
fn check_select(store: &MemoryStore, root: &ChunkAddress, map: &Map) -> TestResult {
    let reader = reader(store);
    let src = source(map);
    let total = listing(store, root)?.len();
    for index in 0..total {
        let idx = as_u64(index);
        let want = block_on(reader.select(root, idx))?.ok_or("reader select gap")?;
        let proof = prove_select::<V1, _>(&src, root, idx)?;
        ensure_eq(&verify_select::<V1>(root, idx, &proof)?, &want, "select")?;
    }
    // The first index past the last key has no proof.
    ensure(
        prove_select::<V1, _>(&src, root, as_u64(total)).is_err(),
        "an out-of-range index must not admit a select proof",
    )?;
    Ok(())
}

#[test]
fn rank_count_and_select_agree_with_the_reader() -> TestResult {
    let pairs = vec![
        (b"about".to_vec(), 0xD4),
        (b"about/team".to_vec(), 0xE5),
        (b"img/icon.svg".to_vec(), 0xC3),
        (b"img/logo.png".to_vec(), 0xB2),
        (b"index.html".to_vec(), 0xA1),
    ];
    let (store, root, map) = build(&pairs).ok_or("unexpected spill")?;
    check_rank_and_count(
        &store,
        &root,
        &map,
        &[
            &b""[..],
            &b"about"[..],
            &b"about/team"[..],
            &b"img/"[..],
            &b"index.html"[..],
            &b"zzz"[..],
        ],
    )?;
    check_select(&store, &root, &map)?;
    Ok(())
}

#[test]
fn a_root_entry_leads_the_ordering() -> TestResult {
    // An empty key rides in the root extension: it is select index zero and lifts
    // every other rank by one.
    let pairs = vec![
        (b"".to_vec(), 0x99),
        (b"a".to_vec(), 0x11),
        (b"b".to_vec(), 0x22),
    ];
    let (store, root, map) = build(&pairs).ok_or("unexpected spill")?;
    let reader = reader(&store);
    let src = source(&map);

    let proof = prove_select::<V1, _>(&src, &root, 0)?;
    ensure_eq(
        &verify_select::<V1>(&root, 0, &proof)?,
        &(Key::empty(), entry(0x99)),
        "root entry is index zero",
    )?;
    for key in [&b""[..], &b"a"[..], &b"b"[..], &b"c"[..]] {
        let key = Key::from(key);
        let proof = prove_rank::<V1, _>(&src, &root, &key)?;
        ensure_eq(
            &verify_rank::<V1>(&root, &key, &proof)?,
            &block_on(reader.rank(&root, &key))?,
            "rank with a root entry",
        )?;
    }
    check_select(&store, &root, &map)?;
    Ok(())
}

#[test]
fn a_referenced_child_yields_a_multi_node_descent() -> TestResult {
    // Keys under one leading byte, enough that the shared subtree spills to its
    // own chunk, so a rank descent crosses a referenced hop.
    let pairs: Vec<(Vec<u8>, u8)> = (0u8..55).map(|i| (vec![b'a', i], i)).collect();
    let (store, root, map) = build(&pairs).ok_or("unexpected spill")?;
    let src = source(&map);

    let key = Key::from(&[b'a', 5][..]);
    let proof = prove_rank::<V1, _>(&src, &root, &key)?;
    ensure(
        proof.path().len() >= 2,
        "a referenced child must yield a path of at least two nodes",
    )?;
    let reader = reader(&store);
    ensure_eq(
        &verify_rank::<V1>(&root, &key, &proof)?,
        &block_on(reader.rank(&root, &key))?,
        "rank across a referenced hop",
    )?;
    check_select(&store, &root, &map)?;
    Ok(())
}

/// The content address of a node payload.
fn address_of(payload: &[u8]) -> Result<ChunkAddress, Box<dyn Error>> {
    let chunk = ContentChunk::<DEFAULT_BODY_SIZE>::new(payload.to_vec())?;
    Ok(*chunk.address())
}

#[test]
fn an_on_path_count_inconsistency_is_rejected() -> TestResult {
    // A root whose single fork references the whole subtree: its trailing
    // child_count is the final payload byte. Inflating it past the child's real
    // total and re-addressing the root leaves the referenced child unchanged, so
    // the descent fetches it and the cross-check catches the lie.
    let pairs: Vec<(Vec<u8>, u8)> = (0u8..55).map(|i| (vec![b'a', i], i)).collect();
    let (_store, root, map) = build(&pairs).ok_or("unexpected spill")?;
    let src = source(&map);
    let key = Key::from(&[b'a', 5][..]);

    let honest = prove_rank::<V1, _>(&src, &root, &key)?;
    let mut nodes: Vec<Vec<u8>> = honest.path().nodes().to_vec();
    ensure(nodes.len() >= 2, "the descent must reach the child")?;

    // The honest child_count is 55 keys, a single trailing byte; bump it.
    let root_bytes = nodes.first_mut().ok_or("no root node")?;
    ensure_eq(
        &root_bytes.last().copied(),
        &Some(55u8),
        "the trailing byte is the subtree count",
    )?;
    if let Some(byte) = root_bytes.last_mut() {
        *byte = 100;
    }
    let inflated_root = address_of(root_bytes)?;
    let tampered = RankProof::new(CountedPath::new(nodes));

    ensure(
        matches!(
            verify_rank::<V1>(&inflated_root, &key, &tampered),
            Err(VerifyError::CountMismatch)
        ),
        "an inflated on-path count must be rejected by the cross-check",
    )?;
    Ok(())
}

#[test]
fn a_tampered_or_wrong_root_rank_proof_is_rejected() -> TestResult {
    let pairs: Vec<(Vec<u8>, u8)> = (0u8..55).map(|i| (vec![b'a', i], i)).collect();
    let (_store, root, map) = build(&pairs).ok_or("unexpected spill")?;
    let src = source(&map);
    let key = Key::from(&[b'a', 5][..]);
    let proof = prove_rank::<V1, _>(&src, &root, &key)?;

    // A byte flipped in the intermediate node breaks its authentication.
    let mut nodes: Vec<Vec<u8>> = proof.path().nodes().to_vec();
    if let Some(child) = nodes.get_mut(1)
        && let Some(byte) = child.last_mut()
    {
        *byte ^= 0xFF;
    }
    ensure(
        verify_rank::<V1>(&root, &key, &RankProof::new(CountedPath::new(nodes))).is_err(),
        "a tampered node must be rejected",
    )?;
    // A wrong root breaks the very first hop.
    ensure(
        verify_rank::<V1>(&ChunkAddress::new([0x99; 32]), &key, &proof).is_err(),
        "a wrong root must be rejected",
    )?;
    Ok(())
}

/// Collect a reader page over `[lo, hi)`.
fn page_oracle(
    store: &MemoryStore,
    root: &ChunkAddress,
    lo: &Key,
    hi: &Key,
    offset: u64,
    limit: usize,
) -> Result<Listing, Box<dyn Error>> {
    let reader = reader(store);
    let mut cursor = block_on(reader.paginate(root, lo, hi, offset, limit))?;
    let mut out = Vec::new();
    while let Some(pair) = block_on(cursor.next())? {
        out.push(pair);
    }
    Ok(out)
}

/// Collect a reader prefix page.
fn prefix_oracle(
    store: &MemoryStore,
    root: &ChunkAddress,
    prefix: &Key,
    offset: u64,
    limit: usize,
) -> Result<Listing, Box<dyn Error>> {
    let reader = reader(store);
    let mut cursor = block_on(reader.paginate_prefix(root, prefix, offset, limit))?;
    let mut out = Vec::new();
    while let Some(pair) = block_on(cursor.next())? {
        out.push(pair);
    }
    Ok(out)
}

#[test]
fn page_proofs_match_the_true_slice() -> TestResult {
    let pairs: Vec<(Vec<u8>, u8)> = (0u8..60)
        .map(|i| (vec![b'a', i], i))
        .chain((0u8..60).map(|i| (vec![b'b', i], 100u8.wrapping_add(i))))
        .collect();
    let (store, root, map) = build(&pairs).ok_or("unexpected spill")?;
    let src = source(&map);

    // Range pages: whole window and sub-windows, at several offsets and limits.
    let lo = Key::from(&b""[..]);
    let hi = Key::from(&[0xFFu8; 8][..]);
    for (offset, limit) in [(0u64, 5usize), (10, 7), (50, 20), (119, 4), (200, 3)] {
        let want = page_oracle(&store, &root, &lo, &hi, offset, limit)?;
        let proof = prove_page::<V1, _>(&src, &root, &lo, &hi, offset, limit)?;
        ensure_eq(
            &verify_page::<V1>(&root, &lo, &hi, offset, limit, &proof)?,
            &want,
            "range page",
        )?;
    }

    // A bounded sub-window that starts and ends inside the key set.
    let sub_lo = Key::from(&[b'a', 10][..]);
    let sub_hi = Key::from(&[b'b', 20][..]);
    for (offset, limit) in [(0u64, 8usize), (30, 10), (70, 5)] {
        let want = page_oracle(&store, &root, &sub_lo, &sub_hi, offset, limit)?;
        let proof = prove_page::<V1, _>(&src, &root, &sub_lo, &sub_hi, offset, limit)?;
        ensure_eq(
            &verify_page::<V1>(&root, &sub_lo, &sub_hi, offset, limit, &proof)?,
            &want,
            "sub-window page",
        )?;
    }

    // Prefix pages, including the whole-manifest empty prefix (unbounded above)
    // and a single-letter prefix (bounded by its successor).
    for prefix in [&b""[..], &b"a"[..], &b"b"[..]] {
        let prefix = Key::from(prefix);
        for (offset, limit) in [(0u64, 6usize), (25, 9), (100, 4)] {
            let want = prefix_oracle(&store, &root, &prefix, offset, limit)?;
            let proof = prove_page_prefix::<V1, _>(&src, &root, &prefix, offset, limit)?;
            ensure_eq(
                &verify_page_prefix::<V1>(&root, &prefix, offset, limit, &proof)?,
                &want,
                "prefix page",
            )?;
        }
    }
    Ok(())
}

#[test]
fn a_page_with_a_dropped_entry_is_rejected() -> TestResult {
    let pairs: Vec<(Vec<u8>, u8)> = (0u8..60).map(|i| (vec![b'a', i], i)).collect();
    let (_store, root, map) = build(&pairs).ok_or("unexpected spill")?;
    let src = source(&map);
    let lo = Key::from(&b""[..]);
    let hi = Key::from(&[0xFFu8; 8][..]);

    let proof = prove_page::<V1, _>(&src, &root, &lo, &hi, 0, 6)?;
    ensure(
        proof.entries().len() == 6,
        "the page proof carries one descent per returned key",
    )?;
    // Dropping a returned-key descent leaves fewer entries than the proven window
    // demands, so the slice no longer matches its length.
    let kept: Vec<CountedPath> = proof.entries().iter().skip(1).cloned().collect();
    let short = PageProof::new(proof.lo().clone(), proof.hi().cloned(), kept);
    ensure(
        matches!(
            verify_page::<V1>(&root, &lo, &hi, 0, 6, &short),
            Err(VerifyError::PageShape)
        ),
        "a page missing a returned key must be rejected",
    )?;
    Ok(())
}

#[test]
fn adversarial_bytes_are_rejected_not_panicked() -> TestResult {
    // A rank proof over untrusted node bytes must error, never panic or index out
    // of bounds. Each payload is addressed honestly so authentication passes and
    // the flattening parser itself faces the hostile bytes.
    let key = Key::from(&b"anything"[..]);
    let cases: Vec<Vec<u8>> = vec![
        vec![],
        vec![0x6D],
        vec![0x6D, 0x01],
        vec![0x6D, 0x01, 0x00],
        vec![0x6D, 0x01, 0x40],
        vec![0x6D, 0x01, 0x20],
        vec![0x6D, 0x01, 0x00, 0x01, 0x00],
        vec![0x6D, 0x01, 0x00, 0x01, 0x00, 0x61, 0x00, 0x00],
        vec![0xFF; 4096],
    ];
    for bytes in &cases {
        let root = address_of(bytes)?;
        let proof = RankProof::new(CountedPath::new(vec![bytes.clone()]));
        // Either the parse rejects the bytes, or it yields some rank without
        // panicking; both are acceptable, a panic is not.
        let _ = verify_rank::<V1>(&root, &key, &proof);
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    #[test]
    fn counted_proofs_agree_with_the_reader(
        pairs in proptest::collection::vec(
            (proptest::collection::vec(any::<u8>(), 1..5), any::<u8>()),
            1..24,
        ),
        offset in 0u64..8,
        limit in 1usize..6,
    ) {
        let dedup: BTreeMap<Vec<u8>, u8> = pairs.into_iter().collect();
        let pairs: Vec<(Vec<u8>, u8)> = dedup.into_iter().collect();
        if let Some((store, root, map)) = build(&pairs) {
            let run = || -> TestResult {
                let src = source(&map);
                let reader = reader(&store);
                let keys = listing(&store, &root)?;
                // Rank and select at every key and one probe past the end.
                for (index, (key, value)) in keys.iter().enumerate() {
                    let idx = as_u64(index);
                    let rank_proof = prove_rank::<V1, _>(&src, &root, key)?;
                    ensure_eq(
                        &verify_rank::<V1>(&root, key, &rank_proof)?,
                        &block_on(reader.rank(&root, key))?,
                        "rank",
                    )?;
                    let select_proof = prove_select::<V1, _>(&src, &root, idx)?;
                    ensure_eq(
                        &verify_select::<V1>(&root, idx, &select_proof)?,
                        &(key.clone(), value.clone()),
                        "select",
                    )?;
                }
                // A whole-manifest prefix page against the reader.
                let prefix = Key::empty();
                let want = prefix_oracle(&store, &root, &prefix, offset, limit)?;
                let proof = prove_page_prefix::<V1, _>(&src, &root, &prefix, offset, limit)?;
                ensure_eq(
                    &verify_page_prefix::<V1>(&root, &prefix, offset, limit, &proof)?,
                    &want,
                    "prefix page",
                )?;
                Ok(())
            };
            run().map_err(|e| TestCaseError::fail(e.to_string()))?;
        }
    }
}
