//! Emit BMT-segment proof fixtures in the byte layout the Foundry
//! `MantarayProofVerifier` library consumes.
//!
//! This is the authoritative Rust side of the Rust-to-Solidity byte contract:
//! it builds manifests, proves inclusion and exclusion at segment granularity,
//! and serializes each case (root, key, value, proof) into a single binary
//! file under `contracts/test/fixtures/`. The Foundry tests load these with
//! `vm.readFileBinary` and replay the descent on-chain.
//!
//! Wire layout (all multi-byte integers BIG-endian, chosen for cheap Solidity
//! parsing; note the raw mantaray node bytes carried inside each segment keep
//! their own little-endian u16 fields, which the on-chain descent reads as
//! little-endian, matching this crate):
//!
//! ```text
//! fixture := root[32]
//!            u32 key_len  || key[key_len]
//!            u8  present            (1 = inclusion, 0 = exclusion)
//!            u32 value_len || value[value_len]   (value only when present)
//!            proof
//! proof   := u32 n_steps || step[n_steps]
//! step    := u64 span || u32 n_seg || segment[n_seg]
//! segment := data[32] || sibling[7][32]          (segment_index implicit = position)
//! ```
//!
//! The run always starts at segment zero and is contiguous, so the index is
//! positional. The BMT sibling path is exactly seven levels (128 leaves), and
//! the node prefix is empty for content chunks, so none is carried.
//!
//! Regenerating is deterministic: the same key set yields the same node bytes,
//! addresses and proofs, so a re-run leaves the committed fixtures unchanged.
//! When the target directory cannot be created (for example a crate-only
//! checkout without the sibling `contracts/` tree) the test verifies in memory
//! and skips the write rather than failing.

use std::error::Error;
use std::fs;
use std::path::PathBuf;

use nectar_manifest::{Builder, Entry, Key, V1};
use nectar_manifest_proof::{
    ForkPathProof, Granularity, PathStep, Verdict, prove_exclusion, prove_inclusion, verify,
};
use nectar_primitives::store::{ChunkGet, MemoryStore};
use nectar_primitives::{ChunkAddress, ChunkOps, ChunkRef};

use futures::executor::block_on;
use nectar_manifest::{Child, ForkTable, Node, Reader};
use std::collections::BTreeMap;

type TestResult = Result<(), Box<dyn Error>>;
type Map = BTreeMap<ChunkAddress, Node<V1>>;

/// A ref32 entry keyed on a value byte.
fn entry(byte: u8) -> Entry {
    ChunkRef::new(ChunkAddress::new([byte; 32])).into()
}

/// Build a manifest from `pairs`, returning its store, root and a map of every
/// plain node reachable from the root.
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

/// Fetch, decode and record the node at `address` and every plain child it
/// references. Returns `None` on a spilled node.
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

/// The reader's answer for `key`.
fn oracle(
    store: &MemoryStore,
    root: &ChunkAddress,
    key: &Key,
) -> Result<Option<Entry>, Box<dyn Error>> {
    let reader: Reader<_> = Reader::new(store);
    Ok(block_on(reader.get(root, key))?)
}

/// The on-chain value bytes for an entry: the 32-byte reference, or the inline
/// bytes. Encrypted ref64 entries are out of the restricted profile.
fn value_bytes(entry: &Entry) -> Result<Vec<u8>, Box<dyn Error>> {
    match entry {
        Entry::Ref32(reference) => Ok(reference.address().as_bytes().to_vec()),
        Entry::Inline(value) => Ok(value.as_bytes().to_vec()),
        Entry::Ref64(_) => Err("ref64 entry is out of the restricted on-chain profile".into()),
    }
}

/// Append a big-endian `u32`.
fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_be_bytes());
}

/// Append a big-endian u32 length, rejecting a field that overflows it.
fn put_len(out: &mut Vec<u8>, len: usize) -> Result<(), Box<dyn Error>> {
    put_u32(
        out,
        u32::try_from(len).map_err(|_| "fixture field exceeds u32")?,
    );
    Ok(())
}

/// Append a length-prefixed byte string (big-endian u32 length).
fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) -> Result<(), Box<dyn Error>> {
    put_len(out, bytes.len())?;
    out.extend_from_slice(bytes);
    Ok(())
}

/// Serialize a segment-granularity proof into the on-chain wire.
fn encode_proof(proof: &ForkPathProof) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut out = Vec::new();
    put_len(&mut out, proof.len())?;
    for step in proof.steps() {
        let PathStep::Segment { segments } = step else {
            return Err("only segment-granularity proofs are emitted for Solidity".into());
        };
        let span = segments.first().map_or(0, |s| s.span);
        out.extend_from_slice(&span.to_be_bytes());
        put_len(&mut out, segments.len())?;
        for (expected, segment) in segments.iter().enumerate() {
            if segment.segment_index != expected {
                return Err("segment run is not contiguous from zero".into());
            }
            if segment.prefix.is_some() {
                return Err("content-chunk proofs carry no BMT prefix".into());
            }
            out.extend_from_slice(segment.segment.as_slice());
            for sibling in &segment.proof_segments {
                out.extend_from_slice(sibling.as_slice());
            }
        }
    }
    Ok(out)
}

/// Serialize a whole fixture: root, key, presence, value, proof.
fn encode_fixture(
    root: &ChunkAddress,
    key: &Key,
    value: Option<&[u8]>,
    proof: &ForkPathProof,
) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut out = Vec::new();
    out.extend_from_slice(root.as_bytes());
    put_bytes(&mut out, key.as_bytes())?;
    out.push(u8::from(value.is_some()));
    put_bytes(&mut out, value.unwrap_or(&[]))?;
    out.extend_from_slice(&encode_proof(proof)?);
    Ok(out)
}

/// Prove `key` present under `root`, verify the round trip, and return the
/// serialized inclusion fixture alongside the proof it wraps.
fn inclusion_fixture(
    store: &MemoryStore,
    map: &Map,
    root: &ChunkAddress,
    key: &Key,
) -> Result<(Vec<u8>, ForkPathProof), Box<dyn Error>> {
    let want = oracle(store, root, key)?.ok_or("expected a present key")?;
    let value = value_bytes(&want)?;
    let proof = prove_inclusion(&source(map), root, key, Granularity::Segment)?;
    if verify::<V1>(root, key, &proof)? != Verdict::Present(want) {
        return Err("inclusion proof did not verify present".into());
    }
    let bytes = encode_fixture(root, key, Some(&value), &proof)?;
    Ok((bytes, proof))
}

/// Prove `key` absent under `root`, verify the round trip, and return the
/// serialized exclusion fixture.
fn exclusion_fixture(
    store: &MemoryStore,
    map: &Map,
    root: &ChunkAddress,
    key: &Key,
) -> Result<Vec<u8>, Box<dyn Error>> {
    if oracle(store, root, key)?.is_some() {
        return Err("expected an absent key".into());
    }
    let proof = prove_exclusion(&source(map), root, key, Granularity::Segment)?;
    if verify::<V1>(root, key, &proof)? != Verdict::Absent {
        return Err("exclusion proof did not verify absent".into());
    }
    encode_fixture(root, key, None, &proof)
}

/// Flip a byte inside the proof's first segment so no node authenticates.
fn tamper(proof: &ForkPathProof) -> ForkPathProof {
    let mut steps: Vec<PathStep> = proof.steps().to_vec();
    if let Some(PathStep::Segment { segments }) = steps.last_mut()
        && let Some(seg) = segments.first_mut()
    {
        let mut bytes = seg.segment.0;
        if let Some(first) = bytes.first_mut() {
            *first ^= 0xFF;
        }
        seg.segment = alloy_primitives::B256::from(bytes);
    }
    ForkPathProof::new(steps)
}

/// The `contracts/test/fixtures` directory beside this workspace, or `None`
/// when it cannot be created.
fn fixtures_dir() -> Option<PathBuf> {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../contracts/test/fixtures")
        .canonicalize()
        .ok()
        .or_else(|| {
            let raw =
                PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../contracts/test/fixtures");
            fs::create_dir_all(&raw).ok().map(|()| raw)
        })?;
    fs::create_dir_all(&dir).ok()?;
    Some(dir)
}

/// Build both manifests, emit every fixture, and write them when a target
/// directory is available.
#[test]
fn emit_solidity_fixtures() -> TestResult {
    // Manifest A: a single embedded node. Keys share the `index.` edge so the
    // exclusion shapes (gap, divergence, exhausted edge, empty key) all arise.
    let pairs_a = vec![
        (b"index.html".to_vec(), 0xA1u8),
        (b"index.css".to_vec(), 0x0Cu8),
        (b"about".to_vec(), 0xD4u8),
    ];
    let (store_a, root_a, map_a) = build(&pairs_a).ok_or("manifest A unexpectedly spilled")?;

    // Manifest B: many keys sharing a leading byte, so the `a` subtree spills
    // into its own chunk and the descent crosses a referenced hop.
    let pairs_b: Vec<(Vec<u8>, u8)> = (0u8..55).map(|i| (vec![b'a', i], i)).collect();
    let (store_b, root_b, map_b) = build(&pairs_b).ok_or("manifest B unexpectedly spilled")?;

    let mut files: Vec<(&str, Vec<u8>)> = Vec::new();

    // Inclusion, embedded single-node path.
    let key = Key::from(&b"index.html"[..]);
    let (incl_embedded, embedded_proof) = inclusion_fixture(&store_a, &map_a, &root_a, &key)?;
    files.push(("incl_embedded.bin", incl_embedded));

    // A tampered variant of that same proof: one flipped segment byte.
    let tampered = encode_fixture(
        &root_a,
        &key,
        Some(&value_bytes(&entry(0xA1))?),
        &tamper(&embedded_proof),
    )?;
    files.push(("tampered.bin", tampered));

    // Inclusion, referenced multi-node path.
    let key_b = Key::from(&[b'a', 5u8][..]);
    let (incl_referenced, ref_proof) = inclusion_fixture(&store_b, &map_b, &root_b, &key_b)?;
    if ref_proof.len() < 2 {
        return Err("referenced inclusion proof must span at least two nodes".into());
    }
    files.push(("incl_referenced.bin", incl_referenced));

    // Exclusion shapes over manifest A.
    files.push((
        "excl_gap.bin",
        exclusion_fixture(&store_a, &map_a, &root_a, &Key::from(&b"missing"[..]))?,
    ));
    files.push((
        "excl_divergent.bin",
        exclusion_fixture(&store_a, &map_a, &root_a, &Key::from(&b"index.htmlx"[..]))?,
    ));
    files.push((
        "excl_exhausted.bin",
        exclusion_fixture(&store_a, &map_a, &root_a, &Key::from(&b"index"[..]))?,
    ));
    files.push((
        "excl_empty.bin",
        exclusion_fixture(&store_a, &map_a, &root_a, &Key::empty())?,
    ));

    // Exclusion across a referenced hop.
    files.push((
        "excl_referenced.bin",
        exclusion_fixture(&store_b, &map_b, &root_b, &Key::from(&[b'a', 200u8][..]))?,
    ));

    let Some(dir) = fixtures_dir() else {
        // No writable target: the in-memory round trips above are the check.
        return Ok(());
    };
    for (name, bytes) in &files {
        fs::write(dir.join(name), bytes)?;
    }
    Ok(())
}
