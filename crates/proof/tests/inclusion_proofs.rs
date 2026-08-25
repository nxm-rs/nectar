//! 32-byte anchor inclusion-proof vectors from the pinned reference client.
//!
//! The document is `tools/stamp-vectors/testdata/inclusion-proofs-anchor32.json`.
//! It is generated, not copied: the arm re-runs the reference client's
//! inclusion-proof construction with a 32-byte anchor in place of the
//! regression test's one-byte anchor. Its provenance is recorded in the
//! document's `provenance` block. The witness content is the literal
//! `Unstoppable data! Chunk #1..16` chunks the regression test builds, with
//! its identifiers, secp256k1 key and stamp fields. The reference sorts the
//! sample items by transformed address, so the witness at each proof
//! position depends on the anchor and the roots taken from the document.

#![allow(clippy::expect_used)]

use alloy_primitives::{B256, hex, keccak256};
use nectar_proof::{Segment, Verifier};
use serde::Deserialize;

const DOCUMENT: &str =
    include_str!("../../../tools/stamp-vectors/testdata/inclusion-proofs-anchor32.json");

// The arm's fixed anchor: 31 zero bytes then one.
const ANCHOR: [u8; 32] = {
    let mut bytes = [0u8; 32];
    bytes[31] = 1;
    bytes
};
// The level-one segment index of each witness, from the unchanged anchor2.
const SEGMENT_INDEXES: [usize; 3] = [0, 6, 30];
// The level-two and three segment index, from the upstream test's anchor byte.
const WITNESS_SEGMENT_INDEX: usize = 30;
// The sample chunk span: 16 items of 64-byte (address || transformed) pairs.
const SAMPLE_SPAN: u64 = 1024;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Document {
    sample_chunk_address: String,
    anchor: String,
    proofs: ProofSet,
}

#[derive(Deserialize)]
struct ProofSet {
    proof1: ProofVector,
    proof2: ProofVector,
    #[serde(rename = "proofLast")]
    proof_last: ProofVector,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ProofVector {
    proof_segments: [String; 7],
    prove_segment: String,
    proof_segments2: [String; 7],
    prove_segment2: String,
    chunk_span: u64,
    proof_segments3: [String; 7],
    soc_proof: Vec<SocProof>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SocProof {
    identifier: String,
    chunk_addr: String,
}

fn document() -> Document {
    serde_json::from_str(DOCUMENT).expect("corpus parses")
}

fn b32(value: &str) -> B256 {
    B256::from_slice(&hex::decode(value).expect("corpus hex"))
}

fn hashes(v: &[String]) -> [B256; 7] {
    v.iter()
        .map(|s| b32(s))
        .collect::<Vec<_>>()
        .try_into()
        .expect("corpus length")
}

fn level_one(v: &ProofVector, segment_index: usize) -> nectar_primitives::bmt::Proof {
    nectar_primitives::bmt::Proof::new(
        segment_index,
        b32(&v.prove_segment),
        hashes(&v.proof_segments),
        SAMPLE_SPAN,
        B256::ZERO,
    )
}

fn level_two(v: &ProofVector) -> nectar_primitives::bmt::Proof {
    nectar_primitives::bmt::Proof::new(
        WITNESS_SEGMENT_INDEX,
        b32(&v.prove_segment2),
        hashes(&v.proof_segments2),
        v.chunk_span,
        B256::ZERO,
    )
}

fn level_three(v: &ProofVector) -> nectar_primitives::bmt::Proof {
    nectar_primitives::bmt::Proof::new(
        WITNESS_SEGMENT_INDEX,
        b32(&v.prove_segment2),
        hashes(&v.proof_segments3),
        v.chunk_span,
        B256::from_slice(&ANCHOR),
    )
}

// The sample leaf of a single-owner witness is the seal address, so the
// level-two proof binds at the wrapped content address instead.
fn level_two_root(v: &ProofVector) -> B256 {
    v.soc_proof
        .first()
        .map_or_else(|| b32(&v.prove_segment), |soc| b32(&soc.chunk_addr))
}

// The level-one sibling of the sample leaf is the plain witness's anchored
// transform; a single-owner witness pins keccak(seal || anchored transform),
// which the nectar-side transform derives instead.
fn level_three_root(v: &ProofVector) -> B256 {
    let Some(soc) = v.soc_proof.first() else {
        return b32(&v.proof_segments[0]);
    };
    use nectar_primitives::chunk::{ChunkOps, ContentChunk};
    let n = (1..=16u32)
        .find(|n| keccak256(format!("ID #{n}").as_bytes()) == b32(&soc.identifier))
        .expect("the identifier names the regression construction");
    let content = format!("Unstoppable data! Chunk #{n}");
    let inner: ContentChunk = ContentChunk::new(content).expect("content chunk");
    B256::from_slice(inner.transformed_address(&ANCHOR).as_bytes())
}

fn verify_at(trusted: &B256, proof: &nectar_primitives::bmt::Proof, what: &str) {
    assert!(
        Segment::verify(trusted, proof),
        "the {what} must bind at the trusted address"
    );
}

#[test]
fn the_document_names_the_anchor() {
    assert_eq!(document().anchor, hex::encode_prefixed(ANCHOR));
}

#[test]
fn level_one_verifies_at_the_sample_chunk_address() {
    let d = document();
    let roots = [&d.proofs.proof1, &d.proofs.proof2, &d.proofs.proof_last];
    for (v, segment_index) in roots.into_iter().zip(SEGMENT_INDEXES) {
        verify_at(
            &b32(&d.sample_chunk_address),
            &level_one(v, segment_index),
            "level-one proof",
        );
    }
}

#[test]
fn level_two_verifies_at_the_witness_addresses() {
    let d = document();
    let roots = [&d.proofs.proof1, &d.proofs.proof2, &d.proofs.proof_last];
    for v in roots {
        verify_at(&level_two_root(v), &level_two(v), "level-two proof");
    }
}

#[test]
fn level_three_verifies_at_the_anchored_transforms() {
    let d = document();
    let roots = [&d.proofs.proof1, &d.proofs.proof2, &d.proofs.proof_last];
    for v in roots {
        verify_at(&level_three_root(v), &level_three(v), "level-three proof");
    }
}

#[test]
fn the_anchored_proof_does_not_bind_without_the_anchor() {
    let v = document().proofs.proof1;
    // The same siblings and segment, fed to the fold without the anchor
    // bytes, land at the plain root and miss the anchored one.
    let without = nectar_primitives::bmt::Proof::new(
        WITNESS_SEGMENT_INDEX,
        b32(&v.prove_segment2),
        hashes(&v.proof_segments3),
        v.chunk_span,
        B256::ZERO,
    );
    let anchored_root = level_three_root(&v);
    assert!(!Segment::verify(&anchored_root, &without));
}
