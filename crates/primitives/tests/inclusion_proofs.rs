//! BMT inclusion-proof vectors the reference client produced.
//!
//! The corpus is `tools/stamp-vectors/testdata/inclusion-proofs.json`, a
//! verbatim copy of the reference client's
//! `pkg/storageincentives/testdata/inclusion-proofs.json` (the
//! TestMakeInclusionProofsRegression test). Its provenance is recorded in
//! `tools/stamp-vectors/README.md`. The sample chunk address is the value
//! that test pins.

#![allow(clippy::expect_used)]

use alloy_primitives::{Address, B256, hex, keccak256};
use nectar_primitives::bmt::Proof;
use serde::Deserialize;

const DOCUMENT: &str = include_str!("../../../tools/stamp-vectors/testdata/inclusion-proofs.json");

// The sample chunk address the upstream test pins.
const SAMPLE_CHUNK_ADDRESS: &str =
    "0xb012904b0c3e6462158b4416556caa888031a79bad46d2ffa7012408c9c38aa8";
// The sample span: 16 items of (address || transformed address), 64 bytes each.
const SAMPLE_SPAN: u64 = 1024;
// The level-two and three segment index, from the upstream test's anchor byte.
const WITNESS_SEGMENT_INDEX: usize = 30;
// The upstream anchor1 (big.NewInt(100).Bytes()) for the level-three proofs.
const ANCHOR: u8 = 0x64;

#[derive(Deserialize)]
struct Document {
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
    soc_proof: Vec<SocVector>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SocVector {
    signer: String,
    identifier: String,
    chunk_addr: String,
}

fn document() -> Document {
    serde_json::from_str(DOCUMENT).expect("corpus parses")
}

fn b32(value: &str) -> B256 {
    B256::from_slice(&hex::decode(value).expect("corpus hex"))
}

fn level_one(v: &ProofVector, segment_index: usize) -> Proof {
    Proof::new(
        segment_index,
        b32(&v.prove_segment),
        v.proof_segments.to_owned().map(|s| b32(&s)),
        SAMPLE_SPAN,
        None,
    )
}

fn level_two(v: &ProofVector) -> Proof {
    Proof::new(
        WITNESS_SEGMENT_INDEX,
        b32(&v.prove_segment2),
        v.proof_segments2.to_owned().map(|s| b32(&s)),
        v.chunk_span,
        None,
    )
}

fn level_three(v: &ProofVector) -> Proof {
    Proof::new(
        WITNESS_SEGMENT_INDEX,
        b32(&v.prove_segment2),
        v.proof_segments3.to_owned().map(|s| b32(&s)),
        v.chunk_span,
        Some(vec![ANCHOR]),
    )
}

#[test]
fn proof1_level_one_verifies_at_the_sample_chunk_address() {
    let v = &document().proof1;
    let root = b32(SAMPLE_CHUNK_ADDRESS);
    assert!(
        level_one(v, 0).verify(&root).expect("verifies"),
        "proof1 level 1: the sample chunk address"
    );
}

#[test]
fn proof2_level_one_verifies_at_the_sample_chunk_address() {
    let v = &document().proof2;
    let root = b32(SAMPLE_CHUNK_ADDRESS);
    assert!(
        level_one(v, 6).verify(&root).expect("verifies"),
        "proof2 level 1: the sample chunk address"
    );
}

#[test]
fn proof_last_level_one_verifies_at_the_sample_chunk_address() {
    let v = &document().proof_last;
    let root = b32(SAMPLE_CHUNK_ADDRESS);
    assert!(
        level_one(v, 30).verify(&root).expect("verifies"),
        "proofLast level 1: the sample chunk address"
    );
}

#[test]
fn proof1_level_two_verifies_at_the_embedded_witness_address() {
    let v = &document().proof1;
    // The witness address is embedded in the sample as the level-1 leaf.
    let root = b32(&v.prove_segment);
    assert!(
        level_two(v).verify(&root).expect("verifies"),
        "proof1 level 2: the embedded witness address"
    );
}

#[test]
fn proof2_level_two_verifies_at_the_wrapped_chunk_address() {
    let v = &document().proof2;
    // The witness is a single-owner chunk, so the proof runs over the stored
    // wrapped content, not the single-owner address.
    let soc = v
        .soc_proof
        .first()
        .expect("proof2 carries the wrapped chunk address");
    let root = b32(&soc.chunk_addr);
    assert!(
        level_two(v).verify(&root).expect("verifies"),
        "proof2 level 2: the wrapped chunk address"
    );
}

#[test]
fn proof_last_level_two_verifies_at_the_embedded_witness_address() {
    let v = &document().proof_last;
    let root = b32(&v.prove_segment);
    assert!(
        level_two(v).verify(&root).expect("verifies"),
        "proofLast level 2: the embedded witness address"
    );
}

#[test]
fn proof1_level_three_verifies_at_the_transformed_address() {
    let v = &document().proof1;
    // The level-1 sibling of the sample leaf is the item's transformed address.
    let root = b32(v.proof_segments.first().expect("sibling present"));
    assert!(
        level_three(v).verify(&root).expect("verifies"),
        "proof1 level 3: the transformed address"
    );
}

#[test]
fn proof2_level_three_verifies_at_the_wrapped_chunk_transformed() {
    use nectar_primitives::chunk::{ChunkOps, ContentChunk};

    let v = &document().proof2;
    // The upstream test constructs the witness content; the corpus identifier
    // names the construction nectar re-derives here.
    let soc = v
        .soc_proof
        .first()
        .expect("proof2 carries a single-owner proof");
    let n = (1..=16u32)
        .find(|n| keccak256(format!("ID #{n}").as_bytes()) == b32(&soc.identifier))
        .expect("the identifier names the upstream construction");
    let content = format!("Unstoppable data! Chunk #{n}");
    let inner: ContentChunk = ContentChunk::new(content).expect("content chunk");
    // The sample stores the single-owner sealed transform, so the reference
    // client's anchor proof runs over the stored content instead.
    let root = B256::from_slice(inner.transformed_address(&[ANCHOR]).as_bytes());
    assert!(
        level_three(v).verify(&root).expect("verifies"),
        "proof2 level 3: the wrapped chunk's transformed address"
    );
}

#[test]
fn proof_last_level_three_verifies_at_the_transformed_address() {
    let v = &document().proof_last;
    let root = b32(v.proof_segments.first().expect("sibling present"));
    assert!(
        level_three(v).verify(&root).expect("verifies"),
        "proofLast level 3: the transformed address"
    );
}

#[test]
fn proof2_single_owner_address_derives_from_identifier_and_signer() {
    let v = &document().proof2;
    let soc = v
        .soc_proof
        .first()
        .expect("proof2 carries a single-owner proof");
    let mut preimage = [0u8; 52];
    preimage[..32].copy_from_slice(&b32(&soc.identifier).0);
    let owner: Address = soc.signer.parse().expect("signer is an address");
    preimage[32..].copy_from_slice(owner.0.as_slice());
    let derived = keccak256(preimage);
    assert_eq!(
        derived,
        b32(&v.prove_segment),
        "proof2: keccak256(identifier || signer) is the embedded single-owner address"
    );
}
