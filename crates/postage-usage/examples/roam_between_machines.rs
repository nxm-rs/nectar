//! Roam a postage batch's issuer state between machines.
//!
//! The headline of `nectar-postage-usage` is that a user can issue stamps from a
//! batch on one machine, persist the per-bucket counters *inside the batch
//! itself* as single-owner chunks, and then resume issuing from a completely
//! fresh machine holding nothing but their key and the batch id. This example
//! walks that story end to end against an immutable batch.
//!
//! Run it with:
//!
//! ```text
//! cargo run -p nectar-postage-usage --example roam_between_machines --features seal
//! ```
//!
//! It uses no network: "machine A" and "machine B" are two scopes in the same
//! process, and the only thing that crosses between them is the bytes machine A
//! would have uploaded to the network (the sealed snapshot chunks). Machine B
//! starts from the key and batch id alone, exactly as a real second device
//! would, and recovers the counters by parsing those bytes.

use alloy_primitives::{Address, B256, hex};
use alloy_signer_local::PrivateKeySigner;
use nectar_postage_usage::{
    Mutability, PublishedSequence, RootInfo, SealedChunk, Snapshot, SwarmAddress, UsageError,
    UsageTable, seal_plan, usage_chunk_address,
};
use nectar_primitives::Chunk;

/// A depth-20 batch with 65536 buckets of 16 slots each. Small enough that the
/// whole snapshot fits inline in a single root chunk, so the cross-machine hop
/// carries exactly one payload.
const DEPTH: u8 = 20;
const BUCKET_DEPTH: u8 = 16;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The owner key is the one secret a user carries between machines. The batch
    // id is public and recoverable; together they pin every snapshot chunk
    // address, so machine B can find the state without any local store.
    let signer = PrivateKeySigner::random();
    let owner: Address = signer.address();
    let batch_id = B256::repeat_byte(0x42);

    println!("Postage batch usage: roaming between machines");
    println!("==============================================\n");
    println!("owner    {}", hex::encode_prefixed(owner));
    println!("batch id {}\n", hex::encode_prefixed(batch_id));

    // The bytes that travel from machine A to machine B: the sealed snapshot
    // chunks machine A uploads to the network. Machine B fetches the root and
    // any leaves back from these same addresses.
    let uploaded = machine_a(&signer, owner, batch_id)?;

    machine_b(&signer, owner, batch_id, &uploaded)?;

    stale_persist_is_rejected(owner, batch_id)?;

    println!("\nDone: state roamed from A to B, the sequence advanced, the");
    println!("snapshot's own slots were reused, and a stale persist was refused.");
    Ok(())
}

/// Machine A: a fresh immutable batch. Issue one content stamp, persist, seal,
/// and return the chunk bytes that would be uploaded to the network.
fn machine_a(
    signer: &PrivateKeySigner,
    owner: Address,
    batch_id: B256,
) -> Result<Vec<SealedChunk>, Box<dyn std::error::Error>> {
    println!("Machine A: fresh batch, first issuance");
    println!("-------------------------------------");

    // A genuinely new, never-persisted batch starts at sequence 0 with no slots.
    let table = UsageTable::new(batch_id, DEPTH, BUCKET_DEPTH, Mutability::Immutable)?;
    let mut snapshot = Snapshot::new(table);

    // Issue a stamp for a content chunk through the snapshot's issuing handle.
    // The handle is the sole counter-advance path and is reserved-aware by
    // construction, so a stamp can never land on the snapshot's own slots.
    let content = SwarmAddress::from(B256::repeat_byte(0x99));
    let stamp_index = snapshot.issuer(owner).record_address(&content)?;
    println!(
        "issued a content stamp at bucket {}, slot {}",
        stamp_index.bucket(),
        stamp_index.index()
    );

    // Plan a persist. This is a brand new batch that has never been published,
    // so a live network read of the root chunk address would return nothing and
    // the floor is `NONE`. The first persist therefore emits sequence 1.
    let plan = snapshot
        .revalidate(PublishedSequence::NONE)?
        .plan_persist(&owner)?;
    println!(
        "planned persist sequence {} ({} chunk(s) to publish)",
        plan.sequence,
        plan.chunks.len()
    );

    // Seal the plan: sign each snapshot chunk as a single-owner chunk and stamp
    // it with its planned slot. The timestamp is the wall clock the reserve uses
    // to overwrite the previous version in place; it must strictly increase
    // across persists, which the seal enforces in process.
    let sealed = seal_plan(&mut snapshot, &plan, 1_000, signer)?;

    println!("sealed {} snapshot chunk(s), uploading to:", sealed.len());
    for (planned, chunk) in plan.chunks.iter().zip(sealed.iter()) {
        // The sealed single-owner chunk lands at exactly the address derived from
        // the batch id and owner, so machine B can recompute it blind.
        let derived = usage_chunk_address(&batch_id, &owner, planned.index);
        assert_eq!(*chunk.chunk.address(), derived);
        println!(
            "  chunk {} -> {}  (stamp bucket {}, slot {})",
            planned.index,
            hex::encode_prefixed(derived),
            planned.stamp_index.bucket(),
            planned.stamp_index.index()
        );
    }
    println!();

    Ok(sealed)
}

/// Machine B: a fresh start with only the key and batch id. Recover the state
/// from the bytes machine A uploaded, then issue and persist again.
fn machine_b(
    signer: &PrivateKeySigner,
    owner: Address,
    batch_id: B256,
    uploaded: &[SealedChunk],
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Machine B: fresh start, recover and resume");
    println!("------------------------------------------");

    // Machine B knows where the root lives from the key and batch id alone:
    // chunk index 0 of this batch.
    let root_address = usage_chunk_address(&batch_id, &owner, 0);
    println!("root chunk address {}", hex::encode_prefixed(root_address));

    // Fetch the root chunk bytes back (here, straight from what A uploaded). On a
    // real network this is a single chunk retrieval at the address above.
    let root_bytes =
        chunk_payload(uploaded, &root_address).expect("machine A uploaded the root chunk");

    // Parse the root. This is also the live network read that supplies the
    // published-sequence floor for the next persist: whatever sequence is
    // already published is the floor a fresh persist must strictly exceed.
    let root = RootInfo::parse(root_bytes)?;
    let floor = PublishedSequence::from(&root);
    println!(
        "parsed root: published sequence {}, {} leaf chunk(s) to fetch",
        floor.get(),
        root.leaf_count()
    );

    // Fetch each leaf the root commits to and reassemble. This snapshot is small
    // enough to be inline, so there are no leaves, but the recovery path is the
    // same at any size: `assemble` rebuilds the snapshot through `from_parts`,
    // preserving the recovered sequence and slots.
    let leaves: Vec<&[u8]> = (0..root.leaf_count())
        .map(|leaf| {
            let address = usage_chunk_address(&batch_id, &owner, leaf + 1);
            chunk_payload(uploaded, &address).expect("machine A uploaded every leaf")
        })
        .collect();
    let mut snapshot = root.assemble(&leaves)?;
    println!(
        "recovered snapshot at sequence {} with slots {:?}",
        snapshot.sequence(),
        snapshot.allocated_slots()
    );

    let recovered_slots = snapshot.allocated_slots().to_vec();

    // Resume issuing from the recovered state. A different content chunk lands in
    // a different bucket, advancing that counter without disturbing the
    // snapshot's own reserved slots.
    let content = SwarmAddress::from(B256::repeat_byte(0xab));
    let stamp_index = snapshot.issuer(owner).record_address(&content)?;
    println!(
        "issued a content stamp at bucket {}, slot {}",
        stamp_index.bucket(),
        stamp_index.index()
    );

    // Persist again against the floor read from the live root. The next sequence
    // (2) strictly exceeds the published floor (1), so the persist is admitted.
    let plan = snapshot.revalidate(floor)?.plan_persist(&owner)?;
    println!("planned persist sequence {}", plan.sequence);
    assert_eq!(plan.sequence, floor.get() + 1, "sequence advanced by one");
    assert_eq!(
        snapshot.allocated_slots(),
        recovered_slots.as_slice(),
        "the snapshot's own slots were reused, not re-allocated",
    );
    println!(
        "slots reused: {:?} (no new metadata slot burned)",
        snapshot.allocated_slots()
    );

    // Seal the resumed persist with a strictly newer timestamp than machine A
    // used, so the reserve overwrites the previous version in place.
    let sealed = seal_plan(&mut snapshot, &plan, 2_000, signer)?;
    println!("sealed {} snapshot chunk(s) for upload\n", sealed.len());

    Ok(())
}

/// Show that a persist whose next sequence does not strictly exceed the live
/// published floor is rejected, so it can never overwrite a newer published
/// version in place.
fn stale_persist_is_rejected(
    owner: Address,
    batch_id: B256,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Stale persist: rejected by the published floor");
    println!("----------------------------------------------");

    // A snapshot sitting at sequence 1 (its next persist would emit 2).
    let table = UsageTable::new(batch_id, DEPTH, BUCKET_DEPTH, Mutability::Immutable)?;
    let mut snapshot = Snapshot::new(table);
    snapshot
        .revalidate(PublishedSequence::NONE)?
        .plan_persist(&owner)?;
    assert_eq!(snapshot.sequence(), 1);

    // Suppose the live network already published sequence 2 (another device got
    // there first). Reading that as the floor rejects this snapshot's persist:
    // its next sequence (2) does not strictly exceed the floor (2).
    let floor = PublishedSequence::new(2);
    let result = snapshot.revalidate(floor);
    match result {
        Err(UsageError::StaleSequence { next, floor }) => {
            println!("refused: next sequence {next} does not exceed published floor {floor}");
        }
        other => panic!("expected StaleSequence, got {other:?}"),
    }

    Ok(())
}

/// Look up the payload of an uploaded snapshot chunk by its single-owner chunk
/// address, simulating a network chunk retrieval.
fn chunk_payload<'a>(uploaded: &'a [SealedChunk], address: &SwarmAddress) -> Option<&'a [u8]> {
    uploaded
        .iter()
        .find(|sealed| sealed.chunk.address() == address)
        .map(|sealed| sealed.chunk.data().as_ref())
}
