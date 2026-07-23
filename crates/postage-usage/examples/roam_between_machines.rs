//! Roam a postage batch's issuer state between machines.
//!
//! The headline of `nectar-postage-usage` is that a user can issue stamps from a
//! batch on one machine, persist the per-bucket counters *inside the batch
//! itself* as single-owner chunks, and then resume issuing from a completely
//! fresh machine holding nothing but their key and the batch id. This example
//! walks that story end to end against an immutable batch through the high-level
//! [`BatchStamper`] facade, which collapses the cross-machine ceremony into
//! `open` / `stamp` / `flush`.
//!
//! Run it with:
//!
//! ```text
//! cargo run -p nectar-postage-usage --example roam_between_machines --features "client seal"
//! ```
//!
//! It uses no real network: "machine A" and "machine B" are two scopes in the
//! same process sharing one in-memory [`MemNet`], which implements both
//! [`SnapshotSource`] and [`SnapshotSink`]. Machine B starts from the key and batch id
//! alone, exactly as a real second device would, and the facade recovers the
//! counters by fetching and parsing the chunks machine A uploaded.
//!
//! Power users who need partial persists, a custom seal timestamp policy, or
//! direct access to the [`PersistPlan`](nectar_postage_usage::PersistPlan) can
//! drop to the low-level `Snapshot` / `revalidate` / `seal_plan` path the facade
//! is built on; it stays public and unchanged.

// Bench, example, and integration-test code: unwraps, direct indexing,
// casts, and assertions are setup and illustration, not shipped surface.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::panic,
    clippy::panic_in_result_fn,
    clippy::as_conversions,
    clippy::missing_panics_doc
)]
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use alloy_primitives::{Address, B256, hex};
use alloy_signer_local::PrivateKeySigner;
use bytes::Bytes;
use nectar_postage::{Batch, BatchId, BucketDepth};
use nectar_postage_usage::{
    BatchStamper, ChunkAddress, Mutability, PublishedSequence, SealedChunk, Snapshot, SnapshotSink,
    SnapshotSource, UsageError, UsageTable,
};
use nectar_primitives::ChunkOps;

/// A depth-20 batch with 65536 buckets of 16 slots each. Small enough that the
/// whole snapshot fits inline in a single root chunk, so the cross-machine hop
/// carries exactly one payload.
const DEPTH: u8 = 20;
const BUCKET_DEPTH: u8 = 16;

/// A shared in-memory network keyed by single-owner-chunk address. The same
/// value backs the [`SnapshotSource`] machine B reads from and the [`SnapshotSink`]
/// machine A uploads to, so the only thing crossing between the two machines is
/// the bytes on the wire.
#[derive(Clone, Default)]
struct MemNet {
    chunks: Arc<Mutex<HashMap<ChunkAddress, Bytes>>>,
}

#[derive(Debug, thiserror::Error)]
#[error("in-memory network error")]
struct MemError;

impl SnapshotSource for MemNet {
    type Error = MemError;
    async fn fetch(&self, address: &ChunkAddress) -> Result<Option<Bytes>, Self::Error> {
        Ok(self.chunks.lock().unwrap().get(address).cloned())
    }
}

impl SnapshotSink for MemNet {
    type Error = MemError;
    async fn push(&self, sealed: &SealedChunk) -> Result<(), Self::Error> {
        let address = *sealed.chunk.address();
        let payload = Bytes::copy_from_slice(sealed.chunk.data().as_ref());
        self.chunks.lock().unwrap().insert(address, payload);
        Ok(())
    }
}

// Sanctioned tokio entry point: the main macro expands to `Runtime::block_on`.
#[tokio::main(flavor = "current_thread")]
#[allow(clippy::disallowed_methods)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The owner key is the one secret a user carries between machines. The batch
    // id is public and recoverable; together they pin every snapshot chunk
    // address, so machine B can find the state without any local store.
    let signer = PrivateKeySigner::random();
    let owner: Address = signer.address();
    let batch_id = BatchId::new([0x42; 32]);
    let batch = Batch::new(
        batch_id,
        0,
        0,
        owner,
        DEPTH,
        BucketDepth::new(BUCKET_DEPTH)?,
        true,
    );

    println!("Postage batch usage: roaming between machines");
    println!("==============================================\n");
    println!("owner    {}", hex::encode_prefixed(owner));
    println!("batch id {}\n", hex::encode_prefixed(batch_id));

    // The shared in-memory network. Machine A uploads into it; machine B reads
    // back out of it.
    let net = MemNet::default();

    machine_a(&signer, &batch, &net).await?;
    machine_b(&signer, &batch, &net).await?;

    stale_persist_is_rejected(owner, batch_id)?;

    println!("\nDone: state roamed from A to B, the sequence advanced, the");
    println!("snapshot's own slots were reused, and a stale persist was refused.");
    Ok(())
}

/// Machine A: a fresh immutable batch. Open, issue one content stamp, flush.
async fn machine_a(
    signer: &PrivateKeySigner,
    batch: &Batch,
    net: &MemNet,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Machine A: fresh batch, first issuance");
    println!("-------------------------------------");

    // Open the stamper. The network read returns nothing for a never-published
    // batch, so the facade starts fresh at sequence 0.
    let mut stamper = BatchStamper::open(signer.clone(), batch, net.clone(), net.clone()).await?;
    println!("opened fresh at sequence {}", stamper.snapshot().sequence());

    // Issue a content stamp. This is local: no network round trip, and the
    // reserved snapshot slots can never be assigned to content.
    let content = ChunkAddress::from(B256::repeat_byte(0x99));
    let stamp_index = stamper.stamp(&content)?;
    println!(
        "issued a content stamp at bucket {}, slot {}",
        stamp_index.bucket(),
        stamp_index.index()
    );

    // Flush: re-read the live floor (NONE for a fresh batch), revalidate, plan,
    // seal, and upload. The first persist emits sequence 1.
    stamper.flush().await?;
    println!(
        "flushed: published sequence {} with slots {:?}\n",
        stamper.snapshot().sequence(),
        stamper.snapshot().allocated_slots()
    );

    Ok(())
}

/// Machine B: a fresh start with only the key and batch id. Open against the
/// same in-memory network to recover the state, then issue and flush again.
async fn machine_b(
    signer: &PrivateKeySigner,
    batch: &Batch,
    net: &MemNet,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Machine B: fresh start, recover and resume");
    println!("------------------------------------------");

    // Open recovers the published root and any leaves, preserving the recovered
    // sequence and slots. Machine B holds no local store; the key and batch id
    // are enough.
    let mut stamper = BatchStamper::open(signer.clone(), batch, net.clone(), net.clone()).await?;
    let recovered_slots = stamper.snapshot().allocated_slots().to_vec();
    println!(
        "recovered snapshot at sequence {} with slots {:?}",
        stamper.snapshot().sequence(),
        recovered_slots
    );

    // Resume issuing from the recovered state. A different content chunk lands in
    // a different bucket, advancing that counter without disturbing the
    // snapshot's own reserved slots.
    let content = ChunkAddress::from(B256::repeat_byte(0xab));
    let stamp_index = stamper.stamp(&content)?;
    println!(
        "issued a content stamp at bucket {}, slot {}",
        stamp_index.bucket(),
        stamp_index.index()
    );

    // Flush again. The live floor read from the root is 1, so the next sequence
    // (2) is admitted, and the snapshot's own slots are reused rather than
    // re-allocated.
    stamper.flush().await?;
    assert_eq!(stamper.snapshot().sequence(), 2, "sequence advanced by one");
    assert_eq!(
        stamper.snapshot().allocated_slots(),
        recovered_slots.as_slice(),
        "the snapshot's own slots were reused, not re-allocated",
    );
    println!(
        "flushed: sequence {}, slots reused {:?} (no new metadata slot burned)\n",
        stamper.snapshot().sequence(),
        stamper.snapshot().allocated_slots()
    );

    Ok(())
}

/// Show that a persist whose next sequence does not strictly exceed the live
/// published floor is rejected, so it can never overwrite a newer published
/// version in place. This is the low-level guard the facade enforces on every
/// flush.
fn stale_persist_is_rejected(
    owner: Address,
    batch_id: BatchId,
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
    match snapshot.revalidate(floor) {
        Err(UsageError::StaleSequence { next, floor }) => {
            println!("refused: next sequence {next} does not exceed published floor {floor}");
        }
        other => panic!("expected StaleSequence, got {other:?}"),
    }

    Ok(())
}
