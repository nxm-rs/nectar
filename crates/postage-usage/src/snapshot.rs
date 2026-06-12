//! A usage table together with its persistence state.

use alloc::vec::Vec;

use alloy_primitives::{Address, B256};
use bytes::Bytes;
use nectar_postage::{BatchId, StampIndex, calculate_bucket};
use nectar_primitives::SwarmAddress;

use crate::codec::{self, Encoded};
use crate::table::UsageTable;
use crate::{Result, UsageError, usage_chunk_address, usage_chunk_id};

/// A [`UsageTable`] together with the state needed to persist it inside its
/// own batch: a monotone sequence number and the within-bucket slots
/// allocated to the snapshot chunks themselves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    table: UsageTable,
    sequence: u64,
    slots: Vec<u32>,
}

/// One chunk of a persist plan: the payload to publish and the slot to
/// stamp it with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedChunk {
    /// The snapshot chunk index (0 is the root).
    pub index: u16,
    /// The single-owner chunk id.
    pub id: B256,
    /// The single-owner chunk address.
    pub address: SwarmAddress,
    /// The stamp index to use. Constant across persists for a given chunk;
    /// reusing it with a newer timestamp overwrites the previous version in
    /// place instead of consuming another slot.
    pub stamp_index: StampIndex,
    /// The chunk payload.
    pub payload: Bytes,
    /// Whether this chunk's slot was allocated by this plan (true exactly
    /// once per chunk, on its first appearance).
    pub newly_allocated: bool,
}

/// The output of [`Snapshot::plan_persist`]: every chunk of the snapshot in
/// chunk-index order, ready to be signed, stamped, and published.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistPlan {
    /// The batch the snapshot describes and is stamped against.
    pub batch_id: BatchId,
    /// The sequence number of this snapshot version.
    pub sequence: u64,
    /// The chunks to publish. Callers may skip re-publishing leaves whose
    /// payload is unchanged from the previously persisted version, but must
    /// always publish the root.
    pub chunks: Vec<PlannedChunk>,
}

impl Snapshot {
    /// Wraps a table that has never been persisted.
    pub const fn new(table: UsageTable) -> Self {
        Self {
            table,
            sequence: 0,
            slots: Vec::new(),
        }
    }

    /// Reconstructs a snapshot from its parts.
    pub fn from_parts(table: UsageTable, sequence: u64, slots: Vec<u32>) -> Result<Self> {
        let capacity = table.bucket_capacity();
        if slots.len() > u16::MAX as usize {
            return Err(UsageError::Malformed("too many allocated chunks"));
        }
        if let Some(&slot) = slots.iter().find(|&&slot| slot >= capacity) {
            return Err(UsageError::InvalidSlot { slot, capacity });
        }
        Ok(Self {
            table,
            sequence,
            slots,
        })
    }

    /// Returns the usage table.
    pub const fn table(&self) -> &UsageTable {
        &self.table
    }

    /// Returns a mutable reference to the usage table, for recording stamp
    /// issuance between persists.
    pub const fn table_mut(&mut self) -> &mut UsageTable {
        &mut self.table
    }

    /// Consumes the snapshot and returns the usage table.
    pub fn into_table(self) -> UsageTable {
        self.table
    }

    /// Returns the sequence number of the last planned persist (0 if never
    /// persisted).
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Returns the within-bucket slots allocated to snapshot chunks, in
    /// chunk-index order (entry 0 is the root's own slot).
    pub fn allocated_slots(&self) -> &[u32] {
        &self.slots
    }

    /// Returns the stamp indices occupied by the snapshot's own chunks for a
    /// batch owned by `owner`, in chunk-index order.
    ///
    /// On a mutable batch any slot may be deliberately re-stamped with a
    /// different chunk and a newer timestamp, evicting the chunk that held
    /// it. The indices returned here hold the usage data itself and must
    /// never be reused for another chunk. Fresh issuance cannot collide with
    /// them (they sit below the per-bucket counter watermark); only
    /// deliberate slot-reuse tooling needs to consult this list. It covers
    /// every chunk ever allocated, including leaves a smaller re-encoding no
    /// longer references, since their previous versions still occupy their
    /// slots on the network.
    pub fn reserved_stamp_indices(&self, owner: &Address) -> Vec<StampIndex> {
        let batch_id = self.table.batch_id();
        let bucket_depth = self.table.bucket_depth();
        self.slots
            .iter()
            .enumerate()
            .map(|(index, &slot)| {
                let address = usage_chunk_address(&batch_id, owner, index as u16);
                StampIndex::new(calculate_bucket(&address, bucket_depth), slot)
            })
            .collect()
    }

    /// Returns whether a stamp index is occupied by one of the snapshot's
    /// own chunks and therefore must not be reused for another chunk.
    pub fn is_reserved(&self, owner: &Address, index: StampIndex) -> bool {
        self.reserved_stamp_indices(owner).contains(&index)
    }

    /// Encodes the snapshot with its current sequence number.
    ///
    /// Fails if the snapshot has never been persisted (no slot is allocated
    /// for the root); use [`plan_persist`](Self::plan_persist) instead.
    pub fn encode(&self) -> Result<Encoded> {
        codec::encode(&self.table, self.sequence, &self.slots)
    }

    /// Plans the next persist: bumps the sequence number, allocates slots
    /// for any snapshot chunk that does not have one yet (folding those
    /// stamps into the table itself), and encodes.
    ///
    /// Allocation runs to a fixed point: stamping a new snapshot chunk
    /// increments a counter, which can grow the encoding by another leaf.
    /// Slots are never deallocated, so steady-state persists allocate
    /// nothing and the loop is bounded by the maximum leaf count.
    ///
    /// `owner` is the batch owner address; it determines the single-owner
    /// chunk addresses and therefore which buckets the snapshot occupies.
    ///
    /// On error (for example a full bucket on first allocation) the snapshot
    /// is left unchanged.
    pub fn plan_persist(&mut self, owner: &Address) -> Result<PersistPlan> {
        let mut work = self.clone();
        work.sequence += 1;

        let batch_id = work.table.batch_id();
        let bucket_depth = work.table.bucket_depth();
        let previously_allocated = self.slots.len();

        let allocate = |work: &mut Self| -> Result<()> {
            let index = work.slots.len() as u16;
            let address = usage_chunk_address(&batch_id, owner, index);
            let bucket = calculate_bucket(&address, bucket_depth);
            let slot = work.table.record(bucket)?;
            work.slots.push(slot);
            Ok(())
        };

        if work.slots.is_empty() {
            allocate(&mut work)?;
        }
        let encoded = loop {
            let encoded = work.encode()?;
            if work.slots.len() > encoded.leaves.len() {
                break encoded;
            }
            allocate(&mut work)?;
        };

        let mut chunks = Vec::with_capacity(1 + encoded.leaves.len());
        let payloads = core::iter::once(&encoded.root).chain(encoded.leaves.iter());
        for (index, payload) in payloads.enumerate() {
            let index = index as u16;
            let id = usage_chunk_id(&batch_id, index);
            let address = usage_chunk_address(&batch_id, owner, index);
            let bucket = calculate_bucket(&address, bucket_depth);
            chunks.push(PlannedChunk {
                index,
                id,
                address,
                stamp_index: StampIndex::new(bucket, work.slots[index as usize]),
                payload: payload.clone(),
                newly_allocated: (index as usize) >= previously_allocated,
            });
        }

        let plan = PersistPlan {
            batch_id,
            sequence: work.sequence,
            chunks,
        };
        *self = work;
        Ok(plan)
    }
}
