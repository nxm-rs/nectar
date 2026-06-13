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
    ///
    /// A snapshot whose table is mutable is *not* yet reserved-aware: mapping
    /// its allocated slots to buckets needs the owner address (the slots are
    /// single-owner chunks), which is not part of the snapshot. Before issuing
    /// any content stamp on such a snapshot the caller must call
    /// [`sync_reserved`](Self::sync_reserved) (or use the owner-aware
    /// content-issuance entry points [`record_address`](Self::record_address)
    /// / [`into_issuer`](Self::into_issuer), which sync internally). Immutable
    /// snapshots need no sync: their watermarks already dominate every
    /// reserved slot.
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

    /// Returns a mutable reference to the usage table.
    ///
    /// This is an escape hatch behind the `raw-table` feature. It bypasses the
    /// reserved-aware issuance path: recording content directly through the
    /// returned table on a mutable batch does not skip the snapshot's own
    /// reserved slots, so after the ring wraps it can overwrite the data that
    /// records the batch state. Prefer [`record_address`](Self::record_address)
    /// (or the [`into_issuer`](Self::into_issuer) adapter) for content issuance
    /// and [`dilute`](Self::dilute) for dilution; reach for this only when you
    /// need raw table access and have arranged reserved-slot safety yourself.
    #[cfg(feature = "raw-table")]
    pub const fn table_mut(&mut self) -> &mut UsageTable {
        &mut self.table
    }

    /// Applies an on-chain dilution to the underlying table, growing the
    /// per-bucket capacity without changing any counter or cursor.
    ///
    /// Safe on both immutable and mutable batches: the reserved snapshot slots
    /// stay below the old capacity and so remain valid in the larger ring.
    pub fn dilute(&mut self, new_depth: u8) -> Result<()> {
        self.table.dilute(new_depth)
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
    /// The indices returned here hold the usage data itself and must never be
    /// reused for another chunk. They cover every chunk ever allocated,
    /// including leaves a smaller re-encoding no longer references, since their
    /// previous versions still occupy their slots on the network.
    ///
    /// The enforcement differs by batch mode. On an *immutable* batch fresh
    /// issuance cannot collide with these indices because they sit below the
    /// per-bucket counter watermark, so this list is advisory: only deliberate
    /// slot-reuse tooling needs to consult it. On a *mutable* batch the ring
    /// cursor would otherwise wrap onto these slots and evict the snapshot, so
    /// this set is *enforced*: the issuance path skips it. It is installed into
    /// the table by [`sync_reserved`](Self::sync_reserved), which the
    /// owner-aware content-issuance entry points call automatically.
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

    /// Recomputes the snapshot's reserved `(bucket, index)` slots for `owner`
    /// and installs them into the table, making it reserved-aware.
    ///
    /// This is required for mutable batches before any content issuance: it is
    /// what carves the snapshot's own slots out of the per-bucket ring so they
    /// are never overwritten. It is idempotent and safe (a no-op on issuance
    /// behaviour) for immutable batches. The owner-aware content-issuance entry
    /// points call it for you; call it explicitly after recovering a mutable
    /// snapshot from the wire (see [`from_parts`](Self::from_parts)).
    pub fn sync_reserved(&mut self, owner: &Address) {
        let reserved: Vec<(u32, u32)> = self
            .reserved_stamp_indices(owner)
            .into_iter()
            .map(|index| (index.bucket(), index.index()))
            .collect();
        self.table.set_reserved(reserved);
    }

    /// Records a content chunk address against the shared table and returns its
    /// stamp index, skipping the snapshot's own reserved slots.
    ///
    /// This is the wired shared-table path: content stamping and snapshot
    /// allocation persist through the same [`UsageTable`], so their slots
    /// provably never collide. On an immutable batch the chunk lands at the
    /// bucket watermark, which already dominates every reserved slot. On a
    /// mutable batch the table is made reserved-aware for `owner` first, then
    /// the ring cursor skips the reserved slots.
    ///
    /// Use this (or the [`into_issuer`](Self::into_issuer) adapter) for all
    /// content issuance whenever content and snapshot chunks share one table on
    /// a mutable batch; it is the only path that keeps the reserved slots safe.
    pub fn record_address(
        &mut self,
        owner: &Address,
        address: &SwarmAddress,
    ) -> Result<StampIndex> {
        if self.table.is_mutable() {
            self.sync_reserved(owner);
        }
        self.table.record_address(address)
    }

    /// Consumes the snapshot and returns a [`StampIssuer`] adapter bound to
    /// `owner`, so content stamping drops into a `BatchStamper` while
    /// persisting through the same table as snapshot allocation.
    #[cfg(feature = "issuer")]
    pub const fn into_issuer(self, owner: Address) -> crate::SnapshotIssuer {
        crate::SnapshotIssuer::new(self, owner)
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
            // On a mutable batch the ring cursor would otherwise wrap onto a
            // slot already held by an earlier snapshot chunk in the same
            // bucket; sync the reserved set so this allocation skips them.
            if work.table.is_mutable() {
                work.sync_reserved(owner);
            }
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

        // Leave the table reserved-aware so a mutable batch can issue content
        // stamps immediately after persisting, without re-syncing.
        if work.table.is_mutable() {
            work.sync_reserved(owner);
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
