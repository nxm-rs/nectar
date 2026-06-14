//! A usage table together with its persistence state.

use alloc::collections::BTreeSet;
use alloc::vec::Vec;

use alloy_primitives::{Address, B256};
use bytes::Bytes;
use nectar_postage::{Batch, BatchId, StampIndex, calculate_bucket};
use nectar_primitives::SwarmAddress;

use crate::codec::{self, Encoded};
use crate::table::{TableView, UsageTable};
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

/// The opaque, indivisible parts of a recovered or extracted [`Snapshot`]: its
/// inert table, its persist sequence, and its allocated slots, kept together so
/// they can never be split.
///
/// This is the *only* value [`Snapshot::from_parts`] accepts and the *only*
/// thing [`Snapshot::into_parts`] hands out. It exposes no way to obtain an owned
/// table: the table is held privately and surfaced only as a borrowed
/// [`TableView`] that exposes only read getters. Were an owned table reachable
/// on its own, by move or by clone, it could be fed to [`Snapshot::new`], which
/// resets the sequence to 0 and drops the allocated slots, downgrading a
/// recovered snapshot and overwriting a newer persisted version in place at the
/// same metadata chunk addresses. Keeping the three bound together, with only a
/// borrowed view out, makes that in-memory downgrade unrepresentable in safe
/// code: a recovered snapshot can only round-trip through
/// [`from_parts`](Snapshot::from_parts), which preserves the sequence and slots.
///
/// The accessors are read-only and for inspection only (logging a recovered
/// sequence, say); rebuilding a usable snapshot goes through
/// [`from_parts`](Snapshot::from_parts).
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use = "dropping the parts discards the recovered sequence and slots; rebuild with Snapshot::from_parts"]
pub struct SnapshotParts {
    table: UsageTable,
    sequence: u64,
    slots: Vec<u32>,
}

impl SnapshotParts {
    /// Returns a borrowed, read-only [`TableView`] onto the inert usage table.
    ///
    /// There is deliberately no owned-table accessor, and the view is neither
    /// [`Clone`]-to-owned nor a [`Deref`](core::ops::Deref) to the table: an owned
    /// bare table could be passed to [`Snapshot::new`] and reset to sequence 0, so
    /// `parts.table().clone()` must not yield one.
    pub const fn table(&self) -> TableView<'_> {
        TableView::new(&self.table)
    }

    /// Returns the persist sequence carried by these parts.
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Returns the within-bucket slots allocated to the snapshot's own chunks,
    /// in chunk-index order (entry 0 is the root's own slot).
    pub fn allocated_slots(&self) -> &[u32] {
        &self.slots
    }
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
    /// Wraps a table that has never been persisted, starting a fresh persist
    /// history at sequence 0 with no allocated slots.
    ///
    /// This is correct *only* for a genuinely new, never-persisted table. Never
    /// feed it a table recovered from the network or extracted from an existing
    /// snapshot: that resets the sequence to 0 and drops the recovered slots,
    /// which would regress the version at the snapshot's metadata chunk
    /// addresses and re-allocate colliding slots, overwriting a newer persisted
    /// version in place. Recovered or extracted state round-trips through
    /// [`from_parts`](Self::from_parts), which preserves the sequence and slots;
    /// the type system keeps recovered state away from this path because neither
    /// [`into_parts`](Self::into_parts) nor [`table`](Self::table) hands out an
    /// owned table, by move or by clone, that `new` would accept.
    ///
    /// Two residual ways to reach a sequence-0 persist are out of this type's
    /// scope and are enforced at persist time by the network-validation wave
    /// (nectar issue #65), not here. First, the public constructors
    /// ([`UsageTable::new`] and friends) legitimately mint a fresh table for a
    /// genuinely new batch, so a forged fresh table persisted at sequence 0 is a
    /// protocol-level concern, not an in-memory representability bug. Second, the
    /// reserve overwrites a snapshot chunk by stamp timestamp rather than by
    /// snapshot sequence, so full cross-version monotonicity against the
    /// *published* sequence needs a compare-and-swap against the live root chunk.
    /// Both land at persist time under issue #65.
    pub const fn new(table: UsageTable) -> Self {
        Self {
            table,
            sequence: 0,
            slots: Vec::new(),
        }
    }

    /// Wraps a fresh, never-persisted table built from a [`Batch`].
    ///
    /// The table's geometry and mutability are read from the batch: an immutable
    /// batch yields a fill-watermark table, a mutable batch
    /// (`batch.immutable() == false`) a wrapping ring. As with [`new`](Self::new),
    /// this is correct *only* for a genuinely new batch and starts the persist
    /// history at sequence 0 with no allocated slots; recovered or extracted state
    /// round-trips through [`from_parts`](Self::from_parts) instead.
    pub fn from_batch(batch: &Batch) -> Result<Self> {
        Ok(Self::new(UsageTable::from_batch(batch)?))
    }

    /// Validates the slots of a table/sequence/slots triple against the table
    /// geometry, the shared check behind [`from_parts`](Self::from_parts) and
    /// the codec's recovery path.
    pub(crate) fn validate_parts(table: &UsageTable, slots: &[u32]) -> Result<()> {
        let capacity = table.bucket_capacity();
        if slots.len() > u16::MAX as usize {
            return Err(UsageError::Malformed("too many allocated chunks"));
        }
        if let Some(&slot) = slots.iter().find(|&&slot| slot >= capacity) {
            return Err(UsageError::InvalidSlot { slot, capacity });
        }
        Ok(())
    }

    /// Builds the opaque [`SnapshotParts`] from a recovered table, sequence, and
    /// slots, for the codec's decode path. Crate-internal: external callers reach
    /// recovery through [`RootInfo::assemble`](crate::RootInfo::assemble), which
    /// flows through here, or through [`into_parts`](Self::into_parts).
    pub(crate) fn recovered_parts(
        table: UsageTable,
        sequence: u64,
        slots: Vec<u32>,
    ) -> Result<SnapshotParts> {
        Self::validate_parts(&table, &slots)?;
        Ok(SnapshotParts {
            table,
            sequence,
            slots,
        })
    }

    /// Reconstructs a snapshot from its opaque [`SnapshotParts`], the only safe
    /// route back from recovered or extracted state.
    ///
    /// The parts carry the table, the persist sequence, and the allocated slots
    /// together, so reconstruction always preserves the sequence and slots: there
    /// is no way to silently downgrade to sequence 0 the way feeding a bare table
    /// to [`new`](Self::new) would. The reconstructed snapshot is inert: it carries
    /// no issuing state, so a mutable batch recovered this way cannot evict its own
    /// chunks. The reserved slots are mapped from the owner and installed
    /// automatically the moment you obtain an [`Issuer`] through
    /// [`issuer`](Self::issuer), which is the only way to advance a counter.
    /// Immutable batches issue without reserved state.
    pub fn from_parts(parts: SnapshotParts) -> Result<Self> {
        let SnapshotParts {
            table,
            sequence,
            slots,
        } = parts;
        Self::validate_parts(&table, &slots)?;
        Ok(Self {
            table,
            sequence,
            slots,
        })
    }

    /// Returns a borrowed, read-only [`TableView`] onto the usage table.
    ///
    /// The view exposes the counters and geometry a caller needs to inspect, but
    /// yields no owned [`UsageTable`]: it only borrows the table and does not
    /// [`Deref`](core::ops::Deref) to it, so cloning or copying the view yields
    /// another borrowed view, never an owned table that [`new`](Self::new) would
    /// accept at sequence 0. This closes the in-memory clone route that would
    /// otherwise downgrade a recovered snapshot.
    pub const fn table(&self) -> TableView<'_> {
        TableView::new(&self.table)
    }

    /// Returns the inner [`UsageTable`] by reference, for crate-internal callers
    /// that need the table type itself (the codec and the issuance handles).
    /// Never exposed publicly: a public `&UsageTable` would let
    /// `snapshot.table().clone()` reproduce an owned table for
    /// [`new`](Self::new).
    pub(crate) const fn table_ref(&self) -> &UsageTable {
        &self.table
    }

    /// Applies an on-chain dilution, growing per-bucket capacity without
    /// changing any counter or cursor. Safe in both modes: the reserved slots
    /// stay below the old capacity and remain valid in the larger ring.
    pub fn dilute(&mut self, new_depth: u8) -> Result<()> {
        self.table.dilute(new_depth)
    }

    /// Merges another table into this snapshot's table by taking the elementwise
    /// maximum of the counters and the maximum depth.
    ///
    /// Immutable only: rejects with [`MutableMerge`](UsageError::MutableMerge) if
    /// either table is mutable. The persistence state (sequence and slots) is
    /// untouched; only the counters and depth join.
    pub fn merge_max(&mut self, other: &UsageTable) -> Result<()> {
        self.table.merge_max(other)
    }

    /// Consumes the snapshot and returns its opaque [`SnapshotParts`]: the inert
    /// usage table, the sequence number, and the allocated slots, bound together.
    ///
    /// The three are returned as one indivisible value precisely so the
    /// persistence state can never be silently dropped on the way out. The parts
    /// expose no owned-table accessor, only a borrowed [`TableView`], so the only
    /// thing you can do with them is inspect them or rebuild through
    /// [`from_parts`](Self::from_parts), which preserves the sequence and slots.
    /// No in-memory route, by move or by clone, leads back from extracted state to
    /// a fresh sequence-0 snapshot.
    ///
    /// The old `into_table` accessor that handed out the table alone is gone, and
    /// `into_parts` no longer yields a bare table either, so downgrading a
    /// recovered snapshot to sequence 0 through [`new`](Self::new) is a compile
    /// error:
    ///
    /// ```compile_fail
    /// use alloy_primitives::B256;
    /// use nectar_postage_usage::{Mutability, Snapshot, UsageTable};
    ///
    /// let snapshot = Snapshot::new(UsageTable::new(B256::repeat_byte(0x42), 20, 16, Mutability::Immutable).unwrap());
    /// // `into_table` no longer exists; only `into_parts` can consume a snapshot.
    /// let table = snapshot.into_table();
    /// ```
    ///
    /// The move path the gate found no longer type-checks: the parts hold the
    /// table privately and surface it only through a borrowed [`TableView`], never
    /// an owned table, so it cannot be moved into `Snapshot::new`:
    ///
    /// ```compile_fail
    /// use alloy_primitives::{Address, B256};
    /// use nectar_postage_usage::{Mutability, Snapshot, UsageTable};
    ///
    /// let owner = Address::repeat_byte(0x11);
    /// let snapshot = Snapshot::new(UsageTable::new(B256::repeat_byte(0x42), 20, 16, Mutability::Immutable).unwrap());
    /// let parts = snapshot.into_parts();
    /// // `parts.table` is private and only a `TableView` is exposed, so a fresh
    /// // sequence-0 snapshot cannot be rebuilt from extracted state.
    /// let mut reset = Snapshot::new(parts.table);
    /// reset.plan_persist(&owner).unwrap();
    /// ```
    ///
    /// The clone path is closed too: [`table`](Self::table) yields a borrowed
    /// [`TableView`] that only borrows the table and does not deref to it, so
    /// cloning or copying the view yields another borrowed view, never an owned
    /// [`UsageTable`] for `Snapshot::new`:
    ///
    /// ```compile_fail
    /// use alloy_primitives::B256;
    /// use nectar_postage_usage::{Mutability, Snapshot, UsageTable};
    ///
    /// let snapshot = Snapshot::new(UsageTable::new(B256::repeat_byte(0x42), 20, 16, Mutability::Immutable).unwrap());
    /// // `table()` returns a `TableView`; cloning it yields another view, not a
    /// // `UsageTable`, so this does not type-check.
    /// let reset = Snapshot::new(snapshot.table().clone());
    /// ```
    #[must_use = "the parts carry the recovered sequence and slots; dropping them discards that state"]
    pub fn into_parts(self) -> SnapshotParts {
        SnapshotParts {
            table: self.table,
            sequence: self.sequence,
            slots: self.slots,
        }
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

    /// Returns the stamp indices the snapshot's own chunks occupy for `owner`,
    /// in chunk-index order.
    ///
    /// These hold the usage data and must never be reused for another chunk; the
    /// list covers every chunk ever allocated (leaves a smaller re-encoding
    /// dropped still occupy their slots on the network). Immutable issuance
    /// cannot reach these slots (they sit below the watermark), so the list is
    /// advisory. Mutable issuance would wrap onto them, so it is enforced: the
    /// [`Issuer`] obtained from [`issuer`](Self::issuer) installs this set before
    /// any write.
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

    /// Recomputes the reserved `(bucket, index)` slots for `owner`.
    ///
    /// These carve the snapshot's own chunks out of a mutable per-bucket ring so
    /// issuance never evicts them. Used internally by [`plan_persist`] and by the
    /// [`Issuer`] constructor; content issuance reaches it only through
    /// [`issuer`](Self::issuer), which installs the set before any write.
    ///
    /// [`plan_persist`]: Self::plan_persist
    pub(crate) fn reserved_slots(&self, owner: &Address) -> BTreeSet<(u32, u32)> {
        self.reserved_stamp_indices(owner)
            .into_iter()
            .map(|index| (index.bucket(), index.index()))
            .collect()
    }

    /// Advances the counter of `bucket`, skipping the reserved slots, and
    /// returns the assigned within-bucket index.
    ///
    /// The sole counter-advance primitive, shared by content issuance (through
    /// [`Issuer`]) and snapshot-chunk allocation (through
    /// [`plan_persist`](Self::plan_persist)). Immutable: a monotone fill watermark
    /// that fails with [`BucketFull`](UsageError::BucketFull) at capacity. Mutable:
    /// a ring cursor that wraps at capacity and skips `reserved`, never returning
    /// `BucketFull` (it returns [`RingExhausted`](UsageError::RingExhausted) only
    /// if every slot is reserved, which the geometry forbids).
    pub(crate) fn record_bucket(
        &mut self,
        bucket: u32,
        reserved: &BTreeSet<(u32, u32)>,
    ) -> Result<u32> {
        // Both branches (fill watermark and reserved-aware ring) live in the
        // shared counter table now. The reserved set is mapped into the table's
        // per-slot protection predicate so a ring never re-emits a slot held by
        // the snapshot's own chunks.
        self.table
            .counters_mut()
            .record(bucket, |slot| reserved.contains(&(bucket, slot)))
            .map_err(crate::table::map_counter_error)
    }

    /// Returns an issuing handle bound to `owner`, the only way to advance a
    /// counter.
    ///
    /// The handle installs the reserved `(bucket, index)` slots for `owner` at
    /// construction, so issuance is reserved-aware by construction, including for
    /// a snapshot recovered through [`from_parts`](Self::from_parts) or
    /// [`RootInfo::assemble`](crate::RootInfo::assemble). It borrows the snapshot
    /// mutably, so [`into_parts`](Self::into_parts) and
    /// [`plan_persist`](Self::plan_persist) cannot run while issuance is live,
    /// which serializes persisting against issuing.
    ///
    /// This single method is the issuance chokepoint. A future network-validation
    /// gate (nectar issue #65) lands here as one precondition, not a cross-cutting
    /// audit: a `from_cache` constructor would mark the snapshot unvalidated and
    /// this method would refuse to issue until `revalidate` clears it.
    pub fn issuer(&mut self, owner: Address) -> Issuer<'_> {
        let reserved = self.reserved_slots(&owner);
        Issuer {
            snapshot: self,
            owner,
            reserved,
        }
    }

    /// Records a content chunk address through the owner's [`Issuer`] and returns
    /// its stamp index. The one-call form behind
    /// [`SnapshotIssuer`](crate::SnapshotIssuer), routed through the single
    /// issuance path.
    #[cfg(feature = "issuer")]
    pub(crate) fn record_address(
        &mut self,
        owner: Address,
        address: &SwarmAddress,
    ) -> Result<StampIndex> {
        self.issuer(owner).record_address(address)
    }

    /// Consumes the snapshot and returns a [`SnapshotIssuer`](crate::SnapshotIssuer)
    /// bound to `owner`, so content stamping drops into a `BatchStamper` while
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

    /// Plans the next persist: bumps the sequence, allocates a slot for any
    /// snapshot chunk that lacks one (folding those stamps into the table), and
    /// encodes.
    ///
    /// Allocation runs to a fixed point (a new snapshot stamp can grow the
    /// encoding by a leaf); slots are never freed, so steady-state persists
    /// allocate nothing. `owner` fixes the snapshot chunk addresses. On error
    /// (such as a full bucket on first allocation) the snapshot is unchanged.
    pub fn plan_persist(&mut self, owner: &Address) -> Result<PersistPlan> {
        let mut work = self.clone();
        // Defence in depth behind the structural guard: the emitted sequence
        // must strictly exceed the current one so a persist can never regress
        // the version at the snapshot's metadata chunk addresses. The only way
        // `self.sequence + 1` fails to advance is a `u64` wrap at the maximum,
        // which we reject rather than fold back to 0.
        work.sequence = self
            .sequence
            .checked_add(1)
            .ok_or(UsageError::Malformed("persist sequence would overflow"))?;

        let batch_id = work.table.batch_id();
        let bucket_depth = work.table.bucket_depth();
        let previously_allocated = self.slots.len();

        let allocate = |work: &mut Self| -> Result<()> {
            let index = work.slots.len() as u16;
            let address = usage_chunk_address(&batch_id, owner, index);
            let bucket = calculate_bucket(&address, bucket_depth);
            // On a mutable batch the ring cursor would otherwise wrap onto a
            // slot already held by an earlier snapshot chunk in the same
            // bucket; carve out the reserved set so this allocation skips them.
            let reserved = work.reserved_slots(owner);
            let slot = work.record_bucket(bucket, &reserved)?;
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

/// An issuing handle bound to a [`Snapshot`] and a batch owner.
///
/// This is the sole content-issuance surface and the only way to advance a
/// counter. It is not publicly constructible: obtain one through
/// [`Snapshot::issuer`], which installs the owner's reserved `(bucket, index)`
/// slots before any write. The owner and the reserved set are baked in, so
/// issuance is reserved-aware by construction and there is no owner-blind path
/// that could wrap a mutable ring onto the snapshot's own chunks. It borrows the
/// snapshot mutably, so persisting cannot run while issuance is live.
#[derive(Debug)]
pub struct Issuer<'s> {
    snapshot: &'s mut Snapshot,
    owner: Address,
    /// The `(bucket, index)` slots held by the snapshot's own chunks, installed
    /// at construction from the snapshot's allocated slots.
    reserved: BTreeSet<(u32, u32)>,
}

impl Issuer<'_> {
    /// Assigns the next unused slot for a content chunk address and returns the
    /// resulting stamp index, skipping the snapshot's reserved slots.
    ///
    /// The only content-issuance entry point. The reserved set was installed at
    /// construction, so a mutable ring never re-emits a slot held by the
    /// snapshot's own chunks.
    pub fn record_address(&mut self, address: &SwarmAddress) -> Result<StampIndex> {
        let bucket = calculate_bucket(address, self.snapshot.table.bucket_depth());
        let index = self.snapshot.record_bucket(bucket, &self.reserved)?;
        Ok(StampIndex::new(bucket, index))
    }

    /// Returns the batch owner this handle issues for.
    pub const fn owner(&self) -> Address {
        self.owner
    }

    /// Returns the counter of a bucket.
    pub fn count(&self, bucket: u32) -> Result<u32> {
        self.snapshot.table_ref().count(bucket)
    }

    /// Returns the highest counter across all buckets.
    pub fn max_count(&self) -> u32 {
        self.snapshot.table_ref().max_count()
    }

    /// Returns whether the bound batch is a mutable ring.
    pub const fn is_mutable(&self) -> bool {
        self.snapshot.table_ref().is_mutable()
    }

    /// Returns whether a bucket can accept another slot assignment.
    pub fn has_capacity(&self, bucket: u32) -> Result<bool> {
        self.snapshot.table_ref().has_capacity(bucket)
    }

    /// Returns the lifetime number of stamps issued, if one is well-defined.
    ///
    /// Immutable: the monotone counter sum is the lifetime count, returned as
    /// `Some`. Mutable: the counters are ring cursors whose sum is a checksum,
    /// not a lifetime count (a wrapped bucket is full yet its cursor may be
    /// small), so this returns `None` rather than passing the checksum off as a
    /// count. Read the checksum itself through [`checksum`](Self::checksum).
    pub const fn stamps_issued(&self) -> Option<u64> {
        if self.snapshot.table_ref().is_mutable() {
            None
        } else {
            Some(self.snapshot.table_ref().total_issued())
        }
    }

    /// Returns the counter sum the snapshot serializes and re-checks: the
    /// lifetime stamp count in immutable mode, a deterministic checksum in
    /// mutable mode.
    pub const fn checksum(&self) -> u64 {
        self.snapshot.table_ref().total_issued()
    }
}
