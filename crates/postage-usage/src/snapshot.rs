//! A usage table together with its persistence state.

use alloc::collections::BTreeSet;
use alloc::vec::Vec;

use alloy_primitives::Address;
use bytes::Bytes;
use nectar_postage::{Batch, BatchId, StampIndex, calculate_bucket};
use nectar_primitives::{SocId, SwarmAddress};

use crate::codec::{self, Encoded, RootInfo};
use crate::table::{TableView, UsageTable};
use crate::{Result, UsageError, usage_chunk_address, usage_chunk_id};

/// The published persist sequence at a snapshot's root chunk address, the floor
/// a planned persist must strictly exceed.
///
/// This value MUST be derived from a *live* network read of the published root
/// single-owner chunk: fetch the chunk at
/// [`usage_chunk_address`](crate::usage_chunk_address)`(batch_id, owner, 0)`,
/// parse it with [`RootInfo::parse`](crate::RootInfo::parse), and read its
/// [`sequence`](crate::RootInfo::sequence). It must never be taken from a cache
/// nor from the snapshot being persisted: those are exactly the stale values the
/// floor exists to defeat. The type cannot prove freshness on its own; it only
/// makes the precondition legible at the call site and makes the
/// [`NONE`](Self::NONE) bypass auditable.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct PublishedSequence(u64);

impl PublishedSequence {
    /// The floor when the consumer has confirmed, via a live network read, that
    /// no published root chunk exists at this `batch_id` + `owner` + index-0
    /// address (a genuinely new batch). The first legitimate persist emits
    /// sequence 1, and `1 > 0` clears this floor.
    pub const NONE: Self = Self(0);

    /// Wraps a published sequence read live from the network.
    pub const fn new(published: u64) -> Self {
        Self(published)
    }

    /// Returns the wrapped published sequence.
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl From<&RootInfo> for PublishedSequence {
    fn from(r: &RootInfo) -> Self {
        Self(r.sequence())
    }
}

/// A [`UsageTable`] together with the state needed to persist it inside its
/// own batch: a monotone sequence number and the within-bucket slots
/// allocated to the snapshot chunks themselves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    table: UsageTable,
    sequence: u64,
    slots: Vec<u32>,
    /// The counter sum ([`UsageTable::total_issued`]) captured at the last
    /// planned persist, the baseline [`is_dirty`](Self::is_dirty) and
    /// [`stamps_since_persist`](Self::stamps_since_persist) compare against.
    /// `None` until the first persist, so a never-persisted snapshot that has
    /// already issued reads as dirty.
    issued_at_persist: Option<u64>,
    /// The stamp timestamp the previous persist was sealed with in this process,
    /// the in-process monotonicity floor [`seal_plan`](crate::seal_plan) checks a
    /// new timestamp against. `None` until the first seal, and reset to `None` on
    /// recovery (the published timestamp lives in the reserve, not the snapshot;
    /// the cross-process floor is [`PublishedSequence`], nectar issue #70). This
    /// is the single-owner clock-skew guard: it stops a later persist in the same
    /// process from stamping a non-increasing timestamp, which the reserve would
    /// refuse to overwrite the metadata chunk with.
    #[cfg(feature = "seal")]
    last_seal_timestamp: Option<u64>,
}

/// One chunk of a persist plan: the payload to publish and the slot to
/// stamp it with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedChunk {
    /// The snapshot chunk index (0 is the root).
    pub index: u16,
    /// The single-owner chunk id.
    pub id: SocId,
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

/// The output of [`Validated::plan_persist`]: every chunk of the snapshot in
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
    /// The stamp timestamp the previous persist was sealed with in this process,
    /// or `None` if none has been sealed yet. [`seal_plan`](crate::seal_plan)
    /// requires the seal timestamp to strictly exceed this, so a later persist
    /// can never stamp the metadata chunks with a non-increasing timestamp the
    /// reserve would refuse to overwrite in place.
    #[cfg(feature = "seal")]
    pub previous_timestamp: Option<u64>,
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
    /// scope and are enforced by [`Snapshot::revalidate`]'s [`PublishedSequence`]
    /// floor (nectar issue #70), not here. First, the public constructors
    /// ([`UsageTable::new`] and friends) legitimately mint a fresh table for a
    /// genuinely new batch, so a forged fresh table persisted at sequence 0 is a
    /// protocol-level concern, not an in-memory representability bug. Second, the
    /// reserve overwrites a snapshot chunk by stamp timestamp rather than by
    /// snapshot sequence, so full cross-version monotonicity against the
    /// *published* sequence needs a compare-and-swap against the live root chunk.
    /// Both are enforced by [`Snapshot::revalidate`]'s [`PublishedSequence`]
    /// floor.
    pub const fn new(table: UsageTable) -> Self {
        Self {
            table,
            sequence: 0,
            slots: Vec::new(),
            // A genuinely fresh table has never persisted, so any prior issuance
            // is unpersisted: no baseline yet.
            issued_at_persist: None,
            #[cfg(feature = "seal")]
            last_seal_timestamp: None,
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
        if slots.len() > usize::from(u16::MAX) {
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
        // A recovered snapshot reflects a published persist, so it starts clean:
        // its baseline is the recovered counter sum, and issuance afterwards makes
        // it dirty again.
        let issued_at_persist = Some(table.total_issued());
        Ok(Self {
            table,
            sequence,
            slots,
            issued_at_persist,
            // Recovery resets the in-process seal clock: the published timestamp
            // lives in the reserve, and the cross-process guard is the
            // `PublishedSequence` floor (nectar issue #70), not this field.
            #[cfg(feature = "seal")]
            last_seal_timestamp: None,
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
    /// // The move/clone guard is what fails here: `parts.table` is private and
    /// // only a `TableView` is exposed, so `Snapshot::new(parts.table)` cannot
    /// // rebuild a fresh sequence-0 snapshot from extracted state. The persist
    /// // path below is shown only to reference the real call; it is never
    /// // reached because the line above does not type-check.
    /// let mut reset = Snapshot::new(parts.table);
    /// reset.revalidate(nectar_postage_usage::PublishedSequence::NONE).unwrap().plan_persist(&owner).unwrap();
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

    /// Returns the stamp timestamp the previous persist was sealed with in this
    /// process, or `None` if none has been sealed yet (including just after
    /// recovery). This is the in-process monotonicity floor a new seal must
    /// strictly exceed.
    #[cfg(feature = "seal")]
    pub const fn last_seal_timestamp(&self) -> Option<u64> {
        self.last_seal_timestamp
    }

    /// Records the stamp timestamp a persist was sealed with, advancing the
    /// in-process monotonicity floor. Crate-internal: [`seal_plan`](crate::seal_plan)
    /// calls it after a successful seal so the next persist's
    /// [`PersistPlan::previous_timestamp`] reflects it.
    #[cfg(feature = "seal")]
    pub(crate) const fn record_seal_timestamp(&mut self, timestamp: u64) {
        self.last_seal_timestamp = Some(timestamp);
    }

    /// Returns the within-bucket slots allocated to snapshot chunks, in
    /// chunk-index order (entry 0 is the root's own slot).
    pub fn allocated_slots(&self) -> &[u32] {
        &self.slots
    }

    /// Returns whether the snapshot has unpersisted issuance: stamps issued (or,
    /// in mutable mode, ring churn) since the last planned persist.
    ///
    /// A fresh, never-persisted snapshot is dirty the moment it issues anything,
    /// and stays dirty until its first persist. After a persist the snapshot is
    /// clean until the next stamp. In mutable mode this tracks the counter
    /// checksum, so any ring movement reads as dirty even though no lifetime
    /// count is well-defined. Persisting through
    /// [`plan_persist`](Validated::plan_persist) clears it.
    pub const fn is_dirty(&self) -> bool {
        match self.issued_at_persist {
            Some(baseline) => self.table.total_issued() != baseline,
            // Never persisted: dirty exactly when it has issued anything.
            None => self.table.total_issued() != 0,
        }
    }

    /// Returns the number of stamps issued since the last persist, if a count is
    /// well-defined.
    ///
    /// Immutable: the rise in the monotone counter sum since the last persist
    /// (the full count so far if never persisted), returned as `Some`. Mutable:
    /// the counters are ring cursors whose sum is a checksum that can fall on
    /// wrap, so there is no unpersisted *count* to give and this returns `None`;
    /// use [`is_dirty`](Self::is_dirty) to tell whether a mutable snapshot has
    /// unpersisted churn.
    pub const fn stamps_since_persist(&self) -> Option<u64> {
        if self.table.is_mutable() {
            return None;
        }
        let baseline = match self.issued_at_persist {
            Some(baseline) => baseline,
            None => 0,
        };
        Some(self.table.total_issued().saturating_sub(baseline))
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
                // `validate_parts` caps the slot list at u16::MAX entries,
                // so every index fits u16.
                #[allow(clippy::as_conversions)]
                let index = index as u16;
                let address = usage_chunk_address(&batch_id, owner, index);
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
    /// [`plan_persist`]: Validated::plan_persist
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
    /// [`plan_persist`](Validated::plan_persist)). Immutable: a monotone fill watermark
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
    /// [`revalidate`](Self::revalidate) cannot run while issuance is live,
    /// which serializes persisting against issuing.
    ///
    /// This single method is the issuance chokepoint. Issuance still flows
    /// through here, while the network-validation gate (nectar issue #70) guards
    /// *persistence*: [`revalidate`](Self::revalidate) checks the planned
    /// sequence against a [`PublishedSequence`] floor and is the only route to a
    /// [`PersistPlan`], so a snapshot can issue but cannot persist without first
    /// clearing the floor.
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
    /// Crate-internal: the only caller is
    /// [`plan_persist`](Validated::plan_persist), which turns the [`Encoded`]
    /// payloads into a public [`PersistPlan`]. Fails if the snapshot has never
    /// been persisted (no slot is allocated for the root).
    pub(crate) fn encode(&self) -> Result<Encoded> {
        codec::encode(&self.table, self.sequence, &self.slots)
    }

    /// Admits the snapshot to persistence against a published floor read live
    /// from the network, returning a [`Validated`] handle that can mint a
    /// [`PersistPlan`].
    ///
    /// `floor` is the [`PublishedSequence`] the consumer read from the live root
    /// chunk (or [`PublishedSequence::NONE`] for a genuinely new batch). The next
    /// sequence (`self.sequence() + 1`) must strictly exceed it, otherwise this
    /// returns [`StaleSequence`](UsageError::StaleSequence): a fresh-construction
    /// snapshot (sequence 0, next 1) is rejected once the published floor is at
    /// least 1, and a stale recovered snapshot (cached sequence `R`) is rejected
    /// unless `R + 1` exceeds the floor. A `u64` wrap at the maximum surfaces as
    /// [`Malformed`](UsageError::Malformed) overflow rather than a stale error.
    ///
    /// The returned handle borrows `&mut self`, so issuance (which also takes
    /// `&mut self` through [`issuer`](Self::issuer)) and validation cannot be
    /// live at once and the admission ticket cannot outlive a concurrent
    /// mutation. The floor is captured here, so keep the
    /// `revalidate` -> [`plan_persist`](Validated::plan_persist) window tight:
    /// the network floor may advance afterwards.
    pub fn revalidate(&mut self, floor: PublishedSequence) -> Result<Validated<'_>> {
        let next = self
            .sequence
            .checked_add(1)
            .ok_or(UsageError::Malformed("persist sequence would overflow"))?;
        if next <= floor.get() {
            return Err(UsageError::StaleSequence {
                next,
                floor: floor.get(),
            });
        }
        Ok(Validated {
            snapshot: self,
            floor: floor.get(),
        })
    }
}

/// A snapshot admitted to persistence: its next sequence has been checked to
/// strictly exceed a published floor the consumer read from the live network.
/// The only type that can mint a [`PersistPlan`]. Not publicly constructible;
/// obtain one via [`Snapshot::revalidate`]. Short-lived admission ticket, not a
/// durable capability: keep the
/// [`revalidate`](Snapshot::revalidate) -> [`plan_persist`](Self::plan_persist)
/// window tight, since the floor is captured at revalidate time and the network
/// floor may advance afterwards.
#[derive(Debug)]
pub struct Validated<'s> {
    snapshot: &'s mut Snapshot,
    floor: u64,
}

impl Validated<'_> {
    /// Plans the next persist: bumps the sequence, allocates a slot for any
    /// snapshot chunk that lacks one (folding those stamps into the table), and
    /// encodes.
    ///
    /// Allocation runs to a fixed point (a new snapshot stamp can grow the
    /// encoding by a leaf); slots are never freed, so steady-state persists
    /// allocate nothing. `owner` fixes the snapshot chunk addresses. On error
    /// (such as a full bucket on first allocation) the snapshot is unchanged.
    ///
    /// The common steady-state persist allocates nothing and so never mutates
    /// the counter table; it encodes against the live table in place and never
    /// clones the counts vector. A persist that must allocate a fresh snapshot
    /// slot does its allocation on a working clone, so a mid-allocation failure
    /// (a full bucket on a late leaf, say) leaves the snapshot untouched.
    ///
    /// The plan is the chunks to publish, so it is `#[must_use]`: dropping it on
    /// the floor discards the persist (under the crate's `unused_must_use` deny,
    /// ignoring it does not compile):
    ///
    /// ```compile_fail
    /// # #![deny(unused_must_use)]
    /// use alloy_primitives::{Address, B256};
    /// use nectar_postage_usage::{Mutability, PublishedSequence, Snapshot, UsageTable};
    ///
    /// let owner = Address::repeat_byte(0x11);
    /// let table = UsageTable::new(B256::repeat_byte(0x42), 20, 16, Mutability::Immutable).unwrap();
    /// let mut snapshot = Snapshot::new(table);
    /// // Ignoring the plan is a compile error: the persist would be silently lost.
    /// snapshot.revalidate(PublishedSequence::NONE).unwrap().plan_persist(&owner);
    /// ```
    #[must_use = "the persist plan is the chunks to publish; dropping it discards the planned persist"]
    pub fn plan_persist(&mut self, owner: &Address) -> Result<PersistPlan> {
        // Defence in depth behind the structural guard: the emitted sequence
        // must strictly exceed the current one so a persist can never regress
        // the version at the snapshot's metadata chunk addresses. The only way
        // `self.snapshot.sequence + 1` fails to advance is a `u64` wrap at the
        // maximum, which we reject rather than fold back to 0.
        let sequence = self
            .snapshot
            .sequence
            .checked_add(1)
            .ok_or(UsageError::Malformed("persist sequence would overflow"))?;
        // Re-assert the published floor against the sequence we are about to
        // emit. The `checked_add` above runs first so a wrap stays a Malformed
        // overflow rather than masquerading as a stale sequence.
        if sequence <= self.floor {
            return Err(UsageError::StaleSequence {
                next: sequence,
                floor: self.floor,
            });
        }

        let batch_id = self.snapshot.table.batch_id();
        let bucket_depth = self.snapshot.table.bucket_depth();
        let previously_allocated = self.snapshot.slots.len();
        let previous_sequence = self.snapshot.sequence;

        // Probe the encoding against the live snapshot. The new sequence goes
        // into the root header, so bump it in place first, then encode once: when
        // the existing slots already cover every leaf (the steady state) the
        // counter table never changes, so there is nothing to clone for rollback
        // and the bumped sequence stands. The root slot is allocated only once a
        // snapshot has persisted, so a never-persisted snapshot (no slots) always
        // takes the allocation path below; the encode probe needs a root slot.
        let steady_state_encoded = if self.snapshot.slots.is_empty() {
            None
        } else {
            self.snapshot.sequence = sequence;
            let encoded = self.snapshot.encode()?;
            // The existing slots already cover the root and every leaf chunk, so
            // no further allocation is needed.
            if self.snapshot.slots.len() > encoded.leaves.len() {
                Some(encoded)
            } else {
                // Allocation is needed after all: undo the in-place sequence bump
                // so the clone path below starts from the untouched snapshot and
                // a failure there is a clean rollback.
                self.snapshot.sequence = previous_sequence;
                None
            }
        };

        let encoded = if let Some(encoded) = steady_state_encoded {
            // No allocation: the table is unchanged and the sequence is already
            // bumped, so reuse the probe encoding.
            encoded
        } else {
            // Allocation mutates the counter table (and may fail part way on a
            // full bucket), so run it on a working clone and only commit the
            // clone once the whole plan succeeds.
            let mut work = self.snapshot.clone();
            // Bump the sequence on the clone before encoding so the root payload
            // carries the new sequence.
            work.sequence = sequence;

            let allocate = |work: &mut Snapshot| -> Result<()> {
                // The allocation loop stops once the slots outnumber the
                // leaves (at most the digests that fit a root chunk), far
                // below u16::MAX.
                #[allow(clippy::as_conversions)]
                let index = work.slots.len() as u16;
                let address = usage_chunk_address(&batch_id, owner, index);
                let bucket = calculate_bucket(&address, bucket_depth);
                // On a mutable batch the ring cursor would otherwise wrap onto a
                // slot already held by an earlier snapshot chunk in the same
                // bucket; carve out the reserved set so this allocation skips
                // them.
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

            // Commit the allocation: the snapshot adopts the working clone's
            // table, slots, and sequence. Up to here `self.snapshot` is
            // untouched, so any earlier failure is a clean rollback.
            *self.snapshot = work;
            encoded
        };

        // The snapshot now reflects this persist, so capture the counter sum as
        // the clean baseline: issuance after this point makes the snapshot dirty.
        self.snapshot.issued_at_persist = Some(self.snapshot.table.total_issued());

        let slots = &self.snapshot.slots;
        // The leaf count is bounded by the chunk geometry (at most
        // MAX_PAYLOAD_SIZE / 32 digests fit the root), so `1 + len` cannot
        // overflow.
        #[allow(clippy::arithmetic_side_effects)]
        let mut chunks = Vec::with_capacity(1 + encoded.leaves.len());
        let payloads = core::iter::once(&encoded.root).chain(encoded.leaves.iter());
        for (index, payload) in payloads.enumerate() {
            // Chunk indices count the root plus the leaves, bounded by the
            // allocated slot list (`validate_parts` caps it at u16::MAX).
            #[allow(clippy::as_conversions)]
            let index = index as u16;
            let id = usage_chunk_id(&batch_id, index);
            let address = usage_chunk_address(&batch_id, owner, index);
            let bucket = calculate_bucket(&address, bucket_depth);
            // The allocation loop above breaks only once
            // `slots.len() > encoded.leaves.len()`, so every payload index
            // (root plus one per leaf) has an allocated slot.
            #[allow(clippy::indexing_slicing)]
            chunks.push(PlannedChunk {
                index,
                id,
                address,
                stamp_index: StampIndex::new(bucket, slots[usize::from(index)]),
                payload: payload.clone(),
                newly_allocated: usize::from(index) >= previously_allocated,
            });
        }

        Ok(PersistPlan {
            batch_id,
            sequence,
            chunks,
            #[cfg(feature = "seal")]
            previous_timestamp: self.snapshot.last_seal_timestamp,
        })
    }

    /// Returns the published floor this admission was checked against.
    pub const fn floor(&self) -> u64 {
        self.floor
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
        Ok(self.record_address_reporting_wrap(address)?.0)
    }

    /// Issues like [`record_address`](Self::record_address) but also reports
    /// whether the write wrapped the ring onto a previously-used slot, evicting
    /// whatever content held it.
    ///
    /// The boolean is `true` only for a mutable batch whose target bucket cursor
    /// had reached the bucket bound, so the write wrapped and the assigned slot
    /// was last used by an earlier stamp (the snapshot's own reserved slots are
    /// skipped and never reported as evicted). It fires at the wrap boundary,
    /// once per ring cycle: a wrap resets the cursor low, so an immediately
    /// following overwrite is not re-flagged until the cursor climbs back to the
    /// bound. An immutable bucket never wraps, so the flag is always `false`
    /// there: a full immutable bucket fails instead of overwriting. A caller that
    /// must not evict live content can stop or rotate batches when this reports
    /// `true`.
    pub fn record_address_reporting_wrap(
        &mut self,
        address: &SwarmAddress,
    ) -> Result<(StampIndex, bool)> {
        let bucket = calculate_bucket(address, self.snapshot.table.bucket_depth());
        // Decide before the write: afterwards the cursor has already advanced.
        let wrapped = self.will_wrap(bucket)?;
        let index = self.snapshot.record_bucket(bucket, &self.reserved)?;
        Ok((StampIndex::new(bucket, index), wrapped))
    }

    /// Returns whether the next content write into `bucket` would wrap a mutable
    /// ring onto a previously-used slot, overwriting whatever content held it.
    ///
    /// `true` only for a mutable batch whose bucket cursor sits at the bound (no
    /// fresh slot left); `false` for an immutable bucket (which fails at capacity
    /// rather than overwriting) and for a mutable bucket whose cursor is below the
    /// bound. Because a wrap resets the cursor low, this predicts the wrap at the
    /// boundary once per ring cycle rather than every overwrite within a cycle:
    /// the underlying ring cursor records position, not whether the bucket has
    /// ever filled. This is the look-before-you-leap companion to
    /// [`record_address_reporting_wrap`](Self::record_address_reporting_wrap):
    /// the same bucket, queried instead of written.
    pub fn will_wrap(&self, bucket: u32) -> Result<bool> {
        if !self.snapshot.table_ref().is_mutable() {
            return Ok(false);
        }
        // A mutable ring is at its wrap boundary exactly when the bucket has no
        // fresh slot left (the cursor sits at the bound).
        self.snapshot
            .table_ref()
            .has_capacity(bucket)
            .map(|free| !free)
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

/// `Arbitrary` implementation that generates *valid* snapshots, routed
/// through the same [`Snapshot::from_parts`] validation the recovery path
/// uses: the table is valid by construction, every allocated slot sits below
/// the per-bucket capacity, and the sequence leaves headroom so
/// [`Snapshot::revalidate`] + [`Validated::plan_persist`] can advance it. A
/// structured fuzz target can therefore persist the snapshot and assert the
/// parse/assemble round trip instead of merely "no panic".
#[cfg(any(test, feature = "arbitrary"))]
mod arbitrary_impls {
    use alloc::vec::Vec;
    use arbitrary::{Arbitrary, Result as ArbitraryResult, Unstructured};

    use super::Snapshot;
    use crate::UsageTable;

    impl<'a> Arbitrary<'a> for Snapshot {
        fn arbitrary(u: &mut Unstructured<'a>) -> ArbitraryResult<Self> {
            let table = UsageTable::arbitrary(u)?;
            // Leave headroom so revalidate/plan_persist can advance the
            // sequence without overflowing.
            let sequence = u.int_in_range(0..=u64::MAX - 1)?;
            // Allocated slots must sit below the per-bucket capacity
            // (`validate_parts`); entry 0, when present, is the root's own
            // slot. An empty list is a never-persisted snapshot.
            let capacity = table.bucket_capacity();
            let allocated = u.int_in_range(0..=4usize)?;
            let mut slots = Vec::with_capacity(allocated);
            for _ in 0..allocated {
                // `bucket_capacity()` is `1 << counter_bits >= 1`, so the
                // decrement cannot underflow.
                #[allow(clippy::arithmetic_side_effects)]
                slots.push(u.int_in_range(0..=capacity - 1)?);
            }
            // Cannot fail for the values generated above; map defensively
            // rather than panicking inside the generator.
            let parts = Self::recovered_parts(table, sequence, slots)
                .map_err(|_| arbitrary::Error::IncorrectFormat)?;
            Self::from_parts(parts).map_err(|_| arbitrary::Error::IncorrectFormat)
        }
    }
}
