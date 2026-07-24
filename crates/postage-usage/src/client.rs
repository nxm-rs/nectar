//! A high-level async facade over the snapshot persistence machinery.
//!
//! The low-level path ([`Snapshot`], [`revalidate`](Snapshot::revalidate),
//! [`Validated::plan_persist`], [`seal_plan`], and the chunk-address helpers) is
//! a cross-machine ceremony of roughly a dozen calls: derive the root address,
//! fetch and parse the root, fetch every leaf, reassemble, issue, re-read the
//! live floor, revalidate, plan, choose a strictly increasing timestamp, seal,
//! and upload each sealed chunk. [`BatchStamper`] collapses that into
//! `open` / `stamp` / `flush`, leaving the consumer to supply only the network
//! through the [`SnapshotSource`] and [`SnapshotSink`] traits.
//!
//! Power users who need partial persists, custom timestamp policies, or direct
//! access to the [`PersistPlan`] should keep reaching for the low-level
//! [`Snapshot`] / [`revalidate`](Snapshot::revalidate) / [`seal_plan`] path; the
//! facade is purely additive and never hides it.
//!
//! # Floor safety
//!
//! The whole point of the published-sequence floor (nectar issue #70) is that a
//! persist can never regress the version published at a snapshot's own chunk
//! addresses. That guarantee survives only if a failed network read is never
//! mistaken for "the chunk is absent". [`SnapshotSource::fetch`] therefore
//! distinguishes `Ok(None)` (the network definitively agrees the chunk does not
//! exist) from `Err` (the read could not be completed). [`BatchStamper::open`]
//! and [`BatchStamper::flush`] both abort on `Err`: a transport failure never
//! becomes a fresh sequence-0 start nor a [`PublishedSequence::NONE`] floor over
//! a batch that already has a published root.

use alloc::vec::Vec;

use nectar_clock::{Clock, SystemClock};

use alloy_primitives::Address;
use alloy_signer::SignerSync;
use bytes::Bytes;
use nectar_postage::{Batch, BatchId, StampIndex};
use nectar_primitives::{ChunkAddress, Mainnet, SwarmSpec};
use thiserror::Error;

use crate::codec::RootInfoFor;
use crate::seal::{SealError, SealedChunk, seal_plan};
use crate::snapshot::{PublishedSequence, SnapshotFor};
use crate::{UsageError, usage_chunk_address};

/// Reads a chunk's payload from the network by its single-owner-chunk address.
///
/// The returned [`Bytes`] are the chunk's data payload, the snapshot payload
/// [`RootInfo::parse`] consumes, not the full single-owner-chunk wire form.
///
/// `Ok(None)` means the chunk is *definitively* absent: the network agrees it
/// does not exist. `Err` means the read could not be completed and the caller
/// must *not* treat the chunk as absent. This distinction is load-bearing:
/// treating a transport failure as absence would read the published-sequence
/// floor as [`PublishedSequence::NONE`] and reopen the downgrade that nectar
/// issue #70 closes.
#[auto_impl::auto_impl(&, Arc, Box)]
pub trait SnapshotSource {
    /// The error a failed read reports. A value of this type means the read did
    /// not complete; it never means the chunk is absent.
    type Error: core::error::Error + Send + Sync + 'static;

    /// Fetches the data payload of the chunk at `address`, or `Ok(None)` if the
    /// network confirms no such chunk exists.
    ///
    /// The returned future intentionally carries no `Send` bound: a native
    /// transport whose future is `Send` propagates that through
    /// [`BatchStamper::open`] and [`BatchStamper::flush`] automatically, while a
    /// browser transport with a `!Send` future can still implement this trait.
    fn fetch(
        &self,
        address: &ChunkAddress,
    ) -> impl core::future::Future<Output = Result<Option<Bytes>, Self::Error>>;
}

/// Publishes a sealed snapshot chunk (the single-owner chunk and its stamp) to
/// the network.
#[auto_impl::auto_impl(&, Arc, Box)]
pub trait SnapshotSink {
    /// The error a failed publish reports.
    type Error: core::error::Error + Send + Sync + 'static;

    /// Uploads a sealed snapshot chunk.
    ///
    /// As with [`SnapshotSource::fetch`], the returned future carries no `Send`
    /// bound so a `!Send` browser transport can implement it; native `Send`-ness
    /// propagates through the facade on its own.
    fn push(
        &self,
        sealed: &SealedChunk,
    ) -> impl core::future::Future<Output = Result<(), Self::Error>>;
}

/// Errors produced by the [`BatchStamper`] facade.
///
/// The variants unify the lower-level error taxonomies ([`UsageError`],
/// [`SealError`]) with the consumer-supplied source and sink errors, so a caller
/// matches one enum across the whole `open` / `stamp` / `flush` cycle. The
/// [`Source`](Self::Source) and [`Sink`](Self::Sink) variants carry the
/// transport failures that abort `open` and `flush` rather than degrading into a
/// fresh or [`PublishedSequence::NONE`] persist.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum ClientError<SrcErr, SnkErr>
where
    SrcErr: core::error::Error + Send + Sync + 'static,
    SnkErr: core::error::Error + Send + Sync + 'static,
{
    /// A usage-table operation failed (issuance, revalidation against the floor,
    /// or planning the persist).
    #[error(transparent)]
    Usage(#[from] UsageError),
    /// Sealing the persist plan failed.
    #[error(transparent)]
    Seal(#[from] SealError),
    /// The [`SnapshotSource`] read could not be completed. This is *not* an absence:
    /// `open` and `flush` abort here rather than starting fresh or persisting
    /// against a [`PublishedSequence::NONE`] floor.
    #[error("chunk source read failed")]
    Source(#[source] SrcErr),
    /// The [`SnapshotSink`] publish failed.
    #[error("chunk sink publish failed")]
    Sink(#[source] SnkErr),
    /// A published root committed to a leaf the source reported as absent. A
    /// missing leaf for a published root is corruption, never a fresh batch.
    #[error("published root commits to leaf {index} but the source reports it absent")]
    MissingLeaf {
        /// The snapshot chunk index of the absent leaf (1 is the first leaf).
        index: u16,
    },
}

/// A high-level, async facade that collapses the cross-machine roam into
/// `open` / `stamp` / `flush`.
///
/// [`open`](Self::open) recovers a snapshot already published for this
/// `batch` + owner, or starts a fresh one when the network confirms none exists.
/// [`stamp`](Self::stamp) issues a content stamp locally, with no network round
/// trip. [`flush`](Self::flush) re-reads the live published floor, revalidates,
/// plans, seals with a strictly increasing timestamp, and uploads every sealed
/// chunk through the sink.
///
/// The owner is fixed at `open` from the signer's address, and every snapshot
/// chunk address is derived from it and the batch id, so a second machine
/// holding only the same key and batch id recovers the same state.
///
/// Seal timestamps come from the clock type parameter, defaulting to the
/// system clock; [`open_with_clock`](Self::open_with_clock) injects a
/// deterministic source.
#[derive(Debug)]
pub struct BatchStamperFor<Sg, Src, Snk, S: SwarmSpec = Mainnet, C = SystemClock> {
    signer: Sg,
    owner: Address,
    batch_id: BatchId,
    source: Src,
    sink: Snk,
    snapshot: SnapshotFor<S>,
    /// The timestamp source for seals.
    clock: C,
    /// Whether a persist has been emitted in this session. A clean snapshot that
    /// has already persisted once this session makes [`flush`](Self::flush) a
    /// no-op; a clean but never-persisted snapshot still flushes once so a fresh
    /// batch publishes its sequence-1 root.
    persisted_this_session: bool,
}

/// The [`BatchStamperFor`] of the mainnet spec.
pub type BatchStamper<Sg, Src, Snk, C = SystemClock> = BatchStamperFor<Sg, Src, Snk, Mainnet, C>;

impl<Sg, Src, Snk, S> BatchStamperFor<Sg, Src, Snk, S>
where
    Sg: SignerSync + alloy_signer::Signer,
    Src: SnapshotSource,
    Snk: SnapshotSink,
    S: SwarmSpec,
{
    /// Opens a stamper for `batch`, recovering published state or starting fresh.
    ///
    /// Seal timestamps come from the system clock; see
    /// [`open_with_clock`](Self::open_with_clock) for the recovery contract and
    /// for injecting a deterministic source.
    pub async fn open(
        signer: Sg,
        batch: &Batch<S>,
        source: Src,
        sink: Snk,
    ) -> Result<Self, ClientError<Src::Error, Snk::Error>> {
        Self::open_with_clock(signer, batch, source, sink, SystemClock).await
    }
}

impl<Sg, Src, Snk, S, C> BatchStamperFor<Sg, Src, Snk, S, C>
where
    Sg: SignerSync + alloy_signer::Signer,
    Src: SnapshotSource,
    Snk: SnapshotSink,
    S: SwarmSpec,
    C: Clock,
{
    /// Opens a stamper for `batch` whose seal timestamps read from `clock`,
    /// recovering published state or starting fresh.
    ///
    /// The owner is taken from `signer.address()`. The root chunk address is
    /// derived from the batch id, owner, and index 0, then read through `source`:
    ///
    /// - `Ok(Some(bytes))`: a published root exists. The root is parsed and every
    ///   leaf it commits to is fetched (a leaf the source reports absent is
    ///   corruption, surfaced as [`ClientError::MissingLeaf`], never a fresh
    ///   batch) and the snapshot is reassembled, preserving the recovered
    ///   sequence and slots.
    /// - `Ok(None)`: the network confirms no root exists, so a fresh snapshot is
    ///   built from the batch (picking fill-watermark or ring mutability from
    ///   `batch.immutable()`).
    /// - `Err`: the read could not be completed. This aborts; it never starts
    ///   fresh, so a transport failure cannot downgrade a batch that already has
    ///   a published root.
    pub async fn open_with_clock(
        signer: Sg,
        batch: &Batch<S>,
        source: Src,
        sink: Snk,
        clock: C,
    ) -> Result<Self, ClientError<Src::Error, Snk::Error>> {
        let owner = signer.address();
        let batch_id = batch.id();
        let root_addr = usage_chunk_address(&batch_id, &owner, 0);

        let snapshot = match source
            .fetch(&root_addr)
            .await
            .map_err(ClientError::Source)?
        {
            Some(root_bytes) => {
                // A published root exists: recover its sequence and slots. Every
                // committed leaf must be present; a missing leaf is corruption,
                // not a reason to start over.
                let root = RootInfoFor::<S>::parse(&root_bytes)?;
                let mut leaves: Vec<Bytes> = Vec::with_capacity(usize::from(root.leaf_count()));
                for leaf in 0..root.leaf_count() {
                    // `leaf < leaf_count() <= u16::MAX`, so the increment
                    // cannot overflow.
                    #[allow(clippy::arithmetic_side_effects)]
                    let index = leaf + 1;
                    let leaf_addr = usage_chunk_address(&batch_id, &owner, index);
                    match source
                        .fetch(&leaf_addr)
                        .await
                        .map_err(ClientError::Source)?
                    {
                        Some(bytes) => leaves.push(bytes),
                        None => return Err(ClientError::MissingLeaf { index }),
                    }
                }
                root.assemble(&leaves)?
            }
            // The network confirms no published root: a genuinely fresh batch.
            None => SnapshotFor::from_batch(batch)?,
        };

        Ok(Self {
            signer,
            owner,
            batch_id,
            source,
            sink,
            snapshot,
            clock,
            persisted_this_session: false,
        })
    }

    /// Issues a content stamp for `content`, advancing the matching bucket
    /// counter and returning the assigned stamp index.
    ///
    /// This is local issuance: it touches no network. The reserved snapshot slots
    /// are skipped by construction, so a stamp never lands on the snapshot's own
    /// chunks. Persist the resulting state with [`flush`](Self::flush).
    pub fn stamp(
        &mut self,
        content: &ChunkAddress,
    ) -> Result<StampIndex, ClientError<Src::Error, Snk::Error>> {
        Ok(self.snapshot.issuer(self.owner).record_address(content)?)
    }

    /// Persists the snapshot: re-reads the live floor, revalidates, plans, seals,
    /// and uploads every sealed chunk.
    ///
    /// A no-op (returns `Ok`) when the snapshot is clean and a persist has already
    /// happened this session. Otherwise the live root chunk is re-read through the
    /// source to derive the published floor:
    ///
    /// - `Ok(Some(bytes))`: the floor is the published root's sequence.
    /// - `Ok(None)`: the network confirms no published root, so the floor is
    ///   [`PublishedSequence::NONE`].
    /// - `Err`: the read could not be completed. This aborts; it never persists
    ///   against a floor it could not read.
    ///
    /// The seal timestamp is the clock reading, nudged past the previous seal so the
    /// in-process monotonicity guard in [`seal_plan`] never trips and the reserve
    /// overwrites each metadata chunk in place. A persist whose next sequence does
    /// not strictly exceed the live floor surfaces as
    /// [`UsageError::StaleSequence`].
    pub async fn flush(&mut self) -> Result<(), ClientError<Src::Error, Snk::Error>> {
        if !self.snapshot.is_dirty() && self.persisted_this_session {
            return Ok(());
        }

        let root_addr = usage_chunk_address(&self.batch_id, &self.owner, 0);
        let floor = match self
            .source
            .fetch(&root_addr)
            .await
            .map_err(ClientError::Source)?
        {
            Some(root_bytes) => PublishedSequence::from(&RootInfoFor::<S>::parse(&root_bytes)?),
            // The network confirms no published root: the floor is NONE.
            None => PublishedSequence::NONE,
        };

        let plan = self.snapshot.revalidate(floor)?.plan_persist(&self.owner)?;

        // The seal timestamp must strictly increase across flushes so the reserve
        // overwrites each metadata chunk in place. Read the clock (pre-epoch
        // clamps to zero), but lift it past the previous seal so a coarse or
        // non-advancing clock never trips the in-process guard in `seal_plan`.
        let now = u64::try_from(self.clock.now_secs()).unwrap_or(0);
        // `previous` is a wall-clock seal timestamp in seconds, recorded by
        // an earlier flush, so it sits far below u64::MAX and the increment
        // cannot overflow.
        #[allow(clippy::arithmetic_side_effects)]
        let timestamp = self
            .snapshot
            .last_seal_timestamp()
            .map_or(now, |previous| now.max(previous + 1));

        let sealed = seal_plan(&mut self.snapshot, &plan, timestamp, &self.signer)?;
        for chunk in &sealed {
            self.sink.push(chunk).await.map_err(ClientError::Sink)?;
        }

        self.persisted_this_session = true;
        Ok(())
    }

    /// Returns the wrapped snapshot.
    pub const fn snapshot(&self) -> &SnapshotFor<S> {
        &self.snapshot
    }

    /// Returns the batch owner, fixed at [`open`](Self::open) from the signer.
    pub const fn owner(&self) -> Address {
        self.owner
    }

    /// Returns the batch id this stamper persists into.
    pub const fn batch_id(&self) -> BatchId {
        self.batch_id
    }

    /// Returns whether the snapshot has unpersisted issuance since the last
    /// [`flush`](Self::flush).
    pub const fn is_dirty(&self) -> bool {
        self.snapshot.is_dirty()
    }
}

// Sanctioned tokio adapter tests: the test macro expands to `Runtime::block_on`.
#[cfg(test)]
#[allow(clippy::disallowed_methods)]
mod tests {
    use alloc::collections::BTreeMap;
    use nectar_postage::BucketDepth;
    use std::sync::Mutex;

    use alloy_primitives::B256;
    use alloy_signer_local::PrivateKeySigner;

    use super::*;
    use crate::{Mutability, Snapshot, UsageTable};

    /// A shared in-memory network backing both a [`SnapshotSource`] and a
    /// [`SnapshotSink`], keyed by single-owner-chunk address.
    #[derive(Debug, Default, Clone)]
    struct MemNet {
        chunks: std::sync::Arc<Mutex<BTreeMap<ChunkAddress, Bytes>>>,
    }

    #[derive(Debug, Error)]
    #[error("mem net error")]
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
            use nectar_primitives::ChunkOps;
            let address = *sealed.chunk.address();
            let payload = Bytes::copy_from_slice(sealed.chunk.data().as_ref());
            self.chunks.lock().unwrap().insert(address, payload);
            Ok(())
        }
    }

    /// A source whose every read fails, to prove a transport failure never
    /// degrades into a fresh start or a NONE floor.
    #[derive(Debug, Default, Clone)]
    struct FailingSource;

    impl SnapshotSource for FailingSource {
        type Error = MemError;
        async fn fetch(&self, _address: &ChunkAddress) -> Result<Option<Bytes>, Self::Error> {
            Err(MemError)
        }
    }

    impl SnapshotSink for FailingSource {
        type Error = MemError;
        async fn push(&self, _sealed: &SealedChunk) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    /// A transport whose futures capture an [`Rc`](std::rc::Rc) and are therefore
    /// `!Send`, standing in for a browser transport (`fetch`, websocket) that
    /// cannot produce `Send` futures.
    struct LocalNet(std::rc::Rc<()>);

    impl SnapshotSource for LocalNet {
        type Error = MemError;
        fn fetch(
            &self,
            _address: &ChunkAddress,
        ) -> impl core::future::Future<Output = Result<Option<Bytes>, Self::Error>> {
            let hold = self.0.clone();
            async move {
                let _hold = &hold;
                Ok(None)
            }
        }
    }

    impl SnapshotSink for LocalNet {
        type Error = MemError;
        fn push(
            &self,
            _sealed: &SealedChunk,
        ) -> impl core::future::Future<Output = Result<(), Self::Error>> {
            let hold = self.0.clone();
            async move {
                let _hold = &hold;
                Ok(())
            }
        }
    }

    /// Compile-time proof that a `!Send` transport satisfies [`SnapshotSource`] and
    /// [`SnapshotSink`]. This only type-checks while neither trait bounds its future
    /// with `Send`, which is exactly what lets a single-threaded browser
    /// transport implement the facade. Re-adding a `+ Send` bound breaks here.
    #[test]
    fn non_send_transport_satisfies_the_traits() {
        fn assert_source<S: SnapshotSource>(_: &S) {}
        fn assert_sink<K: SnapshotSink>(_: &K) {}
        let local = LocalNet(std::rc::Rc::new(()));
        assert_source(&local);
        assert_sink(&local);
    }

    fn test_batch(signer: &PrivateKeySigner, immutable: bool) -> Batch {
        Batch::new(
            BatchId::new([0x42; 32]),
            0,
            0,
            signer.address(),
            20,
            BucketDepth::new(16).unwrap(),
            immutable,
        )
    }

    #[tokio::test]
    async fn open_miss_starts_fresh_and_flush_publishes() {
        let signer = PrivateKeySigner::random();
        let batch = test_batch(&signer, true);
        let net = MemNet::default();

        let mut stamper = BatchStamper::open(signer, &batch, net.clone(), net.clone())
            .await
            .unwrap();
        assert_eq!(stamper.snapshot().sequence(), 0);

        let content = ChunkAddress::from(B256::repeat_byte(0x99));
        stamper.stamp(&content).unwrap();
        assert!(stamper.is_dirty());

        stamper.flush().await.unwrap();
        assert_eq!(stamper.snapshot().sequence(), 1);
        assert!(!stamper.is_dirty());

        // A clean, already-persisted snapshot flushes as a no-op.
        stamper.flush().await.unwrap();
        assert_eq!(stamper.snapshot().sequence(), 1);
    }

    #[tokio::test]
    async fn flush_seals_with_the_injected_clock() {
        use nectar_clock::ManualClock;

        let signer = PrivateKeySigner::random();
        let batch = test_batch(&signer, true);
        let net = MemNet::default();

        let clock = ManualClock::new(1_000 * 1_000_000_000);
        let mut stamper =
            BatchStamper::open_with_clock(signer, &batch, net.clone(), net.clone(), &clock)
                .await
                .unwrap();

        stamper
            .stamp(&ChunkAddress::from(B256::repeat_byte(0x99)))
            .unwrap();
        stamper.flush().await.unwrap();
        assert_eq!(stamper.snapshot().last_seal_timestamp(), Some(1_000));

        // A non-advancing clock still seals strictly after the previous seal.
        stamper
            .stamp(&ChunkAddress::from(B256::repeat_byte(0x77)))
            .unwrap();
        stamper.flush().await.unwrap();
        assert_eq!(stamper.snapshot().last_seal_timestamp(), Some(1_001));

        clock.advance(core::time::Duration::from_secs(60));
        stamper
            .stamp(&ChunkAddress::from(B256::repeat_byte(0x55)))
            .unwrap();
        stamper.flush().await.unwrap();
        assert_eq!(stamper.snapshot().last_seal_timestamp(), Some(1_060));
    }

    #[tokio::test]
    async fn open_recovers_published_batch() {
        let signer = PrivateKeySigner::random();
        let owner = signer.address();
        let batch = test_batch(&signer, true);
        let net = MemNet::default();

        // Machine A: fresh, stamp, flush.
        {
            let mut a = BatchStamper::open(signer.clone(), &batch, net.clone(), net.clone())
                .await
                .unwrap();
            a.stamp(&ChunkAddress::from(B256::repeat_byte(0x99)))
                .unwrap();
            a.flush().await.unwrap();
        }

        // Machine B: same key and batch id, recovers the published state.
        let b = BatchStamper::open(signer, &batch, net.clone(), net.clone())
            .await
            .unwrap();
        assert_eq!(
            b.snapshot().sequence(),
            1,
            "recovered the published sequence"
        );
        assert_eq!(b.owner(), owner);
        assert!(!b.snapshot().allocated_slots().is_empty());
    }

    #[tokio::test]
    async fn open_aborts_on_source_error() {
        let signer = PrivateKeySigner::random();
        let batch = test_batch(&signer, true);

        let result = BatchStamper::open(signer, &batch, FailingSource, FailingSource).await;
        assert!(
            matches!(result, Err(ClientError::Source(_))),
            "a failed read must abort open, not start fresh",
        );
    }

    #[tokio::test]
    async fn flush_aborts_on_floor_read_error() {
        let signer = PrivateKeySigner::random();
        let owner = signer.address();
        let batch = test_batch(&signer, true);

        // Open against a working empty network so the snapshot starts fresh, then
        // swap in a source whose floor read fails on flush.
        let net = MemNet::default();
        let mut stamper = BatchStamper {
            signer: signer.clone(),
            owner,
            batch_id: batch.id(),
            source: FailingSource,
            sink: net.clone(),
            snapshot: Snapshot::from_batch(&batch).unwrap(),
            clock: SystemClock,
            persisted_this_session: false,
        };
        stamper
            .stamp(&ChunkAddress::from(B256::repeat_byte(0x99)))
            .unwrap();

        let result = stamper.flush().await;
        assert!(
            matches!(result, Err(ClientError::Source(_))),
            "a failed floor read must abort flush, not persist against NONE",
        );
        // Nothing was published.
        assert!(net.chunks.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn stamp_then_flush_advances_sequence_and_reuses_slots() {
        let signer = PrivateKeySigner::random();
        let net = MemNet::default();
        let batch = test_batch(&signer, true);

        let mut stamper = BatchStamper::open(signer, &batch, net.clone(), net.clone())
            .await
            .unwrap();
        stamper
            .stamp(&ChunkAddress::from(B256::repeat_byte(0x99)))
            .unwrap();
        stamper.flush().await.unwrap();
        let slots_after_first = stamper.snapshot().allocated_slots().to_vec();
        assert_eq!(stamper.snapshot().sequence(), 1);

        stamper
            .stamp(&ChunkAddress::from(B256::repeat_byte(0xab)))
            .unwrap();
        stamper.flush().await.unwrap();
        assert_eq!(stamper.snapshot().sequence(), 2, "sequence advanced");
        assert_eq!(
            stamper.snapshot().allocated_slots(),
            slots_after_first.as_slice(),
            "the snapshot's own slots were reused, not re-allocated",
        );
    }

    #[tokio::test]
    async fn flush_rejects_stale_sequence() {
        let signer = PrivateKeySigner::random();
        let owner = signer.address();
        let batch = test_batch(&signer, true);
        let net = MemNet::default();

        // Publish sequence 1 and 2 from machine A so the live floor sits at 2.
        {
            let mut a = BatchStamper::open(signer.clone(), &batch, net.clone(), net.clone())
                .await
                .unwrap();
            a.stamp(&ChunkAddress::from(B256::repeat_byte(0x01)))
                .unwrap();
            a.flush().await.unwrap();
            a.stamp(&ChunkAddress::from(B256::repeat_byte(0x02)))
                .unwrap();
            a.flush().await.unwrap();
            assert_eq!(a.snapshot().sequence(), 2);
        }

        // A stale machine B sitting at sequence 1: open it, but rewind its
        // snapshot to a sequence-1 state, then issue and flush. The live floor (2)
        // rejects the next sequence (2).
        let table = UsageTable::new(
            batch.id(),
            20,
            BucketDepth::new(16).unwrap(),
            Mutability::Immutable,
        )
        .unwrap();
        let mut stale = Snapshot::new(table);
        stale
            .revalidate(PublishedSequence::NONE)
            .unwrap()
            .plan_persist(&owner)
            .unwrap();
        assert_eq!(stale.sequence(), 1);

        let mut b = BatchStamper {
            signer,
            owner,
            batch_id: batch.id(),
            source: net.clone(),
            sink: net.clone(),
            snapshot: stale,
            clock: SystemClock,
            persisted_this_session: false,
        };
        b.stamp(&ChunkAddress::from(B256::repeat_byte(0x03)))
            .unwrap();
        let result = b.flush().await;
        assert!(
            matches!(
                result,
                Err(ClientError::Usage(UsageError::StaleSequence {
                    next: 2,
                    floor: 2
                })),
            ),
            "a persist whose next sequence does not exceed the live floor is rejected",
        );
    }
}
