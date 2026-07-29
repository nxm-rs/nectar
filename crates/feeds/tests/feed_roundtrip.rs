//! Write-read round trips, latest-update search and the trust seam through
//! the public API, all over in-memory stores.
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        clippy::panic
    )
)]

use alloy_primitives::hex;
use alloy_signer_local::PrivateKeySigner;
use nectar_feeds::{Feed, FeedError, Getter, Index, Sequence, Topic, Updater};
use nectar_primitives::chunk::{
    ChunkAddress, ChunkOps, SingleOwnerChunk, SingleOwnerOnlyChunkSet, TrustedSource, Unverified,
};
use nectar_primitives::store::{
    ChunkGet, ChunkHas, ChunkPut, ChunkStoreError, MemoryStore, SingleOwnerGet,
};
use nectar_primitives::{
    Chunk, ChunkRegistry, DEFAULT_BODY_SIZE, DefaultContentChunk, DefaultMemoryStore,
    StandardChunkSet,
};
use nectar_testing::{Drive, run};
use proptest::prelude::*;

use core::future::Future;
use core::num::NonZeroUsize;
use core::pin::Pin;
use core::task::{Context, Poll, Waker};
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

/// Single-owner-only store the feed handles are typed against.
type SocStore = MemoryStore<SingleOwnerOnlyChunkSet>;

fn signer() -> PrivateKeySigner {
    let pk = hex!("2c7536e3605d9c16a7a3d7b1898e529396a65c23a3bcbd4012a11cf2731b0fbc");
    PrivateKeySigner::from_slice(&pk).unwrap()
}

fn feed_for(signer: &PrivateKeySigner) -> Feed {
    Feed::new(Topic::from_label("roundtrip"), signer.address())
}

#[test]
fn append_then_read_round_trips() {
    run(async {
        let signer = signer();
        let feed = feed_for(&signer);
        let store = SocStore::new();
        let mut updater = Updater::new(feed, &store, &signer);

        let payloads: [&[u8]; 3] = [b"first", b"second", b"third"];
        for payload in payloads {
            updater.append(payload.to_vec()).await.unwrap();
        }
        assert_eq!(updater.next_index(), Some(Sequence::new(3)));

        let getter = Getter::new(feed, &store);
        for (n, payload) in (0u64..).zip(payloads) {
            let update = getter.at(Sequence::new(n)).await.unwrap();
            assert_eq!(update.payload().as_ref(), payload);
            assert_eq!(update.index(), &Sequence::new(n));
            assert_eq!(update.address(), &feed.update_address(&Sequence::new(n)));
            assert_eq!(update.content().data().as_ref(), payload);
        }
    });
}

#[test]
fn empty_feed_has_no_latest() {
    run(async {
        let signer = signer();
        let getter = Getter::new(feed_for(&signer), SocStore::new());

        for latest in [
            getter.latest().await.unwrap(),
            getter.latest_linear_from(Sequence::ZERO).await.unwrap(),
        ] {
            assert!(latest.update.is_none());
            assert_eq!(latest.next, Some(Sequence::ZERO));
        }
    });
}

#[test]
fn finders_agree_while_the_feed_grows() {
    run(async {
        let signer = signer();
        let feed = feed_for(&signer);
        let store = SocStore::new();
        let mut updater = Updater::new(feed, &store, &signer);
        let getter = Getter::new(feed, &store);

        for n in 0u64..33 {
            updater.append(n.to_be_bytes().to_vec()).await.unwrap();

            let latest = getter.latest().await.unwrap();
            let linear = getter.latest_linear_from(Sequence::ZERO).await.unwrap();

            for found in [&latest, &linear] {
                let update = found.update.as_ref().unwrap();
                assert_eq!(update.index(), &Sequence::new(n));
                assert_eq!(update.payload().as_ref(), n.to_be_bytes());
                assert_eq!(found.next, Some(Sequence::new(n + 1)));
            }
        }
    });
}

#[test]
fn latest_from_respects_the_floor() {
    run(async {
        let signer = signer();
        let feed = feed_for(&signer);
        let store = SocStore::new();
        let mut updater = Updater::new(feed, &store, &signer);
        for n in 0u64..5 {
            updater.append(n.to_be_bytes().to_vec()).await.unwrap();
        }
        let getter = Getter::new(feed, &store);

        let latest = getter.latest_from(Sequence::new(3)).await.unwrap();
        assert_eq!(latest.update.unwrap().index(), &Sequence::new(4));
        assert_eq!(latest.next, Some(Sequence::new(5)));

        // A floor past the head is an empty result carrying the floor back.
        let latest = getter.latest_from(Sequence::new(5)).await.unwrap();
        assert!(latest.update.is_none());
        assert_eq!(latest.next, Some(Sequence::new(5)));
    });
}

#[test]
fn wrong_signer_is_rejected_before_the_write() {
    run(async {
        let signer = signer();
        let other = PrivateKeySigner::from_slice(&[0x42u8; 32]).unwrap();
        let feed = Feed::<DEFAULT_BODY_SIZE>::new(Topic::from_label("roundtrip"), other.address());
        let store = SocStore::new();
        let mut updater = Updater::new(feed, &store, &signer);

        let err = updater.append(b"payload".to_vec()).await.unwrap_err();
        assert!(matches!(err, FeedError::OwnerMismatch { .. }));
        assert!(store.is_empty());
        // The rejected append does not advance the cursor.
        assert_eq!(updater.next_index(), Some(Sequence::ZERO));
    });
}

#[test]
fn sequence_space_exhausts_at_the_top_slot() {
    run(async {
        let signer = signer();
        let feed = feed_for(&signer);
        let store = SocStore::new();
        let mut updater = Updater::resume(feed, &store, &signer, Sequence::MAX);

        let update = updater.append(b"last".to_vec()).await.unwrap();
        assert_eq!(update.index(), &Sequence::MAX);
        assert_eq!(updater.next_index(), None);
        assert!(matches!(
            updater.append(b"over".to_vec()).await.unwrap_err(),
            FeedError::Exhausted
        ));

        // The finder resumed at the top slot reports the space as spent.
        let getter = Getter::new(feed, &store);
        let latest = getter.latest_from(Sequence::MAX).await.unwrap();
        assert_eq!(latest.update.unwrap().index(), &Sequence::MAX);
        assert_eq!(latest.next, None);
        let linear = getter.latest_linear_from(Sequence::MAX).await.unwrap();
        assert_eq!(linear.update.unwrap().index(), &Sequence::MAX);
        assert_eq!(linear.next, None);
    });
}

#[test]
fn missing_update_surfaces_the_store_error() {
    run(async {
        let signer = signer();
        let getter = Getter::new(feed_for(&signer), SocStore::new());
        assert!(matches!(
            getter.at(Sequence::ZERO).await.unwrap_err(),
            FeedError::Store(_)
        ));
    });
}

/// Store double reading back unverified parses of what the inner store holds,
/// exercising the getter's certification path.
struct Unverifying<'a>(&'a SocStore);

impl ChunkGet<SingleOwnerOnlyChunkSet> for Unverifying<'_> {
    type Trust = Unverified;
    type Error = ChunkStoreError;

    async fn get(
        &self,
        address: &ChunkAddress,
    ) -> Result<Chunk<Unverified, SingleOwnerOnlyChunkSet>, Self::Error> {
        let chunk = ChunkGet::get(self.0, address).await?;
        Chunk::parse(*address, &chunk.typed_bytes())
            .map_err(|_| ChunkStoreError::not_found(address))
    }
}

impl ChunkHas for Unverifying<'_> {
    async fn has(&self, address: &ChunkAddress) -> bool {
        ChunkHas::has(self.0, address).await
    }
}

#[test]
fn unverified_reads_are_certified() {
    run(async {
        let signer = signer();
        let feed = feed_for(&signer);
        let store = SocStore::new();
        let mut updater = Updater::new(feed, &store, &signer);
        updater.append(b"payload".to_vec()).await.unwrap();

        let getter = Getter::new(feed, Unverifying(&store));
        let update = getter.at(Sequence::ZERO).await.unwrap();
        assert_eq!(update.payload().as_ref(), b"payload");

        let latest = getter.latest().await.unwrap();
        assert_eq!(latest.update.unwrap().index(), &Sequence::ZERO);
    });
}

/// Store that serves one fixed slot's bytes under whatever address is asked
/// for: certification must reject the relabelled chunk.
struct Rebinding<'a> {
    inner: &'a SocStore,
    from: ChunkAddress,
}

impl ChunkGet<SingleOwnerOnlyChunkSet> for Rebinding<'_> {
    type Trust = Unverified;
    type Error = ChunkStoreError;

    async fn get(
        &self,
        address: &ChunkAddress,
    ) -> Result<Chunk<Unverified, SingleOwnerOnlyChunkSet>, Self::Error> {
        let chunk = ChunkGet::get(self.inner, &self.from).await?;
        Chunk::parse(*address, &chunk.typed_bytes())
            .map_err(|_| ChunkStoreError::not_found(address))
    }
}

#[test]
fn relabelled_chunk_fails_certification() {
    run(async {
        let signer = signer();
        let feed = feed_for(&signer);
        let store = SocStore::new();
        let mut updater = Updater::new(feed, &store, &signer);
        updater.append(b"payload".to_vec()).await.unwrap();

        let getter = Getter::new(
            feed,
            Rebinding {
                inner: &store,
                from: feed.update_address(&Sequence::ZERO),
            },
        );
        assert!(matches!(
            getter.at(Sequence::new(1)).await.unwrap_err(),
            FeedError::Chunk(_)
        ));
    });
}

/// Trusted general store lying about type: a content chunk vouched for at
/// the feed slot must still be rejected on the narrowing seam.
struct LyingTrusted {
    bytes: Vec<u8>,
    source: TrustedSource,
}

impl ChunkGet<StandardChunkSet> for LyingTrusted {
    type Trust = nectar_primitives::Verified;
    type Error = ChunkStoreError;

    async fn get(
        &self,
        address: &ChunkAddress,
    ) -> Result<Chunk<nectar_primitives::Verified, StandardChunkSet>, Self::Error> {
        let parsed = Chunk::<Unverified, StandardChunkSet>::parse(*address, &self.bytes)
            .map_err(|_| ChunkStoreError::not_found(address))?;
        Ok(parsed.assume_verified(&self.source))
    }
}

#[test]
fn content_chunk_at_a_feed_slot_is_a_typed_store_error() {
    run(async {
        let signer = signer();
        let feed = feed_for(&signer);
        let content = DefaultContentChunk::new(&b"not a feed update"[..]).unwrap();
        let store = LyingTrusted {
            bytes: StandardChunkSet::encode_typed(&content.into()),
            // Test-only vouching; the store deliberately lies about type.
            source: unsafe { TrustedSource::grant() },
        };

        let getter = Getter::new(feed, SingleOwnerGet::new(store));
        assert!(matches!(
            getter.at(Sequence::ZERO).await.unwrap_err(),
            FeedError::Store(_)
        ));
    });
}

#[test]
fn windowed_finders_agree_with_sequential() {
    run(async {
        let signer = signer();
        let feed = feed_for(&signer);
        let store = SocStore::new();
        let mut updater = Updater::new(feed, &store, &signer);
        for n in 0u64..21 {
            updater.append(n.to_be_bytes().to_vec()).await.unwrap();
        }

        for width in [1usize, 2, 7, 15] {
            let getter =
                Getter::new(feed, &store).with_window(core::num::NonZeroUsize::new(width).unwrap());
            for latest in [
                getter.latest().await.unwrap(),
                getter.latest_from(Sequence::new(3)).await.unwrap(),
                getter.latest_linear_from(Sequence::ZERO).await.unwrap(),
            ] {
                assert_eq!(latest.update.unwrap().index(), &Sequence::new(20));
                assert_eq!(latest.next, Some(Sequence::new(21)));
            }

            let empty = getter.latest_from(Sequence::new(21)).await.unwrap();
            assert!(empty.update.is_none());
            assert_eq!(empty.next, Some(Sequence::new(21)));
        }
    });
}

#[test]
fn shared_general_store_adapts_through_the_narrowing_get() {
    run(async {
        let signer = signer();
        let feed = feed_for(&signer);
        let shared = DefaultMemoryStore::new();
        let soc = SingleOwnerChunk::new(
            feed.update_id(&Sequence::ZERO),
            b"payload".to_vec(),
            &signer,
        )
        .unwrap();
        ChunkPut::put(&shared, Chunk::from_envelope(soc.into()).unwrap())
            .await
            .unwrap();

        let getter = Getter::new(feed, SingleOwnerGet::new(&shared));
        let update = getter.at(Sequence::ZERO).await.unwrap();
        assert_eq!(update.payload().as_ref(), b"payload");

        let latest = getter.latest().await.unwrap();
        assert_eq!(latest.update.unwrap().index(), &Sequence::ZERO);
        assert_eq!(latest.next, Sequence::ZERO.next());
    });
}

/// Probe-completion gate: a `has` probe parks by address until the test
/// grants it, so completion order is the caller's, not the store's. Records
/// the high-water mark of concurrently parked probes.
#[derive(Clone, Default)]
struct Gate {
    inner: Arc<Mutex<GateInner>>,
}

#[derive(Default)]
struct GateInner {
    parked: Vec<(ChunkAddress, Waker)>,
    granted: BTreeSet<ChunkAddress>,
    peak: usize,
}

impl Gate {
    /// Addresses currently parked, in arrival order.
    fn parked(&self) -> Vec<ChunkAddress> {
        self.inner
            .lock()
            .unwrap()
            .parked
            .iter()
            .map(|(address, _)| *address)
            .collect()
    }

    /// Grant one parked probe and wake it.
    fn grant(&self, address: ChunkAddress) {
        let waker = {
            let mut inner = self.inner.lock().unwrap();
            inner.granted.insert(address);
            inner
                .parked
                .iter()
                .position(|(parked, _)| *parked == address)
                .map(|pos| inner.parked.remove(pos).1)
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// High-water mark of concurrently parked probes.
    fn peak(&self) -> usize {
        self.inner.lock().unwrap().peak
    }
}

/// A `has` probe parked on the gate; resolves once its address is granted.
struct HasProbe {
    address: ChunkAddress,
    present: bool,
    gate: Gate,
}

impl Future for HasProbe {
    type Output = bool;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<bool> {
        let this = self.get_mut();
        let mut inner = this.gate.inner.lock().unwrap();
        if inner.granted.remove(&this.address) {
            return Poll::Ready(this.present);
        }
        match inner.parked.iter_mut().find(|(a, _)| *a == this.address) {
            Some(slot) => slot.1 = cx.waker().clone(),
            None => {
                inner.parked.push((this.address, cx.waker().clone()));
                let depth = inner.parked.len();
                inner.peak = inner.peak.max(depth);
            }
        }
        Poll::Pending
    }
}

/// Store double over a single-owner store: `get` resolves at once; `has`
/// parks on the gate, so the caller chooses the order presence answers land.
struct GatedStore<'a> {
    inner: &'a SocStore,
    gate: Gate,
}

impl ChunkGet<SingleOwnerOnlyChunkSet> for GatedStore<'_> {
    type Trust = nectar_primitives::Verified;
    type Error = ChunkStoreError;

    async fn get(
        &self,
        address: &ChunkAddress,
    ) -> Result<Chunk<nectar_primitives::Verified, SingleOwnerOnlyChunkSet>, Self::Error> {
        ChunkGet::get(self.inner, address).await
    }
}

impl ChunkHas for GatedStore<'_> {
    async fn has(&self, address: &ChunkAddress) -> bool {
        HasProbe {
            address: *address,
            present: self.inner.get(address).is_some(),
            gate: self.gate.clone(),
        }
        .await
    }
}

/// Chooses the next probe to grant from the parked set, given the turn.
type Pick = fn(usize, &[ChunkAddress]) -> ChunkAddress;

/// Drive the real finder to a verdict, granting one parked probe per pending
/// turn in the order `pick` chooses. Returns the found index, the next slot
/// and the peak concurrency the run reached.
fn drive_order(
    feed: Feed,
    inner: &SocStore,
    floor: Sequence,
    linear: bool,
    window: NonZeroUsize,
    mut pick: impl FnMut(usize, &[ChunkAddress]) -> ChunkAddress,
) -> (Option<Sequence>, Option<Sequence>, usize) {
    let gate = Gate::default();
    let getter = Getter::new(
        feed,
        GatedStore {
            inner,
            gate: gate.clone(),
        },
    )
    .with_window(window);
    let mut driver = Drive::new(async {
        if linear {
            getter.latest_linear_from(floor).await
        } else {
            getter.latest_from(floor).await
        }
    });

    let mut step = 0usize;
    let mut budget = 100_000usize;
    let latest = loop {
        match driver.poll() {
            Poll::Ready(result) => break result.unwrap(),
            Poll::Pending => {
                let parked = gate.parked();
                assert!(!parked.is_empty(), "finder parked with no probe in flight");
                gate.grant(pick(step, &parked));
                step += 1;
            }
        }
        budget -= 1;
        assert!(budget > 0, "finder did not converge");
    };
    (latest.update.map(|u| *u.index()), latest.next, gate.peak())
}

/// The shipped async finder, under adversarial out-of-order probe completion,
/// reaches the in-order verdict.
///
/// The property test in `probe.rs` proves this of a model; this pins the real
/// code, where a divergence surfacing only under true out-of-order landing
/// would otherwise go untested.
#[test]
fn probe_policy_holds_under_adversarial_completion_order() {
    let signer = signer();
    let feed = feed_for(&signer);
    let store = SocStore::new();
    run(async {
        let mut updater = Updater::new(feed, &store, &signer);
        for n in 0u64..21 {
            updater.append(n.to_be_bytes().to_vec()).await.unwrap();
        }
    });

    let floor = Sequence::ZERO;
    let one = NonZeroUsize::MIN;
    let wide = NonZeroUsize::new(8).unwrap();

    // Width one is the sequential scan: the in-order baseline.
    let (base_index, base_next, _) = drive_order(feed, &store, floor, false, one, |_, p| p[0]);
    assert_eq!(base_index, Some(Sequence::new(20)));
    assert_eq!(base_next, Some(Sequence::new(21)));

    // Deterministic adversarial orders, no clock or rng: newest-first, a
    // fixed rotation, and the mid probe first.
    let orders: [(&str, Pick); 3] = [
        ("reverse", |_, parked| *parked.last().unwrap()),
        ("rotate", |step, parked| {
            parked[(step * 3 + 1) % parked.len()]
        }),
        ("middle", |_, parked| parked[parked.len() / 2]),
    ];

    for linear in [false, true] {
        for (name, pick) in orders {
            let (index, next, peak) = drive_order(feed, &store, floor, linear, wide, pick);
            assert_eq!(index, base_index, "order {name}, linear {linear}");
            assert_eq!(next, base_next, "order {name}, linear {linear}");
            assert!(
                peak >= 2,
                "order {name}, linear {linear}: no concurrent probes, order was not exercised"
            );
        }
    }
}

/// Admission fills the window and stops there: the first round parks the head
/// slot plus its speculation, never one probe more and never one fewer.
///
/// Admitting past the window exceeds the store capacity the window buys;
/// admitting short of it silently serializes the finder.
#[test]
fn probes_fill_exactly_the_window() {
    let signer = signer();
    let feed = feed_for(&signer);
    let store = SocStore::new();
    run(async {
        let mut updater = Updater::new(feed, &store, &signer);
        for n in 0u64..21 {
            updater.append(n.to_be_bytes().to_vec()).await.unwrap();
        }
    });

    for width in [1usize, 2, 7, 15] {
        for linear in [false, true] {
            let (index, next, peak) = drive_order(
                feed,
                &store,
                Sequence::ZERO,
                linear,
                NonZeroUsize::new(width).unwrap(),
                |_, parked| parked[0],
            );
            assert_eq!(index, Some(Sequence::new(20)), "width {width}");
            assert_eq!(next, Some(Sequence::new(21)), "width {width}");
            assert_eq!(peak, width, "width {width}, linear {linear}");
        }
    }
}

/// Store answering presence from a real feed but failing every fetch: the
/// search converges on probes alone.
struct ProbeOnly<'a>(&'a SocStore);

impl ChunkGet<SingleOwnerOnlyChunkSet> for ProbeOnly<'_> {
    type Trust = nectar_primitives::Verified;
    type Error = ChunkStoreError;

    async fn get(
        &self,
        address: &ChunkAddress,
    ) -> Result<Chunk<nectar_primitives::Verified, SingleOwnerOnlyChunkSet>, Self::Error> {
        Err(ChunkStoreError::not_found(address))
    }
}

impl ChunkHas for ProbeOnly<'_> {
    async fn has(&self, address: &ChunkAddress) -> bool {
        ChunkHas::has(self.0, address).await
    }
}

/// The fetch of the committed update is the only fallible step of the search,
/// so its error surfaces at the commit and nowhere earlier: an absent floor
/// still returns an empty result.
#[test]
fn fetch_failure_surfaces_only_at_the_commit() {
    run(async {
        let signer = signer();
        let feed = feed_for(&signer);
        let store = SocStore::new();
        let mut updater = Updater::new(feed, &store, &signer);
        for n in 0u64..5 {
            updater.append(n.to_be_bytes().to_vec()).await.unwrap();
        }

        let getter =
            Getter::new(feed, ProbeOnly(&store)).with_window(NonZeroUsize::new(4).unwrap());
        assert!(matches!(
            getter.latest().await.unwrap_err(),
            FeedError::Store(_)
        ));
        assert!(matches!(
            getter.latest_linear_from(Sequence::ZERO).await.unwrap_err(),
            FeedError::Store(_)
        ));

        let empty = getter.latest_from(Sequence::new(5)).await.unwrap();
        assert!(empty.update.is_none());
        assert_eq!(empty.next, Some(Sequence::new(5)));
    });
}

proptest! {
    /// Round trip over generator-drawn feeds: whatever identity and payload,
    /// an appended update reads back byte-identical.
    #[test]
    fn arbitrary_feed_round_trips(
        seed in proptest::collection::vec(any::<u8>(), 64..256),
        payload in proptest::collection::vec(any::<u8>(), 1..64),
    ) {
        let mut u = arbitrary::Unstructured::new(&seed);
        let Ok((feed, signer)) =
            nectar_feeds::generators::feed_with_signer::<DEFAULT_BODY_SIZE>(&mut u)
        else {
            return Ok(());
        };

        run(async {
            let store = SocStore::new();
            let mut updater = Updater::new(feed, &store, &signer);
            let written = updater.append(payload.clone()).await.unwrap();

            let getter = Getter::new(feed, &store);
            let read = getter.at(Sequence::ZERO).await.unwrap();
            prop_assert_eq!(read.payload().as_ref(), payload.as_slice());
            prop_assert_eq!(read.address(), written.address());
            prop_assert_eq!(read.chunk(), written.chunk());

            let latest = getter.latest().await.unwrap();
            prop_assert_eq!(latest.next, Sequence::ZERO.next());
            Ok(())
        })?;
    }
}
