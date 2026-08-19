//! The permit seam: a claimed slot, its admission-window token, and the seal.

use alloy_primitives::Signature;
use core::fmt;

use nectar_governor::Window;
use nectar_postage::{
    BatchDepth, BatchId, Bucket, Stamp, StampDigest, StampError, StampIndex, StampedChunk,
    Validated,
};
use nectar_primitives::{AnyChunkSet, Chunk, ChunkAddress, Mainnet, SwarmSpec, Verified};

use crate::pipeline::BoundSigner;

#[cfg(multi_thread)]
type Shared<T> = alloc::sync::Arc<T>;
#[cfg(not(multi_thread))]
type Shared<T> = alloc::rc::Rc<T>;

/// Atomics where threads exist, plain cells where they do not.
#[cfg(multi_thread)]
mod word {
    use core::sync::atomic::{AtomicUsize, Ordering};

    // Relaxed: every access is a read-modify-write of one location and
    // publishes no other data.
    const ORDER: Ordering = Ordering::Relaxed;

    #[derive(Debug, Default)]
    pub(super) struct Occupancy(AtomicUsize);

    impl Occupancy {
        pub(super) fn get(&self) -> usize {
            self.0.load(ORDER)
        }

        pub(super) fn take(&self, limit: usize) -> bool {
            let mut held = self.0.load(ORDER);
            loop {
                if held >= limit {
                    return false;
                }
                match self
                    .0
                    .compare_exchange_weak(held, held.saturating_add(1), ORDER, ORDER)
                {
                    Ok(_) => return true,
                    Err(observed) => held = observed,
                }
            }
        }

        pub(super) fn give(&self) {
            self.0.fetch_sub(1, ORDER);
        }
    }
}

/// Atomics where threads exist, plain cells where they do not.
#[cfg(not(multi_thread))]
mod word {
    use core::cell::Cell;

    #[derive(Debug, Default)]
    pub(super) struct Occupancy(Cell<usize>);

    impl Occupancy {
        pub(super) fn get(&self) -> usize {
            self.0.get()
        }

        pub(super) fn take(&self, limit: usize) -> bool {
            let held = self.0.get();
            if held >= limit {
                return false;
            }
            self.0.set(held.saturating_add(1));
            true
        }

        pub(super) fn give(&self) {
            self.0.set(self.0.get().saturating_sub(1));
        }
    }
}

use word::Occupancy;

#[derive(Debug)]
struct Slots {
    window: Window,
    held: Occupancy,
}

/// Clone-shared admission window: one token holds one slot.
#[derive(Debug, Clone)]
pub struct AdmissionWindow(Shared<Slots>);

impl AdmissionWindow {
    /// Opens a window of `window` slots.
    pub fn new(window: Window) -> Self {
        Self(Shared::new(Slots {
            window,
            held: Occupancy::default(),
        }))
    }

    /// The window slots were sized against.
    pub fn window(&self) -> Window {
        self.0.window
    }

    /// Tokens currently outstanding.
    pub fn in_flight(&self) -> usize {
        self.0.held.get()
    }

    /// Slots free to admit against.
    pub fn room(&self) -> usize {
        usize::from(self.0.window.get()).saturating_sub(self.in_flight())
    }

    /// Takes one slot, or `None` when the window is full.
    pub fn try_acquire(&self) -> Option<WindowToken> {
        self.0
            .held
            .take(usize::from(self.0.window.get()))
            .then(|| WindowToken(Shared::clone(&self.0)))
    }
}

/// One occupied admission slot.
#[derive(Debug)]
pub struct WindowToken(Shared<Slots>);

impl Drop for WindowToken {
    fn drop(&mut self) {
        self.0.held.give();
    }
}

/// A claimed slot: constructing this consumed it.
///
/// Dropping returns the window token but never the slot: the per-bucket
/// watermark is monotone and cannot express a hole.
pub struct Prepared<S: SwarmSpec = Mainnet> {
    address: ChunkAddress,
    batch: BatchId,
    bucket: Bucket<S>,
    depth: BatchDepth<S>,
    slot: u32,
    timestamp: u64,
    token: Option<WindowToken>,
}

impl<S: SwarmSpec> fmt::Debug for Prepared<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Prepared")
            .field("address", &self.address)
            .field("batch", &self.batch)
            .field("bucket", &self.bucket.value())
            .field("depth", &self.depth.get())
            .field("slot", &self.slot)
            .field("timestamp", &self.timestamp)
            .field("admitted", &self.token.is_some())
            .finish()
    }
}

impl<S: SwarmSpec> Prepared<S> {
    /// Mints the permit for a slot the issuer has already claimed. Minting
    /// one without claiming the slot double-spends it.
    pub const fn new(
        address: ChunkAddress,
        batch: BatchId,
        bucket: Bucket<S>,
        depth: BatchDepth<S>,
        slot: u32,
        timestamp: u64,
    ) -> Self {
        Self {
            address,
            batch,
            bucket,
            depth,
            slot,
            timestamp,
            token: None,
        }
    }

    /// Attaches the admission-window token the permit was admitted under.
    #[must_use]
    pub fn with_token(mut self, token: WindowToken) -> Self {
        self.token = Some(token);
        self
    }

    /// Detaches the token so a later stage holds the slot open past the seal.
    pub const fn take_token(&mut self) -> Option<WindowToken> {
        self.token.take()
    }

    /// Whether the permit still holds an admission-window token.
    pub const fn is_admitted(&self) -> bool {
        self.token.is_some()
    }

    /// The address the slot was allocated for.
    pub const fn address(&self) -> &ChunkAddress {
        &self.address
    }

    /// The batch the slot belongs to.
    pub const fn batch(&self) -> BatchId {
        self.batch
    }

    /// The collision bucket the slot sits in.
    pub const fn bucket(&self) -> Bucket<S> {
        self.bucket
    }

    /// The batch depth the slot was allocated under. A later dilution only
    /// widens it, so the slot stays in range.
    pub const fn depth(&self) -> BatchDepth<S> {
        self.depth
    }

    /// The wire index of the slot.
    pub const fn index(&self) -> StampIndex {
        StampIndex::new(self.bucket.value(), self.slot)
    }

    /// The stamp timestamp the slot was allocated with.
    pub const fn timestamp(&self) -> u64 {
        self.timestamp
    }

    /// The digest to sign.
    pub const fn digest(&self) -> StampDigest {
        StampDigest::new(self.address, self.batch, self.index(), self.timestamp)
    }

    /// Mints the stamp from a signature over [`digest`](Self::digest).
    pub fn stamp(self, signature: Signature) -> Stamp {
        Stamp::with_index(self.batch, self.index(), self.timestamp, signature)
    }

    /// Mints the sealed pair, validated by construction: `signer` is bound to
    /// the batch owner, so nothing here recovers a signature.
    ///
    /// # Errors
    ///
    /// [`StampError::AddressMismatch`] when `chunk` is not the one the slot
    /// was allocated for, or whatever [`StampedChunk::issued_by`] refuses the
    /// pairing with; the slot burns with the returned permit.
    pub fn seal<Sg, const BODY_SIZE: usize>(
        self,
        chunk: Chunk<Verified, AnyChunkSet<BODY_SIZE>>,
        signature: Signature,
        signer: &Sg,
    ) -> Result<StampedChunk<Verified, Validated, BODY_SIZE>, StampError>
    where
        Sg: BoundSigner<Spec = S>,
    {
        if chunk.address() != &self.address {
            return Err(StampError::AddressMismatch {
                expected: self.address,
                offered: *chunk.address(),
            });
        }
        let (batch, owner) = (signer.batch(), signer.address());
        StampedChunk::new(chunk, self.stamp(signature)).issued_by(batch, owner)
    }
}

#[cfg(all(test, feature = "std"))]
mod tests {
    use super::*;
    use crate::pipeline::{BatchSigner, Eip191, SignPrehash};
    use crate::testing::{batch_owned_by, key};
    use alloy_primitives::U256;
    use alloy_signer_local::PrivateKeySigner;
    use nectar_postage::BucketDepth;
    use nectar_primitives::{ContentChunk, DEFAULT_BODY_SIZE};

    const fn window(slots: u16) -> Window {
        Window::new(slots).unwrap()
    }

    fn bound() -> BatchSigner<Eip191<PrivateKeySigner>> {
        let signer = key(1);
        let batch = batch_owned_by(signer.address(), BatchId::ZERO);
        BatchSigner::from_signer(signer, batch).unwrap()
    }

    fn signature() -> Signature {
        Signature::new(U256::from(1), U256::from(2), false)
    }

    fn chunk(payload: &'static [u8]) -> Chunk<Verified> {
        let content: ContentChunk<DEFAULT_BODY_SIZE> = ContentChunk::new(payload).unwrap();
        Chunk::from_envelope(content.into()).unwrap()
    }

    fn permit(address: ChunkAddress) -> Prepared {
        let bucket_depth: BucketDepth = BucketDepth::new(16).unwrap();
        Prepared::new(
            address,
            BatchId::ZERO,
            Bucket::of(&address, bucket_depth),
            BatchDepth::new(20, bucket_depth).unwrap(),
            7,
            42,
        )
    }

    #[test]
    fn a_token_holds_a_slot_until_it_drops() {
        let admission = AdmissionWindow::new(window(2));
        assert_eq!(admission.room(), 2);

        let first = admission.try_acquire().unwrap();
        let second = admission.try_acquire().unwrap();
        assert_eq!(admission.in_flight(), 2);
        assert!(admission.try_acquire().is_none());

        drop(first);
        assert_eq!(admission.room(), 1);
        assert!(admission.try_acquire().is_some());
        drop(second);
        assert_eq!(admission.in_flight(), 0);
    }

    #[test]
    fn dropping_a_permit_returns_its_token() {
        let admission = AdmissionWindow::new(window(1));
        let held =
            permit(ChunkAddress::new([0xAB; 32])).with_token(admission.try_acquire().unwrap());
        assert!(held.is_admitted());
        assert!(admission.try_acquire().is_none());

        drop(held);
        assert_eq!(admission.in_flight(), 0);
        assert!(admission.try_acquire().is_some());
    }

    #[test]
    fn a_detached_token_outlives_the_permit() {
        let admission = AdmissionWindow::new(window(1));
        let mut held =
            permit(ChunkAddress::new([0xCD; 32])).with_token(admission.try_acquire().unwrap());
        let token = held.take_token().unwrap();
        assert!(!held.is_admitted());

        drop(held);
        assert_eq!(admission.in_flight(), 1);
        drop(token);
        assert_eq!(admission.in_flight(), 0);
    }

    #[test]
    fn the_digest_carries_the_slot_the_permit_claimed() {
        let address = ChunkAddress::new([0xAB; 32]);
        let digest = permit(address).digest();
        assert_eq!(digest.chunk_address, address);
        assert_eq!(digest.index.index(), 7);
        assert_eq!(digest.timestamp, 42);
    }

    #[test]
    fn seal_pairs_the_chunk_the_slot_was_allocated_for() {
        let chunk = chunk(b"hello swarm");
        let address = *chunk.address();

        let sealed: StampedChunk<Verified, Validated> =
            permit(address).seal(chunk, signature(), &bound()).unwrap();

        assert_eq!(sealed.address(), &address);
        assert_eq!(sealed.stamp().index(), 7);
        assert_eq!(sealed.stamp().timestamp(), 42);
    }

    /// The pair a real signer seals is validated by construction, and the
    /// recovery route agrees with it.
    #[test]
    fn a_sealed_pair_also_validates_the_long_way() {
        let signer = key(1);
        let batch = batch_owned_by(signer.address(), BatchId::ZERO);
        let bound = BatchSigner::from_signer(signer, batch.clone()).unwrap();
        let chunk = chunk(b"hello swarm");
        let address = *chunk.address();
        let permit = permit(address);
        let signature = bound.sign_prehash(&permit.digest().to_prehash()).unwrap();

        let sealed = permit.seal(chunk.clone(), signature, &bound).unwrap();

        let recovered = StampedChunk::new(chunk, sealed.stamp().clone())
            .validate(&batch)
            .unwrap();
        assert_eq!(sealed, recovered);
    }

    #[test]
    fn seal_refuses_a_chunk_the_slot_was_not_allocated_for() {
        let allocated = chunk(b"hello swarm");
        let other = chunk(b"another chunk");
        let expected = *allocated.address();
        let offered = *other.address();

        // The token returns even though the seal failed: dropping the permit
        // burns the slot and recovers the backpressure.
        let admission = AdmissionWindow::new(window(1));
        let held = permit(expected).with_token(admission.try_acquire().unwrap());

        assert_eq!(
            held.seal::<_, DEFAULT_BODY_SIZE>(other, signature(), &bound())
                .unwrap_err(),
            StampError::AddressMismatch { expected, offered }
        );
        assert_eq!(admission.in_flight(), 0);
    }

    #[test]
    fn seal_refuses_a_permit_from_another_batch() {
        let chunk = chunk(b"hello swarm");
        let address = *chunk.address();
        let elsewhere = batch_owned_by(key(1).address(), BatchId::new([0xEE; 32]));
        let bound = BatchSigner::from_signer(key(1), elsewhere).unwrap();

        assert!(matches!(
            permit(address).seal::<_, DEFAULT_BODY_SIZE>(chunk, signature(), &bound),
            Err(StampError::BatchMismatch { .. })
        ));
    }
}
