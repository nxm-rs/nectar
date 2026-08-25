//! Verifying boundary adapter over an untrusted read medium.

use crate::chunk::{Chunk, ChunkAddress, ChunkRegistry, Unverified, Verified};
use crate::error::{PrimitivesError, StoreError};

use super::typed::{ChunkGet, ChunkPut, PutUnit};

/// Lifts an untrusted medium to `Trust = Verified`: every get runs the
/// member's full acceptance rule against the requested address, so a
/// misrouted chunk fails exactly like a tampered one.
///
/// The inner `Trust = Unverified` bound makes wrapping an already trusted
/// store a compile error, so the acceptance rule never runs twice.
#[derive(Debug, Clone, Copy, Default)]
pub struct VerifyingStore<S>(S);

impl<S> VerifyingStore<S> {
    /// Wrap an untrusted store.
    pub const fn new(inner: S) -> Self {
        Self(inner)
    }

    /// Borrow the inner store.
    pub const fn inner(&self) -> &S {
        &self.0
    }

    /// Consume into the inner store.
    pub fn into_inner(self) -> S {
        self.0
    }
}

/// Failure of a verifying get.
#[derive(Debug, thiserror::Error)]
pub enum VerifyError<E> {
    /// The inner store failed.
    #[error(transparent)]
    Store(E),
    /// The store answered with a chunk claiming a different address.
    #[error("store returned {returned} for requested {requested}")]
    AddressMismatch {
        /// Address the get asked for.
        requested: ChunkAddress,
        /// Address the returned chunk claims.
        returned: ChunkAddress,
    },
    /// The returned bytes fail the acceptance rule at the requested address.
    #[error(transparent)]
    Chunk(PrimitivesError),
}

impl<R: ChunkRegistry, S: ChunkGet<R, Trust = Unverified>> ChunkGet<R> for VerifyingStore<S> {
    type Trust = Verified;
    type Error = VerifyError<S::Error>;

    async fn get(&self, address: &ChunkAddress) -> Result<Chunk<Verified, R>, Self::Error> {
        let claimed = self.0.get(address).await.map_err(VerifyError::Store)?;
        let returned = *claimed.claimed_address();
        if returned != *address {
            return Err(VerifyError::AddressMismatch {
                requested: *address,
                returned,
            });
        }
        claimed.verify().map_err(VerifyError::Chunk)
    }
}

// Writes carry sealed chunks already, so they pass through untouched.
impl<U: PutUnit, S: ChunkPut<U>> ChunkPut<U> for VerifyingStore<S> {
    type Error = S::Error;

    async fn put(&self, unit: U) -> Result<(), Self::Error> {
        self.0.put(unit).await
    }
}

impl<E: StoreError> StoreError for VerifyError<E> {
    /// A miss passes through the lift unchanged; a mismatched claim or a
    /// failed acceptance rule is a terminal failure, never an absence.
    fn is_definitely_absent(&self) -> bool {
        matches!(
            self,
            Self::Store(error) if error.is_definitely_absent()
        )
    }

    fn is_transient(&self) -> bool {
        matches!(self, Self::Store(error) if error.is_transient())
    }
}

#[cfg(test)]
mod tests {
    use alloc::boxed::Box;
    use alloc::collections::BTreeMap;
    use alloc::vec::Vec;

    use nectar_testing::run;

    use super::super::{ChunkStoreError, TrustedGet};
    use super::*;
    use crate::chunk::{ChunkOps, ContentChunk, StandardChunkSet};

    /// Untrusted byte store: answers a request with a claimed address and
    /// typed bytes it is free to lie about.
    struct RawStore {
        entries: BTreeMap<ChunkAddress, (ChunkAddress, Vec<u8>)>,
    }

    impl ChunkGet<StandardChunkSet> for RawStore {
        type Trust = Unverified;
        type Error = ChunkStoreError;

        async fn get(
            &self,
            address: &ChunkAddress,
        ) -> Result<Chunk<Unverified, StandardChunkSet>, Self::Error> {
            let (claimed, bytes) = self
                .entries
                .get(address)
                .ok_or_else(|| ChunkStoreError::not_found(address))?;
            Chunk::parse(*claimed, bytes).map_err(|e| ChunkStoreError::Other(Box::new(e)))
        }
    }

    fn sealed(payload: &'static [u8]) -> (ChunkAddress, Vec<u8>) {
        let content = ContentChunk::new(payload).unwrap();
        let address = *content.address();
        (address, StandardChunkSet::encode_typed(&content.into()))
    }

    fn assert_trusted<S: TrustedGet>(_: &S) {}

    #[test]
    fn honest_answer_certifies_at_the_requested_address() {
        let (address, typed) = sealed(b"honest bytes");
        let store = VerifyingStore::new(RawStore {
            entries: BTreeMap::from([(address, (address, typed))]),
        });
        // The lift satisfies the trusted bound.
        assert_trusted(&store);

        let chunk = run(store.get(&address)).unwrap();
        assert_eq!(chunk.address(), &address);
    }

    #[test]
    fn misrouted_claim_is_an_address_mismatch() {
        let (address, _) = sealed(b"asked for");
        let (other, other_typed) = sealed(b"answered with");
        // Internally consistent chunk, wrong slot.
        let store = VerifyingStore::new(RawStore {
            entries: BTreeMap::from([(address, (other, other_typed))]),
        });

        assert!(matches!(
            run(store.get(&address)),
            Err(VerifyError::AddressMismatch { requested, returned })
                if requested == address && returned == other
        ));
    }

    #[test]
    fn misrouted_bytes_fail_the_acceptance_rule() {
        let (address, _) = sealed(b"asked for");
        let (_, other_typed) = sealed(b"answered with");
        // The claim matches the request, the bytes do not.
        let store = VerifyingStore::new(RawStore {
            entries: BTreeMap::from([(address, (address, other_typed))]),
        });

        assert!(matches!(
            run(store.get(&address)),
            Err(VerifyError::Chunk(_))
        ));
    }

    #[test]
    fn inner_store_errors_pass_through() {
        let store = VerifyingStore::new(RawStore {
            entries: BTreeMap::new(),
        });

        assert!(matches!(
            run(store.get(&ChunkAddress::default())),
            Err(VerifyError::Store(ChunkStoreError::NotFound(_)))
        ));
    }

    /// New variants must be classified, so the match stays exhaustive; the
    /// predicates stay mutually exclusive per variant.
    #[test]
    fn every_variant_is_classified_into_exactly_one_group() {
        let miss = VerifyError::Store(ChunkStoreError::not_found(&ChunkAddress::default()));
        let mismatched = VerifyError::AddressMismatch {
            requested: ChunkAddress::default(),
            returned: ChunkAddress::default(),
        };
        let failed = VerifyError::Chunk(PrimitivesError::Store(ChunkStoreError::not_found(
            &ChunkAddress::default(),
        )));
        for error in [miss, mismatched, failed] {
            let absent = StoreError::is_definitely_absent(&error);
            let transient = StoreError::is_transient(&error);
            assert!(!(absent && transient), "the groups stay exclusive");
            match &error {
                VerifyError::Store(inner) => {
                    assert_eq!(
                        (absent, transient),
                        (inner.is_definitely_absent(), inner.is_transient()),
                        "the lift passes the inner classification through"
                    );
                }
                VerifyError::AddressMismatch { .. } | VerifyError::Chunk(..) => {
                    assert!(!absent, "a verification failure is not an absence answer");
                    assert!(!transient, "a verification failure is not retryable");
                }
            }
        }
    }
}
