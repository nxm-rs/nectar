//! Node persistence seam: the read and write verbs every manifest format
//! persists its nodes through.
//!
//! The unit of transfer `N` is a parameter, not a decision: a format whose
//! node is one stored image instantiates at that image, and a format whose
//! stored shape spans chunks instantiates at its decoded node, because no
//! single image exists for it.

use alloc::vec::Vec;
use core::future::Future;

use nectar_marker::{MaybeSend, MaybeSync};
use nectar_primitives::EntryRef;
use nectar_primitives::chunk::{ChunkAddress, Reference};
use nectar_primitives::store::{ChunkStoreError, NullLoader};

/// Read seam: the logical node of unit `N` behind a full-width reference.
///
/// The reference is the runtime union, not a width parameter: a trie may mix
/// widths across its own forks, and an encrypted reference carries its key in
/// band.
pub trait NodeLoader<N: MaybeSend>: MaybeSend + MaybeSync {
    /// Loader failure, wrapped by the format into its own errors.
    type Error: core::error::Error + MaybeSend + MaybeSync + 'static;

    /// The node `reference` reaches.
    fn load(
        &self,
        reference: &EntryRef,
    ) -> impl Future<Output = Result<N, Self::Error>> + MaybeSend;

    /// The node plus the chunk addresses its stored shape occupies, root
    /// first.
    fn load_traced(
        &self,
        reference: &EntryRef,
    ) -> impl Future<Output = Result<(N, Vec<ChunkAddress>), Self::Error>> + MaybeSend {
        async move {
            let node = self.load(reference).await?;
            Ok((node, alloc::vec![*reference.address()]))
        }
    }
}

/// Write seam: persist one logical node of unit `N` under a new reference of
/// width `R`.
///
/// The width is a trait parameter rather than an associated type, so one
/// saver may mint both plain and encrypted references.
pub trait NodeSaver<N: ?Sized + MaybeSync, R: Reference>: MaybeSend + MaybeSync {
    /// Saver failure, wrapped by the format into its own errors.
    type Error: core::error::Error + MaybeSend + MaybeSync + 'static;

    /// Persist `node` and return the full-width reference reaching it.
    fn save(&self, node: &N) -> impl Future<Output = Result<R, Self::Error>> + MaybeSend;
}

/// Purely in-memory manifests: every load is a typed not-found.
impl<N: MaybeSend> NodeLoader<N> for NullLoader {
    type Error = ChunkStoreError;

    async fn load(&self, reference: &EntryRef) -> Result<N, Self::Error> {
        Err(ChunkStoreError::not_found(reference.address()))
    }
}

#[cfg(test)]
mod tests {
    use alloc::vec;

    use nectar_primitives::chunk::ChunkRef;
    use nectar_testing::run;

    use super::*;

    #[test]
    fn null_loader_is_not_found() {
        let reference = EntryRef::from(ChunkRef::new(ChunkAddress::new([7; 32])));
        let error = run(NodeLoader::<Vec<u8>>::load(&NullLoader, &reference)).unwrap_err();
        assert!(matches!(error, ChunkStoreError::NotFound(a) if a == *reference.address()));
    }

    #[test]
    fn traced_defaults_to_the_node_and_its_root_address() {
        struct Fixed;

        impl NodeLoader<u8> for Fixed {
            type Error = ChunkStoreError;

            async fn load(&self, _: &EntryRef) -> Result<u8, Self::Error> {
                Ok(9)
            }
        }

        let reference = EntryRef::from(ChunkRef::new(ChunkAddress::new([3; 32])));
        let (node, addresses) = run(Fixed.load_traced(&reference)).unwrap();
        assert_eq!(node, 9);
        assert_eq!(addresses, vec![*reference.address()]);
    }
}
