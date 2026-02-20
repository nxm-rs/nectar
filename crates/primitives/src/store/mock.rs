//! In-memory mock chunk store for testing.

use std::cell::RefCell;
use std::collections::HashMap;

use crate::chunk::ChunkAddress;

use super::raw::{ChunkGetter, ChunkPutter, ChunkStoreError};

/// In-memory chunk store for testing.
#[derive(Debug, Default)]
pub struct MockChunkStore {
    store: RefCell<HashMap<ChunkAddress, Vec<u8>>>,
}

impl MockChunkStore {
    /// Create a new empty mock store.
    pub fn new() -> Self {
        Self::default()
    }
}

impl ChunkGetter for MockChunkStore {
    fn get(&self, address: &ChunkAddress) -> Result<Vec<u8>, ChunkStoreError> {
        self.store
            .borrow()
            .get(address)
            .cloned()
            .ok_or_else(|| ChunkStoreError::NotFound {
                address_hex: format!("{address}"),
            })
    }
}

impl ChunkPutter for MockChunkStore {
    fn put(&self, address: &ChunkAddress, data: &[u8]) -> Result<(), ChunkStoreError> {
        self.store.borrow_mut().insert(*address, data.to_vec());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::FixedBytes;

    #[test]
    fn round_trip_put_get() {
        let store = MockChunkStore::new();
        let address = ChunkAddress::from(FixedBytes::random());
        let data = b"hello world";

        store.put(&address, data).unwrap();
        let retrieved = store.get(&address).unwrap();
        assert_eq!(retrieved, data);
    }

    #[test]
    fn not_found_error() {
        let store = MockChunkStore::new();
        let address = ChunkAddress::from(FixedBytes::random());

        let err = store.get(&address).unwrap_err();
        assert!(matches!(err, ChunkStoreError::NotFound { .. }));
    }

    #[test]
    fn blanket_impl_ref() {
        let store = MockChunkStore::new();
        let address = ChunkAddress::from(FixedBytes::random());
        let data = b"test data";

        // Use via &T
        let store_ref: &MockChunkStore = &store;
        store_ref.put(&address, data).unwrap();
        let retrieved = store_ref.get(&address).unwrap();
        assert_eq!(retrieved, data);
    }

    #[test]
    fn blanket_impl_box() {
        let store: Box<dyn super::super::raw::ChunkStore> = Box::new(MockChunkStore::new());
        let address = ChunkAddress::from(FixedBytes::random());
        let data = b"boxed data";

        store.put(&address, data).unwrap();
        let retrieved = store.get(&address).unwrap();
        assert_eq!(retrieved, data);
    }

    #[test]
    fn overwrite_existing() {
        let store = MockChunkStore::new();
        let address = ChunkAddress::from(FixedBytes::random());

        store.put(&address, b"first").unwrap();
        store.put(&address, b"second").unwrap();
        let retrieved = store.get(&address).unwrap();
        assert_eq!(retrieved, b"second");
    }
}
