//! Storage traits and mock implementation for mantaray persistence.

extern crate alloc;

use alloc::vec::Vec;
use alloy_primitives::map::HashMap;

use crate::{Result, keccak256};

/// Load node data by reference.
pub trait MantarayLoader {
    /// Load serialised node data for the given reference.
    fn load(&self, reference: &[u8]) -> Result<Vec<u8>>;
}

/// Save node data, returning the content-addressed reference.
pub trait MantaraySaver {
    /// Save serialised node data and return its reference.
    fn save(&self, data: &[u8]) -> Result<Vec<u8>>;
}

/// Combined loader and saver.
pub trait MantarayStore: MantarayLoader + MantaraySaver {}
impl<T: MantarayLoader + MantaraySaver> MantarayStore for T {}

/// In-memory store using keccak256 content addressing, for testing.
#[derive(Debug, Default)]
pub struct MockStore {
    store: HashMap<[u8; 32], Vec<u8>>,
}

impl MockStore {
    /// Create a new empty mock store.
    pub fn new() -> Self {
        Self::default()
    }
}

impl MantarayLoader for MockStore {
    fn load(&self, reference: &[u8]) -> Result<Vec<u8>> {
        let mut key = [0u8; 32];
        key.copy_from_slice(&reference[..32]);
        Ok(self.store[&key].clone())
    }
}

impl MantaraySaver for MockStore {
    fn save(&self, _data: &[u8]) -> Result<Vec<u8>> {
        // MockStore needs interior mutability for save; use RefCell-based version below.
        unimplemented!("use MockStoreCell for mutable saves")
    }
}

/// In-memory store with interior mutability for testing save/load cycles.
#[derive(Debug, Default)]
pub struct MockStoreCell {
    store: core::cell::RefCell<HashMap<[u8; 32], Vec<u8>>>,
}

impl MockStoreCell {
    /// Create a new empty mock store.
    pub fn new() -> Self {
        Self::default()
    }
}

impl MantarayLoader for MockStoreCell {
    fn load(&self, reference: &[u8]) -> Result<Vec<u8>> {
        let mut key = [0u8; 32];
        key.copy_from_slice(&reference[..32]);
        Ok(self.store.borrow()[&key].clone())
    }
}

impl MantaraySaver for MockStoreCell {
    fn save(&self, data: &[u8]) -> Result<Vec<u8>> {
        let ref_ = keccak256(data);
        self.store.borrow_mut().insert(ref_, data.to_vec());
        Ok(ref_.to_vec())
    }
}

impl<T: MantarayLoader> MantarayLoader for &T {
    fn load(&self, reference: &[u8]) -> Result<Vec<u8>> {
        (**self).load(reference)
    }
}

impl<T: MantaraySaver> MantaraySaver for &T {
    fn save(&self, data: &[u8]) -> Result<Vec<u8>> {
        (**self).save(data)
    }
}
