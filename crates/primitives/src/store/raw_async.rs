//! Async raw-byte chunk storage traits.

use std::future::Future;
use std::sync::Arc;

use crate::chunk::ChunkAddress;

use super::raw::ChunkStoreError;

/// Async retrieve raw chunk data by address.
pub trait AsyncChunkGetter: Send + Sync {
    /// Load chunk data for the given address.
    fn get(
        &self,
        address: &ChunkAddress,
    ) -> impl Future<Output = Result<Vec<u8>, ChunkStoreError>> + Send;
}

/// Async store raw chunk data by address.
pub trait AsyncChunkPutter: Send + Sync {
    /// Store chunk data at the given address.
    fn put(
        &self,
        address: &ChunkAddress,
        data: &[u8],
    ) -> impl Future<Output = Result<(), ChunkStoreError>> + Send;
}

/// Combined async getter and putter.
pub trait AsyncChunkStore: AsyncChunkGetter + AsyncChunkPutter {}
impl<T: AsyncChunkGetter + AsyncChunkPutter> AsyncChunkStore for T {}

// Blanket impls for references

impl<T: AsyncChunkGetter> AsyncChunkGetter for &T {
    fn get(
        &self,
        address: &ChunkAddress,
    ) -> impl Future<Output = Result<Vec<u8>, ChunkStoreError>> + Send {
        (**self).get(address)
    }
}

impl<T: AsyncChunkPutter> AsyncChunkPutter for &T {
    fn put(
        &self,
        address: &ChunkAddress,
        data: &[u8],
    ) -> impl Future<Output = Result<(), ChunkStoreError>> + Send {
        (**self).put(address, data)
    }
}

// Blanket impls for Box

impl<T: AsyncChunkGetter + ?Sized> AsyncChunkGetter for Box<T> {
    fn get(
        &self,
        address: &ChunkAddress,
    ) -> impl Future<Output = Result<Vec<u8>, ChunkStoreError>> + Send {
        (**self).get(address)
    }
}

impl<T: AsyncChunkPutter + ?Sized> AsyncChunkPutter for Box<T> {
    fn put(
        &self,
        address: &ChunkAddress,
        data: &[u8],
    ) -> impl Future<Output = Result<(), ChunkStoreError>> + Send {
        (**self).put(address, data)
    }
}

// Blanket impls for Arc

impl<T: AsyncChunkGetter + ?Sized> AsyncChunkGetter for Arc<T> {
    fn get(
        &self,
        address: &ChunkAddress,
    ) -> impl Future<Output = Result<Vec<u8>, ChunkStoreError>> + Send {
        (**self).get(address)
    }
}

impl<T: AsyncChunkPutter + ?Sized> AsyncChunkPutter for Arc<T> {
    fn put(
        &self,
        address: &ChunkAddress,
        data: &[u8],
    ) -> impl Future<Output = Result<(), ChunkStoreError>> + Send {
        (**self).put(address, data)
    }
}
