//! Shared helpers for the manifest integration tests.

#![allow(dead_code)]

use std::error::Error;
use std::sync::Arc;

use nectar_file::{Plain, Split};
use nectar_primitives::{ChunkAddress, DEFAULT_BODY_SIZE, MemoryStore};

/// The result type the Result-returning integration tests report through.
pub(crate) type TestResult = Result<(), Box<dyn Error>>;

/// A fallible assertion: Result-returning tests report failures as errors.
pub(crate) fn ensure(cond: bool, what: &str) -> TestResult {
    if cond { Ok(()) } else { Err(what.into()) }
}

/// A fallible equality assertion.
pub(crate) fn ensure_eq<T: PartialEq + core::fmt::Debug>(
    left: T,
    right: T,
    what: &str,
) -> TestResult {
    if left == right {
        Ok(())
    } else {
        Err(format!("{what}: {left:?} != {right:?}").into())
    }
}

/// Split `data` whole through the streaming engine into a fresh store,
/// returning the root and the split store.
pub(crate) async fn split_whole(
    data: &[u8],
) -> Result<(ChunkAddress, MemoryStore), Box<dyn Error>> {
    let store = Arc::new(MemoryStore::default());
    let root =
        Split::<Arc<MemoryStore>, Plain, DEFAULT_BODY_SIZE>::collect(Arc::clone(&store), data)
            .await?;
    let store = Arc::into_inner(store).ok_or("split still holds the store")?;
    Ok((root, store))
}
