//! Async io adapters over the poll-native handle surfaces.
//!
//! [`TokioReader`] shims [`AsyncRead`](::tokio::io::AsyncRead) and
//! [`AsyncSeek`](::tokio::io::AsyncSeek) straight over a
//! [`Reader`](crate::Reader): every poll drains the walk in place, so the
//! fetch window stays in flight across polls and no future is created per
//! call. Positions are zero-based within the clipped range, so
//! [`SeekFrom::End`] resolves against the effective length. The write
//! direction needs no shim: wrap any `AsyncRead` in
//! [`AsyncReadSource`](crate::AsyncReadSource) and hand it to
//! [`File::save`](crate::File::save).
//!
//! Reading a byte range through the shim:
//!
//! ```
//! use std::sync::Arc;
//!
//! use nectar_file::{File, Policy, TokioReader};
//! use nectar_primitives::chunk::AnyChunkSet;
//! use nectar_primitives::store::{ContentGet, MemoryStore};
//! use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() {
//! let data: Vec<u8> = (0u32..20_000)
//!     .map(|i| u8::try_from(i % 251).unwrap())
//!     .collect();
//! let store = Arc::new(MemoryStore::<AnyChunkSet<4096>>::new());
//! let root = File::<_, 4096>::new(Arc::clone(&store), Policy::DEFAULT)
//!     .save(&data[..])
//!     .await
//!     .unwrap();
//!
//! let file = File::<_, 4096>::new(ContentGet::new(store), Policy::DEFAULT);
//!
//! // A plain AsyncRead + AsyncSeek: seek to a range, then read it back.
//! let mut reader = TokioReader::from(file.open(root.into()).await.unwrap());
//! reader.seek(SeekFrom::Start(5_000)).await.unwrap();
//! let mut range = vec![0u8; 5_000];
//! reader.read_exact(&mut range).await.unwrap();
//! assert_eq!(range, data[5_000..10_000]);
//! # }
//! ```
//!
//! `tokio_util::io::ReaderStream` turns the reader into a `Stream` of
//! `Bytes` for a streaming http body.

mod reader;
// Sanctioned tokio adapter tests: the test macro expands to `Runtime::block_on`.
#[cfg(test)]
#[allow(clippy::disallowed_methods)]
mod tests;

use std::io::SeekFrom;

pub use reader::TokioReader;

/// A relative seek whose resolved target leaves the unsigned position
/// space.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("seek by {delta} from {base} leaves the position space")]
pub struct SeekOverflow {
    /// Position the displacement was applied to.
    pub base: u64,
    /// Requested displacement.
    pub delta: i64,
}

/// Resolve a [`SeekFrom`] into a target within the clipped range;
/// past-the-end targets are the reader's typed concern.
fn resolve(seek: SeekFrom, position: u64, effective_len: u64) -> Result<u64, SeekOverflow> {
    let (base, delta) = match seek {
        SeekFrom::Start(target) => return Ok(target),
        SeekFrom::Current(delta) => (position, delta),
        SeekFrom::End(delta) => (effective_len, delta),
    };
    base.checked_add_signed(delta)
        .ok_or(SeekOverflow { base, delta })
}
