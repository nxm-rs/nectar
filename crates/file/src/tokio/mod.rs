//! Async io adapters over the poll-native read and write surfaces.
//!
//! [`TokioReader`] shims [`AsyncRead`](::tokio::io::AsyncRead) and
//! [`AsyncSeek`](::tokio::io::AsyncSeek) straight over a
//! [`FileReader`](crate::FileReader): every poll drains the walk in place,
//! so the fetch window stays in flight across polls and no future is
//! created per call. [`SpawnedReader`] opts into a runtime task that keeps
//! the walk advancing between reads. Positions are zero-based within the
//! clipped range, so [`SeekFrom::End`] resolves against the effective
//! length. [`TokioWriter`] shims [`AsyncWrite`](::tokio::io::AsyncWrite)
//! over a [`Split`](crate::Split): `poll_shutdown` drives the finish and
//! `into_inner` hands back the delivered root.
//!
//! Reading a byte range through the shim:
//!
//! ```
//! use std::sync::Arc;
//!
//! use nectar_file::{File, Plain, PutWindow, Split, TokioReader};
//! use nectar_primitives::chunk::AnyChunkSet;
//! use nectar_primitives::store::MemoryStore;
//! use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};
//!
//! type Store = Arc<MemoryStore<AnyChunkSet<4096>>>;
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() {
//! let data: Vec<u8> = (0u32..20_000)
//!     .map(|i| u8::try_from(i % 251).unwrap())
//!     .collect();
//! # let store: Store = Arc::new(MemoryStore::new());
//! # let root = {
//! #     let mut split = Split::<_, Plain, 4096>::new(Arc::clone(&store), PutWindow::DEFAULT);
//! #     let mut buf = data.as_slice();
//! #     while !buf.is_empty() {
//! #         let n = core::future::poll_fn(|cx| split.poll_write(cx, buf)).await.unwrap();
//! #         buf = &buf[n..];
//! #     }
//! #     core::future::poll_fn(|cx| split.poll_finish(cx)).await.unwrap()
//! # };
//! let file: File<Store> = File::open(store, root).await.unwrap();
//!
//! // A plain AsyncRead + AsyncSeek: seek to a range, then read it back.
//! let mut reader = TokioReader::from(file.read().build());
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
#[cfg(not(any(target_arch = "wasm32", feature = "unsync")))]
mod spawned;
#[cfg(test)]
mod tests;
// The writer maps split failures into `io::Error`, which boxes them
// `Send + Sync`; the wasm32 and `unsync` error chains are not.
#[cfg(not(any(target_arch = "wasm32", feature = "unsync")))]
mod writer;

use std::io::SeekFrom;

pub use reader::TokioReader;
#[cfg(not(any(target_arch = "wasm32", feature = "unsync")))]
pub use spawned::SpawnedReader;
#[cfg(not(any(target_arch = "wasm32", feature = "unsync")))]
pub use writer::TokioWriter;

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
/// past-the-end targets are the readers' typed concern.
fn resolve(seek: SeekFrom, position: u64, effective_len: u64) -> Result<u64, SeekOverflow> {
    let (base, delta) = match seek {
        SeekFrom::Start(target) => return Ok(target),
        SeekFrom::Current(delta) => (position, delta),
        SeekFrom::End(delta) => (effective_len, delta),
    };
    base.checked_add_signed(delta)
        .ok_or(SeekOverflow { base, delta })
}
