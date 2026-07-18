//! Async io adapters over the poll-native read surface.
//!
//! [`TokioReader`] shims [`AsyncRead`](::tokio::io::AsyncRead) and
//! [`AsyncSeek`](::tokio::io::AsyncSeek) straight over a
//! [`FileReader`](crate::FileReader): every poll drains the walk in place,
//! so the fetch window stays in flight across polls and no future is
//! created per call. [`SpawnedReader`] opts into a runtime task that keeps
//! the walk advancing between reads. Positions are zero-based within the
//! clipped range, so [`SeekFrom::End`] resolves against the effective
//! length.
//!
//! Serving one http range request through the shim:
//!
//! ```
//! # #![allow(deprecated)]
//! use std::sync::Arc;
//!
//! use axum::Router;
//! use axum::body::Body;
//! use axum::extract::State;
//! use axum::http::{HeaderMap, Request, StatusCode, header};
//! use axum::response::Response;
//! use axum::routing::get;
//! use http_body_util::BodyExt;
//! use nectar_file::{File, TokioReader};
//! use nectar_primitives::chunk::AnyChunkSet;
//! use nectar_primitives::store::MemoryStore;
//! use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};
//! use tokio_util::io::ReaderStream;
//! use tower::util::ServiceExt;
//!
//! type Store = MemoryStore<AnyChunkSet<4096>>;
//!
//! async fn blob(State(file): State<Arc<File<Store>>>, headers: HeaderMap) -> Response {
//!     let (start, end): (u64, u64) = headers[header::RANGE]
//!         .to_str()
//!         .ok()
//!         .and_then(|range| range.strip_prefix("bytes="))
//!         .and_then(|range| range.split_once('-'))
//!         .map(|(start, end)| (start.parse().unwrap(), end.parse().unwrap()))
//!         .unwrap();
//!     let mut reader = TokioReader::from(file.read().build());
//!     reader.seek(SeekFrom::Start(start)).await.unwrap();
//!     Response::builder()
//!         .status(StatusCode::PARTIAL_CONTENT)
//!         .header(
//!             header::CONTENT_RANGE,
//!             format!("bytes {start}-{end}/{}", file.len()),
//!         )
//!         .body(Body::from_stream(ReaderStream::new(
//!             reader.take(end - start + 1),
//!         )))
//!         .unwrap()
//! }
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() {
//! let data: Vec<u8> = (0u32..20_000)
//!     .map(|i| u8::try_from(i % 251).unwrap())
//!     .collect();
//! # let (root, store) = nectar_primitives::file::split::<4096>(&data).unwrap();
//! let file = Arc::new(File::open(store, root).await.unwrap());
//! let app = Router::new().route("/blob", get(blob)).with_state(file);
//!
//! let response = app
//!     .oneshot(
//!         Request::builder()
//!             .uri("/blob")
//!             .header(header::RANGE, "bytes=5000-9999")
//!             .body(Body::empty())
//!             .unwrap(),
//!     )
//!     .await
//!     .unwrap();
//! assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
//! let body = response.into_body().collect().await.unwrap().to_bytes();
//! assert_eq!(&body[..], &data[5000..10_000]);
//! # }
//! ```

mod reader;
#[cfg(not(any(target_arch = "wasm32", feature = "unsync")))]
mod spawned;
#[cfg(test)]
mod tests;

use std::io::SeekFrom;

pub use reader::TokioReader;
#[cfg(not(any(target_arch = "wasm32", feature = "unsync")))]
pub use spawned::SpawnedReader;

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
