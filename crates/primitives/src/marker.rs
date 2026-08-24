//! Thread-safety markers: `Send`/`Sync` on multi-threaded targets, unbounded
//! on wasm32 and under the `unsync` feature (the single-thread escape for
//! non-wasm targets such as zkVM guests).

pub use nectar_marker::{MaybeSend, MaybeSync};
