//! Typed split failures; every error is terminal (the engine never retries).

use nectar_primitives::PrimitivesError;
use nectar_primitives::chunk::ChunkAddress;

/// Failure sealing one payload into a chunk.
#[derive(Debug, thiserror::Error)]
pub enum SealError {
    /// Chunk construction or encryption rejected the payload.
    #[error("chunk construction failed")]
    Chunk(#[from] PrimitivesError),
    /// The key source yielded no key for the chunk.
    #[cfg(feature = "encryption")]
    #[error("key source failed")]
    Key(#[from] super::encrypted::KeyError),
}

/// Terminal split failure.
#[derive(Debug, thiserror::Error)]
pub enum SplitError<E> {
    /// A store put failed.
    #[error("put failed at {address}")]
    Put {
        /// Address of the chunk the put carried.
        address: ChunkAddress,
        /// Store error behind the failure.
        source: E,
    },
    /// Sealing a payload into a chunk failed.
    #[error("seal failed")]
    Seal(#[from] SealError),
    /// Accumulated child spans overflowed the `u64` length domain.
    #[error("span overflow adding {add} to {span}")]
    SpanOverflow {
        /// Span already accumulated at the level.
        span: u64,
        /// Child span whose addition overflowed.
        add: u64,
    },
    /// A write arrived after `finish` began; the split accepts no more
    /// bytes.
    #[error("write after finish")]
    Finished,
    /// An earlier failure fused the split shut.
    #[error("poisoned by an earlier failure")]
    Poisoned,
    /// The pool dropped a leaf seal without replying; the worker panicked
    /// or died. Only the rayon batch ingest raises it, but the variant is
    /// always present so a cross-crate match over `SplitError` stays
    /// exhaustive regardless of the file crate's feature resolution.
    #[error("hash pool dropped a leaf seal")]
    PoolDropped,
    /// The spine emptied without yielding a root; a split invariant is
    /// broken.
    #[error("spine depleted without a root")]
    SpineDepleted,
}
