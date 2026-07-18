//! Typed walk failures; every error is terminal (the engine never retries).

use nectar_primitives::chunk::ChunkAddress;

/// Terminal walk failure.
#[derive(Debug, thiserror::Error)]
pub enum WalkError<E> {
    /// A store fetch failed.
    #[error("fetch failed at {address}")]
    Fetch {
        /// Requested chunk address.
        address: ChunkAddress,
        /// Store error behind the failure.
        source: E,
    },
    /// The store completed a fetch with a chunk at a different address.
    #[error("store returned {returned} for requested {requested}")]
    AddressMismatch {
        /// Address the engine asked for.
        requested: ChunkAddress,
        /// Address of the chunk the store handed back.
        returned: ChunkAddress,
    },
    /// The tree's bytes contradict its declared spans.
    #[error(transparent)]
    Shape(#[from] ShapeError),
    /// The engine could neither admit nor await work; a walk invariant is
    /// broken.
    #[error("walk stalled with {pending} pending nodes and occupancy {occupancy}")]
    Stalled {
        /// Nodes still queued in the frontiers.
        pending: usize,
        /// Leaf bodies held (in flight plus buffered).
        occupancy: usize,
    },
}

/// Contradiction between a node's bytes and the span its parent declared.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ShapeError {
    /// A leaf body's length disagrees with its declared span.
    #[error("leaf at {offset} spans {span} bytes but carries {len}")]
    LeafLength {
        /// Absolute offset of the leaf's first byte.
        offset: u64,
        /// Span the parent declared for the leaf.
        span: u64,
        /// Bytes the leaf body actually carries.
        len: u64,
    },
    /// An intermediate body carries fewer references than its span requires.
    #[error("intermediate at {offset} holds {have} of {expected} references")]
    Arity {
        /// Absolute offset of the intermediate's first byte.
        offset: u64,
        /// References the declared span requires.
        expected: u64,
        /// References the body actually holds.
        have: u64,
    },
    /// A child offset overflows the addressable file range.
    #[error("child offsets overflow at {offset} with span {span}")]
    Offset {
        /// Absolute offset of the overflowing parent.
        offset: u64,
        /// Span the parent declared.
        span: u64,
    },
}
