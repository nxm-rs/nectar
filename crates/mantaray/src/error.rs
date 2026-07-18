//! Error types for mantaray operations.

use nectar_primitives::chunk::ChunkAddress;
use nectar_primitives::error::PrimitivesError;
use nectar_primitives::store::SharedError;

/// Result type alias for mantaray operations.
pub type Result<T> = core::result::Result<T, MantarayError>;

/// Result type alias for node wire decoding.
pub type DecodeResult<T> = core::result::Result<T, DecodeError>;

/// Wire decode failures for a mantaray node chunk.
///
/// Reported to callers through [`MantarayError::Corrupt`], which pairs the
/// failure with the address of the chunk the malformed bytes came from so a
/// deep-load failure names the offending chunk. bee-spec node.md governs the
/// layout these variants reject.
#[non_exhaustive]
#[derive(thiserror::Error, Debug)]
pub enum DecodeError {
    /// Data is too short to contain a header or a declared field.
    #[error("data too short")]
    TooShort,
    /// Version hash does not match any known version.
    #[error("invalid version hash")]
    InvalidVersionHash,
    /// Header-declared reference width does not match the entry type width.
    #[error("reference size mismatch: expected {expected}, got {actual}")]
    RefSizeMismatch {
        /// Entry-type reference width in bytes.
        expected: usize,
        /// Width declared by the node header.
        actual: usize,
    },
    /// Header-declared reference width matches no known reference kind.
    #[error("unsupported reference width: {actual}")]
    UnsupportedRefWidth {
        /// The header's `ref_size` byte.
        actual: u8,
    },
    /// A zero-width node declares forks, whose reference slots would be empty.
    #[error("zero reference width with a non-empty fork index")]
    ZeroWidthForks,
    /// Fork data has insufficient bytes.
    #[error("insufficient fork bytes: expected {expected}, got {actual} at byte {byte_index}")]
    InsufficientForkBytes {
        /// Expected number of bytes.
        expected: usize,
        /// Actual number of bytes.
        actual: usize,
        /// Byte index of the fork.
        byte_index: usize,
    },
    /// Prefix length is outside the 1..=30 wire range.
    #[error("invalid prefix length: max {max}, got {actual}")]
    InvalidPrefixLength {
        /// Maximum allowed length.
        max: usize,
        /// Actual length.
        actual: usize,
    },
    /// Entry bytes are not a valid reference of the expected width.
    #[error("malformed entry of {size} bytes")]
    Entry {
        /// Entry reference width in bytes.
        size: usize,
    },
    /// Fork metadata is not valid JSON.
    #[error("invalid metadata")]
    Metadata(#[from] serde_json::Error),
}

/// Failures of the depth-guarded reader.
///
/// Absent paths are not errors: lookups report them as `Ok(None)` or
/// `Ok(false)`.
#[non_exhaustive]
#[derive(thiserror::Error, Debug)]
pub enum ReaderError {
    /// The store failed to produce the node chunk at `address`.
    #[error("store get error for {address}: {source}")]
    Store {
        /// Address of the node chunk the store could not produce.
        address: ChunkAddress,
        /// Original store error, preserved for downcasting.
        source: SharedError,
    },
    /// A fetched chunk's bytes are not a decodable mantaray node.
    #[error("corrupt chunk {address}: {source}")]
    Corrupt {
        /// Address of the chunk whose bytes failed to decode.
        address: ChunkAddress,
        /// The underlying wire decode failure.
        source: DecodeError,
    },
    /// A lookup needed more node fetches than the reader's budget allows.
    #[error("max depth exceeded: {max_depth}")]
    MaxDepth {
        /// The node-fetch budget that was exhausted.
        max_depth: usize,
    },
}

/// Failures of the listing cursor and address stream.
///
/// Exhaustion is not an error: a finished walk ends with `None`.
#[non_exhaustive]
#[derive(thiserror::Error, Debug)]
pub enum CursorError {
    /// The store failed to produce the node chunk at `address`.
    #[error("store get error for {address}: {source}")]
    Store {
        /// Address of the node chunk the store could not produce.
        address: ChunkAddress,
        /// Original store error, preserved for downcasting.
        source: SharedError,
    },
    /// A fetched chunk's bytes are not a decodable mantaray node.
    #[error("corrupt chunk {address}: {source}")]
    Corrupt {
        /// Address of the chunk whose bytes failed to decode.
        address: ChunkAddress,
        /// The underlying wire decode failure.
        source: DecodeError,
    },
    /// The store returned a chunk other than the requested one.
    #[error("address mismatch: requested {requested}, returned {returned}")]
    AddressMismatch {
        /// Address the walk requested.
        requested: ChunkAddress,
        /// Address of the chunk the store returned.
        returned: ChunkAddress,
    },
    /// No fetch in flight while nodes remain queued; the walk cannot
    /// progress.
    #[error("walk stalled with {pending} nodes queued")]
    Stalled {
        /// Frontier nodes still awaiting a fetch.
        pending: usize,
    },
}

/// Failures of the submission-order editor.
#[non_exhaustive]
#[derive(thiserror::Error, Debug)]
pub enum EditorError {
    /// Applying one recorded op to the trie failed; `index` is its zero-based
    /// position in the submission-order log.
    #[error("apply op {index} at path {}: {source}", String::from_utf8_lossy(path))]
    Apply {
        /// Zero-based position of the failed op in the log.
        index: usize,
        /// Path the failed op targets.
        path: Vec<u8>,
        /// The underlying trie failure.
        source: MantarayError,
    },
    /// Persisting the applied trie to the store failed.
    #[error(transparent)]
    Commit(#[from] MantarayError),
}

/// Errors that can occur during mantaray trie operations.
#[non_exhaustive]
#[derive(thiserror::Error, Debug)]
pub enum MantarayError {
    /// Node is not a value type (has no entry).
    #[error("not a value type")]
    NotValueType,
    /// No fork found for node with the given reference.
    #[error("no fork found for node: {}", reference.map_or_else(|| "<none>".to_string(), |a| a.to_string()))]
    NoForkFound {
        /// Reference of the node.
        reference: Option<ChunkAddress>,
    },
    /// No entry found for node with the given reference.
    #[error("no entry found for node: {}", reference.map_or_else(|| "<none>".to_string(), |a| a.to_string()))]
    NoEntryFound {
        /// Reference of the node.
        reference: Option<ChunkAddress>,
    },
    /// Entry size does not match expected reference byte size.
    #[error("entry size mismatch: expected {expected}, got {actual}")]
    EntrySizeMismatch {
        /// Expected size.
        expected: usize,
        /// Actual size.
        actual: usize,
    },
    /// Entry reference kind does not match the manifest's reference type.
    #[error(transparent)]
    WrongRefKind(#[from] nectar_primitives::chunk::WrongRefKind),
    /// Path cannot be empty for this operation.
    #[error("empty path")]
    EmptyPath,
    /// Prefix not found in path.
    #[error("path prefix not found: {prefix}")]
    PathPrefixNotFound {
        /// The prefix that was not found.
        prefix: String,
    },
    /// A chunk's bytes could not be decoded into a node.
    #[error("corrupt chunk {address}: {source}")]
    Corrupt {
        /// Address of the chunk whose bytes failed to decode.
        address: ChunkAddress,
        /// The underlying wire decode failure.
        source: DecodeError,
    },
    /// Reference is too long.
    #[error("reference too long: max {max}, got {actual}")]
    RefTooLong {
        /// Maximum allowed length.
        max: usize,
        /// Actual length.
        actual: usize,
    },
    /// Metadata exceeds maximum allowed size.
    #[error("metadata too large: max {max}, got {actual}")]
    MetadataTooLarge {
        /// Maximum allowed size.
        max: usize,
        /// Actual size.
        actual: usize,
    },
    /// Metadata could not be parsed.
    #[error("invalid metadata")]
    Metadata(#[from] serde_json::Error),
    /// Node has not been saved yet (reference is empty).
    #[error("missing reference")]
    MissingReference,
    /// Root node carries its own metadata, which the wire format cannot hold.
    ///
    /// Only fork children serialize metadata; a root's would be silently
    /// dropped, so the builder rejects it at save.
    #[error("root node metadata is not serializable")]
    RootMetadata,
    /// Error from primitives (chunk creation, BMT, etc.).
    #[error(transparent)]
    Primitives(#[from] PrimitivesError),
    /// Error from the file splitter or joiner across the file/manifest seam.
    #[allow(deprecated)]
    #[error(transparent)]
    File(#[from] nectar_primitives::file::FileError),
    /// Error from the typed chunk store during get operations.
    #[error("store get error: {source}")]
    StoreGet {
        /// Original store error, preserved for downcasting.
        source: SharedError,
    },
    /// Error from the typed chunk store during put operations.
    #[error("store put error: {source}")]
    StorePut {
        /// Original store error, preserved for downcasting.
        source: SharedError,
    },
}

/// A wire-level short read is a truncated node buffer.
impl From<nectar_primitives::wire::Underrun> for DecodeError {
    fn from(_: nectar_primitives::wire::Underrun) -> Self {
        Self::TooShort
    }
}
