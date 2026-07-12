//! Chunk mode traits for plain and encrypted file operations.

use std::fmt::Debug;

use bytes::Bytes;

use crate::bmt::SPAN_SIZE;
use crate::chunk::encryption::{EncryptedChunkRef, EncryptionKey, decrypt_chunk_data};
use crate::chunk::{BmtChunk, Chunk, ChunkAddress, ChunkRef, ContentChunk, Reference};
use crate::store::MaybeSend;
use crate::wire::Cursor;

use super::constants::{compute_spans_inline, subspan_for_spans};
use super::error::{FileError, Result};

/// Convert a `PrimitivesError` from chunk creation into a `FileError`.
fn chunk_creation_error(e: crate::error::PrimitivesError) -> FileError {
    match e {
        crate::error::PrimitivesError::Chunk(c) => FileError::Chunk(c),
        other => FileError::Store(Box::new(other)),
    }
}

/// Create a `ContentChunk` from raw bytes.
#[inline]
fn create_chunk<const BS: usize>(data: Bytes) -> Result<ContentChunk<BS>> {
    ContentChunk::<BS>::try_from(data).map_err(chunk_creation_error)
}

/// Joiner-side chunk mode operations.
pub trait JoinMode: Sized + 'static {
    /// The reference type this mode reads; its width is a fact of the type.
    type Ref: Reference + Clone + Debug + Send + Sync;

    /// Wire width of one reference, derived from [`Self::Ref`] so no mode
    /// restates 32 or 64.
    const REF_SIZE: usize = <Self::Ref as Reference>::SIZE;

    /// Root reference type: `ChunkAddress` (plain) or `EncryptedChunkRef` (encrypted).
    type RootRef: Clone + Debug + Send + Sync;

    /// Per-chunk context carried through tree traversal: `()` (plain) or `EncryptionKey`.
    type JoinerContext: Clone + Debug + Send + Sync;

    /// Number of child references per intermediate chunk.
    #[allow(clippy::arithmetic_side_effects)] // REF_SIZE is the nonzero constant 32 or 64
    #[inline]
    fn refs_per_chunk(body_size: usize) -> usize {
        body_size / Self::REF_SIZE
    }

    /// Tree depth for the given file length.
    #[inline]
    fn levels(length: u64, chunk_size: usize) -> usize {
        super::constants::tree_depth(length, chunk_size, Self::REF_SIZE)
    }

    /// Subspan size for a given parent span.
    #[allow(clippy::arithmetic_side_effects)] // REF_SIZE is the nonzero constant 32 or 64
    #[inline]
    fn subspan_size<const BS: usize>(span: u64) -> u64 {
        let spans = compute_spans_inline(BS / Self::REF_SIZE);
        subspan_for_spans::<BS>(span, &spans)
    }

    /// Compute the span covered by a child at `child_index` within a parent of `parent_span`.
    #[allow(clippy::arithmetic_side_effects)]
    // branches = BS / REF_SIZE >= 1 for the asserted BS >= 64, so branches - 1 cannot underflow; child_index * subspan addresses a child inside the parent span for any tree the splitters can produce
    #[inline]
    fn child_span<const BS: usize>(parent_span: u64, subspan: u64, child_index: usize) -> u64 {
        let branches = Self::refs_per_chunk(BS);
        let child_index = crate::cast::u64_from_usize(child_index);
        if child_index == crate::cast::u64_from_usize(branches - 1) {
            let preceding = child_index * subspan;
            parent_span.saturating_sub(preceding)
        } else {
            subspan.min(parent_span.saturating_sub(child_index * subspan))
        }
    }

    /// Extract the chunk address from a root reference (for fetching).
    fn root_address(input: &Self::RootRef) -> ChunkAddress;

    /// Initialize joiner from a root ref and pre-fetched root chunk.
    fn init_from_chunk<const BS: usize>(
        input: Self::RootRef,
        chunk: ContentChunk<BS>,
    ) -> Result<(ChunkAddress, u64, Self::JoinerContext)>;

    /// Decode a fetched chunk into body bytes (decrypting if needed).
    fn decode_body<const BS: usize>(
        chunk: ContentChunk<BS>,
        context: &Self::JoinerContext,
        span: u64,
    ) -> Result<Bytes>;

    /// Read this mode's per-child context, the bytes that trail the address in
    /// a child reference: plain carries none, encrypted reads its key. The
    /// [`Cursor`] is the sole fallible read.
    fn context_from_wire(cursor: &mut Cursor<'_>) -> Result<Self::JoinerContext>;
}

/// Initialize joiner: fetch root chunk, extract span and context.
pub(crate) async fn joiner_init<
    M: JoinMode + MaybeSend + Sync,
    G: crate::store::ChunkGet<BS>,
    const BS: usize,
>(
    getter: &G,
    input: M::RootRef,
) -> Result<(ChunkAddress, u64, M::JoinerContext)> {
    let addr = M::root_address(&input);
    let any = getter.get(&addr).await.map_err(FileError::getter)?;
    let chunk = any.into_content().ok_or(FileError::InvalidChunkType {
        type_name: "non-content",
    })?;
    M::init_from_chunk::<BS>(input, chunk)
}

/// Read chunk body at address with context. Returns body bytes (after decryption if needed).
pub(crate) async fn read_chunk_body<
    M: JoinMode + MaybeSend + Sync,
    G: crate::store::ChunkGet<BS>,
    const BS: usize,
>(
    getter: &G,
    address: &ChunkAddress,
    context: &M::JoinerContext,
    span: u64,
) -> Result<Bytes> {
    let address = *address;
    let context = context.clone();
    let any = getter.get(&address).await.map_err(FileError::getter)?;
    let chunk = any.into_content().ok_or(FileError::InvalidChunkType {
        type_name: "non-content",
    })?;
    M::decode_body::<BS>(chunk, &context, span)
}

/// Splitter-side chunk mode operations (extends JoinMode).
pub trait SplitMode: JoinMode {
    /// Prepare chunk data (span + body), returning the chunk and the reference
    /// that names it. Takes ownership of the payload to avoid an extra
    /// allocation.
    fn prepare_chunk<const BS: usize>(data: Vec<u8>) -> Result<(ContentChunk<BS>, Self::Ref)>;

    /// Produce the chunk for an empty file, returning it and the root ref.
    fn empty_chunk<const BS: usize>() -> Result<(ContentChunk<BS>, Self::RootRef)>;

    /// Append a reference's `REF_SIZE` wire bytes to `out`.
    fn extend_ref_bytes(reference: &Self::Ref, out: &mut Vec<u8>);

    /// The root reference of a finished tree from its sole top reference.
    fn root_ref(reference: Self::Ref) -> Self::RootRef;
}

/// Plain (unencrypted) chunk mode.
#[derive(Debug)]
pub struct PlainMode;

impl JoinMode for PlainMode {
    type Ref = ChunkRef;
    type RootRef = ChunkAddress;
    type JoinerContext = ();

    #[inline]
    fn root_address(input: &ChunkAddress) -> ChunkAddress {
        *input
    }

    fn init_from_chunk<const BS: usize>(
        root: ChunkAddress,
        chunk: ContentChunk<BS>,
    ) -> Result<(ChunkAddress, u64, ())> {
        let span = chunk.span();
        Ok((root, span, ()))
    }

    #[inline]
    fn decode_body<const BS: usize>(
        chunk: ContentChunk<BS>,
        _context: &(),
        _span: u64,
    ) -> Result<Bytes> {
        Ok(chunk.data().clone())
    }

    #[inline]
    fn context_from_wire(_cursor: &mut Cursor<'_>) -> Result<()> {
        // A plain reference is the address alone; it carries no trailing context.
        Ok(())
    }
}

impl SplitMode for PlainMode {
    #[inline]
    fn prepare_chunk<const BS: usize>(data: Vec<u8>) -> Result<(ContentChunk<BS>, ChunkRef)> {
        let chunk = create_chunk::<BS>(Bytes::from(data))?;
        let reference = ChunkRef::new(*chunk.address());
        Ok((chunk, reference))
    }

    fn empty_chunk<const BS: usize>() -> Result<(ContentChunk<BS>, ChunkAddress)> {
        // Use `new` (not `try_from`) because Bytes::new() is raw content,
        // not pre-formatted span+body data.
        let chunk = ContentChunk::<BS>::new(Bytes::new()).map_err(chunk_creation_error)?;
        let address = *chunk.address();
        Ok((chunk, address))
    }

    #[inline]
    fn extend_ref_bytes(reference: &ChunkRef, out: &mut Vec<u8>) {
        out.extend_from_slice(reference.address().as_bytes());
    }

    #[inline]
    fn root_ref(reference: ChunkRef) -> ChunkAddress {
        reference.into_address()
    }
}

/// Encrypted chunk mode.
///
/// `JoinMode` (decryption) is always available. `SplitMode` (encryption)
/// requires the `encryption` feature because key generation depends on `rand`.
#[derive(Debug)]
pub struct EncryptedMode;

impl EncryptedMode {
    /// Calculate data length for decryption of a chunk with given span.
    #[allow(clippy::arithmetic_side_effects)] // sub = subspan_size(..) >= BS > 0 for the div_ceil; num_children <= branches (sub covers at least span / branches), so the product is bounded by BS
    fn decrypt_data_length<const BS: usize>(span: u64) -> usize {
        if span <= crate::cast::u64_from_usize(BS) {
            // span <= BS here, so it fits usize on all supported targets.
            crate::cast::usize_from_u64(span)
        } else {
            let sub = Self::subspan_size::<BS>(span);
            // num_children <= branches (sub covers at least span / branches).
            let num_children = crate::cast::usize_from_u64(span.div_ceil(sub));
            let raw = num_children * EncryptedChunkRef::SIZE;
            raw.min(BS)
        }
    }
}

impl JoinMode for EncryptedMode {
    type Ref = EncryptedChunkRef;
    type RootRef = EncryptedChunkRef;
    type JoinerContext = EncryptionKey;

    fn root_address(input: &EncryptedChunkRef) -> ChunkAddress {
        *input.address()
    }

    fn init_from_chunk<const BS: usize>(
        root_ref: EncryptedChunkRef,
        chunk: ContentChunk<BS>,
    ) -> Result<(ChunkAddress, u64, EncryptionKey)> {
        let encrypted_data: Bytes = chunk.into();

        let span_buf = decrypt_span::<BS>(&encrypted_data, root_ref.key())?;
        let span = u64::from_le_bytes(span_buf);

        let (address, key) = root_ref.into_parts();
        Ok((address, span, key))
    }

    fn decode_body<const BS: usize>(
        chunk: ContentChunk<BS>,
        key: &EncryptionKey,
        span: u64,
    ) -> Result<Bytes> {
        let encrypted_data: Bytes = chunk.into();

        let data_length = Self::decrypt_data_length::<BS>(span);
        let decrypted = decrypt_chunk_data::<BS>(&encrypted_data, key, data_length)?;
        Ok(Bytes::from(decrypted).slice(SPAN_SIZE..))
    }

    fn context_from_wire(cursor: &mut Cursor<'_>) -> Result<EncryptionKey> {
        // An encrypted reference trails its address with the decryption key.
        let key_bytes = cursor
            .take_slice(EncryptionKey::SIZE)
            .map_err(|_| FileError::InvalidReference { level: 0 })?;
        Ok(EncryptionKey::try_from(key_bytes)?)
    }
}

#[cfg(feature = "encryption")]
impl SplitMode for EncryptedMode {
    fn prepare_chunk<const BS: usize>(
        data: Vec<u8>,
    ) -> Result<(ContentChunk<BS>, EncryptedChunkRef)> {
        use crate::chunk::encryption::encrypt_chunk;

        let key = EncryptionKey::generate();
        let ciphertext = encrypt_chunk::<BS>(&data, &key)?;
        let chunk = create_chunk::<BS>(Bytes::from(ciphertext))?;
        let reference = EncryptedChunkRef::new(*chunk.address(), key);
        Ok((chunk, reference))
    }

    fn empty_chunk<const BS: usize>() -> Result<(ContentChunk<BS>, EncryptedChunkRef)> {
        use crate::chunk::encryption::encrypt_chunk;

        let key = EncryptionKey::generate();
        let chunk_bytes = 0u64.to_le_bytes().to_vec();
        let ciphertext = encrypt_chunk::<BS>(&chunk_bytes, &key)?;
        let chunk = create_chunk::<BS>(Bytes::from(ciphertext))?;
        let address = *chunk.address();
        Ok((chunk, EncryptedChunkRef::new(address, key)))
    }

    fn extend_ref_bytes(reference: &EncryptedChunkRef, out: &mut Vec<u8>) {
        out.extend_from_slice(&<[u8; EncryptedChunkRef::SIZE]>::from(reference));
    }

    fn root_ref(reference: EncryptedChunkRef) -> EncryptedChunkRef {
        reference
    }
}

/// Decrypt just the span (first 8 bytes) from encrypted chunk data.
#[allow(clippy::arithmetic_side_effects, clippy::indexing_slicing)] // SPAN_SIZE + BODY_SIZE sums small compile-time constants; the exact-length check above guarantees encrypted_data has at least SPAN_SIZE bytes; EncryptionKey::SIZE is a nonzero constant
fn decrypt_span<const BODY_SIZE: usize>(
    encrypted_data: &[u8],
    key: &EncryptionKey,
) -> Result<[u8; SPAN_SIZE]> {
    use crate::chunk::encryption::transcrypt;

    let expected_len = SPAN_SIZE + BODY_SIZE;
    if encrypted_data.len() != expected_len {
        return Err(FileError::Encryption(
            crate::chunk::encryption::EncryptionError::DataTooShort {
                len: encrypted_data.len(),
                min: expected_len,
            },
        ));
    }

    // BODY_SIZE / 32 is 128 for the default 4096-byte body and stays far
    // below u32::MAX for any chunk-sized body.
    #[allow(clippy::as_conversions)]
    let span_ctr = (BODY_SIZE / EncryptionKey::SIZE) as u32;
    let mut span_buf = [0u8; SPAN_SIZE];
    transcrypt(key, span_ctr, &encrypted_data[..SPAN_SIZE], &mut span_buf)
        .map_err(FileError::Encryption)?;
    Ok(span_buf)
}
