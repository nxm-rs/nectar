//! Chunk mode traits for plain and encrypted file operations.

use std::fmt::Debug;

use bytes::Bytes;

use crate::bmt::SPAN_SIZE;
use crate::chunk::encryption::{EncryptedChunkRef, EncryptionKey, decrypt_chunk_data};
use crate::chunk::{BmtChunk, Chunk, ChunkAddress, ContentChunk};
use crate::store::MaybeSend;

use super::constants::{ENCRYPTED_REF_SIZE, REF_SIZE, compute_spans_inline, subspan_for_spans};
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
    /// Size of a single reference in bytes (32 plain, 64 encrypted).
    const REF_SIZE: usize;

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

    /// Parse a child reference from body bytes at offset. Returns (address, child_context).
    fn parse_child_ref(
        body: &[u8],
        ref_start: usize,
    ) -> Result<(ChunkAddress, Self::JoinerContext)>;
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
    /// Fixed-size byte array for a reference: `[u8; 32]` or `[u8; 64]`.
    type RefBytes: AsRef<[u8]> + AsMut<[u8]> + Clone + Debug + Send + Sync;

    /// Prepare chunk data (span + body), returning chunk and reference bytes.
    /// Takes ownership of the payload to avoid an extra allocation.
    fn prepare_chunk<const BS: usize>(data: Vec<u8>) -> Result<(ContentChunk<BS>, Self::RefBytes)>;

    /// Produce the chunk for an empty file, returning it and the root ref.
    fn empty_chunk<const BS: usize>() -> Result<(ContentChunk<BS>, Self::RootRef)>;

    /// Extract root reference from top of buffer.
    fn extract_root(buffer: &[u8]) -> Result<Self::RootRef>;
}

/// Plain (unencrypted) chunk mode.
#[derive(Debug)]
pub struct PlainMode;

impl JoinMode for PlainMode {
    const REF_SIZE: usize = REF_SIZE;
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
    fn parse_child_ref(body: &[u8], ref_start: usize) -> Result<(ChunkAddress, ())> {
        // Bounds-check the untrusted body instead of panicking on a short slice.
        let child_addr_bytes: [u8; 32] = ref_start
            .checked_add(REF_SIZE)
            .and_then(|ref_end| body.get(ref_start..ref_end))
            .and_then(|slice| slice.try_into().ok())
            .ok_or(FileError::InvalidReference { level: 0 })?;
        Ok((ChunkAddress::from(child_addr_bytes), ()))
    }
}

impl SplitMode for PlainMode {
    type RefBytes = [u8; REF_SIZE];

    #[inline]
    fn prepare_chunk<const BS: usize>(data: Vec<u8>) -> Result<(ContentChunk<BS>, [u8; REF_SIZE])> {
        let chunk = create_chunk::<BS>(Bytes::from(data))?;
        let ref_bytes = (*chunk.address()).into();
        Ok((chunk, ref_bytes))
    }

    fn empty_chunk<const BS: usize>() -> Result<(ContentChunk<BS>, ChunkAddress)> {
        // Use `new` (not `try_from`) because Bytes::new() is raw content,
        // not pre-formatted span+body data.
        let chunk = ContentChunk::<BS>::new(Bytes::new()).map_err(chunk_creation_error)?;
        let address = *chunk.address();
        Ok((chunk, address))
    }

    fn extract_root(buffer: &[u8]) -> Result<ChunkAddress> {
        let root_bytes: [u8; 32] = buffer
            .get(..REF_SIZE)
            .and_then(|s| s.try_into().ok())
            .ok_or(FileError::InvalidReference { level: 0 })?;
        Ok(ChunkAddress::from(root_bytes))
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
            let raw = num_children * ENCRYPTED_REF_SIZE;
            raw.min(BS)
        }
    }
}

impl JoinMode for EncryptedMode {
    const REF_SIZE: usize = ENCRYPTED_REF_SIZE;
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

    fn parse_child_ref(body: &[u8], ref_start: usize) -> Result<(ChunkAddress, EncryptionKey)> {
        // Bounds-check the untrusted body instead of panicking on a short slice.
        let ref_bytes = ref_start
            .checked_add(ENCRYPTED_REF_SIZE)
            .and_then(|ref_end| body.get(ref_start..ref_end))
            .ok_or(FileError::InvalidReference { level: 0 })?;
        let (addr_bytes, key_bytes) = ref_bytes.split_at(32);
        let child_addr_bytes: [u8; 32] = addr_bytes
            .try_into()
            .map_err(|_| FileError::InvalidReference { level: 0 })?;
        let child_key = EncryptionKey::try_from(key_bytes)?;
        Ok((ChunkAddress::from(child_addr_bytes), child_key))
    }
}

#[cfg(feature = "encryption")]
impl SplitMode for EncryptedMode {
    type RefBytes = [u8; ENCRYPTED_REF_SIZE];

    fn prepare_chunk<const BS: usize>(
        data: Vec<u8>,
    ) -> Result<(ContentChunk<BS>, [u8; ENCRYPTED_REF_SIZE])> {
        use crate::chunk::encryption::encrypt_chunk;

        let key = EncryptionKey::generate();
        let ciphertext = encrypt_chunk::<BS>(&data, &key)?;
        let chunk = create_chunk::<BS>(Bytes::from(ciphertext))?;

        let mut ref_bytes = [0u8; ENCRYPTED_REF_SIZE];
        ref_bytes[..32].copy_from_slice(chunk.address().as_bytes());
        ref_bytes[32..].copy_from_slice(key.as_bytes());
        Ok((chunk, ref_bytes))
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

    fn extract_root(buffer: &[u8]) -> Result<EncryptedChunkRef> {
        let root_ref_bytes = buffer
            .get(..ENCRYPTED_REF_SIZE)
            .ok_or(FileError::InvalidReference { level: 0 })?;
        EncryptedChunkRef::try_from(root_ref_bytes)
            .map_err(|_| FileError::InvalidReference { level: 0 })
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
