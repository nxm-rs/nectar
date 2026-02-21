//! Chunk mode traits for plain and encrypted file operations.

use std::fmt::Debug;

use bytes::Bytes;

use crate::bmt::SPAN_SIZE;
use crate::chunk::encryption::{EncryptedChunkRef, EncryptionKey, decrypt_chunk_data};
use crate::chunk::{BmtChunk, Chunk, ChunkAddress, ContentChunk};
use crate::store::{ChunkGet, ChunkPut};

use super::constants::{
    ENCRYPTED_REF_SIZE, ENCRYPTED_SPANS, LEVEL_LIMIT, REF_SIZE, SPANS,
};
use super::error::{FileError, Result};

/// Convert a `PrimitivesError` from chunk creation into a `FileError`.
fn chunk_creation_error(e: crate::error::PrimitivesError) -> FileError {
    match e {
        crate::error::PrimitivesError::Chunk(c) => FileError::Chunk(c),
        other => FileError::Sink(Box::new(other)),
    }
}

/// Joiner-side chunk mode operations.
pub trait JoinMode: Sized + 'static {
    const REF_SIZE: usize;

    /// Root reference type: `ChunkAddress` (plain) or `EncryptedChunkRef` (encrypted).
    type RootRef: Clone + Debug + Send + Sync;

    /// Per-chunk context carried through tree traversal: `()` (plain) or `EncryptionKey`.
    type JoinerContext: Clone + Debug + Send + Sync;

    fn spans() -> &'static [u64; LEVEL_LIMIT];

    fn refs_per_chunk(body_size: usize) -> usize {
        body_size / Self::REF_SIZE
    }

    fn levels(length: u64, chunk_size: usize) -> usize {
        super::constants::tree_depth(length, chunk_size, Self::REF_SIZE)
    }

    fn subspan_size<const BS: usize>(span: u64) -> u64 {
        super::constants::subspan_for_spans::<BS>(span, Self::spans())
    }

    /// Compute the span covered by a child at `child_index` within a parent of `parent_span`.
    fn child_span<const BS: usize>(parent_span: u64, subspan: u64, child_index: usize) -> u64 {
        let refs_per_chunk = Self::refs_per_chunk(BS);
        if child_index == refs_per_chunk - 1 {
            let preceding = child_index as u64 * subspan;
            parent_span.saturating_sub(preceding)
        } else {
            subspan.min(parent_span - child_index as u64 * subspan)
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

    /// Initialize joiner: fetch root chunk, extract span and context.
    fn joiner_init<const BS: usize, G: ChunkGet<BS>>(
        getter: &G,
        input: Self::RootRef,
    ) -> Result<(ChunkAddress, u64, Self::JoinerContext)> {
        let addr = Self::root_address(&input);
        let chunk = getter.get(&addr).map_err(FileError::getter)?;
        Self::init_from_chunk::<BS>(input, chunk)
    }

    /// Read chunk body at address with context. Returns body bytes (after decryption if needed).
    fn read_chunk_body<const BS: usize, G: ChunkGet<BS>>(
        getter: &G,
        address: &ChunkAddress,
        context: &Self::JoinerContext,
        span: u64,
    ) -> Result<Bytes> {
        let chunk = getter.get(address).map_err(FileError::getter)?;
        Self::decode_body::<BS>(chunk, context, span)
    }

    /// Parse a child reference from body bytes at offset. Returns (address, child_context).
    fn parse_child_ref(
        body: &[u8],
        ref_start: usize,
    ) -> Result<(ChunkAddress, Self::JoinerContext)>;
}

/// Splitter-side chunk mode operations (extends JoinMode).
pub trait SplitMode: JoinMode {
    /// Fixed-size byte array for a reference: `[u8; 32]` or `[u8; 64]`.
    type RefBytes: AsRef<[u8]> + AsMut<[u8]> + Clone + Debug + Send + Sync;

    /// Create a zero-initialized reference buffer.
    fn empty_ref() -> Self::RefBytes;

    /// Prepare chunk data (span + body) for storage, returning chunk and reference bytes.
    fn prepare_chunk<const BS: usize>(
        data: &[u8],
    ) -> Result<(ContentChunk<BS>, Self::RefBytes)>;

    /// Process chunk data (span + body), store it, return reference bytes.
    fn process_chunk<const BS: usize, S: ChunkPut<BS>>(
        data: &[u8],
        sink: &mut S,
    ) -> Result<Self::RefBytes> {
        let (chunk, ref_bytes) = Self::prepare_chunk::<BS>(data)?;
        sink.put(chunk).map_err(FileError::sink)?;
        Ok(ref_bytes)
    }

    /// Process empty file, store chunk, return root ref.
    fn process_empty<const BS: usize, S: ChunkPut<BS>>(sink: &mut S) -> Result<Self::RootRef>;

    /// Extract root reference from top of buffer.
    fn extract_root(buffer: &[u8]) -> Self::RootRef;
}

/// Plain (unencrypted) chunk mode.
#[derive(Debug)]
pub struct PlainMode;

impl JoinMode for PlainMode {
    const REF_SIZE: usize = REF_SIZE;
    type RootRef = ChunkAddress;
    type JoinerContext = ();

    fn spans() -> &'static [u64; LEVEL_LIMIT] {
        &SPANS
    }

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

    fn decode_body<const BS: usize>(
        chunk: ContentChunk<BS>,
        _context: &(),
        _span: u64,
    ) -> Result<Bytes> {
        Ok(chunk.data().clone())
    }

    fn parse_child_ref(
        body: &[u8],
        ref_start: usize,
    ) -> Result<(ChunkAddress, ())> {
        let ref_end = ref_start + REF_SIZE;
        let child_addr_bytes: [u8; 32] = body[ref_start..ref_end]
            .try_into()
            .map_err(|_| FileError::InvalidReference { level: 0 })?;
        Ok((ChunkAddress::from(child_addr_bytes), ()))
    }
}

impl SplitMode for PlainMode {
    type RefBytes = [u8; REF_SIZE];

    fn empty_ref() -> [u8; REF_SIZE] {
        [0u8; REF_SIZE]
    }

    fn prepare_chunk<const BS: usize>(
        data: &[u8],
    ) -> Result<(ContentChunk<BS>, [u8; REF_SIZE])> {
        let chunk = ContentChunk::<BS>::try_from(Bytes::from(data.to_vec())).map_err(chunk_creation_error)?;
        let address = *chunk.address();
        Ok((chunk, address.into()))
    }

    fn process_empty<const BS: usize, S: ChunkPut<BS>>(
        sink: &mut S,
    ) -> Result<ChunkAddress> {
        let chunk = ContentChunk::<BS>::new(Bytes::new()).map_err(chunk_creation_error)?;
        let address = *chunk.address();
        sink.put(chunk).map_err(FileError::sink)?;
        Ok(address)
    }

    fn extract_root(buffer: &[u8]) -> ChunkAddress {
        let root_bytes: [u8; 32] = buffer[..REF_SIZE].try_into().unwrap();
        ChunkAddress::from(root_bytes)
    }
}

/// Encrypted chunk mode.
#[derive(Debug)]
pub struct EncryptedMode;

impl EncryptedMode {
    /// Calculate data length for decryption of a chunk with given span.
    fn decrypt_data_length<const BS: usize>(span: u64) -> usize {
        if span <= BS as u64 {
            span as usize
        } else {
            let subspan = Self::subspan_size::<BS>(span);
            let num_children = span.div_ceil(subspan) as usize;
            let raw = num_children * ENCRYPTED_REF_SIZE;
            raw.min(BS)
        }
    }
}

impl JoinMode for EncryptedMode {
    const REF_SIZE: usize = ENCRYPTED_REF_SIZE;
    type RootRef = EncryptedChunkRef;
    type JoinerContext = EncryptionKey;

    fn spans() -> &'static [u64; LEVEL_LIMIT] {
        &ENCRYPTED_SPANS
    }

    fn root_address(input: &EncryptedChunkRef) -> ChunkAddress {
        input.address
    }

    fn init_from_chunk<const BS: usize>(
        root_ref: EncryptedChunkRef,
        chunk: ContentChunk<BS>,
    ) -> Result<(ChunkAddress, u64, EncryptionKey)> {
        let encrypted_data: Bytes = chunk.into();

        let span_buf = decrypt_span::<BS>(&encrypted_data, &root_ref.key)?;
        let span = u64::from_le_bytes(span_buf);

        Ok((root_ref.address, span, root_ref.key))
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

    fn parse_child_ref(
        body: &[u8],
        ref_start: usize,
    ) -> Result<(ChunkAddress, EncryptionKey)> {
        let ref_end = ref_start + ENCRYPTED_REF_SIZE;
        let child_addr_bytes: [u8; 32] = body[ref_start..ref_start + 32]
            .try_into()
            .map_err(|_| FileError::InvalidReference { level: 0 })?;
        let child_key = EncryptionKey::try_from(&body[ref_start + 32..ref_end])?;
        Ok((ChunkAddress::from(child_addr_bytes), child_key))
    }
}

#[cfg(feature = "encryption")]
impl SplitMode for EncryptedMode {
    type RefBytes = [u8; ENCRYPTED_REF_SIZE];

    fn empty_ref() -> [u8; ENCRYPTED_REF_SIZE] {
        [0u8; ENCRYPTED_REF_SIZE]
    }

    fn prepare_chunk<const BS: usize>(
        data: &[u8],
    ) -> Result<(ContentChunk<BS>, [u8; ENCRYPTED_REF_SIZE])> {
        use crate::chunk::encryption::encrypt_chunk;

        let (key, ciphertext) = encrypt_chunk::<BS>(data)?;

        let chunk = ContentChunk::<BS>::try_from(Bytes::from(ciphertext)).map_err(chunk_creation_error)?;
        let address = *chunk.address();

        let mut ref_bytes = [0u8; ENCRYPTED_REF_SIZE];
        ref_bytes[..32].copy_from_slice(address.as_bytes());
        ref_bytes[32..].copy_from_slice(<EncryptionKey as AsRef<[u8]>>::as_ref(&key));
        Ok((chunk, ref_bytes))
    }

    fn process_empty<const BS: usize, S: ChunkPut<BS>>(
        sink: &mut S,
    ) -> Result<EncryptedChunkRef> {
        use crate::chunk::encryption::encrypt_chunk;

        let chunk_bytes = 0u64.to_le_bytes().to_vec();
        let (key, ciphertext) = encrypt_chunk::<BS>(&chunk_bytes)?;
        let chunk = ContentChunk::<BS>::try_from(Bytes::from(ciphertext)).map_err(chunk_creation_error)?;
        let address = *chunk.address();
        sink.put(chunk).map_err(FileError::sink)?;
        Ok(EncryptedChunkRef { address, key })
    }

    fn extract_root(buffer: &[u8]) -> EncryptedChunkRef {
        let root_ref_bytes = &buffer[..ENCRYPTED_REF_SIZE];
        let address_bytes: [u8; 32] = root_ref_bytes[..32].try_into().unwrap();
        let key_bytes: [u8; 32] = root_ref_bytes[32..64].try_into().unwrap();

        EncryptedChunkRef {
            address: ChunkAddress::from(address_bytes),
            key: EncryptionKey::from(key_bytes),
        }
    }
}

/// Decrypt just the span (first 8 bytes) from encrypted chunk data.
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

    let span_ctr = (BODY_SIZE / 32) as u32;
    let mut span_buf = [0u8; SPAN_SIZE];
    transcrypt(key, span_ctr, &encrypted_data[..SPAN_SIZE], &mut span_buf);
    Ok(span_buf)
}
