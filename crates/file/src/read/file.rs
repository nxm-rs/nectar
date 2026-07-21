//! File handles: one opened chunk tree, mode pinned or runtime-dispatched.

use alloc::vec::Vec;
use core::fmt;

use nectar_primitives::chunk::encryption::{EncryptedChunkRef, EncryptionKey, transcrypt_in_place};
use nectar_primitives::chunk::{AnyChunkSet, ChunkAddress, ChunkOps};
use nectar_primitives::store::TrustedGet;
use nectar_primitives::{DEFAULT_BODY_SIZE, EntryRef};

use super::download::DownloadBuilder;
use super::error::{CollectError, OpenError};
use super::reader::ReadBuilder;
use crate::config::Window;
use crate::geometry::Mode;
use crate::walk::{DecodeError, Encrypted, Plain, WalkMode};

/// One opened file: the root reference resolved to its address, context and
/// total span. Opening fetches the root chunk once; reads re-fetch it so the
/// fetch set stays identical to a cold walk.
pub struct File<S, M: WalkMode = Plain, const B: usize = DEFAULT_BODY_SIZE> {
    store: S,
    root: ChunkAddress,
    context: M::Context,
    span: u64,
}

impl<S, M: WalkMode, const B: usize> File<S, M, B> {
    /// Total file length in bytes.
    pub const fn len(&self) -> u64 {
        self.span
    }

    /// Whether the file carries no bytes.
    pub const fn is_empty(&self) -> bool {
        self.span == 0
    }

    /// Address of the root chunk.
    pub const fn root(&self) -> &ChunkAddress {
        &self.root
    }
}

impl<S: Clone, M: WalkMode, const B: usize> File<S, M, B> {
    /// Start building an ordered, seekable read over the whole file.
    pub fn read(&self) -> ReadBuilder<S, M, B> {
        ReadBuilder::new(
            self.store.clone(),
            self.root,
            self.context.clone(),
            self.span,
            Window::DEFAULT,
            0..u64::MAX,
        )
    }

    /// Start building a restartable download of the whole file.
    pub fn download(&self) -> DownloadBuilder<S, M, B> {
        DownloadBuilder::new(
            self.store.clone(),
            self.root,
            self.context.clone(),
            self.span,
            Window::DEFAULT,
            0..u64::MAX,
        )
    }
}

impl<S, M, const B: usize> File<S, M, B>
where
    S: TrustedGet<AnyChunkSet<B>> + Clone + 'static,
    M: WalkMode,
{
    /// Assemble the whole file in memory, at most `max` bytes.
    pub async fn collect(&self, max: u64) -> Result<Vec<u8>, CollectError<S::Error>> {
        self.read().collect(max).await
    }
}

impl<S, const B: usize> File<S, Plain, B>
where
    S: TrustedGet<AnyChunkSet<B>> + Clone + 'static,
{
    /// Open a plain file at its root address, reading the span off the root
    /// chunk.
    pub async fn open(store: S, root: ChunkAddress) -> Result<Self, OpenError<S::Error>> {
        let chunk = fetch_root(&store, root).await?;
        let span = chunk.span();
        Ok(Self {
            store,
            root,
            context: (),
            span,
        })
    }
}

impl<S, const B: usize> File<S, Encrypted, B>
where
    S: TrustedGet<AnyChunkSet<B>> + Clone + 'static,
{
    /// Open an encrypted file at its root reference, decrypting the span off
    /// the root chunk with the reference's key.
    pub async fn open_encrypted(
        store: S,
        root: EncryptedChunkRef,
    ) -> Result<Self, OpenError<S::Error>> {
        let (address, key) = root.into_parts();
        let chunk = fetch_root(&store, address).await?;
        let len = chunk.data().len();
        if len != B {
            return Err(DecodeError::CiphertextLength { len, expected: B }.into());
        }
        // The span header rides its own keystream offset past the body's
        // block count, so its bytes never share keystream with body bytes.
        let mut span_bytes = chunk.span().to_le_bytes();
        transcrypt_in_place(&key, span_counter(B), &mut span_bytes);
        let span = u64::from_le_bytes(span_bytes);
        Ok(Self {
            store,
            root: address,
            context: key,
            span,
        })
    }
}

impl<S, M: WalkMode, const B: usize> fmt::Debug for File<S, M, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("File")
            .field("root", &self.root)
            .field("span", &self.span)
            .field("mode", &M::MODE)
            .finish_non_exhaustive()
    }
}

/// One opened file of either reference width, dispatched at runtime.
#[derive(Debug)]
pub enum AnyFile<S, const B: usize = DEFAULT_BODY_SIZE> {
    /// A plain tree behind a 32-byte reference.
    Plain(File<S, Plain, B>),
    /// An encrypted tree behind a 64-byte reference.
    Encrypted(File<S, Encrypted, B>),
}

impl<S, const B: usize> AnyFile<S, B>
where
    S: TrustedGet<AnyChunkSet<B>> + Clone + 'static,
{
    /// Open a file from a wire reference, dispatching the mode on the
    /// reference width.
    pub async fn open(store: S, root: EntryRef) -> Result<Self, OpenError<S::Error>> {
        match root {
            EntryRef::Plain(reference) => File::open(store, reference.into_address())
                .await
                .map(Self::Plain),
            EntryRef::Encrypted(reference) => File::open_encrypted(store, reference)
                .await
                .map(Self::Encrypted),
        }
    }

    /// Assemble the whole file in memory, at most `max` bytes.
    pub async fn collect(&self, max: u64) -> Result<Vec<u8>, CollectError<S::Error>> {
        match self {
            Self::Plain(file) => file.collect(max).await,
            Self::Encrypted(file) => file.collect(max).await,
        }
    }
}

impl<S, const B: usize> AnyFile<S, B> {
    /// Total file length in bytes.
    pub const fn len(&self) -> u64 {
        match self {
            Self::Plain(file) => file.len(),
            Self::Encrypted(file) => file.len(),
        }
    }

    /// Whether the file carries no bytes.
    pub const fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Address of the root chunk.
    pub const fn root(&self) -> &ChunkAddress {
        match self {
            Self::Plain(file) => file.root(),
            Self::Encrypted(file) => file.root(),
        }
    }

    /// Reference layout of the opened tree.
    pub const fn mode(&self) -> Mode {
        match self {
            Self::Plain(_) => Mode::Plain,
            Self::Encrypted(_) => Mode::Encrypted,
        }
    }
}

impl<S, const B: usize> From<File<S, Plain, B>> for AnyFile<S, B> {
    fn from(file: File<S, Plain, B>) -> Self {
        Self::Plain(file)
    }
}

impl<S, const B: usize> From<File<S, Encrypted, B>> for AnyFile<S, B> {
    fn from(file: File<S, Encrypted, B>) -> Self {
        Self::Encrypted(file)
    }
}

/// Fetch the root envelope, insisting the store answered for the requested
/// address.
async fn fetch_root<S, const B: usize>(
    store: &S,
    address: ChunkAddress,
) -> Result<
    <AnyChunkSet<B> as nectar_primitives::chunk::ChunkRegistry>::Envelope,
    OpenError<S::Error>,
>
where
    S: TrustedGet<AnyChunkSet<B>>,
{
    let chunk = store
        .get(&address)
        .await
        .map_err(|source| OpenError::Fetch { address, source })?;
    let returned = *chunk.address();
    if returned != address {
        return Err(OpenError::AddressMismatch {
            requested: address,
            returned,
        });
    }
    Ok(chunk.into_envelope())
}

/// Span-header keystream counter: the body's 32-byte block count, one past
/// the body's own keystream.
fn span_counter(body_size: usize) -> u32 {
    let blocks = body_size
        .checked_div(EncryptionKey::SIZE)
        .unwrap_or_default();
    // The profile guard pins the body size within u32.
    u32::try_from(blocks).unwrap_or(u32::MAX)
}
