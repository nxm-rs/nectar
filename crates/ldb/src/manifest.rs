//! The [`Manifest`] seam over the key-value database, read through its folder
//! view.
//!
//! One store serves both roles: the trie nodes and the entry data behind a
//! reference. A key bound to inline bytes carries its own data, so a load of
//! it never reaches the file pipeline.

use alloc::boxed::Box;
use alloc::vec::Vec;
use core::future::Future;

use bytes::Bytes;
use nectar_file::read::AnyFile;
use nectar_manifest::{
    DataSink, ListEntry, Listing, Manifest, ManifestMetadata, ManifestOp, ManifestPath, SinkError,
    WellKnownKey,
};
use nectar_primitives::chunk::ContentOnlyChunkSet;
use nectar_primitives::store::{BoxedError, ChunkPut, MaybeSend, MaybeSync, TrustedGet};
use nectar_primitives::EntryRef;

use crate::apply::{ApplyError, Changeset, apply};
use crate::error::{MetadataTooLong, NotAReference};
use crate::folder::DirEntry;
use crate::format::{Format, V1};
use crate::meta::{KeyId, Metadata};
use crate::node::NodeRef;
use crate::reader::{Reader, ReaderError};
use crate::store::{Plaintext, Seal};
use crate::value::{Entry, Key};

/// A failure crossing the manifest seam.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum ManifestError {
    /// A lookup or a listing walk failed.
    #[error(transparent)]
    Read(#[from] ReaderError),
    /// Folding the batch into a new root failed.
    #[error(transparent)]
    Apply(#[from] ApplyError),
    /// The rebuilt metadata block exceeded the format's bound.
    #[error(transparent)]
    Metadata(#[from] MetadataTooLong),
    /// The entry is bound to inline bytes where a reference was required.
    #[error(transparent)]
    NotAReference(#[from] NotAReference),
    /// No entry is bound at the requested path.
    #[error("no entry at {path:?}")]
    NotFound {
        /// The path that resolved to nothing.
        path: ManifestPath,
    },
    /// Reading the entry's data through the file pipeline failed.
    #[error("load entry data")]
    Data(#[source] BoxedError),
    /// Writing into the sink failed.
    #[error("write into the sink")]
    Sink(#[source] BoxedError),
}

impl ManifestError {
    /// Box a data-side failure behind the seam.
    fn data<E: core::error::Error + MaybeSend + MaybeSync + 'static>(error: E) -> Self {
        Self::Data(Box::new(error))
    }

    /// Box a sink failure behind the seam.
    fn sink<E: core::error::Error + MaybeSend + MaybeSync + 'static>(error: E) -> Self {
        Self::Sink(Box::new(error))
    }
}

/// The key-value database as a [`Manifest`], keyed by path.
///
/// The seal is the write-side secret: a plaintext database seals with
/// [`Plaintext`], an encrypted one with the sealer that carries the secret the
/// base tree was built under. Reads need no such state, because an encrypted
/// reference carries its own key.
#[derive(Clone, Copy, Debug)]
pub struct LdbManifest<S, K = Plaintext> {
    store: S,
    seal: K,
}

impl<S, K> LdbManifest<S, K> {
    /// A manifest over `store`, publishing rewritten nodes through `seal`.
    pub const fn new(store: S, seal: K) -> Self {
        Self { store, seal }
    }

    /// The backing store.
    pub const fn store(&self) -> &S {
        &self.store
    }

    /// The write-side sealer.
    pub const fn seal(&self) -> &K {
        &self.seal
    }
}

impl<S> LdbManifest<S, Plaintext> {
    /// A plaintext manifest over `store`.
    pub const fn plain(store: S) -> Self {
        Self::new(store, Plaintext)
    }
}

impl<S, K, R> Manifest<R> for LdbManifest<S, K>
where
    S: TrustedGet<ContentOnlyChunkSet> + ChunkPut + Clone + MaybeSend + MaybeSync + 'static,
    K: Seal<R> + MaybeSend + MaybeSync,
    R: NodeRef,
{
    /// The database's metadata: the typed key registry, absent as `None`.
    type Metadata = Option<Metadata<V1>>;

    type Error = ManifestError;

    fn list(
        &self,
        root: &R,
        dir: &ManifestPath,
    ) -> impl Future<Output = Result<Listing<R>, Self::Error>> + MaybeSend {
        let root = root.clone();
        let key = Key::from(dir.as_bytes());
        async move {
            let reader: Reader<_, V1, R> = Reader::new(&self.store);
            let mut listing = reader.list(&root, &key).await?;
            let mut entries = Vec::new();
            while let Some(item) = listing.next().await? {
                entries.push(listed(item));
            }
            Ok(Listing::new(entries))
        }
    }

    fn load<T: DataSink<Error: SinkError> + MaybeSend>(
        &self,
        root: &R,
        path: &ManifestPath,
        sink: &mut T,
    ) -> impl Future<Output = Result<(), Self::Error>> + MaybeSend {
        let root = root.clone();
        let path = path.clone();
        async move {
            let reader: Reader<_, V1, R> = Reader::new(&self.store);
            let entry = reader
                .get(&root, &Key::from(path.as_bytes()))
                .await?
                .ok_or_else(|| ManifestError::NotFound { path })?;
            match entry {
                Entry::Inline(value) => sink
                    .write_at(0, value.as_bytes())
                    .map_err(ManifestError::sink)?,
                bound => {
                    let reference = EntryRef::try_from(bound)?;
                    match AnyFile::open(self.store.clone(), reference)
                        .await
                        .map_err(ManifestError::data)?
                    {
                        AnyFile::Plain(file) => file.download().run(sink).await,
                        AnyFile::Encrypted(file) => file.download().run(sink).await,
                    }
                    .map_err(ManifestError::data)?;
                }
            }
            Ok(())
        }
    }

    fn apply(
        &self,
        base: &R,
        ops: impl IntoIterator<Item = ManifestOp<R, Self::Metadata>> + MaybeSend,
    ) -> impl Future<Output = Result<R, Self::Error>> + MaybeSend {
        // Staged before the first await: the changeset is the batch, and the
        // source iterator never crosses an await point.
        let mut changeset = Changeset::<V1>::new();
        for op in ops {
            match op {
                ManifestOp::Put {
                    path,
                    reference,
                    meta,
                } => {
                    changeset.put(
                        Key::from(path.as_bytes()),
                        Entry::from(reference.into_entry_ref()),
                        meta,
                    );
                }
                ManifestOp::Remove { path } => {
                    changeset.remove(Key::from(path.as_bytes()));
                }
            }
        }
        let base = base.clone();
        async move {
            Ok(apply::<S, V1, R, K>(&self.store, &self.seal, &base, &changeset).await?)
        }
    }

    fn metadata_from_view(
        &self,
        view: &dyn ManifestMetadata,
    ) -> Result<Self::Metadata, Self::Error> {
        let mut meta: Option<Metadata<V1>> = None;
        for (key, id) in [
            (WellKnownKey::ContentType, KeyId::ContentType),
            (WellKnownKey::IndexDocument, KeyId::WebsiteIndexDocument),
            (WellKnownKey::ErrorDocument, KeyId::WebsiteErrorDocument),
        ] {
            let Some(value) = view.get(&key) else {
                continue;
            };
            let value = Bytes::copy_from_slice(value.as_bytes());
            match meta.as_mut() {
                Some(block) => {
                    block.insert(id, value)?;
                }
                None => meta = Some(Metadata::new(id, value)?),
            }
        }
        Ok(meta)
    }
}

/// One folder-view child as a seam listing entry.
///
/// A key bound to inline bytes, or to a reference of the other width, still
/// names a path: it lists as a value rather than failing the listing.
fn listed<R: NodeRef>(entry: DirEntry<V1>) -> ListEntry<R> {
    match entry {
        DirEntry::Dir { key } => ListEntry::Dir {
            path: ManifestPath::new(key.as_bytes().to_vec()),
        },
        DirEntry::File { key, entry } => {
            let path = ManifestPath::new(key.as_bytes().to_vec());
            match EntryRef::try_from(entry).map(R::from_entry_ref) {
                Ok(Ok(reference)) => ListEntry::File { path, reference },
                Ok(Err(_)) | Err(_) => ListEntry::Value { path },
            }
        }
    }
}

/// The seam's separator is the format's own, so a path splits into the same
/// segments on either side of the trait.
const _: () = assert!(ManifestPath::SEPARATOR == <V1 as Format>::SEPARATOR);
