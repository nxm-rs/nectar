//! Save-consuming builder for the mantaray write path.
//!
//! [`ManifestBuilder`] owns the only sanctioned route to wire bytes: entries
//! are staged in memory, then [`save`](ManifestBuilder::save) consumes the
//! builder, persists children before parents, and hands back the typed root
//! alongside a read handle over the same store. Because `save` takes `self`,
//! an unsaved builder holds no address and a saved one cannot be saved again.
//!
//! A root node carrying its own metadata is unrepresentable on the wire (only
//! fork children serialize metadata), so `save` rejects it rather than drop it
//! silently.
//!
//! ```
//! # use nectar_mantaray::{ManifestBuilder, DefaultMemoryStore};
//! # use nectar_primitives::chunk::ChunkAddress;
//! # futures::executor::block_on(async {
//! let mut builder = ManifestBuilder::new(DefaultMemoryStore::new());
//! builder
//!     .add("index.html", ChunkAddress::from([1u8; 32]))
//!     .await
//!     .unwrap();
//! let (_root, mut manifest) = builder.save().await.unwrap();
//! let entry = manifest.lookup("index.html").await.unwrap();
//! assert_eq!(entry.address(), Some(&ChunkAddress::from([1u8; 32])));
//! # });
//! ```
//!
//! `save` consumes the builder, so a second save cannot compile:
//!
//! ```compile_fail
//! # use nectar_mantaray::{ManifestBuilder, DefaultMemoryStore};
//! # futures::executor::block_on(async {
//! let builder = ManifestBuilder::new(DefaultMemoryStore::new());
//! let _ = builder.save().await;
//! let _ = builder.save().await;
//! # });
//! ```

use std::collections::BTreeMap;

use nectar_primitives::bmt::DEFAULT_BODY_SIZE;
use nectar_primitives::chunk::{ChunkAddress, ChunkRef, Reference};
use nectar_primitives::file::{ChunkPutExt, ReadAt};
use nectar_primitives::store::{ChunkGet, ChunkPut, MaybeSend};

use crate::entry::Entry;
use crate::manifest::Manifest;
use crate::{MantarayError, Result};

/// Staging buffer for a mantaray manifest whose `save` consumes the builder.
///
/// The reference type parameter `R` fixes what `add` accepts and what `save`
/// returns (a plain [`ChunkAddress`] or an encrypted
/// [`ManifestRef`](crate::ManifestRef)), mirroring [`Manifest`].
#[derive(Debug)]
pub struct ManifestBuilder<S, R: Reference = ChunkRef, const BS: usize = DEFAULT_BODY_SIZE> {
    manifest: Manifest<S, R, BS>,
}

impl<S, const BS: usize> ManifestBuilder<S, ChunkRef, BS> {
    /// Create a new plain builder (no obfuscation, 32-byte refs).
    pub fn new(store: S) -> Self {
        Self {
            manifest: Manifest::new(store),
        }
    }
}

#[cfg(feature = "encryption")]
impl<S, const BS: usize> ManifestBuilder<S, nectar_primitives::EncryptedChunkRef, BS> {
    /// Create a new encrypted builder (random obfuscation key, 64-byte refs).
    pub fn new_encrypted(store: S) -> Self {
        Self {
            manifest: Manifest::new_encrypted(store),
        }
    }
}

impl<S, R: Reference, const BS: usize> ManifestBuilder<S, R, BS> {
    /// Access the underlying chunk store.
    pub const fn store(&self) -> &S {
        self.manifest.store()
    }

    /// Reject a root that carries its own metadata: the wire format serializes
    /// metadata only on fork children, so a root's would be dropped on encode.
    fn ensure_root_serializable(&self) -> Result<()> {
        if self.manifest.root().metadata().is_empty() {
            Ok(())
        } else {
            Err(MantarayError::RootMetadata)
        }
    }
}

impl<S: ChunkGet<BS>, R: Reference + MaybeSend, const BS: usize> ManifestBuilder<S, R, BS> {
    /// Stage a path with a typed reference.
    pub async fn add(&mut self, path: &str, reference: impl Into<R>) -> Result<()> {
        self.manifest.add(path, reference).await
    }

    /// Stage a path with a typed reference and metadata.
    pub async fn add_with_metadata(
        &mut self,
        path: &str,
        reference: impl Into<R>,
        metadata: BTreeMap<String, String>,
    ) -> Result<()> {
        self.manifest
            .add_with_metadata(path, reference, metadata)
            .await
    }

    /// Stage a path with a pre-built [`Entry`] (metadata + reference).
    pub async fn add_entry(&mut self, path: &str, entry: Entry) -> Result<()> {
        self.manifest.add_entry(path, entry).await
    }

    /// Remove a staged path.
    pub async fn remove(&mut self, path: &str) -> Result<()> {
        self.manifest.remove(path).await
    }

    /// Set the website index document on the root path metadata.
    ///
    /// The value lands on the `/` fork child, not the root node, so it stays
    /// serializable.
    pub async fn set_index_document(&mut self, filename: &str) -> Result<()> {
        self.manifest.set_index_document(filename).await
    }

    /// Set the website error document on the root path metadata.
    pub async fn set_error_document(&mut self, path: &str) -> Result<()> {
        self.manifest.set_error_document(path).await
    }
}

impl<S: ChunkGet<BS> + ChunkPut<BS>, const BS: usize> ManifestBuilder<S, ChunkRef, BS> {
    /// Split `data`, store its chunks, and stage the resulting root at `path`.
    ///
    /// One mode end to end: a plain builder splits in plain mode and stages a
    /// plain reference, so a plain-encrypted pairing cannot be expressed.
    pub async fn put_file<D: ReadAt + Sync>(&mut self, path: &str, data: D) -> Result<()> {
        let root = self.store().write_file(data).await?;
        self.add(path, root).await
    }

    /// Persist the plain manifest, consuming the builder.
    ///
    /// Returns the root chunk address and a read handle over the same store.
    pub async fn save(mut self) -> Result<(ChunkAddress, Manifest<S, ChunkRef, BS>)> {
        self.ensure_root_serializable()?;
        let root = self.manifest.save().await?;
        Ok((root, self.manifest))
    }
}

#[cfg(feature = "encryption")]
impl<S: ChunkGet<BS> + ChunkPut<BS>, const BS: usize>
    ManifestBuilder<S, nectar_primitives::EncryptedChunkRef, BS>
{
    /// Split `data` in encrypted mode, store its chunks, and stage the root at `path`.
    ///
    /// One mode end to end: an encrypted builder splits in encrypted mode and
    /// stages an encrypted reference, so a plain-encrypted pairing cannot be
    /// expressed. Drives the splitter directly rather than the deprecated
    /// `write_encrypted_file` ergonomic wrapper it supersedes.
    ///
    /// `data` is dropped before the first store await, mirroring `write_file`,
    /// so the returned future never holds the source across a suspension point.
    pub async fn put_file<D: ReadAt + Sync>(&mut self, path: &str, data: D) -> Result<()> {
        use nectar_primitives::file::{EncryptedParallelSplitter, FileError};
        let (root, chunks) = EncryptedParallelSplitter::<BS>::split_to_vec(&data)?;
        drop(data);
        for chunk in chunks {
            self.store().put(chunk).await.map_err(FileError::store)?;
        }
        self.add(path, root).await
    }

    /// Persist the encrypted manifest, consuming the builder.
    ///
    /// Returns a [`ManifestRef`](crate::ManifestRef) and a read handle over the
    /// same store.
    pub async fn save(
        mut self,
    ) -> Result<(
        crate::ManifestRef,
        Manifest<S, nectar_primitives::EncryptedChunkRef, BS>,
    )> {
        self.ensure_root_serializable()?;
        let root = self.manifest.save().await?;
        Ok((root, self.manifest))
    }
}

#[cfg(test)]
mod tests {
    use futures::executor::block_on;
    use nectar_primitives::bmt::DEFAULT_BODY_SIZE;
    use nectar_primitives::chunk::ChunkAddress;
    use nectar_primitives::store::MemoryStore;

    use super::*;
    use crate::metadata;

    type Store = MemoryStore<DEFAULT_BODY_SIZE>;

    fn make_addr(s: &str) -> ChunkAddress {
        let bytes = s.as_bytes();
        let mut buf = [0u8; 32];
        let len = bytes.len().min(32);
        buf[..len].copy_from_slice(&bytes[..len]);
        ChunkAddress::from(buf)
    }

    #[test]
    fn save_returns_root_and_read_handle() {
        let mut builder: ManifestBuilder<Store> = ManifestBuilder::new(Store::new());
        let paths = &["index.html", "img/1.png", "img/2.png", "robots.txt"];
        for &path in paths {
            block_on(builder.add(path, make_addr(path))).unwrap();
        }

        let (root, manifest) = block_on(builder.save()).unwrap();
        assert_eq!(manifest.reference(), Some(&root));

        for &path in paths {
            let entry = block_on(manifest.get(path)).unwrap().unwrap();
            assert_eq!(entry.address(), Some(&make_addr(path)));
        }
    }

    #[test]
    fn empty_builder_saves_to_a_root() {
        let builder: ManifestBuilder<Store> = ManifestBuilder::new(Store::new());
        let (_root, manifest) = block_on(builder.save()).unwrap();
        assert!(block_on(manifest.entries()).unwrap().is_empty());
    }

    #[test]
    fn root_metadata_is_rejected_at_save() {
        let mut builder: ManifestBuilder<Store> = ManifestBuilder::new(Store::new());
        // The empty path targets the root node itself; attaching metadata there
        // produces the unserializable shape the guard must catch.
        let mut meta = BTreeMap::new();
        meta.insert("k".to_string(), "v".to_string());
        block_on(builder.add_with_metadata("", make_addr("root"), meta)).unwrap();

        let err = block_on(builder.save()).unwrap_err();
        assert!(matches!(err, MantarayError::RootMetadata));
    }

    #[test]
    fn root_entry_without_metadata_saves() {
        // A root value with no metadata is serializable (the entry slot lives
        // in the node header); only root metadata is rejected.
        let mut builder: ManifestBuilder<Store> = ManifestBuilder::new(Store::new());
        block_on(builder.add("", make_addr("root"))).unwrap();
        assert!(block_on(builder.save()).is_ok());
    }

    #[test]
    fn oversized_metadata_is_rejected_at_save() {
        let mut builder: ManifestBuilder<Store> = ManifestBuilder::new(Store::new());
        let mut meta = BTreeMap::new();
        meta.insert("k".to_string(), "a".repeat(usize::from(u16::MAX) + 16));
        block_on(builder.add_with_metadata("file", make_addr("file"), meta)).unwrap();

        let err = block_on(builder.save()).unwrap_err();
        assert!(matches!(err, MantarayError::MetadataTooLarge { .. }));
    }

    #[test]
    fn website_documents_round_trip_through_the_handle() {
        let mut builder: ManifestBuilder<Store> = ManifestBuilder::new(Store::new());
        block_on(builder.add("index.html", make_addr("index.html"))).unwrap();
        block_on(builder.set_index_document("index.html")).unwrap();
        block_on(builder.set_error_document("404.html")).unwrap();

        let (_root, manifest) = block_on(builder.save()).unwrap();
        let index = block_on(manifest.get(metadata::ROOT_PATH)).unwrap().unwrap();
        assert_eq!(
            index.metadata().get(metadata::WEBSITE_INDEX_DOCUMENT),
            Some(&"index.html".to_string())
        );
        assert_eq!(
            index.metadata().get(metadata::WEBSITE_ERROR_DOCUMENT),
            Some(&"404.html".to_string())
        );
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn encrypted_builder_round_trips_the_reference() {
        use nectar_primitives::file::EntryRef;
        use nectar_primitives::{EncryptedChunkRef, EncryptionKey};

        let mut builder = ManifestBuilder::<Store, EncryptedChunkRef>::new_encrypted(Store::new());
        let eref = EncryptedChunkRef::new(
            make_addr("index.html"),
            EncryptionKey::from([7u8; EncryptionKey::SIZE]),
        );
        block_on(builder.add("index.html", eref.clone())).unwrap();

        let (root, manifest) = block_on(builder.save()).unwrap();
        // The returned reference addresses the saved root.
        assert_eq!(manifest.reference(), Some(root.address()));

        // Address and decryption key both survive encode and decode.
        let entry = block_on(manifest.get("index.html")).unwrap().unwrap();
        assert_eq!(entry.reference(), Some(&EntryRef::Encrypted(eref)));
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn encrypted_builder_rejects_root_metadata() {
        use nectar_primitives::{EncryptedChunkRef, EncryptionKey};

        let mut builder = ManifestBuilder::<Store, EncryptedChunkRef>::new_encrypted(Store::new());
        let eref = EncryptedChunkRef::new(
            make_addr("root"),
            EncryptionKey::from([9u8; EncryptionKey::SIZE]),
        );
        let mut meta = BTreeMap::new();
        meta.insert("k".to_string(), "v".to_string());
        block_on(builder.add_with_metadata("", eref, meta)).unwrap();

        let err = block_on(builder.save()).unwrap_err();
        assert!(matches!(err, MantarayError::RootMetadata));
    }

    #[test]
    fn put_file_round_trips_through_read() {
        let mut builder: ManifestBuilder<Store> = ManifestBuilder::new(Store::new());
        // A multi-chunk payload exercises the splitter and joiner, not just a
        // single content chunk.
        let big = vec![0xabu8; DEFAULT_BODY_SIZE * 3 + 17];
        block_on(builder.put_file("a.txt", b"file A contents".to_vec())).unwrap();
        block_on(builder.put_file("dir/big.bin", big.clone())).unwrap();

        let (root, manifest) = block_on(builder.save()).unwrap();

        assert_eq!(
            block_on(manifest.read("a.txt")).unwrap().unwrap(),
            b"file A contents"
        );
        assert_eq!(
            block_on(manifest.read("dir/big.bin")).unwrap().unwrap(),
            big
        );
        // Absent path reads as None, not an error.
        assert!(block_on(manifest.read("missing.txt")).unwrap().is_none());

        // Reopen from storage: read drives the lazy-load path.
        let (_, store) = manifest.into_parts();
        let reopened = super::Manifest::<Store, ChunkRef>::open(root, store);
        assert_eq!(
            block_on(reopened.read("dir/big.bin")).unwrap().unwrap(),
            big
        );
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn encrypted_put_file_round_trips_through_read() {
        use nectar_primitives::EncryptedChunkRef;

        let mut builder = ManifestBuilder::<Store, EncryptedChunkRef>::new_encrypted(Store::new());
        let big = vec![0x5cu8; DEFAULT_BODY_SIZE * 2 + 5];
        block_on(builder.put_file("secret.txt", b"secret data".to_vec())).unwrap();
        block_on(builder.put_file("blob.bin", big.clone())).unwrap();

        let (_root, manifest) = block_on(builder.save()).unwrap();

        assert_eq!(
            block_on(manifest.read("secret.txt")).unwrap().unwrap(),
            b"secret data"
        );
        assert_eq!(block_on(manifest.read("blob.bin")).unwrap().unwrap(), big);
        assert!(block_on(manifest.read("absent")).unwrap().is_none());
    }

    #[test]
    fn remove_before_save_drops_the_path() {
        let mut builder: ManifestBuilder<Store> = ManifestBuilder::new(Store::new());
        block_on(builder.add("a.txt", make_addr("a"))).unwrap();
        block_on(builder.add("b.txt", make_addr("b"))).unwrap();
        block_on(builder.remove("a.txt")).unwrap();

        let (_root, manifest) = block_on(builder.save()).unwrap();
        let paths: Vec<_> = block_on(manifest.entries())
            .unwrap()
            .into_iter()
            .map(|e| e.path().to_vec())
            .collect();
        assert_eq!(paths, vec![b"b.txt".to_vec()]);
    }
}
