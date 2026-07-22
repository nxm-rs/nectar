//! High-level publish and read: files in, root out; root in, bytes out.
//!
//! [`build_files`](crate::build_files) is the publish half: an iterator of
//! `(key, file)` streamed through BMT into one published root. This module is
//! the read half over the same store, resolving a looked-up entry all the way
//! back to file bytes through the streaming file reader, so a caller never
//! touches a chunk. Inline bytes return as-is; a reference is reassembled by
//! BMT, the same tree the builder split, so the round trip is byte-exact.

use bytes::Bytes;
use nectar_file::{CollectError, File, OpenError};
use nectar_primitives::ChunkAddress;
use nectar_primitives::store::{ChunkGet, MaybeSync};

use crate::format::Format;
use crate::reader::{Reader, ReaderError};
use crate::store::NodeGet;
use crate::value::{Entry, Key};

/// The store error of `S` over the standard registry.
type StoreError<S> = <S as ChunkGet>::Error;

/// A failure resolving a key or entry to its file bytes.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum FetchError<E> {
    /// Looking the key up in the manifest failed.
    #[error(transparent)]
    Read(#[from] ReaderError),
    /// Opening the referenced file at its root failed.
    #[error("open file")]
    Open(#[source] OpenError<E>),
    /// Assembling the referenced file from its chunks failed.
    #[error("collect file")]
    Collect(#[source] CollectError<E>),
    /// The entry names an encrypted file body. Per-reference node encryption
    /// (the `encryption` feature) seals and opens nodes; reassembling an
    /// encrypted file's own chunks is a separate decrypting reader the
    /// manifest does not carry.
    #[error("encrypted file body is not reassembled by the manifest")]
    Encrypted,
}

impl<S, F> Reader<S, F>
where
    S: NodeGet + MaybeSync + Clone + 'static,
    F: Format,
{
    /// Reassemble the full file bytes `entry` names.
    ///
    /// Inline bytes return directly; a plain reference is read from its BMT
    /// chunks over the reader's store. A ref64 names an encrypted file body,
    /// which the manifest does not reassemble.
    pub async fn read(&self, entry: &Entry<F>) -> Result<Bytes, FetchError<StoreError<S>>> {
        match entry {
            Entry::Inline(value) => Ok(value.clone().into_bytes()),
            Entry::Ref32(reference) => {
                let file = File::open(self.store().clone(), *reference.address())
                    .await
                    .map_err(FetchError::Open)?;
                let bytes = file.collect(u64::MAX).await.map_err(FetchError::Collect)?;
                Ok(Bytes::from(bytes))
            }
            Entry::Ref64(_) => Err(FetchError::Encrypted),
        }
    }

    /// Open the manifest at `root` and reassemble the file bound to `key`, or
    /// `None` when the key is absent.
    ///
    /// The read half of the publish round trip: a key looked up here yields the
    /// exact bytes [`build_files`](crate::build_files) streamed in under it.
    ///
    /// ```
    /// use nectar_testing::run;
    /// use nectar_manifest::{build_files, Key, Reader};
    /// use nectar_primitives::MemoryStore;
    ///
    /// let store = MemoryStore::default();
    /// let files = [(
    ///     Key::from(&b"index.html"[..]),
    ///     bytes::Bytes::from_static(b"<h1>hi</h1>"),
    /// )];
    /// let root = *run(build_files(&store, files)).unwrap().root();
    ///
    /// let reader: Reader<_> = Reader::new(store.clone());
    /// let page = run(reader.fetch(&root, &Key::from(&b"index.html"[..]))).unwrap();
    /// assert_eq!(page.as_deref(), Some(&b"<h1>hi</h1>"[..]));
    /// ```
    pub async fn fetch(
        &self,
        root: &ChunkAddress,
        key: &Key,
    ) -> Result<Option<Bytes>, FetchError<StoreError<S>>> {
        match self.get(root, key).await? {
            Some(entry) => Ok(Some(self.read(&entry).await?)),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use nectar_primitives::store::MemoryStore;
    use nectar_testing::run;

    use crate::builder::{Builder, build_files};
    use crate::value::{Entry, Key};

    use super::*;

    #[test]
    fn publish_then_fetch_round_trips_file_bytes() {
        let store = MemoryStore::default();
        let files = [
            (
                Key::from(&b"index.html"[..]),
                Bytes::from_static(b"<h1>hi</h1>"),
            ),
            (
                Key::from(&b"img/logo.png"[..]),
                Bytes::from(vec![0xAB; 9000]),
            ),
        ];
        let root = *run(build_files(&store, files)).unwrap().root();

        let reader: Reader<_> = Reader::new(store);
        assert_eq!(
            run(reader.fetch(&root, &Key::from(&b"index.html"[..]))).unwrap(),
            Some(Bytes::from_static(b"<h1>hi</h1>")),
        );
        // A multi-chunk file rejoins byte-exact through the shared BMT.
        assert_eq!(
            run(reader.fetch(&root, &Key::from(&b"img/logo.png"[..]))).unwrap(),
            Some(Bytes::from(vec![0xAB; 9000])),
        );
    }

    #[test]
    fn fetch_of_an_absent_key_is_none() {
        let store = MemoryStore::default();
        let files = [(Key::from(&b"a"[..]), Bytes::from_static(b"x"))];
        let root = *run(build_files(&store, files)).unwrap().root();

        let reader: Reader<_> = Reader::new(store);
        assert_eq!(
            run(reader.fetch(&root, &Key::from(&b"missing"[..]))).unwrap(),
            None,
        );
    }

    #[test]
    fn read_returns_inline_bytes_directly() {
        let store = MemoryStore::default();
        let mut builder = Builder::new();
        let value: Entry = Entry::inline(Bytes::from_static(b"inline")).unwrap();
        builder.insert(Key::from(&b"k"[..]), value, None);
        let root = *run(builder.build(&store)).unwrap().root();

        let reader: Reader<_> = Reader::new(store);
        assert_eq!(
            run(reader.fetch(&root, &Key::from(&b"k"[..]))).unwrap(),
            Some(Bytes::from_static(b"inline")),
        );
    }
}
