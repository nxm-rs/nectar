//! The root-bound handle: a database read at one root, edited against one base.
//!
//! [`Database::at`] hands back a [`View`], [`Database::edit`] an [`Editor`]
//! whose `commit` yields the new root. Nothing reaches storage until a read or
//! a commit is awaited.

use core::marker::PhantomData;
use core::ops::RangeBounds;

use bytes::Bytes;

use nectar_primitives::ChunkRef;
use nectar_primitives::store::{ChunkPut, MaybeSync};

use crate::apply::{ApplyError, Changeset, apply};
use crate::folder::{Listing, Served, Website, dir_at};
use crate::format::{Format, V1};
use crate::meta::{Metadata, MetadataKey};
use crate::node::NodeRef;
use crate::reader::{Reader, ReaderError};
use crate::scan::{Cursor, half_open};
use crate::store::{NodeGet, Plaintext, Seal};
use crate::value::{Entry, Key};

/// A key-value database over one store, at whatever root a caller names.
///
/// Roots are values, not state: one database serves every root behind the same
/// store, and a write hands back the root it produced.
///
/// The seal is the write-side secret: [`Plaintext`], or the sealer carrying
/// the secret the base tree was built under. Reads need none, since an
/// encrypted reference carries its own key.
///
/// ```
/// use nectar_ldb::{Builder, Database, Entry, Key, Plaintext, V1};
/// use nectar_primitives::store::{ContentGet, MemoryStore};
/// use nectar_primitives::{ChunkAddress, ChunkRef};
///
/// # nectar_testing::run(async {
/// let store = ContentGet::new(MemoryStore::default());
/// let empty = *Builder::<V1>::new()
///     .build(&store, &Plaintext)
///     .await
///     .unwrap()
///     .root();
/// let db: Database<_> = Database::plain(&store);
/// let key = Key::from(&b"index.html"[..]);
/// let entry: Entry = ChunkRef::new(ChunkAddress::new([7; 32])).into();
///
/// // A write yields a new root; the base root stays as it was.
/// let root = db.insert(&empty, key.clone(), entry.clone()).await.unwrap();
/// assert_eq!(db.at(&root).get(&key).await.unwrap(), Some(entry));
/// assert!(!db.at(&empty).contains_key(&key).await.unwrap());
///
/// let mut writer = db.edit(&root);
/// writer.remove(key.clone());
/// let pruned = writer.commit().await.unwrap();
/// assert_eq!(pruned, empty);
/// # });
/// ```
#[derive(Clone, Copy, Debug, Default)]
pub struct Database<S, K = Plaintext, F: Format = V1> {
    store: S,
    seal: K,
    _format: PhantomData<F>,
}

impl<S, K, F: Format> Database<S, K, F> {
    /// A database over `store`, publishing rewritten nodes through `seal`.
    #[must_use]
    pub const fn new(store: S, seal: K) -> Self {
        Self {
            store,
            seal,
            _format: PhantomData,
        }
    }

    /// The backing store.
    #[must_use]
    pub const fn store(&self) -> &S {
        &self.store
    }

    /// The write-side sealer.
    #[must_use]
    pub const fn seal(&self) -> &K {
        &self.seal
    }

    /// Unwrap into the store and the sealer.
    #[must_use]
    pub fn into_parts(self) -> (S, K) {
        (self.store, self.seal)
    }

    /// The read view of the database rooted at `root`.
    #[must_use]
    pub fn at<R: NodeRef>(&self, root: &R) -> View<'_, S, F, R> {
        View::new(&self.store, root.clone())
    }

    /// A writer staging a batch against the database rooted at `base`.
    #[must_use]
    pub fn edit<R: NodeRef>(&self, base: &R) -> Editor<'_, S, K, F, R> {
        Editor {
            store: &self.store,
            seal: &self.seal,
            base: base.clone(),
            changeset: Changeset::new(),
        }
    }
}

impl<S, F: Format> Database<S, Plaintext, F> {
    /// A plaintext database over `store`.
    #[must_use]
    pub const fn plain(store: S) -> Self {
        Self::new(store, Plaintext)
    }
}

impl<S, K, F> Database<S, K, F>
where
    S: NodeGet + ChunkPut + MaybeSync,
    F: Format,
{
    /// Insert one key, clearing any metadata bound at it.
    ///
    /// To set metadata, go through [`edit`](Self::edit).
    pub async fn insert<R: NodeRef>(
        &self,
        root: &R,
        key: Key,
        entry: Entry<F>,
    ) -> Result<R, ApplyError>
    where
        K: Seal<R>,
    {
        let mut editor = self.edit(root);
        editor.insert(key, entry);
        editor.commit().await
    }

    /// Remove one key. Exact-key: nothing below `key` goes with it.
    pub async fn remove<R: NodeRef>(&self, root: &R, key: Key) -> Result<R, ApplyError>
    where
        K: Seal<R>,
    {
        let mut editor = self.edit(root);
        editor.remove(key);
        editor.commit().await
    }
}

/// The read view of a database, bound to one immutable root. A lookup handle,
/// not a cached snapshot.
#[derive(Debug)]
pub struct View<'a, S, F: Format = V1, R: NodeRef = ChunkRef> {
    store: &'a S,
    root: R,
    _format: PhantomData<F>,
}

impl<S, F: Format, R: NodeRef> Clone for View<'_, S, F, R> {
    fn clone(&self) -> Self {
        Self {
            store: self.store,
            root: self.root.clone(),
            _format: PhantomData,
        }
    }
}

impl<'a, S, F: Format, R: NodeRef> View<'a, S, F, R> {
    const fn new(store: &'a S, root: R) -> Self {
        Self {
            store,
            root,
            _format: PhantomData,
        }
    }

    /// The root the view reads from.
    #[must_use]
    pub const fn root(&self) -> &R {
        &self.root
    }

    /// The backing store.
    #[must_use]
    pub const fn store(&self) -> &'a S {
        self.store
    }
}

impl<S, F, R> View<'_, S, F, R>
where
    S: NodeGet + MaybeSync,
    F: Format,
    R: NodeRef,
{
    const fn reader(&self) -> Reader<&'_ S, F, R> {
        Reader::new(self.store)
    }

    /// The value bound to `key`. The empty key reads the database's own
    /// value.
    pub async fn get(&self, key: &Key) -> Result<Option<Entry<F>>, ReaderError> {
        self.reader().get(&self.root, key).await
    }

    /// Whether `key` is bound.
    pub async fn contains_key(&self, key: &Key) -> Result<bool, ReaderError> {
        self.reader().contains_key(&self.root, key).await
    }

    /// The metadata bound to `key`. The empty key reads the database's own
    /// manifest metadata, whether or not the root binds an entry.
    pub async fn metadata(&self, key: &Key) -> Result<Option<Metadata<F>>, ReaderError> {
        self.reader().metadata(&self.root, key).await
    }

    /// The greatest key `<= key`, with its value.
    pub async fn floor(&self, key: &Key) -> Result<Option<(Key, Entry<F>)>, ReaderError> {
        self.reader().floor(&self.root, key).await
    }

    /// The reference of the single chunk holding exactly the keys carrying
    /// `prefix`.
    pub async fn subtree(&self, prefix: &Key) -> Result<Option<R>, ReaderError> {
        self.reader().subtree(&self.root, prefix).await
    }

    /// The database's site-level document conventions.
    pub async fn website(&self) -> Result<Website, ReaderError> {
        self.reader().website(&self.root).await
    }

    /// Resolve a request path to the entry a website server would return.
    pub async fn serve(&self, path: &Key) -> Result<Served<F>, ReaderError> {
        self.reader().serve(&self.root, path).await
    }
}

impl<'a, S, F, R> View<'a, S, F, R>
where
    S: NodeGet + MaybeSync,
    F: Format,
    R: NodeRef,
{
    /// Every `(key, value)` in ascending key order. The walk outlives the
    /// view it was opened through.
    pub async fn iter(&self) -> Result<Cursor<'a, S, F, R>, ReaderError> {
        Cursor::seek(self.store, &self.root, &[], None).await
    }

    /// Every `(key, value)` within `bounds`, in ascending key order. Keys
    /// order as byte strings.
    pub async fn range(
        &self,
        bounds: impl RangeBounds<Key>,
    ) -> Result<Cursor<'a, S, F, R>, ReaderError> {
        let (start, end) = half_open(&bounds);
        Cursor::seek(self.store, &self.root, &start, end).await
    }

    /// Every `(key, value)` whose key starts with `prefix`, in ascending order.
    pub async fn prefix(&self, prefix: &Key) -> Result<Cursor<'a, S, F, R>, ReaderError> {
        let end = crate::scan::successor(prefix.as_bytes());
        Cursor::seek(self.store, &self.root, prefix.as_bytes(), end).await
    }

    /// The immediate children of the directory named by `dir` in key order,
    /// collapsing deeper keys at the next separator.
    pub async fn dir(&self, dir: &Key) -> Result<Listing<'a, S, F, R>, ReaderError> {
        dir_at(self.store, &self.root, dir).await
    }
}

/// The write handle of a database, bound to one base root.
///
/// Staging touches no storage: the ops accumulate in a [`Changeset`], and
/// [`commit`](Self::commit) folds the whole batch in one pass. The staging
/// order never reaches the produced root.
#[derive(Clone, Debug)]
pub struct Editor<'a, S, K, F: Format = V1, R: NodeRef = ChunkRef> {
    store: &'a S,
    seal: &'a K,
    base: R,
    changeset: Changeset<F>,
}

impl<S, K, F: Format, R: NodeRef> Editor<'_, S, K, F, R> {
    /// The base root the batch is staged against.
    #[must_use]
    pub const fn base(&self) -> &R {
        &self.base
    }

    /// The staged batch.
    #[must_use]
    pub const fn changeset(&self) -> &Changeset<F> {
        &self.changeset
    }

    /// Stage `key` bound to `entry` with no metadata.
    ///
    /// An insert replaces the whole binding, clearing existing metadata unless
    /// [`insert_with`](Self::insert_with) carries some.
    pub fn insert(&mut self, key: Key, entry: Entry<F>) -> &mut Self {
        self.insert_with(key, entry, None)
    }

    /// Stage `key` bound to `entry`, carrying `metadata`. On the empty key the
    /// metadata is the database's own manifest metadata.
    pub fn insert_with(
        &mut self,
        key: Key,
        entry: Entry<F>,
        metadata: impl Into<Option<Metadata<F>>>,
    ) -> &mut Self {
        self.changeset.insert(key, entry, metadata.into());
        self
    }

    /// Stage the removal of `key`.
    ///
    /// Exact-key: the key's own value and metadata go, and no other key does.
    /// The keys below it survive. An absent key is a no-op.
    pub fn remove(&mut self, key: Key) -> &mut Self {
        self.changeset.remove(key);
        self
    }

    /// Stage a merge of `key` into the database's own manifest metadata.
    ///
    /// A merge, not a replace: only `key` moves, and a `None` value clears it.
    /// It is the only write reaching that slot without binding the empty key.
    pub fn set_root_metadata(
        &mut self,
        key: impl Into<MetadataKey<F>>,
        value: Option<Bytes>,
    ) -> &mut Self {
        self.changeset.set_root_metadata(key, value);
        self
    }

    /// Number of staged updates.
    #[must_use]
    pub fn len(&self) -> usize {
        self.changeset.len()
    }

    /// Whether nothing is staged.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.changeset.is_empty()
    }
}

impl<S, K, F, R> Editor<'_, S, K, F, R>
where
    S: NodeGet + ChunkPut + MaybeSync,
    K: Seal<R>,
    F: Format,
    R: NodeRef,
{
    /// Fold the staged batch into the base root, returning the new root.
    ///
    /// The whole batch lands or none of it does. An empty batch returns the
    /// base root.
    pub async fn commit(self) -> Result<R, ApplyError> {
        apply(self.store, self.seal, &self.base, &self.changeset).await
    }
}
