//! The root-bound handle: a database read at one root, edited against one base.
//!
//! A database is a map, so it speaks the map vocabulary. [`Database::at`] binds
//! a root and hands back a [`View`] to read it: `get`, `contains_key`, `range`,
//! `iter` and the folder reads over the same keys. [`Database::edit`] binds a
//! base root and hands back an [`Editor`] to stage `insert` and `remove` ops,
//! and its `commit` yields the new root, because the map itself is immutable.
//!
//! The handle is a root plus two borrows, so building one per lookup is free:
//! nothing reaches storage until a read or a commit is awaited. The store is
//! the read seam and the seal is the write secret, exactly as the free
//! [`apply`] takes them.

use core::marker::PhantomData;
use core::ops::RangeBounds;

use nectar_primitives::ChunkRef;
use nectar_primitives::store::{ChunkPut, MaybeSync};

use crate::apply::{ApplyError, Changeset, apply};
use crate::folder::{Listing, Served, Website, dir_at};
use crate::format::{Format, V1};
use crate::meta::Metadata;
use crate::node::NodeRef;
use crate::reader::{Reader, ReaderError};
use crate::scan::{Cursor, half_open};
use crate::store::{NodeGet, Plaintext, Seal};
use crate::value::{Entry, Key};

/// A key-value database over one store, at whatever root a caller names.
///
/// Roots are values, not state: one database serves every root behind the same
/// store, and a write hands back the root it produced rather than mutating the
/// one it started from.
///
/// The seal is the write-side secret: a plaintext database seals with
/// [`Plaintext`], an encrypted one with the sealer that carries the secret the
/// base tree was built under. Reads need no such state, because an encrypted
/// reference carries its own key.
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
/// // A write yields a new root, and the base root stays exactly as it was.
/// let root = db.insert(&empty, key.clone(), entry.clone()).await.unwrap();
/// assert_eq!(db.at(&root).get(&key).await.unwrap(), Some(entry));
/// assert!(!db.at(&empty).contains_key(&key).await.unwrap());
///
/// // A batch stages against one base and commits to one root.
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
    /// Insert one key into the database rooted at `root`, returning the new
    /// root.
    ///
    /// Sugar over an [`edit`](Self::edit) of one op; metadata rides the writer,
    /// so an insert that carries it goes through the handle.
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

    /// Remove one key from the database rooted at `root`, returning the new
    /// root.
    pub async fn remove<R: NodeRef>(&self, root: &R, key: Key) -> Result<R, ApplyError>
    where
        K: Seal<R>,
    {
        let mut editor = self.edit(root);
        editor.remove(key);
        editor.commit().await
    }
}

/// The read view of a database, bound to one immutable root.
///
/// Cheap: a store reference and a clone of the root. Every read descends from
/// that root, so a view is a lookup handle rather than a cached snapshot, and
/// two views on two roots never interfere.
#[derive(Debug)]
pub struct View<'a, S, F: Format = V1, R: NodeRef = ChunkRef> {
    store: &'a S,
    root: R,
    _format: PhantomData<F>,
}

/// Cloning a view clones the root and copies the store reference; the store
/// itself is never cloned, so a clone stays as cheap as the handle is.
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
    /// A view over `store` at `root`.
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
    /// The streaming reader the point reads descend through.
    const fn reader(&self) -> Reader<&'_ S, F, R> {
        Reader::new(self.store)
    }

    /// The value bound to `key`, or `None` when the key is absent.
    ///
    /// The root key, the separator alone, reads the database's own value.
    pub async fn get(&self, key: &Key) -> Result<Option<Entry<F>>, ReaderError> {
        self.reader().get(&self.root, key).await
    }

    /// Whether `key` is bound.
    pub async fn contains_key(&self, key: &Key) -> Result<bool, ReaderError> {
        self.reader().contains_key(&self.root, key).await
    }

    /// The metadata bound to `key`, or `None` when the key carries none.
    ///
    /// The root key reads the database's own manifest metadata, whether or not
    /// the root binds an entry.
    pub async fn metadata(&self, key: &Key) -> Result<Option<Metadata<F>>, ReaderError> {
        self.reader().metadata(&self.root, key).await
    }

    /// The greatest key `<= key` and its value, or `None` when every key is
    /// larger.
    pub async fn floor(&self, key: &Key) -> Result<Option<(Key, Entry<F>)>, ReaderError> {
        self.reader().floor(&self.root, key).await
    }

    /// The reference of the single chunk holding exactly the keys carrying
    /// `prefix`, so a directory can be handed off rather than walked.
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
    /// Every `(key, value)` in ascending key order.
    ///
    /// The walk keeps the view's own store reference, so it outlives the view
    /// it was opened through.
    pub async fn iter(&self) -> Result<Cursor<'a, S, F, R>, ReaderError> {
        Cursor::seek(self.store, &self.root, &[], None).await
    }

    /// Every `(key, value)` within `bounds`, in ascending key order.
    ///
    /// Keys order as byte strings, so every bound is exact: an excluded bound
    /// is the included one with a zero byte appended.
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
/// [`commit`](Self::commit) folds the whole batch in one pass. Keys accumulate
/// in key order, so the order they were staged in never reaches the produced
/// root.
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

    /// Stage `key` bound to `entry`, with metadata as a suffix.
    ///
    /// The op lands when the returned guard is dropped, which is the end of
    /// the statement, so an insert with no metadata needs nothing extra:
    /// `editor.insert(key, entry);` stages it, and
    /// `editor.insert(key, entry).meta(meta);` stages it with metadata.
    ///
    /// An insert replaces the whole binding; existing metadata is cleared
    /// unless [`meta`](Insert::meta) is given.
    pub const fn insert(&mut self, key: Key, entry: Entry<F>) -> Insert<'_, F> {
        Insert {
            changeset: &mut self.changeset,
            pending: Some((key, entry)),
            meta: None,
        }
    }

    /// Stage the removal of `key`.
    pub fn remove(&mut self, key: Key) -> &mut Self {
        self.changeset.remove(key);
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
    /// The whole batch lands or none of it does: the new root only exists once
    /// every rewritten node is written. An empty batch returns the base root.
    pub async fn commit(self) -> Result<R, ApplyError> {
        apply(self.store, self.seal, &self.base, &self.changeset).await
    }
}

/// A staged insert, awaiting the metadata it may carry.
///
/// [`Editor::insert`] hands one back so metadata reads as a suffix on the
/// insert it belongs to. The op is staged when the guard is dropped.
#[derive(Debug)]
pub struct Insert<'e, F: Format = V1> {
    changeset: &'e mut Changeset<F>,
    /// The staged key and value, taken by the drop that records them.
    pending: Option<(Key, Entry<F>)>,
    /// Metadata to attach; none by default.
    meta: Option<Metadata<F>>,
}

impl<F: Format> Insert<'_, F> {
    /// Attach `metadata` to the insert, replacing whatever the key carried.
    ///
    /// On the root key this is the database's own manifest metadata. A bare
    /// insert carries none, so it clears the key's metadata.
    pub fn meta(&mut self, metadata: Metadata<F>) -> &mut Self {
        self.meta = Some(metadata);
        self
    }
}

impl<F: Format> Drop for Insert<'_, F> {
    fn drop(&mut self) {
        if let Some((key, entry)) = self.pending.take() {
            self.changeset.insert(key, entry, self.meta.take());
        }
    }
}
