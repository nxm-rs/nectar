//! The in-memory manifest node: an optional root extension over a fork
//! table.
//!
//! Magic, version and every bound come from `F`; no flags are stored, the
//! presence bits are derived from the structure at encode time.

use crate::fork::ForkTable;
use crate::format::{Format, V1};
use crate::meta::Metadata;
use crate::value::Entry;

/// The root extension: what a trie root carries about itself, complete in
/// the root's own bytes.
///
/// At least one part is always present; an absent extension is
/// `Option<RootExtension>` on the node, so no in-band empty exists. Only a
/// trie root may carry one; the fetching party enforces that on fetch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RootExtension<F: Format = V1> {
    /// The value at the empty key.
    Entry(Entry<F>),
    /// Manifest-level metadata.
    Metadata(Metadata<F>),
    /// Both the empty-key value and manifest-level metadata.
    Both {
        /// The value at the empty key.
        entry: Entry<F>,
        /// Manifest-level metadata.
        metadata: Metadata<F>,
    },
}

impl<F: Format> RootExtension<F> {
    /// Assemble from parts; `None` when both are absent.
    #[must_use]
    pub fn new(entry: Option<Entry<F>>, metadata: Option<Metadata<F>>) -> Option<Self> {
        match (entry, metadata) {
            (Some(entry), Some(metadata)) => Some(Self::Both { entry, metadata }),
            (Some(entry), None) => Some(Self::Entry(entry)),
            (None, Some(metadata)) => Some(Self::Metadata(metadata)),
            (None, None) => None,
        }
    }

    /// The value at the empty key.
    #[must_use]
    pub const fn entry(&self) -> Option<&Entry<F>> {
        match self {
            Self::Entry(entry) | Self::Both { entry, .. } => Some(entry),
            Self::Metadata(_) => None,
        }
    }

    /// The manifest-level metadata.
    #[must_use]
    pub const fn metadata(&self) -> Option<&Metadata<F>> {
        match self {
            Self::Metadata(metadata) | Self::Both { metadata, .. } => Some(metadata),
            Self::Entry(_) => None,
        }
    }
}

impl<F: Format> From<Entry<F>> for RootExtension<F> {
    fn from(entry: Entry<F>) -> Self {
        Self::Entry(entry)
    }
}

impl<F: Format> From<Metadata<F>> for RootExtension<F> {
    fn from(metadata: Metadata<F>) -> Self {
        Self::Metadata(metadata)
    }
}

/// One in-memory manifest node of format `F`.
///
/// The wire preamble is `F::PREAMBLE`, carried by the type, not the value.
/// A node reached through a fork child must have no root extension; the
/// fetching party knows the context and enforces that on fetch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Node<F: Format = V1> {
    root: Option<RootExtension<F>>,
    forks: ForkTable<F>,
}

impl<F: Format> Node<F> {
    /// The empty-map root: no extension, no forks.
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            root: None,
            forks: ForkTable::new(),
        }
    }

    /// A node from its parts.
    #[must_use]
    pub const fn new(root: Option<RootExtension<F>>, forks: ForkTable<F>) -> Self {
        Self { root, forks }
    }

    /// Returns `true` for the empty-map root.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.root.is_none() && self.forks.is_empty()
    }

    /// The root extension.
    #[must_use]
    pub const fn root(&self) -> Option<&RootExtension<F>> {
        self.root.as_ref()
    }

    /// Set or clear the root extension.
    pub fn set_root(&mut self, root: Option<RootExtension<F>>) {
        self.root = root;
    }

    /// The value at the empty key.
    #[must_use]
    pub const fn entry(&self) -> Option<&Entry<F>> {
        match &self.root {
            Some(root) => root.entry(),
            None => None,
        }
    }

    /// The manifest-level metadata.
    #[must_use]
    pub const fn metadata(&self) -> Option<&Metadata<F>> {
        match &self.root {
            Some(root) => root.metadata(),
            None => None,
        }
    }

    /// The fork table.
    #[must_use]
    pub const fn forks(&self) -> &ForkTable<F> {
        &self.forks
    }

    /// Mutable access to the fork table.
    pub const fn forks_mut(&mut self) -> &mut ForkTable<F> {
        &mut self.forks
    }
}

impl<F: Format> Default for Node<F> {
    fn default() -> Self {
        Self::empty()
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use nectar_primitives::{ChunkAddress, ChunkRef};

    use crate::bounded::Prefix;
    use crate::meta::KeyId;

    use super::*;

    fn entry() -> Entry {
        ChunkRef::new(ChunkAddress::new([1; 32])).into()
    }

    fn metadata() -> Metadata {
        Metadata::new(
            KeyId::WebsiteIndexDocument,
            Bytes::from_static(b"index.html"),
        )
        .unwrap()
    }

    #[test]
    fn the_empty_map_root_is_the_default() {
        let node: Node = Node::default();
        assert!(node.is_empty());
        assert_eq!(node, Node::empty());
        assert_eq!(node.root(), None);
        assert_eq!(node.entry(), None);
        assert_eq!(node.metadata(), None);
        assert!(node.forks().is_empty());
    }

    #[test]
    fn root_extension_requires_at_least_one_part() {
        assert_eq!(RootExtension::<V1>::new(None, None), None);

        let only_entry = RootExtension::new(Some(entry()), None).unwrap();
        assert_eq!(only_entry.entry(), Some(&entry()));
        assert_eq!(only_entry.metadata(), None);

        let only_meta = RootExtension::new(None, Some(metadata())).unwrap();
        assert_eq!(only_meta.entry(), None);
        assert_eq!(only_meta.metadata(), Some(&metadata()));

        let both = RootExtension::new(Some(entry()), Some(metadata())).unwrap();
        assert_eq!(both.entry(), Some(&entry()));
        assert_eq!(both.metadata(), Some(&metadata()));
    }

    #[test]
    fn node_reads_through_the_root_extension() {
        let mut node = Node::new(
            RootExtension::new(Some(entry()), Some(metadata())),
            ForkTable::new(),
        );
        assert!(!node.is_empty());
        assert_eq!(node.entry(), Some(&entry()));
        assert_eq!(node.metadata(), Some(&metadata()));

        node.set_root(None);
        assert!(node.is_empty());
    }

    #[test]
    fn forks_make_a_node_non_empty() {
        let mut node = Node::empty();
        node.forks_mut()
            .insert(Prefix::try_from(&b"a"[..]).unwrap(), entry().into(), None)
            .unwrap();
        assert!(!node.is_empty());
        assert_eq!(node.forks().len(), 1);
    }
}
