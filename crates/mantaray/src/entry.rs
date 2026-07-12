//! Manifest entry type: path, reference, and metadata.

use alloc::collections::BTreeMap;

use nectar_primitives::chunk::{ChunkAddress, Reference};
use nectar_primitives::file::EntryRef;

use crate::metadata;
use crate::node::Node;

/// A manifest entry: a path, typed reference, and optional metadata.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Entry {
    /// The path for this entry.
    pub(crate) path: Vec<u8>,
    /// The typed chunk reference (`None` for empty/non-value entries like root metadata).
    pub(crate) reference: Option<EntryRef>,
    /// Key-value metadata.
    pub(crate) metadata: BTreeMap<String, String>,
}

impl Entry {
    /// Create a new entry with the given chunk reference.
    pub fn new(reference: impl Into<EntryRef>) -> Self {
        Self {
            reference: Some(reference.into()),
            path: Vec::new(),
            metadata: BTreeMap::new(),
        }
    }

    /// The path for this entry.
    pub fn path(&self) -> &[u8] {
        &self.path
    }

    /// The typed chunk reference.
    pub const fn reference(&self) -> Option<&EntryRef> {
        self.reference.as_ref()
    }

    /// Key-value metadata.
    pub const fn metadata(&self) -> &BTreeMap<String, String> {
        &self.metadata
    }

    /// Set the content type (MIME type) metadata.
    pub fn with_content_type(mut self, ct: &str) -> Self {
        self.metadata
            .insert(metadata::CONTENT_TYPE.into(), ct.into());
        self
    }

    /// Set the filename metadata.
    pub fn with_filename(mut self, name: &str) -> Self {
        self.metadata.insert(metadata::FILENAME.into(), name.into());
        self
    }

    /// Set an arbitrary metadata key-value pair.
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Get the content type metadata value, if present.
    pub fn content_type(&self) -> Option<&str> {
        self.metadata
            .get(metadata::CONTENT_TYPE)
            .map(|s| s.as_str())
    }

    /// Get the filename metadata value, if present.
    pub fn filename(&self) -> Option<&str> {
        self.metadata.get(metadata::FILENAME).map(|s| s.as_str())
    }

    /// Path as a UTF-8 string. Returns `None` if the path is not valid UTF-8.
    pub fn path_str(&self) -> Option<&str> {
        core::str::from_utf8(&self.path).ok()
    }

    /// Chunk address from the reference.
    pub fn address(&self) -> Option<&ChunkAddress> {
        self.reference.as_ref().map(|r| r.address())
    }

    /// Reconstruct an `Entry` from a trie node and its accumulated path.
    ///
    /// Flows the node's typed reference straight into an [`EntryRef`], keeping
    /// `Entry` non-generic for the public API without a wire round-trip.
    pub(crate) fn from_node<R: Reference>(path: &[u8], node: &Node<R>) -> Self {
        let reference = node.entry().cloned().map(Reference::into_entry_ref);
        let metadata = if node.metadata().is_empty() {
            BTreeMap::new()
        } else {
            node.metadata().clone()
        };
        Self {
            path: path.to_vec(),
            reference,
            metadata,
        }
    }
}

impl From<ChunkAddress> for Entry {
    fn from(address: ChunkAddress) -> Self {
        Self::new(address)
    }
}

impl From<nectar_primitives::EncryptedChunkRef> for Entry {
    fn from(enc_ref: nectar_primitives::EncryptedChunkRef) -> Self {
        Self::new(enc_ref)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entry_builder() {
        let addr = ChunkAddress::from([1u8; 32]);
        let entry = Entry::new(addr)
            .with_content_type("image/png")
            .with_filename("logo.png")
            .with_metadata("custom-key", "custom-value");

        assert_eq!(entry.address(), Some(&addr));
        assert!(entry.path().is_empty());
        assert_eq!(entry.content_type(), Some("image/png"));
        assert_eq!(entry.filename(), Some("logo.png"));
        assert_eq!(
            entry.metadata().get("custom-key").map(|s| s.as_str()),
            Some("custom-value")
        );
    }

    #[test]
    fn entry_builder_no_metadata() {
        let addr = ChunkAddress::from([2u8; 32]);
        let entry = Entry::new(addr);
        assert_eq!(entry.content_type(), None);
        assert_eq!(entry.filename(), None);
        assert!(entry.metadata().is_empty());
    }
}
