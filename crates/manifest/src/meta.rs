//! The erased metadata view: the one lossy point in the seam.
//!
//! Each format keeps its native metadata on the static path (a string map for
//! the trie, a typed key registry for the key-value format). Only the erased
//! path unifies them, and it does so through this well-known-key view, so what
//! crosses the seam is stated here rather than laundered through strings.

use alloc::collections::BTreeMap;
use alloc::string::String;

use nectar_marker::{MaybeSend, MaybeSync};

/// A metadata key both formats understand.
///
/// The three registered keys are the ones the manifest layer itself acts on. A
/// [`Custom`](Self::Custom) key travels by name and is looked up verbatim, but
/// only the registered keys cross the erased apply path, because the view
/// cannot be enumerated.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum WellKnownKey<'a> {
    /// MIME type of the entry's content.
    ContentType,
    /// Site index document, root scope. Not an entry's metadata: set it
    /// through [`MapWriter::with_index_document`].
    ///
    /// [`MapWriter::with_index_document`]: crate::MapWriter::with_index_document
    IndexDocument,
    /// Site error document, root scope.
    ErrorDocument,
    /// Any other key, by name.
    Custom(&'a str),
}

impl WellKnownKey<'_> {
    /// The key's canonical name; a custom key is its own name.
    #[must_use]
    pub const fn name(&self) -> &str {
        match self {
            Self::ContentType => "content-type",
            Self::IndexDocument => "website-index-document",
            Self::ErrorDocument => "website-error-document",
            Self::Custom(name) => name,
        }
    }
}

/// Read access to one entry's metadata, whatever the format stores.
///
/// The view answers by key and cannot be enumerated, so the erased apply path
/// reconstructs native metadata from the registered keys alone: a custom key,
/// or one the format cannot represent, is dropped there and nowhere else.
pub trait ManifestMetadata: MaybeSend + MaybeSync {
    /// The value bound to `key`, or `None` when the entry carries no such key.
    fn get(&self, key: &WellKnownKey<'_>) -> Option<&str>;
}

/// Metadata held in the seam's own vocabulary, keyed by canonical name.
///
/// The type a caller builds ops with when it holds an erased manifest and so
/// cannot name either format's metadata type.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MetadataView {
    pairs: BTreeMap<String, String>,
}

impl MetadataView {
    /// An empty view.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            pairs: BTreeMap::new(),
        }
    }

    /// Bind `key` to `value`, replacing any previous binding.
    pub fn set(&mut self, key: WellKnownKey<'_>, value: impl Into<String>) -> &mut Self {
        self.pairs.insert(String::from(key.name()), value.into());
        self
    }

    /// Bind `key` to `value`, by value, for building a view inline.
    #[must_use]
    pub fn with(mut self, key: WellKnownKey<'_>, value: impl Into<String>) -> Self {
        self.set(key, value);
        self
    }

    /// Whether the view binds nothing.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.pairs.is_empty()
    }

    /// The bound pairs, in canonical-name order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.pairs
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
    }
}

impl ManifestMetadata for MetadataView {
    fn get(&self, key: &WellKnownKey<'_>) -> Option<&str> {
        self.pairs.get(key.name()).map(String::as_str)
    }
}

/// An entry with no metadata at all, for an insert that carries none.
impl ManifestMetadata for () {
    fn get(&self, _key: &WellKnownKey<'_>) -> Option<&str> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_custom_key_is_looked_up_by_name() {
        let view = MetadataView::new()
            .with(WellKnownKey::ContentType, "text/html")
            .with(WellKnownKey::Custom("etag"), "abc");
        assert_eq!(view.get(&WellKnownKey::ContentType), Some("text/html"));
        assert_eq!(view.get(&WellKnownKey::Custom("etag")), Some("abc"));
        assert_eq!(view.get(&WellKnownKey::Custom("missing")), None);
    }

    #[test]
    fn the_content_type_key_is_not_the_custom_spelling_of_its_name() {
        // A custom key spelled as a registered name must resolve to the same
        // slot, or a round trip through the view would fork the vocabulary.
        let view = MetadataView::new().with(WellKnownKey::Custom("content-type"), "text/css");
        assert_eq!(view.get(&WellKnownKey::ContentType), Some("text/css"));
    }
}
