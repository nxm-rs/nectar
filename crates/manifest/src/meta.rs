//! The erased metadata seam: enumerable and bidirectional.

use alloc::collections::BTreeMap;
use alloc::string::String;

use nectar_marker::{MaybeSend, MaybeSync};

/// A metadata key both formats spell, whatever each stores it as.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum WellKnownKey<'a> {
    /// MIME type of the entry's content.
    ContentType,
    /// Original file name of the entry's content.
    Filename,
    /// Site index document, root scope. Not an entry's metadata: set it
    /// through [`Batch::set_index_document`](crate::Batch::set_index_document).
    IndexDocument,
    /// Site error document, root scope.
    ErrorDocument,
    /// Any other key, by name.
    Custom(&'a str),
}

impl WellKnownKey<'_> {
    /// Every registered key: the one table the seam matches names against.
    pub const REGISTERED: [WellKnownKey<'static>; 4] = [
        WellKnownKey::ContentType,
        WellKnownKey::Filename,
        WellKnownKey::IndexDocument,
        WellKnownKey::ErrorDocument,
    ];

    /// The key's canonical name, in the reference client's spelling.
    #[must_use]
    pub const fn name(&self) -> &str {
        match self {
            Self::ContentType => "Content-Type",
            Self::Filename => "Filename",
            Self::IndexDocument => "website-index-document",
            Self::ErrorDocument => "website-error-document",
            Self::Custom(name) => name,
        }
    }

    /// The registered key `name` spells, matched ASCII-case-insensitively.
    #[must_use]
    pub fn registered(name: &str) -> Option<WellKnownKey<'static>> {
        Self::REGISTERED
            .into_iter()
            .find(|key| key.name().eq_ignore_ascii_case(name))
    }

    /// This key with a custom spelling of a registered name promoted to its
    /// variant.
    #[must_use]
    pub fn resolve(&self) -> Self {
        match *self {
            Self::Custom(name) => Self::registered(name).unwrap_or(Self::Custom(name)),
            key => key,
        }
    }
}

/// Read access to one entry's metadata, whatever the format stores: by key,
/// and enumerably. Values are bytes; text is a format's own convention.
#[auto_impl::auto_impl(&, Box)]
pub trait MetadataSource: MaybeSend + MaybeSync {
    /// The value bound to `key`, or `None` when no such key is carried.
    fn get(&self, key: &WellKnownKey<'_>) -> Option<&[u8]>;

    /// Call `f` once per carried pair, keyed by name in any registered
    /// spelling.
    fn for_each(&self, f: &mut dyn FnMut(&str, &[u8]));
}

/// A format's native metadata. A cross-format copy goes through
/// [`MetadataSource`], so the target rebuilds its own type from the source's
/// pairs and neither format names the other.
pub trait ManifestMeta: MetadataSource + Default + MaybeSend {
    /// Native metadata rebuilt from `source`; what the format cannot carry is
    /// its stated limit.
    fn from_source(source: &dyn MetadataSource) -> Self;
}

/// A non-empty metadata block whose format type is `Option<Self>`: the
/// orphan-safe route to [`ManifestMeta`], carried by the blanket impls below.
pub trait MetadataBlock: MetadataSource + Sized + MaybeSend {
    /// The block `source` rebuilds, or `None` when no pair crosses.
    fn from_source(source: &dyn MetadataSource) -> Option<Self>;
}

/// An absent block carries nothing.
impl<M: MetadataSource> MetadataSource for Option<M> {
    fn get(&self, key: &WellKnownKey<'_>) -> Option<&[u8]> {
        self.as_ref()?.get(key)
    }

    fn for_each(&self, f: &mut dyn FnMut(&str, &[u8])) {
        if let Some(block) = self {
            block.for_each(f);
        }
    }
}

impl<M: MetadataBlock> ManifestMeta for Option<M> {
    fn from_source(source: &dyn MetadataSource) -> Self {
        M::from_source(source)
    }
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

    /// Bind `key` to `value`, replacing any previous binding; a custom
    /// spelling of a registered name binds the registered slot.
    pub fn set(&mut self, key: WellKnownKey<'_>, value: impl Into<String>) -> &mut Self {
        self.pairs
            .insert(String::from(key.resolve().name()), value.into());
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

impl MetadataSource for MetadataView {
    fn get(&self, key: &WellKnownKey<'_>) -> Option<&[u8]> {
        self.pairs.get(key.resolve().name()).map(String::as_bytes)
    }

    fn for_each(&self, f: &mut dyn FnMut(&str, &[u8])) {
        for (key, value) in &self.pairs {
            f(key, value.as_bytes());
        }
    }
}

/// An entry with no metadata at all, for an insert that carries none.
impl MetadataSource for () {
    fn get(&self, _key: &WellKnownKey<'_>) -> Option<&[u8]> {
        None
    }

    fn for_each(&self, _f: &mut dyn FnMut(&str, &[u8])) {}
}

/// The trie's native metadata: a verbatim string map. `get` matches a
/// registered key under any spelling [`WellKnownKey::registered`] recognises.
impl MetadataSource for BTreeMap<String, String> {
    fn get(&self, key: &WellKnownKey<'_>) -> Option<&[u8]> {
        match key.resolve() {
            WellKnownKey::Custom(name) => Self::get(self, name).map(String::as_bytes),
            known => self.iter().find_map(|(name, value)| {
                (WellKnownKey::registered(name) == Some(known)).then_some(value.as_bytes())
            }),
        }
    }

    fn for_each(&self, f: &mut dyn FnMut(&str, &[u8])) {
        for (key, value) in self {
            f(key, value.as_bytes());
        }
    }
}

/// The map's stated limit is text: a registered key lands under its
/// canonical spelling and a non-UTF-8 value byte is replaced.
impl ManifestMeta for BTreeMap<String, String> {
    fn from_source(source: &dyn MetadataSource) -> Self {
        let mut map = Self::new();
        source.for_each(&mut |name, value| {
            let name = WellKnownKey::registered(name)
                .map_or_else(|| String::from(name), |known| String::from(known.name()));
            map.insert(name, String::from_utf8_lossy(value).into_owned());
        });
        map
    }
}
