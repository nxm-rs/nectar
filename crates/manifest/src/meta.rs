//! Typed metadata: the u8 key registry plus the custom-key escape.
//!
//! Determinism is by rule, not by encoder behaviour: keys are sorted-unique,
//! registered names always travel as their one-byte id, and the encoded
//! length is bounded by `F::META_MAX` at construction.

use core::marker::PhantomData;
use core::mem::size_of;
use std::collections::BTreeMap;

use bytes::Bytes;

use crate::bounded::MetadataLen;
use crate::error::{CustomKeyError, MetadataTooLong};
use crate::format::{Format, V1};

/// Registered metadata key ids of registry version 1.
///
/// The registry is closed per format version: unassigned and reserved ids
/// are unrepresentable here, so a decoder rejects them by construction.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum KeyId {
    /// MIME type bytes of the entry's content.
    ContentType,
    /// Original file name bytes.
    Filename,
    /// Site index document path bytes (root scope).
    WebsiteIndexDocument,
    /// Site error document path bytes (root scope).
    WebsiteErrorDocument,
    /// Feed owner, 20 bytes (root scope).
    SwarmFeedOwner,
    /// Feed topic, 32 bytes (root scope).
    SwarmFeedTopic,
    /// Feed type bytes (root scope).
    SwarmFeedType,
}

impl KeyId {
    /// The one-byte wire id.
    #[must_use]
    pub const fn id(self) -> u8 {
        match self {
            Self::ContentType => 0x01,
            Self::Filename => 0x02,
            Self::WebsiteIndexDocument => 0x03,
            Self::WebsiteErrorDocument => 0x04,
            Self::SwarmFeedOwner => 0x05,
            Self::SwarmFeedTopic => 0x06,
            Self::SwarmFeedType => 0x07,
        }
    }

    /// The registered id for `id`, or `None` for unassigned and reserved
    /// values.
    #[must_use]
    pub const fn from_id(id: u8) -> Option<Self> {
        match id {
            0x01 => Some(Self::ContentType),
            0x02 => Some(Self::Filename),
            0x03 => Some(Self::WebsiteIndexDocument),
            0x04 => Some(Self::WebsiteErrorDocument),
            0x05 => Some(Self::SwarmFeedOwner),
            0x06 => Some(Self::SwarmFeedTopic),
            0x07 => Some(Self::SwarmFeedType),
            _ => None,
        }
    }

    /// The registered key name this id replaces on the wire.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::ContentType => "content-type",
            Self::Filename => "filename",
            Self::WebsiteIndexDocument => "website-index-document",
            Self::WebsiteErrorDocument => "website-error-document",
            Self::SwarmFeedOwner => "swarm-feed-owner",
            Self::SwarmFeedTopic => "swarm-feed-topic",
            Self::SwarmFeedType => "swarm-feed-type",
        }
    }

    /// The id registered for `name`, or `None` for unregistered names.
    #[must_use]
    pub const fn from_name(name: &[u8]) -> Option<Self> {
        match name {
            b"content-type" => Some(Self::ContentType),
            b"filename" => Some(Self::Filename),
            b"website-index-document" => Some(Self::WebsiteIndexDocument),
            b"website-error-document" => Some(Self::WebsiteErrorDocument),
            b"swarm-feed-owner" => Some(Self::SwarmFeedOwner),
            b"swarm-feed-topic" => Some(Self::SwarmFeedTopic),
            b"swarm-feed-type" => Some(Self::SwarmFeedType),
            _ => None,
        }
    }
}

/// An unregistered metadata key, carried behind the 0xFF escape.
///
/// Non-empty, at most `F::CKEY_MAX` bytes, and never equal to a registered
/// name: a registered name must travel as its id, so equality here would
/// fork the canonical encoding.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CustomKey<F: Format = V1> {
    bytes: Bytes,
    _format: PhantomData<F>,
}

impl<F: Format> CustomKey<F> {
    /// Wrap `bytes` as a custom key, rejecting the empty key, lengths above
    /// `F::CKEY_MAX`, and registered names.
    pub fn new(bytes: Bytes) -> Result<Self, CustomKeyError> {
        check_ckey::<F>(&bytes)?;
        Ok(Self {
            bytes,
            _format: PhantomData,
        })
    }

    /// Key length in bytes; always in `1..=F::CKEY_MAX`.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Always `false`: the empty key is rejected at construction.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        false
    }

    /// The key bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// The key bytes as shared [`Bytes`].
    #[must_use]
    pub fn into_bytes(self) -> Bytes {
        self.bytes
    }
}

/// Validity gate shared by the owned and copying constructors, checked
/// before any copy so a rejected slice never allocates.
const fn check_ckey<F: Format>(bytes: &[u8]) -> Result<(), CustomKeyError> {
    if bytes.is_empty() {
        return Err(CustomKeyError::Empty);
    }
    if bytes.len() > F::CKEY_MAX {
        return Err(CustomKeyError::TooLong {
            actual: bytes.len(),
            max: F::CKEY_MAX,
        });
    }
    if let Some(id) = KeyId::from_name(bytes) {
        return Err(CustomKeyError::Registered(id));
    }
    Ok(())
}

impl<F: Format> AsRef<[u8]> for CustomKey<F> {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl<F: Format> TryFrom<Bytes> for CustomKey<F> {
    type Error = CustomKeyError;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        Self::new(bytes)
    }
}

impl<F: Format> TryFrom<&[u8]> for CustomKey<F> {
    type Error = CustomKeyError;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        check_ckey::<F>(bytes)?;
        Ok(Self {
            bytes: Bytes::copy_from_slice(bytes),
            _format: PhantomData,
        })
    }
}

/// A metadata pair key: a registered id or a custom key.
///
/// The derived order is the wire order: registered ids ascending, then
/// custom keys ascending bytewise.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MetadataKey<F: Format = V1> {
    /// A registered key, one byte on the wire.
    Known(KeyId),
    /// An unregistered key behind the 0xFF escape.
    Custom(CustomKey<F>),
}

impl<F: Format> MetadataKey<F> {
    /// Encoded key bytes on the wire: the id, or escape + length + key.
    #[must_use]
    pub const fn encoded_len(&self) -> usize {
        match self {
            Self::Known(_) => size_of::<u8>(),
            Self::Custom(key) => size_of::<u8>()
                .saturating_add(size_of::<u8>())
                .saturating_add(key.len()),
        }
    }
}

impl<F: Format> From<KeyId> for MetadataKey<F> {
    fn from(id: KeyId) -> Self {
        Self::Known(id)
    }
}

impl<F: Format> From<CustomKey<F>> for MetadataKey<F> {
    fn from(key: CustomKey<F>) -> Self {
        Self::Custom(key)
    }
}

/// Typed metadata: sorted-unique pairs bounded by encoded length.
///
/// Non-empty by construction: an absent block is `Option<Metadata>` at the
/// use site, so no in-band empty ambiguity exists. Iteration order is the
/// wire order. Values are opaque bytes; fixed widths are registry
/// convention, not enforced here.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Metadata<F: Format = V1> {
    pairs: BTreeMap<MetadataKey<F>, Bytes>,
    len: MetadataLen<F>,
}

impl<F: Format> Metadata<F> {
    /// A block holding the single pair `key -> value`, rejecting an encoded
    /// length above `F::META_MAX`.
    pub fn new(key: impl Into<MetadataKey<F>>, value: Bytes) -> Result<Self, MetadataTooLong> {
        let key = key.into();
        let len = MetadataLen::new(pair_len(&key, &value))?;
        let mut pairs = BTreeMap::new();
        pairs.insert(key, value);
        Ok(Self { pairs, len })
    }

    /// Insert `key -> value`, replacing any existing value for `key` and
    /// returning it. Rejects the insert, leaving the block unchanged, when
    /// the encoded length would exceed `F::META_MAX`.
    pub fn insert(
        &mut self,
        key: impl Into<MetadataKey<F>>,
        value: Bytes,
    ) -> Result<Option<Bytes>, MetadataTooLong> {
        let key = key.into();
        let replaced = self.pairs.get(&key).map_or(0, |old| pair_len(&key, old));
        let total = self
            .len
            .get()
            .saturating_sub(replaced)
            .saturating_add(pair_len(&key, &value));
        self.len = MetadataLen::new(total)?;
        Ok(self.pairs.insert(key, value))
    }

    /// The value stored for `key`.
    #[must_use]
    pub fn get(&self, key: &MetadataKey<F>) -> Option<&Bytes> {
        self.pairs.get(key)
    }

    /// The pairs in wire order.
    pub fn iter(&self) -> impl Iterator<Item = (&MetadataKey<F>, &Bytes)> {
        self.pairs.iter()
    }

    /// Number of pairs; always at least one.
    #[must_use]
    pub fn pair_count(&self) -> usize {
        self.pairs.len()
    }

    /// Encoded length of the pairs; always at most `F::META_MAX`.
    #[must_use]
    pub const fn encoded_len(&self) -> MetadataLen<F> {
        self.len
    }
}

/// Encoded bytes of one pair: key encoding, the two-byte value length, and
/// the value. Saturating: an over-long value saturates past `F::META_MAX`
/// and is rejected by the [`MetadataLen`] bound.
const fn pair_len<F: Format>(key: &MetadataKey<F>, value: &Bytes) -> usize {
    key.encoded_len()
        .saturating_add(size_of::<u16>())
        .saturating_add(value.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL_IDS: [KeyId; 7] = [
        KeyId::ContentType,
        KeyId::Filename,
        KeyId::WebsiteIndexDocument,
        KeyId::WebsiteErrorDocument,
        KeyId::SwarmFeedOwner,
        KeyId::SwarmFeedTopic,
        KeyId::SwarmFeedType,
    ];

    fn custom(bytes: &[u8]) -> CustomKey {
        CustomKey::try_from(bytes).unwrap()
    }

    #[test]
    fn key_ids_round_trip_and_cover_the_registry() {
        for (offset, id) in ALL_IDS.into_iter().enumerate() {
            assert_eq!(id.id(), u8::try_from(offset).unwrap() + 1);
            assert_eq!(KeyId::from_id(id.id()), Some(id));
            assert_eq!(KeyId::from_name(id.name().as_bytes()), Some(id));
        }
    }

    #[test]
    fn key_ids_reject_reserved_and_unassigned() {
        for id in [0x00, 0x08, 0x7F, 0x80, 0xFE, 0xFF] {
            assert_eq!(KeyId::from_id(id), None);
        }
        assert_eq!(KeyId::from_name(b"x-custom"), None);
    }

    #[test]
    fn custom_key_bounds() {
        assert_eq!(
            CustomKey::<V1>::try_from(&b""[..]),
            Err(CustomKeyError::Empty)
        );

        let max = vec![0x78; V1::CKEY_MAX];
        assert_eq!(custom(&max).len(), V1::CKEY_MAX);

        let over = vec![0x78; V1::CKEY_MAX + 1];
        assert_eq!(
            CustomKey::<V1>::new(Bytes::from(over)),
            Err(CustomKeyError::TooLong {
                actual: V1::CKEY_MAX + 1,
                max: V1::CKEY_MAX
            })
        );
    }

    #[test]
    fn custom_key_rejects_registered_names() {
        for id in ALL_IDS {
            assert_eq!(
                CustomKey::<V1>::try_from(id.name().as_bytes()),
                Err(CustomKeyError::Registered(id))
            );
        }
    }

    #[test]
    fn key_id_order_tracks_the_wire_id() {
        // The canonical wire order of known pairs is ascending key_id, and the
        // encoder emits them in the derived enum order: the two MUST coincide
        // for every id, else a variant reorder silently drifts the address.
        let mut by_ord = ALL_IDS;
        by_ord.sort_unstable();
        let mut by_id = ALL_IDS;
        by_id.sort_unstable_by_key(|id| id.id());
        assert_eq!(by_ord, by_id);
        for pair in ALL_IDS.windows(2) {
            assert!(pair[0].id() < pair[1].id());
            assert!(MetadataKey::<V1>::from(pair[0]) < MetadataKey::from(pair[1]));
        }
    }

    #[test]
    fn metadata_key_order_is_the_wire_order() {
        let known_last: MetadataKey = KeyId::SwarmFeedType.into();
        let custom_first: MetadataKey = custom(b"a").into();
        assert!(known_last < custom_first);
        assert!(MetadataKey::<V1>::from(KeyId::ContentType) < MetadataKey::from(KeyId::Filename));
        assert!(MetadataKey::from(custom(b"a")) < MetadataKey::from(custom(b"ab")));
        assert!(MetadataKey::from(custom(b"ab")) < MetadataKey::from(custom(b"b")));
    }

    #[test]
    fn encoded_len_counts_exact_wire_bytes() {
        // known: id (1) + vlen (2) + value.
        let meta =
            Metadata::<V1>::new(KeyId::ContentType, Bytes::from_static(b"text/html")).unwrap();
        assert_eq!(meta.encoded_len().get(), 1 + 2 + 9);

        // custom: escape (1) + klen (1) + key + vlen (2) + value.
        let meta = Metadata::new(custom(b"note"), Bytes::from_static(b"hi")).unwrap();
        assert_eq!(meta.encoded_len().get(), 2 + 4 + 2 + 2);
    }

    #[test]
    fn insert_replaces_and_retotals() {
        let mut meta =
            Metadata::<V1>::new(KeyId::ContentType, Bytes::from_static(b"text/plain")).unwrap();
        let old = meta
            .insert(KeyId::ContentType, Bytes::from_static(b"text/html"))
            .unwrap();
        assert_eq!(old, Some(Bytes::from_static(b"text/plain")));
        assert_eq!(meta.pair_count(), 1);
        assert_eq!(meta.encoded_len().get(), 1 + 2 + 9);
        assert_eq!(
            meta.get(&KeyId::ContentType.into()),
            Some(&Bytes::from_static(b"text/html"))
        );
    }

    #[test]
    fn iteration_is_wire_order_regardless_of_insertion_order() {
        let mut meta = Metadata::new(custom(b"note"), Bytes::new()).unwrap();
        meta.insert(KeyId::Filename, Bytes::new()).unwrap();
        meta.insert(KeyId::ContentType, Bytes::new()).unwrap();
        let keys: Vec<_> = meta.iter().map(|(k, _)| k.clone()).collect();
        assert_eq!(
            keys,
            vec![
                KeyId::ContentType.into(),
                KeyId::Filename.into(),
                custom(b"note").into(),
            ]
        );
    }

    #[test]
    fn meta_max_bounds_the_block() {
        // One known pair saturating the block: 1 + 2 + value = META_MAX.
        let fit = Bytes::from(vec![0; V1::META_MAX - 3]);
        let mut meta = Metadata::<V1>::new(KeyId::ContentType, fit).unwrap();
        assert_eq!(meta.encoded_len().get(), V1::META_MAX);

        // Any further pair overflows; the block is left unchanged.
        let before = meta.clone();
        let err = meta.insert(KeyId::Filename, Bytes::new()).unwrap_err();
        assert_eq!(err.max, V1::META_MAX);
        assert_eq!(meta, before);

        let over = Bytes::from(vec![0; V1::META_MAX - 2]);
        assert!(Metadata::<V1>::new(KeyId::ContentType, over).is_err());
    }

    #[test]
    fn replacing_within_budget_at_the_boundary_is_accepted() {
        let fit = Bytes::from(vec![0; V1::META_MAX - 3]);
        let mut meta = Metadata::<V1>::new(KeyId::ContentType, fit.clone()).unwrap();
        // Same key, same size: replaced length is credited before the check.
        assert!(meta.insert(KeyId::ContentType, fit).is_ok());
        assert_eq!(meta.encoded_len().get(), V1::META_MAX);
    }
}
