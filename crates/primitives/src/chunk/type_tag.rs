//! Versioned chunk type tags
//!
//! This module pairs a [`ChunkTypeId`] with a [`ChunkVersion`] to form the
//! [`ChunkTypeTag`]: the registry, storage, and wire key for one chunk
//! acceptance rule.

use derive_more::{Display, From, Into};

use super::type_id::ChunkTypeId;

/// Revision of a chunk type id's acceptance rule.
///
/// Each `(id, version)` pair is a distinct, domain-separated predicate: a
/// changed derivation or signature scheme is a new pair, never an in-place
/// mutation of an existing one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into)]
#[display("{_0}")]
pub struct ChunkVersion(u8);

impl ChunkVersion {
    /// Construct from the raw version byte. `const` for static contexts.
    #[inline]
    pub const fn new(version: u8) -> Self {
        Self(version)
    }

    /// Raw version byte.
    #[inline]
    pub const fn as_u8(self) -> u8 {
        self.0
    }
}

/// Versioned chunk type key: a [`ChunkTypeId`] plus its [`ChunkVersion`].
///
/// All packed forms are big-endian, id byte first, so the packed integer,
/// the two-byte storage prefix, and database lexicographic order agree.
///
/// # Examples
///
/// ```
/// use nectar_primitives::{ChunkTypeId, ChunkTypeTag, ChunkVersion};
///
/// let tag = ChunkTypeTag::new(ChunkTypeId::SINGLE_OWNER, ChunkVersion::new(2));
/// assert_eq!(tag.to_u16(), 0x0102);
/// assert_eq!(tag.to_bytes(), [0x01, 0x02]);
/// assert_eq!(ChunkTypeTag::try_from(u32::from(tag)), Ok(tag));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ChunkTypeTag {
    /// Wire-level chunk type identifier.
    pub id: ChunkTypeId,
    /// Revision of the id's acceptance rule.
    pub version: ChunkVersion,
}

impl ChunkTypeTag {
    /// Construct a tag from an id and a version.
    #[inline]
    pub const fn new(id: ChunkTypeId, version: ChunkVersion) -> Self {
        Self { id, version }
    }

    /// Pack as `(id << 8) | version`; `const` twin of `u16::from` for the
    /// compile-time registry.
    #[inline]
    pub const fn to_u16(self) -> u16 {
        u16::from_be_bytes(self.to_bytes())
    }

    /// Big-endian two-byte form, used as the storage prefix.
    #[inline]
    pub const fn to_bytes(self) -> [u8; 2] {
        [self.id.as_u8(), self.version.as_u8()]
    }
}

impl From<[u8; 2]> for ChunkTypeTag {
    #[inline]
    fn from([id, version]: [u8; 2]) -> Self {
        Self::new(ChunkTypeId::new(id), ChunkVersion::new(version))
    }
}

impl From<ChunkTypeTag> for [u8; 2] {
    #[inline]
    fn from(tag: ChunkTypeTag) -> Self {
        tag.to_bytes()
    }
}

impl From<u16> for ChunkTypeTag {
    #[inline]
    fn from(packed: u16) -> Self {
        Self::from(packed.to_be_bytes())
    }
}

impl From<ChunkTypeTag> for u16 {
    #[inline]
    fn from(tag: ChunkTypeTag) -> Self {
        tag.to_u16()
    }
}

impl From<ChunkTypeTag> for u32 {
    #[inline]
    fn from(tag: ChunkTypeTag) -> Self {
        u32::from(tag.to_u16())
    }
}

/// Wire value exceeding the packed `u16` tag range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("tag wire value {0:#x} exceeds u16")]
pub struct TagWireError(pub u32);

impl TryFrom<u32> for ChunkTypeTag {
    type Error = TagWireError;

    #[inline]
    fn try_from(wire: u32) -> Result<Self, TagWireError> {
        u16::try_from(wire)
            .map(Self::from)
            .map_err(|_| TagWireError(wire))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_roundtrips_via_from_impls() {
        let v = ChunkVersion::new(7);
        assert_eq!(v.as_u8(), 7);
        assert_eq!(ChunkVersion::from(7u8), v);
        assert_eq!(u8::from(v), 7);
    }

    #[test]
    fn version_orders_numerically() {
        assert!(ChunkVersion::new(0) < ChunkVersion::new(1));
        assert!(ChunkVersion::new(1) < ChunkVersion::new(255));
    }

    #[test]
    fn version_display_is_decimal() {
        assert_eq!(format!("{}", ChunkVersion::new(200)), "200");
    }

    #[test]
    fn u16_packing_is_id_high_version_low() {
        let tag = ChunkTypeTag::new(ChunkTypeId::new(0xAB), ChunkVersion::new(0xCD));
        assert_eq!(tag.to_u16(), 0xABCD);
        assert_eq!(u16::from(tag), 0xABCD);
        assert_eq!(ChunkTypeTag::from(0xABCDu16), tag);
    }

    #[test]
    fn u16_roundtrip_is_total() {
        for packed in [0u16, 1, 0x0100, 0x01FF, 0x7F00, 0x8000, u16::MAX] {
            assert_eq!(u16::from(ChunkTypeTag::from(packed)), packed);
        }
    }

    #[test]
    fn bytes_are_big_endian_prefix() {
        let tag = ChunkTypeTag::new(ChunkTypeId::SINGLE_OWNER, ChunkVersion::new(3));
        assert_eq!(tag.to_bytes(), [0x01, 0x03]);
        assert_eq!(<[u8; 2]>::from(tag), [0x01, 0x03]);
        assert_eq!(ChunkTypeTag::from([0x01, 0x03]), tag);
        assert_eq!(tag.to_bytes(), tag.to_u16().to_be_bytes());
    }

    #[test]
    fn byte_order_matches_packed_integer_order() {
        let a = ChunkTypeTag::new(ChunkTypeId::CONTENT, ChunkVersion::new(255));
        let b = ChunkTypeTag::new(ChunkTypeId::SINGLE_OWNER, ChunkVersion::new(0));
        assert!(a.to_u16() < b.to_u16());
        assert!(a.to_bytes() < b.to_bytes());
    }

    #[test]
    fn wire_roundtrip() {
        let tag = ChunkTypeTag::new(ChunkTypeId::CONTENT, ChunkVersion::new(0));
        assert_eq!(u32::from(tag), 0);
        assert_eq!(ChunkTypeTag::try_from(0u32), Ok(tag));

        let tag = ChunkTypeTag::from(u16::MAX);
        assert_eq!(u32::from(tag), u32::from(u16::MAX));
        assert_eq!(ChunkTypeTag::try_from(u32::from(tag)), Ok(tag));
    }

    #[test]
    fn wire_above_u16_is_unsupported() {
        let wire = u32::from(u16::MAX) + 1;
        assert_eq!(ChunkTypeTag::try_from(wire), Err(TagWireError(wire)));
        assert!(ChunkTypeTag::try_from(0x0001_0000u32).is_err());
        assert!(ChunkTypeTag::try_from(u32::MAX).is_err());
    }

    #[test]
    fn distinct_versions_are_distinct_tags() {
        let v0 = ChunkTypeTag::new(ChunkTypeId::CONTENT, ChunkVersion::new(0));
        let v1 = ChunkTypeTag::new(ChunkTypeId::CONTENT, ChunkVersion::new(1));
        assert_ne!(v0, v1);
        assert_ne!(v0.to_u16(), v1.to_u16());
        assert_ne!(v0.to_bytes(), v1.to_bytes());
    }
}
