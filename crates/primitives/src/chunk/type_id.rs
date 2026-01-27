//! Chunk type identification
//!
//! This module provides the [`ChunkTypeId`] type for identifying chunk types
//! at the wire level (serialization/deserialization).

use core::fmt;

/// Wire-level chunk type identifier.
///
/// This type represents the type ID byte used in chunk headers for serialization
/// and deserialization dispatch. It provides type-safe constants for known chunk
/// types and supports custom types.
///
/// # Type ID Ranges
/// - `0-127`: Reserved for standard Swarm chunk types
/// - `128-255`: Available for custom/experimental chunk types
///
/// # Examples
///
/// ```
/// use nectar_primitives::ChunkTypeId;
///
/// // Use predefined constants
/// let content_type = ChunkTypeId::CONTENT;
/// let soc_type = ChunkTypeId::SINGLE_OWNER;
///
/// // Create custom type ID
/// let custom_type = ChunkTypeId::custom(200);
///
/// // Compare type IDs
/// assert_ne!(content_type, soc_type);
/// assert_eq!(content_type.as_u8(), 0);
/// ```
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct ChunkTypeId(u8);

impl ChunkTypeId {
    /// Content-addressed chunk type (CAC).
    ///
    /// These chunks have their address derived from the BMT hash of their content.
    pub const CONTENT: Self = Self(0);

    /// Single-owner chunk type (SOC).
    ///
    /// These chunks include owner identification and a digital signature.
    pub const SINGLE_OWNER: Self = Self(1);

    // Reserved type IDs for future standard types:
    // 2 - Encrypted chunk (planned)
    // 3 - Manifest chunk (planned)
    // 4-127 - Reserved for future standard types

    /// Create a new chunk type ID from a raw byte value.
    ///
    /// This is a const fn allowing use in const contexts.
    #[inline]
    pub const fn new(id: u8) -> Self {
        Self(id)
    }

    /// Create a custom chunk type ID.
    ///
    /// Custom types should use IDs in the range 128-255 to avoid conflicts
    /// with standard types.
    ///
    /// # Examples
    ///
    /// ```
    /// use nectar_primitives::ChunkTypeId;
    ///
    /// let custom = ChunkTypeId::custom(200);
    /// assert!(custom.is_custom());
    /// ```
    #[inline]
    pub const fn custom(id: u8) -> Self {
        Self(id)
    }

    /// Get the raw byte value of this type ID.
    #[inline]
    pub const fn as_u8(self) -> u8 {
        self.0
    }

    /// Check if this is a standard (reserved) type ID.
    ///
    /// Standard types have IDs in the range 0-127.
    #[inline]
    pub const fn is_standard(self) -> bool {
        self.0 < 128
    }

    /// Check if this is a custom type ID.
    ///
    /// Custom types have IDs in the range 128-255.
    #[inline]
    pub const fn is_custom(self) -> bool {
        self.0 >= 128
    }

    /// Get the human-readable name for known type IDs.
    ///
    /// Returns `None` for unknown or custom types.
    pub const fn name(self) -> Option<&'static str> {
        match self.0 {
            0 => Some("content"),
            1 => Some("single_owner"),
            _ => None,
        }
    }

    /// Get the abbreviated name for known type IDs.
    ///
    /// Returns common abbreviations like "CAC" for content-addressed chunks
    /// and "SOC" for single-owner chunks. Returns `None` for unknown or custom types.
    ///
    /// # Examples
    ///
    /// ```
    /// use nectar_primitives::ChunkTypeId;
    ///
    /// assert_eq!(ChunkTypeId::CONTENT.abbreviation(), Some("CAC"));
    /// assert_eq!(ChunkTypeId::SINGLE_OWNER.abbreviation(), Some("SOC"));
    /// assert_eq!(ChunkTypeId::custom(200).abbreviation(), None);
    /// ```
    pub const fn abbreviation(self) -> Option<&'static str> {
        match self.0 {
            0 => Some("CAC"),
            1 => Some("SOC"),
            _ => None,
        }
    }
}

impl fmt::Debug for ChunkTypeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.name() {
            Some(name) => write!(f, "ChunkTypeId::{}({})", name.to_uppercase(), self.0),
            None => write!(f, "ChunkTypeId({})", self.0),
        }
    }
}

impl fmt::Display for ChunkTypeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.name() {
            Some(name) => write!(f, "{}", name),
            None => write!(f, "custom({})", self.0),
        }
    }
}

impl From<u8> for ChunkTypeId {
    #[inline]
    fn from(id: u8) -> Self {
        Self(id)
    }
}

impl From<ChunkTypeId> for u8 {
    #[inline]
    fn from(id: ChunkTypeId) -> Self {
        id.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constants() {
        assert_eq!(ChunkTypeId::CONTENT.as_u8(), 0);
        assert_eq!(ChunkTypeId::SINGLE_OWNER.as_u8(), 1);
    }

    #[test]
    fn test_equality() {
        assert_eq!(ChunkTypeId::CONTENT, ChunkTypeId::new(0));
        assert_eq!(ChunkTypeId::SINGLE_OWNER, ChunkTypeId::new(1));
        assert_ne!(ChunkTypeId::CONTENT, ChunkTypeId::SINGLE_OWNER);
    }

    #[test]
    fn test_is_standard() {
        assert!(ChunkTypeId::CONTENT.is_standard());
        assert!(ChunkTypeId::SINGLE_OWNER.is_standard());
        assert!(ChunkTypeId::new(127).is_standard());
        assert!(!ChunkTypeId::new(128).is_standard());
        assert!(!ChunkTypeId::custom(200).is_standard());
    }

    #[test]
    fn test_is_custom() {
        assert!(!ChunkTypeId::CONTENT.is_custom());
        assert!(!ChunkTypeId::SINGLE_OWNER.is_custom());
        assert!(!ChunkTypeId::new(127).is_custom());
        assert!(ChunkTypeId::new(128).is_custom());
        assert!(ChunkTypeId::custom(200).is_custom());
    }

    #[test]
    fn test_name() {
        assert_eq!(ChunkTypeId::CONTENT.name(), Some("content"));
        assert_eq!(ChunkTypeId::SINGLE_OWNER.name(), Some("single_owner"));
        assert_eq!(ChunkTypeId::new(50).name(), None);
        assert_eq!(ChunkTypeId::custom(200).name(), None);
    }

    #[test]
    fn test_abbreviation() {
        assert_eq!(ChunkTypeId::CONTENT.abbreviation(), Some("CAC"));
        assert_eq!(ChunkTypeId::SINGLE_OWNER.abbreviation(), Some("SOC"));
        assert_eq!(ChunkTypeId::new(50).abbreviation(), None);
        assert_eq!(ChunkTypeId::custom(200).abbreviation(), None);
    }

    #[test]
    fn test_conversions() {
        let id: ChunkTypeId = 5u8.into();
        assert_eq!(id.as_u8(), 5);

        let byte: u8 = ChunkTypeId::CONTENT.into();
        assert_eq!(byte, 0);
    }

    #[test]
    fn test_debug_display() {
        assert_eq!(
            format!("{:?}", ChunkTypeId::CONTENT),
            "ChunkTypeId::CONTENT(0)"
        );
        assert_eq!(
            format!("{:?}", ChunkTypeId::SINGLE_OWNER),
            "ChunkTypeId::SINGLE_OWNER(1)"
        );
        assert_eq!(
            format!("{:?}", ChunkTypeId::custom(200)),
            "ChunkTypeId(200)"
        );

        assert_eq!(format!("{}", ChunkTypeId::CONTENT), "content");
        assert_eq!(format!("{}", ChunkTypeId::SINGLE_OWNER), "single_owner");
        assert_eq!(format!("{}", ChunkTypeId::custom(200)), "custom(200)");
    }

    #[test]
    fn test_hash() {
        use std::collections::HashSet;

        let mut set = HashSet::new();
        set.insert(ChunkTypeId::CONTENT);
        set.insert(ChunkTypeId::SINGLE_OWNER);
        set.insert(ChunkTypeId::custom(200));

        assert!(set.contains(&ChunkTypeId::CONTENT));
        assert!(set.contains(&ChunkTypeId::SINGLE_OWNER));
        assert!(set.contains(&ChunkTypeId::custom(200)));
        assert!(!set.contains(&ChunkTypeId::custom(201)));
    }
}
