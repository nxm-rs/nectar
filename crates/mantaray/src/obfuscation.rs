//! XOR obfuscation key for mantaray node serialisation.

/// 32-byte XOR obfuscation key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObfuscationKey([u8; 32]);

impl ObfuscationKey {
    /// Size of the obfuscation key in bytes.
    pub const SIZE: usize = size_of::<Self>();

    /// All-zero key (no obfuscation).
    pub const ZERO: Self = Self([0u8; 32]);

    /// Raw bytes of the key.
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Generate a random obfuscation key.
    #[cfg(feature = "std")]
    pub fn generate() -> Self {
        use rand::RngExt;
        let mut bytes = [0u8; 32];
        rand::rng().fill(&mut bytes);
        Self(bytes)
    }
}

impl From<[u8; 32]> for ObfuscationKey {
    fn from(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}

impl Default for ObfuscationKey {
    fn default() -> Self {
        Self::ZERO
    }
}

#[cfg(any(test, feature = "arbitrary"))]
impl<'a> arbitrary::Arbitrary<'a> for ObfuscationKey {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(Self(u.arbitrary::<[u8; 32]>()?))
    }
}
