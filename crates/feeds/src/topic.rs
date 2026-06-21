//! Feed topic.

use alloy_primitives::{B256, keccak256};

/// A feed topic: a 32-byte value mixed into every update id.
///
/// The topic is used **raw** in id derivation. A caller that wants to derive a
/// topic from an arbitrary-length label (a string, a path, an application
/// namespace) must hash it first with [`Topic::from_bytes`]. [`Topic::new`]
/// wraps an already-32-byte value verbatim.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, derive_more::From, derive_more::Into)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Topic(pub B256);

impl Topic {
    /// Wrap a raw 32-byte topic value verbatim.
    pub const fn new(raw: B256) -> Self {
        Self(raw)
    }

    /// Derive a topic by hashing arbitrary input with keccak256.
    pub fn from_bytes(input: impl AsRef<[u8]>) -> Self {
        Self(keccak256(input))
    }

    /// The raw 32-byte topic value.
    pub const fn as_bytes(&self) -> &B256 {
        &self.0
    }
}

impl AsRef<[u8]> for Topic {
    fn as_ref(&self) -> &[u8] {
        self.0.as_slice()
    }
}
