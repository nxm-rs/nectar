//! Hasher-derived BMT root.

use alloy_primitives::B256;

/// A BMT root carrying hasher provenance in the type.
///
/// The field is private and the only constructor lives inside the bmt
/// module, so a held value was computed by
/// [`Hasher::sum_derived`](super::Hasher::sum_derived), never supplied.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DerivedAddress(B256);

impl DerivedAddress {
    /// Wrap a freshly computed root; unreachable outside the bmt module.
    pub(super) const fn new(root: B256) -> Self {
        Self(root)
    }
}

impl From<DerivedAddress> for B256 {
    fn from(derived: DerivedAddress) -> Self {
        derived.0
    }
}

#[cfg(test)]
mod tests {
    use super::super::Hasher;
    use super::*;

    #[test]
    fn derived_root_equals_sum() {
        let mut hasher: Hasher = Hasher::new();
        hasher.set_span(11);
        hasher.update(b"hello world");
        assert_eq!(B256::from(hasher.sum_derived()), hasher.sum());
    }

    /// A configured prefix participates: the derived value tracks the
    /// transformed root, exactly as `sum` does.
    #[test]
    fn prefixed_derivation_tracks_the_transformed_root() {
        let mut plain: Hasher = Hasher::new();
        plain.set_span(3);
        plain.update(b"foo");

        let mut prefixed: Hasher = Hasher::with_prefix(b"anchor");
        prefixed.set_span(3);
        prefixed.update(b"foo");

        assert_eq!(B256::from(prefixed.sum_derived()), prefixed.sum());
        assert_ne!(prefixed.sum_derived(), plain.sum_derived());
    }
}
