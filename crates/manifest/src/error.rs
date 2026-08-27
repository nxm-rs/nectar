//! The seam-owned failure taxonomy shared by every manifest format.

use alloc::boxed::Box;

use nectar_marker::{MaybeSend, MaybeSync};
use nectar_primitives::store::BoxedError;

use crate::path::ManifestPath;
use crate::reserved::ReservedKey;

/// A failure crossing the manifest seam; `F` is the format's own union.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum ManifestError<F> {
    /// The batch staged a write at a key the map reserves, so none of it landed.
    #[error("the batch named a reserved key")]
    Reserved(#[from] ReservedKey),
    /// No entry is bound at the requested path.
    #[error("no entry at {0:?}")]
    NotFound(ManifestPath),
    /// The entry at the path names no data a load can reach.
    #[error("the entry at {0:?} names no data")]
    NoData(ManifestPath),
    /// Reading the entry's data through the file pipeline failed.
    #[error("load entry data")]
    Data(#[source] BoxedError),
    /// Writing into the sink failed.
    #[error("write into the sink")]
    Sink(#[source] BoxedError),
    /// The format's own failure.
    #[error(transparent)]
    Format(F),
}

/// A failure to assemble an entry's data in memory under a byte bound.
/// `F` is the error type a load on the view fails with.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum CollectError<F> {
    /// The entry outran the bound.
    ///
    /// The refusal names the end of the first frame past the bound: the
    /// entry's total size is not known up front, so that end is its size
    /// witness.
    #[error("the entry outruns the {max}-byte bound")]
    TooLarge {
        /// The end of the first frame past the bound, a lower bound on the
        /// entry's size.
        exceeds: u64,
        /// The bound the caller set.
        max: u64,
    },
    /// The load itself failed.
    #[error(transparent)]
    Load(F),
}

impl<F> CollectError<F> {
    /// Whether the entry outran the bound: a larger bound answers.
    #[must_use]
    pub const fn is_too_large(&self) -> bool {
        matches!(self, Self::TooLarge { .. })
    }

    /// Whether the load itself failed: whatever the bound, the outcome holds.
    #[must_use]
    pub const fn is_load_failure(&self) -> bool {
        matches!(self, Self::Load(_))
    }
}

/// The boxed format union an erased handle carries.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct ErasedFormat(#[from] pub(crate) BoxedError);

/// The seam failure of an erased handle.
pub type ErasedManifestError = ManifestError<ErasedFormat>;

impl<F> ManifestError<F> {
    /// The reserved key a refused batch names.
    #[must_use]
    pub const fn as_reserved(&self) -> Option<&ReservedKey> {
        match self {
            Self::Reserved(reserved) => Some(reserved),
            _ => None,
        }
    }

    /// Whether the requested path resolved to nothing.
    #[must_use]
    pub const fn is_not_found(&self) -> bool {
        matches!(self, Self::NotFound(_))
    }

    /// The same failure with the format union mapped through `map`.
    #[must_use]
    pub fn map_format<G>(self, map: impl FnOnce(F) -> G) -> ManifestError<G> {
        match self {
            Self::Reserved(reserved) => ManifestError::Reserved(reserved),
            Self::NotFound(path) => ManifestError::NotFound(path),
            Self::NoData(path) => ManifestError::NoData(path),
            Self::Data(source) => ManifestError::Data(source),
            Self::Sink(source) => ManifestError::Sink(source),
            Self::Format(source) => ManifestError::Format(map(source)),
        }
    }

    /// Box a data-side failure behind the seam.
    pub fn data<E: core::error::Error + MaybeSend + MaybeSync + 'static>(error: E) -> Self {
        Self::Data(Box::new(error))
    }

    /// Box a sink failure behind the seam.
    pub fn sink(error: impl Into<BoxedError>) -> Self {
        Self::Sink(error.into())
    }
}

/// Route a format's sources into [`ManifestError::Format`] so `?` converts them.
#[macro_export]
macro_rules! format_error_from {
    ($fmt:ty: $($src:ty),+ $(,)?) => {$(
        impl From<$src> for $crate::ManifestError<$fmt> {
            fn from(source: $src) -> Self {
                Self::Format(<$fmt>::from(source))
            }
        }
    )+};
}

#[cfg(test)]
mod tests {
    use alloc::string::ToString;

    use super::*;
    use crate::reserved::reserved_key;

    /// A format union stand-in with one source hop.
    #[derive(Debug, thiserror::Error)]
    #[error("union failed")]
    struct Union(#[source] ReservedKey);

    fn reserved() -> ReservedKey {
        ReservedKey::new(ManifestPath::from("/"))
    }

    #[test]
    fn the_seam_taxonomy_holds_typed_and_erased() {
        let refused = ManifestError::<Union>::Reserved(reserved());
        assert_eq!(refused.as_reserved(), Some(&reserved()));
        assert!(!refused.is_not_found());
        let missing = ManifestError::<Union>::NotFound(ManifestPath::from("a"));
        assert_eq!(missing.as_reserved(), None);
        assert!(missing.is_not_found());

        // `map_format` touches the format variant alone.
        let mapped = ManifestError::Format(Union(reserved())).map_format(|_| 7u8);
        assert!(matches!(mapped, ManifestError::Format(7)));
        assert_eq!(refused.map_format(|_| 7u8).as_reserved(), Some(&reserved()));

        // The format variant is transparent: no chain node of its own.
        let wrapped = ManifestError::Format(Union(reserved()));
        assert_eq!(wrapped.to_string(), "union failed");
        let source = core::error::Error::source(&wrapped);
        assert!(source.is_some_and(|s| s.downcast_ref::<ReservedKey>().is_some()));

        // The erased convenience still finds the reserved key through a box.
        let erased: ErasedManifestError =
            ManifestError::Reserved(reserved()).map_format(|u: Union| ErasedFormat(Box::new(u)));
        assert_eq!(erased.as_reserved(), Some(&reserved()));
        let boxed: BoxedError = Box::new(erased);
        let path = reserved_key(&*boxed).map(ReservedKey::path);
        assert_eq!(path, Some(&ManifestPath::from("/")));
    }

    /// New collect variants must be classified, so the match stays exhaustive;
    /// the predicates stay mutually exclusive per variant.
    #[test]
    fn every_collect_variant_is_classified_into_exactly_one_group() {
        let variants = [
            CollectError::<Union>::TooLarge { exceeds: 1, max: 0 },
            CollectError::Load(Union(reserved())),
        ];
        for error in &variants {
            match error {
                CollectError::TooLarge { .. } => {
                    assert!(error.is_too_large(), "a refusal is too large");
                    assert!(!error.is_load_failure(), "a refusal is not a load failure");
                }
                CollectError::Load(_) => {
                    assert!(!error.is_too_large(), "a load failure is not a refusal");
                    assert!(error.is_load_failure(), "a load failure is a load failure");
                }
            }
        }
    }
}
