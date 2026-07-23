//! Structural-regime test-value generators.
//!
//! Each builder names the packing regime it drives, with every bound taken
//! from the format consts rather than restated. Generators stay deterministic
//! in `u`, so shrinking and replay work. Bridge to proptest by mapping a
//! byte-vector strategy through [`arbitrary::Unstructured`].

use arbitrary::{Arbitrary, Unstructured};
use bytes::Bytes;
use nectar_primitives::EncryptedChunkRef;

use crate::format::Format;
use crate::meta::{KeyId, Metadata};
use crate::value::{Entry, Key};

/// One insert row: key, entry, optional metadata.
pub type Row<F> = (Key, Entry<F>, Option<Metadata<F>>);

/// Least key length whose shared run chains through `links` full `PLEN_MAX`
/// forks: one byte past the links (511 and 766 bytes for two and three links
/// at the frozen 255-byte `PLEN_MAX`).
#[must_use]
pub const fn chain_threshold<F: Format>(links: usize) -> usize {
    F::PLEN_MAX.saturating_mul(links).saturating_add(1)
}

/// A short key over a five-symbol alphabet: dense shared prefixes, so a set
/// branches, compacts and re-branches.
pub fn branch_key(u: &mut Unstructured<'_>) -> arbitrary::Result<Key> {
    let len = u.int_in_range(1..=8usize)?;
    let mut bytes = Vec::with_capacity(len);
    for _ in 0..len {
        bytes.push(u.int_in_range(0..=4u8)?);
    }
    Ok(Key::from(bytes))
}

/// A low-entropy binary-alphabet key up to [`chain_threshold`]`(3)` bytes:
/// shared runs cross the two- and three-link chain thresholds, so compaction
/// must chain forks rather than stop one link short.
pub fn chain_key<F: Format>(u: &mut Unstructured<'_>) -> arbitrary::Result<Key> {
    let len = u.int_in_range(1..=chain_threshold::<F>(3))?;
    let mut bytes = Vec::with_capacity(len);
    for _ in 0..len {
        bytes.push(u.int_in_range(0..=1u8)?);
    }
    Ok(Key::from(bytes))
}

/// Two binary-alphabet keys sharing exactly `shared` leading bytes and
/// diverging on the next, pinning a fork boundary at that depth; pass a
/// [`chain_threshold`] to pin a full chain.
pub fn diverging_pair(u: &mut Unstructured<'_>, shared: usize) -> arbitrary::Result<(Key, Key)> {
    let mut left = Vec::with_capacity(shared.saturating_add(1));
    for _ in 0..shared {
        left.push(u.int_in_range(0..=1u8)?);
    }
    let mut right = left.clone();
    left.push(0);
    right.push(1);
    Ok((Key::from(left), Key::from(right)))
}

/// A ref64 entry: an encrypted child carrying its decryption key in-band.
pub fn encrypted_entry<F: Format>(u: &mut Unstructured<'_>) -> arbitrary::Result<Entry<F>> {
    Ok(Entry::from(EncryptedChunkRef::arbitrary(u)?))
}

/// Optional filler metadata up to the format's `META_MAX` block: the record
/// weight lever of the spill regime.
pub fn metadata<F: Format>(u: &mut Unstructured<'_>) -> arbitrary::Result<Option<Metadata<F>>> {
    if u.arbitrary::<bool>()? {
        filler_metadata(u, 0).map(Some)
    } else {
        Ok(None)
    }
}

/// Filler metadata between `min` filler bytes and the format's `META_MAX`
/// encoded block.
fn filler_metadata<F: Format>(
    u: &mut Unstructured<'_>,
    min: usize,
) -> arbitrary::Result<Metadata<F>> {
    // Encoded length adds the key byte and the value length prefix.
    let cap = F::META_MAX.saturating_sub(3);
    let len = u.int_in_range(min.min(cap)..=cap)?;
    let fill = u8::arbitrary(u)?;
    Metadata::new(KeyId::ContentType, Bytes::from(vec![fill; len]))
        .map_err(|_| arbitrary::Error::IncorrectFormat)
}

/// Up to 64 branching rows: short shared-prefix keys with mixed entries.
pub fn branch_rows<F: Format>(u: &mut Unstructured<'_>) -> arbitrary::Result<Vec<Row<F>>> {
    rows(u, 64, branch_key)
}

/// Up to eight chain rows: long low-entropy keys whose shared runs span
/// multiple `PLEN_MAX` links while the two-way fanout keeps nodes in budget.
pub fn chain_rows<F: Format>(u: &mut Unstructured<'_>) -> arbitrary::Result<Vec<Row<F>>> {
    rows(u, 8, chain_key::<F>)
}

/// Up to 400 wide, heavy rows: short full-alphabet keys with heavy metadata,
/// so a naive single-chunk node overruns the body budget and the table cuts
/// at `SEG_TARGET` into a directory of `CAP_FORK`-capped leaf segments.
pub fn spill_rows<F: Format>(u: &mut Unstructured<'_>) -> arbitrary::Result<Vec<Row<F>>> {
    let count = u.int_in_range(0..=400usize)?;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let len = u.int_in_range(1..=5usize)?;
        let mut bytes = Vec::with_capacity(len);
        for _ in 0..len {
            bytes.push(u8::arbitrary(u)?);
        }
        out.push((
            Key::from(bytes),
            Entry::arbitrary(u)?,
            Some(filler_metadata(u, 256)?),
        ));
    }
    Ok(out)
}

/// `max`-bounded rows over the given key regime.
fn rows<F: Format>(
    u: &mut Unstructured<'_>,
    max: usize,
    mut key: impl FnMut(&mut Unstructured<'_>) -> arbitrary::Result<Key>,
) -> arbitrary::Result<Vec<Row<F>>> {
    let count = u.int_in_range(0..=max)?;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        out.push((key(u)?, Entry::arbitrary(u)?, metadata(u)?));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::format::V1;

    #[test]
    fn chain_thresholds_pin_the_fork_chains() {
        assert_eq!(chain_threshold::<V1>(2), 511);
        assert_eq!(chain_threshold::<V1>(3), 766);
    }

    #[test]
    fn exhausted_input_stays_in_regime() {
        let mut u = Unstructured::new(&[]);
        let key = chain_key::<V1>(&mut u).unwrap();
        assert_eq!(key.len(), 1);
        let (left, right) = diverging_pair(&mut u, chain_threshold::<V1>(2)).unwrap();
        assert_eq!(left.len(), 512);
        assert_eq!(right.len(), 512);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        #[test]
        fn chain_keys_stay_in_regime(seed in proptest::collection::vec(any::<u8>(), 0..2048)) {
            let mut u = Unstructured::new(&seed);
            let key = chain_key::<V1>(&mut u).unwrap();
            prop_assert!(!key.is_empty());
            prop_assert!(key.len() <= chain_threshold::<V1>(3));
            prop_assert!(key.as_bytes().iter().all(|&b| b <= 1));
        }

        #[test]
        fn diverging_pairs_share_exactly_the_run(
            seed in proptest::collection::vec(any::<u8>(), 0..1024),
            shared in 0usize..600,
        ) {
            let mut u = Unstructured::new(&seed);
            let (left, right) = diverging_pair(&mut u, shared).unwrap();
            prop_assert_eq!(&left.as_bytes()[..shared], &right.as_bytes()[..shared]);
            prop_assert_ne!(left.as_bytes()[shared], right.as_bytes()[shared]);
        }

        #[test]
        fn encrypted_entries_are_ref64(seed in proptest::collection::vec(any::<u8>(), 0..256)) {
            let mut u = Unstructured::new(&seed);
            prop_assert!(matches!(encrypted_entry::<V1>(&mut u).unwrap(), Entry::Ref64(_)));
        }

        #[test]
        fn rows_are_deterministic(seed in proptest::collection::vec(any::<u8>(), 0..4096)) {
            let first = spill_rows::<V1>(&mut Unstructured::new(&seed)).unwrap();
            let second = spill_rows::<V1>(&mut Unstructured::new(&seed)).unwrap();
            prop_assert_eq!(first, second);
        }
    }
}
