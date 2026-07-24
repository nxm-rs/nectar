//! Valid-by-construction test-value generators.

use alloy_signer_local::PrivateKeySigner;
use arbitrary::{Arbitrary, Unstructured};

use crate::feed::Feed;
use crate::topic::Topic;

/// A feed whose owner is controlled by the returned signer.
///
/// Deterministic in `u`; the signer comes from the primitives generator so
/// layered draws replay stably.
pub fn feed_with_signer<const BODY_SIZE: usize>(
    u: &mut Unstructured<'_>,
) -> arbitrary::Result<(Feed<BODY_SIZE>, PrivateKeySigner)> {
    let signer = nectar_primitives::generators::signer(u)?;
    let feed = Feed::new(Topic::arbitrary(u)?, signer.address());
    Ok((feed, signer))
}
