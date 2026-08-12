//! The manifest seam's shared data-load lane, called by every format adapter.

use nectar_manifest::{DataSink, ManifestError, SinkError};
use nectar_primitives::EntryRef;
use nectar_primitives::chunk::ContentOnlyChunkSet;
use nectar_primitives::store::{MaybeSend, MaybeSync, TrustedGet};

use crate::{File, LoadError, Policy};

/// Drain the file at `reference` into `sink` under `policy`, reporting a sink
/// failure as [`ManifestError::Sink`] and any other as [`ManifestError::Data`].
///
/// The writes are idempotent overwrites, so rerun a failed load in full.
pub async fn load_reference<S, K, F, const B: usize>(
    store: S,
    policy: Policy,
    reference: EntryRef,
    sink: &mut K,
) -> Result<(), ManifestError<F>>
where
    S: TrustedGet<ContentOnlyChunkSet<B>> + Clone + MaybeSend + MaybeSync + 'static,
    K: DataSink<Error: SinkError> + MaybeSend,
{
    File::<S, B>::new(store, policy)
        .load(reference, sink)
        .await
        .map(drop)
        .map_err(|error| match error {
            LoadError::Sink { source, .. } => ManifestError::sink(source),
            data => ManifestError::data(data),
        })
}
