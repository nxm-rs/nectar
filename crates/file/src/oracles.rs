//! Shared fuzz and test oracle for the walk's rejection of malformed
//! intermediate chunks.
//!
//! One oracle per invariant: the fuzz target and the stable pins call the
//! same body, so the rungs cannot drift. The oracle returns `Err` instead
//! of panicking; call sites assert. Exposed only to in-crate tests and,
//! under `arbitrary`, to the fuzz workspace; exempt from semver guarantees.

use alloc::vec::Vec;

use nectar_primitives::chunk::{AnyChunkSet, Chunk, ChunkAddress, ContentChunk, Verified};
use nectar_primitives::oracles::Violation;
use nectar_primitives::store::MemoryStore;

use crate::sync::drive;
use crate::{File, Plain};

/// Tiny body size: fan-out 8, so shallow inputs reach deep shape checks.
const BODY: usize = 256;
/// Synthetic-chunk cap per run.
const MAX_CHUNKS: usize = 16;
/// Roots probed per run.
const MAX_PROBES: usize = 4;
/// Collect bound; larger declared spans must fail typed, not allocate.
const COLLECT_BOUND: u64 = 1 << 20;

/// A content chunk from an arbitrary span and body, wire-decoded so the
/// address is honestly derived from the (possibly nonsensical) content.
fn synth(span: u64, payload: &[u8]) -> Option<ContentChunk<BODY>> {
    let payload = payload.get(..payload.len().min(BODY))?;
    let mut wire = Vec::with_capacity(payload.len().saturating_add(8));
    wire.extend_from_slice(&span.to_le_bytes());
    wire.extend_from_slice(payload);
    ContentChunk::try_from(wire.as_slice()).ok()
}

/// Open and drain the tree at `root`; malformed trees may only fail typed.
/// Returns whether the open and the bounded collect both succeeded, in
/// which case the delivered length must equal the declared span.
fn probe(store: &MemoryStore<AnyChunkSet<BODY>>, root: ChunkAddress) -> Result<bool, Violation> {
    let store = store.clone();
    let outcome = drive(async move {
        let Ok(file) = File::<_, Plain, BODY>::open(store, root).await else {
            return Ok(false);
        };
        let span = file.len();
        let mut delivered = false;
        if let Ok(bytes) = file.collect(COLLECT_BOUND).await {
            if crate::num::u64_from_usize(bytes.len()) != span {
                return Err(Violation::new(
                    "an accepted tree must deliver exactly its declared span",
                ));
            }
            delivered = true;
        }
        // A mid-file reader probe walks the same tree down a partial range.
        let mut reader = file.read().range(span / 2..span).build();
        let mut buf = [0u8; 64];
        while let Ok(n) = reader.read(&mut buf).await {
            if n == 0 {
                break;
            }
        }
        Ok(delivered)
    });
    outcome.unwrap_or(Err(Violation::new("a ready store must never pend")))
}

/// Author content-addressed chunks whose spans and bodies need not obey the
/// tree grammar, plus one synthesized intermediate referencing them, and
/// open the file at each: the walk must reject every contradiction typed.
/// Returns whether the synthesized intermediate was accepted end to end
/// (open plus an in-bound collect of its exact span), or `None` when no
/// intermediate could be synthesized.
pub fn malformed_walk(
    specs: &[(u64, Vec<u8>)],
    root_span: u64,
    arity: u8,
) -> Result<Option<bool>, Violation> {
    let mut chunks: Vec<Chunk<Verified, AnyChunkSet<BODY>>> = Vec::new();
    for (span, payload) in specs.iter().take(MAX_CHUNKS) {
        if let Some(chunk) = synth(*span, payload) {
            chunks.push(chunk.seal());
        }
    }
    let addresses: Vec<ChunkAddress> = chunks.iter().map(|chunk| *chunk.address()).collect();
    let store = MemoryStore::from_chunks(chunks);

    // One synthesized intermediate: real child addresses under a fuzzed
    // span, so shape contradictions surface below the root.
    let mut accepted = None;
    if !addresses.is_empty() {
        let children = (usize::from(arity) % (BODY / 32)).saturating_add(1);
        let mut body = Vec::with_capacity(children.saturating_mul(32));
        for address in addresses.iter().cycle().take(children) {
            body.extend_from_slice(address.as_bytes());
        }
        if let Some(root) = synth(root_span, &body) {
            let sealed: Chunk<Verified, AnyChunkSet<BODY>> = root.seal();
            let root_address = *sealed.address();
            let store =
                MemoryStore::from_chunks(store.clone().into_chunks().into_values().chain([sealed]));
            accepted = Some(probe(&store, root_address)?);
        }
    }

    for address in addresses.iter().take(MAX_PROBES) {
        probe(&store, *address)?;
    }

    // An absent root must fail the open, not the process.
    if probe(&store, ChunkAddress::new([0x5A; 32]))? {
        return Err(Violation::new("an absent root must fail the open"));
    }
    Ok(accepted)
}

#[cfg(test)]
mod tests {
    use alloc::vec::Vec;

    use arbitrary::{Arbitrary, Unstructured};

    /// Decode one committed seed through the exact grammar the fuzz target
    /// uses and run the shared oracle.
    fn outcome(name: &str, data: &[u8]) -> Option<bool> {
        let (specs, root_span, arity) =
            <(Vec<(u64, Vec<u8>)>, u64, u8)>::arbitrary_take_rest(Unstructured::new(data))
                .unwrap_or_else(|e| panic!("seed {name} must decode a walk input: {e}"));
        super::malformed_walk(&specs, root_span, arity)
            .unwrap_or_else(|v| panic!("seed {name}: {v}"))
    }

    /// Replay the committed seed corpus of the `file_malformed_intermediate`
    /// fuzz target through the shared oracle. Seed intent is pinned by name:
    /// `valid-*` must open and collect its exact declared span, `invalid-*`
    /// must stay rejected typed, `edge-*` only asserts no violation. This
    /// keeps the fuzz seeds meaningful on stable without running the fuzzer
    /// itself.
    #[test]
    fn seed_replay_file_malformed_intermediate() {
        nectar_testing::SeedReplay::corpus(
            env!("CARGO_MANIFEST_DIR"),
            "file_malformed_intermediate",
        )
        .each(|name, data| {
            let _ = outcome(name, data);
        })
        .on("valid-", |name, data| {
            assert_eq!(
                outcome(name, data),
                Some(true),
                "seed {name} must deliver its declared span"
            );
        })
        .on("invalid-", |name, data| {
            assert_eq!(
                outcome(name, data),
                Some(false),
                "seed {name} must remain rejected"
            );
        })
        .covers("edge-")
        .floor(4)
        .run();
    }
}
