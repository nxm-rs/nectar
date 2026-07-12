//! Chunk type set trait
//!
//! This module provides the [`ChunkTypeSet`] trait for defining sets of supported
//! chunk types at compile time, and [`StandardChunkSet`] as the default implementation.

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;

use crate::bmt::DEFAULT_BODY_SIZE;

use super::type_id::ChunkTypeId;

/// Trait defining a set of supported chunk types with configurable body size.
///
/// This trait is implemented by marker types that define which chunk types
/// a system supports. It enables compile-time configuration of valid chunk
/// types; decoding into a runtime-polymorphic chunk goes through
/// [`AnyChunk::from_wire_bytes`](super::any_chunk::AnyChunk::from_wire_bytes).
///
/// # Design Rationale
///
/// This trait uses associated functions (not methods) because the supported
/// types are determined at compile time, not per-instance. This allows:
///
/// - Zero-cost type checking at compile time
/// - Generic programming over chunk type sets
///
/// # Example
///
/// ```ignore
/// use nectar_primitives::{ChunkTypeSet, ChunkTypeId, AnyChunk, StandardChunkSet};
///
/// // Check if a type is supported
/// assert!(StandardChunkSet::supports(ChunkTypeId::CONTENT));
/// assert!(StandardChunkSet::supports(ChunkTypeId::SINGLE_OWNER));
/// assert!(!StandardChunkSet::supports(ChunkTypeId::custom(200)));
///
/// // Get supported types
/// let types = StandardChunkSet::supported_types();
/// assert_eq!(types.len(), 2);
/// ```
pub trait ChunkTypeSet<const BODY_SIZE: usize = DEFAULT_BODY_SIZE>: Send + Sync + 'static {
    /// The chunk body size in bytes for this set.
    ///
    /// This is exposed as an associated const so consumers can access the body size
    /// at compile time through the type system.
    const BODY_SIZE: usize = BODY_SIZE;

    /// Check if a chunk type ID is supported by this set.
    ///
    /// Returns `true` if chunks with the given type ID can be
    /// deserialized and processed by this set.
    fn supports(type_id: ChunkTypeId) -> bool;

    /// Get the list of all supported type IDs.
    ///
    /// This returns a static slice for efficiency in const contexts.
    fn supported_types() -> &'static [ChunkTypeId];

    /// Format the supported chunk types as a human-readable string.
    ///
    /// Returns a comma-separated list with abbreviations and hex codes,
    /// e.g., "CAC (0x00), SOC (0x01)".
    ///
    /// # Example
    ///
    /// ```
    /// use nectar_primitives::{ChunkTypeSet, StandardChunkSet, DEFAULT_BODY_SIZE};
    ///
    /// let formatted = <StandardChunkSet as ChunkTypeSet<DEFAULT_BODY_SIZE>>::format_supported_types();
    /// assert!(formatted.contains("CAC"));
    /// assert!(formatted.contains("SOC"));
    /// ```
    fn format_supported_types() -> String {
        let types = Self::supported_types();
        let names: Vec<_> = types
            .iter()
            .map(|t| {
                let abbrev = t.abbreviation().unwrap_or("???");
                alloc::format!("{} (0x{:02x})", abbrev, t.as_u8())
            })
            .collect();
        names.join(", ")
    }
}

/// Standard Swarm chunk type set.
///
/// This set includes the two fundamental chunk types in the Swarm network:
/// - Content-addressed chunks (CAC) - [`ChunkTypeId::CONTENT`]
/// - Single-owner chunks (SOC) - [`ChunkTypeId::SINGLE_OWNER`]
///
/// This is the default chunk set used by most Swarm nodes.
///
/// # Example
///
/// ```ignore
/// use nectar_primitives::{ChunkTypeSet, ChunkTypeId, StandardChunkSet};
///
/// // Check support
/// assert!(StandardChunkSet::supports(ChunkTypeId::CONTENT));
/// assert!(StandardChunkSet::supports(ChunkTypeId::SINGLE_OWNER));
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct StandardChunkSet;

impl<const BODY_SIZE: usize> ChunkTypeSet<BODY_SIZE> for StandardChunkSet {
    fn supports(type_id: ChunkTypeId) -> bool {
        matches!(type_id, ChunkTypeId::CONTENT | ChunkTypeId::SINGLE_OWNER)
    }

    fn supported_types() -> &'static [ChunkTypeId] {
        &[ChunkTypeId::CONTENT, ChunkTypeId::SINGLE_OWNER]
    }
}

/// A chunk type set that accepts only content-addressed chunks.
///
/// This is useful for systems that only need to handle immutable content.
#[derive(Debug, Clone, Copy, Default)]
pub struct ContentOnlyChunkSet;

impl<const BODY_SIZE: usize> ChunkTypeSet<BODY_SIZE> for ContentOnlyChunkSet {
    fn supports(type_id: ChunkTypeId) -> bool {
        type_id == ChunkTypeId::CONTENT
    }

    fn supported_types() -> &'static [ChunkTypeId] {
        &[ChunkTypeId::CONTENT]
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::super::address::ChunkAddress;
    use super::super::any_chunk::AnyChunk;
    use super::super::content::ContentChunk;
    use super::super::error::ChunkError;
    use super::super::single_owner::SingleOwnerChunk;
    use super::super::traits::Chunk;
    use super::*;
    use crate::error::Result;

    type DefaultContentChunk = ContentChunk<DEFAULT_BODY_SIZE>;

    #[test]
    fn test_standard_chunk_set_supports() {
        assert!(
            <StandardChunkSet as ChunkTypeSet<DEFAULT_BODY_SIZE>>::supports(ChunkTypeId::CONTENT)
        );
        assert!(
            <StandardChunkSet as ChunkTypeSet<DEFAULT_BODY_SIZE>>::supports(
                ChunkTypeId::SINGLE_OWNER
            )
        );
        assert!(
            !<StandardChunkSet as ChunkTypeSet<DEFAULT_BODY_SIZE>>::supports(ChunkTypeId::custom(
                100
            ))
        );
        assert!(
            !<StandardChunkSet as ChunkTypeSet<DEFAULT_BODY_SIZE>>::supports(ChunkTypeId::new(50))
        );
    }

    #[test]
    fn test_standard_chunk_set_supported_types() {
        let types = <StandardChunkSet as ChunkTypeSet<DEFAULT_BODY_SIZE>>::supported_types();
        assert_eq!(types.len(), 2);
        assert!(types.contains(&ChunkTypeId::CONTENT));
        assert!(types.contains(&ChunkTypeId::SINGLE_OWNER));
    }

    #[test]
    fn test_format_supported_types() {
        let formatted =
            <StandardChunkSet as ChunkTypeSet<DEFAULT_BODY_SIZE>>::format_supported_types();
        assert_eq!(formatted, "CAC (0x00), SOC (0x01)");

        let content_only =
            <ContentOnlyChunkSet as ChunkTypeSet<DEFAULT_BODY_SIZE>>::format_supported_types();
        assert_eq!(content_only, "CAC (0x00)");
    }

    #[test]
    fn test_content_only_chunk_set_supports() {
        assert!(
            <ContentOnlyChunkSet as ChunkTypeSet<DEFAULT_BODY_SIZE>>::supports(
                ChunkTypeId::CONTENT
            )
        );
        assert!(
            !<ContentOnlyChunkSet as ChunkTypeSet<DEFAULT_BODY_SIZE>>::supports(
                ChunkTypeId::SINGLE_OWNER
            )
        );
        assert!(
            !<ContentOnlyChunkSet as ChunkTypeSet<DEFAULT_BODY_SIZE>>::supports(
                ChunkTypeId::custom(100)
            )
        );
    }

    #[test]
    fn test_from_wire_bytes_content_chunk() {
        // Create a content chunk and serialize it
        let content = DefaultContentChunk::new(&b"hello world"[..]).unwrap();
        let bytes: Bytes = content.clone().into();

        // Decode through the address-keyed wire decoder
        let any_chunk =
            AnyChunk::<DEFAULT_BODY_SIZE>::from_wire_bytes(content.address(), bytes).unwrap();

        assert!(any_chunk.is_content());
        assert_eq!(*any_chunk.address(), *content.address());
    }

    #[test]
    fn test_from_wire_bytes_empty_bytes_fails() {
        let result =
            AnyChunk::<DEFAULT_BODY_SIZE>::from_wire_bytes(&ChunkAddress::default(), Bytes::new());
        assert!(result.is_err());
    }

    /// Mirrors the body of the `chunk_decode` fuzz target: run the input
    /// through every decode entry point the fuzzer drives and force the lazy
    /// address/owner computations. The fuzz oracle is "no panic"; `Err` is an
    /// acceptable outcome for arbitrary bytes.
    fn exercise_chunk_decode(data: &[u8]) -> Result<AnyChunk<DEFAULT_BODY_SIZE>> {
        let bytes = Bytes::copy_from_slice(data);

        // Address-mismatch arm: the zero address matches (almost) no input,
        // so both trial parses and their address computations run to `Err`.
        let _ =
            AnyChunk::<DEFAULT_BODY_SIZE>::from_wire_bytes(&ChunkAddress::default(), bytes.clone());

        let content = ContentChunk::<DEFAULT_BODY_SIZE>::try_from(data);
        let soc = SingleOwnerChunk::<DEFAULT_BODY_SIZE>::try_from(data);
        if let Ok(soc) = &soc {
            // ECDSA public-key recovery over bytes 32..97 must not panic.
            let _ = soc.owner();
            let _ = soc.address();
        }

        // Ok arm: key the wire decoder by the address of whichever direct
        // parse succeeded, CAC first (the same trial order the decoder uses).
        let address = content
            .ok()
            .map(|c| *c.address())
            .or_else(|| soc.ok().map(|s| *s.address()))
            .ok_or_else(|| ChunkError::invalid_format("no structural parse"))?;
        let result = AnyChunk::from_wire_bytes(&address, bytes);
        if let Ok(chunk) = &result {
            let _ = chunk.address();
        }
        result
    }

    /// Replay crafted edge inputs through the exact entry points the
    /// `chunk_decode` fuzz target exercises: length boundaries around the
    /// 8-byte span, the 97-byte SOC id+signature header, and the maximum
    /// CAC/SOC encodings, in all-zero and all-0xff flavours.
    #[test]
    fn chunk_decode_edge_inputs_do_not_panic() {
        let edge_inputs: Vec<Vec<u8>> = alloc::vec![
            Vec::new(),
            alloc::vec![0x00],
            alloc::vec![0xff; 7],                     // one short of a CAC span
            alloc::vec![0x00; 8],                     // zero span, empty payload
            alloc::vec![0xff; 8],                     // max span, empty payload
            alloc::vec![0xff; 96],                    // one short of the SOC header
            alloc::vec![0xff; 97],                    // SOC header, no body
            alloc::vec![0xff; 105],                   // SOC header + span, empty payload
            alloc::vec![0xff; 8 + DEFAULT_BODY_SIZE], // max CAC encoding
            alloc::vec![0xff; 8 + DEFAULT_BODY_SIZE + 1], // one past max CAC
            alloc::vec![0x00; 97 + 8 + DEFAULT_BODY_SIZE], // max SOC encoding
            alloc::vec![0xff; 97 + 8 + DEFAULT_BODY_SIZE + 1], // one past max SOC
        ];
        for data in &edge_inputs {
            let _ = exercise_chunk_decode(data);
        }
    }

    /// Replay the committed seed corpus of the `chunk_decode` fuzz target
    /// (`fuzz/seeds/chunk_decode/`). Seed intent is pinned by name:
    /// `valid-*` must deserialize `Ok` (and `valid-soc-*` must also decode as
    /// a SOC directly), `invalid-*` must stay `Err`. This keeps the fuzz
    /// seeds meaningful on stable without running the fuzzer itself.
    #[test]
    fn seed_replay_chunk_decode() {
        let seed_dir =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fuzz/seeds/chunk_decode");
        let mut replayed = 0usize;
        for entry in std::fs::read_dir(&seed_dir)
            .unwrap_or_else(|e| panic!("seed dir {} must exist: {e}", seed_dir.display()))
        {
            let path = entry.unwrap().path();
            let name = path.file_name().unwrap().to_string_lossy().into_owned();
            let data = std::fs::read(&path).unwrap();

            let result = exercise_chunk_decode(&data);

            if name.starts_with("valid-") {
                assert!(result.is_ok(), "seed {name} must deserialize successfully");
            } else if name.starts_with("invalid-") {
                assert!(result.is_err(), "seed {name} must remain an Err input");
            }
            if name.starts_with("valid-soc-") {
                assert!(
                    SingleOwnerChunk::<DEFAULT_BODY_SIZE>::try_from(data.as_slice()).is_ok(),
                    "seed {name} must decode as a single-owner chunk"
                );
            }
            replayed += 1;
        }
        assert!(
            replayed >= 4,
            "expected at least the 4 curated seeds, found {replayed}"
        );
    }
}
