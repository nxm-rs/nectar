// nectar/crates/primitives/src/chunk/custom/registry.rs
//! Registry for custom chunk deserializers

use super::CustomChunk;
use crate::chunk::error::ChunkError;
use crate::error::Result;
use bytes::Bytes;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::sync::Arc;

// Only include parking_lot on non-WASM platforms
#[cfg(not(target_arch = "wasm32"))]
use parking_lot::RwLock;

// For WASM, use a simpler mutex from std that works in single-threaded contexts
#[cfg(target_arch = "wasm32")]
use std::sync::Mutex as RwLock;

/// Minimum valid type ID for custom chunks
const CUSTOM_CHUNK_TYPE_MIN: u8 = 0xE0;

/// Maximum valid type ID for custom chunks
const CUSTOM_CHUNK_TYPE_MAX: u8 = 0xEF;

/// Registry for custom chunk deserializers
struct CustomChunkRegistry {
    // Map of (type_id, version) to deserializer functions
    deserializers:
        HashMap<(u8, u8), Arc<dyn Fn(Bytes) -> Result<Box<dyn CustomChunk>> + Send + Sync>>,
}

impl CustomChunkRegistry {
    /// Create a new registry
    fn new() -> Self {
        Self {
            deserializers: HashMap::new(),
        }
    }

    /// Register a deserializer for a custom chunk type and version
    #[cfg(not(target_arch = "wasm32"))]
    fn register<F>(&mut self, type_id: u8, version: u8, deserializer: F) -> &mut Self
    where
        F: Fn(Bytes) -> Result<Box<dyn CustomChunk>> + Send + Sync + 'static,
    {
        // Only register for the allowed custom type ID range
        if type_id < CUSTOM_CHUNK_TYPE_MIN || type_id > CUSTOM_CHUNK_TYPE_MAX {
            return self;
        }

        self.deserializers
            .insert((type_id, version), Arc::new(deserializer));
        self
    }

    /// Register a deserializer for a custom chunk type and version - WASM version (no-op)
    #[cfg(target_arch = "wasm32")]
    fn register<F>(&mut self, _type_id: u8, _version: u8, _deserializer: F) -> &mut Self
    where
        F: Fn(Bytes) -> Result<Box<dyn CustomChunk>> + Send + Sync + 'static,
    {
        // No-op for WASM - custom registrations are not supported
        self
    }

    /// Try to deserialize custom chunk data
    #[cfg(not(target_arch = "wasm32"))]
    fn deserialize(
        &self,
        data: Bytes,
        type_id: u8,
        version: u8,
    ) -> Result<Option<Box<dyn CustomChunk>>> {
        if let Some(deserializer) = self.deserializers.get(&(type_id, version)) {
            match deserializer(data) {
                Ok(chunk) => Ok(Some(chunk)),
                Err(e) => Err(e),
            }
        } else {
            Ok(None)
        }
    }

    /// Try to deserialize custom chunk data - WASM version (always returns None)
    #[cfg(target_arch = "wasm32")]
    fn deserialize(
        &self,
        _data: Bytes,
        _type_id: u8,
        _version: u8,
    ) -> Result<Option<Box<dyn CustomChunk>>> {
        // For WASM, always return None as custom chunks are not supported
        Ok(None)
    }

    /// Try to deserialize custom chunk data by trying all deserializers
    #[cfg(not(target_arch = "wasm32"))]
    fn detect_and_deserialize(&self, data: Bytes) -> Result<Option<Box<dyn CustomChunk>>> {
        // Try each deserializer in the custom namespace (0xE0-0xEF)
        for ((type_id, _version), deserializer) in &self.deserializers {
            if *type_id >= CUSTOM_CHUNK_TYPE_MIN && *type_id <= CUSTOM_CHUNK_TYPE_MAX {
                match deserializer(data.clone()) {
                    Ok(chunk) => return Ok(Some(chunk)),
                    Err(_) => continue,
                }
            }
        }

        Ok(None)
    }

    /// Try to deserialize custom chunk data by trying all deserializers - WASM version (always returns None)
    #[cfg(target_arch = "wasm32")]
    fn detect_and_deserialize(&self, _data: Bytes) -> Result<Option<Box<dyn CustomChunk>>> {
        // For WASM, always return None as custom chunks are not supported
        Ok(None)
    }

    /// Check if a type ID is in the valid custom chunk range
    fn is_valid_custom_type_id(type_id: u8) -> bool {
        type_id >= CUSTOM_CHUNK_TYPE_MIN && type_id <= CUSTOM_CHUNK_TYPE_MAX
    }
}

impl Default for CustomChunkRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// Create a global registry with appropriate locking primitive for the platform
#[cfg(not(target_arch = "wasm32"))]
static GLOBAL_REGISTRY: Lazy<RwLock<CustomChunkRegistry>> =
    Lazy::new(|| RwLock::new(CustomChunkRegistry::new()));

#[cfg(target_arch = "wasm32")]
static GLOBAL_REGISTRY: Lazy<RwLock<CustomChunkRegistry>> =
    Lazy::new(|| RwLock::new(CustomChunkRegistry::new()));

/// Register a custom chunk deserializer
#[cfg(not(target_arch = "wasm32"))]
pub fn register_custom_deserializer<F>(type_id: u8, version: u8, deserializer: F) -> Result<()>
where
    F: Fn(Bytes) -> Result<Box<dyn CustomChunk>> + Send + Sync + 'static,
{
    // Validate type ID is in custom range
    if !CustomChunkRegistry::is_valid_custom_type_id(type_id) {
        return Err(ChunkError::invalid_custom_type(type_id).into());
    }

    let mut registry = GLOBAL_REGISTRY.write();
    registry.register(type_id, version, deserializer);
    Ok(())
}

/// Register a custom chunk deserializer - WASM version (no-op)
#[cfg(target_arch = "wasm32")]
pub fn register_custom_deserializer<F>(_type_id: u8, _version: u8, _deserializer: F) -> Result<()>
where
    F: Fn(Bytes) -> Result<Box<dyn CustomChunk>> + Send + Sync + 'static,
{
    // Custom chunk registration not supported in WASM
    Err(ChunkError::format("Custom chunk registration not supported in WASM environments").into())
}

/// Try to deserialize custom chunk data
#[cfg(not(target_arch = "wasm32"))]
pub fn deserialize(data: Bytes, type_id: u8, version: u8) -> Result<Option<Box<dyn CustomChunk>>> {
    // Validate type ID is in custom range
    if !CustomChunkRegistry::is_valid_custom_type_id(type_id) {
        return Err(ChunkError::invalid_custom_type(type_id).into());
    }

    let registry = GLOBAL_REGISTRY.read();
    registry.deserialize(data, type_id, version)
}

/// Try to deserialize custom chunk data - WASM version (always returns None)
#[cfg(target_arch = "wasm32")]
pub fn deserialize(
    _data: Bytes,
    type_id: u8,
    _version: u8,
) -> Result<Option<Box<dyn CustomChunk>>> {
    // Always return an error in WASM, since custom chunks are not supported
    Err(ChunkError::format(format!(
        "Custom chunk type {:#04x} not supported in WASM environments",
        type_id
    ))
    .into())
}

/// Try to detect and deserialize custom chunk data
#[cfg(not(target_arch = "wasm32"))]
pub fn detect_and_deserialize(data: Bytes) -> Result<Option<Box<dyn CustomChunk>>> {
    let registry = GLOBAL_REGISTRY.read();
    registry.detect_and_deserialize(data)
}

/// Try to detect and deserialize custom chunk data - WASM version (always returns None)
#[cfg(target_arch = "wasm32")]
pub fn detect_and_deserialize(_data: Bytes) -> Result<Option<Box<dyn CustomChunk>>> {
    // Always return None in WASM, since custom chunks are not supported
    Ok(None)
}
