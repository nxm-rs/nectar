//! WASM bindings for chunk functionality.
//!
//! This module provides JavaScript-friendly wrappers around chunk types.

use super::{ChunkAddress, ChunkData};
use bytes::Bytes;
use js_sys::Uint8Array;
use wasm_bindgen::prelude::*;

/// WASM-friendly wrapper for ChunkAddress
#[wasm_bindgen(js_name = ChunkAddress)]
pub struct WasmChunkAddress(pub(crate) ChunkAddress);

#[wasm_bindgen(js_class = ChunkAddress)]
impl WasmChunkAddress {
    /// Create a new zero-filled address
    #[wasm_bindgen(static_method_of = ChunkAddress)]
    pub fn zero() -> Self {
        Self(ChunkAddress::zero())
    }

    /// Create from bytes
    #[wasm_bindgen(static_method_of = ChunkAddress, js_name = fromBytes)]
    pub fn from_bytes(bytes: &Uint8Array) -> Result<WasmChunkAddress, JsValue> {
        match ChunkAddress::from_slice(&bytes.to_vec()) {
            Ok(addr) => Ok(WasmChunkAddress(addr)),
            Err(e) => Err(JsValue::from_str(&e.to_string())),
        }
    }

    /// Get the address bytes
    #[wasm_bindgen(js_name = asBytes)]
    pub fn as_bytes(&self) -> Uint8Array {
        let bytes = self.0.as_bytes();
        let result = Uint8Array::new_with_length(bytes.len() as u32);
        result.copy_from(bytes);
        result
    }

    /// Check if this address is zeros
    #[wasm_bindgen(js_name = isZero)]
    pub fn is_zero(&self) -> bool {
        self.0.is_zero()
    }

    /// Calculate proximity between two addresses
    #[wasm_bindgen]
    pub fn proximity(&self, other: &WasmChunkAddress) -> u8 {
        self.0.proximity(&other.0)
    }

    /// Check if address is within proximity
    #[wasm_bindgen(js_name = isWithinProximity)]
    pub fn is_within_proximity(&self, other: &WasmChunkAddress, min_proximity: u8) -> bool {
        self.0.is_within_proximity(&other.0, min_proximity)
    }
}

/// WASM-friendly wrapper for ChunkData
#[wasm_bindgen(js_name = ChunkData)]
pub struct WasmChunkData(pub(crate) ChunkData);

#[wasm_bindgen(js_class = ChunkData)]
impl WasmChunkData {
    /// Deserialize bytes into a chunk
    #[wasm_bindgen(static_method_of = ChunkData)]
    pub fn deserialize(data: &Uint8Array, has_type_prefix: bool) -> Result<WasmChunkData, JsValue> {
        let bytes = Bytes::copy_from_slice(&data.to_vec());
        match ChunkData::deserialize(bytes, has_type_prefix) {
            Ok(chunk) => Ok(WasmChunkData(chunk)),
            Err(e) => Err(JsValue::from_str(&e.to_string())),
        }
    }

    /// Get the chunk's address
    #[wasm_bindgen]
    pub fn address(&self) -> WasmChunkAddress {
        WasmChunkAddress(self.0.address())
    }

    /// Get the chunk type as a byte
    #[wasm_bindgen(js_name = chunkTypeByte)]
    pub fn chunk_type_byte(&self) -> u8 {
        self.0.chunk_type().to_byte()
    }

    /// Get the chunk's version
    #[wasm_bindgen]
    pub fn version(&self) -> u8 {
        self.0.version()
    }

    /// Get the header size
    #[wasm_bindgen(js_name = headerSize)]
    pub fn header_size(&self) -> usize {
        self.0.header_size()
    }

    /// Get the header bytes
    #[wasm_bindgen]
    pub fn header(&self) -> Uint8Array {
        let header = self.0.header();
        let result = Uint8Array::new_with_length(header.len() as u32);
        result.copy_from(header);
        result
    }

    /// Get the payload bytes
    #[wasm_bindgen]
    pub fn payload(&self) -> Uint8Array {
        let payload = self.0.payload();
        let result = Uint8Array::new_with_length(payload.len() as u32);
        result.copy_from(payload);
        result
    }

    /// Get the full data bytes
    #[wasm_bindgen]
    pub fn data(&self) -> Uint8Array {
        let data = self.0.data();
        let result = Uint8Array::new_with_length(data.len() as u32);
        result.copy_from(data);
        result
    }

    /// Get the chunk size in bytes
    #[wasm_bindgen]
    pub fn size(&self) -> usize {
        self.0.size()
    }

    /// Verify chunk integrity
    #[wasm_bindgen(js_name = verifyIntegrity)]
    pub fn verify_integrity(&self) -> Result<(), JsValue> {
        match self.0.verify_integrity() {
            Ok(_) => Ok(()),
            Err(e) => Err(JsValue::from_str(&e.to_string())),
        }
    }

    /// Verify the chunk matches an expected address
    #[wasm_bindgen]
    pub fn verify(&self, expected: &WasmChunkAddress) -> Result<(), JsValue> {
        match self.0.verify(expected.0.clone()) {
            Ok(_) => Ok(()),
            Err(e) => Err(JsValue::from_str(&e.to_string())),
        }
    }

    /// Serialize the chunk to bytes
    #[wasm_bindgen]
    pub fn serialize(&self, with_type_prefix: bool) -> Uint8Array {
        let bytes = self.0.serialize(with_type_prefix);
        let result = Uint8Array::new_with_length(bytes.len() as u32);
        result.copy_from(&bytes);
        result
    }
}
