//! WASM bindings for Chunk functionality.

use super::any_chunk::AnyChunk;
use super::content::ContentChunk;
use super::traits::ChunkOps;
use crate::bmt::{BRANCHES, DEFAULT_BODY_SIZE, HASH_SIZE, SPAN_SIZE};
use js_sys::{Array, Uint8Array};
use wasm_bindgen::prelude::*;

/// WASM-friendly wrapper for ContentChunk.
#[wasm_bindgen(js_name = ContentChunk)]
pub struct WasmContentChunk(ContentChunk<DEFAULT_BODY_SIZE>);

#[wasm_bindgen(js_class = ContentChunk)]
impl WasmContentChunk {
    /// Create a new content chunk from data.
    #[wasm_bindgen(constructor)]
    pub fn new(data: &Uint8Array) -> Result<WasmContentChunk, JsValue> {
        ContentChunk::<DEFAULT_BODY_SIZE>::new(data.to_vec())
            .map(WasmContentChunk)
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    /// Get the chunk's content address (32 bytes).
    #[wasm_bindgen]
    pub fn address(&self) -> Uint8Array {
        let result = Uint8Array::new_with_length(32);
        result.copy_from(self.0.address().as_bytes());
        result
    }

    /// Get the chunk's data (without span header).
    #[wasm_bindgen]
    pub fn data(&self) -> Uint8Array {
        let data = self.0.data();
        let result = Uint8Array::new_with_length(data.len() as u32);
        result.copy_from(data);
        result
    }

    /// Get the span (data size this chunk represents).
    #[wasm_bindgen]
    pub fn span(&self) -> u64 {
        self.0.span()
    }

    /// Get the serialized chunk (span + data).
    #[wasm_bindgen]
    pub fn serialize(&self) -> Uint8Array {
        let bytes: bytes::Bytes = self.0.clone().into();
        let result = Uint8Array::new_with_length(bytes.len() as u32);
        result.copy_from(&bytes);
        result
    }
}

/// Result of splitting a file into chunks.
#[wasm_bindgen(js_name = SplitResult)]
pub struct WasmSplitResult {
    root: [u8; 32],
    chunks: Vec<AnyChunk<DEFAULT_BODY_SIZE>>,
}

#[wasm_bindgen(js_class = SplitResult)]
impl WasmSplitResult {
    /// Get the root address (32 bytes).
    #[wasm_bindgen(getter, js_name = rootAddress)]
    pub fn root_address(&self) -> Uint8Array {
        let result = Uint8Array::new_with_length(32);
        result.copy_from(&self.root);
        result
    }

    /// Get the number of chunks generated.
    #[wasm_bindgen(getter, js_name = chunkCount)]
    pub fn chunk_count(&self) -> usize {
        self.chunks.len()
    }

    /// Get all chunks as an array of WasmContentChunk.
    #[wasm_bindgen]
    pub fn chunks(&self) -> Array {
        let arr = Array::new();
        for chunk in &self.chunks {
            if let Some(content) = chunk.as_content() {
                arr.push(&WasmContentChunk(content.clone()).into());
            }
        }
        arr
    }

    /// Get a chunk by index.
    #[wasm_bindgen(js_name = getChunk)]
    pub fn get_chunk(&self, index: usize) -> Option<WasmContentChunk> {
        self.chunks
            .get(index)
            .and_then(|c| c.as_content())
            .cloned()
            .map(WasmContentChunk)
    }

    /// Get all chunk addresses as an array of Uint8Array.
    #[wasm_bindgen]
    pub fn addresses(&self) -> Array {
        let arr = Array::new();
        for chunk in &self.chunks {
            let addr = Uint8Array::new_with_length(32);
            addr.copy_from(chunk.address().as_bytes());
            arr.push(&addr);
        }
        arr
    }
}

/// Split data into BMT chunks.
///
/// Returns a SplitResult containing the root address and all generated chunks.
#[allow(deprecated)]
#[wasm_bindgen(js_name = splitFile)]
pub fn split_file(data: &Uint8Array) -> Result<WasmSplitResult, JsValue> {
    let bytes = data.to_vec();
    let (root, store) = crate::file::split::<DEFAULT_BODY_SIZE>(&bytes)
        .map_err(|e| JsValue::from_str(&e.to_string()))?;

    Ok(WasmSplitResult {
        root: root.into(),
        chunks: store
            .into_chunks()
            .into_values()
            .map(|c| c.into_envelope())
            .collect(),
    })
}

/// Hash a single chunk of data and return the 32-byte address.
///
/// Useful for parallel hashing in web workers.
#[wasm_bindgen(js_name = hashChunkData)]
pub fn hash_chunk_data(data: &Uint8Array, span: u64) -> Result<Uint8Array, JsValue> {
    use crate::bmt::Hasher;

    let data_vec = data.to_vec();

    // Validate size
    if data_vec.len() > DEFAULT_BODY_SIZE {
        return Err(JsValue::from_str(&format!(
            "Data too large: {} bytes (max {})",
            data_vec.len(),
            DEFAULT_BODY_SIZE
        )));
    }

    // Create hasher and compute hash
    let mut hasher = Hasher::<DEFAULT_BODY_SIZE>::new();
    hasher.set_span(span);
    hasher.update(&data_vec);
    let hash = hasher.sum();

    let result = Uint8Array::new_with_length(32);
    result.copy_from(hash.as_slice());
    Ok(result)
}

/// Create a chunk from raw span + data bytes (as stored/transmitted).
#[wasm_bindgen(js_name = chunkFromBytes)]
pub fn chunk_from_bytes(bytes: &Uint8Array) -> Result<WasmContentChunk, JsValue> {
    let bytes_vec = bytes.to_vec();
    ContentChunk::<DEFAULT_BODY_SIZE>::try_from(bytes::Bytes::from(bytes_vec))
        .map(WasmContentChunk)
        .map_err(|e| JsValue::from_str(&e.to_string()))
}

/// BMT constants for JavaScript use.
#[wasm_bindgen(js_name = getConstants)]
pub fn get_constants() -> Result<JsValue, JsValue> {
    use js_sys::Object;

    let obj = Object::new();
    js_sys::Reflect::set(&obj, &"BODY_SIZE".into(), &JsValue::from(DEFAULT_BODY_SIZE))?;
    js_sys::Reflect::set(&obj, &"SPAN_SIZE".into(), &JsValue::from(SPAN_SIZE))?;
    js_sys::Reflect::set(&obj, &"HASH_SIZE".into(), &JsValue::from(HASH_SIZE))?;
    js_sys::Reflect::set(&obj, &"BRANCHES".into(), &JsValue::from(BRANCHES))?;
    Ok(obj.into())
}
