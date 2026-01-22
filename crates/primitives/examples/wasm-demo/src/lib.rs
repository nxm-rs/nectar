//! WASM demo for BMT hasher functionality
//!
//! This module provides JavaScript-friendly wrappers around the BMT hasher.

use std::ops::Deref;

use alloy_primitives::{hex, Address, B256};
use alloy_signer_local::PrivateKeySigner;
use bytes::Bytes;
use nectar_primitives::bmt::Hasher;
use nectar_primitives::{Chunk, ChunkAddress, ContentChunk, SingleOwnerChunk};
use wasm_bindgen::prelude::*;

// Add SVG generator modules
mod svg_generators;
mod svg_generators_additional;

// Helpful for debugging
#[cfg(feature = "console_error_panic_hook")]
fn set_panic_hook() {
    #[cfg(feature = "console_error_panic_hook")]
    console_error_panic_hook::set_once();
}

//------------------------------------------------------------------------------
// BMT Hash Result
//------------------------------------------------------------------------------

#[wasm_bindgen]
pub struct HashResult {
    hex: String,
    bytes: Vec<u8>,
}

#[wasm_bindgen]
impl HashResult {
    #[wasm_bindgen(getter)]
    pub fn hex(&self) -> String {
        self.hex.clone()
    }

    #[wasm_bindgen(getter)]
    pub fn bytes(&self) -> js_sys::Uint8Array {
        js_sys::Uint8Array::from(&self.bytes[..])
    }
}

/// Compute a BMT hash for the given text and span
///
/// @param {string} text - The input text to hash
/// @param {number} span - The span value to use (typically the length of the data)
/// @returns {HashResult} The computed hash result with hex and binary representations
#[wasm_bindgen]
pub fn calculate_bmt_hash(text: &str, span: u32) -> HashResult {
    // Set panic hook for better debugging
    set_panic_hook();

    // Create a BMT hasher
    let mut hasher = Hasher::new();

    // Set the specified span (convert to u64)
    hasher.set_span(span as u64);

    // Update with the text data
    hasher.update(text.as_bytes());

    // Compute the hash
    let result = hasher.sum();

    // Convert to hex for display
    let hex = format!("0x{}", hex::encode(result.as_slice()));

    HashResult {
        hex,
        bytes: result.as_slice().to_vec(),
    }
}

/// Benchmark function that hashes data of a specific size
///
/// @param {number} size - The size of data to hash in each iteration (in bytes)
/// @param {number} iterations - The number of hash operations to perform
/// @returns {number} The average time per hash operation in milliseconds
#[wasm_bindgen]
pub fn benchmark_hash(size: u32, iterations: u32) -> f64 {
    set_panic_hook();

    // Ensure size doesn't exceed 4096 bytes
    let size = size.min(4096);

    // Generate a repeatable pattern of data
    let mut data = Vec::with_capacity(size as usize);
    for i in 0..size {
        data.push((i % 256) as u8);
    }

    // Get current time
    let start = js_sys::Date::now();

    // Run the hash multiple times
    for _ in 0..iterations {
        let mut hasher = Hasher::new();
        hasher.set_span(size as u64);
        hasher.update(&data);
        let _result = hasher.sum();
    }

    // Calculate elapsed time in milliseconds
    let elapsed = js_sys::Date::now() - start;

    // Return average time per operation in milliseconds
    elapsed / iterations as f64
}

/// Benchmark function that hashes pre-generated random data
/// Each iteration gets its own unique chunk of data
///
/// @param {Uint8Array} data - Pre-generated random data buffer
/// @param {number} chunk_size - Size of each chunk to hash
/// @param {number} iterations - Number of hash operations to perform
/// @returns {number} The average time per hash operation in milliseconds, or -1 if error
#[wasm_bindgen]
pub fn benchmark_hash_with_random_data(data: &[u8], chunk_size: u32, iterations: u32) -> f64 {
    set_panic_hook();

    // Ensure chunk size is valid
    let chunk_size = chunk_size as usize;

    // Validate that we have enough data for all iterations
    if data.len() < chunk_size * iterations as usize {
        // Not enough data provided, return error value
        return -1.0;
    }

    // Get current time
    let start = js_sys::Date::now();

    // Run the hash multiple times using a unique chunk of data each time
    for i in 0..iterations {
        // Calculate the offset for this iteration
        let offset = i as usize * chunk_size;

        // Create a slice of the data for this iteration
        let chunk = &data[offset..offset + chunk_size];

        // Create a new hasher and hash the chunk
        let mut hasher = Hasher::new();
        hasher.set_span(chunk_size as u64);
        hasher.update(chunk);
        let _result = hasher.sum();
    }

    // Calculate elapsed time in milliseconds
    let elapsed = js_sys::Date::now() - start;

    // Return average time per operation in milliseconds
    elapsed / iterations as f64
}

/// Utility function to help with debugging
///
/// @returns {string} Information about the library version
#[wasm_bindgen]
pub fn get_library_info() -> String {
    "BMT Hash Calculator powered by nectar-primitives - WASM Demo".to_string()
}

//------------------------------------------------------------------------------
// SVG Icon Generator
//------------------------------------------------------------------------------

/// Generator function types for SVG icon generation
#[wasm_bindgen]
#[derive(Clone, Copy)]
pub enum GeneratorFunction {
    /// Geometric patterns based on chunk data
    Geometric,
    /// Abstract art representation of chunk data
    Abstract,
    /// Circular design patterns
    Circular,
    /// Pixelated grid representation
    Pixelated,
    /// Molecular-style node and bond structure
    Molecular,
}

/// Shape options for generated icons
#[wasm_bindgen]
#[derive(Clone, Copy)]
pub enum IconShape {
    /// Square icon (default)
    Square,
    /// Circular icon with clipping
    Circle,
}

/// Color scheme options for generated icons
#[wasm_bindgen]
#[derive(Clone, Copy)]
pub enum ColorScheme {
    /// Bright, contrasting colors
    Vibrant,
    /// Soft, muted colors
    Pastel,
    /// Black, white, and grayscale
    Monochrome,
    /// Colors from opposite sides of the color wheel
    Complementary,
}

/// Configuration for icon generation
#[wasm_bindgen]
pub struct IconConfig {
    size: u32,
    shape: IconShape,
    generator: GeneratorFunction,
    color_scheme: ColorScheme,
}

#[wasm_bindgen]
impl IconConfig {
    /// Create a new icon configuration
    ///
    /// @param {number} size - The size of the icon in pixels
    /// @param {IconShape} shape - The shape of the icon (Square or Circle)
    /// @param {GeneratorFunction} generator - The algorithm to use for generation
    /// @param {ColorScheme} color_scheme - The color scheme to use
    /// @returns {IconConfig} A new configuration object
    #[wasm_bindgen(constructor)]
    pub fn new(
        size: u32,
        shape: IconShape,
        generator: GeneratorFunction,
        color_scheme: ColorScheme,
    ) -> Self {
        Self {
            size,
            shape,
            generator,
            color_scheme,
        }
    }

    #[wasm_bindgen(getter)]
    pub fn size(&self) -> u32 {
        self.size
    }

    #[wasm_bindgen(getter)]
    pub fn shape(&self) -> IconShape {
        self.shape
    }

    #[wasm_bindgen(getter)]
    pub fn generator(&self) -> GeneratorFunction {
        self.generator
    }

    #[wasm_bindgen(getter)]
    pub fn color_scheme(&self) -> ColorScheme {
        self.color_scheme
    }
}

// Rename ChunkData to IconData to avoid name conflict with nectar_primitives
/// Data structure representing chunk data for icon generation
#[wasm_bindgen]
pub struct IconData {
    address: B256,
    chunk_type: u8,
    version: u8,
    header: Vec<u8>,
    payload: Vec<u8>,
}

#[wasm_bindgen]
impl IconData {
    /// Create a new IconData instance
    ///
    /// @param {Uint8Array} address_bytes - 32-byte chunk address
    /// @param {number} chunk_type - Chunk type identifier (1 byte)
    /// @param {number} version - Chunk version (1 byte)
    /// @param {Uint8Array} header_bytes - Chunk header data
    /// @param {Uint8Array} payload_bytes - Chunk payload data
    /// @returns {IconData} A new IconData instance
    #[wasm_bindgen(constructor)]
    pub fn new(
        address_bytes: &[u8],
        chunk_type: u8,
        version: u8,
        header_bytes: &[u8],
        payload_bytes: &[u8],
    ) -> Result<IconData, JsValue> {
        // Validate address length
        if address_bytes.len() != 32 {
            return Err(JsValue::from_str("Address must be exactly 32 bytes"));
        }

        // Create B256 from bytes
        let mut address = B256::default();
        address.copy_from_slice(address_bytes);

        Ok(IconData {
            address,
            chunk_type,
            version,
            header: header_bytes.to_vec(),
            payload: payload_bytes.to_vec(),
        })
    }

    #[wasm_bindgen(getter)]
    pub fn address(&self) -> js_sys::Uint8Array {
        js_sys::Uint8Array::from(self.address.as_slice())
    }

    #[wasm_bindgen(getter)]
    pub fn chunk_type(&self) -> u8 {
        self.chunk_type
    }

    #[wasm_bindgen(getter)]
    pub fn version(&self) -> u8 {
        self.version
    }

    #[wasm_bindgen(getter)]
    pub fn header(&self) -> js_sys::Uint8Array {
        js_sys::Uint8Array::from(self.header.as_slice())
    }

    #[wasm_bindgen(getter)]
    pub fn payload(&self) -> js_sys::Uint8Array {
        js_sys::Uint8Array::from(self.payload.as_slice())
    }
}

/// Create a IconData instance from hex strings (convenience function for JS)
///
/// @param {string} address_hex - 32-byte address as hex string
/// @param {string} type_hex - Chunk type as hex string (1 byte)
/// @param {string} version_hex - Version as hex string (1 byte)
/// @param {string} header_hex - Header data as hex string
/// @param {string} payload_hex - Payload data as hex string
/// @returns {IconData} A new IconData instance from the hex values
#[wasm_bindgen]
pub fn create_icon_from_hex(
    address_hex: &str,
    type_hex: &str,
    version_hex: &str,
    header_hex: &str,
    payload_hex: &str,
) -> Result<IconData, JsValue> {
    // Parse hex strings to bytes
    let address_bytes = match hex::decode(address_hex.trim_start_matches("0x")) {
        Ok(bytes) => bytes,
        Err(_) => return Err(JsValue::from_str("Invalid hex for address")),
    };

    let chunk_type = match u8::from_str_radix(type_hex.trim_start_matches("0x"), 16) {
        Ok(val) => val,
        Err(_) => return Err(JsValue::from_str("Invalid hex for chunk type")),
    };

    let version = match u8::from_str_radix(version_hex.trim_start_matches("0x"), 16) {
        Ok(val) => val,
        Err(_) => return Err(JsValue::from_str("Invalid hex for version")),
    };

    let header_bytes = match hex::decode(header_hex.trim_start_matches("0x")) {
        Ok(bytes) => bytes,
        Err(_) => return Err(JsValue::from_str("Invalid hex for header")),
    };

    let payload_bytes = match hex::decode(payload_hex.trim_start_matches("0x")) {
        Ok(bytes) => bytes,
        Err(_) => return Err(JsValue::from_str("Invalid hex for payload")),
    };

    IconData::new(
        &address_bytes,
        chunk_type,
        version,
        &header_bytes,
        &payload_bytes,
    )
}

/// Generate a random chunk address (32 bytes)
///
/// @returns {Uint8Array} A randomly generated 32-byte address
#[wasm_bindgen]
pub fn generate_random_chunk_address() -> js_sys::Uint8Array {
    let mut bytes = [0u8; 32];
    getrandom::getrandom(&mut bytes).expect("Failed to generate random bytes");
    js_sys::Uint8Array::from(&bytes[..])
}

/// Generate an SVG icon based on IconData and configuration
///
/// @param {IconData} data - The chunk data to visualize
/// @param {IconConfig} config - Configuration options for the icon
/// @returns {string} SVG content representing the chunk data
#[wasm_bindgen]
pub fn generate_svg_icon(data: &IconData, config: &IconConfig) -> String {
    set_panic_hook();

    // Combine all chunk data into a seed for deterministic generation
    let mut seed_data = Vec::with_capacity(64 + 2 + data.header.len() + data.payload.len());
    seed_data.extend_from_slice(data.address.as_slice());
    seed_data.push(data.chunk_type);
    seed_data.push(data.version);
    seed_data.extend_from_slice(&data.header);
    seed_data.extend_from_slice(&data.payload);

    // Call the appropriate generator function based on configuration
    match config.generator {
        GeneratorFunction::Geometric => svg_generators::generate_geometric_icon(&seed_data, config),
        GeneratorFunction::Abstract => svg_generators::generate_abstract_icon(&seed_data, config),
        GeneratorFunction::Circular => {
            svg_generators_additional::generate_circular_icon(&seed_data, config)
        }
        GeneratorFunction::Pixelated => {
            svg_generators_additional::generate_pixelated_icon(&seed_data, config)
        }
        GeneratorFunction::Molecular => {
            svg_generators_additional::generate_molecular_icon(&seed_data, config)
        }
    }
}

/// Create a builder for complex icon configuration
///
/// @returns {IconConfigBuilder} A new icon config builder
#[wasm_bindgen]
pub fn create_icon_config_builder() -> IconConfigBuilder {
    IconConfigBuilder {
        size: 200,
        shape: IconShape::Square,
        generator: GeneratorFunction::Geometric,
        color_scheme: ColorScheme::Vibrant,
    }
}

/// Builder for creating IconConfig objects with a fluent API
#[wasm_bindgen]
pub struct IconConfigBuilder {
    size: u32,
    shape: IconShape,
    generator: GeneratorFunction,
    color_scheme: ColorScheme,
}

#[wasm_bindgen]
impl IconConfigBuilder {
    /// Set the size of the generated icon
    ///
    /// @param {number} size - Size in pixels (both width and height)
    /// @returns {IconConfigBuilder} The builder for method chaining
    #[wasm_bindgen]
    pub fn with_size(mut self, size: u32) -> Self {
        self.size = size;
        self
    }

    /// Set the shape of the generated icon
    ///
    /// @param {IconShape} shape - The shape to use
    /// @returns {IconConfigBuilder} The builder for method chaining
    #[wasm_bindgen]
    pub fn with_shape(mut self, shape: IconShape) -> Self {
        self.shape = shape;
        self
    }

    /// Set the generator function for the icon
    ///
    /// @param {GeneratorFunction} generator - The algorithm to use
    /// @returns {IconConfigBuilder} The builder for method chaining
    #[wasm_bindgen]
    pub fn with_generator(mut self, generator: GeneratorFunction) -> Self {
        self.generator = generator;
        self
    }

    /// Set the color scheme for the icon
    ///
    /// @param {ColorScheme} color_scheme - The color scheme to use
    /// @returns {IconConfigBuilder} The builder for method chaining
    #[wasm_bindgen]
    pub fn with_color_scheme(mut self, color_scheme: ColorScheme) -> Self {
        self.color_scheme = color_scheme;
        self
    }

    /// Build the final IconConfig object
    ///
    /// @returns {IconConfig} The configured IconConfig
    #[wasm_bindgen]
    pub fn build(self) -> IconConfig {
        IconConfig {
            size: self.size,
            shape: self.shape,
            generator: self.generator,
            color_scheme: self.color_scheme,
        }
    }
}

//------------------------------------------------------------------------------
// Chunk Creation and Deserialization
//------------------------------------------------------------------------------

/// Represents the type of a chunk
#[wasm_bindgen]
#[derive(Clone, Copy)]
pub enum ChunkType {
    /// Content-addressed chunk (type 0)
    Content = 0,
    /// Single-owner chunk (type 1)
    SingleOwner = 1,
    /// Unknown chunk type
    Unknown = 255,
}

/// Result of creating a ContentChunk
#[wasm_bindgen]
pub struct ContentChunkResult {
    address: B256,
    data: Vec<u8>,
    serialized: Vec<u8>,
}

#[wasm_bindgen]
impl ContentChunkResult {
    #[wasm_bindgen(getter)]
    pub fn address(&self) -> js_sys::Uint8Array {
        js_sys::Uint8Array::from(self.address.as_slice())
    }

    #[wasm_bindgen(getter)]
    pub fn address_hex(&self) -> String {
        format!("0x{}", hex::encode(self.address.as_slice()))
    }

    #[wasm_bindgen(getter)]
    pub fn data(&self) -> js_sys::Uint8Array {
        js_sys::Uint8Array::from(self.data.as_slice())
    }

    #[wasm_bindgen(getter)]
    pub fn serialized(&self) -> js_sys::Uint8Array {
        js_sys::Uint8Array::from(self.serialized.as_slice())
    }

    #[wasm_bindgen(getter)]
    pub fn serialized_hex(&self) -> String {
        format!("0x{}", hex::encode(&self.serialized))
    }

    #[wasm_bindgen(getter)]
    pub fn size(&self) -> usize {
        self.serialized.len()
    }
}

/// Result of creating a SingleOwnerChunk
#[wasm_bindgen]
pub struct SingleOwnerChunkResult {
    address: B256,
    id: B256,
    owner: Address,
    data: Vec<u8>,
    serialized: Vec<u8>,
    signature: Vec<u8>,
}

#[wasm_bindgen]
impl SingleOwnerChunkResult {
    #[wasm_bindgen(getter)]
    pub fn address(&self) -> js_sys::Uint8Array {
        js_sys::Uint8Array::from(self.address.as_slice())
    }

    #[wasm_bindgen(getter)]
    pub fn address_hex(&self) -> String {
        format!("0x{}", hex::encode(self.address.as_slice()))
    }

    #[wasm_bindgen(getter)]
    pub fn id(&self) -> js_sys::Uint8Array {
        js_sys::Uint8Array::from(self.id.as_slice())
    }

    #[wasm_bindgen(getter)]
    pub fn id_hex(&self) -> String {
        format!("0x{}", hex::encode(self.id.as_slice()))
    }

    #[wasm_bindgen(getter)]
    pub fn owner(&self) -> js_sys::Uint8Array {
        js_sys::Uint8Array::from(self.owner.as_slice())
    }

    #[wasm_bindgen(getter)]
    pub fn owner_hex(&self) -> String {
        format!("0x{}", hex::encode(self.owner.as_slice()))
    }

    #[wasm_bindgen(getter)]
    pub fn data(&self) -> js_sys::Uint8Array {
        js_sys::Uint8Array::from(self.data.as_slice())
    }

    #[wasm_bindgen(getter)]
    pub fn serialized(&self) -> js_sys::Uint8Array {
        js_sys::Uint8Array::from(self.serialized.as_slice())
    }

    #[wasm_bindgen(getter)]
    pub fn serialized_hex(&self) -> String {
        format!("0x{}", hex::encode(&self.serialized))
    }

    #[wasm_bindgen(getter)]
    pub fn signature(&self) -> js_sys::Uint8Array {
        js_sys::Uint8Array::from(self.signature.as_slice())
    }

    #[wasm_bindgen(getter)]
    pub fn signature_hex(&self) -> String {
        format!("0x{}", hex::encode(&self.signature))
    }

    #[wasm_bindgen(getter)]
    pub fn size(&self) -> usize {
        self.serialized.len()
    }
}

/// Result of parsing and analyzing a chunk
#[wasm_bindgen]
pub struct ChunkAnalysisResult {
    is_valid: bool,
    chunk_type: ChunkType,
    address: B256,
    data: Vec<u8>,
    id: Option<B256>,
    owner: Option<Address>,
    signature: Option<Vec<u8>>,
    error_message: Option<String>,
}

#[wasm_bindgen]
impl ChunkAnalysisResult {
    #[wasm_bindgen(getter)]
    pub fn is_valid(&self) -> bool {
        self.is_valid
    }

    #[wasm_bindgen(getter)]
    pub fn chunk_type(&self) -> ChunkType {
        self.chunk_type
    }

    #[wasm_bindgen(getter)]
    pub fn address(&self) -> js_sys::Uint8Array {
        js_sys::Uint8Array::from(self.address.as_slice())
    }

    #[wasm_bindgen(getter)]
    pub fn address_hex(&self) -> String {
        format!("0x{}", hex::encode(self.address.as_slice()))
    }

    #[wasm_bindgen(getter)]
    pub fn data(&self) -> js_sys::Uint8Array {
        js_sys::Uint8Array::from(self.data.as_slice())
    }

    #[wasm_bindgen(getter)]
    pub fn has_id(&self) -> bool {
        self.id.is_some()
    }

    #[wasm_bindgen(getter)]
    pub fn id(&self) -> Option<js_sys::Uint8Array> {
        self.id
            .as_ref()
            .map(|id| js_sys::Uint8Array::from(id.as_slice()))
    }

    #[wasm_bindgen(getter)]
    pub fn id_hex(&self) -> Option<String> {
        self.id
            .as_ref()
            .map(|id| format!("0x{}", hex::encode(id.as_slice())))
    }

    #[wasm_bindgen(getter)]
    pub fn has_owner(&self) -> bool {
        self.owner.is_some()
    }

    #[wasm_bindgen(getter)]
    pub fn owner(&self) -> Option<js_sys::Uint8Array> {
        self.owner
            .as_ref()
            .map(|owner| js_sys::Uint8Array::from(owner.as_slice()))
    }

    #[wasm_bindgen(getter)]
    pub fn owner_hex(&self) -> Option<String> {
        self.owner
            .as_ref()
            .map(|owner| format!("0x{}", hex::encode(owner.as_slice())))
    }

    #[wasm_bindgen(getter)]
    pub fn has_signature(&self) -> bool {
        self.signature.is_some()
    }

    #[wasm_bindgen(getter)]
    pub fn signature(&self) -> Option<js_sys::Uint8Array> {
        self.signature
            .as_ref()
            .map(|sig| js_sys::Uint8Array::from(sig.as_slice()))
    }

    #[wasm_bindgen(getter)]
    pub fn signature_hex(&self) -> Option<String> {
        self.signature
            .as_ref()
            .map(|sig| format!("0x{}", hex::encode(sig.as_slice())))
    }

    #[wasm_bindgen(getter)]
    pub fn has_error(&self) -> bool {
        self.error_message.is_some()
    }

    #[wasm_bindgen(getter)]
    pub fn error_message(&self) -> Option<String> {
        self.error_message.clone()
    }
}

/// Create a ContentChunk from data
///
/// @param {Uint8Array} data - Data to include in the chunk
/// @returns {ContentChunkResult} Result of the chunk creation
#[wasm_bindgen]
pub fn create_content_chunk(data: &[u8]) -> Result<ContentChunkResult, JsValue> {
    set_panic_hook();

    // Create the content chunk
    let chunk = match ContentChunk::new(data.to_vec()) {
        Ok(chunk) => chunk,
        Err(e) => {
            return Err(JsValue::from_str(&format!(
                "Failed to create content chunk: {}",
                e
            )))
        }
    };

    // Serialize the chunk - use the From trait
    let serialized = Bytes::from(chunk.clone()).to_vec();

    // Return the result
    Ok(ContentChunkResult {
        address: *chunk.address().deref(),
        data: data.to_vec(),
        serialized,
    })
}

/// Create a new random private key for signing
///
/// @returns {Uint8Array} A random private key (32 bytes)
#[wasm_bindgen]
pub fn generate_random_private_key() -> js_sys::Uint8Array {
    let signer = PrivateKeySigner::random();
    let private_key = signer.to_bytes();
    js_sys::Uint8Array::from(private_key.as_slice())
}

/// Get the address from a private key
///
/// @param {Uint8Array} private_key - Private key bytes
/// @returns {Uint8Array} The corresponding address (20 bytes)
#[wasm_bindgen]
pub fn get_address_from_private_key(private_key: &[u8]) -> Result<js_sys::Uint8Array, JsValue> {
    // Create signer from private key
    let signer = match PrivateKeySigner::from_slice(private_key) {
        Ok(signer) => signer,
        Err(e) => return Err(JsValue::from_str(&format!("Invalid private key: {}", e))),
    };

    // Get the address
    let address = signer.address();
    Ok(js_sys::Uint8Array::from(address.as_slice()))
}

/// Create a SingleOwnerChunk from data and private key
///
/// @param {Uint8Array} id - Chunk ID (32 bytes)
/// @param {Uint8Array} data - Data to include in the chunk
/// @param {Uint8Array} private_key - Private key for signing
/// @returns {SingleOwnerChunkResult} Result of the chunk creation
#[wasm_bindgen]
pub fn create_single_owner_chunk(
    id: &[u8],
    data: &[u8],
    private_key: &[u8],
) -> Result<SingleOwnerChunkResult, JsValue> {
    set_panic_hook();

    // Check ID length
    if id.len() != 32 {
        return Err(JsValue::from_str("ID must be exactly 32 bytes"));
    }

    // Create B256 from ID
    let mut chunk_id = B256::default();
    chunk_id.copy_from_slice(id);

    // Create signer from private key
    let signer = match PrivateKeySigner::from_slice(private_key) {
        Ok(signer) => signer,
        Err(e) => return Err(JsValue::from_str(&format!("Invalid private key: {}", e))),
    };

    // Create the single owner chunk
    let chunk = match SingleOwnerChunk::new(chunk_id, data.to_vec(), &signer) {
        Ok(chunk) => chunk,
        Err(e) => {
            return Err(JsValue::from_str(&format!(
                "Failed to create single owner chunk: {}",
                e
            )))
        }
    };

    // Get owner address
    let owner = chunk
        .owner()
        .map_err(|e| JsValue::from_str(&format!("Failed to recover owner: {}", e)))?;

    // Serialize the chunk - use the From trait
    let serialized: Vec<u8> = Bytes::from(chunk.clone()).to_vec();

    // Return the result
    Ok(SingleOwnerChunkResult {
        address: *chunk.address().deref(),
        id: chunk_id,
        owner,
        data: data.to_vec(),
        serialized,
        signature: chunk.signature().as_bytes().to_vec(),
    })
}

/// Generate a random chunk ID (32 bytes)
///
/// @returns {Uint8Array} A randomly generated 32-byte chunk ID
#[wasm_bindgen]
pub fn generate_random_chunk_id() -> js_sys::Uint8Array {
    let mut bytes = [0u8; 32];
    getrandom::getrandom(&mut bytes).expect("Failed to generate random bytes");
    js_sys::Uint8Array::from(&bytes[..])
}

/// Analyze a chunk and determine its type and properties
///
/// @param {Uint8Array} chunk_data - Serialized chunk data
/// @param {Uint8Array} expected_address - Expected address for verification (32 bytes)
/// @returns {ChunkAnalysisResult} Analysis result
#[wasm_bindgen]
pub fn analyze_chunk(
    chunk_data: &[u8],
    expected_address: &[u8],
) -> Result<ChunkAnalysisResult, JsValue> {
    set_panic_hook();

    // Check expected address length
    if expected_address.len() != 32 {
        return Err(JsValue::from_str(
            "Expected address must be exactly 32 bytes",
        ));
    }

    // Create ChunkAddress from expected address
    let mut address_bytes = [0u8; 32];
    address_bytes.copy_from_slice(expected_address);
    let expected = ChunkAddress::new(address_bytes);

    // Try to parse as ContentChunk
    let content_result = ContentChunk::try_from(chunk_data);
    let single_owner_result = SingleOwnerChunk::try_from(chunk_data);

    // Prioritize any successful parse that matches the expected address
    match (content_result, single_owner_result) {
        (Ok(content_chunk), _) if content_chunk.address() == &expected => {
            return Ok(ChunkAnalysisResult {
                is_valid: true,
                chunk_type: ChunkType::Content,
                address: *content_chunk.address().deref(),
                data: content_chunk.data().to_vec(),
                id: None,
                owner: None,
                signature: None,
                error_message: None,
            });
        }
        (_, Ok(single_owner_chunk)) if single_owner_chunk.address() == &expected => {
            return Ok(ChunkAnalysisResult {
                is_valid: true,
                chunk_type: ChunkType::SingleOwner,
                address: *single_owner_chunk.address().deref(),
                data: single_owner_chunk.data().to_vec(),
                id: Some(single_owner_chunk.id()),
                owner: single_owner_chunk.owner().ok(),
                signature: Some(single_owner_chunk.signature().as_bytes().to_vec()),
                error_message: None,
            });
        }
        // Return any successful parse with address mismatch
        (Ok(content_chunk), _) => {
            return Ok(ChunkAnalysisResult {
                is_valid: false,
                chunk_type: ChunkType::Content,
                address: *content_chunk.address().deref(),
                data: content_chunk.data().to_vec(),
                id: None,
                owner: None,
                signature: None,
                error_message: Some(format!(
                    "Content chunk address mismatch. Expected: 0x{}, Actual: 0x{}",
                    hex::encode(expected.as_bytes()),
                    hex::encode(content_chunk.address().as_bytes())
                )),
            });
        }
        (_, Ok(single_owner_chunk)) => {
            return Ok(ChunkAnalysisResult {
                is_valid: false,
                chunk_type: ChunkType::SingleOwner,
                address: *single_owner_chunk.address().deref(),
                data: single_owner_chunk.data().to_vec(),
                id: Some(single_owner_chunk.id()),
                owner: single_owner_chunk.owner().ok(),
                signature: Some(single_owner_chunk.signature().as_bytes().to_vec()),
                error_message: Some(format!(
                    "Single owner chunk address mismatch. Expected: 0x{}, Actual: 0x{}",
                    hex::encode(expected.as_bytes()),
                    hex::encode(single_owner_chunk.address().as_bytes())
                )),
            });
        }
        // Both failed to parse
        (Err(e1), Err(e2)) => {
            return Ok(ChunkAnalysisResult {
                is_valid: false,
                chunk_type: ChunkType::Unknown,
                address: B256::default(),
                data: chunk_data.to_vec(),
                id: None,
                owner: None,
                signature: None,
                error_message: Some(format!(
                    "Failed to parse chunk: ContentChunk error: {}, SingleOwnerChunk error: {}",
                    e1, e2
                )),
            });
        }
    }
}

/// Get SVG icon for a chunk address
///
/// @param {Uint8Array} address_bytes - Address bytes (32 bytes)
/// @param {IconConfig} config - Configuration for the icon
/// @returns {string} SVG content representing the address
#[wasm_bindgen]
pub fn generate_svg_for_address(
    address_bytes: &[u8],
    config: &IconConfig,
) -> Result<String, JsValue> {
    set_panic_hook();

    // Check address length
    if address_bytes.len() != 32 {
        return Err(JsValue::from_str("Address must be exactly 32 bytes"));
    }

    // Create a very simple IconData with just the address
    let icon_data = IconData::new(
        address_bytes,
        0,   // Default type
        1,   // Default version
        &[], // Empty header
        &[], // Empty payload
    )?;

    // Generate the SVG
    Ok(generate_svg_icon(&icon_data, config))
}
