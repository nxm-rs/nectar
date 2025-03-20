use alloy_primitives::{hex, Bytes, FixedBytes};
use digest::Digest;
use nectar_primitives::bmt::BMTHasher;
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
#[wasm_bindgen]
pub fn calculate_bmt_hash(text: &str, span: u32) -> HashResult {
    // Set panic hook for better debugging
    set_panic_hook();

    // Create a BMT hasher
    let mut hasher = BMTHasher::new();

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
        let mut hasher = BMTHasher::new();
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
        let mut hasher = BMTHasher::new();
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
#[wasm_bindgen]
pub fn get_library_info() -> String {
    "BMT Hash Calculator powered by nectar-primitives - WASM Demo".to_string()
}

//------------------------------------------------------------------------------
// SVG Icon Generator
//------------------------------------------------------------------------------

#[wasm_bindgen]
#[derive(Clone, Copy)]
pub enum GeneratorFunction {
    Geometric,
    Abstract,
    Circular,
    Pixelated,
    Molecular,
}

#[wasm_bindgen]
#[derive(Clone, Copy)]
pub enum IconShape {
    Square,
    Circle,
}

#[wasm_bindgen]
#[derive(Clone, Copy)]
pub enum ColorScheme {
    Vibrant,
    Pastel,
    Monochrome,
    Complementary,
}

#[wasm_bindgen]
pub struct IconConfig {
    size: u32,
    shape: IconShape,
    generator: GeneratorFunction,
    color_scheme: ColorScheme,
}

#[wasm_bindgen]
impl IconConfig {
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
#[wasm_bindgen]
pub struct IconData {
    address: FixedBytes<32>,
    chunk_type: u8,
    version: u8,
    header: Bytes,
    payload: Bytes,
}

#[wasm_bindgen]
impl IconData {
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

        // Create FixedBytes<32> from bytes
        let mut address = FixedBytes::<32>::default();
        address.copy_from_slice(address_bytes);

        // Create Bytes from slices
        let header = Bytes::copy_from_slice(header_bytes);
        let payload = Bytes::copy_from_slice(payload_bytes);

        Ok(IconData {
            address,
            chunk_type,
            version,
            header,
            payload,
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
        js_sys::Uint8Array::from(self.header.as_ref())
    }

    #[wasm_bindgen(getter)]
    pub fn payload(&self) -> js_sys::Uint8Array {
        js_sys::Uint8Array::from(self.payload.as_ref())
    }
}

/// Create a IconData instance from hex strings (convenience function for JS)
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
#[wasm_bindgen]
pub fn generate_random_chunk_address() -> js_sys::Uint8Array {
    let mut bytes = [0u8; 32];
    getrandom::getrandom(&mut bytes).expect("Failed to generate random bytes");
    js_sys::Uint8Array::from(&bytes[..])
}

/// Generate an SVG icon based on IconData and configuration
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
