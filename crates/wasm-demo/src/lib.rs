//! WASM demo for parallel BMT hashing, chunk generation, and postage stamping.
//!
//! This crate provides WASM bindings for parallel processing using rayon + Web Workers.
//!
//! # Usage
//!
//! ```javascript
//! import init, { initThreadPool, splitAndStampParallel } from './nectar_wasm_demo.js';
//!
//! await init();
//! await initThreadPool(navigator.hardwareConcurrency);
//!
//! const result = splitAndStampParallel(data);
//! console.log('Root hash:', result.rootHash);
//! console.log('Stamps:', result.stamps.length);
//! ```
//!
//! Uses [wasm-bindgen-rayon](https://github.com/RReverser/wasm-bindgen-rayon) for
//! rayon-based parallelism via SharedArrayBuffer and Web Workers.

use alloy_primitives::B256;
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use js_sys::{Array, Date, Object, Uint8Array};
use nectar_postage::{STAMP_SIZE, Stamp};
use nectar_postage_issuer::{BatchStamper, MemoryIssuer, StampIssuer};
use nectar_primitives::SwarmAddress;
use nectar_primitives::bmt::{BRANCHES, DEFAULT_BODY_SIZE, Hasher, Prover, SPAN_SIZE};
use nectar_primitives::chunk::AnyChunk;
use nectar_primitives::file::{self, SyncParallelSplitter};
use nectar_primitives::store::MemoryStore;
use rayon::prelude::*;
use std::sync::Mutex;
use wasm_bindgen::prelude::*;

// Re-export thread pool initialization from wasm-bindgen-rayon
pub use wasm_bindgen_rayon::init_thread_pool;

/// Initialize panic hook for better error messages in the browser console.
#[wasm_bindgen(start)]
pub fn init_panic_hook() {
    console_error_panic_hook::set_once();
}

/// Timestamp as nanoseconds since epoch (millisecond precision).
///
/// Uses JS Date API since std::time::SystemTime isn't supported in WASM with atomics.
fn wasm_timestamp() -> u64 {
    (Date::now() * 1_000_000.0) as u64
}

/// Calculate appropriate batch depth for a given number of chunks.
///
/// With bucket_depth=16 (65536 buckets), we need enough depth to handle
/// the birthday paradox - some buckets will get more chunks than average.
///
/// We use: depth = bucket_depth + ceil(log2(chunks)) + margin
/// This ensures bucket_capacity >> expected_chunks_per_bucket
fn calculate_batch_depth(chunk_count: usize) -> u8 {
    const BUCKET_DEPTH: u8 = 16;
    const MARGIN: u8 = 4; // Extra bits for birthday paradox safety

    if chunk_count == 0 {
        return BUCKET_DEPTH + 1;
    }

    // log2(chunk_count) rounded up
    let log2_chunks = (usize::BITS - chunk_count.leading_zeros()) as u8;

    // depth = bucket_depth + log2(chunks) + margin
    // This gives bucket_capacity = 2^(log2_chunks + margin) = chunks * 16
    // So each bucket can hold 16x the average, which handles birthday paradox well
    (BUCKET_DEPTH + log2_chunks + MARGIN).min(32) // Cap at 32 for sanity
}

/// Get the body size constant (4096 bytes).
#[allow(clippy::missing_const_for_fn)] // wasm_bindgen doesn't support const fn
#[wasm_bindgen(js_name = getBodySize)]
pub fn get_body_size() -> u32 {
    DEFAULT_BODY_SIZE as u32
}

/// Get the span size constant (8 bytes).
#[allow(clippy::missing_const_for_fn)] // wasm_bindgen doesn't support const fn
#[wasm_bindgen(js_name = getSpanSize)]
pub fn get_span_size() -> usize {
    SPAN_SIZE
}

/// Get the number of branches in the BMT (128).
#[allow(clippy::missing_const_for_fn)] // wasm_bindgen doesn't support const fn
#[wasm_bindgen(js_name = getBranches)]
pub fn get_branches() -> usize {
    BRANCHES
}

/// Hash a single data chunk with the given span, returning a 32-byte BMT hash.
///
/// Core function for Web Workers to call for parallel hashing.
#[wasm_bindgen(js_name = hashChunk)]
pub fn hash_chunk(data: &Uint8Array, span: u64) -> Result<Uint8Array, JsValue> {
    let data_vec = data.to_vec();

    if data_vec.len() > DEFAULT_BODY_SIZE {
        return Err(JsValue::from_str(&format!(
            "Data exceeds maximum chunk size: {} > {}",
            data_vec.len(),
            DEFAULT_BODY_SIZE
        )));
    }

    let mut hasher = Hasher::<DEFAULT_BODY_SIZE>::new();
    hasher.set_span(span);
    hasher.update(&data_vec);
    let hash = hasher.sum();

    let result = Uint8Array::new_with_length(32);
    result.copy_from(hash.as_slice());
    Ok(result)
}

/// Hash multiple data chunks, returning array of 32-byte hashes.
///
/// Takes an array of `{data: Uint8Array, span: number}` objects.
#[wasm_bindgen(js_name = hashChunkBatch)]
pub fn hash_chunk_batch(chunks: &Array) -> Result<Array, JsValue> {
    let results = Array::new();

    for i in 0..chunks.length() {
        let chunk_obj = chunks.get(i);
        let data = js_sys::Reflect::get(&chunk_obj, &"data".into())?;
        let span = js_sys::Reflect::get(&chunk_obj, &"span".into())?;

        let data_array: Uint8Array = data.dyn_into()?;
        let span_val: f64 = span
            .as_f64()
            .ok_or_else(|| JsValue::from_str("Invalid span"))?;

        let hash = hash_chunk(&data_array, span_val as u64)?;
        results.push(&hash);
    }

    Ok(results)
}

/// Calculate intermediate chunk hash from child hashes.
///
/// Used for building the Merkle tree from leaf hashes.
///
/// Takes up to BRANCHES (128) child hashes of 32 bytes each.
#[wasm_bindgen(js_name = hashIntermediateChunk)]
pub fn hash_intermediate_chunk(child_hashes: &Array, span: u64) -> Result<Uint8Array, JsValue> {
    if child_hashes.length() > BRANCHES as u32 {
        return Err(JsValue::from_str(&format!(
            "Too many child hashes (max {BRANCHES})"
        )));
    }

    // Build reference data from child hashes
    let mut data = Vec::with_capacity(child_hashes.length() as usize * 32);
    for i in 0..child_hashes.length() {
        let hash: Uint8Array = child_hashes.get(i).dyn_into()?;
        if hash.length() != 32 {
            return Err(JsValue::from_str("Each hash must be 32 bytes"));
        }
        data.extend_from_slice(&hash.to_vec());
    }

    let mut hasher = Hasher::<DEFAULT_BODY_SIZE>::new();
    hasher.set_span(span);
    hasher.update(&data);
    let hash = hasher.sum();

    let result = Uint8Array::new_with_length(32);
    result.copy_from(hash.as_slice());
    Ok(result)
}

/// Build JS result object for split operations.
fn build_split_result(
    root: &[u8],
    chunks: &[AnyChunk<DEFAULT_BODY_SIZE>],
) -> Result<JsValue, JsValue> {
    let result = Object::new();

    let root_hash = Uint8Array::new_with_length(32);
    root_hash.copy_from(root);
    js_sys::Reflect::set(&result, &"rootHash".into(), &root_hash)?;

    let chunks_array = Array::new();
    for chunk in chunks {
        let chunk_obj = Object::new();

        let address = Uint8Array::new_with_length(32);
        address.copy_from(chunk.address().as_slice());
        js_sys::Reflect::set(&chunk_obj, &"address".into(), &address)?;

        let chunk_data = Uint8Array::new_with_length(chunk.data().len() as u32);
        chunk_data.copy_from(chunk.data());
        js_sys::Reflect::set(&chunk_obj, &"data".into(), &chunk_data)?;

        js_sys::Reflect::set(&chunk_obj, &"span".into(), &JsValue::from(chunk.span()))?;

        chunks_array.push(&chunk_obj);
    }
    js_sys::Reflect::set(&result, &"chunks".into(), &chunks_array)?;
    js_sys::Reflect::set(&result, &"chunkCount".into(), &JsValue::from(chunks.len()))?;

    Ok(result.into())
}

/// Split file data into chunks (sequential version).
#[wasm_bindgen(js_name = splitFileSequential)]
pub fn split_file_sequential(data: &Uint8Array) -> Result<JsValue, JsValue> {
    let bytes = data.to_vec();
    let (root, store) = file::sync_split::<DEFAULT_BODY_SIZE>(&bytes)
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    let chunks: Vec<_> = store.into_chunks().into_values().collect();

    build_split_result(root.as_slice(), &chunks)
}

/// Split file data into chunks using parallel processing (rayon).
///
/// Requires `initThreadPool(numThreads)` to be called first.
#[wasm_bindgen(js_name = splitFileParallel)]
pub fn split_file_parallel(data: &Uint8Array) -> Result<JsValue, JsValue> {
    let bytes = data.to_vec();

    let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
    let splitter = SyncParallelSplitter::new(store);

    let root = splitter
        .split(&bytes)
        .map_err(|e| JsValue::from_str(&e.to_string()))?;

    let chunks: Vec<_> = splitter.into_store().into_chunks().into_values().collect();

    build_split_result(root.as_slice(), &chunks)
}

/// Calculate tree parameters for a given file size.
///
/// Returns an object with:
/// - `dataChunks`: Number of leaf (data) chunks
/// - `totalChunks`: Total chunks including intermediate nodes
/// - `depth`: Tree depth
/// - `chunkOffsets`: Array of {offset, size, span} for each data chunk
#[wasm_bindgen(js_name = calculateTreeParams)]
pub fn calculate_tree_params(size: u64) -> Result<JsValue, JsValue> {
    let tree = file::TreeParams::<DEFAULT_BODY_SIZE>::new(size);

    let result = Object::new();
    js_sys::Reflect::set(
        &result,
        &"dataChunks".into(),
        &JsValue::from(tree.data_chunks()),
    )?;
    js_sys::Reflect::set(
        &result,
        &"totalChunks".into(),
        &JsValue::from(tree.total_chunks()),
    )?;
    js_sys::Reflect::set(&result, &"depth".into(), &JsValue::from(tree.depth()))?;

    // Calculate offsets for data chunks
    let offsets = Array::new();
    let data_chunks = tree.data_chunks();

    for i in 0..data_chunks {
        let offset = i * DEFAULT_BODY_SIZE as u64;
        let chunk_size = ((size - offset) as usize).min(DEFAULT_BODY_SIZE);
        let span = if i + 1 == data_chunks {
            size - offset // Last chunk
        } else {
            DEFAULT_BODY_SIZE as u64
        };

        let chunk_info = Object::new();
        js_sys::Reflect::set(&chunk_info, &"index".into(), &JsValue::from(i))?;
        js_sys::Reflect::set(&chunk_info, &"offset".into(), &JsValue::from(offset))?;
        js_sys::Reflect::set(&chunk_info, &"size".into(), &JsValue::from(chunk_size))?;
        js_sys::Reflect::set(&chunk_info, &"span".into(), &JsValue::from(span))?;
        offsets.push(&chunk_info);
    }
    js_sys::Reflect::set(&result, &"chunkOffsets".into(), &offsets)?;

    Ok(result.into())
}

/// Generate a proof for a specific segment.
#[wasm_bindgen(js_name = generateProof)]
pub fn generate_proof(data: &Uint8Array, segment_index: usize) -> Result<JsValue, JsValue> {
    let data_vec = data.to_vec();

    let mut hasher = Hasher::<DEFAULT_BODY_SIZE>::new();
    hasher.set_span(data_vec.len() as u64);
    hasher.update(&data_vec);

    let proof = hasher
        .generate_proof(&data_vec, segment_index)
        .map_err(|e| JsValue::from_str(&e.to_string()))?;

    let result = Object::new();

    js_sys::Reflect::set(
        &result,
        &"segmentIndex".into(),
        &JsValue::from(proof.segment_index),
    )?;

    let segment = Uint8Array::new_with_length(32);
    segment.copy_from(proof.segment.as_slice());
    js_sys::Reflect::set(&result, &"segment".into(), &segment)?;

    let proof_segments = Array::new();
    for seg in &proof.proof_segments {
        let seg_array = Uint8Array::new_with_length(32);
        seg_array.copy_from(seg.as_slice());
        proof_segments.push(&seg_array);
    }
    js_sys::Reflect::set(&result, &"proofSegments".into(), &proof_segments)?;

    js_sys::Reflect::set(&result, &"span".into(), &JsValue::from(proof.span))?;

    Ok(result.into())
}

/// Verify a proof against a root hash.
#[wasm_bindgen(js_name = verifyProof)]
pub fn verify_proof(
    segment_index: usize,
    segment: &Uint8Array,
    proof_segments: &Array,
    span: u64,
    root_hash: &Uint8Array,
) -> Result<bool, JsValue> {
    use alloy_primitives::B256;
    use nectar_primitives::Proof;

    // Reconstruct proof
    let segment_bytes: [u8; 32] = segment
        .to_vec()
        .try_into()
        .map_err(|_| JsValue::from_str("Invalid segment length"))?;

    let mut proof_segs = Vec::new();
    for i in 0..proof_segments.length() {
        let seg: Uint8Array = proof_segments.get(i).dyn_into()?;
        let seg_bytes: [u8; 32] = seg
            .to_vec()
            .try_into()
            .map_err(|_| JsValue::from_str("Invalid proof segment length"))?;
        proof_segs.push(B256::from(seg_bytes));
    }

    let proof = Proof::new(
        segment_index,
        B256::from(segment_bytes),
        proof_segs,
        span,
        None,
    );

    Hasher::<DEFAULT_BODY_SIZE>::verify_proof(&proof, &root_hash.to_vec())
        .map_err(|e| JsValue::from_str(&e.to_string()))
}

/// Performance timing helper using web Performance API.
#[wasm_bindgen(js_name = now)]
pub fn now() -> f64 {
    web_sys::window()
        .and_then(|w| w.performance())
        .map(|p| p.now())
        .unwrap_or(0.0)
}

/// Log a message to the browser console.
#[wasm_bindgen(js_name = consoleLog)]
pub fn console_log(msg: &str) {
    web_sys::console::log_1(&JsValue::from_str(msg));
}

/// Split file and stamp all chunks using a random signer.
///
/// This demonstrates the full upload pipeline: splitting + ECDSA signing.
///
/// Returns an object containing:
/// - `rootHash`: The root BMT hash (Uint8Array)
/// - `chunkCount`: Number of chunks generated
/// - `stamps`: Array of stamp objects with {batchId, bucket, index, timestamp, signature}
/// - `signerAddress`: The Ethereum address of the random signer
/// - `splitTimeMs`: Time spent on splitting
/// - `stampTimeMs`: Time spent on ECDSA signing
#[wasm_bindgen(js_name = splitAndStampSequential)]
pub fn split_and_stamp_sequential(data: &Uint8Array) -> Result<JsValue, JsValue> {
    let bytes = data.to_vec();

    // Split file (timed)
    let split_start = now();
    let (root, store) = file::sync_split::<DEFAULT_BODY_SIZE>(&bytes)
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    let chunks: Vec<_> = store.into_chunks().into_values().collect();
    let split_time = now() - split_start;

    // Create random signer
    let signer = PrivateKeySigner::random();
    let signer_address = signer.address();

    // Create batch with depth sized for chunk count (handles birthday paradox)
    let batch_id = B256::random();
    let depth = calculate_batch_depth(chunks.len());
    let mut issuer = MemoryIssuer::new(batch_id, depth, 16);

    // Stamp all chunks using WASM-compatible timestamp (timed)
    let stamp_start = now();
    let mut stamps = Vec::with_capacity(chunks.len());
    for chunk in &chunks {
        let timestamp = wasm_timestamp();
        let digest = issuer
            .prepare_stamp(chunk.address(), timestamp)
            .map_err(|e| JsValue::from_str(&format!("Prepare stamp failed: {e}")))?;

        let prehash = digest.to_prehash();
        let sig = signer
            .sign_message_sync(prehash.as_slice())
            .map_err(|e| JsValue::from_str(&format!("Signing failed: {e}")))?;

        let stamp =
            BatchStamper::<MemoryIssuer, PrivateKeySigner>::stamp_from_signature(&digest, sig);
        stamps.push(stamp);
    }
    let stamp_time = now() - stamp_start;

    build_stamp_result(
        root.as_slice(),
        &chunks,
        &stamps,
        signer_address.as_slice(),
        split_time,
        stamp_time,
    )
}

/// Split file and stamp all chunks in parallel using rayon.
///
/// This demonstrates parallel ECDSA signing across web workers.
///
/// Returns an object containing:
/// - `rootHash`: The root BMT hash (Uint8Array)
/// - `chunkCount`: Number of chunks generated
/// - `stamps`: Array of stamp objects
/// - `signerAddress`: The Ethereum address of the random signer
/// - `splitTimeMs`: Time spent on parallel splitting
/// - `stampTimeMs`: Time spent on parallel ECDSA signing
#[wasm_bindgen(js_name = splitAndStampParallel)]
pub fn split_and_stamp_parallel(data: &Uint8Array) -> Result<JsValue, JsValue> {
    let bytes = data.to_vec();

    // Use parallel splitter (timed)
    let split_start = now();
    let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
    let splitter = SyncParallelSplitter::new(store);

    let root = splitter
        .split(&bytes)
        .map_err(|e| JsValue::from_str(&e.to_string()))?;

    let chunks: Vec<_> = splitter.into_store().into_chunks().into_values().collect();
    let split_time = now() - split_start;

    // Create random signer
    let signer = PrivateKeySigner::random();
    let signer_address = signer.address();

    // Create batch with depth sized for chunk count (handles birthday paradox)
    let batch_id = B256::random();
    let depth = calculate_batch_depth(chunks.len());

    // Parallel stamping using rayon
    let stamp_start = now();

    // Collect chunk addresses for parallel processing
    let chunk_addresses: Vec<&SwarmAddress> = chunks.iter().map(|c| c.address()).collect();

    // Use mutex-wrapped issuer for thread-safe stamping
    let issuer = Mutex::new(MemoryIssuer::new(batch_id, depth, 16));

    let stamps: Result<Vec<Stamp>, String> = chunk_addresses
        .par_iter()
        .map(|&address| {
            // Get timestamp using WASM-compatible API
            let timestamp = wasm_timestamp();

            // Prepare digest (needs mutex for issuer state)
            let digest = {
                let mut guard = issuer.lock().unwrap();
                guard
                    .prepare_stamp(address, timestamp)
                    .map_err(|e| format!("Prepare stamp failed: {e}"))?
            };

            // Sign (can be done in parallel - this is the expensive part)
            let prehash = digest.to_prehash();
            let sig = signer
                .sign_message_sync(prehash.as_slice())
                .map_err(|e| format!("Signing failed: {e}"))?;

            Ok(BatchStamper::<MemoryIssuer, PrivateKeySigner>::stamp_from_signature(&digest, sig))
        })
        .collect();

    let stamps = stamps.map_err(|e| JsValue::from_str(&e))?;
    let stamp_time = now() - stamp_start;

    build_stamp_result(
        root.as_slice(),
        &chunks,
        &stamps,
        signer_address.as_slice(),
        split_time,
        stamp_time,
    )
}

/// Helper to build the JS result object for stamping functions.
fn build_stamp_result(
    root: &[u8],
    chunks: &[nectar_primitives::chunk::AnyChunk<DEFAULT_BODY_SIZE>],
    stamps: &[Stamp],
    signer_address: &[u8],
    split_time: f64,
    stamp_time: f64,
) -> Result<JsValue, JsValue> {
    let result = Object::new();

    // Root hash
    let root_hash = Uint8Array::new_with_length(32);
    root_hash.copy_from(root);
    js_sys::Reflect::set(&result, &"rootHash".into(), &root_hash)?;

    // Chunk count
    js_sys::Reflect::set(&result, &"chunkCount".into(), &JsValue::from(chunks.len()))?;

    // Chunks array
    let chunks_array = Array::new();
    for chunk in chunks {
        let chunk_obj = Object::new();

        let address = Uint8Array::new_with_length(32);
        address.copy_from(chunk.address().as_slice());
        js_sys::Reflect::set(&chunk_obj, &"address".into(), &address)?;

        let chunk_data = Uint8Array::new_with_length(chunk.data().len() as u32);
        chunk_data.copy_from(chunk.data());
        js_sys::Reflect::set(&chunk_obj, &"data".into(), &chunk_data)?;

        js_sys::Reflect::set(&chunk_obj, &"span".into(), &JsValue::from(chunk.span()))?;

        chunks_array.push(&chunk_obj);
    }
    js_sys::Reflect::set(&result, &"chunks".into(), &chunks_array)?;

    // Stamps array
    let stamps_array = Array::new();
    for stamp in stamps {
        let stamp_obj = Object::new();

        let batch_id = Uint8Array::new_with_length(32);
        batch_id.copy_from(stamp.batch().as_slice());
        js_sys::Reflect::set(&stamp_obj, &"batchId".into(), &batch_id)?;

        js_sys::Reflect::set(&stamp_obj, &"bucket".into(), &JsValue::from(stamp.bucket()))?;
        js_sys::Reflect::set(&stamp_obj, &"index".into(), &JsValue::from(stamp.index()))?;
        js_sys::Reflect::set(
            &stamp_obj,
            &"timestamp".into(),
            &JsValue::from(stamp.timestamp() as f64),
        )?;

        let sig_bytes = stamp.signature().as_bytes();
        let signature = Uint8Array::new_with_length(65);
        signature.copy_from(&sig_bytes);
        js_sys::Reflect::set(&stamp_obj, &"signature".into(), &signature)?;

        // Also include full serialized stamp (113 bytes)
        let stamp_bytes = stamp.to_bytes();
        let full_stamp = Uint8Array::new_with_length(STAMP_SIZE as u32);
        full_stamp.copy_from(&stamp_bytes);
        js_sys::Reflect::set(&stamp_obj, &"bytes".into(), &full_stamp)?;

        stamps_array.push(&stamp_obj);
    }
    js_sys::Reflect::set(&result, &"stamps".into(), &stamps_array)?;

    // Signer address
    let addr = Uint8Array::new_with_length(20);
    addr.copy_from(signer_address);
    js_sys::Reflect::set(&result, &"signerAddress".into(), &addr)?;

    // Timing
    js_sys::Reflect::set(&result, &"splitTimeMs".into(), &JsValue::from(split_time))?;
    js_sys::Reflect::set(&result, &"stampTimeMs".into(), &JsValue::from(stamp_time))?;

    Ok(result.into())
}

/// Get the stamp size constant (113 bytes).
#[allow(clippy::missing_const_for_fn)] // wasm_bindgen doesn't support const fn
#[wasm_bindgen(js_name = getStampSize)]
pub fn get_stamp_size() -> u32 {
    STAMP_SIZE as u32
}

#[cfg(test)]
mod tests {
    use super::*;
    use wasm_bindgen_test::*;

    wasm_bindgen_test_configure!(run_in_browser);

    #[wasm_bindgen_test]
    fn test_hash_chunk() {
        let data = Uint8Array::from(&b"hello world"[..]);
        let result = hash_chunk(&data, 11).unwrap();
        assert_eq!(result.length(), 32);
    }

    #[wasm_bindgen_test]
    fn test_calculate_tree_params() {
        let params = calculate_tree_params(10000).unwrap();
        let obj: Object = params.dyn_into().unwrap();
        let data_chunks = js_sys::Reflect::get(&obj, &"dataChunks".into()).unwrap();
        assert!(data_chunks.as_f64().unwrap() >= 3.0);
    }
}
