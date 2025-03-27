let wasm;

function addToExternrefTable0(obj) {
    const idx = wasm.__externref_table_alloc();
    wasm.__wbindgen_export_2.set(idx, obj);
    return idx;
}

function handleError(f, args) {
    try {
        return f.apply(this, args);
    } catch (e) {
        const idx = addToExternrefTable0(e);
        wasm.__wbindgen_exn_store(idx);
    }
}

const cachedTextDecoder = (typeof TextDecoder !== 'undefined' ? new TextDecoder('utf-8', { ignoreBOM: true, fatal: true }) : { decode: () => { throw Error('TextDecoder not available') } } );

if (typeof TextDecoder !== 'undefined') { cachedTextDecoder.decode(); };

let cachedUint8ArrayMemory0 = null;

function getUint8ArrayMemory0() {
    if (cachedUint8ArrayMemory0 === null || cachedUint8ArrayMemory0.byteLength === 0) {
        cachedUint8ArrayMemory0 = new Uint8Array(wasm.memory.buffer);
    }
    return cachedUint8ArrayMemory0;
}

function getStringFromWasm0(ptr, len) {
    ptr = ptr >>> 0;
    return cachedTextDecoder.decode(getUint8ArrayMemory0().subarray(ptr, ptr + len));
}

let WASM_VECTOR_LEN = 0;

const cachedTextEncoder = (typeof TextEncoder !== 'undefined' ? new TextEncoder('utf-8') : { encode: () => { throw Error('TextEncoder not available') } } );

const encodeString = (typeof cachedTextEncoder.encodeInto === 'function'
    ? function (arg, view) {
    return cachedTextEncoder.encodeInto(arg, view);
}
    : function (arg, view) {
    const buf = cachedTextEncoder.encode(arg);
    view.set(buf);
    return {
        read: arg.length,
        written: buf.length
    };
});

function passStringToWasm0(arg, malloc, realloc) {

    if (realloc === undefined) {
        const buf = cachedTextEncoder.encode(arg);
        const ptr = malloc(buf.length, 1) >>> 0;
        getUint8ArrayMemory0().subarray(ptr, ptr + buf.length).set(buf);
        WASM_VECTOR_LEN = buf.length;
        return ptr;
    }

    let len = arg.length;
    let ptr = malloc(len, 1) >>> 0;

    const mem = getUint8ArrayMemory0();

    let offset = 0;

    for (; offset < len; offset++) {
        const code = arg.charCodeAt(offset);
        if (code > 0x7F) break;
        mem[ptr + offset] = code;
    }

    if (offset !== len) {
        if (offset !== 0) {
            arg = arg.slice(offset);
        }
        ptr = realloc(ptr, len, len = offset + arg.length * 3, 1) >>> 0;
        const view = getUint8ArrayMemory0().subarray(ptr + offset, ptr + len);
        const ret = encodeString(arg, view);

        offset += ret.written;
        ptr = realloc(ptr, len, offset, 1) >>> 0;
    }

    WASM_VECTOR_LEN = offset;
    return ptr;
}

let cachedDataViewMemory0 = null;

function getDataViewMemory0() {
    if (cachedDataViewMemory0 === null || cachedDataViewMemory0.buffer.detached === true || (cachedDataViewMemory0.buffer.detached === undefined && cachedDataViewMemory0.buffer !== wasm.memory.buffer)) {
        cachedDataViewMemory0 = new DataView(wasm.memory.buffer);
    }
    return cachedDataViewMemory0;
}

function isLikeNone(x) {
    return x === undefined || x === null;
}
/**
 * Compute a BMT hash for the given text and span
 *
 * @param {string} text - The input text to hash
 * @param {number} span - The span value to use (typically the length of the data)
 * @returns {HashResult} The computed hash result with hex and binary representations
 * @param {string} text
 * @param {number} span
 * @returns {HashResult}
 */
export function calculate_bmt_hash(text, span) {
    const ptr0 = passStringToWasm0(text, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
    const len0 = WASM_VECTOR_LEN;
    const ret = wasm.calculate_bmt_hash(ptr0, len0, span);
    return HashResult.__wrap(ret);
}

/**
 * Benchmark function that hashes data of a specific size
 *
 * @param {number} size - The size of data to hash in each iteration (in bytes)
 * @param {number} iterations - The number of hash operations to perform
 * @returns {number} The average time per hash operation in milliseconds
 * @param {number} size
 * @param {number} iterations
 * @returns {number}
 */
export function benchmark_hash(size, iterations) {
    const ret = wasm.benchmark_hash(size, iterations);
    return ret;
}

function passArray8ToWasm0(arg, malloc) {
    const ptr = malloc(arg.length * 1, 1) >>> 0;
    getUint8ArrayMemory0().set(arg, ptr / 1);
    WASM_VECTOR_LEN = arg.length;
    return ptr;
}
/**
 * Benchmark function that hashes pre-generated random data
 * Each iteration gets its own unique chunk of data
 *
 * @param {Uint8Array} data - Pre-generated random data buffer
 * @param {number} chunk_size - Size of each chunk to hash
 * @param {number} iterations - Number of hash operations to perform
 * @returns {number} The average time per hash operation in milliseconds, or -1 if error
 * @param {Uint8Array} data
 * @param {number} chunk_size
 * @param {number} iterations
 * @returns {number}
 */
export function benchmark_hash_with_random_data(data, chunk_size, iterations) {
    const ptr0 = passArray8ToWasm0(data, wasm.__wbindgen_malloc);
    const len0 = WASM_VECTOR_LEN;
    const ret = wasm.benchmark_hash_with_random_data(ptr0, len0, chunk_size, iterations);
    return ret;
}

/**
 * Utility function to help with debugging
 *
 * @returns {string} Information about the library version
 * @returns {string}
 */
export function get_library_info() {
    let deferred1_0;
    let deferred1_1;
    try {
        const ret = wasm.get_library_info();
        deferred1_0 = ret[0];
        deferred1_1 = ret[1];
        return getStringFromWasm0(ret[0], ret[1]);
    } finally {
        wasm.__wbindgen_free(deferred1_0, deferred1_1, 1);
    }
}

function takeFromExternrefTable0(idx) {
    const value = wasm.__wbindgen_export_2.get(idx);
    wasm.__externref_table_dealloc(idx);
    return value;
}
/**
 * Create a IconData instance from hex strings (convenience function for JS)
 *
 * @param {string} address_hex - 32-byte address as hex string
 * @param {string} type_hex - Chunk type as hex string (1 byte)
 * @param {string} version_hex - Version as hex string (1 byte)
 * @param {string} header_hex - Header data as hex string
 * @param {string} payload_hex - Payload data as hex string
 * @returns {IconData} A new IconData instance from the hex values
 * @param {string} address_hex
 * @param {string} type_hex
 * @param {string} version_hex
 * @param {string} header_hex
 * @param {string} payload_hex
 * @returns {IconData}
 */
export function create_icon_from_hex(address_hex, type_hex, version_hex, header_hex, payload_hex) {
    const ptr0 = passStringToWasm0(address_hex, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
    const len0 = WASM_VECTOR_LEN;
    const ptr1 = passStringToWasm0(type_hex, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
    const len1 = WASM_VECTOR_LEN;
    const ptr2 = passStringToWasm0(version_hex, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
    const len2 = WASM_VECTOR_LEN;
    const ptr3 = passStringToWasm0(header_hex, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
    const len3 = WASM_VECTOR_LEN;
    const ptr4 = passStringToWasm0(payload_hex, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
    const len4 = WASM_VECTOR_LEN;
    const ret = wasm.create_icon_from_hex(ptr0, len0, ptr1, len1, ptr2, len2, ptr3, len3, ptr4, len4);
    if (ret[2]) {
        throw takeFromExternrefTable0(ret[1]);
    }
    return IconData.__wrap(ret[0]);
}

/**
 * Generate a random chunk address (32 bytes)
 *
 * @returns {Uint8Array} A randomly generated 32-byte address
 * @returns {Uint8Array}
 */
export function generate_random_chunk_address() {
    const ret = wasm.generate_random_chunk_address();
    return ret;
}

function _assertClass(instance, klass) {
    if (!(instance instanceof klass)) {
        throw new Error(`expected instance of ${klass.name}`);
    }
}
/**
 * Generate an SVG icon based on IconData and configuration
 *
 * @param {IconData} data - The chunk data to visualize
 * @param {IconConfig} config - Configuration options for the icon
 * @returns {string} SVG content representing the chunk data
 * @param {IconData} data
 * @param {IconConfig} config
 * @returns {string}
 */
export function generate_svg_icon(data, config) {
    let deferred1_0;
    let deferred1_1;
    try {
        _assertClass(data, IconData);
        _assertClass(config, IconConfig);
        const ret = wasm.generate_svg_icon(data.__wbg_ptr, config.__wbg_ptr);
        deferred1_0 = ret[0];
        deferred1_1 = ret[1];
        return getStringFromWasm0(ret[0], ret[1]);
    } finally {
        wasm.__wbindgen_free(deferred1_0, deferred1_1, 1);
    }
}

/**
 * Create a builder for complex icon configuration
 *
 * @returns {IconConfigBuilder} A new icon config builder
 * @returns {IconConfigBuilder}
 */
export function create_icon_config_builder() {
    const ret = wasm.create_icon_config_builder();
    return IconConfigBuilder.__wrap(ret);
}

/**
 * Create a ContentChunk from data
 *
 * @param {Uint8Array} data - Data to include in the chunk
 * @returns {ContentChunkResult} Result of the chunk creation
 * @param {Uint8Array} data
 * @returns {ContentChunkResult}
 */
export function create_content_chunk(data) {
    const ptr0 = passArray8ToWasm0(data, wasm.__wbindgen_malloc);
    const len0 = WASM_VECTOR_LEN;
    const ret = wasm.create_content_chunk(ptr0, len0);
    if (ret[2]) {
        throw takeFromExternrefTable0(ret[1]);
    }
    return ContentChunkResult.__wrap(ret[0]);
}

/**
 * Create a new random private key for signing
 *
 * @returns {Uint8Array} A random private key (32 bytes)
 * @returns {Uint8Array}
 */
export function generate_random_private_key() {
    const ret = wasm.generate_random_private_key();
    return ret;
}

/**
 * Get the address from a private key
 *
 * @param {Uint8Array} private_key - Private key bytes
 * @returns {Uint8Array} The corresponding address (20 bytes)
 * @param {Uint8Array} private_key
 * @returns {Uint8Array}
 */
export function get_address_from_private_key(private_key) {
    const ptr0 = passArray8ToWasm0(private_key, wasm.__wbindgen_malloc);
    const len0 = WASM_VECTOR_LEN;
    const ret = wasm.get_address_from_private_key(ptr0, len0);
    if (ret[2]) {
        throw takeFromExternrefTable0(ret[1]);
    }
    return takeFromExternrefTable0(ret[0]);
}

/**
 * Create a SingleOwnerChunk from data and private key
 *
 * @param {Uint8Array} id - Chunk ID (32 bytes)
 * @param {Uint8Array} data - Data to include in the chunk
 * @param {Uint8Array} private_key - Private key for signing
 * @returns {SingleOwnerChunkResult} Result of the chunk creation
 * @param {Uint8Array} id
 * @param {Uint8Array} data
 * @param {Uint8Array} private_key
 * @returns {SingleOwnerChunkResult}
 */
export function create_single_owner_chunk(id, data, private_key) {
    const ptr0 = passArray8ToWasm0(id, wasm.__wbindgen_malloc);
    const len0 = WASM_VECTOR_LEN;
    const ptr1 = passArray8ToWasm0(data, wasm.__wbindgen_malloc);
    const len1 = WASM_VECTOR_LEN;
    const ptr2 = passArray8ToWasm0(private_key, wasm.__wbindgen_malloc);
    const len2 = WASM_VECTOR_LEN;
    const ret = wasm.create_single_owner_chunk(ptr0, len0, ptr1, len1, ptr2, len2);
    if (ret[2]) {
        throw takeFromExternrefTable0(ret[1]);
    }
    return SingleOwnerChunkResult.__wrap(ret[0]);
}

/**
 * Generate a random chunk ID (32 bytes)
 *
 * @returns {Uint8Array} A randomly generated 32-byte chunk ID
 * @returns {Uint8Array}
 */
export function generate_random_chunk_id() {
    const ret = wasm.generate_random_chunk_id();
    return ret;
}

/**
 * Analyze a chunk and determine its type and properties
 *
 * @param {Uint8Array} chunk_data - Serialized chunk data
 * @param {Uint8Array} expected_address - Expected address for verification (32 bytes)
 * @returns {ChunkAnalysisResult} Analysis result
 * @param {Uint8Array} chunk_data
 * @param {Uint8Array} expected_address
 * @returns {ChunkAnalysisResult}
 */
export function analyze_chunk(chunk_data, expected_address) {
    const ptr0 = passArray8ToWasm0(chunk_data, wasm.__wbindgen_malloc);
    const len0 = WASM_VECTOR_LEN;
    const ptr1 = passArray8ToWasm0(expected_address, wasm.__wbindgen_malloc);
    const len1 = WASM_VECTOR_LEN;
    const ret = wasm.analyze_chunk(ptr0, len0, ptr1, len1);
    if (ret[2]) {
        throw takeFromExternrefTable0(ret[1]);
    }
    return ChunkAnalysisResult.__wrap(ret[0]);
}

/**
 * Get SVG icon for a chunk address
 *
 * @param {Uint8Array} address_bytes - Address bytes (32 bytes)
 * @param {IconConfig} config - Configuration for the icon
 * @returns {string} SVG content representing the address
 * @param {Uint8Array} address_bytes
 * @param {IconConfig} config
 * @returns {string}
 */
export function generate_svg_for_address(address_bytes, config) {
    let deferred3_0;
    let deferred3_1;
    try {
        const ptr0 = passArray8ToWasm0(address_bytes, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        _assertClass(config, IconConfig);
        const ret = wasm.generate_svg_for_address(ptr0, len0, config.__wbg_ptr);
        var ptr2 = ret[0];
        var len2 = ret[1];
        if (ret[3]) {
            ptr2 = 0; len2 = 0;
            throw takeFromExternrefTable0(ret[2]);
        }
        deferred3_0 = ptr2;
        deferred3_1 = len2;
        return getStringFromWasm0(ptr2, len2);
    } finally {
        wasm.__wbindgen_free(deferred3_0, deferred3_1, 1);
    }
}

/**
 * Represents the type of a chunk
 * @enum {0 | 1 | 255}
 */
export const ChunkType = Object.freeze({
    /**
     * Content-addressed chunk (type 0)
     */
    Content: 0, "0": "Content",
    /**
     * Single-owner chunk (type 1)
     */
    SingleOwner: 1, "1": "SingleOwner",
    /**
     * Unknown chunk type
     */
    Unknown: 255, "255": "Unknown",
});
/**
 * Color scheme options for generated icons
 * @enum {0 | 1 | 2 | 3}
 */
export const ColorScheme = Object.freeze({
    /**
     * Bright, contrasting colors
     */
    Vibrant: 0, "0": "Vibrant",
    /**
     * Soft, muted colors
     */
    Pastel: 1, "1": "Pastel",
    /**
     * Black, white, and grayscale
     */
    Monochrome: 2, "2": "Monochrome",
    /**
     * Colors from opposite sides of the color wheel
     */
    Complementary: 3, "3": "Complementary",
});
/**
 * Generator function types for SVG icon generation
 * @enum {0 | 1 | 2 | 3 | 4}
 */
export const GeneratorFunction = Object.freeze({
    /**
     * Geometric patterns based on chunk data
     */
    Geometric: 0, "0": "Geometric",
    /**
     * Abstract art representation of chunk data
     */
    Abstract: 1, "1": "Abstract",
    /**
     * Circular design patterns
     */
    Circular: 2, "2": "Circular",
    /**
     * Pixelated grid representation
     */
    Pixelated: 3, "3": "Pixelated",
    /**
     * Molecular-style node and bond structure
     */
    Molecular: 4, "4": "Molecular",
});
/**
 * Shape options for generated icons
 * @enum {0 | 1}
 */
export const IconShape = Object.freeze({
    /**
     * Square icon (default)
     */
    Square: 0, "0": "Square",
    /**
     * Circular icon with clipping
     */
    Circle: 1, "1": "Circle",
});

const ChunkAnalysisResultFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_chunkanalysisresult_free(ptr >>> 0, 1));
/**
 * Result of parsing and analyzing a chunk
 */
export class ChunkAnalysisResult {

    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(ChunkAnalysisResult.prototype);
        obj.__wbg_ptr = ptr;
        ChunkAnalysisResultFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }

    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        ChunkAnalysisResultFinalization.unregister(this);
        return ptr;
    }

    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_chunkanalysisresult_free(ptr, 0);
    }
    /**
     * @returns {boolean}
     */
    get is_valid() {
        const ret = wasm.chunkanalysisresult_is_valid(this.__wbg_ptr);
        return ret !== 0;
    }
    /**
     * @returns {ChunkType}
     */
    get chunk_type() {
        const ret = wasm.chunkanalysisresult_chunk_type(this.__wbg_ptr);
        return ret;
    }
    /**
     * @returns {Uint8Array}
     */
    get address() {
        const ret = wasm.chunkanalysisresult_address(this.__wbg_ptr);
        return ret;
    }
    /**
     * @returns {string}
     */
    get address_hex() {
        let deferred1_0;
        let deferred1_1;
        try {
            const ret = wasm.chunkanalysisresult_address_hex(this.__wbg_ptr);
            deferred1_0 = ret[0];
            deferred1_1 = ret[1];
            return getStringFromWasm0(ret[0], ret[1]);
        } finally {
            wasm.__wbindgen_free(deferred1_0, deferred1_1, 1);
        }
    }
    /**
     * @returns {Uint8Array}
     */
    get data() {
        const ret = wasm.chunkanalysisresult_data(this.__wbg_ptr);
        return ret;
    }
    /**
     * @returns {boolean}
     */
    get has_id() {
        const ret = wasm.chunkanalysisresult_has_id(this.__wbg_ptr);
        return ret !== 0;
    }
    /**
     * @returns {Uint8Array | undefined}
     */
    get id() {
        const ret = wasm.chunkanalysisresult_id(this.__wbg_ptr);
        return ret;
    }
    /**
     * @returns {string | undefined}
     */
    get id_hex() {
        const ret = wasm.chunkanalysisresult_id_hex(this.__wbg_ptr);
        let v1;
        if (ret[0] !== 0) {
            v1 = getStringFromWasm0(ret[0], ret[1]).slice();
            wasm.__wbindgen_free(ret[0], ret[1] * 1, 1);
        }
        return v1;
    }
    /**
     * @returns {boolean}
     */
    get has_owner() {
        const ret = wasm.chunkanalysisresult_has_owner(this.__wbg_ptr);
        return ret !== 0;
    }
    /**
     * @returns {Uint8Array | undefined}
     */
    get owner() {
        const ret = wasm.chunkanalysisresult_owner(this.__wbg_ptr);
        return ret;
    }
    /**
     * @returns {string | undefined}
     */
    get owner_hex() {
        const ret = wasm.chunkanalysisresult_owner_hex(this.__wbg_ptr);
        let v1;
        if (ret[0] !== 0) {
            v1 = getStringFromWasm0(ret[0], ret[1]).slice();
            wasm.__wbindgen_free(ret[0], ret[1] * 1, 1);
        }
        return v1;
    }
    /**
     * @returns {boolean}
     */
    get has_signature() {
        const ret = wasm.chunkanalysisresult_has_signature(this.__wbg_ptr);
        return ret !== 0;
    }
    /**
     * @returns {Uint8Array | undefined}
     */
    get signature() {
        const ret = wasm.chunkanalysisresult_signature(this.__wbg_ptr);
        return ret;
    }
    /**
     * @returns {string | undefined}
     */
    get signature_hex() {
        const ret = wasm.chunkanalysisresult_signature_hex(this.__wbg_ptr);
        let v1;
        if (ret[0] !== 0) {
            v1 = getStringFromWasm0(ret[0], ret[1]).slice();
            wasm.__wbindgen_free(ret[0], ret[1] * 1, 1);
        }
        return v1;
    }
    /**
     * @returns {boolean}
     */
    get has_error() {
        const ret = wasm.chunkanalysisresult_has_error(this.__wbg_ptr);
        return ret !== 0;
    }
    /**
     * @returns {string | undefined}
     */
    get error_message() {
        const ret = wasm.chunkanalysisresult_error_message(this.__wbg_ptr);
        let v1;
        if (ret[0] !== 0) {
            v1 = getStringFromWasm0(ret[0], ret[1]).slice();
            wasm.__wbindgen_free(ret[0], ret[1] * 1, 1);
        }
        return v1;
    }
}

const ContentChunkResultFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_contentchunkresult_free(ptr >>> 0, 1));
/**
 * Result of creating a ContentChunk
 */
export class ContentChunkResult {

    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(ContentChunkResult.prototype);
        obj.__wbg_ptr = ptr;
        ContentChunkResultFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }

    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        ContentChunkResultFinalization.unregister(this);
        return ptr;
    }

    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_contentchunkresult_free(ptr, 0);
    }
    /**
     * @returns {Uint8Array}
     */
    get address() {
        const ret = wasm.contentchunkresult_address(this.__wbg_ptr);
        return ret;
    }
    /**
     * @returns {string}
     */
    get address_hex() {
        let deferred1_0;
        let deferred1_1;
        try {
            const ret = wasm.contentchunkresult_address_hex(this.__wbg_ptr);
            deferred1_0 = ret[0];
            deferred1_1 = ret[1];
            return getStringFromWasm0(ret[0], ret[1]);
        } finally {
            wasm.__wbindgen_free(deferred1_0, deferred1_1, 1);
        }
    }
    /**
     * @returns {Uint8Array}
     */
    get data() {
        const ret = wasm.contentchunkresult_data(this.__wbg_ptr);
        return ret;
    }
    /**
     * @returns {Uint8Array}
     */
    get serialized() {
        const ret = wasm.contentchunkresult_serialized(this.__wbg_ptr);
        return ret;
    }
    /**
     * @returns {string}
     */
    get serialized_hex() {
        let deferred1_0;
        let deferred1_1;
        try {
            const ret = wasm.contentchunkresult_serialized_hex(this.__wbg_ptr);
            deferred1_0 = ret[0];
            deferred1_1 = ret[1];
            return getStringFromWasm0(ret[0], ret[1]);
        } finally {
            wasm.__wbindgen_free(deferred1_0, deferred1_1, 1);
        }
    }
    /**
     * @returns {number}
     */
    get size() {
        const ret = wasm.contentchunkresult_size(this.__wbg_ptr);
        return ret >>> 0;
    }
}

const HashResultFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_hashresult_free(ptr >>> 0, 1));

export class HashResult {

    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(HashResult.prototype);
        obj.__wbg_ptr = ptr;
        HashResultFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }

    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        HashResultFinalization.unregister(this);
        return ptr;
    }

    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_hashresult_free(ptr, 0);
    }
    /**
     * @returns {string}
     */
    get hex() {
        let deferred1_0;
        let deferred1_1;
        try {
            const ret = wasm.hashresult_hex(this.__wbg_ptr);
            deferred1_0 = ret[0];
            deferred1_1 = ret[1];
            return getStringFromWasm0(ret[0], ret[1]);
        } finally {
            wasm.__wbindgen_free(deferred1_0, deferred1_1, 1);
        }
    }
    /**
     * @returns {Uint8Array}
     */
    get bytes() {
        const ret = wasm.hashresult_bytes(this.__wbg_ptr);
        return ret;
    }
}

const HasherFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_hasher_free(ptr >>> 0, 1));
/**
 * WASM-friendly wrapper for the Hasher
 */
export class Hasher {

    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        HasherFinalization.unregister(this);
        return ptr;
    }

    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_hasher_free(ptr, 0);
    }
    /**
     * Create a new Hasher
     */
    constructor() {
        const ret = wasm.hasher_new();
        this.__wbg_ptr = ret >>> 0;
        HasherFinalization.register(this, this.__wbg_ptr, this);
        return this;
    }
    /**
     * Set the span of data to be hashed
     * @param {bigint} span
     */
    set_span(span) {
        wasm.hasher_set_span(this.__wbg_ptr, span);
    }
    /**
     * Add a prefix to the hash calculation
     * @param {Uint8Array} prefix
     */
    prefixWith(prefix) {
        wasm.hasher_prefixWith(this.__wbg_ptr, prefix);
    }
    /**
     * Update the hasher with more data
     * @param {Uint8Array} data
     */
    update(data) {
        wasm.hasher_update(this.__wbg_ptr, data);
    }
    /**
     * Get the current hash value without modifying the hasher
     * @returns {Uint8Array}
     */
    sum() {
        const ret = wasm.hasher_sum(this.__wbg_ptr);
        return ret;
    }
    /**
     * Generate a proof for a specific segment
     * @param {Uint8Array} data
     * @param {number} segment_index
     * @returns {Proof}
     */
    generateProof(data, segment_index) {
        const ret = wasm.hasher_generateProof(this.__wbg_ptr, data, segment_index);
        if (ret[2]) {
            throw takeFromExternrefTable0(ret[1]);
        }
        return Proof.__wrap(ret[0]);
    }
    /**
     * Verify a proof against a root hash
     * @param {Proof} proof
     * @param {Uint8Array} root_hash
     * @returns {boolean}
     */
    static verifyProof(proof, root_hash) {
        _assertClass(proof, Proof);
        const ret = wasm.hasher_verifyProof(proof.__wbg_ptr, root_hash);
        if (ret[2]) {
            throw takeFromExternrefTable0(ret[1]);
        }
        return ret[0] !== 0;
    }
}

const IconConfigFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_iconconfig_free(ptr >>> 0, 1));
/**
 * Configuration for icon generation
 */
export class IconConfig {

    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(IconConfig.prototype);
        obj.__wbg_ptr = ptr;
        IconConfigFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }

    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        IconConfigFinalization.unregister(this);
        return ptr;
    }

    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_iconconfig_free(ptr, 0);
    }
    /**
     * Create a new icon configuration
     *
     * @param {number} size - The size of the icon in pixels
     * @param {IconShape} shape - The shape of the icon (Square or Circle)
     * @param {GeneratorFunction} generator - The algorithm to use for generation
     * @param {ColorScheme} color_scheme - The color scheme to use
     * @returns {IconConfig} A new configuration object
     * @param {number} size
     * @param {IconShape} shape
     * @param {GeneratorFunction} generator
     * @param {ColorScheme} color_scheme
     */
    constructor(size, shape, generator, color_scheme) {
        const ret = wasm.iconconfig_new(size, shape, generator, color_scheme);
        this.__wbg_ptr = ret >>> 0;
        IconConfigFinalization.register(this, this.__wbg_ptr, this);
        return this;
    }
    /**
     * @returns {number}
     */
    get size() {
        const ret = wasm.iconconfig_size(this.__wbg_ptr);
        return ret >>> 0;
    }
    /**
     * @returns {IconShape}
     */
    get shape() {
        const ret = wasm.iconconfig_shape(this.__wbg_ptr);
        return ret;
    }
    /**
     * @returns {GeneratorFunction}
     */
    get generator() {
        const ret = wasm.iconconfig_generator(this.__wbg_ptr);
        return ret;
    }
    /**
     * @returns {ColorScheme}
     */
    get color_scheme() {
        const ret = wasm.iconconfig_color_scheme(this.__wbg_ptr);
        return ret;
    }
}

const IconConfigBuilderFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_iconconfigbuilder_free(ptr >>> 0, 1));
/**
 * Builder for creating IconConfig objects with a fluent API
 */
export class IconConfigBuilder {

    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(IconConfigBuilder.prototype);
        obj.__wbg_ptr = ptr;
        IconConfigBuilderFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }

    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        IconConfigBuilderFinalization.unregister(this);
        return ptr;
    }

    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_iconconfigbuilder_free(ptr, 0);
    }
    /**
     * Set the size of the generated icon
     *
     * @param {number} size - Size in pixels (both width and height)
     * @returns {IconConfigBuilder} The builder for method chaining
     * @param {number} size
     * @returns {IconConfigBuilder}
     */
    with_size(size) {
        const ptr = this.__destroy_into_raw();
        const ret = wasm.iconconfigbuilder_with_size(ptr, size);
        return IconConfigBuilder.__wrap(ret);
    }
    /**
     * Set the shape of the generated icon
     *
     * @param {IconShape} shape - The shape to use
     * @returns {IconConfigBuilder} The builder for method chaining
     * @param {IconShape} shape
     * @returns {IconConfigBuilder}
     */
    with_shape(shape) {
        const ptr = this.__destroy_into_raw();
        const ret = wasm.iconconfigbuilder_with_shape(ptr, shape);
        return IconConfigBuilder.__wrap(ret);
    }
    /**
     * Set the generator function for the icon
     *
     * @param {GeneratorFunction} generator - The algorithm to use
     * @returns {IconConfigBuilder} The builder for method chaining
     * @param {GeneratorFunction} generator
     * @returns {IconConfigBuilder}
     */
    with_generator(generator) {
        const ptr = this.__destroy_into_raw();
        const ret = wasm.iconconfigbuilder_with_generator(ptr, generator);
        return IconConfigBuilder.__wrap(ret);
    }
    /**
     * Set the color scheme for the icon
     *
     * @param {ColorScheme} color_scheme - The color scheme to use
     * @returns {IconConfigBuilder} The builder for method chaining
     * @param {ColorScheme} color_scheme
     * @returns {IconConfigBuilder}
     */
    with_color_scheme(color_scheme) {
        const ptr = this.__destroy_into_raw();
        const ret = wasm.iconconfigbuilder_with_color_scheme(ptr, color_scheme);
        return IconConfigBuilder.__wrap(ret);
    }
    /**
     * Build the final IconConfig object
     *
     * @returns {IconConfig} The configured IconConfig
     * @returns {IconConfig}
     */
    build() {
        const ptr = this.__destroy_into_raw();
        const ret = wasm.iconconfigbuilder_build(ptr);
        return IconConfig.__wrap(ret);
    }
}

const IconDataFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_icondata_free(ptr >>> 0, 1));
/**
 * Data structure representing chunk data for icon generation
 */
export class IconData {

    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(IconData.prototype);
        obj.__wbg_ptr = ptr;
        IconDataFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }

    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        IconDataFinalization.unregister(this);
        return ptr;
    }

    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_icondata_free(ptr, 0);
    }
    /**
     * Create a new IconData instance
     *
     * @param {Uint8Array} address_bytes - 32-byte chunk address
     * @param {number} chunk_type - Chunk type identifier (1 byte)
     * @param {number} version - Chunk version (1 byte)
     * @param {Uint8Array} header_bytes - Chunk header data
     * @param {Uint8Array} payload_bytes - Chunk payload data
     * @returns {IconData} A new IconData instance
     * @param {Uint8Array} address_bytes
     * @param {number} chunk_type
     * @param {number} version
     * @param {Uint8Array} header_bytes
     * @param {Uint8Array} payload_bytes
     */
    constructor(address_bytes, chunk_type, version, header_bytes, payload_bytes) {
        const ptr0 = passArray8ToWasm0(address_bytes, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passArray8ToWasm0(header_bytes, wasm.__wbindgen_malloc);
        const len1 = WASM_VECTOR_LEN;
        const ptr2 = passArray8ToWasm0(payload_bytes, wasm.__wbindgen_malloc);
        const len2 = WASM_VECTOR_LEN;
        const ret = wasm.icondata_new(ptr0, len0, chunk_type, version, ptr1, len1, ptr2, len2);
        if (ret[2]) {
            throw takeFromExternrefTable0(ret[1]);
        }
        this.__wbg_ptr = ret[0] >>> 0;
        IconDataFinalization.register(this, this.__wbg_ptr, this);
        return this;
    }
    /**
     * @returns {Uint8Array}
     */
    get address() {
        const ret = wasm.icondata_address(this.__wbg_ptr);
        return ret;
    }
    /**
     * @returns {number}
     */
    get chunk_type() {
        const ret = wasm.icondata_chunk_type(this.__wbg_ptr);
        return ret;
    }
    /**
     * @returns {number}
     */
    get version() {
        const ret = wasm.icondata_version(this.__wbg_ptr);
        return ret;
    }
    /**
     * @returns {Uint8Array}
     */
    get header() {
        const ret = wasm.icondata_header(this.__wbg_ptr);
        return ret;
    }
    /**
     * @returns {Uint8Array}
     */
    get payload() {
        const ret = wasm.icondata_payload(this.__wbg_ptr);
        return ret;
    }
}

const ProofFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_proof_free(ptr >>> 0, 1));
/**
 * WASM-friendly wrapper for proofs
 */
export class Proof {

    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(Proof.prototype);
        obj.__wbg_ptr = ptr;
        ProofFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }

    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        ProofFinalization.unregister(this);
        return ptr;
    }

    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_proof_free(ptr, 0);
    }
    /**
     * Get the segment index this proof is for
     * @returns {number}
     */
    segmentIndex() {
        const ret = wasm.proof_segmentIndex(this.__wbg_ptr);
        return ret >>> 0;
    }
    /**
     * Get the segment being proven
     * @returns {Uint8Array}
     */
    segment() {
        const ret = wasm.proof_segment(this.__wbg_ptr);
        return ret;
    }
    /**
     * Get the proof segments (sibling hashes)
     * @returns {Array<any>}
     */
    proofSegments() {
        const ret = wasm.proof_proofSegments(this.__wbg_ptr);
        return ret;
    }
    /**
     * Get the span of the data
     * @returns {bigint}
     */
    span() {
        const ret = wasm.proof_span(this.__wbg_ptr);
        return BigInt.asUintN(64, ret);
    }
    /**
     * Verify this proof against a root hash
     * @param {Uint8Array} root_hash
     * @returns {boolean}
     */
    verify(root_hash) {
        const ret = wasm.proof_verify(this.__wbg_ptr, root_hash);
        if (ret[2]) {
            throw takeFromExternrefTable0(ret[1]);
        }
        return ret[0] !== 0;
    }
}

const SingleOwnerChunkResultFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_singleownerchunkresult_free(ptr >>> 0, 1));
/**
 * Result of creating a SingleOwnerChunk
 */
export class SingleOwnerChunkResult {

    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(SingleOwnerChunkResult.prototype);
        obj.__wbg_ptr = ptr;
        SingleOwnerChunkResultFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }

    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        SingleOwnerChunkResultFinalization.unregister(this);
        return ptr;
    }

    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_singleownerchunkresult_free(ptr, 0);
    }
    /**
     * @returns {Uint8Array}
     */
    get address() {
        const ret = wasm.singleownerchunkresult_address(this.__wbg_ptr);
        return ret;
    }
    /**
     * @returns {string}
     */
    get address_hex() {
        let deferred1_0;
        let deferred1_1;
        try {
            const ret = wasm.singleownerchunkresult_address_hex(this.__wbg_ptr);
            deferred1_0 = ret[0];
            deferred1_1 = ret[1];
            return getStringFromWasm0(ret[0], ret[1]);
        } finally {
            wasm.__wbindgen_free(deferred1_0, deferred1_1, 1);
        }
    }
    /**
     * @returns {Uint8Array}
     */
    get id() {
        const ret = wasm.singleownerchunkresult_id(this.__wbg_ptr);
        return ret;
    }
    /**
     * @returns {string}
     */
    get id_hex() {
        let deferred1_0;
        let deferred1_1;
        try {
            const ret = wasm.singleownerchunkresult_id_hex(this.__wbg_ptr);
            deferred1_0 = ret[0];
            deferred1_1 = ret[1];
            return getStringFromWasm0(ret[0], ret[1]);
        } finally {
            wasm.__wbindgen_free(deferred1_0, deferred1_1, 1);
        }
    }
    /**
     * @returns {Uint8Array}
     */
    get owner() {
        const ret = wasm.singleownerchunkresult_owner(this.__wbg_ptr);
        return ret;
    }
    /**
     * @returns {string}
     */
    get owner_hex() {
        let deferred1_0;
        let deferred1_1;
        try {
            const ret = wasm.singleownerchunkresult_owner_hex(this.__wbg_ptr);
            deferred1_0 = ret[0];
            deferred1_1 = ret[1];
            return getStringFromWasm0(ret[0], ret[1]);
        } finally {
            wasm.__wbindgen_free(deferred1_0, deferred1_1, 1);
        }
    }
    /**
     * @returns {Uint8Array}
     */
    get data() {
        const ret = wasm.singleownerchunkresult_data(this.__wbg_ptr);
        return ret;
    }
    /**
     * @returns {Uint8Array}
     */
    get serialized() {
        const ret = wasm.singleownerchunkresult_serialized(this.__wbg_ptr);
        return ret;
    }
    /**
     * @returns {string}
     */
    get serialized_hex() {
        let deferred1_0;
        let deferred1_1;
        try {
            const ret = wasm.singleownerchunkresult_serialized_hex(this.__wbg_ptr);
            deferred1_0 = ret[0];
            deferred1_1 = ret[1];
            return getStringFromWasm0(ret[0], ret[1]);
        } finally {
            wasm.__wbindgen_free(deferred1_0, deferred1_1, 1);
        }
    }
    /**
     * @returns {Uint8Array}
     */
    get signature() {
        const ret = wasm.singleownerchunkresult_signature(this.__wbg_ptr);
        return ret;
    }
    /**
     * @returns {string}
     */
    get signature_hex() {
        let deferred1_0;
        let deferred1_1;
        try {
            const ret = wasm.singleownerchunkresult_signature_hex(this.__wbg_ptr);
            deferred1_0 = ret[0];
            deferred1_1 = ret[1];
            return getStringFromWasm0(ret[0], ret[1]);
        } finally {
            wasm.__wbindgen_free(deferred1_0, deferred1_1, 1);
        }
    }
    /**
     * @returns {number}
     */
    get size() {
        const ret = wasm.contentchunkresult_size(this.__wbg_ptr);
        return ret >>> 0;
    }
}

async function __wbg_load(module, imports) {
    if (typeof Response === 'function' && module instanceof Response) {
        if (typeof WebAssembly.instantiateStreaming === 'function') {
            try {
                return await WebAssembly.instantiateStreaming(module, imports);

            } catch (e) {
                if (module.headers.get('Content-Type') != 'application/wasm') {
                    console.warn("`WebAssembly.instantiateStreaming` failed because your server does not serve Wasm with `application/wasm` MIME type. Falling back to `WebAssembly.instantiate` which is slower. Original error:\n", e);

                } else {
                    throw e;
                }
            }
        }

        const bytes = await module.arrayBuffer();
        return await WebAssembly.instantiate(bytes, imports);

    } else {
        const instance = await WebAssembly.instantiate(module, imports);

        if (instance instanceof WebAssembly.Instance) {
            return { instance, module };

        } else {
            return instance;
        }
    }
}

function __wbg_get_imports() {
    const imports = {};
    imports.wbg = {};
    imports.wbg.__wbg_buffer_609cc3eee51ed158 = function(arg0) {
        const ret = arg0.buffer;
        return ret;
    };
    imports.wbg.__wbg_call_672a4d21634d4a24 = function() { return handleError(function (arg0, arg1) {
        const ret = arg0.call(arg1);
        return ret;
    }, arguments) };
    imports.wbg.__wbg_call_7cccdd69e0791ae2 = function() { return handleError(function (arg0, arg1, arg2) {
        const ret = arg0.call(arg1, arg2);
        return ret;
    }, arguments) };
    imports.wbg.__wbg_crypto_ed58b8e10a292839 = function(arg0) {
        const ret = arg0.crypto;
        return ret;
    };
    imports.wbg.__wbg_error_7534b8e9a36f1ab4 = function(arg0, arg1) {
        let deferred0_0;
        let deferred0_1;
        try {
            deferred0_0 = arg0;
            deferred0_1 = arg1;
            console.error(getStringFromWasm0(arg0, arg1));
        } finally {
            wasm.__wbindgen_free(deferred0_0, deferred0_1, 1);
        }
    };
    imports.wbg.__wbg_getRandomValues_bcb4912f16000dc4 = function() { return handleError(function (arg0, arg1) {
        arg0.getRandomValues(arg1);
    }, arguments) };
    imports.wbg.__wbg_length_a446193dc22c12f8 = function(arg0) {
        const ret = arg0.length;
        return ret;
    };
    imports.wbg.__wbg_msCrypto_0a36e2ec3a343d26 = function(arg0) {
        const ret = arg0.msCrypto;
        return ret;
    };
    imports.wbg.__wbg_new_78feb108b6472713 = function() {
        const ret = new Array();
        return ret;
    };
    imports.wbg.__wbg_new_8a6f238a6ece86ea = function() {
        const ret = new Error();
        return ret;
    };
    imports.wbg.__wbg_new_a12002a7f91c75be = function(arg0) {
        const ret = new Uint8Array(arg0);
        return ret;
    };
    imports.wbg.__wbg_newnoargs_105ed471475aaf50 = function(arg0, arg1) {
        const ret = new Function(getStringFromWasm0(arg0, arg1));
        return ret;
    };
    imports.wbg.__wbg_newwithbyteoffsetandlength_d97e637ebe145a9a = function(arg0, arg1, arg2) {
        const ret = new Uint8Array(arg0, arg1 >>> 0, arg2 >>> 0);
        return ret;
    };
    imports.wbg.__wbg_newwithlength_a381634e90c276d4 = function(arg0) {
        const ret = new Uint8Array(arg0 >>> 0);
        return ret;
    };
    imports.wbg.__wbg_node_02999533c4ea02e3 = function(arg0) {
        const ret = arg0.node;
        return ret;
    };
    imports.wbg.__wbg_now_807e54c39636c349 = function() {
        const ret = Date.now();
        return ret;
    };
    imports.wbg.__wbg_process_5c1d670bc53614b8 = function(arg0) {
        const ret = arg0.process;
        return ret;
    };
    imports.wbg.__wbg_push_737cfc8c1432c2c6 = function(arg0, arg1) {
        const ret = arg0.push(arg1);
        return ret;
    };
    imports.wbg.__wbg_randomFillSync_ab2cfe79ebbf2740 = function() { return handleError(function (arg0, arg1) {
        arg0.randomFillSync(arg1);
    }, arguments) };
    imports.wbg.__wbg_require_79b1e9274cde3c87 = function() { return handleError(function () {
        const ret = module.require;
        return ret;
    }, arguments) };
    imports.wbg.__wbg_set_65595bdd868b3009 = function(arg0, arg1, arg2) {
        arg0.set(arg1, arg2 >>> 0);
    };
    imports.wbg.__wbg_stack_0ed75d68575b0f3c = function(arg0, arg1) {
        const ret = arg1.stack;
        const ptr1 = passStringToWasm0(ret, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        getDataViewMemory0().setInt32(arg0 + 4 * 1, len1, true);
        getDataViewMemory0().setInt32(arg0 + 4 * 0, ptr1, true);
    };
    imports.wbg.__wbg_static_accessor_GLOBAL_88a902d13a557d07 = function() {
        const ret = typeof global === 'undefined' ? null : global;
        return isLikeNone(ret) ? 0 : addToExternrefTable0(ret);
    };
    imports.wbg.__wbg_static_accessor_GLOBAL_THIS_56578be7e9f832b0 = function() {
        const ret = typeof globalThis === 'undefined' ? null : globalThis;
        return isLikeNone(ret) ? 0 : addToExternrefTable0(ret);
    };
    imports.wbg.__wbg_static_accessor_SELF_37c5d418e4bf5819 = function() {
        const ret = typeof self === 'undefined' ? null : self;
        return isLikeNone(ret) ? 0 : addToExternrefTable0(ret);
    };
    imports.wbg.__wbg_static_accessor_WINDOW_5de37043a91a9c40 = function() {
        const ret = typeof window === 'undefined' ? null : window;
        return isLikeNone(ret) ? 0 : addToExternrefTable0(ret);
    };
    imports.wbg.__wbg_subarray_aa9065fa9dc5df96 = function(arg0, arg1, arg2) {
        const ret = arg0.subarray(arg1 >>> 0, arg2 >>> 0);
        return ret;
    };
    imports.wbg.__wbg_versions_c71aa1626a93e0a1 = function(arg0) {
        const ret = arg0.versions;
        return ret;
    };
    imports.wbg.__wbindgen_init_externref_table = function() {
        const table = wasm.__wbindgen_export_2;
        const offset = table.grow(4);
        table.set(0, undefined);
        table.set(offset + 0, undefined);
        table.set(offset + 1, null);
        table.set(offset + 2, true);
        table.set(offset + 3, false);
        ;
    };
    imports.wbg.__wbindgen_is_function = function(arg0) {
        const ret = typeof(arg0) === 'function';
        return ret;
    };
    imports.wbg.__wbindgen_is_object = function(arg0) {
        const val = arg0;
        const ret = typeof(val) === 'object' && val !== null;
        return ret;
    };
    imports.wbg.__wbindgen_is_string = function(arg0) {
        const ret = typeof(arg0) === 'string';
        return ret;
    };
    imports.wbg.__wbindgen_is_undefined = function(arg0) {
        const ret = arg0 === undefined;
        return ret;
    };
    imports.wbg.__wbindgen_memory = function() {
        const ret = wasm.memory;
        return ret;
    };
    imports.wbg.__wbindgen_string_new = function(arg0, arg1) {
        const ret = getStringFromWasm0(arg0, arg1);
        return ret;
    };
    imports.wbg.__wbindgen_throw = function(arg0, arg1) {
        throw new Error(getStringFromWasm0(arg0, arg1));
    };

    return imports;
}

function __wbg_init_memory(imports, memory) {

}

function __wbg_finalize_init(instance, module) {
    wasm = instance.exports;
    __wbg_init.__wbindgen_wasm_module = module;
    cachedDataViewMemory0 = null;
    cachedUint8ArrayMemory0 = null;


    wasm.__wbindgen_start();
    return wasm;
}

function initSync(module) {
    if (wasm !== undefined) return wasm;


    if (typeof module !== 'undefined') {
        if (Object.getPrototypeOf(module) === Object.prototype) {
            ({module} = module)
        } else {
            console.warn('using deprecated parameters for `initSync()`; pass a single object instead')
        }
    }

    const imports = __wbg_get_imports();

    __wbg_init_memory(imports);

    if (!(module instanceof WebAssembly.Module)) {
        module = new WebAssembly.Module(module);
    }

    const instance = new WebAssembly.Instance(module, imports);

    return __wbg_finalize_init(instance, module);
}

async function __wbg_init(module_or_path) {
    if (wasm !== undefined) return wasm;


    if (typeof module_or_path !== 'undefined') {
        if (Object.getPrototypeOf(module_or_path) === Object.prototype) {
            ({module_or_path} = module_or_path)
        } else {
            console.warn('using deprecated parameters for the initialization function; pass a single object instead')
        }
    }

    if (typeof module_or_path === 'undefined') {
        module_or_path = new URL('bmt-wasm-demo_bg.wasm', import.meta.url);
    }
    const imports = __wbg_get_imports();

    if (typeof module_or_path === 'string' || (typeof Request === 'function' && module_or_path instanceof Request) || (typeof URL === 'function' && module_or_path instanceof URL)) {
        module_or_path = fetch(module_or_path);
    }

    __wbg_init_memory(imports);

    const { instance, module } = await __wbg_load(await module_or_path, imports);

    return __wbg_finalize_init(instance, module);
}

export { initSync };
export default __wbg_init;
