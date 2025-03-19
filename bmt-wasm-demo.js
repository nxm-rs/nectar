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
 * @param {number} size
 * @param {number} iterations
 * @returns {number}
 */
export function benchmark_hash(size, iterations) {
    const ret = wasm.benchmark_hash(size, iterations);
    return ret;
}

/**
 * Utility function to help with debugging
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

function passArray8ToWasm0(arg, malloc) {
    const ptr = malloc(arg.length * 1, 1) >>> 0;
    getUint8ArrayMemory0().set(arg, ptr / 1);
    WASM_VECTOR_LEN = arg.length;
    return ptr;
}

function takeFromExternrefTable0(idx) {
    const value = wasm.__wbindgen_export_2.get(idx);
    wasm.__externref_table_dealloc(idx);
    return value;
}
/**
 * Create a IconData instance from hex strings (convenience function for JS)
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
 * @enum {0 | 1 | 2 | 3}
 */
export const ColorScheme = Object.freeze({
    Vibrant: 0, "0": "Vibrant",
    Pastel: 1, "1": "Pastel",
    Monochrome: 2, "2": "Monochrome",
    Complementary: 3, "3": "Complementary",
});
/**
 * @enum {0 | 1 | 2 | 3 | 4}
 */
export const GeneratorFunction = Object.freeze({
    Geometric: 0, "0": "Geometric",
    Abstract: 1, "1": "Abstract",
    Circular: 2, "2": "Circular",
    Pixelated: 3, "3": "Pixelated",
    Molecular: 4, "4": "Molecular",
});
/**
 * @enum {0 | 1}
 */
export const IconShape = Object.freeze({
    Square: 0, "0": "Square",
    Circle: 1, "1": "Circle",
});

const BMTHasherFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_bmthasher_free(ptr >>> 0, 1));
/**
 * WASM-friendly wrapper for the BMTHasher
 */
export class BMTHasher {

    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        BMTHasherFinalization.unregister(this);
        return ptr;
    }

    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_bmthasher_free(ptr, 0);
    }
    /**
     * Create a new BMT hasher
     */
    constructor() {
        const ret = wasm.bmthasher_new();
        this.__wbg_ptr = ret >>> 0;
        BMTHasherFinalization.register(this, this.__wbg_ptr, this);
        return this;
    }
    /**
     * Set the span of data to be hashed
     * @param {bigint} span
     */
    set_span(span) {
        wasm.bmthasher_set_span(this.__wbg_ptr, span);
    }
    /**
     * Add a prefix to the hash calculation
     * @param {Uint8Array} prefix
     */
    prefixWith(prefix) {
        wasm.bmthasher_prefixWith(this.__wbg_ptr, prefix);
    }
    /**
     * Update the hasher with more data
     * @param {Uint8Array} data
     */
    update(data) {
        wasm.bmthasher_update(this.__wbg_ptr, data);
    }
    /**
     * Get the current hash value without modifying the hasher
     * @returns {Uint8Array}
     */
    sum() {
        const ret = wasm.bmthasher_sum(this.__wbg_ptr);
        return ret;
    }
    /**
     * Calculate the chunk address for the given data
     * @param {Uint8Array} data
     * @returns {ChunkAddress}
     */
    chunkAddress(data) {
        const ret = wasm.bmthasher_chunkAddress(this.__wbg_ptr, data);
        if (ret[2]) {
            throw takeFromExternrefTable0(ret[1]);
        }
        return ChunkAddress.__wrap(ret[0]);
    }
    /**
     * Generate a proof for a specific segment
     * @param {Uint8Array} data
     * @param {number} segment_index
     * @returns {BMTProof}
     */
    generateProof(data, segment_index) {
        const ret = wasm.bmthasher_generateProof(this.__wbg_ptr, data, segment_index);
        if (ret[2]) {
            throw takeFromExternrefTable0(ret[1]);
        }
        return BMTProof.__wrap(ret[0]);
    }
    /**
     * Verify a proof against a root hash
     * @param {BMTProof} proof
     * @param {Uint8Array} root_hash
     * @returns {boolean}
     */
    static verifyProof(proof, root_hash) {
        _assertClass(proof, BMTProof);
        const ret = wasm.bmthasher_verifyProof(proof.__wbg_ptr, root_hash);
        if (ret[2]) {
            throw takeFromExternrefTable0(ret[1]);
        }
        return ret[0] !== 0;
    }
}

const BMTProofFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_bmtproof_free(ptr >>> 0, 1));
/**
 * WASM-friendly wrapper for BMT proofs
 */
export class BMTProof {

    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(BMTProof.prototype);
        obj.__wbg_ptr = ptr;
        BMTProofFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }

    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        BMTProofFinalization.unregister(this);
        return ptr;
    }

    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_bmtproof_free(ptr, 0);
    }
    /**
     * Get the segment index this proof is for
     * @returns {number}
     */
    segmentIndex() {
        const ret = wasm.bmtproof_segmentIndex(this.__wbg_ptr);
        return ret >>> 0;
    }
    /**
     * Get the segment being proven
     * @returns {Uint8Array}
     */
    segment() {
        const ret = wasm.bmtproof_segment(this.__wbg_ptr);
        return ret;
    }
    /**
     * Get the proof segments (sibling hashes)
     * @returns {Array<any>}
     */
    proofSegments() {
        const ret = wasm.bmtproof_proofSegments(this.__wbg_ptr);
        return ret;
    }
    /**
     * Get the span of the data
     * @returns {bigint}
     */
    span() {
        const ret = wasm.bmtproof_span(this.__wbg_ptr);
        return BigInt.asUintN(64, ret);
    }
    /**
     * Verify this proof against a root hash
     * @param {Uint8Array} root_hash
     * @returns {boolean}
     */
    verify(root_hash) {
        const ret = wasm.bmtproof_verify(this.__wbg_ptr, root_hash);
        if (ret[2]) {
            throw takeFromExternrefTable0(ret[1]);
        }
        return ret[0] !== 0;
    }
}

const ChunkAddressFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_chunkaddress_free(ptr >>> 0, 1));
/**
 * WASM-friendly wrapper for ChunkAddress
 */
export class ChunkAddress {

    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(ChunkAddress.prototype);
        obj.__wbg_ptr = ptr;
        ChunkAddressFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }

    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        ChunkAddressFinalization.unregister(this);
        return ptr;
    }

    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_chunkaddress_free(ptr, 0);
    }
    /**
     * Create a new zero-filled address
     * @returns {ChunkAddress}
     */
    static zero() {
        const ret = wasm.chunkaddress_zero();
        return ChunkAddress.__wrap(ret);
    }
    /**
     * Create from bytes
     * @param {Uint8Array} bytes
     * @returns {ChunkAddress}
     */
    static fromBytes(bytes) {
        const ret = wasm.chunkaddress_fromBytes(bytes);
        if (ret[2]) {
            throw takeFromExternrefTable0(ret[1]);
        }
        return ChunkAddress.__wrap(ret[0]);
    }
    /**
     * Get the address bytes
     * @returns {Uint8Array}
     */
    asBytes() {
        const ret = wasm.chunkaddress_asBytes(this.__wbg_ptr);
        return ret;
    }
    /**
     * Check if this address is zeros
     * @returns {boolean}
     */
    isZero() {
        const ret = wasm.chunkaddress_isZero(this.__wbg_ptr);
        return ret !== 0;
    }
    /**
     * Calculate proximity between two addresses
     * @param {ChunkAddress} other
     * @returns {number}
     */
    proximity(other) {
        _assertClass(other, ChunkAddress);
        const ret = wasm.chunkaddress_proximity(this.__wbg_ptr, other.__wbg_ptr);
        return ret;
    }
    /**
     * Check if address is within proximity
     * @param {ChunkAddress} other
     * @param {number} min_proximity
     * @returns {boolean}
     */
    isWithinProximity(other, min_proximity) {
        _assertClass(other, ChunkAddress);
        const ret = wasm.chunkaddress_isWithinProximity(this.__wbg_ptr, other.__wbg_ptr, min_proximity);
        return ret !== 0;
    }
}

const ChunkDataFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_chunkdata_free(ptr >>> 0, 1));
/**
 * WASM-friendly wrapper for ChunkData
 */
export class ChunkData {

    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(ChunkData.prototype);
        obj.__wbg_ptr = ptr;
        ChunkDataFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }

    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        ChunkDataFinalization.unregister(this);
        return ptr;
    }

    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_chunkdata_free(ptr, 0);
    }
    /**
     * Deserialize bytes into a chunk
     * @param {Uint8Array} data
     * @param {boolean} has_type_prefix
     * @returns {ChunkData}
     */
    static deserialize(data, has_type_prefix) {
        const ret = wasm.chunkdata_deserialize(data, has_type_prefix);
        if (ret[2]) {
            throw takeFromExternrefTable0(ret[1]);
        }
        return ChunkData.__wrap(ret[0]);
    }
    /**
     * Get the chunk's address
     * @returns {ChunkAddress}
     */
    address() {
        const ret = wasm.chunkdata_address(this.__wbg_ptr);
        return ChunkAddress.__wrap(ret);
    }
    /**
     * Get the chunk type as a byte
     * @returns {number}
     */
    chunkTypeByte() {
        const ret = wasm.chunkdata_chunkTypeByte(this.__wbg_ptr);
        return ret;
    }
    /**
     * Get the chunk's version
     * @returns {number}
     */
    version() {
        const ret = wasm.chunkdata_version(this.__wbg_ptr);
        return ret;
    }
    /**
     * Get the header size
     * @returns {number}
     */
    headerSize() {
        const ret = wasm.chunkdata_headerSize(this.__wbg_ptr);
        return ret >>> 0;
    }
    /**
     * Get the header bytes
     * @returns {Uint8Array}
     */
    header() {
        const ret = wasm.chunkdata_header(this.__wbg_ptr);
        return ret;
    }
    /**
     * Get the payload bytes
     * @returns {Uint8Array}
     */
    payload() {
        const ret = wasm.chunkdata_payload(this.__wbg_ptr);
        return ret;
    }
    /**
     * Get the full data bytes
     * @returns {Uint8Array}
     */
    data() {
        const ret = wasm.chunkdata_data(this.__wbg_ptr);
        return ret;
    }
    /**
     * Get the chunk size in bytes
     * @returns {number}
     */
    size() {
        const ret = wasm.chunkdata_size(this.__wbg_ptr);
        return ret >>> 0;
    }
    /**
     * Verify chunk integrity
     */
    verifyIntegrity() {
        const ret = wasm.chunkdata_verifyIntegrity(this.__wbg_ptr);
        if (ret[1]) {
            throw takeFromExternrefTable0(ret[0]);
        }
    }
    /**
     * Verify the chunk matches an expected address
     * @param {ChunkAddress} expected
     */
    verify(expected) {
        _assertClass(expected, ChunkAddress);
        const ret = wasm.chunkdata_verify(this.__wbg_ptr, expected.__wbg_ptr);
        if (ret[1]) {
            throw takeFromExternrefTable0(ret[0]);
        }
    }
    /**
     * Serialize the chunk to bytes
     * @param {boolean} with_type_prefix
     * @returns {Uint8Array}
     */
    serialize(with_type_prefix) {
        const ret = wasm.chunkdata_serialize(this.__wbg_ptr, with_type_prefix);
        return ret;
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

const IconConfigFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_iconconfig_free(ptr >>> 0, 1));

export class IconConfig {

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

const IconDataFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_icondata_free(ptr >>> 0, 1));

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
