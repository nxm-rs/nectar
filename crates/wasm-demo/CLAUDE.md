# WASM Demo

Demonstrates parallel BMT hashing, chunk generation, and ECDSA postage stamping in the browser using wasm-bindgen-rayon.

## Building

The WASM demo requires **nightly Rust** with special target features and linker arguments for SharedArrayBuffer support.

**Use the dedicated `wasm` nix development shell:**

```bash
cd crates/wasm-demo
nix develop .#wasm --command wasm-pack build --target web
```

The `.#wasm` shell (defined in `flake.nix`) provides:
- Nightly Rust toolchain with `rust-src` component
- Full RUSTFLAGS for wasm-bindgen-rayon:
  - `-C target-feature=+atomics,+bulk-memory,+mutable-globals`
  - `-C link-arg=--shared-memory`
  - `-C link-arg=--max-memory=1073741824`
  - `-C link-arg=--import-memory`
  - `-C link-arg=--export=__wasm_init_tls`
  - `-C link-arg=--export=__tls_size`
  - `-C link-arg=--export=__tls_align`
  - `-C link-arg=--export=__tls_base`
- wasm-pack and wasm-bindgen-cli

**Do NOT use the default nix shell** (`nix develop`) as it uses stable Rust without the required RUSTFLAGS. This will cause:
- Build error: `Did you forget to enable atomics and bulk-memory features?`
- Runtime error: `WebAssembly.Memory object could not be cloned`

See: https://github.com/RReverser/wasm-bindgen-rayon

**Note:** The `.cargo/config.toml` file contains all the necessary RUSTFLAGS and build-std settings. These are picked up automatically by cargo/wasm-pack.

## Deploying

After building, copy the pkg to www and patch the worker helper:

```bash
rm -rf www/pkg && cp -r pkg www/

# Patch workerHelpers.js for browser compatibility
# wasm-bindgen-rayon uses import('../../..') which browsers can't resolve
sed -i "s|import('../../..')|import('../../../nectar_wasm_demo.js')|" \
  www/pkg/snippets/wasm-bindgen-rayon-*/src/workerHelpers.js
```

**Why the patch is needed:** The generated workerHelpers.js uses `import('../../..')` to dynamically import the main module. Browsers can't resolve directory imports via package.json like Node.js does, so we must explicitly specify the JS file path.

## Running

Serve with cross-origin isolation headers (required for SharedArrayBuffer):

```bash
cd www
python3 -c "
from http.server import HTTPServer, SimpleHTTPRequestHandler
class Handler(SimpleHTTPRequestHandler):
    def end_headers(self):
        self.send_header('Cross-Origin-Opener-Policy', 'same-origin')
        self.send_header('Cross-Origin-Embedder-Policy', 'require-corp')
        super().end_headers()
HTTPServer(('', 8080), Handler).serve_forever()
"
```

Then open http://localhost:8080

## Features

- **Split Comparison**: Sequential vs parallel file splitting into BMT chunks
- **Full Pipeline**: Combined split + ECDSA stamp with timing breakdown
  - Displays split time and stamp time separately
  - Shows total throughput for the combined pipeline
- Uses random keypairs for ECDSA signing
- Dynamically sizes batch depth to handle birthday paradox
