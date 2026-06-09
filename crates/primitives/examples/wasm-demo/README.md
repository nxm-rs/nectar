# BMT Hasher & Icon Generator WASM Demo

This example demonstrates the BMT (Binary Merkle Tree) hasher and SVG icon generation functionality from nectar-primitives in a web browser using WebAssembly.

## Features

- **BMT Hasher**: Calculate BMT hashes of input text with visual representations
- **Icon Generator**: Create unique SVG icons from chunk data with various styles
- **Performance Benchmark**: Test BMT hashing performance in your browser

## Running the Demo

### Prerequisites

- [Rust](https://www.rust-lang.org/tools/install) (1.85.0 or later recommended)
- [Trunk](https://trunkrs.dev/) - A WASM application bundler for Rust
- WebAssembly target: `rustup target add wasm32-unknown-unknown`

### Quick Start

1. Install Trunk if not already installed:
   ```bash
   cargo install trunk
   ```

2. Run the development server:
   ```bash
   # From the wasm-demo directory
   trunk serve
   ```

3. Your browser will open automatically to http://localhost:8080

### Building for Production

To build a production version:

```bash
trunk build --release
```

The compiled files will be in the `dist` directory, ready for deployment to any static hosting service.

### Cross-origin isolation (SharedArrayBuffer)

The demo links nectar-primitives with wasm-bindgen-rayon for parallel BMT hashing, so the wasm module imports a shared memory and needs `SharedArrayBuffer`. Browsers only expose `SharedArrayBuffer` in a cross-origin-isolated context, which requires these response headers:

```
Cross-Origin-Opener-Policy: same-origin
Cross-Origin-Embedder-Policy: require-corp
```

When you control the server, send those headers directly (for example `trunk serve` configured with them, or `miniserve --header ...`). Static hosts such as GitHub Pages cannot set custom response headers, so the demo ships `coi-serviceworker.min.js`, which registers a service worker that injects the headers and reloads once to enter an isolated context. It is loaded first in `index.html` so it runs before the wasm bootstrap.

## How It Works

1. **BMT Hasher Tab**: Calculate BMT hashes of text input with real-time visualization
   - Generates both a color-based byte visualization and an optional SVG icon
   - Configure span parameter for BMT hasher

2. **Icon Generator Tab**: Create unique SVG icons from chunk data
   - Specify chunk address, type, version, header, and payload
   - Choose from 5 different generator styles, 2 shapes, and 4 color schemes
   - Download or copy the generated SVG code

3. **Benchmark Tab**: Test BMT hashing performance
   - Configure data size and iterations
   - View detailed performance metrics

The demo showcases how WebAssembly enables complex cryptographic and visual operations to run efficiently in the browser using Rust code.

## Learn More

- [Trunk Documentation](https://trunkrs.dev/)
- [wasm-bindgen Documentation](https://rustwasm.github.io/docs/wasm-bindgen/)
- [nectar-primitives Documentation](https://docs.rs/nectar-primitives)
