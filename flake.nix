{
  description = "Nectar - Swarm primitives for Rust";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, flake-utils, rust-overlay }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs {
          inherit system overlays;
        };

        # Stable toolchain for regular development
        rustToolchain = pkgs.rust-bin.stable.latest.default.override {
          extensions = [ "rust-analyzer" "rust-src" "clippy" "rustfmt" ];
          targets = [ "wasm32-unknown-unknown" ];
        };

        # Nightly toolchain for WASM threading (wasm-bindgen-rayon)
        # Required for -Z build-std and atomics/bulk-memory features
        rustNightly = pkgs.rust-bin.nightly.latest.default.override {
          extensions = [ "rust-src" "clippy" "rustfmt" ];
          targets = [ "wasm32-unknown-unknown" ];
        };
      in
      {
        devShells.default = pkgs.mkShell {
          name = "nectar-dev";

          buildInputs = with pkgs; [
            rustToolchain
            rustNightly
            wasm-pack
            wasm-bindgen-cli
            miniserve
            pkg-config
            openssl
            openssl.dev
            # Release tooling: cut releases (cargo-release), generate the
            # changelog (git-cliff), and run the pre-release advisory checks.
            cargo-release
            git-cliff
            cargo-deny
            cargo-audit
          ];

          OPENSSL_DIR = "${pkgs.openssl.dev}";
          OPENSSL_LIB_DIR = "${pkgs.openssl.out}/lib";
          PKG_CONFIG_PATH = "${pkgs.openssl.dev}/lib/pkgconfig";

          shellHook = ''
            # Alias for building WASM with threading support
            alias wasm-build-threaded='RUSTFLAGS="-C target-feature=+atomics,+bulk-memory,+mutable-globals" cargo +nightly build --target wasm32-unknown-unknown -Z build-std=panic_abort,std'
          '';
        };

        # Dedicated shell for WASM development with nightly as default
        devShells.wasm = pkgs.mkShell {
          name = "nectar-wasm";

          buildInputs = with pkgs; [
            rustNightly
            wasm-pack
            wasm-bindgen-cli
            miniserve
            pkg-config
            openssl
            openssl.dev
          ];

          OPENSSL_DIR = "${pkgs.openssl.dev}";
          OPENSSL_LIB_DIR = "${pkgs.openssl.out}/lib";
          PKG_CONFIG_PATH = "${pkgs.openssl.dev}/lib/pkgconfig";

          # Required for wasm-bindgen-rayon (SharedArrayBuffer + atomics)
          # See: https://github.com/RReverser/wasm-bindgen-rayon
          RUSTFLAGS = "-C target-feature=+atomics,+bulk-memory,+mutable-globals -C link-arg=--shared-memory -C link-arg=--max-memory=1073741824 -C link-arg=--import-memory -C link-arg=--export=__wasm_init_tls -C link-arg=--export=__tls_size -C link-arg=--export=__tls_align -C link-arg=--export=__tls_base";

          shellHook = ''
            alias wasm-serve='miniserve --header "Cross-Origin-Opener-Policy:same-origin" --header "Cross-Origin-Embedder-Policy:require-corp" -p 8080'
          '';
        };
      }
    );
}
