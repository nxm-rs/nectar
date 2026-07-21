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

        # Pinned to match CI and rust-toolchain.toml exactly. Develop with
        # `nix develop` so the local toolchain matches CI/CD.
        rustToolchain = pkgs.rust-bin.stable."1.94.0".default.override {
          extensions = [ "rust-analyzer" "rust-src" "clippy" "rustfmt" ];
          targets = [ "wasm32-unknown-unknown" "riscv64imac-unknown-none-elf" ];
        };

        # Nightly toolchain for WASM threading (wasm-bindgen-rayon)
        # Required for -Z build-std and atomics/bulk-memory features
        rustNightly = pkgs.rust-bin.nightly.latest.default.override {
          extensions = [ "rust-src" "clippy" "rustfmt" ];
          targets = [ "wasm32-unknown-unknown" ];
        };

        # Nightly toolchain for fuzzing (cargo-fuzz needs -Zsanitizer et al).
        # llvm-tools supplies the llvm-profdata/llvm-cov binaries that
        # `cargo fuzz coverage` looks up via the rustc sysroot.
        rustFuzz = pkgs.rust-bin.nightly.latest.default.override {
          extensions = [ "rust-src" "clippy" "rustfmt" "llvm-tools-preview" ];
        };
      in
      {
        devShells.default = pkgs.mkShell {
          name = "nectar-dev";

          buildInputs = with pkgs; [
            rustToolchain
            rustNightly
            cargo-nextest
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
          ] ++ pkgs.lib.optionals pkgs.stdenv.isLinux [ pkgs.mold ];

          OPENSSL_DIR = "${pkgs.openssl.dev}";
          OPENSSL_LIB_DIR = "${pkgs.openssl.out}/lib";
          PKG_CONFIG_PATH = "${pkgs.openssl.dev}/lib/pkgconfig";

          # Link native Linux builds with mold. Scoped per-target so it never
          # touches wasm linking, and set as env (not a committed .cargo config)
          # so CI keeps its own linker setup.
          CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS =
            pkgs.lib.optionalString pkgs.stdenv.isLinux "-Clink-arg=-fuse-ld=mold";
          CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_RUSTFLAGS =
            pkgs.lib.optionalString pkgs.stdenv.isLinux "-Clink-arg=-fuse-ld=mold";

          shellHook = ''
            # Opt into sccache only when the host provides it: a client must
            # match its server's version exactly, so a copy pinned by this flake
            # would fight the host server for the socket.
            if command -v sccache >/dev/null; then
              export RUSTC_WRAPPER=sccache
              export CARGO_INCREMENTAL=0 # sccache and incremental are exclusive
            fi
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

        # Dedicated shell for fuzzing (see fuzz/README.md). Nightly is the
        # default cargo here so `cargo fuzz run <target>` just works.
        devShells.fuzz = pkgs.mkShell {
          name = "nectar-fuzz";

          buildInputs = with pkgs; [
            rustFuzz
            cargo-fuzz
            # libfuzzer-sys compiles the libFuzzer C++ runtime via the `cc`
            # crate, which needs a working clang/clang++.
            clang
            pkg-config
            openssl
            openssl.dev
          ];

          OPENSSL_DIR = "${pkgs.openssl.dev}";
          OPENSSL_LIB_DIR = "${pkgs.openssl.out}/lib";
          PKG_CONFIG_PATH = "${pkgs.openssl.dev}/lib/pkgconfig";
        };
      }
    );
}
