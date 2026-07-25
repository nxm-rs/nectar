//! Emits the `multi_thread` cfg alias.

fn main() {
    // Send/Sync bounds apply; wasm32, bare metal, and the `unsync` feature
    // relax them.
    println!("cargo::rustc-check-cfg=cfg(multi_thread)");
    let wasm32 = std::env::var("CARGO_CFG_TARGET_ARCH").as_deref() == Ok("wasm32");
    let bare_metal = std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("none");
    let unsync = std::env::var_os("CARGO_FEATURE_UNSYNC").is_some();
    if !wasm32 && !bare_metal && !unsync {
        println!("cargo::rustc-cfg=multi_thread");
    }
}
