//! Emits the `multi_thread` cfg alias.

fn main() {
    cfg_aliases::cfg_aliases! {
        // Send/Sync bounds apply; wasm32 and the `unsync` feature relax them.
        multi_thread: { not(any(target_arch = "wasm32", feature = "unsync")) },
    }
}
