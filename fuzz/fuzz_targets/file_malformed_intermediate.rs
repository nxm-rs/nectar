//! Fuzz the walk's rejection of malformed intermediate chunks.
//!
//! The fuzzed input authors content-addressed chunks whose spans and bodies
//! need not obey the tree grammar, plus one synthesized intermediate
//! referencing them; the shared `nectar_file::oracles::malformed_walk`
//! oracle opens the file at each and holds the walk to typed rejection: no
//! panic, no hang, and an `Ok` collect delivers exactly the declared span.
//!
//! Seeds live in `fuzz/seeds/file_malformed_intermediate/` and are replayed
//! on stable by `seed_replay_file_malformed_intermediate` in
//! `crates/file/src/oracles.rs`, through the same oracle.

#![no_main]

use libfuzzer_sys::fuzz_target;
use nectar_file::oracles;

fuzz_target!(|input: (Vec<(u64, Vec<u8>)>, u64, u8)| {
    let (specs, root_span, arity) = input;
    let _ = oracles::malformed_walk(&specs, root_span, arity)
        .expect("the walk must reject malformed trees typed");
});
