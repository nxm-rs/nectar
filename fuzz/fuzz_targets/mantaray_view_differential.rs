//! Differential decode fuzz: the width-pinned node decoder vs [`NodeView`].
//!
//! The view reads its reference width from each node's own header, so raw
//! attacker-controlled bytes drive both decoders through the shared
//! `nectar_mantaray::oracles::view_differential` oracle, whose invariant is
//! threefold: the view accepts exactly when either width-pinned decode
//! accepts, an accepted view agrees field-by-field with the accepting width
//! (fork flags by containment of the named `NodeType` bits), and the view's
//! emit/decode pair is a fixed point.
//!
//! Seeds live in `fuzz/seeds/mantaray_view_differential/` and are replayed on
//! stable by `seed_replay_mantaray_view_differential` in
//! `crates/mantaray/src/view.rs`, through the same oracle.

#![no_main]

use libfuzzer_sys::fuzz_target;
use nectar_mantaray::oracles;

fuzz_target!(|data: &[u8]| {
    oracles::view_differential(data).expect("the view and the node decoder must agree");
});
