//! Fuzz the streaming read facade for split-then-read consistency.
//!
//! One store is filled by the streaming split over fuzzed bytes; the oracle
//! is byte equality between the whole-file collect, a buffered reader
//! drain, and the source bytes. A small body size keeps multi-level trees
//! reachable from short fuzz inputs.

#![no_main]

use std::sync::Arc;

use nectar_testing::run;
use libfuzzer_sys::fuzz_target;
use nectar_file::{File, Plain};
use nectar_fuzz::{split, tile};

/// Tiny body size: fan-out 8, so a few KiB already builds a deep tree.
const BODY: usize = 256;

fuzz_target!(|input: (Vec<u8>, u16)| {
    let (seed, copies) = input;
    let data = tile(&seed, copies);

    let (root, store) = split(&data);

    let collected = run(async {
        let file = File::<_, Plain, BODY>::open(Arc::clone(&store), root)
            .await
            .expect("open must succeed over a complete store");
        file.collect(u64::MAX)
            .await
            .expect("collect must succeed over a complete store")
    });
    assert_eq!(collected, data, "collect diverged from the source bytes");

    // The buffered reader drains the same bytes through its own path.
    let drained = run(async {
        let file = File::<_, Plain, BODY>::open(store, root)
            .await
            .expect("reopen must succeed over a complete store");
        let mut reader = file.read().build();
        let mut out = Vec::new();
        let mut buf = [0u8; 97];
        loop {
            let n = reader
                .read(&mut buf)
                .await
                .expect("read must succeed over a complete store");
            if n == 0 {
                break;
            }
            out.extend_from_slice(&buf[..n]);
        }
        out
    });
    assert_eq!(drained, data, "reader drain diverged from the collect");
});
