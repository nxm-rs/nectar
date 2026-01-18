//! Memory comparison between batch-collect (rayon) and streaming (hybrid) approaches.
//!
//! Run with: cargo run --example memory_comparison --features "parallel,streaming" --release
//!
//! Key insight: Streaming saves memory when processing data from an EXTERNAL source
//! (disk, network) without loading everything into memory first.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use alloy_primitives::B256;
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use nectar_postage::parallel::{sign_stamps_parallel, ShardedIssuer};
use nectar_postage::streaming::{streaming_signer, SignRequest};
use nectar_primitives::SwarmAddress;

/// A global allocator wrapper that tracks memory usage.
struct TrackingAllocator {
    current: AtomicUsize,
    peak: AtomicUsize,
}

impl TrackingAllocator {
    const fn new() -> Self {
        Self {
            current: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        }
    }

    fn current(&self) -> usize {
        self.current.load(Ordering::Relaxed)
    }

    fn peak(&self) -> usize {
        self.peak.load(Ordering::Relaxed)
    }

    fn reset_peak(&self) {
        self.peak.store(self.current.load(Ordering::Relaxed), Ordering::Relaxed);
    }
}

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            let size = layout.size();
            let current = self.current.fetch_add(size, Ordering::Relaxed) + size;
            let mut peak = self.peak.load(Ordering::Relaxed);
            while current > peak {
                match self.peak.compare_exchange_weak(peak, current, Ordering::Relaxed, Ordering::Relaxed) {
                    Ok(_) => break,
                    Err(p) => peak = p,
                }
            }
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        self.current.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) };
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
        if !new_ptr.is_null() {
            let old_size = layout.size();
            if new_size > old_size {
                let diff = new_size - old_size;
                let current = self.current.fetch_add(diff, Ordering::Relaxed) + diff;
                let mut peak = self.peak.load(Ordering::Relaxed);
                while current > peak {
                    match self.peak.compare_exchange_weak(peak, current, Ordering::Relaxed, Ordering::Relaxed) {
                        Ok(_) => break,
                        Err(p) => peak = p,
                    }
                }
            } else {
                self.current.fetch_sub(old_size - new_size, Ordering::Relaxed);
            }
        }
        new_ptr
    }
}

#[global_allocator]
static ALLOCATOR: TrackingAllocator = TrackingAllocator::new();

fn random_address() -> SwarmAddress {
    let mut bytes = [0u8; 32];
    for b in &mut bytes {
        *b = rand::random();
    }
    SwarmAddress::new(bytes)
}

fn format_bytes(bytes: usize) -> String {
    if bytes >= 1024 * 1024 {
        format!("{:.2} MB", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.2} KB", bytes as f64 / 1024.0)
    } else {
        format!("{} B", bytes)
    }
}

/// Batch-collect approach: MUST load all addresses into memory first, then all results
fn bench_batch_collect(count: usize, signer: &PrivateKeySigner) -> (usize, usize) {
    ALLOCATOR.reset_peak();
    let baseline = ALLOCATOR.current();

    // Step 1: Load all addresses into memory (simulates loading from disk)
    let addresses: Vec<SwarmAddress> = (0..count).map(|_| random_address()).collect();
    let after_input = ALLOCATOR.peak();

    let sign_fn = |prehash: &B256| signer.sign_message_sync(prehash.as_slice());
    let issuer = ShardedIssuer::new(B256::ZERO, 32, 16);

    // Step 2: Process all, collect all results
    let results = sign_stamps_parallel(&issuer, &sign_fn, &addresses);
    let _count = results.iter().filter(|r| r.result.is_ok()).count();

    let peak = ALLOCATOR.peak();
    (after_input.saturating_sub(baseline), peak.saturating_sub(baseline))
}

/// Streaming approach: processes items as they're generated, bounded memory
fn bench_streaming(
    rt: &tokio::runtime::Runtime,
    count: usize,
    signer: &PrivateKeySigner,
    channel_size: usize,
    batch_size: usize,
) -> usize {
    ALLOCATOR.reset_peak();
    let baseline = ALLOCATOR.current();

    rt.block_on(async {
        let signer_clone = signer.clone();
        let sign_fn = Arc::new(move |prehash: &B256| {
            signer_clone.sign_message_sync(prehash.as_slice())
        });

        let issuer = Arc::new(ShardedIssuer::new(B256::ZERO, 32, 16));
        let tx = streaming_signer(issuer, sign_fn, channel_size, batch_size);

        // Simulate: generate addresses on-the-fly, process results immediately
        // Only `channel_size` items are in flight at once
        let mut pending = Vec::with_capacity(channel_size);
        let mut success_count = 0usize;

        for i in 0..count {
            // Generate address on-demand (simulates reading from disk/network)
            let addr = random_address();

            let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();

            // Send may block if channel is full (backpressure)
            tx.send(SignRequest {
                address: addr,
                response: resp_tx,
            })
            .await
            .unwrap();

            pending.push(resp_rx);

            // When we hit channel_size, drain some responses to make room
            if pending.len() >= channel_size || i == count - 1 {
                // Process all pending responses
                for rx in pending.drain(..) {
                    if let Ok(Ok(_stamp)) = rx.await {
                        success_count += 1;
                    }
                }
            }
        }

        let _ = success_count;
    });

    let peak = ALLOCATOR.peak();
    peak.saturating_sub(baseline)
}

fn main() {
    println!("Memory Comparison: Batch-Collect vs Streaming\n");
    println!("==============================================\n");
    println!("Simulates real-world usage: data comes from external source\n");
    println!("Batch-collect: must load ALL input + ALL output into memory");
    println!("Streaming: only 'channel_size' items in memory at once\n");

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let signer = PrivateKeySigner::random();

    for &count in &[1_000, 10_000, 100_000] {
        println!("Processing {} stamps:", count);
        println!("--------------------------");

        // Batch collect - must load all into memory
        let (batch_input, batch_total) = bench_batch_collect(count, &signer);

        // Streaming with different channel sizes
        let stream_100 = bench_streaming(&rt, count, &signer, 100, 256);
        let stream_500 = bench_streaming(&rt, count, &signer, 500, 256);
        let stream_1000 = bench_streaming(&rt, count, &signer, 1000, 256);

        println!("  Batch-collect:");
        println!("    Input memory:   {}", format_bytes(batch_input));
        println!("    Peak total:     {}", format_bytes(batch_total));
        println!();
        println!("  Streaming (channel_size controls max in-flight):");
        println!("    channel=100:    {}", format_bytes(stream_100));
        println!("    channel=500:    {}", format_bytes(stream_500));
        println!("    channel=1000:   {}", format_bytes(stream_1000));

        // Calculate savings
        if batch_total > 0 {
            println!("\n  Memory savings vs batch-collect peak:");
            for (name, stream) in [("100", stream_100), ("500", stream_500), ("1000", stream_1000)] {
                let savings = ((batch_total as f64 - stream as f64) / batch_total as f64) * 100.0;
                println!("    channel={}: {:.1}%", name, savings);
            }
        }

        println!();
    }

    println!("Summary:");
    println!("--------");
    println!("Streaming memory is O(channel_size), not O(total_count).");
    println!("For 100k items with channel=100: ~{} vs ~{} = huge savings!",
        format_bytes(bench_streaming(&rt, 100_000, &signer, 100, 256)),
        format_bytes(bench_batch_collect(100_000, &signer).1));
}
