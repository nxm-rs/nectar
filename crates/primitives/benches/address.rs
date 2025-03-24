#![allow(missing_docs)]
use alloy_primitives::{B256, b256};
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use nectar_primitives::address::SwarmAddress;
use rand::prelude::*;

pub fn address_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("address");
    let mut rng = rand::thread_rng();

    // Generate random addresses for benchmarking
    let addresses: Vec<SwarmAddress> = (0..1000)
        .map(|_| {
            let mut bytes = [0u8; 32];
            rng.fill(&mut bytes);
            SwarmAddress::from(B256::from_slice(&bytes))
        })
        .collect();

    // Define some fixed addresses for consistent benchmarks
    let base_addr = SwarmAddress::from(b256!(
        "9100000000000000000000000000000000000000000000000000000000000000"
    ));
    let near_addr = SwarmAddress::from(b256!(
        "9180000000000000000000000000000000000000000000000000000000000000"
    ));
    let far_addr = SwarmAddress::from(b256!(
        "1100000000000000000000000000000000000000000000000000000000000000"
    ));

    // Benchmark proximity calculation between addresses
    group.bench_function("proximity_same", |b| {
        b.iter(|| black_box(base_addr.proximity(&base_addr)))
    });

    group.bench_function("proximity_near", |b| {
        b.iter(|| black_box(base_addr.proximity(&near_addr)))
    });

    group.bench_function("proximity_far", |b| {
        b.iter(|| black_box(base_addr.proximity(&far_addr)))
    });

    // Benchmark extended proximity calculation between addresses
    group.bench_function("extended_proximity_same", |b| {
        b.iter(|| black_box(base_addr.extended_proximity(&base_addr)))
    });

    group.bench_function("extended_proximity_near", |b| {
        b.iter(|| black_box(base_addr.extended_proximity(&near_addr)))
    });

    group.bench_function("extended_proximity_far", |b| {
        b.iter(|| black_box(base_addr.extended_proximity(&far_addr)))
    });

    // Benchmark distance calculation between addresses
    group.bench_function("distance_same", |b| {
        b.iter(|| black_box(base_addr.distance(&base_addr)))
    });

    group.bench_function("distance_near", |b| {
        b.iter(|| black_box(base_addr.distance(&near_addr)))
    });

    group.bench_function("distance_far", |b| {
        b.iter(|| black_box(base_addr.distance(&far_addr)))
    });

    // Benchmark distance comparison between addresses
    group.bench_function("distance_cmp_same", |b| {
        b.iter(|| black_box(base_addr.distance_cmp(&near_addr, &near_addr)))
    });

    group.bench_function("distance_cmp_different", |b| {
        b.iter(|| black_box(base_addr.distance_cmp(&near_addr, &far_addr)))
    });

    // Benchmark "closer" function between addresses
    group.bench_function("closer", |b| {
        b.iter(|| black_box(base_addr.closer(&near_addr, &far_addr)))
    });

    // Benchmark proximity across different PO values
    let po_test_cases = [
        // Test address with 0 matching bits (PO = 0)
        b256!("8000000000000000000000000000000000000000000000000000000000000000"),
        // Test address with 7 matching bits (PO = 7)
        b256!("0100000000000000000000000000000000000000000000000000000000000000"),
        // Test address with 15 matching bits (PO = 15)
        b256!("0001000000000000000000000000000000000000000000000000000000000000"),
        // Test address with 23 matching bits (PO = 23)
        b256!("0000010000000000000000000000000000000000000000000000000000000000"),
        // Test address with 31 matching bits (PO = 31)
        b256!("0000000100000000000000000000000000000000000000000000000000000000"),
    ];

    for (i, &addr_bytes) in po_test_cases.iter().enumerate() {
        let expected_po = i * 8;
        let test_addr = SwarmAddress::from(addr_bytes);
        group.bench_with_input(
            BenchmarkId::new("proximity_po", expected_po),
            &expected_po,
            |b, _| b.iter(|| black_box(SwarmAddress::zero().proximity(&test_addr))),
        );
    }

    // Benchmark with real-world usage patterns
    // Scenario: Finding closest addresses from a pool
    group.bench_function("find_closest_address", |b| {
        let target = &addresses[0];
        let pool = &addresses[1..100]; // Use 99 other addresses

        b.iter(|| {
            // Find the closest address to target from the pool
            let closest = pool.iter().min_by(|&a, &b| target.distance_cmp(a, b));
            black_box(closest)
        })
    });

    // Benchmark batch proximity calculation (a common operation in Kademlia)
    let batch_sizes = [10, 100, 500];
    for &size in &batch_sizes {
        group.bench_with_input(
            BenchmarkId::new("batch_proximity", size),
            &size,
            |b, &size| {
                b.iter(|| {
                    for i in 0..size {
                        black_box(base_addr.proximity(&addresses[i % addresses.len()]));
                    }
                })
            },
        );
    }

    // Benchmark batch distance calculation
    for &size in &batch_sizes {
        group.bench_with_input(
            BenchmarkId::new("batch_distance", size),
            &size,
            |b, &size| {
                b.iter(|| {
                    for i in 0..size {
                        black_box(base_addr.distance(&addresses[i % addresses.len()]));
                    }
                })
            },
        );
    }

    group.finish();
}

criterion_group!(benches, address_benchmarks);
criterion_main!(benches);
