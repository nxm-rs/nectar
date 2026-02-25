fn main() {
    use alloy_primitives::{B256, U256};
    
    // Test B256 - it's a fixed-size byte array wrapper
    let b = B256::default();
    println!("B256 as_slice: {:?}", b.as_slice());
    
    // Check if we can do bitwise operations
    // B256 wraps [u8; 32] directly
    println!("B256 len: {}", b.len());
    
    // BitVec usage pattern
}
