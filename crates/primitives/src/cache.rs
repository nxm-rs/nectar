//! Caching utilities for lazy computed values
//!
//! This module provides components for caching expensive computations
//! that only need to be calculated once.

#[cfg(not(feature = "std"))]
use alloc::boxed::Box;
#[cfg(not(feature = "std"))]
use once_cell::race::OnceBox;
#[cfg(feature = "std")]
use std::sync::OnceLock;

/// Generic cache for lazily computed values.
///
/// This structure provides an efficient way to cache and retrieve any value
/// that only needs to be computed once, computing it only when first needed.
///
/// Backed by `OnceLock` under `std` and by the lock-free boxed once-cell on
/// the `no_std` side, so holders stay `Sync` on both.
#[derive(Debug)]
pub(crate) struct OnceCache<T> {
    /// The cached value
    #[cfg(feature = "std")]
    value: OnceLock<T>,
    /// The cached value
    #[cfg(not(feature = "std"))]
    value: OnceBox<T>,
}

impl<T> OnceCache<T> {
    /// Create a new empty cache
    pub(crate) const fn new() -> Self {
        Self {
            #[cfg(feature = "std")]
            value: OnceLock::new(),
            #[cfg(not(feature = "std"))]
            value: OnceBox::new(),
        }
    }

    /// Create a new cache with a pre-computed value
    pub(crate) fn with_value(value: T) -> Self {
        let cache = Self::new();
        // This will only fail if the value is already set, which is impossible for a new cache
        #[cfg(feature = "std")]
        let _ = cache.value.set(value);
        #[cfg(not(feature = "std"))]
        let _ = cache.value.set(Box::new(value));
        cache
    }

    /// Get the cached value if it has been computed
    pub(crate) fn get(&self) -> Option<&T> {
        self.value.get()
    }

    /// Get the cached value, computing it if necessary
    pub(crate) fn get_or_compute<F>(&self, compute_fn: F) -> &T
    where
        F: FnOnce() -> T,
    {
        #[cfg(feature = "std")]
        {
            self.value.get_or_init(compute_fn)
        }
        #[cfg(not(feature = "std"))]
        {
            self.value.get_or_init(|| Box::new(compute_fn()))
        }
    }
}

impl<T> Default for OnceCache<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Clone> Clone for OnceCache<T> {
    fn clone(&self) -> Self {
        self.value
            .get()
            .map_or_else(Self::new, |value| Self::with_value(value.clone()))
    }
}
