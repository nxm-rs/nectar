//! Caching utilities for lazy computed values
//!
//! This module provides components for caching expensive computations
//! that only need to be calculated once.

use std::sync::OnceLock;

/// Generic cache for lazily computed values.
///
/// This structure provides an efficient way to cache and retrieve any value
/// that only needs to be computed once, computing it only when first needed.
#[derive(Debug)]
pub(crate) struct OnceCache<T> {
    /// The cached value
    value: OnceLock<T>,
}

impl<T> OnceCache<T> {
    /// Create a new empty cache
    pub(crate) fn new() -> Self {
        Self {
            value: OnceLock::new(),
        }
    }

    /// Create a new cache with a pre-computed value
    pub(crate) fn with_value(value: T) -> Self {
        let cache = Self::new();
        // This will only fail if the value is already set, which is impossible for a new cache
        let _ = cache.value.set(value);
        cache
    }

    /// Get the cached value if it exists
    pub(crate) fn get(&self) -> Option<&T> {
        self.value.get()
    }

    /// Try to set the cached value, returning Ok if successful or Err if already set
    pub(crate) fn try_set(&self, value: T) -> Result<(), T> {
        self.value.set(value)
    }

    /// Get the cached value, computing it if necessary
    pub(crate) fn get_or_compute<F>(&self, compute_fn: F) -> &T
    where
        F: FnOnce() -> T,
    {
        self.value.get_or_init(compute_fn)
    }
}

impl<T> Default for OnceCache<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Clone> Clone for OnceCache<T> {
    fn clone(&self) -> Self {
        if let Some(value) = self.value.get() {
            Self::with_value(value.clone())
        } else {
            Self::new()
        }
    }
}
