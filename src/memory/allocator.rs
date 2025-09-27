use crate::utils::config::MemoryConfig;
use anyhow::Result;
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

/// Custom memory allocator for high-performance memory management
pub struct OneEngineAllocator {
    config: MemoryConfig,
    allocated_bytes: AtomicUsize,
}

impl OneEngineAllocator {
    pub fn new(config: MemoryConfig) -> Self {
        Self {
            config,
            allocated_bytes: AtomicUsize::new(0),
        }
    }

    pub fn get_allocated_bytes(&self) -> usize {
        self.allocated_bytes.load(Ordering::Relaxed)
    }

    pub fn get_memory_usage_percentage(&self) -> f64 {
        let allocated = self.get_allocated_bytes();
        let max_bytes = self.config.max_memory_mb as usize * 1024 * 1024;
        allocated as f64 / max_bytes as f64
    }
}

unsafe impl GlobalAlloc for OneEngineAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let size = layout.size();
        
        // Check memory limits
        let current = self.allocated_bytes.load(Ordering::Relaxed);
        let max_bytes = self.config.max_memory_mb as usize * 1024 * 1024;
        
        if current + size > max_bytes {
            return std::ptr::null_mut();
        }
        
        // Allocate using system allocator
        let ptr = System.alloc(layout);
        if !ptr.is_null() {
            self.allocated_bytes.fetch_add(size, Ordering::Relaxed);
        }
        
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        let size = layout.size();
        System.dealloc(ptr, layout);
        self.allocated_bytes.fetch_sub(size, Ordering::Relaxed);
    }
}
