use crate::columnar::batch::Batch;
use anyhow::Result;
use std::sync::Arc;
use std::collections::HashMap;
use uuid::Uuid;

/// Execution context for operators
#[derive(Debug, Clone)]
pub struct ExecContext {
    pub query_id: Uuid,
    pub session_id: Uuid,
    pub config: ExecConfig,
    pub metrics: Arc<ExecMetrics>,
    pub memory_pool: Arc<MemoryPool>,
}

/// Execution configuration
#[derive(Debug, Clone)]
pub struct ExecConfig {
    pub batch_size: usize,
    pub max_memory: usize,
    pub enable_simd: bool,
    pub enable_spill: bool,
    pub spill_threshold: usize,
    pub num_threads: usize,
}

impl Default for ExecConfig {
    fn default() -> Self {
        Self {
            batch_size: 4096,
            max_memory: 1024 * 1024 * 1024, // 1GB
            enable_simd: true,
            enable_spill: true,
            spill_threshold: 512 * 1024 * 1024, // 512MB
            num_threads: num_cpus::get(),
        }
    }
}

/// Execution metrics
#[derive(Debug, Clone)]
pub struct ExecMetrics {
    pub rows_processed: u64,
    pub bytes_processed: u64,
    pub cpu_time_ns: u64,
    pub memory_used: usize,
    pub spill_bytes: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
}

impl Default for ExecMetrics {
    fn default() -> Self {
        Self {
            rows_processed: 0,
            bytes_processed: 0,
            cpu_time_ns: 0,
            memory_used: 0,
            spill_bytes: 0,
            cache_hits: 0,
            cache_misses: 0,
        }
    }
}

/// Memory pool for execution
#[derive(Debug)]
pub struct MemoryPool {
    pub total_memory: usize,
    pub used_memory: usize,
    pub allocations: HashMap<Uuid, usize>,
}

impl MemoryPool {
    pub fn new(total_memory: usize) -> Self {
        Self {
            total_memory,
            used_memory: 0,
            allocations: HashMap::new(),
        }
    }

    pub fn allocate(&mut self, size: usize) -> Result<MemoryAllocation> {
        if self.used_memory + size > self.total_memory {
            return Err(anyhow::anyhow!("Insufficient memory"));
        }
        
        let allocation_id = Uuid::new_v4();
        self.allocations.insert(allocation_id, size);
        self.used_memory += size;
        
        Ok(MemoryAllocation {
            id: allocation_id,
            size,
        })
    }

    pub fn deallocate(&mut self, allocation: MemoryAllocation) {
        if let Some(size) = self.allocations.remove(&allocation.id) {
            self.used_memory = self.used_memory.saturating_sub(size);
        }
    }

    pub fn available_memory(&self) -> usize {
        self.total_memory - self.used_memory
    }
}

#[derive(Debug, Clone)]
pub struct MemoryAllocation {
    pub id: Uuid,
    pub size: usize,
}
