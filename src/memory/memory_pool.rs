use crate::utils::config::MemoryConfig;
use anyhow::Result;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, warn};

/// High-performance memory pool for efficient memory management
pub struct MemoryPool {
    config: MemoryConfig,
    pools: Arc<RwLock<HashMap<usize, VecDeque<MemoryBlock>>>>,
    total_allocated: Arc<RwLock<usize>>,
    total_capacity: usize,
}

/// A memory block in the pool
#[derive(Debug, Clone)]
pub struct MemoryBlock {
    pub data: Vec<u8>,
    pub size: usize,
    pub is_allocated: bool,
    pub allocation_time: std::time::Instant,
}

/// Memory allocation statistics
#[derive(Debug, Clone)]
pub struct MemoryStats {
    pub total_capacity: usize,
    pub total_allocated: usize,
    pub available_memory: usize,
    pub allocation_count: usize,
    pub pool_sizes: HashMap<usize, usize>,
}

impl MemoryPool {
    /// Create a new memory pool
    pub fn new(config: MemoryConfig) -> Self {
        let total_capacity = config.max_memory_mb as usize * 1024 * 1024; // Convert MB to bytes
        
        Self {
            config,
            pools: Arc::new(RwLock::new(HashMap::new())),
            total_allocated: Arc::new(RwLock::new(0)),
            total_capacity,
        }
    }

    /// Allocate memory of the specified size
    pub async fn allocate(&self, size: usize) -> Result<MemoryBlock> {
        debug!("Allocating memory block of size: {} bytes", size);

        // Check if we have enough capacity
        let current_allocated = *self.total_allocated.read().await;
        if current_allocated + size > self.total_capacity {
            return Err(anyhow::anyhow!("Insufficient memory capacity"));
        }

        // Try to get from pool first
        if let Some(block) = self.try_get_from_pool(size).await? {
            return Ok(block);
        }

        // Allocate new block
        let block = MemoryBlock {
            data: vec![0u8; size],
            size,
            is_allocated: true,
            allocation_time: std::time::Instant::now(),
        };

        // Update allocation count
        {
            let mut total = self.total_allocated.write().await;
            *total += size;
        }

        debug!("Allocated new memory block of size: {} bytes", size);
        Ok(block)
    }

    /// Deallocate a memory block
    pub async fn deallocate(&self, mut block: MemoryBlock) -> Result<()> {
        debug!("Deallocating memory block of size: {} bytes", block.size);

        let block_size = block.size;
        block.is_allocated = false;

        // Return to pool if pooling is enabled
        if self.config.enable_memory_pooling {
            self.return_to_pool(block).await?;
        }

        // Update allocation count
        {
            let mut total = self.total_allocated.write().await;
            *total = total.saturating_sub(block_size);
        }

        debug!("Deallocated memory block of size: {} bytes", block_size);
        Ok(())
    }

    /// Try to get a block from the pool
    async fn try_get_from_pool(&self, size: usize) -> Result<Option<MemoryBlock>> {
        let mut pools = self.pools.write().await;
        
        if let Some(pool) = pools.get_mut(&size) {
            if let Some(mut block) = pool.pop_front() {
                block.is_allocated = true;
                block.allocation_time = std::time::Instant::now();
                debug!("Retrieved block from pool for size: {} bytes", size);
                return Ok(Some(block));
            }
        }

        Ok(None)
    }

    /// Return a block to the pool
    async fn return_to_pool(&self, block: MemoryBlock) -> Result<()> {
        let mut pools = self.pools.write().await;
        let block_size = block.size;
        
        // Limit pool size to prevent memory bloat
        let max_pool_size = 100;
        
        if let Some(pool) = pools.get_mut(&block_size) {
            if pool.len() < max_pool_size {
                pool.push_back(block);
                debug!("Returned block to pool for size: {} bytes", block_size);
            }
        } else {
            let mut new_pool = VecDeque::new();
            new_pool.push_back(block);
            pools.insert(block_size, new_pool);
            debug!("Created new pool for size: {} bytes", block_size);
        }

        Ok(())
    }

    /// Get memory statistics
    pub async fn get_stats(&self) -> MemoryStats {
        let pools = self.pools.read().await;
        let total_allocated = *self.total_allocated.read().await;
        
        let pool_sizes: HashMap<usize, usize> = pools
            .iter()
            .map(|(size, pool)| (*size, pool.len()))
            .collect();

        let allocation_count: usize = pool_sizes.values().sum();

        MemoryStats {
            total_capacity: self.total_capacity,
            total_allocated,
            available_memory: self.total_capacity - total_allocated,
            allocation_count,
            pool_sizes,
        }
    }

    /// Check if garbage collection is needed
    pub async fn needs_gc(&self) -> bool {
        let total_allocated = *self.total_allocated.read().await;
        let threshold = (self.total_capacity as f64 * self.config.gc_threshold) as usize;
        total_allocated > threshold
    }

    /// Perform garbage collection
    pub async fn garbage_collect(&self) -> Result<usize> {
        debug!("Starting garbage collection");

        let mut pools = self.pools.write().await;
        let mut cleaned_blocks = 0;

        // Remove old blocks from pools
        for (_, pool) in pools.iter_mut() {
            let now = std::time::Instant::now();
            let max_age = std::time::Duration::from_secs(300); // 5 minutes

            while let Some(block) = pool.front() {
                if now.duration_since(block.allocation_time) > max_age {
                    pool.pop_front();
                    cleaned_blocks += 1;
                } else {
                    break;
                }
            }
        }

        debug!("Garbage collection completed, cleaned {} blocks", cleaned_blocks);
        Ok(cleaned_blocks)
    }

    /// Clear all pools
    pub async fn clear_pools(&self) {
        let mut pools = self.pools.write().await;
        pools.clear();
        debug!("All memory pools cleared");
    }
}
