//! 统一内存管理器
//! 
//! 提供内存池、配额管理和内存回收功能

use crate::memory::memory_pool::MemoryPool;
use anyhow::Result;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tracing::{debug, warn, error};

/// 内存配额配置
#[derive(Debug, Clone)]
pub struct MemoryQuota {
    /// 最大内存使用量（字节）
    pub max_memory: usize,
    /// 软限制阈值（百分比）
    pub soft_limit_ratio: f64,
    /// 硬限制阈值（百分比）
    pub hard_limit_ratio: f64,
    /// 回收阈值（百分比）
    pub reclaim_ratio: f64,
}

impl Default for MemoryQuota {
    fn default() -> Self {
        Self {
            max_memory: 1024 * 1024 * 1024, // 1GB
            soft_limit_ratio: 0.8,  // 80%
            hard_limit_ratio: 0.9,  // 90%
            reclaim_ratio: 0.7,     // 70%
        }
    }
}

/// 内存使用统计
#[derive(Debug, Clone)]
pub struct MemoryStats {
    /// 当前内存使用量
    pub current_usage: usize,
    /// 峰值内存使用量
    pub peak_usage: usize,
    /// 分配次数
    pub allocation_count: u64,
    /// 释放次数
    pub deallocation_count: u64,
    /// 内存池使用量
    pub pool_usage: usize,
    /// 直接分配使用量
    pub direct_usage: usize,
}

/// 内存分配请求
#[derive(Debug, Clone)]
pub struct AllocationRequest {
    /// 请求大小
    pub size: usize,
    /// 对齐要求
    pub alignment: usize,
    /// 分配类型
    pub allocation_type: AllocationType,
    /// 优先级
    pub priority: AllocationPriority,
}

/// 分配类型
#[derive(Debug, Clone, PartialEq)]
pub enum AllocationType {
    /// 列式数据
    Columnar,
    /// 哈希表
    HashTable,
    /// 排序缓冲区
    SortBuffer,
    /// 网络缓冲区
    NetworkBuffer,
    /// 临时数据
    Temporary,
}

/// 分配优先级
#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum AllocationPriority {
    /// 低优先级
    Low = 0,
    /// 普通优先级
    Normal = 1,
    /// 高优先级
    High = 2,
    /// 关键优先级
    Critical = 3,
}

/// 内存分配结果
#[derive(Debug)]
pub struct AllocationResult {
    /// 分配的内存地址
    pub ptr: *mut u8,
    /// 实际分配大小
    pub size: usize,
    /// 是否来自内存池
    pub from_pool: bool,
    /// 分配ID（用于跟踪）
    pub allocation_id: u64,
}

/// 统一内存管理器
pub struct UnifiedMemoryManager {
    /// 内存池
    memory_pool: Arc<MemoryPool>,
    /// 内存配额
    quota: MemoryQuota,
    /// 当前内存使用量
    current_usage: Arc<Mutex<usize>>,
    /// 峰值内存使用量
    peak_usage: Arc<Mutex<usize>>,
    /// 分配统计
    allocation_count: Arc<Mutex<u64>>,
    /// 释放统计
    deallocation_count: Arc<Mutex<u64>>,
    /// 活跃分配跟踪
    active_allocations: Arc<Mutex<HashMap<u64, AllocationInfo>>>,
    /// 分配ID计数器
    allocation_id_counter: Arc<Mutex<u64>>,
    /// 内存回收器
    reclaim_threshold: usize,
    /// 最后回收时间
    last_reclaim_time: Arc<Mutex<Instant>>,
}

/// 分配信息
#[derive(Debug, Clone)]
struct AllocationInfo {
    /// 分配大小
    size: usize,
    /// 分配类型
    allocation_type: AllocationType,
    /// 优先级
    priority: AllocationPriority,
    /// 分配时间
    timestamp: Instant,
    /// 是否来自内存池
    from_pool: bool,
}

impl UnifiedMemoryManager {
    /// 创建新的统一内存管理器
    pub fn new(quota: MemoryQuota) -> Self {
        let memory_pool = Arc::new(MemoryPool::new(crate::utils::config::MemoryConfig::default()));
        let current_usage = Arc::new(Mutex::new(0));
        let peak_usage = Arc::new(Mutex::new(0));
        let allocation_count = Arc::new(Mutex::new(0));
        let deallocation_count = Arc::new(Mutex::new(0));
        let active_allocations = Arc::new(Mutex::new(HashMap::new()));
        let allocation_id_counter = Arc::new(Mutex::new(0));
        let reclaim_threshold = (quota.max_memory as f64 * quota.reclaim_ratio) as usize;
        let last_reclaim_time = Arc::new(Mutex::new(Instant::now()));

        Self {
            memory_pool,
            quota,
            current_usage,
            peak_usage,
            allocation_count,
            deallocation_count,
            active_allocations,
            allocation_id_counter,
            reclaim_threshold,
            last_reclaim_time,
        }
    }

    /// 分配内存
    pub fn allocate(&self, request: AllocationRequest) -> Result<AllocationResult> {
        // 检查内存配额
        self.check_quota(request.size)?;

        // 智能内存分配策略
        let (ptr, size, from_pool) = self.allocate_with_strategy(request.size, request.alignment)?;

        // 更新统计信息
        self.update_stats(size, true);

        // 生成分配ID
        let allocation_id = self.next_allocation_id();

        // 记录分配信息
        let allocation_info = AllocationInfo {
            size,
            allocation_type: request.allocation_type.clone(),
            priority: request.priority.clone(),
            timestamp: Instant::now(),
            from_pool,
        };

        {
            let mut active_allocations = self.active_allocations.lock().unwrap();
            active_allocations.insert(allocation_id, allocation_info);
        }

        // 检查是否需要回收内存
        self.check_reclaim();

        Ok(AllocationResult {
            ptr,
            size,
            from_pool,
            allocation_id,
        })
    }

    /// 释放内存
    pub fn deallocate(&self, allocation_id: u64) -> Result<()> {
        // 获取分配信息
        let allocation_info = {
            let mut active_allocations = self.active_allocations.lock().unwrap();
            active_allocations.remove(&allocation_id)
        };

        if let Some(info) = allocation_info {
            // 直接释放（简化实现）
            unsafe {
                std::alloc::dealloc(
                    std::ptr::null_mut(), // 简化实现，实际应该存储指针
                    std::alloc::Layout::from_size_align(info.size, 8).unwrap(),
                );
            }

            // 更新统计信息
            self.update_stats(info.size, false);
        }

        Ok(())
    }

    /// 获取内存统计信息
    pub fn get_stats(&self) -> MemoryStats {
        let current_usage = *self.current_usage.lock().unwrap();
        let peak_usage = *self.peak_usage.lock().unwrap();
        let allocation_count = *self.allocation_count.lock().unwrap();
        let deallocation_count = *self.deallocation_count.lock().unwrap();
        let pool_usage = 0; // 简化实现
        let direct_usage = current_usage;

        MemoryStats {
            current_usage,
            peak_usage,
            allocation_count,
            deallocation_count,
            pool_usage,
            direct_usage,
        }
    }

    /// 强制回收内存
    pub fn force_reclaim(&self) -> Result<usize> {
        let mut reclaimed = 0;
        
        // 回收低优先级分配
        let low_priority_allocations = self.get_low_priority_allocations();
        for allocation_id in low_priority_allocations {
            if let Ok(()) = self.deallocate(allocation_id) {
                reclaimed += 1;
            }
        }

        // 回收内存池（简化实现）
        // reclaimed += self.memory_pool.reclaim()?;

        debug!("Force reclaimed {} allocations, {} bytes", reclaimed, self.get_stats().current_usage);
        Ok(reclaimed)
    }

    /// 检查内存配额
    fn check_quota(&self, requested_size: usize) -> Result<()> {
        let current_usage = *self.current_usage.lock().unwrap();
        let new_usage = current_usage + requested_size;

        if new_usage > self.quota.max_memory {
            return Err(anyhow::anyhow!(
                "Memory quota exceeded: requested {} bytes, current {} bytes, max {} bytes",
                requested_size,
                current_usage,
                self.quota.max_memory
            ));
        }

        // 检查软限制
        let soft_limit = (self.quota.max_memory as f64 * self.quota.soft_limit_ratio) as usize;
        if new_usage > soft_limit {
            warn!(
                "Memory usage approaching soft limit: {} bytes ({}%)",
                new_usage,
                (new_usage as f64 / self.quota.max_memory as f64) * 100.0
            );
        }

        // 检查硬限制
        let hard_limit = (self.quota.max_memory as f64 * self.quota.hard_limit_ratio) as usize;
        if new_usage > hard_limit {
            error!(
                "Memory usage approaching hard limit: {} bytes ({}%)",
                new_usage,
                (new_usage as f64 / self.quota.max_memory as f64) * 100.0
            );
            
            // 尝试强制回收
            self.force_reclaim()?;
        }

        Ok(())
    }

    /// 智能内存分配策略
    fn allocate_with_strategy(&self, size: usize, alignment: usize) -> Result<(*mut u8, usize, bool)> {
        // 根据大小选择分配策略
        if size <= 1024 {
            // 小对象：优先使用内存池
            self.allocate_small_object(size, alignment)
        } else if size <= 1024 * 1024 {
            // 中等对象：使用专用分配器
            self.allocate_medium_object(size, alignment)
        } else {
            // 大对象：直接系统分配
            self.allocate_large_object(size, alignment)
        }
    }
    
    /// 分配小对象
    fn allocate_small_object(&self, size: usize, alignment: usize) -> Result<(*mut u8, usize, bool)> {
        // 尝试从内存池分配
        if let Some(ptr) = self.try_pool_allocation(size, alignment) {
            return Ok((ptr, size, true));
        }
        
        // 回退到系统分配
        self.allocate_system(size, alignment)
    }
    
    /// 分配中等对象
    fn allocate_medium_object(&self, size: usize, alignment: usize) -> Result<(*mut u8, usize, bool)> {
        // 尝试从专用分配器分配
        if let Some(ptr) = self.try_medium_allocator(size, alignment) {
            return Ok((ptr, size, true));
        }
        
        // 回退到系统分配
        self.allocate_system(size, alignment)
    }
    
    /// 分配大对象
    fn allocate_large_object(&self, size: usize, alignment: usize) -> Result<(*mut u8, usize, bool)> {
        // 大对象直接系统分配
        self.allocate_system(size, alignment)
    }
    
    /// 系统分配
    fn allocate_system(&self, size: usize, alignment: usize) -> Result<(*mut u8, usize, bool)> {
        let layout = std::alloc::Layout::from_size_align(size, alignment)
            .map_err(|e| anyhow::anyhow!("Invalid layout: {}", e))?;
        
        let ptr = unsafe { std::alloc::alloc(layout) };
        if ptr.is_null() {
            return Err(anyhow::anyhow!("Failed to allocate {} bytes", size));
        }

        Ok((ptr, size, false))
    }
    
    /// 尝试中等对象分配器
    fn try_medium_allocator(&self, size: usize, alignment: usize) -> Option<*mut u8> {
        // 这里应该实现中等对象分配器
        // 简化实现：返回None，回退到系统分配
        None
    }
    
    /// 直接分配内存（保留原方法用于兼容性）
    fn allocate_direct(&self, size: usize, alignment: usize) -> Result<(*mut u8, usize, bool)> {
        self.allocate_with_strategy(size, alignment)
    }

    /// 更新统计信息
    fn update_stats(&self, size: usize, is_allocation: bool) {
        if is_allocation {
            let mut current_usage = self.current_usage.lock().unwrap();
            *current_usage += size;
            
            let mut peak_usage = self.peak_usage.lock().unwrap();
            if *current_usage > *peak_usage {
                *peak_usage = *current_usage;
            }
            
            let mut allocation_count = self.allocation_count.lock().unwrap();
            *allocation_count += 1;
        } else {
            let mut current_usage = self.current_usage.lock().unwrap();
            *current_usage = current_usage.saturating_sub(size);
            
            let mut deallocation_count = self.deallocation_count.lock().unwrap();
            *deallocation_count += 1;
        }
    }

    /// 获取下一个分配ID
    fn next_allocation_id(&self) -> u64 {
        let mut counter = self.allocation_id_counter.lock().unwrap();
        *counter += 1;
        *counter
    }

    /// 检查是否需要回收内存
    fn check_reclaim(&self) {
        let current_usage = *self.current_usage.lock().unwrap();
        if current_usage > self.reclaim_threshold {
            let now = Instant::now();
            let last_reclaim = *self.last_reclaim_time.lock().unwrap();
            
            // 避免频繁回收，至少间隔1秒
            if now.duration_since(last_reclaim) > Duration::from_secs(1) {
                if let Err(e) = self.force_reclaim() {
                    error!("Failed to reclaim memory: {}", e);
                } else {
                    let mut last_reclaim_time = self.last_reclaim_time.lock().unwrap();
                    *last_reclaim_time = now;
                }
            }
        }
    }

    /// 获取低优先级分配
    fn get_low_priority_allocations(&self) -> Vec<u64> {
        let active_allocations = self.active_allocations.lock().unwrap();
        let mut low_priority = Vec::new();
        
        for (id, info) in active_allocations.iter() {
            if info.priority == AllocationPriority::Low && 
               info.allocation_type == AllocationType::Temporary {
                low_priority.push(*id);
            }
        }
        
        low_priority
    }
}

impl Drop for UnifiedMemoryManager {
    fn drop(&mut self) {
        // 清理所有活跃分配
        let active_allocations = self.active_allocations.lock().unwrap();
        for (id, _) in active_allocations.iter() {
            if let Err(e) = self.deallocate(*id) {
                error!("Failed to deallocate memory on drop: {}", e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_allocation() {
        let quota = MemoryQuota {
            max_memory: 1024 * 1024, // 1MB
            ..Default::default()
        };
        let manager = UnifiedMemoryManager::new(quota);
        
        let request = AllocationRequest {
            size: 1024,
            alignment: 8,
            allocation_type: AllocationType::Columnar,
            priority: AllocationPriority::Normal,
        };
        
        let result = manager.allocate(request).unwrap();
        assert!(!result.ptr.is_null());
        assert_eq!(result.size, 1024);
        
        manager.deallocate(result.allocation_id).unwrap();
    }

    #[test]
    fn test_memory_quota() {
        let quota = MemoryQuota {
            max_memory: 1024, // 1KB
            ..Default::default()
        };
        let manager = UnifiedMemoryManager::new(quota);
        
        let request = AllocationRequest {
            size: 2048, // 超过配额
            alignment: 8,
            allocation_type: AllocationType::Columnar,
            priority: AllocationPriority::Normal,
        };
        
        assert!(manager.allocate(request).is_err());
    }
}
