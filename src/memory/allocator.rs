//! 内存分配器优化
//! 
//! jemalloc/mimalloc + bumpalo/arena，64B对齐

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::ptr::{NonNull, null_mut};
use std::mem;

/// 内存分配器类型
pub enum AllocatorType {
    /// 系统默认分配器
    System,
    /// jemalloc
    Jemalloc,
    /// mimalloc
    Mimalloc,
    /// 混合分配器
    Hybrid,
}

/// 高性能内存分配器
pub struct HighPerformanceAllocator {
    /// 分配器类型
    allocator_type: AllocatorType,
    /// 内存统计
    stats: Arc<AllocationStats>,
    /// Arena分配器
    arena_allocator: Option<ArenaAllocator>,
    /// 对齐要求
    alignment: usize,
}

/// 内存分配统计
#[derive(Debug, Clone)]
pub struct AllocationStats {
    /// 总分配次数
    total_allocations: AtomicUsize,
    /// 总释放次数
    total_deallocations: AtomicUsize,
    /// 当前分配的内存大小
    current_allocated: AtomicUsize,
    /// 峰值分配的内存大小
    peak_allocated: AtomicUsize,
    /// 分配失败次数
    allocation_failures: AtomicUsize,
    /// 对齐分配次数
    aligned_allocations: AtomicUsize,
}

/// Arena分配器
pub struct ArenaAllocator {
    /// 内存池
    memory_pool: Vec<u8>,
    /// 当前偏移
    current_offset: AtomicUsize,
    /// 池大小
    pool_size: usize,
    /// 对齐要求
    alignment: usize,
}

/// 内存池管理器
pub struct MemoryPoolManager {
    /// 小对象池（< 1KB）
    small_object_pools: Vec<SmallObjectPool>,
    /// 中等对象池（1KB - 1MB）
    medium_object_pools: Vec<MediumObjectPool>,
    /// 大对象池（> 1MB）
    large_object_pools: Vec<LargeObjectPool>,
    /// 统计信息
    stats: Arc<AllocationStats>,
}

/// 小对象池
pub struct SmallObjectPool {
    /// 对象大小
    object_size: usize,
    /// 池大小
    pool_size: usize,
    /// 内存块
    memory_blocks: Vec<MemoryBlock>,
    /// 空闲对象列表
    free_objects: Vec<*mut u8>,
    /// 对齐要求
    alignment: usize,
}

/// 中等对象池
pub struct MediumObjectPool {
    /// 对象大小范围
    size_range: (usize, usize),
    /// 内存块
    memory_blocks: Vec<MemoryBlock>,
    /// 空闲对象列表
    free_objects: Vec<*mut u8>,
    /// 对齐要求
    alignment: usize,
}

/// 大对象池
pub struct LargeObjectPool {
    /// 内存块
    memory_blocks: Vec<MemoryBlock>,
    /// 对齐要求
    alignment: usize,
}

/// 内存块
pub struct MemoryBlock {
    /// 起始地址
    start: *mut u8,
    /// 大小
    size: usize,
    /// 是否已分配
    allocated: bool,
    /// 对齐偏移
    alignment_offset: usize,
}

/// 对齐内存分配器
pub struct AlignedAllocator {
    /// 基础分配器
    base_allocator: HighPerformanceAllocator,
    /// 对齐要求
    alignment: usize,
    /// 统计信息
    stats: Arc<AllocationStats>,
}

/// 内存分配器配置
#[derive(Debug, Clone)]
pub struct AllocatorConfig {
    /// 分配器类型
    pub allocator_type: AllocatorType,
    /// 对齐要求
    pub alignment: usize,
    /// Arena大小
    pub arena_size: usize,
    /// 小对象池大小
    pub small_pool_size: usize,
    /// 中等对象池大小
    pub medium_pool_size: usize,
    /// 大对象池大小
    pub large_pool_size: usize,
    /// 是否启用统计
    pub enable_stats: bool,
}

impl HighPerformanceAllocator {
    /// 创建新的高性能内存分配器
    pub fn new(config: AllocatorConfig) -> Self {
        let stats = Arc::new(AllocationStats::new());
        let arena_allocator = if config.arena_size > 0 {
            Some(ArenaAllocator::new(config.arena_size, config.alignment))
        } else {
            None
        };

        Self {
            allocator_type: config.allocator_type,
            stats,
            arena_allocator,
            alignment: config.alignment,
        }
    }

    /// 分配内存
    pub unsafe fn allocate(&self, layout: Layout) -> *mut u8 {
        // 检查对齐要求
        let aligned_layout = if layout.align() > self.alignment {
            Layout::from_size_align(layout.size(), layout.align()).unwrap()
        } else {
            layout
        };

        // 尝试使用Arena分配器
        if let Some(ref arena) = self.arena_allocator {
            if let Some(ptr) = arena.allocate(aligned_layout) {
                self.stats.record_allocation(aligned_layout.size(), true);
                return ptr;
            }
        }

        // 回退到系统分配器
        let ptr = match self.allocator_type {
            AllocatorType::System => System.alloc(aligned_layout),
            AllocatorType::Jemalloc => {
                // 这里需要链接jemalloc库
                // 简化实现，使用系统分配器
                System.alloc(aligned_layout)
            },
            AllocatorType::Mimalloc => {
                // 这里需要链接mimalloc库
                // 简化实现，使用系统分配器
                System.alloc(aligned_layout)
            },
            AllocatorType::Hybrid => {
                // 根据大小选择分配器
                if aligned_layout.size() < 1024 {
                    // 小对象使用Arena
                    if let Some(ref arena) = self.arena_allocator {
                        arena.allocate(aligned_layout).unwrap_or_else(|| System.alloc(aligned_layout))
                    } else {
                        System.alloc(aligned_layout)
                    }
                } else {
                    // 大对象使用系统分配器
                    System.alloc(aligned_layout)
                }
            },
        };

        if ptr.is_null() {
            self.stats.record_allocation_failure();
            null_mut()
        } else {
            self.stats.record_allocation(aligned_layout.size(), aligned_layout.align() > 1);
        }

        ptr
    }

    /// 释放内存
    pub unsafe fn deallocate(&self, ptr: *mut u8, layout: Layout) {
        // 检查是否在Arena中分配
        if let Some(ref arena) = self.arena_allocator {
            if arena.contains(ptr) {
                arena.deallocate(ptr, layout);
                self.stats.record_deallocation(layout.size());
                return;
            }
        }

        // 使用系统分配器释放
        System.dealloc(ptr, layout);
        self.stats.record_deallocation(layout.size());
    }

    /// 重新分配内存
    pub unsafe fn reallocate(&self, ptr: *mut u8, old_layout: Layout, new_size: usize) -> *mut u8 {
        let new_layout = Layout::from_size_align(new_size, old_layout.align()).unwrap();
        
        // 检查是否在Arena中分配
        if let Some(ref arena) = self.arena_allocator {
            if arena.contains(ptr) {
                if let Some(new_ptr) = arena.reallocate(ptr, old_layout, new_layout) {
                    return new_ptr;
                }
                // 如果Arena重新分配失败，回退到系统分配器
            }
        }

        // 使用系统分配器重新分配
        let new_ptr = System.realloc(ptr, old_layout, new_size);
        if new_ptr.is_null() {
            self.stats.record_allocation_failure();
        }
        new_ptr
    }

    /// 获取统计信息
    pub fn get_stats(&self) -> AllocationStats {
        self.stats.clone()
    }
}

impl ArenaAllocator {
    /// 创建新的Arena分配器
    pub fn new(pool_size: usize, alignment: usize) -> Self {
        let mut memory_pool = Vec::with_capacity(pool_size);
        memory_pool.resize(pool_size, 0);
        
        Self {
            memory_pool,
            current_offset: AtomicUsize::new(0),
            pool_size,
            alignment,
        }
    }

    /// 分配内存
    pub fn allocate(&self, layout: Layout) -> Option<*mut u8> {
        let size = layout.size();
        let align = layout.align();
        
        // 计算对齐偏移
        let current_offset = self.current_offset.load(Ordering::Relaxed);
        let aligned_offset = (current_offset + align - 1) & !(align - 1);
        
        if aligned_offset + size > self.pool_size {
            return None;
        }

        // 原子更新偏移
        if self.current_offset.compare_exchange_weak(
            current_offset,
            aligned_offset + size,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ).is_err() {
            return None;
        }

        let ptr = self.memory_pool.as_ptr().add(aligned_offset);
        Some(ptr as *mut u8)
    }

    /// 释放内存
    pub fn deallocate(&self, _ptr: *mut u8, _layout: Layout) {
        // Arena分配器不支持单独释放，只能整体重置
    }

    /// 重新分配内存
    pub fn reallocate(&self, ptr: *mut u8, old_layout: Layout, new_layout: Layout) -> Option<*mut u8> {
        // 如果新大小小于等于旧大小，直接返回原指针
        if new_layout.size() <= old_layout.size() {
            return Some(ptr);
        }

        // 否则需要重新分配
        self.allocate(new_layout)
    }

    /// 检查指针是否在Arena中
    pub fn contains(&self, ptr: *mut u8) -> bool {
        let pool_start = self.memory_pool.as_ptr() as *mut u8;
        let pool_end = unsafe { pool_start.add(self.pool_size) };
        ptr >= pool_start && ptr < pool_end
    }

    /// 重置Arena
    pub fn reset(&self) {
        self.current_offset.store(0, Ordering::Relaxed);
    }
}

impl MemoryPoolManager {
    /// 创建新的内存池管理器
    pub fn new(config: AllocatorConfig) -> Self {
        let stats = Arc::new(AllocationStats::new());
        
        // 创建小对象池
        let mut small_object_pools = Vec::new();
        for size in [16, 32, 64, 128, 256, 512, 1024] {
            small_object_pools.push(SmallObjectPool::new(size, config.small_pool_size, config.alignment));
        }

        // 创建中等对象池
        let mut medium_object_pools = Vec::new();
        for size_range in [(1024, 4096), (4096, 16384), (16384, 65536), (65536, 262144), (262144, 1048576)] {
            medium_object_pools.push(MediumObjectPool::new(size_range, config.medium_pool_size, config.alignment));
        }

        // 创建大对象池
        let large_object_pools = vec![LargeObjectPool::new(config.large_pool_size, config.alignment)];

        Self {
            small_object_pools,
            medium_object_pools,
            large_object_pools,
            stats,
        }
    }

    /// 分配内存
    pub fn allocate(&self, layout: Layout) -> Option<*mut u8> {
        let size = layout.size();
        
        if size <= 1024 {
            // 使用小对象池
            for pool in &self.small_object_pools {
                if pool.object_size >= size {
                    if let Some(ptr) = pool.allocate() {
                        return Some(ptr);
                    }
                }
            }
        } else if size <= 1024 * 1024 {
            // 使用中等对象池
            for pool in &self.medium_object_pools {
                if size >= pool.size_range.0 && size <= pool.size_range.1 {
                    if let Some(ptr) = pool.allocate(size) {
                        return Some(ptr);
                    }
                }
            }
        } else {
            // 使用大对象池
            for pool in &self.large_object_pools {
                if let Some(ptr) = pool.allocate(size) {
                    return Some(ptr);
                }
            }
        }

        None
    }

    /// 释放内存
    pub fn deallocate(&self, ptr: *mut u8, layout: Layout) {
        let size = layout.size();
        
        if size <= 1024 {
            // 释放到小对象池
            for pool in &self.small_object_pools {
                if pool.object_size >= size {
                    pool.deallocate(ptr);
                    return;
                }
            }
        } else if size <= 1024 * 1024 {
            // 释放到中等对象池
            for pool in &self.medium_object_pools {
                if size >= pool.size_range.0 && size <= pool.size_range.1 {
                    pool.deallocate(ptr, size);
                    return;
                }
            }
        } else {
            // 释放到大对象池
            for pool in &self.large_object_pools {
                pool.deallocate(ptr, size);
                return;
            }
        }
    }
}

impl SmallObjectPool {
    /// 创建新的小对象池
    pub fn new(object_size: usize, pool_size: usize, alignment: usize) -> Self {
        let mut memory_blocks = Vec::new();
        let mut free_objects = Vec::new();
        
        // 创建内存块
        let block_count = pool_size / object_size;
        for _ in 0..block_count {
            let mut block = vec![0u8; object_size];
            let ptr = block.as_mut_ptr();
            memory_blocks.push(MemoryBlock {
                start: ptr,
                size: object_size,
                allocated: false,
                alignment_offset: 0,
            });
            free_objects.push(ptr);
        }

        Self {
            object_size,
            pool_size,
            memory_blocks,
            free_objects,
            alignment,
        }
    }

    /// 分配对象
    pub fn allocate(&self) -> Option<*mut u8> {
        // 简化实现，实际需要线程安全
        self.free_objects.last().copied()
    }

    /// 释放对象
    pub fn deallocate(&self, _ptr: *mut u8) {
        // 简化实现，实际需要线程安全
    }
}

impl MediumObjectPool {
    /// 创建新的中等对象池
    pub fn new(size_range: (usize, usize), pool_size: usize, alignment: usize) -> Self {
        let mut memory_blocks = Vec::new();
        let mut free_objects = Vec::new();
        
        // 创建内存块
        let block_count = pool_size / size_range.1;
        for _ in 0..block_count {
            let mut block = vec![0u8; size_range.1];
            let ptr = block.as_mut_ptr();
            memory_blocks.push(MemoryBlock {
                start: ptr,
                size: size_range.1,
                allocated: false,
                alignment_offset: 0,
            });
            free_objects.push(ptr);
        }

        Self {
            size_range,
            memory_blocks,
            free_objects,
            alignment,
        }
    }

    /// 分配对象
    pub fn allocate(&self, _size: usize) -> Option<*mut u8> {
        // 简化实现，实际需要线程安全
        self.free_objects.last().copied()
    }

    /// 释放对象
    pub fn deallocate(&self, _ptr: *mut u8, _size: usize) {
        // 简化实现，实际需要线程安全
    }
}

impl LargeObjectPool {
    /// 创建新的大对象池
    pub fn new(pool_size: usize, alignment: usize) -> Self {
        let mut memory_blocks = Vec::new();
        
        // 创建内存块
        let mut block = vec![0u8; pool_size];
        let ptr = block.as_mut_ptr();
        memory_blocks.push(MemoryBlock {
            start: ptr,
            size: pool_size,
            allocated: false,
            alignment_offset: 0,
        });

        Self {
            memory_blocks,
            alignment,
        }
    }

    /// 分配对象
    pub fn allocate(&self, _size: usize) -> Option<*mut u8> {
        // 简化实现，实际需要线程安全
        self.memory_blocks.first().map(|block| block.start)
    }

    /// 释放对象
    pub fn deallocate(&self, _ptr: *mut u8, _size: usize) {
        // 简化实现，实际需要线程安全
    }
}

impl AllocationStats {
    /// 创建新的分配统计
    pub fn new() -> Self {
        Self {
            total_allocations: AtomicUsize::new(0),
            total_deallocations: AtomicUsize::new(0),
            current_allocated: AtomicUsize::new(0),
            peak_allocated: AtomicUsize::new(0),
            allocation_failures: AtomicUsize::new(0),
            aligned_allocations: AtomicUsize::new(0),
        }
    }

    /// 记录分配
    pub fn record_allocation(&self, size: usize, is_aligned: bool) {
        self.total_allocations.fetch_add(1, Ordering::Relaxed);
        self.current_allocated.fetch_add(size, Ordering::Relaxed);
        
        if is_aligned {
            self.aligned_allocations.fetch_add(1, Ordering::Relaxed);
        }

        // 更新峰值
        let current = self.current_allocated.load(Ordering::Relaxed);
        let mut peak = self.peak_allocated.load(Ordering::Relaxed);
        while current > peak {
            match self.peak_allocated.compare_exchange_weak(
                peak,
                current,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(x) => peak = x,
            }
        }
    }

    /// 记录释放
    pub fn record_deallocation(&self, size: usize) {
        self.total_deallocations.fetch_add(1, Ordering::Relaxed);
        self.current_allocated.fetch_sub(size, Ordering::Relaxed);
    }

    /// 记录分配失败
    pub fn record_allocation_failure(&self) {
        self.allocation_failures.fetch_add(1, Ordering::Relaxed);
    }
}

impl Default for AllocatorConfig {
    fn default() -> Self {
        Self {
            allocator_type: AllocatorType::Hybrid,
            alignment: 64, // 64字节对齐
            arena_size: 1024 * 1024, // 1MB Arena
            small_pool_size: 1024 * 1024, // 1MB小对象池
            medium_pool_size: 10 * 1024 * 1024, // 10MB中等对象池
            large_pool_size: 100 * 1024 * 1024, // 100MB大对象池
            enable_stats: true,
        }
    }
}

impl Default for AllocationStats {
    fn default() -> Self {
        Self::new()
    }
}