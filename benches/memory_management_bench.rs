//! 内存管理基准测试
//! 
//! 测试统一内存管理器、数据溢写和背压控制的性能

use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};
use oneengine::memory::{
    unified_memory_manager::{UnifiedMemoryManager, MemoryQuota, AllocationRequest, AllocationType, AllocationPriority},
    spill_manager::{SpillManager, SpillConfig, CompressionType},
    backpressure_controller::{BackpressureController, BackpressureConfig},
};
use oneengine::columnar::batch::{Batch, BatchSchema, Field};
use oneengine::columnar::column::Column;
use oneengine::columnar::types::DataType;
use std::sync::Arc;
use std::time::Duration;

/// 内存分配基准测试
fn bench_memory_allocation(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_allocation");
    
    let quota = MemoryQuota {
        max_memory: 100 * 1024 * 1024, // 100MB
        ..Default::default()
    };
    let memory_manager = Arc::new(UnifiedMemoryManager::new(quota));
    
    let allocation_sizes = vec![64, 256, 1024, 4096, 16384, 65536];
    
    for size in allocation_sizes {
        group.bench_with_input(BenchmarkId::new("allocate", size), &size, |b, &size| {
            b.iter(|| {
                let request = AllocationRequest {
                    size,
                    alignment: 8,
                    allocation_type: AllocationType::Columnar,
                    priority: AllocationPriority::Normal,
                };
                
                let result = memory_manager.allocate(request).unwrap();
                memory_manager.deallocate(result.allocation_id).unwrap();
            });
        });
    }
    
    group.finish();
}

/// 内存分配类型基准测试
fn bench_allocation_types(c: &mut Criterion) {
    let mut group = c.benchmark_group("allocation_types");
    
    let quota = MemoryQuota {
        max_memory: 100 * 1024 * 1024, // 100MB
        ..Default::default()
    };
    let memory_manager = Arc::new(UnifiedMemoryManager::new(quota));
    
    let allocation_types = vec![
        (AllocationType::Columnar, "columnar"),
        (AllocationType::HashTable, "hashtable"),
        (AllocationType::SortBuffer, "sortbuffer"),
        (AllocationType::NetworkBuffer, "network"),
        (AllocationType::Temporary, "temporary"),
    ];
    
    for (allocation_type, name) in allocation_types {
        group.bench_with_input(BenchmarkId::new("type", name), &allocation_type, |b, allocation_type| {
            b.iter(|| {
                let request = AllocationRequest {
                    size: 1024,
                    alignment: 8,
                    allocation_type: allocation_type.clone(),
                    priority: AllocationPriority::Normal,
                };
                
                let result = memory_manager.allocate(request).unwrap();
                memory_manager.deallocate(result.allocation_id).unwrap();
            });
        });
    }
    
    group.finish();
}

/// 内存分配优先级基准测试
fn bench_allocation_priorities(c: &mut Criterion) {
    let mut group = c.benchmark_group("allocation_priorities");
    
    let quota = MemoryQuota {
        max_memory: 100 * 1024 * 1024, // 100MB
        ..Default::default()
    };
    let memory_manager = Arc::new(UnifiedMemoryManager::new(quota));
    
    let priorities = vec![
        (AllocationPriority::Low, "low"),
        (AllocationPriority::Normal, "normal"),
        (AllocationPriority::High, "high"),
        (AllocationPriority::Critical, "critical"),
    ];
    
    for (priority, name) in priorities {
        group.bench_with_input(BenchmarkId::new("priority", name), &priority, |b, priority| {
            b.iter(|| {
                let request = AllocationRequest {
                    size: 1024,
                    alignment: 8,
                    allocation_type: AllocationType::Columnar,
                    priority: priority.clone(),
                };
                
                let result = memory_manager.allocate(request).unwrap();
                memory_manager.deallocate(result.allocation_id).unwrap();
            });
        });
    }
    
    group.finish();
}

/// 数据溢写基准测试
fn bench_spill_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("spill_operations");
    
    let spill_config = SpillConfig {
        spill_dir: std::env::temp_dir().join("oneengine_spill_bench"),
        max_memory_bytes: 10 * 1024 * 1024, // 10MB
        compression: CompressionType::LZ4,
        batch_size: 1000,
        enable_compression: true,
    };
    let spill_manager = Arc::new(SpillManager::new(spill_config).unwrap());
    
    // 创建测试批次
    let mut schema = BatchSchema::new();
    schema.fields.push(Field {
        name: "id".to_string(),
        data_type: DataType::Int32,
        nullable: false,
    });
    schema.fields.push(Field {
        name: "value".to_string(),
        data_type: DataType::Float64,
        nullable: true,
    });
    
    let batch_sizes = vec![100, 1000, 10000, 100000];
    
    for batch_size in batch_sizes {
        group.bench_with_input(BenchmarkId::new("spill_single", batch_size), &batch_size, |b, &batch_size| {
            let mut columns = Vec::new();
            let id_column = Column::new(DataType::Int32, batch_size);
            let value_column = Column::new(DataType::Float64, batch_size);
            columns.push(id_column);
            columns.push(value_column);
            let batch = Batch::from_columns(columns, schema.clone()).unwrap();
            
            b.iter(|| {
                let spill_file = spill_manager.spill_batch(&batch, "bench_spill").unwrap();
                let _recovered = spill_manager.read_spill_file(&spill_file).unwrap();
                spill_manager.delete_spill_file(&spill_file).unwrap();
            });
        });
    }
    
    group.finish();
}

/// 压缩算法基准测试
fn bench_compression_algorithms(c: &mut Criterion) {
    let mut group = c.benchmark_group("compression_algorithms");
    
    let compression_types = vec![
        (CompressionType::None, "none"),
        (CompressionType::LZ4, "lz4"),
        (CompressionType::ZSTD, "zstd"),
        (CompressionType::GZIP, "gzip"),
    ];
    
    for (compression_type, name) in compression_types {
        let spill_config = SpillConfig {
            spill_dir: std::env::temp_dir().join("oneengine_spill_bench"),
            max_memory_bytes: 10 * 1024 * 1024, // 10MB
            compression: compression_type.clone(),
            batch_size: 1000,
            enable_compression: true,
        };
        let spill_manager = Arc::new(SpillManager::new(spill_config).unwrap());
        
        group.bench_with_input(BenchmarkId::new("compress", name), &compression_type, |b, _compression_type| {
            let mut schema = BatchSchema::new();
            schema.fields.push(Field {
                name: "data".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            });
            
            let mut columns = Vec::new();
            let data_column = Column::new(DataType::Int32, 10000);
            columns.push(data_column);
            let batch = Batch::from_columns(columns, schema).unwrap();
            
            b.iter(|| {
                let spill_file = spill_manager.spill_batch(&batch, "bench_compress").unwrap();
                let _recovered = spill_manager.read_spill_file(&spill_file).unwrap();
                spill_manager.delete_spill_file(&spill_file).unwrap();
            });
        });
    }
    
    group.finish();
}

/// 背压控制基准测试
fn bench_backpressure_control(c: &mut Criterion) {
    let mut group = c.benchmark_group("backpressure_control");
    
    let backpressure_config = BackpressureConfig {
        high_watermark: 0.8,
        low_watermark: 0.6,
        max_queue_size: 1000,
        check_interval: Duration::from_millis(1),
        max_wait_time: Duration::from_secs(1),
    };
    let backpressure_controller = Arc::new(BackpressureController::new(backpressure_config));
    
    let queue_sizes = vec![10, 100, 500, 800, 900, 950];
    
    for queue_size in queue_sizes {
        group.bench_with_input(BenchmarkId::new("update_queue", queue_size), &queue_size, |b, &queue_size| {
            b.iter(|| {
                backpressure_controller.update_queue_size(queue_size).unwrap();
                let _watermark = backpressure_controller.get_queue_watermark();
                let _state = backpressure_controller.get_state();
                let _can_process = backpressure_controller.can_process();
            });
        });
    }
    
    group.finish();
}

/// 内存统计基准测试
fn bench_memory_stats(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_stats");
    
    let quota = MemoryQuota {
        max_memory: 100 * 1024 * 1024, // 100MB
        ..Default::default()
    };
    let memory_manager = Arc::new(UnifiedMemoryManager::new(quota));
    
    // 预先分配一些内存
    let mut allocations = Vec::new();
    for i in 0..100 {
        let request = AllocationRequest {
            size: 1024 * (i + 1),
            alignment: 8,
            allocation_type: AllocationType::Columnar,
            priority: AllocationPriority::Normal,
        };
        let result = memory_manager.allocate(request).unwrap();
        allocations.push(result);
    }
    
    group.bench_function("get_stats", |b| {
        b.iter(|| {
            let _stats = memory_manager.get_stats();
        });
    });
    
    // 清理
    for allocation in allocations {
        memory_manager.deallocate(allocation.allocation_id).unwrap();
    }
    
    group.finish();
}

/// 内存回收基准测试
fn bench_memory_reclaim(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_reclaim");
    
    let quota = MemoryQuota {
        max_memory: 10 * 1024 * 1024, // 10MB
        ..Default::default()
    };
    let memory_manager = Arc::new(UnifiedMemoryManager::new(quota));
    
    let reclaim_counts = vec![10, 50, 100, 500, 1000];
    
    for count in reclaim_counts {
        group.bench_with_input(BenchmarkId::new("force_reclaim", count), &count, |b, &count| {
            // 预先分配内存
            let mut allocations = Vec::new();
            for i in 0..count {
                let request = AllocationRequest {
                    size: 1024,
                    alignment: 8,
                    allocation_type: AllocationType::Temporary,
                    priority: AllocationPriority::Low,
                };
                let result = memory_manager.allocate(request).unwrap();
                allocations.push(result);
            }
            
            b.iter(|| {
                let _reclaimed = memory_manager.force_reclaim().unwrap();
            });
            
            // 清理
            for allocation in allocations {
                let _ = memory_manager.deallocate(allocation.allocation_id);
            }
        });
    }
    
    group.finish();
}

criterion_group!(
    memory_benches,
    bench_memory_allocation,
    bench_allocation_types,
    bench_allocation_priorities,
    bench_spill_operations,
    bench_compression_algorithms,
    bench_backpressure_control,
    bench_memory_stats,
    bench_memory_reclaim
);

criterion_main!(memory_benches);
