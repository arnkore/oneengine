/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


//! 简化的内存管理示例
//! 
//! 演示统一内存管理器和数据溢写的基本功能

use oneengine::memory::{
    unified_memory_manager::{UnifiedMemoryManager, MemoryQuota, AllocationRequest, AllocationType, AllocationPriority},
    spill_manager::{SpillManager, SpillConfig, CompressionType},
};
use oneengine::columnar::batch::{Batch, BatchSchema, Field};
use oneengine::columnar::column::Column;
use oneengine::columnar::types::DataType;
use std::sync::Arc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== OneEngine Phase 2: 简化内存管理示例 ===\n");

    // 1. 创建统一内存管理器
    println!("1. 创建统一内存管理器");
    let quota = MemoryQuota {
        max_memory: 10 * 1024 * 1024, // 10MB
        soft_limit_ratio: 0.7,
        hard_limit_ratio: 0.9,
        reclaim_ratio: 0.6,
    };
    let memory_manager = Arc::new(UnifiedMemoryManager::new(quota.clone()));
    println!("   最大内存: {} MB", quota.max_memory / 1024 / 1024);
    println!("   软限制: {}%", quota.soft_limit_ratio * 100.0);
    println!("   硬限制: {}%", quota.hard_limit_ratio * 100.0);
    println!();

    // 2. 创建数据溢写管理器
    println!("2. 创建数据溢写管理器");
    let spill_config = SpillConfig {
        spill_dir: std::env::temp_dir().join("oneengine_spill_simple"),
        max_memory_bytes: 5 * 1024 * 1024, // 5MB
        compression: CompressionType::LZ4,
        batch_size: 1000,
        enable_compression: true,
    };
    let spill_manager = Arc::new(SpillManager::new(spill_config.clone())?);
    println!("   溢写目录: {:?}", spill_config.spill_dir);
    println!("   压缩类型: {:?}", spill_config.compression);
    println!();

    // 3. 演示内存分配
    println!("3. 演示内存分配");
    let allocation_requests = vec![
        AllocationRequest {
            size: 1024,
            alignment: 8,
            allocation_type: AllocationType::Columnar,
            priority: AllocationPriority::Normal,
        },
        AllocationRequest {
            size: 2048,
            alignment: 16,
            allocation_type: AllocationType::HashTable,
            priority: AllocationPriority::High,
        },
        AllocationRequest {
            size: 512,
            alignment: 8,
            allocation_type: AllocationType::Temporary,
            priority: AllocationPriority::Low,
        },
    ];

    let mut allocation_results = Vec::new();
    for (i, request) in allocation_requests.iter().enumerate() {
        let result = memory_manager.allocate(request.clone())?;
        let size = result.size;
        let allocation_type = request.allocation_type.clone();
        let priority = request.priority.clone();
        allocation_results.push(result);
        println!("   分配 {}: {} 字节, 类型: {:?}, 优先级: {:?}", 
                i + 1, size, allocation_type, priority);
    }
    println!();

    // 4. 演示内存统计
    println!("4. 内存使用统计");
    let stats = memory_manager.get_stats();
    println!("   当前使用: {} 字节", stats.current_usage);
    println!("   峰值使用: {} 字节", stats.peak_usage);
    println!("   分配次数: {}", stats.allocation_count);
    println!("   释放次数: {}", stats.deallocation_count);
    println!("   内存池使用: {} 字节", stats.pool_usage);
    println!("   直接分配: {} 字节", stats.direct_usage);
    println!();

    // 5. 演示数据溢写
    println!("5. 演示数据溢写");
    
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

    let mut test_batches = Vec::new();
    for i in 0..3 {
        let mut columns = Vec::new();
        
        // ID列
        let _id_data: Vec<i32> = (0..1000).map(|j| (i * 1000 + j) as i32).collect();
        let id_column = Column::new(DataType::Int32, 1000);
        columns.push(id_column);
        
        // Value列
        let value_column = Column::new(DataType::Float64, 1000);
        columns.push(value_column);
        
        let batch = Batch::from_columns(columns, schema.clone())?;
        test_batches.push(batch);
    }

    // 检查是否需要溢写
    let total_memory = test_batches.iter().map(|b| b.len() * 8).sum::<usize>();
    if spill_manager.should_spill(total_memory) {
        println!("   检测到内存压力，开始溢写...");
        
        // 溢写单个批次
        let spill_file = spill_manager.spill_batch(&test_batches[0], "test_spill")?;
        println!("   溢写文件: {:?}", spill_file.path);
        println!("   文件大小: {} 字节", spill_file.size);
        println!("   批次数量: {}", spill_file.batch_count);
        
        // 读取溢写文件
        let recovered_batches = spill_manager.read_spill_file(&spill_file)?;
        println!("   恢复批次数量: {}", recovered_batches.len());
        
        // 清理溢写文件
        spill_manager.delete_spill_file(&spill_file)?;
        println!("   已清理溢写文件");
    } else {
        println!("   内存使用正常，无需溢写");
    }
    println!();

    // 6. 演示内存释放
    println!("6. 演示内存释放");
    for (i, result) in allocation_results.iter().enumerate() {
        memory_manager.deallocate(result.allocation_id)?;
        println!("   释放分配 {}: {} 字节", i + 1, result.size);
    }
    println!();

    // 7. 最终统计
    println!("7. 最终统计");
    let final_stats = memory_manager.get_stats();
    println!("   当前使用: {} 字节", final_stats.current_usage);
    println!("   峰值使用: {} 字节", final_stats.peak_usage);
    println!("   分配次数: {}", final_stats.allocation_count);
    println!("   释放次数: {}", final_stats.deallocation_count);
    
    let spill_stats = spill_manager.get_spill_stats();
    println!("   溢写文件数: {}", spill_stats.total_files);
    println!("   溢写总大小: {} 字节", spill_stats.total_size);
    println!("   溢写批次数: {}", spill_stats.total_batches);
    println!();

    // 8. 清理
    println!("8. 清理资源");
    spill_manager.cleanup_all()?;
    println!("   已清理所有溢写文件");
    println!();

    println!("=== 简化内存管理示例完成 ===");
    Ok(())
}
