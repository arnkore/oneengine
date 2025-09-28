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

//! 统一湖仓读取器示例
//! 
//! 演示如何使用统一湖仓读取器支持多种湖仓格式（Iceberg、Paimon、Hudi等）

use oneengine::datalake::unified_lake_reader::*;
use std::collections::HashMap;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt::init();
    
    println!("🚀 统一湖仓读取器示例");
    println!("================================================");
    
    // 示例1：Iceberg表读取
    println!("🔄 示例1：Iceberg表读取");
    println!("------------------------");
    test_iceberg_reading()?;
    println!();
    
    // 示例2：Paimon表读取
    println!("🔄 示例2：Paimon表读取");
    println!("------------------------");
    test_paimon_reading()?;
    println!();
    
    // 示例3：Hudi表读取
    println!("🔄 示例3：Hudi表读取");
    println!("------------------------");
    test_hudi_reading()?;
    println!();
    
    // 示例4：Parquet文件读取
    println!("🔄 示例4：Parquet文件读取");
    println!("------------------------");
    test_parquet_reading()?;
    println!();
    
    // 示例5：ORC文件读取
    println!("🔄 示例5：ORC文件读取");
    println!("------------------------");
    test_orc_reading()?;
    println!();
    
    println!("🎯 统一湖仓读取器示例完成！");
    println!("✅ 支持多种湖仓格式：Iceberg、Paimon、Hudi、Parquet、ORC");
    println!("✅ 提供统一的API接口和配置");
    println!("✅ 支持谓词下推、列投影、分区剪枝等优化");
    println!("✅ 支持时间旅行和增量读取（适用于支持的格式）");
    println!("✅ 为未来扩展更多湖仓格式提供了良好的架构基础");
    
    Ok(())
}

/// 测试Iceberg表读取
fn test_iceberg_reading() -> Result<(), Box<dyn std::error::Error>> {
    let config = UnifiedLakeReaderConfig {
        format: LakeFormat::Iceberg,
        table_path: "s3://warehouse/sample_iceberg_table".to_string(),
        warehouse_path: Some("s3://warehouse".to_string()),
        predicates: vec![
            UnifiedPredicate::Equal { 
                column: "year".to_string(), 
                value: "2023".to_string() 
            },
            UnifiedPredicate::GreaterThan { 
                column: "amount".to_string(), 
                value: "1000".to_string() 
            },
        ],
        column_projection: Some(ColumnProjection {
            columns: vec!["id".to_string(), "name".to_string(), "amount".to_string()],
            select_all: false,
        }),
        time_travel: Some(TimeTravelConfig {
            snapshot_id: Some(12345),
            timestamp_ms: None,
            branch: None,
            tag: None,
        }),
        partition_pruning: Some(PartitionPruningInfo {
            partition_columns: vec!["year".to_string(), "month".to_string()],
            partition_values: HashMap::from([
                ("year".to_string(), "2023".to_string()),
                ("month".to_string(), "12".to_string()),
            ]),
            matches: true,
        }),
        enable_predicate_pushdown: true,
        enable_column_projection: true,
        enable_partition_pruning: true,
        enable_time_travel: true,
        enable_incremental_read: false,
        batch_size: 8192,
        max_concurrent_reads: 4,
        incremental_read: None,
    };
    
    let mut reader = UnifiedLakeReader::new(config);
    
    // 打开表
    reader.open()?;
    println!("✅ Iceberg表打开成功");
    
    // 获取元数据
    let metadata = reader.get_metadata()?;
    println!("  📈 表ID: {}", metadata.table_id);
    println!("  📈 表名: {}", metadata.table_name);
    println!("  📈 命名空间: {}", metadata.namespace);
    println!("  📈 当前快照ID: {:?}", metadata.current_snapshot_id);
    
    // 获取统计信息
    let stats = reader.get_statistics()?;
    println!("  📈 总文件数: {}", stats.total_files);
    println!("  📈 总记录数: {}", stats.total_records);
    println!("  📈 总文件大小: {} bytes", stats.total_size_bytes);
    
    // 应用优化
    let filtered_files = reader.apply_predicate_pushdown()?;
    println!("  📈 谓词下推过滤文件数: {}", filtered_files);
    
    reader.apply_column_projection()?;
    println!("  📈 列投影应用成功");
    
    let pruned_files = reader.apply_partition_pruning()?;
    println!("  📈 分区剪枝过滤文件数: {}", pruned_files);
    
    reader.apply_time_travel()?;
    println!("  📈 时间旅行应用成功");
    
    // 读取数据
    let batches = reader.read_data()?;
    println!("  📈 读取到 {} 个批次", batches.len());
    
    Ok(())
}

/// 测试Paimon表读取
fn test_paimon_reading() -> Result<(), Box<dyn std::error::Error>> {
    let config = UnifiedLakeReaderConfig {
        format: LakeFormat::Paimon,
        table_path: "s3://warehouse/sample_paimon_table".to_string(),
        warehouse_path: Some("s3://warehouse".to_string()),
        predicates: vec![
            UnifiedPredicate::Equal { 
                column: "status".to_string(), 
                value: "active".to_string() 
            },
        ],
        column_projection: Some(ColumnProjection {
            columns: vec!["id".to_string(), "name".to_string(), "value".to_string()],
            select_all: false,
        }),
        enable_predicate_pushdown: true,
        enable_column_projection: true,
        enable_partition_pruning: true,
        enable_time_travel: false,
        enable_incremental_read: true,
        batch_size: 4096,
        max_concurrent_reads: 2,
        time_travel: None,
        partition_pruning: None,
        incremental_read: Some(IncrementalReadConfig {
            start_snapshot_id: Some(100),
            end_snapshot_id: Some(200),
            start_timestamp_ms: None,
            end_timestamp_ms: None,
        }),
    };
    
    let mut reader = UnifiedLakeReader::new(config);
    
    // 打开表
    reader.open()?;
    println!("✅ Paimon表打开成功");
    
    // 获取元数据
    let metadata = reader.get_metadata()?;
    println!("  📈 表ID: {}", metadata.table_id);
    println!("  📈 表名: {}", metadata.table_name);
    
    // 应用增量读取
    reader.apply_incremental_read()?;
    println!("  📈 增量读取应用成功");
    
    // 读取数据
    let batches = reader.read_data()?;
    println!("  📈 读取到 {} 个批次", batches.len());
    
    Ok(())
}

/// 测试Hudi表读取
fn test_hudi_reading() -> Result<(), Box<dyn std::error::Error>> {
    let config = UnifiedLakeReaderConfig {
        format: LakeFormat::Hudi,
        table_path: "s3://warehouse/sample_hudi_table".to_string(),
        warehouse_path: Some("s3://warehouse".to_string()),
        predicates: vec![
            UnifiedPredicate::GreaterThan { 
                column: "timestamp".to_string(), 
                value: "2023-01-01".to_string() 
            },
        ],
        column_projection: Some(ColumnProjection {
            columns: vec!["id".to_string(), "name".to_string(), "timestamp".to_string()],
            select_all: false,
        }),
        enable_predicate_pushdown: true,
        enable_column_projection: true,
        enable_partition_pruning: true,
        enable_time_travel: false,
        enable_incremental_read: true,
        batch_size: 1024,
        max_concurrent_reads: 1,
        time_travel: None,
        partition_pruning: None,
        incremental_read: Some(IncrementalReadConfig {
            start_timestamp_ms: Some(1672531200000), // 2023-01-01
            end_timestamp_ms: Some(1704067200000),   // 2024-01-01
            start_snapshot_id: None,
            end_snapshot_id: None,
        }),
    };
    
    let mut reader = UnifiedLakeReader::new(config);
    
    // 打开表
    reader.open()?;
    println!("✅ Hudi表打开成功");
    
    // 获取元数据
    let metadata = reader.get_metadata()?;
    println!("  📈 表ID: {}", metadata.table_id);
    println!("  📈 表名: {}", metadata.table_name);
    
    // 应用增量读取
    reader.apply_incremental_read()?;
    println!("  📈 增量读取应用成功");
    
    // 读取数据
    let batches = reader.read_data()?;
    println!("  📈 读取到 {} 个批次", batches.len());
    
    Ok(())
}

/// 测试Parquet文件读取
fn test_parquet_reading() -> Result<(), Box<dyn std::error::Error>> {
    let config = UnifiedLakeReaderConfig {
        format: LakeFormat::Parquet,
        table_path: "data/sample.parquet".to_string(),
        warehouse_path: None,
        predicates: vec![
            UnifiedPredicate::Equal { 
                column: "category".to_string(), 
                value: "electronics".to_string() 
            },
        ],
        column_projection: Some(ColumnProjection {
            columns: vec!["id".to_string(), "name".to_string(), "price".to_string()],
            select_all: false,
        }),
        enable_predicate_pushdown: true,
        enable_column_projection: true,
        enable_partition_pruning: false,
        enable_time_travel: false,
        enable_incremental_read: false,
        batch_size: 2048,
        max_concurrent_reads: 1,
        time_travel: None,
        partition_pruning: None,
        incremental_read: None,
    };
    
    let mut reader = UnifiedLakeReader::new(config);
    
    // 打开文件
    reader.open()?;
    println!("✅ Parquet文件打开成功");
    
    // 获取元数据
    let metadata = reader.get_metadata()?;
    println!("  📈 表ID: {}", metadata.table_id);
    println!("  📈 表名: {}", metadata.table_name);
    
    // 应用优化
    let filtered_files = reader.apply_predicate_pushdown()?;
    println!("  📈 谓词下推过滤文件数: {}", filtered_files);
    
    reader.apply_column_projection()?;
    println!("  📈 列投影应用成功");
    
    // 读取数据
    let batches = reader.read_data()?;
    println!("  📈 读取到 {} 个批次", batches.len());
    
    Ok(())
}

/// 测试ORC文件读取
fn test_orc_reading() -> Result<(), Box<dyn std::error::Error>> {
    let config = UnifiedLakeReaderConfig {
        format: LakeFormat::Orc,
        table_path: "data/sample.orc".to_string(),
        warehouse_path: None,
        predicates: vec![
            UnifiedPredicate::Range { 
                column: "age".to_string(), 
                min: "18".to_string(), 
                max: "65".to_string() 
            },
        ],
        column_projection: Some(ColumnProjection {
            columns: vec!["id".to_string(), "name".to_string(), "age".to_string()],
            select_all: false,
        }),
        enable_predicate_pushdown: true,
        enable_column_projection: true,
        enable_partition_pruning: false,
        enable_time_travel: false,
        enable_incremental_read: false,
        batch_size: 1024,
        max_concurrent_reads: 1,
        time_travel: None,
        partition_pruning: None,
        incremental_read: None,
    };
    
    let mut reader = UnifiedLakeReader::new(config);
    
    // 打开文件
    reader.open()?;
    println!("✅ ORC文件打开成功");
    
    // 获取元数据
    let metadata = reader.get_metadata()?;
    println!("  📈 表ID: {}", metadata.table_id);
    println!("  📈 表名: {}", metadata.table_name);
    
    // 应用优化
    let filtered_files = reader.apply_predicate_pushdown()?;
    println!("  📈 谓词下推过滤文件数: {}", filtered_files);
    
    reader.apply_column_projection()?;
    println!("  📈 列投影应用成功");
    
    // 读取数据
    let batches = reader.read_data()?;
    println!("  📈 读取到 {} 个批次", batches.len());
    
    Ok(())
}
