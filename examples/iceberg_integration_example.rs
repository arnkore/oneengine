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


use oneengine::io::iceberg_integration::*;
use std::collections::HashMap;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt::init();
    
    println!("🚀 Iceberg湖仓集成演示");
    println!("================================================");
    
    // 创建Iceberg表配置
    let config = IcebergTableConfig {
        table_path: "s3://warehouse/sample_table".to_string(),
        warehouse_path: "s3://warehouse".to_string(),
        enable_snapshot_isolation: true,
        enable_time_travel: true,
        enable_branches_tags: true,
        enable_incremental_read: true,
        enable_predicate_pushdown: true,
        enable_column_projection: true,
        enable_partition_pruning: true,
        enable_statistics: true,
        max_concurrent_reads: 4,
        batch_size: 8192,
    };
    
    println!("📊 Iceberg表配置：");
    println!("✅ 表路径: {}", config.table_path);
    println!("✅ 仓库路径: {}", config.warehouse_path);
    println!("✅ 快照隔离: {}", if config.enable_snapshot_isolation { "启用" } else { "禁用" });
    println!("✅ 时间旅行: {}", if config.enable_time_travel { "启用" } else { "禁用" });
    println!("✅ 分支和标签: {}", if config.enable_branches_tags { "启用" } else { "禁用" });
    println!("✅ 增量读取: {}", if config.enable_incremental_read { "启用" } else { "禁用" });
    println!("✅ 谓词下推: {}", if config.enable_predicate_pushdown { "启用" } else { "禁用" });
    println!("✅ 列投影: {}", if config.enable_column_projection { "启用" } else { "禁用" });
    println!("✅ 分区剪枝: {}", if config.enable_partition_pruning { "启用" } else { "禁用" });
    println!("✅ 统计信息: {}", if config.enable_statistics { "启用" } else { "禁用" });
    println!("✅ 最大并发读取数: {}", config.max_concurrent_reads);
    println!("✅ 批次大小: {}", config.batch_size);
    println!();
    
    // 创建Iceberg表读取器
    let mut reader = IcebergTableReader::new(config);
    
    // 打开Iceberg表
    println!("🔄 测试场景1：打开Iceberg表");
    println!("------------------------");
    match reader.open() {
        Ok(_) => println!("✅ Iceberg表打开成功"),
        Err(e) => println!("❌ Iceberg表打开失败: {}", e),
    }
    println!();
    
    // 获取表元数据
    println!("🔄 测试场景2：获取表元数据");
    println!("------------------------");
    match reader.get_metadata() {
        Ok(metadata) => {
            println!("✅ 表元数据获取成功:");
            println!("  📈 表ID: {}", metadata.table_id);
            println!("  📈 表名: {}", metadata.table_name);
            println!("  📈 命名空间: {}", metadata.namespace);
            println!("  📈 当前快照ID: {:?}", metadata.current_snapshot_id);
            println!("  📈 快照数量: {}", metadata.snapshots.len());
            println!("  📈 分区字段数: {}", metadata.partition_spec.partition_fields.len());
            println!("  📈 排序字段数: {}", metadata.sort_order.sort_fields.len());
            println!("  📈 表属性数: {}", metadata.properties.len());
        },
        Err(e) => println!("❌ 表元数据获取失败: {}", e),
    }
    println!();
    
    // 获取统计信息
    println!("🔄 测试场景3：获取统计信息");
    println!("------------------------");
    match reader.get_stats() {
        Ok(stats) => {
            println!("✅ 统计信息获取成功:");
            println!("  📈 总文件数: {}", stats.total_files);
            println!("  📈 总记录数: {}", stats.total_records);
            println!("  📈 总文件大小: {} bytes", stats.total_size_bytes);
            println!("  📈 分区数: {}", stats.partition_count);
            println!("  📈 快照数: {}", stats.snapshot_count);
            println!("  📈 平均文件大小: {} bytes", stats.avg_file_size_bytes);
            println!("  📈 平均记录数: {}", stats.avg_records_per_file);
        },
        Err(e) => println!("❌ 统计信息获取失败: {}", e),
    }
    println!();
    
    // 谓词下推测试
    println!("🔄 测试场景4：谓词下推");
    println!("------------------------");
    let predicates = vec![
        IcebergPredicate::Equals { field: "year".to_string(), value: "2023".to_string() },
        IcebergPredicate::In { field: "month".to_string(), values: vec!["12".to_string()] },
        IcebergPredicate::GreaterThan { field: "value".to_string(), value: "100".to_string() },
    ];
    match reader.apply_predicate_pushdown(&predicates) {
        Ok(filtered_files) => println!("✅ 谓词下推成功: {} files filtered", filtered_files),
        Err(e) => println!("❌ 谓词下推失败: {}", e),
    }
    println!();
    
    // 列投影测试
    println!("🔄 测试场景5：列投影");
    println!("------------------------");
    let columns = vec!["id".to_string(), "name".to_string(), "value".to_string(), "timestamp".to_string()];
    match reader.apply_column_projection(&columns) {
        Ok(_) => println!("✅ 列投影成功: {:?}", columns),
        Err(e) => println!("❌ 列投影失败: {}", e),
    }
    println!();
    
    // 分区剪枝测试
    println!("🔄 测试场景6：分区剪枝");
    println!("------------------------");
    let partition_values = HashMap::from([
        ("year".to_string(), "2023".to_string()),
        ("month".to_string(), "12".to_string()),
    ]);
    match reader.apply_partition_pruning(&partition_values) {
        Ok(pruned_files) => println!("✅ 分区剪枝成功: {} files pruned", pruned_files),
        Err(e) => println!("❌ 分区剪枝失败: {}", e),
    }
    println!();
    
    // 时间旅行测试
    println!("🔄 测试场景7：时间旅行");
    println!("------------------------");
    let time_travel_config = TimeTravelConfig {
        snapshot_id: Some(12345),
        timestamp_ms: None,
        branch: None,
        tag: None,
    };
    match reader.apply_time_travel(&time_travel_config) {
        Ok(_) => println!("✅ 时间旅行成功: 快照ID {}", time_travel_config.snapshot_id.unwrap()),
        Err(e) => println!("❌ 时间旅行失败: {}", e),
    }
    println!();
    
    // 分支和标签测试
    println!("🔄 测试场景8：分支和标签");
    println!("------------------------");
    let branch_config = TimeTravelConfig {
        snapshot_id: None,
        timestamp_ms: None,
        branch: Some("main".to_string()),
        tag: None,
    };
    match reader.apply_time_travel(&branch_config) {
        Ok(_) => println!("✅ 分支访问成功: {}", branch_config.branch.unwrap()),
        Err(e) => println!("❌ 分支访问失败: {}", e),
    }
    
    let tag_config = TimeTravelConfig {
        snapshot_id: None,
        timestamp_ms: None,
        branch: None,
        tag: Some("v1.0".to_string()),
    };
    match reader.apply_time_travel(&tag_config) {
        Ok(_) => println!("✅ 标签访问成功: {}", tag_config.tag.unwrap()),
        Err(e) => println!("❌ 标签访问失败: {}", e),
    }
    println!();
    
    // 增量读取测试
    println!("🔄 测试场景9：增量读取");
    println!("------------------------");
    let incremental_config = IncrementalReadConfig {
        start_snapshot_id: Some(12344),
        end_snapshot_id: Some(12345),
        start_timestamp_ms: None,
        end_timestamp_ms: None,
    };
    match reader.apply_incremental_read(&incremental_config) {
        Ok(_) => println!("✅ 增量读取成功: 从快照{}到快照{}", 
                         incremental_config.start_snapshot_id.unwrap(),
                         incremental_config.end_snapshot_id.unwrap()),
        Err(e) => println!("❌ 增量读取失败: {}", e),
    }
    println!();
    
    // 数据读取测试
    println!("🔄 测试场景10：数据读取");
    println!("------------------------");
    match reader.read_data() {
        Ok(batches) => println!("✅ 数据读取成功: {} batches", batches.len()),
        Err(e) => println!("❌ 数据读取失败: {}", e),
    }
    println!();
    
    // 综合功能测试
    println!("🔄 测试场景11：综合功能测试");
    println!("------------------------");
    match comprehensive_iceberg_test(&mut reader) {
        Ok(_) => println!("✅ 综合功能测试成功"),
        Err(e) => println!("❌ 综合功能测试失败: {}", e),
    }
    println!();
    
    // 性能测试
    println!("🔄 测试场景12：性能测试");
    println!("------------------------");
    let iterations = 1000;
    match performance_test(&mut reader, iterations) {
        Ok(results) => {
            let total_time: u128 = results.iter().sum();
            let avg_time = total_time / results.len() as u128;
            let min_time = results.iter().min().unwrap();
            let max_time = results.iter().max().unwrap();
            
            println!("✅ 性能测试结果 ({}次迭代):", iterations);
            println!("  📈 总耗时: {}μs", total_time);
            println!("  📈 平均耗时: {}μs", avg_time);
            println!("  📈 最小耗时: {}μs", min_time);
            println!("  📈 最大耗时: {}μs", max_time);
        },
        Err(e) => println!("❌ 性能测试失败: {}", e),
    }
    println!();
    
    // 高级功能演示
    println!("🔄 测试场景13：高级功能演示");
    println!("------------------------");
    
    // 复杂谓词组合
    let complex_predicates = vec![
        IcebergPredicate::Equals { field: "year".to_string(), value: "2023".to_string() },
        IcebergPredicate::In { field: "month".to_string(), values: vec!["11".to_string(), "12".to_string()] },
        IcebergPredicate::GreaterThan { field: "value".to_string(), value: "1000".to_string() },
        IcebergPredicate::LessThan { field: "value".to_string(), value: "5000".to_string() },
        IcebergPredicate::IsNotNull { field: "name".to_string() },
    ];
    match reader.apply_predicate_pushdown(&complex_predicates) {
        Ok(filtered_files) => println!("✅ 复杂谓词下推成功: {} files filtered", filtered_files),
        Err(e) => println!("❌ 复杂谓词下推失败: {}", e),
    }
    
    // 多分区剪枝
    let multi_partition_values = HashMap::from([
        ("year".to_string(), "2023".to_string()),
        ("month".to_string(), "12".to_string()),
        ("day".to_string(), "25".to_string()),
    ]);
    match reader.apply_partition_pruning(&multi_partition_values) {
        Ok(pruned_files) => println!("✅ 多分区剪枝成功: {} files pruned", pruned_files),
        Err(e) => println!("❌ 多分区剪枝失败: {}", e),
    }
    
    // 时间戳时间旅行
    let timestamp_config = TimeTravelConfig {
        snapshot_id: None,
        timestamp_ms: Some(1703001600000), // 2023-12-20
        branch: None,
        tag: None,
    };
    match reader.apply_time_travel(&timestamp_config) {
        Ok(_) => println!("✅ 时间戳时间旅行成功: {}", timestamp_config.timestamp_ms.unwrap()),
        Err(e) => println!("❌ 时间戳时间旅行失败: {}", e),
    }
    println!();
    
    println!("🎯 Iceberg湖仓集成演示完成！");
    println!("✅ 支持快照隔离和时间旅行");
    println!("✅ 支持分支和标签管理");
    println!("✅ 支持增量读取和谓词下推");
    println!("✅ 支持列投影和分区剪枝");
    println!("✅ 支持复杂谓词组合和多分区剪枝");
    println!("✅ 支持高性能批量处理和统计信息");
    println!("✅ 为湖仓一体化查询提供了完整的Iceberg集成能力");
    
    Ok(())
}
