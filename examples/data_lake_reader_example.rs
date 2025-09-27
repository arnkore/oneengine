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


use oneengine::io::data_lake_reader::{
    DataLakeReaderSync, DataLakeReaderConfig, PredicateFilter, 
    PartitionPruningInfo, BucketPruningInfo, ZoneMapPruningInfo
};
use std::collections::HashMap;
use std::time::Instant;

fn main() {
    println!("🚀 数据湖读取与剪枝演示");
    println!("================================================");
    
    // 创建配置
    let config = DataLakeReaderConfig {
        enable_page_index: true,
        enable_predicate_pushdown: true,
        enable_dictionary_retention: true,
        enable_lazy_materialization: true,
        max_rowgroups: Some(1000),
        batch_size: 8192,
        predicates: vec![
            PredicateFilter::Equals {
                column: "status".to_string(),
                value: "active".to_string(),
            },
            PredicateFilter::Range {
                column: "timestamp".to_string(),
                min: Some("2023-01-01".to_string()),
                max: Some("2023-12-31".to_string()),
            },
        ],
    };
    
    println!("📊 数据湖读取配置：");
    println!("✅ 页索引: {}", if config.enable_page_index { "启用" } else { "禁用" });
    println!("✅ 谓词下推: {}", if config.enable_predicate_pushdown { "启用" } else { "禁用" });
    println!("✅ 字典留存: {}", if config.enable_dictionary_retention { "启用" } else { "禁用" });
    println!("✅ 延迟物化: {}", if config.enable_lazy_materialization { "启用" } else { "禁用" });
    println!("✅ 最大RowGroup数: {:?}", config.max_rowgroups);
    println!("✅ 批次大小: {}", config.batch_size);
    println!("✅ 谓词数量: {}", config.predicates.len());
    println!();
    
    // 创建数据湖读取器
    let mut reader = DataLakeReaderSync::new(config);
    
    // 测试场景1：分区剪枝
    println!("🔄 测试场景1：分区剪枝");
    println!("------------------------");
    
    let mut partition_values = HashMap::new();
    partition_values.insert("year".to_string(), "2023".to_string());
    partition_values.insert("month".to_string(), "12".to_string());
    
    let partition_pruning = PartitionPruningInfo {
        partition_columns: vec!["year".to_string(), "month".to_string()],
        partition_values,
        matches: true,
    };
    
    let start = Instant::now();
    match reader.apply_partition_pruning(&partition_pruning) {
        Ok(selected_rowgroups) => {
            let duration = start.elapsed();
            println!("✅ 分区剪枝成功: {:.2}μs", duration.as_micros());
            println!("  📈 选中RowGroup数量: {}", selected_rowgroups.len());
            println!("  📈 分区列: {:?}", partition_pruning.partition_columns);
        }
        Err(e) => {
            println!("❌ 分区剪枝失败: {}", e);
        }
    }
    println!();
    
    // 测试场景2：分桶剪枝
    println!("🔄 测试场景2：分桶剪枝");
    println!("------------------------");
    
    let bucket_pruning = BucketPruningInfo {
        bucket_columns: vec!["user_id".to_string()],
        bucket_count: 32,
        target_buckets: vec![0, 1, 2, 3],
    };
    
    let start = Instant::now();
    match reader.apply_bucket_pruning(&bucket_pruning) {
        Ok(selected_rowgroups) => {
            let duration = start.elapsed();
            println!("✅ 分桶剪枝成功: {:.2}μs", duration.as_micros());
            println!("  📈 选中RowGroup数量: {}", selected_rowgroups.len());
            println!("  📈 分桶列: {:?}", bucket_pruning.bucket_columns);
            println!("  📈 分桶数量: {}", bucket_pruning.bucket_count);
            println!("  📈 目标分桶: {:?}", bucket_pruning.target_buckets);
        }
        Err(e) => {
            println!("❌ 分桶剪枝失败: {}", e);
        }
    }
    println!();
    
    // 测试场景3：ZoneMap剪枝
    println!("🔄 测试场景3：ZoneMap剪枝");
    println!("------------------------");
    
    let zone_map_pruning = ZoneMapPruningInfo {
        column: "timestamp".to_string(),
        min_value: Some("2023-01-01".to_string()),
        max_value: Some("2023-12-31".to_string()),
        matches: true,
    };
    
    let start = Instant::now();
    match reader.apply_zone_map_pruning(&zone_map_pruning) {
        Ok(selected_rowgroups) => {
            let duration = start.elapsed();
            println!("✅ ZoneMap剪枝成功: {:.2}μs", duration.as_micros());
            println!("  📈 选中RowGroup数量: {}", selected_rowgroups.len());
            println!("  📈 列名: {}", zone_map_pruning.column);
            println!("  📈 最小值: {:?}", zone_map_pruning.min_value);
            println!("  📈 最大值: {:?}", zone_map_pruning.max_value);
        }
        Err(e) => {
            println!("❌ ZoneMap剪枝失败: {}", e);
        }
    }
    println!();
    
    // 测试场景4：页索引剪枝
    println!("🔄 测试场景4：页索引剪枝");
    println!("------------------------");
    
    let start = Instant::now();
    match reader.apply_page_index_pruning() {
        Ok(selected_rowgroups) => {
            let duration = start.elapsed();
            println!("✅ 页索引剪枝成功: {:.2}μs", duration.as_micros());
            println!("  📈 选中RowGroup数量: {}", selected_rowgroups.len());
        }
        Err(e) => {
            println!("❌ 页索引剪枝失败: {}", e);
        }
    }
    println!();
    
    // 测试场景5：谓词下推
    println!("🔄 测试场景5：谓词下推");
    println!("------------------------");
    
    let all_rowgroups = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
    let start = Instant::now();
    match reader.apply_predicate_pushdown(&all_rowgroups) {
        Ok(selected_rowgroups) => {
            let duration = start.elapsed();
            println!("✅ 谓词下推成功: {:.2}μs", duration.as_micros());
            println!("  📈 输入RowGroup数量: {}", all_rowgroups.len());
            println!("  📈 选中RowGroup数量: {}", selected_rowgroups.len());
            println!("  📈 剪枝率: {:.1}%", 
                (1.0 - selected_rowgroups.len() as f64 / all_rowgroups.len() as f64) * 100.0);
        }
        Err(e) => {
            println!("❌ 谓词下推失败: {}", e);
        }
    }
    println!();
    
    // 测试场景6：字典留存
    println!("🔄 测试场景6：字典留存");
    println!("------------------------");
    
    let all_rowgroups = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
    let start = Instant::now();
    match reader.apply_dictionary_retention(&all_rowgroups) {
        Ok(selected_rowgroups) => {
            let duration = start.elapsed();
            println!("✅ 字典留存成功: {:.2}μs", duration.as_micros());
            println!("  📈 输入RowGroup数量: {}", all_rowgroups.len());
            println!("  📈 选中RowGroup数量: {}", selected_rowgroups.len());
        }
        Err(e) => {
            println!("❌ 字典留存失败: {}", e);
        }
    }
    println!();
    
    // 测试场景7：延迟物化
    println!("🔄 测试场景7：延迟物化");
    println!("------------------------");
    
    let filter_columns = vec!["id".to_string(), "status".to_string()];
    let main_columns = vec!["name".to_string(), "value".to_string(), "timestamp".to_string()];
    
    let start = Instant::now();
    match reader.apply_lazy_materialization(filter_columns.clone(), main_columns.clone()) {
        Ok(()) => {
            let duration = start.elapsed();
            println!("✅ 延迟物化配置成功: {:.2}μs", duration.as_micros());
            println!("  📈 过滤列: {:?}", filter_columns);
            println!("  📈 主列: {:?}", main_columns);
        }
        Err(e) => {
            println!("❌ 延迟物化配置失败: {}", e);
        }
    }
    
    // 测试延迟物化读取
    let all_rowgroups = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
    let start = Instant::now();
    match reader.read_with_lazy_materialization(&all_rowgroups) {
        Ok(batches) => {
            let duration = start.elapsed();
            println!("✅ 延迟物化读取成功: {:.2}μs", duration.as_micros());
            println!("  📈 返回批次数量: {}", batches.len());
        }
        Err(e) => {
            println!("❌ 延迟物化读取失败: {}", e);
        }
    }
    println!();
    
    // 测试场景8：综合剪枝测试
    println!("🔄 测试场景8：综合剪枝测试");
    println!("------------------------");
    
    let all_rowgroups = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
    
    // 步骤1：分区剪枝
    let start = Instant::now();
    let mut selected_rowgroups = match reader.apply_partition_pruning(&partition_pruning) {
        Ok(rowgroups) => rowgroups,
        Err(e) => {
            println!("❌ 分区剪枝失败: {}", e);
            all_rowgroups.clone()
        }
    };
    let partition_duration = start.elapsed();
    
    // 步骤2：分桶剪枝
    let start = Instant::now();
    selected_rowgroups = match reader.apply_bucket_pruning(&bucket_pruning) {
        Ok(rowgroups) => rowgroups,
        Err(e) => {
            println!("❌ 分桶剪枝失败: {}", e);
            selected_rowgroups
        }
    };
    let bucket_duration = start.elapsed();
    
    // 步骤3：ZoneMap剪枝
    let start = Instant::now();
    selected_rowgroups = match reader.apply_zone_map_pruning(&zone_map_pruning) {
        Ok(rowgroups) => rowgroups,
        Err(e) => {
            println!("❌ ZoneMap剪枝失败: {}", e);
            selected_rowgroups
        }
    };
    let zone_map_duration = start.elapsed();
    
    // 步骤4：页索引剪枝
    let start = Instant::now();
    selected_rowgroups = match reader.apply_page_index_pruning() {
        Ok(rowgroups) => rowgroups,
        Err(e) => {
            println!("❌ 页索引剪枝失败: {}", e);
            selected_rowgroups
        }
    };
    let page_index_duration = start.elapsed();
    
    // 步骤5：谓词下推
    let start = Instant::now();
    selected_rowgroups = match reader.apply_predicate_pushdown(&selected_rowgroups) {
        Ok(rowgroups) => rowgroups,
        Err(e) => {
            println!("❌ 谓词下推失败: {}", e);
            selected_rowgroups
        }
    };
    let predicate_duration = start.elapsed();
    
    // 步骤6：字典留存
    let start = Instant::now();
    selected_rowgroups = match reader.apply_dictionary_retention(&selected_rowgroups) {
        Ok(rowgroups) => rowgroups,
        Err(e) => {
            println!("❌ 字典留存失败: {}", e);
            selected_rowgroups
        }
    };
    let dictionary_duration = start.elapsed();
    
    println!("✅ 综合剪枝完成:");
    println!("  📈 原始RowGroup数量: {}", all_rowgroups.len());
    println!("  📈 最终RowGroup数量: {}", selected_rowgroups.len());
    println!("  📈 总剪枝率: {:.1}%", 
        (1.0 - selected_rowgroups.len() as f64 / all_rowgroups.len() as f64) * 100.0);
    println!();
    println!("  📊 各步骤耗时:");
    println!("    - 分区剪枝: {:.2}μs", partition_duration.as_micros());
    println!("    - 分桶剪枝: {:.2}μs", bucket_duration.as_micros());
    println!("    - ZoneMap剪枝: {:.2}μs", zone_map_duration.as_micros());
    println!("    - 页索引剪枝: {:.2}μs", page_index_duration.as_micros());
    println!("    - 谓词下推: {:.2}μs", predicate_duration.as_micros());
    println!("    - 字典留存: {:.2}μs", dictionary_duration.as_micros());
    println!();
    
    // 测试场景9：性能测试
    println!("🔄 测试场景9：性能测试");
    println!("------------------------");
    
    let iterations = 1000;
    let all_rowgroups = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
    
    // 测试分区剪枝性能
    let start = Instant::now();
    for _ in 0..iterations {
        let _ = reader.apply_partition_pruning(&partition_pruning);
    }
    let partition_perf = start.elapsed();
    
    // 测试分桶剪枝性能
    let start = Instant::now();
    for _ in 0..iterations {
        let _ = reader.apply_bucket_pruning(&bucket_pruning);
    }
    let bucket_perf = start.elapsed();
    
    // 测试ZoneMap剪枝性能
    let start = Instant::now();
    for _ in 0..iterations {
        let _ = reader.apply_zone_map_pruning(&zone_map_pruning);
    }
    let zone_map_perf = start.elapsed();
    
    // 测试页索引剪枝性能
    let start = Instant::now();
    for _ in 0..iterations {
        let _ = reader.apply_page_index_pruning();
    }
    let page_index_perf = start.elapsed();
    
    // 测试谓词下推性能
    let start = Instant::now();
    for _ in 0..iterations {
        let _ = reader.apply_predicate_pushdown(&all_rowgroups);
    }
    let predicate_perf = start.elapsed();
    
    println!("✅ 性能测试结果 ({}次迭代):", iterations);
    println!("  📈 分区剪枝: {:.2}μs (平均: {:.2}μs/次)", 
        partition_perf.as_micros(), partition_perf.as_micros() as f64 / iterations as f64);
    println!("  📈 分桶剪枝: {:.2}μs (平均: {:.2}μs/次)", 
        bucket_perf.as_micros(), bucket_perf.as_micros() as f64 / iterations as f64);
    println!("  📈 ZoneMap剪枝: {:.2}μs (平均: {:.2}μs/次)", 
        zone_map_perf.as_micros(), zone_map_perf.as_micros() as f64 / iterations as f64);
    println!("  📈 页索引剪枝: {:.2}μs (平均: {:.2}μs/次)", 
        page_index_perf.as_micros(), page_index_perf.as_micros() as f64 / iterations as f64);
    println!("  📈 谓词下推: {:.2}μs (平均: {:.2}μs/次)", 
        predicate_perf.as_micros(), predicate_perf.as_micros() as f64 / iterations as f64);
    println!();
    
    println!("🎯 数据湖读取与剪枝演示完成！");
    println!("✅ 支持分区剪枝、分桶剪枝、ZoneMap剪枝");
    println!("✅ 支持页索引剪枝、谓词下推、字典留存");
    println!("✅ 支持延迟物化：先取过滤列，行号驱动拉取主列");
    println!("✅ 支持高性能批量处理和综合剪枝优化");
    println!("✅ 为数据湖查询提供了完整的读取优化能力");
}
