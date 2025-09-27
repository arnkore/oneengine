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


use oneengine::io::orc_reader::*;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt::init();
    
    println!("🚀 ORC数据湖读取与剪枝演示");
    println!("================================================");
    
    // 创建ORC读取配置
    let config = OrcReaderConfig {
        file_path: "data/sample.orc".to_string(),
        enable_page_index: true,
        enable_predicate_pushdown: true,
        enable_dictionary_retention: true,
        enable_lazy_materialization: true,
        max_stripes: Some(1000),
        batch_size: 8192,
        predicates: vec![
            Predicate::Equals { 
                column: "status".to_string(), 
                value: "active".to_string() 
            },
            Predicate::Range { 
                column: "amount".to_string(), 
                min: "100.0".to_string(), 
                max: "1000.0".to_string() 
            },
        ],
    };
    
    println!("📊 ORC读取配置：");
    println!("✅ 页索引: {}", if config.enable_page_index { "启用" } else { "禁用" });
    println!("✅ 谓词下推: {}", if config.enable_predicate_pushdown { "启用" } else { "禁用" });
    println!("✅ 字典留存: {}", if config.enable_dictionary_retention { "启用" } else { "禁用" });
    println!("✅ 延迟物化: {}", if config.enable_lazy_materialization { "启用" } else { "禁用" });
    println!("✅ 最大Stripe数: {:?}", config.max_stripes);
    println!("✅ 批次大小: {}", config.batch_size);
    println!("✅ 谓词数量: {}", config.predicates.len());
    println!();
    
    // 创建ORC读取器
    let mut reader = OrcDataLakeReader::new(config);
    
    // 打开ORC文件
    println!("🔄 测试场景1：打开ORC文件");
    println!("------------------------");
    match reader.open() {
        Ok(_) => println!("✅ ORC文件打开成功"),
        Err(e) => println!("❌ ORC文件打开失败: {}", e),
    }
    println!();
    
    // 获取统计信息
    println!("🔄 测试场景2：获取统计信息");
    println!("------------------------");
    match reader.get_statistics() {
        Ok(stats) => {
            println!("✅ 统计信息获取成功:");
            println!("  📈 总行数: {}", stats.total_rows);
            println!("  📈 总Stripe数: {}", stats.total_stripes);
            println!("  📈 总文件大小: {} bytes", stats.total_size);
        },
        Err(e) => println!("❌ 统计信息获取失败: {}", e),
    }
    println!();
    
    // 分区剪枝测试
    println!("🔄 测试场景3：分区剪枝");
    println!("------------------------");
    let partition_info = PartitionPruningInfo {
        partition_columns: vec!["year".to_string(), "month".to_string()],
        partition_values: vec!["2023".to_string(), "12".to_string()],
    };
    match reader.apply_partition_pruning(&partition_info) {
        Ok(count) => println!("✅ 分区剪枝成功: {} stripes pruned", count),
        Err(e) => println!("❌ 分区剪枝失败: {}", e),
    }
    println!();
    
    // 分桶剪枝测试
    println!("🔄 测试场景4：分桶剪枝");
    println!("------------------------");
    let bucket_info = BucketPruningInfo {
        bucket_columns: vec!["user_id".to_string()],
        bucket_count: 100,
        target_bucket_id: 42,
    };
    match reader.apply_bucket_pruning(&bucket_info) {
        Ok(count) => println!("✅ 分桶剪枝成功: {} stripes pruned", count),
        Err(e) => println!("❌ 分桶剪枝失败: {}", e),
    }
    println!();
    
    // ZoneMap剪枝测试
    println!("🔄 测试场景5：ZoneMap剪枝");
    println!("------------------------");
    let zone_map_info = ZoneMapPruningInfo {
        column: "amount".to_string(),
        min_value: "100.0".to_string(),
        max_value: "1000.0".to_string(),
    };
    match reader.apply_zone_map_pruning(&zone_map_info) {
        Ok(count) => println!("✅ ZoneMap剪枝成功: {} stripes pruned", count),
        Err(e) => println!("❌ ZoneMap剪枝失败: {}", e),
    }
    println!();
    
    // 页索引剪枝测试
    println!("🔄 测试场景6：页索引剪枝");
    println!("------------------------");
    match reader.apply_page_index_pruning() {
        Ok(count) => println!("✅ 页索引剪枝成功: {} stripes pruned", count),
        Err(e) => println!("❌ 页索引剪枝失败: {}", e),
    }
    println!();
    
    // 谓词下推测试
    println!("🔄 测试场景7：谓词下推");
    println!("------------------------");
    match reader.apply_predicate_pushdown() {
        Ok(count) => println!("✅ 谓词下推成功: {} stripes pruned", count),
        Err(e) => println!("❌ 谓词下推失败: {}", e),
    }
    println!();
    
    // 字典留存测试
    println!("🔄 测试场景8：字典留存");
    println!("------------------------");
    match reader.apply_dictionary_retention() {
        Ok(count) => println!("✅ 字典留存成功: {} stripes retained", count),
        Err(e) => println!("❌ 字典留存失败: {}", e),
    }
    println!();
    
    // 延迟物化测试
    println!("🔄 测试场景9：延迟物化");
    println!("------------------------");
    let filter_columns = vec!["id".to_string(), "status".to_string()];
    let main_columns = vec!["name".to_string(), "value".to_string(), "timestamp".to_string()];
    
    match reader.apply_lazy_materialization(filter_columns.clone(), main_columns.clone()) {
        Ok(_) => {
            println!("✅ 延迟物化配置成功");
            println!("  📈 过滤列: {:?}", filter_columns);
            println!("  📈 主列: {:?}", main_columns);
            
            // 测试延迟物化读取
            match reader.read_with_lazy_materialization() {
                Ok(batches) => {
                    println!("✅ 延迟物化读取成功");
                    println!("  📈 返回批次数量: {}", batches.len());
                },
                Err(e) => println!("❌ 延迟物化读取失败: {}", e),
            }
        },
        Err(e) => println!("❌ 延迟物化配置失败: {}", e),
    }
    println!();
    
    // 综合剪枝测试
    println!("🔄 测试场景10：综合剪枝测试");
    println!("------------------------");
    match comprehensive_pruning_test(&mut reader) {
        Ok(stats) => {
            println!("✅ 综合剪枝完成:");
            println!("  📈 分区剪枝: {} stripes", stats.partition_pruning_count);
            println!("  📈 分桶剪枝: {} stripes", stats.bucket_pruning_count);
            println!("  📈 ZoneMap剪枝: {} stripes", stats.zone_map_pruning_count);
            println!("  📈 页索引剪枝: {} stripes", stats.page_index_pruning_count);
            println!("  📈 谓词下推: {} stripes", stats.predicate_pushdown_count);
            println!("  📈 字典留存: {} stripes", stats.dictionary_retention_count);
        },
        Err(e) => println!("❌ 综合剪枝失败: {}", e),
    }
    println!();
    
    // 性能测试
    println!("🔄 测试场景11：性能测试");
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
    
    println!("🎯 ORC数据湖读取与剪枝演示完成！");
    println!("✅ 支持分区剪枝、分桶剪枝、ZoneMap剪枝");
    println!("✅ 支持页索引剪枝、谓词下推、字典留存");
    println!("✅ 支持延迟物化：先取过滤列，行号驱动拉取主列");
    println!("✅ 支持高性能批量处理和综合剪枝优化");
    println!("✅ 为ORC数据湖查询提供了完整的读取优化能力");
    
    Ok(())
}
