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


//! M4里程碑：自适应批次大小调整示例
//! 
//! 演示根据系统负载、内存使用情况和处理性能动态调整批次大小

use oneengine::execution::operators::adaptive_batch::{AdaptiveBatchAdjuster, AdaptiveBatchConfig, PerformanceTrend};
use arrow::record_batch::RecordBatch;
use arrow::array::{Int32Array, StringArray, Float64Array};
use arrow::datatypes::{Schema, Field, DataType};
use std::sync::Arc;
use std::time::{Instant, Duration};
use std::thread;

/// 创建测试数据
fn create_test_batch(size: usize) -> RecordBatch {
    let id_data = Int32Array::from((0..size).map(|i| i as i32).collect::<Vec<i32>>());
    let name_data = StringArray::from(
        (0..size).map(|i| format!("Name_{}", i)).collect::<Vec<String>>()
    );
    let salary_data = Float64Array::from(
        (0..size).map(|i| 50000.0 + (i as f64 * 1000.0)).collect::<Vec<f64>>()
    );
    
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("salary", DataType::Float64, false),
    ]);
    
    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(id_data),
            Arc::new(name_data),
            Arc::new(salary_data),
        ],
    ).unwrap()
}

/// 模拟数据处理
fn process_batch(batch: &RecordBatch, complexity: f64) -> Duration {
    let start = Instant::now();
    
    // 模拟不同复杂度的处理
    let sleep_duration = Duration::from_micros((batch.num_rows() as f64 * complexity) as u64);
    thread::sleep(sleep_duration);
    
    start.elapsed()
}

/// 模拟内存使用
fn calculate_memory_usage(batch: &RecordBatch, memory_factor: f64) -> usize {
    let base_memory = batch.get_array_memory_size();
    (base_memory as f64 * memory_factor) as usize
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 M4里程碑：自适应批次大小调整演示");
    println!("================================================");
    
    // 创建自适应批次配置
    let config = AdaptiveBatchConfig {
        initial_batch_size: 1000,
        min_batch_size: 100,
        max_batch_size: 10000,
        adjustment_step: 500,
        stats_window_size: 20,
        target_throughput: 50000.0, // 5万行/秒
        target_memory_usage: 10 * 1024 * 1024, // 10MB
        adjustment_interval_ms: 500, // 500ms
        performance_threshold: 0.8,
        memory_threshold: 1.2,
    };
    
    println!("📊 自适应批次配置：");
    println!("✅ 初始批次大小: {}", config.initial_batch_size);
    println!("✅ 最小批次大小: {}", config.min_batch_size);
    println!("✅ 最大批次大小: {}", config.max_batch_size);
    println!("✅ 调整步长: {}", config.adjustment_step);
    println!("✅ 目标吞吐量: {:.0} 行/秒", config.target_throughput);
    println!("✅ 目标内存使用: {} MB", config.target_memory_usage / 1024 / 1024);
    println!();
    
    // 创建自适应批次调整器
    let adjuster = AdaptiveBatchAdjuster::new(config);
    
    // 测试场景1：正常负载
    println!("🔄 测试场景1：正常负载");
    test_normal_load(&adjuster).await?;
    println!();
    
    // 测试场景2：高负载
    println!("🔄 测试场景2：高负载");
    test_high_load(&adjuster).await?;
    println!();
    
    // 测试场景3：内存受限
    println!("🔄 测试场景3：内存受限");
    test_memory_constrained(&adjuster).await?;
    println!();
    
    // 测试场景4：性能优化
    println!("🔄 测试场景4：性能优化");
    test_performance_optimization(&adjuster).await?;
    println!();
    
    // 显示最终统计
    println!("📈 最终性能统计：");
    let summary = adjuster.get_performance_summary();
    println!("✅ 最终批次大小: {}", summary.current_batch_size);
    println!("✅ 总处理行数: {}", summary.total_rows_processed);
    println!("✅ 整体吞吐量: {:.2} 行/秒", summary.overall_throughput);
    println!("✅ 当前内存使用: {} MB", summary.current_memory_usage / 1024 / 1024);
    println!("✅ 性能趋势: {:?}", summary.performance_trend);
    println!("✅ 统计样本数: {}", summary.stats_count);
    
    println!();
    println!("🎯 M4里程碑完成！");
    println!("✅ 自适应批次大小调整已实现");
    println!("✅ 支持基于性能的动态调整");
    println!("✅ 支持基于内存使用的调整");
    println!("✅ 支持性能趋势分析");
    println!("✅ 支持多场景自适应优化");
    println!("✅ 支持实时统计和监控");
    
    Ok(())
}

async fn test_normal_load(adjuster: &AdaptiveBatchAdjuster) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    let mut total_rows = 0;
    
    for i in 0..10 {
        let batch_size = adjuster.get_current_batch_size();
        let batch = create_test_batch(batch_size);
        
        // 模拟正常处理（复杂度1.0）
        let processing_time = process_batch(&batch, 1.0);
        let memory_usage = calculate_memory_usage(&batch, 1.0);
        
        // 记录统计
        adjuster.record_batch_stats(batch_size, processing_time, memory_usage);
        total_rows += batch_size;
        
        // 尝试调整批次大小
        if let Some(new_size) = adjuster.adjust_batch_size() {
            println!("  📊 批次大小调整: {} -> {} (第{}轮)", 
                     batch_size, new_size, i + 1);
        }
        
        // 显示性能统计
        let summary = adjuster.get_performance_summary();
        println!("  📈 第{}轮: 批次大小={}, 吞吐量={:.0} 行/秒, 内存={} MB", 
                 i + 1, batch_size, summary.overall_throughput, 
                 memory_usage / 1024 / 1024);
        
        // 模拟处理间隔
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    
    let duration = start.elapsed();
    println!("✅ 正常负载测试完成: {} 行, {:.2} 秒", total_rows, duration.as_secs_f64());
    
    Ok(())
}

async fn test_high_load(adjuster: &AdaptiveBatchAdjuster) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    let mut total_rows = 0;
    
    for i in 0..8 {
        let batch_size = adjuster.get_current_batch_size();
        let batch = create_test_batch(batch_size);
        
        // 模拟高负载处理（复杂度2.0）
        let processing_time = process_batch(&batch, 2.0);
        let memory_usage = calculate_memory_usage(&batch, 1.2);
        
        // 记录统计
        adjuster.record_batch_stats(batch_size, processing_time, memory_usage);
        total_rows += batch_size;
        
        // 尝试调整批次大小
        if let Some(new_size) = adjuster.adjust_batch_size() {
            println!("  📊 批次大小调整: {} -> {} (第{}轮)", 
                     batch_size, new_size, i + 1);
        }
        
        // 显示性能统计
        let summary = adjuster.get_performance_summary();
        println!("  📈 第{}轮: 批次大小={}, 吞吐量={:.0} 行/秒, 内存={} MB", 
                 i + 1, batch_size, summary.overall_throughput, 
                 memory_usage / 1024 / 1024);
        
        // 模拟处理间隔
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    
    let duration = start.elapsed();
    println!("✅ 高负载测试完成: {} 行, {:.2} 秒", total_rows, duration.as_secs_f64());
    
    Ok(())
}

async fn test_memory_constrained(adjuster: &AdaptiveBatchAdjuster) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    let mut total_rows = 0;
    
    for i in 0..6 {
        let batch_size = adjuster.get_current_batch_size();
        let batch = create_test_batch(batch_size);
        
        // 模拟内存受限处理（内存因子1.5）
        let processing_time = process_batch(&batch, 1.2);
        let memory_usage = calculate_memory_usage(&batch, 1.5);
        
        // 记录统计
        adjuster.record_batch_stats(batch_size, processing_time, memory_usage);
        total_rows += batch_size;
        
        // 尝试调整批次大小
        if let Some(new_size) = adjuster.adjust_batch_size() {
            println!("  📊 批次大小调整: {} -> {} (第{}轮)", 
                     batch_size, new_size, i + 1);
        }
        
        // 显示性能统计
        let summary = adjuster.get_performance_summary();
        println!("  📈 第{}轮: 批次大小={}, 吞吐量={:.0} 行/秒, 内存={} MB", 
                 i + 1, batch_size, summary.overall_throughput, 
                 memory_usage / 1024 / 1024);
        
        // 模拟处理间隔
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    
    let duration = start.elapsed();
    println!("✅ 内存受限测试完成: {} 行, {:.2} 秒", total_rows, duration.as_secs_f64());
    
    Ok(())
}

async fn test_performance_optimization(adjuster: &AdaptiveBatchAdjuster) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    let mut total_rows = 0;
    
    for i in 0..12 {
        let batch_size = adjuster.get_current_batch_size();
        let batch = create_test_batch(batch_size);
        
        // 模拟性能优化处理（复杂度0.8）
        let processing_time = process_batch(&batch, 0.8);
        let memory_usage = calculate_memory_usage(&batch, 0.9);
        
        // 记录统计
        adjuster.record_batch_stats(batch_size, processing_time, memory_usage);
        total_rows += batch_size;
        
        // 尝试调整批次大小
        if let Some(new_size) = adjuster.adjust_batch_size() {
            println!("  📊 批次大小调整: {} -> {} (第{}轮)", 
                     batch_size, new_size, i + 1);
        }
        
        // 显示性能统计
        let summary = adjuster.get_performance_summary();
        println!("  📈 第{}轮: 批次大小={}, 吞吐量={:.0} 行/秒, 内存={} MB", 
                 i + 1, batch_size, summary.overall_throughput, 
                 memory_usage / 1024 / 1024);
        
        // 模拟处理间隔
        tokio::time::sleep(Duration::from_millis(80)).await;
    }
    
    let duration = start.elapsed();
    println!("✅ 性能优化测试完成: {} 行, {:.2} 秒", total_rows, duration.as_secs_f64());
    
    Ok(())
}
