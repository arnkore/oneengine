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


//! M4里程碑：Join build-side自适应选择示例
//! 
//! 演示根据数据大小、内存使用情况和系统负载动态选择Join的build side

use oneengine::execution::operators::join_adaptation::{
    JoinAdaptationSelectorSync, JoinAdaptationConfig, JoinStats, JoinStrategy, StrategyPerformance
};
use arrow::record_batch::RecordBatch;
use arrow::array::{Int32Array, StringArray, Float64Array};
use arrow::datatypes::{Schema, Field, DataType};
use std::sync::Arc;
use std::time::{Instant, Duration};
use std::thread;

/// 创建测试数据
fn create_test_batch(size: usize, prefix: &str) -> RecordBatch {
    let id_data = Int32Array::from((0..size).map(|i| i as i32).collect::<Vec<i32>>());
    let name_data = StringArray::from(
        (0..size).map(|i| format!("{}_{}", prefix, i)).collect::<Vec<String>>()
    );
    let value_data = Float64Array::from(
        (0..size).map(|i| 1000.0 + (i as f64 * 10.0)).collect::<Vec<f64>>()
    );
    
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]);
    
    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(id_data),
            Arc::new(name_data),
            Arc::new(value_data),
        ],
    ).unwrap()
}

/// 模拟Join处理
fn simulate_join_processing(
    left_batch: &RecordBatch,
    right_batch: &RecordBatch,
    strategy: &JoinStrategy,
) -> (Duration, usize) {
    let start = Instant::now();
    
    // 模拟不同策略的处理时间
    let processing_time = match strategy {
        JoinStrategy::LeftBuild => {
            // 左表作为build side，处理时间与左表大小相关
            Duration::from_micros(left_batch.num_rows() as u64 * 10)
        },
        JoinStrategy::RightBuild => {
            // 右表作为build side，处理时间与右表大小相关
            Duration::from_micros(right_batch.num_rows() as u64 * 10)
        },
        JoinStrategy::Broadcast => {
            // 广播Join，处理时间较短
            Duration::from_micros((left_batch.num_rows() + right_batch.num_rows()) as u64 * 5)
        },
        JoinStrategy::Partitioned => {
            // 分区Join，处理时间较长
            Duration::from_micros((left_batch.num_rows() + right_batch.num_rows()) as u64 * 20)
        },
        JoinStrategy::Auto => {
            // 自动选择，使用默认处理时间
            Duration::from_micros((left_batch.num_rows() + right_batch.num_rows()) as u64 * 15)
        },
    };
    
    // 模拟处理
    thread::sleep(processing_time);
    
    // 模拟输出行数（简化版本）
    let output_rows = match strategy {
        JoinStrategy::Broadcast => left_batch.num_rows() + right_batch.num_rows(),
        _ => (left_batch.num_rows() + right_batch.num_rows()) / 2,
    };
    
    (start.elapsed(), output_rows)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 M4里程碑：Join build-side自适应选择演示");
    println!("================================================");
    
    // 创建Join自适应配置
    let config = JoinAdaptationConfig {
        memory_threshold: 50 * 1024 * 1024, // 50MB
        broadcast_threshold: 5000, // 5千行
        partition_threshold: 500000, // 50万行
        memory_efficiency_threshold: 0.01,
        stats_window_size: 20,
        learning_rate: 0.2,
        min_samples: 3,
        strategy_switch_threshold: 0.15,
    };
    
    println!("📊 Join自适应配置：");
    println!("✅ 内存阈值: {} MB", config.memory_threshold / 1024 / 1024);
    println!("✅ 广播Join阈值: {} 行", config.broadcast_threshold);
    println!("✅ 分区Join阈值: {} 行", config.partition_threshold);
    println!("✅ 内存效率阈值: {:.3}", config.memory_efficiency_threshold);
    println!("✅ 学习率: {:.1}", config.learning_rate);
    println!("✅ 最小样本数: {}", config.min_samples);
    println!();
    
    // 创建Join自适应选择器
    let selector = JoinAdaptationSelectorSync::new(config);
    
    // 测试场景1：小表Join（应该选择广播）
    println!("🔄 测试场景1：小表Join（广播策略）");
    test_small_table_join(&selector).await?;
    println!();
    
    // 测试场景2：中等表Join（应该选择build side）
    println!("🔄 测试场景2：中等表Join（build side选择）");
    test_medium_table_join(&selector).await?;
    println!();
    
    // 测试场景3：大表Join（应该选择分区）
    println!("🔄 测试场景3：大表Join（分区策略）");
    test_large_table_join(&selector).await?;
    println!();
    
    // 测试场景4：学习优化
    println!("🔄 测试场景4：学习优化");
    test_learning_optimization(&selector).await?;
    println!();
    
    // 显示策略性能摘要
    println!("📈 策略性能摘要：");
    let strategy_performance = selector.get_strategy_performance_summary();
    for (strategy, perf) in &strategy_performance {
        println!("✅ {:?}: 使用次数={}, 平均处理时间={:.2}μs, 平均内存效率={:.4}, 成功率={:.2}",
                 strategy, perf.usage_count, perf.avg_processing_time, 
                 perf.avg_memory_efficiency, perf.success_rate);
    }
    
    // 显示整体性能摘要
    println!();
    println!("📊 整体性能摘要：");
    let overall_summary = selector.get_overall_performance_summary();
    println!("✅ 总Join次数: {}", overall_summary.total_joins);
    println!("✅ 平均处理时间: {:.2}μs", overall_summary.avg_processing_time);
    println!("✅ 平均输出行数: {:.2}", overall_summary.avg_output_rows);
    println!("✅ 策略数量: {}", overall_summary.strategy_count);
    println!("✅ 统计样本数: {}", overall_summary.stats_count);
    
    println!();
    println!("🎯 M4里程碑完成！");
    println!("✅ Join build-side自适应选择已实现");
    println!("✅ 支持基于规则的策略选择");
    println!("✅ 支持基于学习的选择优化");
    println!("✅ 支持多策略性能比较");
    println!("✅ 支持实时策略调整");
    println!("✅ 支持性能统计和监控");
    
    Ok(())
}

async fn test_small_table_join(selector: &JoinAdaptationSelectorSync) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    
    for i in 0..5 {
        // 创建小表
        let left_batch = create_test_batch(1000, "left");
        let right_batch = create_test_batch(2000, "right");
        
        // 选择Join策略
        let strategy = selector.select_join_strategy(&left_batch, &right_batch, 100 * 1024 * 1024);
        
        // 模拟Join处理
        let (processing_time, output_rows) = simulate_join_processing(&left_batch, &right_batch, &strategy);
        
        // 计算内存效率
        let left_memory = left_batch.get_array_memory_size();
        let right_memory = right_batch.get_array_memory_size();
        let memory_efficiency = output_rows as f64 / (left_memory + right_memory) as f64;
        
        // 记录统计信息
        let stats = JoinStats {
            left_size: left_batch.num_rows(),
            right_size: right_batch.num_rows(),
            left_memory,
            right_memory,
            selected_strategy: strategy.clone(),
            processing_time_us: processing_time.as_micros() as u64,
            output_rows,
            memory_efficiency,
            timestamp: Instant::now(),
        };
        
        selector.record_join_stats(stats);
        
        println!("  📈 第{}轮: 策略={:?}, 处理时间={:.2}ms, 输出行数={}, 内存效率={:.4}",
                 i + 1, strategy, processing_time.as_millis(), output_rows, memory_efficiency);
        
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    
    let duration = start.elapsed();
    println!("✅ 小表Join测试完成: {:.2} 秒", duration.as_secs_f64());
    
    Ok(())
}

async fn test_medium_table_join(selector: &JoinAdaptationSelectorSync) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    
    for i in 0..4 {
        // 创建中等表
        let left_batch = create_test_batch(10000, "left");
        let right_batch = create_test_batch(15000, "right");
        
        // 选择Join策略
        let strategy = selector.select_join_strategy(&left_batch, &right_batch, 200 * 1024 * 1024);
        
        // 模拟Join处理
        let (processing_time, output_rows) = simulate_join_processing(&left_batch, &right_batch, &strategy);
        
        // 计算内存效率
        let left_memory = left_batch.get_array_memory_size();
        let right_memory = right_batch.get_array_memory_size();
        let memory_efficiency = output_rows as f64 / (left_memory + right_memory) as f64;
        
        // 记录统计信息
        let stats = JoinStats {
            left_size: left_batch.num_rows(),
            right_size: right_batch.num_rows(),
            left_memory,
            right_memory,
            selected_strategy: strategy.clone(),
            processing_time_us: processing_time.as_micros() as u64,
            output_rows,
            memory_efficiency,
            timestamp: Instant::now(),
        };
        
        selector.record_join_stats(stats);
        
        println!("  📈 第{}轮: 策略={:?}, 处理时间={:.2}ms, 输出行数={}, 内存效率={:.4}",
                 i + 1, strategy, processing_time.as_millis(), output_rows, memory_efficiency);
        
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    
    let duration = start.elapsed();
    println!("✅ 中等表Join测试完成: {:.2} 秒", duration.as_secs_f64());
    
    Ok(())
}

async fn test_large_table_join(selector: &JoinAdaptationSelectorSync) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    
    for i in 0..3 {
        // 创建大表
        let left_batch = create_test_batch(100000, "left");
        let right_batch = create_test_batch(200000, "right");
        
        // 选择Join策略
        let strategy = selector.select_join_strategy(&left_batch, &right_batch, 500 * 1024 * 1024);
        
        // 模拟Join处理
        let (processing_time, output_rows) = simulate_join_processing(&left_batch, &right_batch, &strategy);
        
        // 计算内存效率
        let left_memory = left_batch.get_array_memory_size();
        let right_memory = right_batch.get_array_memory_size();
        let memory_efficiency = output_rows as f64 / (left_memory + right_memory) as f64;
        
        // 记录统计信息
        let stats = JoinStats {
            left_size: left_batch.num_rows(),
            right_size: right_batch.num_rows(),
            left_memory,
            right_memory,
            selected_strategy: strategy.clone(),
            processing_time_us: processing_time.as_micros() as u64,
            output_rows,
            memory_efficiency,
            timestamp: Instant::now(),
        };
        
        selector.record_join_stats(stats);
        
        println!("  📈 第{}轮: 策略={:?}, 处理时间={:.2}ms, 输出行数={}, 内存效率={:.4}",
                 i + 1, strategy, processing_time.as_millis(), output_rows, memory_efficiency);
        
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    
    let duration = start.elapsed();
    println!("✅ 大表Join测试完成: {:.2} 秒", duration.as_secs_f64());
    
    Ok(())
}

async fn test_learning_optimization(selector: &JoinAdaptationSelectorSync) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    
    // 使用相似的数据进行多次测试，观察学习效果
    for i in 0..6 {
        let left_batch = create_test_batch(5000, "left");
        let right_batch = create_test_batch(8000, "right");
        
        // 选择Join策略
        let strategy = selector.select_join_strategy(&left_batch, &right_batch, 150 * 1024 * 1024);
        
        // 模拟Join处理
        let (processing_time, output_rows) = simulate_join_processing(&left_batch, &right_batch, &strategy);
        
        // 计算内存效率
        let left_memory = left_batch.get_array_memory_size();
        let right_memory = right_batch.get_array_memory_size();
        let memory_efficiency = output_rows as f64 / (left_memory + right_memory) as f64;
        
        // 记录统计信息
        let stats = JoinStats {
            left_size: left_batch.num_rows(),
            right_size: right_batch.num_rows(),
            left_memory,
            right_memory,
            selected_strategy: strategy.clone(),
            processing_time_us: processing_time.as_micros() as u64,
            output_rows,
            memory_efficiency,
            timestamp: Instant::now(),
        };
        
        selector.record_join_stats(stats);
        
        println!("  📈 第{}轮: 策略={:?}, 处理时间={:.2}ms, 输出行数={}, 内存效率={:.4}",
                 i + 1, strategy, processing_time.as_millis(), output_rows, memory_efficiency);
        
        tokio::time::sleep(Duration::from_millis(80)).await;
    }
    
    let duration = start.elapsed();
    println!("✅ 学习优化测试完成: {:.2} 秒", duration.as_secs_f64());
    
    Ok(())
}
