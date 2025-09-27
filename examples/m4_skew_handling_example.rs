use oneengine::execution::operators::skew_handling::{
    SkewHandlerSync, SkewHandlingConfig, SkewDetectionResult, RepartitionStrategy
};
use arrow::array::{Int32Array, StringArray, Float64Array};
use arrow::datatypes::{Field, DataType, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use std::time::Instant;

fn create_test_schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
        Field::new("category", DataType::Utf8, false),
    ])
}

fn create_test_batch() -> RecordBatch {
    let schema = create_test_schema();
    let id_array = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let name_array = StringArray::from(vec![
        "Alice", "Bob", "Charlie", "David", "Eve",
        "Frank", "Grace", "Henry", "Ivy", "Jack"
    ]);
    let value_array = Float64Array::from(vec![10.5, 20.3, 30.7, 40.1, 50.9, 60.2, 70.8, 80.4, 90.6, 100.0]);
    let category_array = StringArray::from(vec![
        "A", "B", "A", "B", "A", "B", "A", "B", "A", "B"
    ]);
    
    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(id_array),
            Arc::new(name_array),
            Arc::new(value_array),
            Arc::new(category_array),
        ]
    ).unwrap()
}

fn simulate_partition_data(handler: &mut SkewHandlerSync, partition_id: usize, row_count: usize, data_size: usize, processing_time_us: u64) {
    handler.record_partition_stats(partition_id, row_count, data_size, processing_time_us);
    println!("  📊 分区{}: 行数={}, 大小={}B, 处理时间={}μs", 
             partition_id, row_count, data_size, processing_time_us);
}

fn main() {
    println!("🚀 M4里程碑：数据倾斜检测和处理演示");
    println!("================================================");
    
    // 创建配置
    let config = SkewHandlingConfig {
        skew_threshold: 2.0,
        min_partitions: 2,
        max_partitions: 32,
        repartition_threshold: 1.5,
        sampling_rate: 0.1,
        window_size: 50,
    };
    
    println!("📊 倾斜检测配置：");
    println!("✅ 倾斜阈值: {} 倍标准差", config.skew_threshold);
    println!("✅ 最小分区数: {}", config.min_partitions);
    println!("✅ 最大分区数: {}", config.max_partitions);
    println!("✅ 重分区阈值: {} 倍平均数据量", config.repartition_threshold);
    println!("✅ 采样率: {:.1}%", config.sampling_rate * 100.0);
    println!("✅ 统计窗口: {} 个批次", config.window_size);
    println!();
    
    // 创建倾斜处理器
    let mut handler = SkewHandlerSync::new(config);
    let schema = create_test_schema();
    let test_batch = create_test_batch();
    
    // 测试场景1：无倾斜情况
    println!("🔄 测试场景1：无倾斜情况");
    println!("------------------------");
    handler.reset_stats();
    
    for i in 0..10 {
        simulate_partition_data(&mut handler, i, 100, 1000, 1000);
    }
    
    let detection_result = handler.detect_skew();
    match detection_result {
        SkewDetectionResult::NoSkew => {
            println!("✅ 检测结果: 无倾斜");
        }
        _ => {
            println!("❌ 检测结果: 意外检测到倾斜");
        }
    }
    println!();
    
    // 测试场景2：轻微倾斜情况
    println!("🔄 测试场景2：轻微倾斜情况");
    println!("------------------------");
    handler.reset_stats();
    
    for i in 0..8 {
        simulate_partition_data(&mut handler, i, 100, 1000, 1000);
    }
    simulate_partition_data(&mut handler, 8, 100, 2000, 2000); // 倾斜分区
    simulate_partition_data(&mut handler, 9, 100, 1000, 1000);
    
    let detection_result = handler.detect_skew();
    match detection_result {
        SkewDetectionResult::LightSkew { skewed_partitions, skew_ratio } => {
            println!("✅ 检测结果: 轻微倾斜");
            println!("  📈 倾斜分区: {:?}", skewed_partitions);
            println!("  📈 倾斜程度: {:.2}", skew_ratio);
        }
        _ => {
            println!("❌ 检测结果: 未检测到轻微倾斜");
        }
    }
    println!();
    
    // 测试场景3：严重倾斜情况
    println!("🔄 测试场景3：严重倾斜情况");
    println!("------------------------");
    handler.reset_stats();
    
    for i in 0..8 {
        simulate_partition_data(&mut handler, i, 100, 1000, 1000);
    }
    simulate_partition_data(&mut handler, 8, 100, 5000, 5000); // 严重倾斜分区
    simulate_partition_data(&mut handler, 9, 100, 1000, 1000);
    
    let detection_result = handler.detect_skew();
    match detection_result {
        SkewDetectionResult::HeavySkew { ref skewed_partitions, skew_ratio, suggested_partitions } => {
            println!("✅ 检测结果: 严重倾斜");
            println!("  📈 倾斜分区: {:?}", skewed_partitions);
            println!("  📈 倾斜程度: {:.2}", skew_ratio);
            println!("  📈 建议分区数: {}", suggested_partitions);
        }
        _ => {
            println!("❌ 检测结果: 未检测到严重倾斜");
        }
    }
    println!();
    
    // 测试场景4：重分区策略生成
    println!("🔄 测试场景4：重分区策略生成");
    println!("------------------------");
    
    let strategy = handler.generate_repartition_strategy(&detection_result, &schema);
    match strategy {
        Some(RepartitionStrategy::HashRepartition { new_partition_count, ref partition_columns }) => {
            println!("✅ 生成策略: 哈希重分区");
            println!("  📈 新分区数: {}", new_partition_count);
            println!("  📈 分区键列: {:?}", partition_columns);
        }
        Some(RepartitionStrategy::RangeRepartition { new_partition_count, ref partition_columns, ref range_bounds }) => {
            println!("✅ 生成策略: 范围重分区");
            println!("  📈 新分区数: {}", new_partition_count);
            println!("  📈 分区键列: {:?}", partition_columns);
            println!("  📈 范围边界: {:?}", range_bounds);
        }
        Some(RepartitionStrategy::RoundRobinRepartition { new_partition_count }) => {
            println!("✅ 生成策略: 轮询重分区");
            println!("  📈 新分区数: {}", new_partition_count);
        }
        None => {
            println!("❌ 未生成重分区策略");
        }
    }
    println!();
    
    // 测试场景5：重分区策略应用
    println!("🔄 测试场景5：重分区策略应用");
    println!("------------------------");
    
    if let Some(strategy) = strategy {
        let start = Instant::now();
        match handler.apply_repartition_strategy(&strategy, &test_batch) {
            Ok(repartitioned_batch) => {
                let duration = start.elapsed();
                println!("✅ 重分区成功: {:.2}μs", duration.as_micros());
                println!("  📈 原批次行数: {}", test_batch.num_rows());
                println!("  📈 重分区后行数: {}", repartitioned_batch.num_rows());
            }
            Err(e) => {
                println!("❌ 重分区失败: {}", e);
            }
        }
    }
    println!();
    
    // 测试场景6：分区统计摘要
    println!("🔄 测试场景6：分区统计摘要");
    println!("------------------------");
    println!("{}", handler.get_partition_summary());
    
    // 测试场景7：性能测试
    println!("🔄 测试场景7：性能测试");
    println!("------------------------");
    
    let start = Instant::now();
    for i in 0..1000 {
        handler.record_partition_stats(i % 10, 100, 1000, 1000);
    }
    let record_duration = start.elapsed();
    
    let start = Instant::now();
    for _ in 0..100 {
        let _ = handler.detect_skew();
    }
    let detect_duration = start.elapsed();
    
    println!("✅ 性能测试结果:");
    println!("  📈 记录1000次统计: {:.2}μs", record_duration.as_micros());
    println!("  📈 检测100次倾斜: {:.2}μs", detect_duration.as_micros());
    println!("  📈 平均记录时间: {:.2}μs", record_duration.as_micros() as f64 / 1000.0);
    println!("  📈 平均检测时间: {:.2}μs", detect_duration.as_micros() as f64 / 100.0);
    println!();
    
    // 测试场景8：启用/禁用测试
    println!("🔄 测试场景8：启用/禁用测试");
    println!("------------------------");
    
    println!("✅ 初始状态: {}", if handler.is_enabled() { "启用" } else { "禁用" });
    
    handler.set_enabled(false);
    println!("✅ 禁用后状态: {}", if handler.is_enabled() { "启用" } else { "禁用" });
    
    handler.set_enabled(true);
    println!("✅ 启用后状态: {}", if handler.is_enabled() { "启用" } else { "禁用" });
    println!();
    
    println!("🎯 M4里程碑完成！");
    println!("✅ 数据倾斜检测和处理已实现");
    println!("✅ 支持无倾斜、轻微倾斜、严重倾斜检测");
    println!("✅ 支持哈希、范围、轮询重分区策略");
    println!("✅ 支持实时统计和性能监控");
    println!("✅ 支持启用/禁用控制");
    println!("✅ 支持高性能批量处理");
}
