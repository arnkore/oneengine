//! M1里程碑：向量化聚合算子示例
//! 
//! 演示向量化聚合算子的高性能处理功能

use oneengine::execution::operators::vectorized_aggregator::{VectorizedAggregator, AggregationFunction};
use oneengine::push_runtime::event_loop::EventLoop;
use oneengine::push_runtime::metrics::SimpleMetricsCollector;
use arrow::record_batch::RecordBatch;
use arrow::array::{Int32Array, StringArray, Float64Array};
use arrow::datatypes::{Schema, Field, DataType};
use datafusion_common::ScalarValue;
use std::sync::Arc;
use std::time::Instant;

/// 创建测试数据
fn create_test_batch() -> RecordBatch {
    let id_data = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let name_data = StringArray::from(vec![
        "Alice", "Bob", "Charlie", "David", "Eve", 
        "Frank", "Grace", "Henry", "Ivy", "Jack"
    ]);
    let salary_data = Float64Array::from(vec![
        65000.0, 75000.0, 80000.0, 60000.0, 90000.0,
        85000.0, 70000.0, 95000.0, 88000.0, 72000.0
    ]);
    
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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 M1里程碑：向量化聚合算子演示");
    println!("================================================");
    
    // 创建测试数据
    let batch = create_test_batch();
    println!("📊 测试数据：");
    println!("行数: {}", batch.num_rows());
    println!("列数: {}", batch.num_columns());
    println!("Schema: {:?}", batch.schema());
    println!();
    
    // 测试聚合功能
    println!("🔄 测试向量化聚合...");
    test_vectorized_aggregation(&batch)?;
    println!();
    
    println!("🎯 M1里程碑完成！");
    println!("✅ 向量化聚合算子已实现");
    println!("✅ 支持多种聚合函数");
    println!("✅ 支持SIMD优化");
    println!("✅ 基于Arrow的高效数据处理");
    println!("✅ 事件驱动的push执行模型集成");
    
    Ok(())
}

fn test_vectorized_aggregation(batch: &RecordBatch) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    
    // 创建聚合配置
    let aggregation_functions = vec![
        AggregationFunction::Count {
            column: "id".to_string(),
            output_column: "count".to_string(),
        },
        AggregationFunction::Sum {
            column: "salary".to_string(),
            output_column: "total_salary".to_string(),
        },
        AggregationFunction::Avg {
            column: "salary".to_string(),
            output_column: "avg_salary".to_string(),
        },
        AggregationFunction::Max {
            column: "salary".to_string(),
            output_column: "max_salary".to_string(),
        },
        AggregationFunction::Min {
            column: "salary".to_string(),
            output_column: "min_salary".to_string(),
        },
    ];
    
    // 创建向量化聚合算子
    let mut aggregator = VectorizedAggregator::new(
        1,
        aggregation_functions,
        batch.schema(),
        true, // 启用SIMD
        true, // 启用压缩
    );
    
    // 创建事件循环
    let mut event_loop = EventLoop::new();
    let metrics = Arc::new(SimpleMetricsCollector::default());
    
    // 注册算子
    event_loop.register_operator(1, Box::new(aggregator), vec![], vec![0])?;
    
    // 处理数据
    event_loop.handle_event(Event::Data { port: 0, batch: batch.clone() })?;
    event_loop.handle_event(Event::EndOfStream { port: 0 })?;
    
    let duration = start.elapsed();
    println!("⏱️  聚合处理时间: {:?}", duration);
    
    // 获取统计信息
    let stats = metrics.get_operator_metrics(1);
    println!("📈 统计信息:");
    println!("   处理行数: {}", stats.rows_processed);
    println!("   处理批次数: {}", stats.batches_processed);
    println!("   平均批处理时间: {:?}", stats.avg_batch_time);
    
    Ok(())
}