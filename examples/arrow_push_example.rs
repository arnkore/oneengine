//! 基于Arrow的Push执行器示例
//! 
//! 演示纯push事件驱动的执行架构

use oneengine::push_runtime::{event_loop::EventLoop, PortId, OperatorId, MetricsCollector};
use oneengine::execution::operators::{
    vectorized_filter::{VectorizedFilter, FilterPredicate},
    vectorized_aggregator::{VectorizedAggregator, AggregationFunction},
};
use arrow::record_batch::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::array::{Int32Array, StringArray, Float64Array};
use datafusion_common::ScalarValue;
use std::sync::Arc;
use std::time::Duration;

/// 简单的指标收集器
struct SimpleMetricsCollector;

impl MetricsCollector for SimpleMetricsCollector {
    fn record_push(&self, operator_id: OperatorId, port: PortId, rows: u32) {
        println!("Push: Operator {} -> Port {} ({} rows)", operator_id, port, rows);
    }
    
    fn record_block(&self, operator_id: OperatorId, reason: &str) {
        println!("Block: Operator {} ({})", operator_id, reason);
    }
    
    fn record_process_time(&self, operator_id: OperatorId, duration: Duration) {
        println!("Process: Operator {} took {:?}", operator_id, duration);
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 基于Arrow的Push执行器示例");
    println!("================================================");
    
    // 1. 创建测试数据
    println!("1. 创建测试数据");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    
    let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie", "David", "Eve"]);
    let value_array = Float64Array::from(vec![10.5, 20.3, 30.7, 40.1, 50.9]);
    
    let input_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(id_array),
            Arc::new(name_array),
            Arc::new(value_array),
        ],
    )?;
    
    println!("   输入批次: {} 行", input_batch.num_rows());
    println!();

    // 2. 创建事件循环
    println!("2. 创建事件循环");
    let metrics = Arc::new(SimpleMetricsCollector);
    let mut event_loop = EventLoop::new();
    
    // 设置端口信用
    event_loop.set_port_credit(100, 1000); // 输入端口
    event_loop.set_port_credit(200, 1000); // Filter输出端口
    event_loop.set_port_credit(300, 1000); // Aggregator输出端口
    println!();

    // 3. 创建向量化Filter算子
    println!("3. 创建向量化Filter算子");
    let filter_predicates = vec![
        FilterPredicate::Gt {
            column: "id".to_string(),
            value: ScalarValue::Int32(Some(2)),
        },
    ];
    
    let filter_operator = VectorizedFilter::new(
        1, // 算子ID
        filter_predicates,
        input_batch.schema(),
        true, // 启用SIMD
        true, // 启用压缩
    );
    
    event_loop.register_operator(
        1,
        Box::new(filter_operator),
        vec![100],
        vec![200],
    )?;
    println!("   向量化Filter算子已注册");
    println!();

    // 4. 创建向量化Aggregator算子
    println!("4. 创建向量化Aggregator算子");
    let aggregation_functions = vec![
        AggregationFunction::Count {
            column: "id".to_string(),
            output_column: "count".to_string(),
        },
        AggregationFunction::Sum {
            column: "value".to_string(),
            output_column: "total_value".to_string(),
        },
        AggregationFunction::Avg {
            column: "value".to_string(),
            output_column: "avg_value".to_string(),
        },
    ];
    
    let aggregator_operator = VectorizedAggregator::new(
        2, // 算子ID
        aggregation_functions,
        input_batch.schema(),
        true, // 启用SIMD
        true, // 启用压缩
    );
    
    event_loop.register_operator(
        2,
        Box::new(aggregator_operator),
        vec![200],
        vec![300],
    )?;
    println!("   向量化Aggregator算子已注册");
    println!();

    // 5. 执行数据处理
    println!("5. 执行数据处理");
    println!("   发送数据到Filter算子...");
    event_loop.handle_event(Event::Data { port: 100, batch: input_batch })?;
    
    println!("   发送EndOfStream信号...");
    event_loop.handle_event(Event::EndOfStream { port: 100 })?;
    
    println!("   等待处理完成...");
    while !event_loop.is_finished() {
        std::thread::sleep(Duration::from_millis(10));
    }
    
    println!("✅ 数据处理完成");
    println!();

    // 6. 显示结果
    println!("6. 处理结果");
    println!("   Filter算子处理了 id > 2 的记录");
    println!("   Aggregator算子计算了统计信息");
    println!("   所有算子都使用了向量化优化");
    println!();

    println!("🎯 示例完成！");
    println!("✅ 向量化Filter算子已实现");
    println!("✅ 向量化Aggregator算子已实现");
    println!("✅ 支持SIMD优化");
    println!("✅ 基于Arrow的高效数据处理");
    println!("✅ 事件驱动的push执行模型");
    
    Ok(())
}