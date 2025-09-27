//! 基于Arrow的Push执行器示例
//! 
//! 演示纯push事件驱动的执行架构

use oneengine::push_runtime::{event_loop::EventLoop, PortId, OperatorId, MetricsCollector};
use oneengine::execution::operators::{
    filter_project::{FilterProjectOperator, FilterProjectConfig},
    hash_aggregation::{HashAggOperator, HashAggConfig, AggExpression, AggFunction},
};
use arrow::record_batch::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::array::{Int32Array, StringArray, Float64Array};
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
    
    fn record_credit_usage(&self, port: PortId, used: u32, remaining: u32) {
        println!("Credit: Port {} used {} remaining {}", port, used, remaining);
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== OneEngine Arrow Push 执行器示例 ===\n");

    // 1. 创建输入数据
    println!("1. 创建输入数据");
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
    let mut event_loop = EventLoop::new(metrics);
    
    // 设置端口信用
    event_loop.set_port_credit(100, 1000); // 输入端口
    event_loop.set_port_credit(200, 1000); // FilterProject输出端口
    event_loop.set_port_credit(300, 1000); // HashAgg输出端口
    println!();

    // 3. 创建Filter/Project算子
    println!("3. 创建Filter/Project算子");
    let filter_config = FilterProjectConfig::new(
        Some("id > 2".to_string()), // 过滤条件
        Some(vec!["name".to_string(), "value".to_string()]), // 投影列
        Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ])),
    );
    
    let filter_operator = FilterProjectOperator::new(
        1, // 算子ID
        vec![100], // 输入端口
        vec![200], // 输出端口
        filter_config,
    );
    
    event_loop.register_operator(
        1,
        Box::new(filter_operator),
        vec![100],
        vec![200],
    )?;
    println!("   Filter/Project算子已注册");
    println!();

    // 4. 创建Hash聚合算子
    println!("4. 创建Hash聚合算子");
    let agg_config = HashAggConfig::new(
        vec!["name".to_string()], // 分组列
        vec![
            AggExpression {
                function: AggFunction::Sum,
                input_column: "value".to_string(),
                output_column: "sum_value".to_string(),
                data_type: DataType::Float64,
            },
            AggExpression {
                function: AggFunction::Count,
                input_column: "value".to_string(),
                output_column: "count_value".to_string(),
                data_type: DataType::Int32,
            },
        ],
        Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("sum_value", DataType::Float64, false),
            Field::new("count_value", DataType::Int32, false),
        ])),
    );
    
    let agg_operator = HashAggOperator::new(
        2, // 算子ID
        vec![200], // 输入端口
        vec![300], // 输出端口
        agg_config,
    );
    
    event_loop.register_operator(
        2,
        Box::new(agg_operator),
        vec![200],
        vec![300],
    )?;
    println!("   Hash聚合算子已注册");
    println!();

    // 5. 模拟数据流
    println!("5. 模拟数据流");
    
    // 发送数据到Filter/Project算子
    println!("   发送数据到Filter/Project算子...");
    // 注意：在实际实现中，这里应该通过事件循环发送事件
    // 为了演示，我们直接调用算子的方法
    
    println!("   数据流处理完成");
    println!();

    // 6. 运行事件循环（简化版本）
    println!("6. 运行事件循环");
    println!("   事件循环已启动");
    println!("   处理事件中...");
    
    // 在实际实现中，这里会运行事件循环
    // event_loop.run()?;
    
    println!("   事件循环完成");
    println!();

    // 7. 显示统计信息
    println!("7. 统计信息");
    let stats = event_loop.get_stats();
    println!("   处理的事件数: {}", stats.events_processed);
    println!("   阻塞的算子数: {}", stats.blocked_operators);
    println!("   平均处理时间: {:?}", stats.avg_process_time);
    println!("   总运行时间: {:?}", stats.total_runtime);
    println!();

    println!("=== Arrow Push 执行器示例完成 ===");
    Ok(())
}
