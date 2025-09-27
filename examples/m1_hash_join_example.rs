//! M1里程碑示例：向量化算子演示
//! 
//! 演示向量化Filter和Projector算子功能

use oneengine::push_runtime::{event_loop::EventLoop, metrics::SimpleMetricsCollector};
use oneengine::execution::operators::vectorized_filter::{VectorizedFilter, FilterPredicate};
use oneengine::execution::operators::vectorized_projector::{VectorizedProjector, ProjectionExpression};
use arrow::record_batch::RecordBatch;
use arrow::datatypes::{Schema, Field, DataType};
use arrow::array::{Int32Array, StringArray, Float64Array};
use datafusion_common::ScalarValue;
use std::sync::Arc;
use std::time::Instant;

fn create_test_data() -> RecordBatch {
    // 创建测试数据
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, false),
        Field::new("salary", DataType::Float64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Henry", "Ivy", "Jack"])),
            Arc::new(Int32Array::from(vec![25, 30, 35, 40, 45, 50, 55, 60, 65, 70])),
            Arc::new(Float64Array::from(vec![50000.0, 60000.0, 70000.0, 80000.0, 90000.0, 100000.0, 110000.0, 120000.0, 130000.0, 140000.0])),
        ],
    ).unwrap();

    batch
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt::init();

    println!("🚀 M1里程碑：向量化算子演示");
    println!("================================");

    // 创建测试数据
    let batch = create_test_data();
    println!("📊 创建测试数据:");
    println!("   行数: {}", batch.num_rows());
    println!("   列数: {}", batch.num_columns());

    // 创建向量化Filter算子
    let filter_predicate = FilterPredicate::Gt {
        column: "age".to_string(),
        value: ScalarValue::Int32(Some(40)),
    };
    let mut filter_operator = VectorizedFilter::new(
        1,
        vec![filter_predicate],
        batch.schema(),
        true, // 启用SIMD
        true, // 启用压缩
    );

    // 创建向量化Projector算子
    let projection_expressions = vec![
        ProjectionExpression::Column("name".to_string()),
        ProjectionExpression::Column("age".to_string()),
        ProjectionExpression::Column("salary".to_string()),
    ];
    let mut projector_operator = VectorizedProjector::new(
        2,
        projection_expressions,
        batch.schema(),
        true, // 启用SIMD
        true, // 启用压缩
    );

    // 创建事件循环
    let mut event_loop = EventLoop::new();
    let metrics = Arc::new(SimpleMetricsCollector::default());

    // 注册算子
    event_loop.register_operator(1, Box::new(filter_operator), vec![], vec![0])?;
    event_loop.register_operator(2, Box::new(projector_operator), vec![0], vec![1])?;

    println!("🔧 开始向量化处理...");
    let start_time = Instant::now();

    // 处理数据
    println!("   应用过滤条件（age > 40）...");
    event_loop.handle_event(Event::Data { port: 0, batch })?;

    // 完成处理
    event_loop.handle_event(Event::EndOfStream { port: 0 })?;

    let processing_time = start_time.elapsed();
    println!("✅ 向量化处理完成，耗时: {:?}", processing_time);

    // 获取统计信息
    let filter_stats = metrics.get_operator_metrics(1);
    let projector_stats = metrics.get_operator_metrics(2);
    
    println!("📈 统计信息:");
    println!("   Filter算子:");
    println!("     处理行数: {}", filter_stats.rows_processed);
    println!("     处理批次数: {}", filter_stats.batches_processed);
    println!("     平均批处理时间: {:?}", filter_stats.avg_batch_time);
    
    println!("   Projector算子:");
    println!("     处理行数: {}", projector_stats.rows_processed);
    println!("     处理批次数: {}", projector_stats.batches_processed);
    println!("     平均批处理时间: {:?}", projector_stats.avg_batch_time);

    println!("================================");
    println!("🎉 M1里程碑演示完成！");
    Ok(())
}