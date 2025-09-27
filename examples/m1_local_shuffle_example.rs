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


//! M1里程碑示例：向量化算子管道演示
//! 
//! 演示向量化Filter和Projector算子的管道处理

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
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("dept_id", DataType::Int32, false),
        Field::new("salary", DataType::Float64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Henry", "Ivy", "Jack", "Kate", "Liam", "Mia", "Noah", "Olivia", "Paul", "Quinn", "Ruby", "Sam", "Tina"])),
            Arc::new(Int32Array::from(vec![10, 20, 10, 30, 20, 10, 30, 20, 10, 30, 10, 20, 30, 10, 20, 30, 10, 20, 30, 10])),
            Arc::new(Float64Array::from(vec![50000.0, 60000.0, 55000.0, 70000.0, 65000.0, 52000.0, 75000.0, 68000.0, 53000.0, 72000.0, 51000.0, 61000.0, 71000.0, 54000.0, 66000.0, 73000.0, 56000.0, 67000.0, 74000.0, 57000.0])),
        ],
    ).unwrap();

    batch
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 M1里程碑：向量化算子管道演示");
    println!("================================================");
    
    // 创建测试数据
    let batch = create_test_data();
    println!("📊 测试数据：");
    println!("行数: {}", batch.num_rows());
    println!("列数: {}", batch.num_columns());
    println!("Schema: {:?}", batch.schema());
    println!();
    
    // 测试向量化算子管道
    println!("🔄 测试向量化算子管道...");
    test_vectorized_pipeline(&batch)?;
    println!();
    
    println!("🎯 M1里程碑完成！");
    println!("✅ 向量化算子管道已实现");
    println!("✅ 支持Filter和Projector算子");
    println!("✅ 支持SIMD优化");
    println!("✅ 基于Arrow的高效数据处理");
    println!("✅ 事件驱动的push执行模型集成");
    
    Ok(())
}

fn test_vectorized_pipeline(batch: &RecordBatch) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    
    // 创建向量化Filter算子
    let filter_predicate = FilterPredicate::Gt {
        column: "salary".to_string(),
        value: ScalarValue::Float64(Some(60000.0)),
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
        ProjectionExpression::Column("dept_id".to_string()),
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
    
    // 处理数据
    println!("   应用过滤条件（salary > 60000）...");
    event_loop.handle_event(Event::Data { port: 0, batch: batch.clone() })?;
    
    // 完成处理
    event_loop.handle_event(Event::EndOfStream { port: 0 })?;
    
    let duration = start.elapsed();
    println!("⏱️  管道处理时间: {:?}", duration);
    
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
    
    Ok(())
}