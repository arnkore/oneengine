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


//! 列式向量化算子示例
//! 
//! 演示完全面向列式的、全向量化极致优化的算子实现

use oneengine::execution::operators::vectorized_filter::*;
use oneengine::execution::operators::vectorized_projector::*;
use oneengine::execution::operators::vectorized_aggregator::*;
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use datafusion_common::ScalarValue;
use std::sync::Arc;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 列式向量化算子示例");
    println!("========================");
    
    // 创建测试数据
    let test_data = create_test_data()?;
    println!("✅ 创建测试数据: {} rows, {} columns", test_data.num_rows(), test_data.num_columns());
    
    // 测试向量化过滤器
    test_vectorized_filter(&test_data)?;
    
    // 测试向量化投影器
    test_vectorized_projector(&test_data)?;
    
    // 测试向量化聚合器
    test_vectorized_aggregator(&test_data)?;
    
    // 综合测试
    test_comprehensive_vectorized()?;
    
    println!("\n🎉 列式向量化算子示例完成！");
    Ok(())
}

/// 创建测试数据
fn create_test_data() -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, false),
        Field::new("salary", DataType::Float64, false),
        Field::new("department", DataType::Utf8, false),
        Field::new("bonus", DataType::Float64, false),
    ]);
    
    let id_array = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let name_array = StringArray::from(vec![
        "Alice", "Bob", "Charlie", "David", "Eve",
        "Frank", "Grace", "Henry", "Ivy", "Jack"
    ]);
    let age_array = Int32Array::from(vec![25, 30, 35, 28, 32, 27, 29, 31, 26, 33]);
    let salary_array = Float64Array::from(vec![50000.0, 60000.0, 70000.0, 55000.0, 65000.0, 
                                              52000.0, 58000.0, 62000.0, 54000.0, 68000.0]);
    let dept_array = StringArray::from(vec![
        "Engineering", "Sales", "Engineering", "Marketing", "Sales",
        "Engineering", "Marketing", "Sales", "Engineering", "Marketing"
    ]);
    let bonus_array = Float64Array::from(vec![5000.0, 6000.0, 7000.0, 5500.0, 6500.0,
                                             5200.0, 5800.0, 6200.0, 5400.0, 6800.0]);
    
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(id_array),
            Arc::new(name_array),
            Arc::new(age_array),
            Arc::new(salary_array),
            Arc::new(dept_array),
            Arc::new(bonus_array),
        ]
    )?;
    
    Ok(batch)
}

/// 测试向量化过滤器
fn test_vectorized_filter(batch: &RecordBatch) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🔍 测试向量化过滤器");
    println!("-------------------");
    
    let config = VectorizedFilterConfig::default();
    
    // 测试等值过滤
    let mut equal_filter = VectorizedFilter::new(
        config.clone(),
        FilterPredicate::Equal {
            value: ScalarValue::Utf8(Some("Engineering".to_string()))
        }
    );
    equal_filter.set_column_index(4); // department列
    
    let start = Instant::now();
    let filtered_batch = equal_filter.filter(batch)?;
    let duration = start.elapsed();
    
    println!("✅ 等值过滤: {} rows -> {} rows ({}μs)", 
             batch.num_rows(), filtered_batch.num_rows(), duration.as_micros());
    
    // 测试范围过滤
    let mut range_filter = VectorizedFilter::new(
        config.clone(),
        FilterPredicate::Between {
            min: ScalarValue::Float64(Some(55000.0)),
            max: ScalarValue::Float64(Some(65000.0))
        }
    );
    range_filter.set_column_index(3); // salary列
    
    let start = Instant::now();
    let range_filtered_batch = range_filter.filter(batch)?;
    let duration = start.elapsed();
    
    println!("✅ 范围过滤: {} rows -> {} rows ({}μs)", 
             batch.num_rows(), range_filtered_batch.num_rows(), duration.as_micros());
    
    // 测试复合过滤
    let mut complex_filter = VectorizedFilter::new(
        config.clone(),
        FilterPredicate::And {
            left: Box::new(FilterPredicate::GreaterThan {
                value: ScalarValue::Int32(Some(30))
            }),
            right: Box::new(FilterPredicate::LessThan {
                value: ScalarValue::Float64(Some(60000.0))
            })
        }
    );
    complex_filter.set_column_index(2); // age列
    
    let start = Instant::now();
    let complex_filtered_batch = complex_filter.filter(batch)?;
    let duration = start.elapsed();
    
    println!("✅ 复合过滤: {} rows -> {} rows ({}μs)", 
             batch.num_rows(), complex_filtered_batch.num_rows(), duration.as_micros());
    
    // 打印统计信息
    let stats = equal_filter.get_stats();
    println!("📊 过滤器统计: 处理行数={}, 过滤行数={}, 选择性={:.2}%", 
             stats.total_rows_processed, stats.total_rows_filtered, stats.selectivity * 100.0);
    
    Ok(())
}

/// 测试向量化投影器
fn test_vectorized_projector(batch: &RecordBatch) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n📊 测试向量化投影器");
    println!("-------------------");
    
    let config = VectorizedProjectorConfig::default();
    
    // 测试简单投影
    let expressions = vec![
        ProjectionExpression::Column { index: 0, name: "id".to_string() },
        ProjectionExpression::Column { index: 2, name: "age".to_string() },
        ProjectionExpression::Column { index: 3, name: "salary".to_string() },
    ];
    
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("age", DataType::Int32, false),
        Field::new("salary", DataType::Float64, false),
    ]));
    
    let mut projector = VectorizedProjector::new(config.clone(), expressions, output_schema);
    
    let start = Instant::now();
    let projected_batch = projector.project(batch)?;
    let duration = start.elapsed();
    
    println!("✅ 简单投影: {} columns -> {} columns ({}μs)", 
             batch.num_columns(), projected_batch.num_columns(), duration.as_micros());
    
    // 测试表达式投影
    let expressions = vec![
        ProjectionExpression::Column { index: 0, name: "id".to_string() },
        ProjectionExpression::Arithmetic {
            left: Box::new(ProjectionExpression::Column { index: 3, name: "salary".to_string() }),
            op: ArithmeticOp::Add,
            right: Box::new(ProjectionExpression::Column { index: 5, name: "bonus".to_string() })
        },
        ProjectionExpression::Arithmetic {
            left: Box::new(ProjectionExpression::Column { index: 3, name: "salary".to_string() }),
            op: ArithmeticOp::Multiply,
            right: Box::new(ProjectionExpression::Literal { value: ScalarValue::Float64(Some(1.1)) })
        },
    ];
    
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("total_compensation", DataType::Float64, false),
        Field::new("salary_with_bonus", DataType::Float64, false),
    ]));
    
    let mut expr_projector = VectorizedProjector::new(config.clone(), expressions, output_schema);
    
    let start = Instant::now();
    let expr_projected_batch = expr_projector.project(batch)?;
    let duration = start.elapsed();
    
    println!("✅ 表达式投影: {} columns -> {} columns ({}μs)", 
             batch.num_columns(), expr_projected_batch.num_columns(), duration.as_micros());
    
    // 打印统计信息
    let stats = projector.get_stats();
    println!("📊 投影器统计: 处理行数={}, 批次数={}, 平均投影时间={}μs", 
             stats.total_rows_processed, stats.total_batches_processed, 
             stats.avg_project_time.as_micros());
    
    Ok(())
}

/// 测试向量化聚合器
fn test_vectorized_aggregator(batch: &RecordBatch) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n📈 测试向量化聚合器");
    println!("-------------------");
    
    let config = VectorizedAggregatorConfig::default();
    
    // 测试按部门分组聚合
    let group_columns = vec![4]; // department列
    let agg_functions = vec![
        AggregationFunction::Count,
        AggregationFunction::Sum { column: 3 }, // salary列
        AggregationFunction::Avg { column: 3 }, // salary列
        AggregationFunction::Min { column: 3 }, // salary列
        AggregationFunction::Max { column: 3 }, // salary列
        AggregationFunction::Sum { column: 5 }, // bonus列
    ];
    
    let mut aggregator = VectorizedAggregator::new(config.clone(), group_columns, agg_functions);
    
    let start = Instant::now();
    let aggregated_batch = aggregator.aggregate(batch)?;
    let duration = start.elapsed();
    
    println!("✅ 按部门分组聚合: {} rows -> {} groups ({}μs)", 
             batch.num_rows(), aggregated_batch.num_rows(), duration.as_micros());
    
    // 测试按年龄分组聚合
    let group_columns = vec![2]; // age列
    let agg_functions = vec![
        AggregationFunction::Count,
        AggregationFunction::Avg { column: 3 }, // salary列
        AggregationFunction::Max { column: 3 }, // salary列
    ];
    
    let mut age_aggregator = VectorizedAggregator::new(config.clone(), group_columns, agg_functions);
    
    let start = Instant::now();
    let age_aggregated_batch = age_aggregator.aggregate(batch)?;
    let duration = start.elapsed();
    
    println!("✅ 按年龄分组聚合: {} rows -> {} groups ({}μs)", 
             batch.num_rows(), age_aggregated_batch.num_rows(), duration.as_micros());
    
    // 打印统计信息
    let stats = aggregator.get_stats();
    println!("📊 聚合器统计: 处理行数={}, 分组数={}, 平均聚合时间={}μs", 
             stats.total_rows_processed, stats.total_groups_created, 
             stats.avg_aggregation_time.as_micros());
    
    Ok(())
}

/// 综合测试
fn test_comprehensive_vectorized() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🎯 综合列式向量化测试");
    println!("---------------------");
    
    let start = Instant::now();
    
    // 创建测试数据
    let test_data = create_test_data()?;
    
    // 创建批量处理器
    let filter_config = VectorizedFilterConfig::default();
    let mut batch_filter = BatchFilterProcessor::new(filter_config);
    
    // 添加多个过滤器
    batch_filter.add_filter(
        FilterPredicate::GreaterThan {
            value: ScalarValue::Int32(Some(25))
        },
        2 // age列
    );
    
    batch_filter.add_filter(
        FilterPredicate::GreaterThan {
            value: ScalarValue::Float64(Some(50000.0))
        },
        3 // salary列
    );
    
    // 执行批量过滤
    let filtered_batch = batch_filter.filter_batch(&test_data)?;
    println!("✅ 批量过滤: {} rows -> {} rows", 
             test_data.num_rows(), filtered_batch.num_rows());
    
    // 创建投影器
    let projector_config = VectorizedProjectorConfig::default();
    let expressions = vec![
        ProjectionExpression::Column { index: 0, name: "id".to_string() },
        ProjectionExpression::Column { index: 1, name: "name".to_string() },
        ProjectionExpression::Arithmetic {
            left: Box::new(ProjectionExpression::Column { index: 3, name: "salary".to_string() }),
            op: ArithmeticOp::Add,
            right: Box::new(ProjectionExpression::Column { index: 5, name: "bonus".to_string() })
        },
    ];
    
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("total_compensation", DataType::Float64, false),
    ]));
    
    let mut projector = VectorizedProjector::new(projector_config, expressions, output_schema);
    let projected_batch = projector.project(&filtered_batch)?;
    println!("✅ 投影处理: {} columns -> {} columns", 
             filtered_batch.num_columns(), projected_batch.num_columns());
    
    // 创建聚合器
    let aggregator_config = VectorizedAggregatorConfig::default();
    let group_columns = vec![1]; // name列
    let agg_functions = vec![
        AggregationFunction::Count,
        AggregationFunction::Avg { column: 2 }, // total_compensation列
    ];
    
    let mut aggregator = VectorizedAggregator::new(aggregator_config, group_columns, agg_functions);
    let aggregated_batch = aggregator.aggregate(&projected_batch)?;
    println!("✅ 聚合处理: {} rows -> {} groups", 
             projected_batch.num_rows(), aggregated_batch.num_rows());
    
    let duration = start.elapsed();
    println!("✅ 综合测试完成: {}μs", duration.as_micros());
    
    Ok(())
}