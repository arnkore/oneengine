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


//! 完整的向量化集成示例
//! 
//! 演示向量化算子与湖仓、driver、pipeline调度体系、task调度的完整集成

use oneengine::executor::vectorized_driver::*;
use oneengine::execution::operators::vectorized_filter::*;
use oneengine::execution::operators::vectorized_projector::*;
use oneengine::execution::operators::vectorized_aggregator::*;
use oneengine::execution::operators::vectorized_scan_operator::*;
use oneengine::datalake::unified_lake_reader::*;
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use datafusion_common::ScalarValue;
use std::sync::Arc;
use std::time::Instant;
use tokio;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 完整的向量化集成示例");
    println!("===============================================");
    
    // 创建向量化驱动配置
    let driver_config = VectorizedDriverConfig {
        max_workers: 4,
        memory_limit: 1024 * 1024 * 1024, // 1GB
        batch_size: 8192,
        enable_vectorization: true,
        enable_simd: true,
        enable_compression: true,
        enable_prefetch: true,
        enable_numa_aware: true,
        enable_adaptive_batching: true,
    };
    
    // 创建向量化驱动
    let mut driver = VectorizedDriver::new(driver_config);
    
    // 启动驱动
    driver.start().await?;
    println!("✅ 向量化驱动启动完成");
    
    // 测试1: 简单的Scan -> Filter -> Project查询
    test_scan_filter_project_query(&mut driver).await?;
    
    // 测试2: 复杂的Scan -> Filter -> Project -> Aggregate查询
    test_complex_aggregation_query(&mut driver).await?;
    
    // 测试3: 湖仓集成测试
    test_data_lake_integration().await?;
    
    // 测试4: 性能基准测试
    test_performance_benchmark(&mut driver).await?;
    
    // 停止驱动
    driver.stop().await?;
    println!("✅ 向量化驱动停止完成");
    
    // 打印最终统计信息
    print_final_stats(&driver);
    
    println!("\n🎉 完整的向量化集成示例完成！");
    Ok(())
}

/// 测试简单的Scan -> Filter -> Project查询
async fn test_scan_filter_project_query(driver: &mut VectorizedDriver) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n📊 测试1: Scan -> Filter -> Project查询");
    println!("----------------------------------------");
    
    // 创建查询计划
    let filter_predicate = FilterPredicate::GreaterThan {
        value: ScalarValue::Int32(Some(25))
    };
    
    let projection_expressions = vec![
        ProjectionExpression::Column { index: 0, name: "id".to_string() },
        ProjectionExpression::Column { index: 1, name: "name".to_string() },
        ProjectionExpression::Column { index: 2, name: "age".to_string() },
    ];
    
    let query_plan = driver.create_simple_query_plan(
        "test_data.parquet".to_string(),
        Some(filter_predicate),
        Some(projection_expressions),
        None,
    );
    
    println!("✅ 查询计划创建完成: {} 个算子", query_plan.operators.len());
    
    // 执行查询
    let start = Instant::now();
    let result = driver.execute_query(query_plan).await?;
    let duration = start.elapsed();
    
    println!("✅ 查询执行完成: {} 批次 ({}μs)", result.len(), duration.as_micros());
    
    Ok(())
}

/// 测试复杂的聚合查询
async fn test_complex_aggregation_query(driver: &mut VectorizedDriver) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n📈 测试2: Scan -> Filter -> Project -> Aggregate查询");
    println!("--------------------------------------------------");
    
    // 创建查询计划
    let filter_predicate = FilterPredicate::GreaterThan {
        value: ScalarValue::Int32(Some(25))
    };
    
    let projection_expressions = vec![
        ProjectionExpression::Column { index: 0, name: "id".to_string() },
        ProjectionExpression::Column { index: 2, name: "age".to_string() },
        ProjectionExpression::Column { index: 3, name: "salary".to_string() },
    ];
    
    let aggregation = (
        vec![2], // 按age分组
        vec![
            AggregationFunction::Count { column_index: 0 },
            AggregationFunction::Sum { column_index: 3 },
            AggregationFunction::Avg { column_index: 3 },
            AggregationFunction::Max { column_index: 3 },
            AggregationFunction::Min { column_index: 3 },
        ]
    );
    
    let query_plan = driver.create_simple_query_plan(
        "test_data.parquet".to_string(),
        Some(filter_predicate),
        Some(projection_expressions),
        Some(aggregation),
    );
    
    println!("✅ 复杂查询计划创建完成: {} 个算子", query_plan.operators.len());
    
    // 执行查询
    let start = Instant::now();
    let result = driver.execute_query(query_plan).await?;
    let duration = start.elapsed();
    
    println!("✅ 复杂查询执行完成: {} 批次 ({}μs)", result.len(), duration.as_micros());
    
    Ok(())
}

/// 测试湖仓集成
async fn test_data_lake_integration() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🏔️ 测试3: 湖仓集成");
    println!("------------------");
    
    // 创建数据湖读取器配置
    let config = DataLakeReaderConfig {
        enable_page_index: true,
        enable_predicate_pushdown: true,
        enable_dictionary_retention: true,
        enable_lazy_materialization: true,
        max_rowgroups: Some(1000),
        batch_size: 8192,
        predicates: vec![
            PredicateFilter::GreaterThan {
                column: "age".to_string(),
                value: "25".to_string(),
            },
            PredicateFilter::Equal {
                column: "department".to_string(),
                value: "Engineering".to_string(),
            },
        ],
    };
    
    // 创建数据湖读取器
    let reader = DataLakeReader::new(config);
    
    // 测试各种剪枝功能
    let pruning_tests = vec![
        ("分区剪枝", test_partition_pruning(&reader)),
        ("分桶剪枝", test_bucket_pruning(&reader)),
        ("ZoneMap剪枝", test_zone_map_pruning(&reader)),
        ("页索引剪枝", test_page_index_pruning(&reader)),
        ("谓词剪枝", test_predicate_pruning(&reader)),
    ];
    
    for (name, test_fn) in pruning_tests {
        let start = Instant::now();
        let result = test_fn?;
        let duration = start.elapsed();
        println!("✅ {}: {} rows ({}μs)", name, result, duration.as_micros());
    }
    
    Ok(())
}

/// 测试性能基准
async fn test_performance_benchmark(driver: &mut VectorizedDriver) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n⚡ 测试4: 性能基准测试");
    println!("----------------------");
    
    // 创建测试数据
    let test_data = create_large_test_data()?;
    println!("✅ 创建测试数据: {} rows, {} columns", test_data.num_rows(), test_data.num_columns());
    
    // 测试向量化过滤器性能
    test_vectorized_filter_performance(&test_data)?;
    
    // 测试向量化投影器性能
    test_vectorized_projector_performance(&test_data)?;
    
    // 测试向量化聚合器性能
    test_vectorized_aggregator_performance(&test_data)?;
    
    // 测试端到端性能
    test_end_to_end_performance(driver, &test_data).await?;
    
    Ok(())
}

/// 创建大型测试数据
fn create_large_test_data() -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, false),
        Field::new("salary", DataType::Float64, false),
        Field::new("department", DataType::Utf8, false),
        Field::new("bonus", DataType::Float64, false),
    ]);
    
    let size = 100000; // 10万行数据
    
    let id_array = Int32Array::from((0..size).collect::<Vec<_>>());
    let name_array = StringArray::from(
        (0..size).map(|i| format!("User_{}", i)).collect::<Vec<_>>()
    );
    let age_array = Int32Array::from(
        (0..size).map(|i| 20 + (i % 50)).collect::<Vec<_>>()
    );
    let salary_array = Float64Array::from(
        (0..size).map(|i| 30000.0 + (i as f64 * 100.0)).collect::<Vec<_>>()
    );
    let dept_array = StringArray::from(
        (0..size).map(|i| {
            let depts = ["Engineering", "Sales", "Marketing", "HR"];
            depts[i % depts.len()]
        }).collect::<Vec<_>>()
    );
    let bonus_array = Float64Array::from(
        (0..size).map(|i| 1000.0 + (i as f64 * 10.0)).collect::<Vec<_>>()
    );
    
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

/// 测试向量化过滤器性能
fn test_vectorized_filter_performance(batch: &RecordBatch) -> Result<(), Box<dyn std::error::Error>> {
    let config = VectorizedFilterConfig::default();
    let predicate = FilterPredicate::GreaterThan {
        value: ScalarValue::Int32(Some(25))
    };
    
    let mut filter = VectorizedFilter::new(
        config,
        predicate,
        1,
        vec![0],
        vec![1],
        "test_filter".to_string(),
    );
    filter.set_column_index(2); // age列
    
    let iterations = 100;
    let start = Instant::now();
    
    for _ in 0..iterations {
        let _ = filter.filter(batch)?;
    }
    
    let duration = start.elapsed();
    let avg_time = duration.as_nanos() as f64 / iterations as f64;
    
    println!("✅ 向量化过滤器性能: {:.2}μs/批次 ({} 次迭代)", 
             avg_time / 1000.0, iterations);
    
    Ok(())
}

/// 测试向量化投影器性能
fn test_vectorized_projector_performance(batch: &RecordBatch) -> Result<(), Box<dyn std::error::Error>> {
    let config = VectorizedProjectorConfig::default();
    let expressions = vec![
        ProjectionExpression::Column { index: 0, name: "id".to_string() },
        ProjectionExpression::Column { index: 1, name: "name".to_string() },
        ProjectionExpression::Column { index: 2, name: "age".to_string() },
    ];
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, false),
    ]));
    
    let mut projector = VectorizedProjector::new(
        config,
        expressions,
        output_schema,
        1,
        vec![0],
        vec![1],
        "test_projector".to_string(),
    );
    
    let iterations = 100;
    let start = Instant::now();
    
    for _ in 0..iterations {
        let _ = projector.project(batch)?;
    }
    
    let duration = start.elapsed();
    let avg_time = duration.as_nanos() as f64 / iterations as f64;
    
    println!("✅ 向量化投影器性能: {:.2}μs/批次 ({} 次迭代)", 
             avg_time / 1000.0, iterations);
    
    Ok(())
}

/// 测试向量化聚合器性能
fn test_vectorized_aggregator_performance(batch: &RecordBatch) -> Result<(), Box<dyn std::error::Error>> {
    let config = VectorizedAggregatorConfig::default();
    let group_columns = vec![2]; // 按age分组
    let agg_functions = vec![
        AggregationFunction::Count { column_index: 0 },
        AggregationFunction::Sum { column_index: 3 },
        AggregationFunction::Avg { column_index: 3 },
    ];
    
    let mut aggregator = VectorizedAggregator::new(
        config,
        group_columns,
        agg_functions,
        1,
        vec![0],
        vec![1],
        "test_aggregator".to_string(),
    );
    
    let iterations = 10; // 聚合器测试次数较少
    let start = Instant::now();
    
    for _ in 0..iterations {
        let _ = aggregator.aggregate(batch)?;
    }
    
    let duration = start.elapsed();
    let avg_time = duration.as_nanos() as f64 / iterations as f64;
    
    println!("✅ 向量化聚合器性能: {:.2}μs/批次 ({} 次迭代)", 
             avg_time / 1000.0, iterations);
    
    Ok(())
}

/// 测试端到端性能
async fn test_end_to_end_performance(driver: &mut VectorizedDriver, batch: &RecordBatch) -> Result<(), Box<dyn std::error::Error>> {
    // 创建端到端查询计划
    let filter_predicate = FilterPredicate::GreaterThan {
        value: ScalarValue::Int32(Some(25))
    };
    
    let projection_expressions = vec![
        ProjectionExpression::Column { index: 0, name: "id".to_string() },
        ProjectionExpression::Column { index: 2, name: "age".to_string() },
        ProjectionExpression::Column { index: 3, name: "salary".to_string() },
    ];
    
    let aggregation = (
        vec![2], // 按age分组
        vec![
            AggregationFunction::Count { column_index: 0 },
            AggregationFunction::Sum { column_index: 3 },
            AggregationFunction::Avg { column_index: 3 },
        ]
    );
    
    let query_plan = driver.create_simple_query_plan(
        "test_data.parquet".to_string(),
        Some(filter_predicate),
        Some(projection_expressions),
        Some(aggregation),
    );
    
    let iterations = 10;
    let start = Instant::now();
    
    for _ in 0..iterations {
        let _ = driver.execute_query(query_plan.clone()).await?;
    }
    
    let duration = start.elapsed();
    let avg_time = duration.as_nanos() as f64 / iterations as f64;
    
    println!("✅ 端到端性能: {:.2}μs/查询 ({} 次迭代)", 
             avg_time / 1000.0, iterations);
    
    Ok(())
}

/// 打印最终统计信息
fn print_final_stats(driver: &VectorizedDriver) {
    println!("\n📊 最终统计信息");
    println!("================");
    
    let stats = driver.get_stats();
    println!("总查询数: {}", stats.total_queries_executed);
    println!("总执行时间: {}μs", stats.total_execution_time.as_micros());
    println!("平均执行时间: {}μs", stats.avg_execution_time.as_micros());
    println!("内存使用峰值: {}MB", stats.peak_memory_usage / 1024 / 1024);
    println!("向量化加速比: {:.2}x", stats.vectorization_speedup);
    println!("SIMD利用率: {:.2}%", stats.simd_utilization * 100.0);
    println!("缓存命中率: {:.2}%", stats.cache_hit_rate * 100.0);
}

// 辅助函数
fn test_partition_pruning(reader: &DataLakeReader) -> Result<u64, Box<dyn std::error::Error>> {
    let pruning_info = PartitionPruningInfo {
        partition_columns: vec!["department".to_string()],
        partition_values: vec!["Engineering".to_string()],
    };
    Ok(reader.test_partition_pruning(&pruning_info)?)
}

fn test_bucket_pruning(reader: &DataLakeReader) -> Result<u64, Box<dyn std::error::Error>> {
    let bucket_info = BucketPruningInfo {
        bucket_columns: vec!["id".to_string()],
        bucket_count: 10,
        bucket_values: vec![1, 3, 5, 7, 9],
    };
    Ok(reader.test_bucket_pruning(&bucket_info)?)
}

fn test_zone_map_pruning(reader: &DataLakeReader) -> Result<u64, Box<dyn std::error::Error>> {
    let zone_map_info = ZoneMapPruningInfo {
        column: "age".to_string(),
        min_value: "25".to_string(),
        max_value: "35".to_string(),
    };
    Ok(reader.test_zone_map_pruning(&zone_map_info)?)
}

fn test_page_index_pruning(reader: &DataLakeReader) -> Result<u64, Box<dyn std::error::Error>> {
    let page_index_info = PageIndexPruningInfo {
        column: "salary".to_string(),
        min_value: "50000".to_string(),
        max_value: "70000".to_string(),
    };
    Ok(reader.test_page_index_pruning(&page_index_info)?)
}

fn test_predicate_pruning(reader: &DataLakeReader) -> Result<u64, Box<dyn std::error::Error>> {
    let predicate_info = PredicatePruningInfo {
        predicates: vec![
            PredicateFilter::GreaterThan {
                column: "age".to_string(),
                value: "25".to_string(),
            },
        ],
    };
    Ok(reader.test_predicate_pruning(&predicate_info)?)
}
