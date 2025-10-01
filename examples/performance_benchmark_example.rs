//! Performance benchmark example
//! 
//! This example demonstrates how to run performance benchmarks
//! for the OneEngine MPP execution engine.

use oneengine::*;
use oneengine::performance::*;
use oneengine::execution::mpp_engine::*;
use oneengine::execution::integrated_engine::*;
use oneengine::execution::operators::*;
use oneengine::expression::*;
use arrow::datatypes::{Schema, Field, DataType};
use arrow::array::{Int64Array, StringArray, Int32Array, Float64Array};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    println!("🚀 Starting OneEngine Performance Benchmark Example");

    // Create benchmark configuration
    let config = BenchmarkConfig {
        warmup_iterations: 2,
        measurement_iterations: 5,
        timeout: Duration::from_secs(60),
        memory_limit: Some(1024 * 1024 * 1024), // 1GB
        cpu_limit: Some(0.8), // 80% CPU usage
    };

    // Create benchmark suite
    let mut suite = BenchmarkSuite::new(config);

    // Run expression engine benchmarks
    println!("\n📊 Running Expression Engine Benchmarks...");
    
    let expr_config = BenchmarkConfig {
        warmup_iterations: 1,
        measurement_iterations: 3,
        timeout: Duration::from_secs(30),
        memory_limit: None,
        cpu_limit: None,
    };
    let mut expr_suite = BenchmarkSuite::new(expr_config);

    // Arithmetic expression benchmark
    let _ = expr_suite.run_benchmark(
        "arithmetic_expression".to_string(),
        || async {
            let engine = VectorizedExpressionEngine::new(ExpressionEngineConfig::default());
            let batch = create_sample_data(10000);
            
            // Create expression: (age * 2) + salary
            let age_col = Expression::ColumnRef(ColumnRef { name: "age".to_string(), index: 2 });
            let salary_col = Expression::ColumnRef(ColumnRef { name: "salary".to_string(), index: 3 });
            let literal_2 = Expression::Literal(Literal { value: datafusion_common::ScalarValue::Int32(Some(2)) });
            
            let multiply = Expression::Arithmetic(ArithmeticExpr {
                left: Box::new(age_col),
                right: Box::new(literal_2),
                op: ArithmeticOp::Multiply,
            });
            
            let add = Expression::Arithmetic(ArithmeticExpr {
                left: Box::new(multiply),
                right: Box::new(salary_col),
                op: ArithmeticOp::Add,
            });
            
            engine.evaluate_expression(&add, &batch).await?;
            Ok(())
        },
    ).await?;

    // Comparison expression benchmark
    let _ = expr_suite.run_benchmark(
        "comparison_expression".to_string(),
        || async {
            let engine = VectorizedExpressionEngine::new(ExpressionEngineConfig::default());
            let batch = create_sample_data(10000);
            
            // Create expression: age > 30
            let age_col = Expression::ColumnRef(ColumnRef { name: "age".to_string(), index: 2 });
            let literal_30 = Expression::Literal(Literal { value: datafusion_common::ScalarValue::Int32(Some(30)) });
            
            let comparison = Expression::Comparison(ComparisonExpr {
                left: Box::new(age_col),
                right: Box::new(literal_30),
                op: ComparisonOp::GreaterThan,
            });
            
            engine.evaluate_expression(&comparison, &batch).await?;
            Ok(())
        },
    ).await?;

    // String function benchmark
    let _ = expr_suite.run_benchmark(
        "string_function".to_string(),
        || async {
            let engine = VectorizedExpressionEngine::new(ExpressionEngineConfig::default());
            let batch = create_sample_data(10000);
            
            // Create expression: UPPER(name)
            let name_col = Expression::ColumnRef(ColumnRef { name: "name".to_string(), index: 1 });
            let upper_func = Expression::FunctionCall(FunctionCall {
                name: "upper".to_string(),
                args: vec![name_col],
            });
            
            engine.evaluate_expression(&upper_func, &batch).await?;
            Ok(())
        },
    ).await?;

    // Run MPP engine benchmarks
    println!("\n🏗️ Running MPP Engine Benchmarks...");
    
    let mpp_config = BenchmarkConfig {
        warmup_iterations: 1,
        measurement_iterations: 3,
        timeout: Duration::from_secs(60),
        memory_limit: Some(512 * 1024 * 1024), // 512MB
        cpu_limit: None,
    };
    let mut mpp_suite = BenchmarkSuite::new(mpp_config);

    // MPP Scan benchmark
    let _ = mpp_suite.run_benchmark(
        "mpp_scan".to_string(),
        || async {
            let config = MppExecutionConfig {
                worker_config: WorkerConfig {
                    worker_id: "worker_0".to_string(),
                    all_workers: vec!["worker_0".to_string()],
                },
                memory_config: MemoryConfig {
                    operator_memory_limit: 1024 * 1024 * 1024,
                    spill_threshold: 0.8,
                },
            };
            let engine_id = uuid::Uuid::new_v4();
            let mut engine = MppExecutionEngine::new(engine_id, config);
            
            let scan_config = MppScanConfig {
                table_path: "/test/data.parquet".to_string(),
                predicate: None,
                projection: None,
                limit: None,
            };
            
            let mut scan_op = MppScanOperator::new(scan_config);
            let context = MppContext {
                worker_id: "worker_0".to_string(),
                task_id: uuid::Uuid::new_v4(),
                worker_ids: vec!["worker_0".to_string()],
                exchange_channels: std::collections::HashMap::new(),
                partition_info: PartitionInfo {
                    total_partitions: 1,
                    local_partitions: vec![0],
                    partition_distribution: std::collections::HashMap::new(),
                },
                config: MppConfig {
                    batch_size: 8192,
                    memory_limit: 1024 * 1024 * 1024,
                    network_timeout: std::time::Duration::from_secs(30),
                    retry_config: RetryConfig::default(),
                    compression_enabled: true,
                    parallelism: num_cpus::get(),
                },
            };
            scan_op.initialize(&context).await?;
            scan_op.process_batch(create_sample_data(5000), &context).await?;
            Ok(())
        },
    ).await?;

    // MPP Aggregation benchmark
    let _ = mpp_suite.run_benchmark(
        "mpp_aggregation".to_string(),
        || async {
            let config = MppExecutionConfig {
                worker_config: WorkerConfig {
                    worker_id: "worker_0".to_string(),
                    all_workers: vec!["worker_0".to_string()],
                },
                memory_config: MemoryConfig {
                    operator_memory_limit: 1024 * 1024 * 1024,
                    spill_threshold: 0.8,
                },
            };
            let engine_id = uuid::Uuid::new_v4();
            let mut engine = MppExecutionEngine::new(engine_id, config);
            
            let agg_config = MppAggregationConfig {
                group_by_columns: vec![4], // department
                aggregate_functions: vec![
                    AggregateFunction {
                        name: "count".to_string(),
                        args: vec![],
                        return_type: DataType::Int64,
                    },
                    AggregateFunction {
                        name: "avg".to_string(),
                        args: vec![3], // salary column
                        return_type: DataType::Float64,
                    },
                ],
                memory_limit: 1024 * 1024 * 1024, // 1GB
            };
            
            let mut agg_op = MppAggregationOperator::new(agg_config);
            let context = MppContext {
                worker_id: "worker_0".to_string(),
                task_id: uuid::Uuid::new_v4(),
                worker_ids: vec!["worker_0".to_string()],
                exchange_channels: std::collections::HashMap::new(),
                partition_info: PartitionInfo {
                    total_partitions: 1,
                    local_partitions: vec![0],
                    partition_distribution: std::collections::HashMap::new(),
                },
                config: MppConfig {
                    batch_size: 8192,
                    memory_limit: 1024 * 1024 * 1024,
                    network_timeout: std::time::Duration::from_secs(30),
                    retry_config: RetryConfig::default(),
                    compression_enabled: true,
                    parallelism: num_cpus::get(),
                },
            };
            agg_op.initialize(&context).await?;
            agg_op.process_batch(create_sample_data(5000), &context).await?;
            Ok(())
        },
    ).await?;

    // Run end-to-end pipeline benchmark
    println!("\n🔄 Running End-to-End Pipeline Benchmark...");
    
    let pipeline_config = BenchmarkConfig {
        warmup_iterations: 1,
        measurement_iterations: 2,
        timeout: Duration::from_secs(120),
        memory_limit: Some(1024 * 1024 * 1024), // 1GB
        cpu_limit: None,
    };
    let mut pipeline_suite = BenchmarkSuite::new(pipeline_config);

    let _ = pipeline_suite.run_benchmark(
        "end_to_end_pipeline".to_string(),
        || async {
            // Simple pipeline benchmark - just process some data
            let batch = create_sample_data(10000);
            
            // Simulate some processing
            let id_array = batch.column(0);
            let name_array = batch.column(1);
            let age_array = batch.column(2);
            let salary_array = batch.column(3);
            let department_array = batch.column(4);
            
            // Simple filtering: count records where age > 30
            let mut count = 0;
            if let Some(age_col) = age_array.as_any().downcast_ref::<Int32Array>() {
                for i in 0..age_col.len() {
                    if let Some(age) = age_col.value(i) {
                        if age > 30 {
                            count += 1;
                        }
                    }
                }
            }
            
            // Simple aggregation: sum of salaries
            let mut total_salary = 0.0;
            if let Some(salary_col) = salary_array.as_any().downcast_ref::<Float64Array>() {
                for i in 0..salary_col.len() {
                    if let Some(salary) = salary_col.value(i) {
                        total_salary += salary;
                    }
                }
            }
            
            // Simulate some processing time
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            
            Ok(())
        },
    ).await?;

    // Generate and display reports
    println!("\n📈 Expression Engine Benchmark Results:");
    println!("{}", expr_suite.generate_report());
    
    println!("\n📈 MPP Engine Benchmark Results:");
    println!("{}", mpp_suite.generate_report());
    
    println!("\n📈 End-to-End Pipeline Benchmark Results:");
    println!("{}", pipeline_suite.generate_report());

    // Export results
    println!("\n💾 Exporting benchmark results...");
    
    let expr_json = expr_suite.export_json()?;
    std::fs::write("expr_benchmark_results.json", expr_json)?;
    println!("✅ Expression engine results exported to expr_benchmark_results.json");
    
    let mpp_json = mpp_suite.export_json()?;
    std::fs::write("mpp_benchmark_results.json", mpp_json)?;
    println!("✅ MPP engine results exported to mpp_benchmark_results.json");
    
    let pipeline_json = pipeline_suite.export_json()?;
    std::fs::write("pipeline_benchmark_results.json", pipeline_json)?;
    println!("✅ Pipeline results exported to pipeline_benchmark_results.json");

    println!("\n🎉 Performance benchmark example completed successfully!");

    Ok(())
}

/// Create sample data for benchmarking
fn create_sample_data(num_rows: usize) -> RecordBatch {
    let id_array = Int64Array::from((0..num_rows as i64).collect::<Vec<_>>());
    let name_array = StringArray::from(
        (0..num_rows)
            .map(|i| format!("user_{}", i))
            .collect::<Vec<_>>()
    );
    let age_array = Int32Array::from(
        (0..num_rows)
            .map(|i| (20 + (i % 50)) as i32)
            .collect::<Vec<_>>()
    );
    let salary_array = Float64Array::from(
        (0..num_rows)
            .map(|i| 50000.0 + (i as f64 * 1000.0))
            .collect::<Vec<_>>()
    );
    let department_array = StringArray::from(
        (0..num_rows)
            .map(|i| match i % 4 {
                0 => "Engineering",
                1 => "Sales", 
                2 => "Marketing",
                _ => "HR",
            })
            .collect::<Vec<_>>()
    );

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, false),
        Field::new("salary", DataType::Float64, false),
        Field::new("department", DataType::Utf8, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(id_array),
            Arc::new(name_array),
            Arc::new(age_array),
            Arc::new(salary_array),
            Arc::new(department_array),
        ],
    ).unwrap()
}

