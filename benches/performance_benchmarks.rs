use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId, Throughput};
use oneengine::*;
use arrow::record_batch::RecordBatch;
use arrow::datatypes::{Schema, Field, DataType};
use arrow::array::*;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;

/// Performance benchmark suite for OneEngine MPP execution engine
/// 
/// This benchmark suite tests:
/// 1. Expression engine performance
/// 2. Vectorized operator performance
/// 3. MPP execution engine performance
/// 4. Data lake reader performance
/// 5. End-to-end pipeline performance

// ============================================================================
// Helper Functions
// ============================================================================

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

/// Create multiple batches for testing
fn create_batches(num_batches: usize, batch_size: usize) -> Vec<RecordBatch> {
    (0..num_batches)
        .map(|_| create_sample_data(batch_size))
        .collect()
}

// ============================================================================
// Expression Engine Benchmarks
// ============================================================================

fn bench_expression_engine_evaluation(c: &mut Criterion) {
    let mut group = c.benchmark_group("expression_engine");
    
    for size in [1000, 10000, 100000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        
        group.bench_with_input(BenchmarkId::new("arithmetic", size), size, |b, &size| {
            let rt = Runtime::new().unwrap();
            let engine = VectorizedExpressionEngine::new(ExpressionEngineConfig::default());
            let batch = create_sample_data(size);
            
            // Create arithmetic expression: (age * 2) + salary
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
            
            b.to_async(&rt).iter(|| {
                let engine = engine.clone();
                let batch = batch.clone();
                async move {
                    engine.evaluate_expression(&add, &batch).await.unwrap()
                }
            });
        });
        
        group.bench_with_input(BenchmarkId::new("comparison", size), size, |b, &size| {
            let rt = Runtime::new().unwrap();
            let engine = VectorizedExpressionEngine::new(ExpressionEngineConfig::default());
            let batch = create_sample_data(size);
            
            // Create comparison expression: age > 30
            let age_col = Expression::ColumnRef(ColumnRef { name: "age".to_string(), index: 2 });
            let literal_30 = Expression::Literal(Literal { value: datafusion_common::ScalarValue::Int32(Some(30)) });
            
            let comparison = Expression::Comparison(ComparisonExpr {
                left: Box::new(age_col),
                right: Box::new(literal_30),
                op: ComparisonOp::GreaterThan,
            });
            
            b.to_async(&rt).iter(|| {
                let engine = engine.clone();
                let batch = batch.clone();
                async move {
                    engine.evaluate_expression(&comparison, &batch).await.unwrap()
                }
            });
        });
        
        group.bench_with_input(BenchmarkId::new("string_functions", size), size, |b, &size| {
            let rt = Runtime::new().unwrap();
            let engine = VectorizedExpressionEngine::new(ExpressionEngineConfig::default());
            let batch = create_sample_data(size);
            
            // Create string function: UPPER(name)
            let name_col = Expression::ColumnRef(ColumnRef { name: "name".to_string(), index: 1 });
            let upper_func = Expression::FunctionCall(FunctionCall {
                name: "upper".to_string(),
                args: vec![name_col],
            });
            
            b.to_async(&rt).iter(|| {
                let engine = engine.clone();
                let batch = batch.clone();
                async move {
                    engine.evaluate_expression(&upper_func, &batch).await.unwrap()
                }
            });
        });
    }
    
    group.finish();
}

// ============================================================================
// Vectorized Operator Benchmarks
// ============================================================================

fn bench_vectorized_operators(c: &mut Criterion) {
    let mut group = c.benchmark_group("vectorized_operators");
    
    for size in [1000, 10000, 100000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        
        group.bench_with_input(BenchmarkId::new("filter", size), size, |b, &size| {
            let rt = Runtime::new().unwrap();
            let batch = create_sample_data(size);
            
            // Create filter: age > 30
            let age_col = Expression::ColumnRef(ColumnRef { name: "age".to_string(), index: 2 });
            let literal_30 = Expression::Literal(Literal { value: datafusion_common::ScalarValue::Int32(Some(30)) });
            let predicate = Expression::Comparison(ComparisonExpr {
                left: Box::new(age_col),
                right: Box::new(literal_30),
                op: ComparisonOp::GreaterThan,
            });
            
            b.to_async(&rt).iter(|| {
                let batch = batch.clone();
                async move {
                    let mut filter = VectorizedFilter::new(
                        VectorizedFilterConfig::default(),
                        predicate.clone(),
                        0,
                    );
                    filter.process_batch(batch).await.unwrap()
                }
            });
        });
        
        group.bench_with_input(BenchmarkId::new("projector", size), size, |b, &size| {
            let rt = Runtime::new().unwrap();
            let batch = create_sample_data(size);
            
            // Create projection: [id, name, salary]
            let expressions = vec![
                Expression::ColumnRef(ColumnRef { name: "id".to_string(), index: 0 }),
                Expression::ColumnRef(ColumnRef { name: "name".to_string(), index: 1 }),
                Expression::ColumnRef(ColumnRef { name: "salary".to_string(), index: 3 }),
            ];
            
            let output_schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("salary", DataType::Float64, false),
            ]));
            
            b.to_async(&rt).iter(|| {
                let batch = batch.clone();
                async move {
                    let mut projector = VectorizedProjector::new(
                        VectorizedProjectorConfig::default(),
                        expressions.clone(),
                        output_schema.clone(),
                        0,
                    );
                    projector.process_batch(batch).await.unwrap()
                }
            });
        });
        
        group.bench_with_input(BenchmarkId::new("aggregator", size), size, |b, &size| {
            let rt = Runtime::new().unwrap();
            let batch = create_sample_data(size);
            
            // Create aggregation: GROUP BY department, COUNT(*), AVG(salary)
            let group_columns = vec![4]; // department column
            let agg_functions = vec!["count".to_string(), "avg".to_string()];
            
            b.to_async(&rt).iter(|| {
                let batch = batch.clone();
                async move {
                    let mut aggregator = VectorizedAggregator::new(
                        VectorizedAggregatorConfig::default(),
                        group_columns.clone(),
                        agg_functions.clone(),
                        0,
                    );
                    aggregator.process_batch(batch).await.unwrap()
                }
            });
        });
    }
    
    group.finish();
}

// ============================================================================
// MPP Engine Benchmarks
// ============================================================================

fn bench_mpp_engine(c: &mut Criterion) {
    let mut group = c.benchmark_group("mpp_engine");
    
    for size in [1000, 10000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        
        group.bench_with_input(BenchmarkId::new("mpp_scan", size), size, |b, &size| {
            let rt = Runtime::new().unwrap();
            let config = MppExecutionConfig::default();
            let context = MppContext::new(WorkerId(0), PartitionId(0));
            let mut engine = MppExecutionEngine::new(config, context);
            
            // Create scan operator
            let scan_config = MppScanConfig {
                table_path: "/test/data.parquet".to_string(),
                predicate: None,
                projection: None,
                limit: None,
            };
            
            b.to_async(&rt).iter(|| {
                let mut engine = engine.clone();
                async move {
                    let mut scan_op = MppScanOperator::new(scan_config.clone());
                    scan_op.initialize(&MppContext::new(WorkerId(0), PartitionId(0))).await.unwrap();
                    scan_op.process_batch(create_sample_data(*size), &MppContext::new(WorkerId(0), PartitionId(0))).await.unwrap()
                }
            });
        });
        
        group.bench_with_input(BenchmarkId::new("mpp_aggregation", size), size, |b, &size| {
            let rt = Runtime::new().unwrap();
            let config = MppExecutionConfig::default();
            let context = MppContext::new(WorkerId(0), PartitionId(0));
            let mut engine = MppExecutionEngine::new(config, context);
            
            // Create aggregation operator
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
            
            b.to_async(&rt).iter(|| {
                let mut engine = engine.clone();
                async move {
                    let mut agg_op = MppAggregationOperator::new(agg_config.clone());
                    agg_op.initialize(&MppContext::new(WorkerId(0), PartitionId(0))).await.unwrap();
                    agg_op.process_batch(create_sample_data(*size), &MppContext::new(WorkerId(0), PartitionId(0))).await.unwrap()
                }
            });
        });
    }
    
    group.finish();
}

// ============================================================================
// Data Lake Reader Benchmarks
// ============================================================================

fn bench_data_lake_reader(c: &mut Criterion) {
    let mut group = c.benchmark_group("data_lake_reader");
    
    for size in [1000, 10000, 100000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        
        group.bench_with_input(BenchmarkId::new("parquet_reader", size), size, |b, &size| {
            let rt = Runtime::new().unwrap();
            let mut reader = UnifiedLakeReader::new(LakeFormat::Parquet);
            
            b.to_async(&rt).iter(|| {
                let mut reader = reader.clone();
                async move {
                    reader.open_table("/test/data.parquet").await.unwrap();
                    reader.read_data().await.unwrap()
                }
            });
        });
        
        group.bench_with_input(BenchmarkId::new("iceberg_reader", size), size, |b, &size| {
            let rt = Runtime::new().unwrap();
            let mut reader = UnifiedLakeReader::new(LakeFormat::Iceberg);
            
            b.to_async(&rt).iter(|| {
                let mut reader = reader.clone();
                async move {
                    reader.open_table("/test/iceberg_table").await.unwrap();
                    reader.read_data().await.unwrap()
                }
            });
        });
    }
    
    group.finish();
}

// ============================================================================
// End-to-End Pipeline Benchmarks
// ============================================================================

fn bench_end_to_end_pipeline(c: &mut Criterion) {
    let mut group = c.benchmark_group("end_to_end_pipeline");
    
    for size in [1000, 10000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        
        group.bench_with_input(BenchmarkId::new("complex_query", size), size, |b, &size| {
            let rt = Runtime::new().unwrap();
            let config = IntegratedEngineConfig::default();
            let engine = IntegratedEngine::new(config);
            
            // Create complex stage plan: Scan -> Filter -> Project -> Sort
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("age", DataType::Int32, false),
                Field::new("salary", DataType::Float64, false),
                Field::new("department", DataType::Utf8, false),
            ]));
            
            // Filter predicate: age > 30
            let age_col = Expression::ColumnRef(ColumnRef { name: "age".to_string(), index: 2 });
            let literal_30 = Expression::Literal(Literal { value: datafusion_common::ScalarValue::Int32(Some(30)) });
            let predicate = Expression::Comparison(ComparisonExpr {
                left: Box::new(age_col),
                right: Box::new(literal_30),
                op: ComparisonOp::GreaterThan,
            });
            
            // Projection: [id, name, salary]
            let projection_expressions = vec![
                Expression::ColumnRef(ColumnRef { name: "id".to_string(), index: 0 }),
                Expression::ColumnRef(ColumnRef { name: "name".to_string(), index: 1 }),
                Expression::ColumnRef(ColumnRef { name: "salary".to_string(), index: 3 }),
            ];
            
            let stage_plan = StageExecutionPlan {
                stage_id: "test_stage".to_string(),
                operators: vec![
                    OperatorNode {
                        operator_id: "scan_1".to_string(),
                        operator_type: OperatorType::MppScan {
                            table_path: "/test/data.parquet".to_string(),
                            predicate: Some(predicate.clone()),
                        },
                        dependencies: vec![],
                        config: OperatorConfig::default(),
                        input_schemas: vec![],
                        output_schema: schema.clone(),
                    },
                    OperatorNode {
                        operator_id: "filter_1".to_string(),
                        operator_type: OperatorType::MppFilter {
                            predicate: predicate.clone(),
                        },
                        dependencies: vec![0],
                        config: OperatorConfig::default(),
                        input_schemas: vec![schema.clone()],
                        output_schema: schema.clone(),
                    },
                    OperatorNode {
                        operator_id: "project_1".to_string(),
                        operator_type: OperatorType::MppProject {
                            expressions: projection_expressions.clone(),
                        },
                        dependencies: vec![1],
                        config: OperatorConfig::default(),
                        input_schemas: vec![schema.clone()],
                        output_schema: Schema::new(vec![
                            Field::new("id", DataType::Int64, false),
                            Field::new("name", DataType::Utf8, false),
                            Field::new("salary", DataType::Float64, false),
                        ]),
                    },
                    OperatorNode {
                        operator_id: "sort_1".to_string(),
                        operator_type: OperatorType::MppSort {
                            sort_columns: vec!["salary".to_string()],
                        },
                        dependencies: vec![2],
                        config: OperatorConfig::default(),
                        input_schemas: vec![Schema::new(vec![
                            Field::new("id", DataType::Int64, false),
                            Field::new("name", DataType::Utf8, false),
                            Field::new("salary", DataType::Float64, false),
                        ])],
                        output_schema: Schema::new(vec![
                            Field::new("id", DataType::Int64, false),
                            Field::new("name", DataType::Utf8, false),
                            Field::new("salary", DataType::Float64, false),
                        ]),
                    },
                ],
            };
            
            b.to_async(&rt).iter(|| {
                let engine = engine.clone();
                let stage_plan = stage_plan.clone();
                async move {
                    engine.execute_stage(stage_plan).await.unwrap()
                }
            });
        });
    }
    
    group.finish();
}

// ============================================================================
// Memory and Concurrency Benchmarks
// ============================================================================

fn bench_memory_usage(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_usage");
    
    for size in [10000, 100000, 1000000].iter() {
        group.bench_with_input(BenchmarkId::new("batch_creation", size), size, |b, &size| {
            b.iter(|| {
                black_box(create_sample_data(size))
            });
        });
        
        group.bench_with_input(BenchmarkId::new("batch_cloning", size), size, |b, &size| {
            let batch = create_sample_data(size);
            b.iter(|| {
                black_box(batch.clone())
            });
        });
    }
    
    group.finish();
}

fn bench_concurrent_execution(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_execution");
    
    for num_tasks in [1, 2, 4, 8].iter() {
        group.bench_with_input(BenchmarkId::new("parallel_processing", num_tasks), num_tasks, |b, &num_tasks| {
            let rt = Runtime::new().unwrap();
            let config = IntegratedEngineConfig::default();
            let engine = IntegratedEngine::new(config);
            
            b.to_async(&rt).iter(|| {
                let engine = engine.clone();
                async move {
                    let mut handles = Vec::new();
                    
                    for i in 0..*num_tasks {
                        let engine = engine.clone();
                        let handle = tokio::spawn(async move {
                            // Create a simple stage plan for each task
                            let stage_plan = StageExecutionPlan {
                                stage_id: format!("task_{}", i),
                                operators: vec![
                                    OperatorNode {
                                        operator_id: format!("scan_{}", i),
                                        operator_type: OperatorType::MppScan {
                                            table_path: format!("/test/data_{}.parquet", i),
                                            predicate: None,
                                        },
                                        dependencies: vec![],
                                        config: OperatorConfig::default(),
                                        input_schemas: vec![],
                                        output_schema: Schema::new(vec![
                                            Field::new("id", DataType::Int64, false),
                                            Field::new("value", DataType::Float64, false),
                                        ]),
                                    },
                                ],
                            };
                            
                            engine.execute_stage(stage_plan).await
                        });
                        handles.push(handle);
                    }
                    
                    // Wait for all tasks to complete
                    for handle in handles {
                        handle.await.unwrap().unwrap();
                    }
                }
            });
        });
    }
    
    group.finish();
}

// ============================================================================
// Criterion Configuration
// ============================================================================

criterion_group!(
    benches,
    bench_expression_engine_evaluation,
    bench_vectorized_operators,
    bench_mpp_engine,
    bench_data_lake_reader,
    bench_end_to_end_pipeline,
    bench_memory_usage,
    bench_concurrent_execution
);

criterion_main!(benches);
