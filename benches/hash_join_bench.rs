//! HashJoin算子基准测试

use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};
use oneengine::execution::operators::hash_join::{HashJoinOperator, HashJoinConfig, JoinType, JoinMode};
use arrow::record_batch::RecordBatch;
use arrow::datatypes::{Schema, Field, DataType};
use arrow::array::{Int32Array, StringArray, Float64Array};
use std::sync::Arc;
use std::time::Instant;

fn create_test_data(size: usize) -> (RecordBatch, RecordBatch) {
    // 创建左表（构建端）
    let left_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("dept_id", DataType::Int32, false),
    ]));

    let left_data: Vec<i32> = (0..size).map(|i| i as i32).collect();
    let left_names: Vec<String> = (0..size).map(|i| format!("User{}", i)).collect();
    let left_depts: Vec<i32> = (0..size).map(|i| (i % 10) as i32).collect();

    let left_batch = RecordBatch::try_new(
        left_schema,
        vec![
            Arc::new(Int32Array::from(left_data)),
            Arc::new(StringArray::from(left_names)),
            Arc::new(Int32Array::from(left_depts)),
        ],
    ).unwrap();

    // 创建右表（探测端）
    let right_schema = Arc::new(Schema::new(vec![
        Field::new("dept_id", DataType::Int32, false),
        Field::new("dept_name", DataType::Utf8, false),
        Field::new("budget", DataType::Float64, false),
    ]));

    let right_depts: Vec<i32> = (0..size * 2).map(|i| (i % 10) as i32).collect();
    let right_names: Vec<String> = (0..size * 2).map(|i| format!("Dept{}", i % 10)).collect();
    let right_budgets: Vec<f64> = (0..size * 2).map(|i| (i as f64) * 1000.0).collect();

    let right_batch = RecordBatch::try_new(
        right_schema,
        vec![
            Arc::new(Int32Array::from(right_depts)),
            Arc::new(StringArray::from(right_names)),
            Arc::new(Float64Array::from(right_budgets)),
        ],
    ).unwrap();

    (left_batch, right_batch)
}

fn bench_hash_join_build(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash_join_build");
    
    for size in [100, 1000, 10000].iter() {
        group.bench_with_input(BenchmarkId::new("build_table", size), size, |b, &size| {
            let (left_batch, _) = create_test_data(size);
            let config = HashJoinConfig {
                left_join_columns: vec!["dept_id".to_string()],
                right_join_columns: vec!["dept_id".to_string()],
                join_type: JoinType::Inner,
                join_mode: JoinMode::Broadcast,
                output_schema: Arc::new(Schema::new(vec![] as Vec<arrow::datatypes::Field>)),
                enable_dictionary_optimization: true,
                max_memory_bytes: 1024 * 1024,
                enable_runtime_filter: false,
            };
            
            b.iter(|| {
                let mut hash_join = HashJoinOperator::new(1, config.clone());
                let _ = hash_join.process_build_side(left_batch.clone());
            });
        });
    }
    
    group.finish();
}

fn bench_hash_join_probe(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash_join_probe");
    
    for size in [100, 1000, 10000].iter() {
        group.bench_with_input(BenchmarkId::new("probe_table", size), size, |b, &size| {
            let (left_batch, right_batch) = create_test_data(size);
            let config = HashJoinConfig {
                left_join_columns: vec!["dept_id".to_string()],
                right_join_columns: vec!["dept_id".to_string()],
                join_type: JoinType::Inner,
                join_mode: JoinMode::Broadcast,
                output_schema: Arc::new(Schema::new(vec![] as Vec<arrow::datatypes::Field>)),
                enable_dictionary_optimization: true,
                max_memory_bytes: 1024 * 1024,
                enable_runtime_filter: false,
            };
            
            b.iter(|| {
                let mut hash_join = HashJoinOperator::new(1, config.clone());
                // 先构建hash表
                let _ = hash_join.process_build_side(left_batch.clone());
                // 然后探测
                let _ = hash_join.process_probe_side(right_batch.clone());
            });
        });
    }
    
    group.finish();
}

fn bench_hash_join_memory_usage(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash_join_memory");
    
    for size in [1000, 10000, 100000].iter() {
        group.bench_with_input(BenchmarkId::new("memory_usage", size), size, |b, &size| {
            let (left_batch, _) = create_test_data(size);
            let config = HashJoinConfig {
                left_join_columns: vec!["dept_id".to_string()],
                right_join_columns: vec!["dept_id".to_string()],
                join_type: JoinType::Inner,
                join_mode: JoinMode::Broadcast,
                output_schema: Arc::new(Schema::new(vec![] as Vec<arrow::datatypes::Field>)),
                enable_dictionary_optimization: true,
                max_memory_bytes: 1024 * 1024,
                enable_runtime_filter: false,
            };
            
            b.iter(|| {
                let mut hash_join = HashJoinOperator::new(1, config.clone());
                let _ = hash_join.process_build_side(left_batch.clone());
                // 测量内存使用情况
                let memory_usage = hash_join.build_table.len() * std::mem::size_of::<Vec<u8>>();
                memory_usage
            });
        });
    }
    
    group.finish();
}

criterion_group!(
    benches,
    bench_hash_join_build,
    bench_hash_join_probe,
    bench_hash_join_memory_usage
);
criterion_main!(benches);
