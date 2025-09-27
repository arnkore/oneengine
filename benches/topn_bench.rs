//! Top-N算子基准测试
//! 
//! 测试Top-N算子的性能

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use oneengine::columnar::{
    batch::{Batch, BatchSchema, Field},
    column::{Column, AnyArray},
    types::DataType,
};
use oneengine::execution::operators::topn::{TopNOperator, TopNConfig};
use rand::Rng;

fn create_test_batch(num_rows: usize) -> Batch {
    let mut rng = rand::thread_rng();
    
    // 创建schema
    let mut schema = BatchSchema::new();
    schema.fields.push(Field { name: "id".to_string(), data_type: DataType::Int32, nullable: false });
    schema.fields.push(Field { name: "score".to_string(), data_type: DataType::Float64, nullable: false });
    schema.fields.push(Field { name: "category".to_string(), data_type: DataType::Utf8, nullable: false });
    
    // 创建ID列
    let mut id_data = Vec::with_capacity(num_rows);
    for i in 0..num_rows {
        id_data.push(i as i32);
    }
    let id_column = Column::new(DataType::Int32, num_rows);
    
    // 创建分数列（随机分数）
    let mut score_data = Vec::with_capacity(num_rows);
    for _ in 0..num_rows {
        score_data.push(rng.gen_range(0.0..100.0));
    }
    let score_column = Column::new(DataType::Float64, num_rows);
    
    // 创建分类列
    let categories = vec!["A", "B", "C", "D", "E"];
    let mut category_data = Vec::with_capacity(num_rows);
    for _ in 0..num_rows {
        let category = categories[rng.gen_range(0..categories.len())];
        category_data.push(category.to_string());
    }
    let category_column = Column::new(DataType::Utf8, num_rows);
    
    Batch::from_columns(vec![id_column, score_column, category_column], schema).unwrap()
}

fn create_topn_config(limit: usize, input_schema: BatchSchema) -> TopNConfig {
    TopNConfig {
        sort_cols: vec![1, 2], // 按分数和分类排序
        ascending: vec![false, true], // 分数降序，分类升序
        limit,
        input_schema,
    }
}

fn bench_topn_processing(c: &mut Criterion) {
    let mut group = c.benchmark_group("topn_processing");
    
    // 测试不同批次大小
    for size in [100, 1000, 10000, 100000].iter() {
        let batch = create_test_batch(*size);
        let schema = batch.schema.clone();
        
        group.bench_with_input(
            BenchmarkId::new("batch_size", size),
            size,
            |b, _| {
                b.iter(|| {
                    let config = create_topn_config(10, schema.clone());
                    let mut operator = TopNOperator::new(config);
                    let _ = operator.process_input(&batch);
                    let _ = operator.finish_sorting();
                });
            },
        );
    }
    
    group.finish();
}

fn bench_topn_limits(c: &mut Criterion) {
    let mut group = c.benchmark_group("topn_limits");
    
    let batch = create_test_batch(10000);
    let schema = batch.schema.clone();
    
    // 测试不同的Top-N限制
    for limit in [1, 10, 100, 1000].iter() {
        group.bench_with_input(
            BenchmarkId::new("limit", limit),
            limit,
            |b, &limit| {
                b.iter(|| {
                    let config = create_topn_config(limit, schema.clone());
                    let mut operator = TopNOperator::new(config);
                    let _ = operator.process_input(&batch);
                    let _ = operator.finish_sorting();
                });
            },
        );
    }
    
    group.finish();
}

fn bench_topn_sort_columns(c: &mut Criterion) {
    let mut group = c.benchmark_group("topn_sort_columns");
    
    let batch = create_test_batch(10000);
    let schema = batch.schema.clone();
    
    // 测试不同数量的排序列
    let sort_configs = vec![
        (vec![1], vec![false]), // 单列排序
        (vec![1, 2], vec![false, true]), // 两列排序
        (vec![0, 1, 2], vec![true, false, true]), // 三列排序
    ];
    
    for (i, (sort_cols, ascending)) in sort_configs.iter().enumerate() {
        group.bench_with_input(
            BenchmarkId::new("sort_columns", i + 1),
            &(sort_cols, ascending),
            |b, (sort_cols, ascending)| {
                b.iter(|| {
                    let config = TopNConfig {
                        sort_cols: sort_cols.to_vec(),
                        ascending: ascending.to_vec(),
                        limit: 10,
                        input_schema: schema.clone(),
                    };
                    let mut operator = TopNOperator::new(config);
                    let _ = operator.process_input(&batch);
                    let _ = operator.finish_sorting();
                });
            },
        );
    }
    
    group.finish();
}

fn bench_topn_multiple_batches(c: &mut Criterion) {
    let mut group = c.benchmark_group("topn_multiple_batches");
    
    let mut schema = BatchSchema::new();
    schema.fields.push(Field { name: "id".to_string(), data_type: DataType::Int32, nullable: false });
    schema.fields.push(Field { name: "score".to_string(), data_type: DataType::Float64, nullable: false });
    
    // 测试不同数量的批次
    for num_batches in [1, 5, 10, 20].iter() {
        let batches: Vec<Batch> = (0..*num_batches)
            .map(|_| create_test_batch(1000))
            .collect();
        
        group.bench_with_input(
            BenchmarkId::new("num_batches", num_batches),
            num_batches,
            |b, _| {
                b.iter(|| {
                    let config = create_topn_config(10, schema.clone());
                    let mut operator = TopNOperator::new(config);
                    for batch in &batches {
                        let _ = operator.process_input(batch);
                    }
                    let _ = operator.finish_sorting();
                });
            },
        );
    }
    
    group.finish();
}

criterion_group!(
    benches,
    bench_topn_processing,
    bench_topn_limits,
    bench_topn_sort_columns,
    bench_topn_multiple_batches
);
criterion_main!(benches);
