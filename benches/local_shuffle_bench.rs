//! Local Shuffle算子基准测试
//! 
//! 测试Local Shuffle算子的性能

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use oneengine::columnar::{
    batch::{Batch, BatchSchema, Field},
    column::{Column, AnyArray},
    types::DataType,
};
use oneengine::execution::operators::local_shuffle::{LocalShuffleOperator, LocalShuffleConfig};
use rand::Rng;

fn create_test_batch(num_rows: usize) -> Batch {
    let mut rng = rand::thread_rng();
    
    // 创建schema
    let mut schema = BatchSchema::new();
    schema.fields.push(Field { name: "id".to_string(), data_type: DataType::Int32, nullable: false });
    schema.fields.push(Field { name: "key".to_string(), data_type: DataType::Int32, nullable: false });
    schema.fields.push(Field { name: "value".to_string(), data_type: DataType::Float64, nullable: false });
    
    // 创建ID列
    let mut id_data = Vec::with_capacity(num_rows);
    for i in 0..num_rows {
        id_data.push(i as i32);
    }
    let id_column = Column::new(DataType::Int32, num_rows);
    
    // 创建key列（用于分区）
    let mut key_data = Vec::with_capacity(num_rows);
    for _ in 0..num_rows {
        key_data.push(rng.gen_range(0..10)); // 0-9的key值
    }
    let key_column = Column::new(DataType::Int32, num_rows);
    
    // 创建value列
    let mut value_data = Vec::with_capacity(num_rows);
    for _ in 0..num_rows {
        value_data.push(rng.gen_range(0.0..100.0));
    }
    let value_column = Column::new(DataType::Float64, num_rows);
    
    Batch::from_columns(vec![id_column, key_column, value_column], schema).unwrap()
}

fn create_shuffle_config(num_partitions: usize, input_schema: BatchSchema) -> LocalShuffleConfig {
    LocalShuffleConfig {
        partition_cols: vec![1], // 按key列分区
        num_partitions,
        input_schema,
    }
}

fn bench_shuffle_processing(c: &mut Criterion) {
    let mut group = c.benchmark_group("shuffle_processing");
    
    // 测试不同批次大小
    for size in [100, 1000, 10000, 100000].iter() {
        let batch = create_test_batch(*size);
        let schema = batch.schema.clone();
        
        group.bench_with_input(
            BenchmarkId::new("batch_size", size),
            size,
            |b, _| {
                b.iter(|| {
                    let config = create_shuffle_config(4, schema.clone());
                    let mut operator = LocalShuffleOperator::new(config);
                    let _ = operator.process_input(&batch);
                    let _ = operator.finish_partitioning();
                });
            },
        );
    }
    
    group.finish();
}

fn bench_shuffle_partitions(c: &mut Criterion) {
    let mut group = c.benchmark_group("shuffle_partitions");
    
    let batch = create_test_batch(10000);
    let schema = batch.schema.clone();
    
    // 测试不同数量的分区
    for num_partitions in [2, 4, 8, 16, 32].iter() {
        group.bench_with_input(
            BenchmarkId::new("num_partitions", num_partitions),
            num_partitions,
            |b, &num_partitions| {
                b.iter(|| {
                    let config = create_shuffle_config(num_partitions, schema.clone());
                    let mut operator = LocalShuffleOperator::new(config);
                    let _ = operator.process_input(&batch);
                    let _ = operator.finish_partitioning();
                });
            },
        );
    }
    
    group.finish();
}

fn bench_shuffle_partition_columns(c: &mut Criterion) {
    let mut group = c.benchmark_group("shuffle_partition_columns");
    
    let batch = create_test_batch(10000);
    let schema = batch.schema.clone();
    
    // 测试不同数量的分区列
    let partition_configs = vec![
        (vec![1], "single_col"), // 单列分区
        (vec![0, 1], "two_cols"), // 两列分区
        (vec![0, 1, 2], "three_cols"), // 三列分区
    ];
    
    for (i, (partition_cols, name)) in partition_configs.iter().enumerate() {
        group.bench_with_input(
            BenchmarkId::new("partition_columns", name),
            &(partition_cols, name),
            |b, (partition_cols, _)| {
                b.iter(|| {
                    let config = LocalShuffleConfig {
                        partition_cols: partition_cols.to_vec(),
                        num_partitions: 4,
                        input_schema: schema.clone(),
                    };
                    let mut operator = LocalShuffleOperator::new(config);
                    let _ = operator.process_input(&batch);
                    let _ = operator.finish_partitioning();
                });
            },
        );
    }
    
    group.finish();
}

fn bench_shuffle_multiple_batches(c: &mut Criterion) {
    let mut group = c.benchmark_group("shuffle_multiple_batches");
    
    let schema = BatchSchema::new();
    
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
                    let config = create_shuffle_config(4, schema.clone());
                    let mut operator = LocalShuffleOperator::new(config);
                    for batch in &batches {
                        let _ = operator.process_input(batch);
                    }
                    let _ = operator.finish_partitioning();
                });
            },
        );
    }
    
    group.finish();
}

fn bench_shuffle_data_distribution(c: &mut Criterion) {
    let mut group = c.benchmark_group("shuffle_data_distribution");
    
    let schema = BatchSchema::new();
    
    // 测试不同的数据分布
    let distributions = vec![
        ("uniform", 0..10), // 均匀分布
        ("skewed", 0..3),   // 倾斜分布（只有3个不同的key）
        ("single", 0..1),   // 单key分布
    ];
    
    for (name, key_range) in distributions.iter() {
        let mut rng = rand::thread_rng();
        let mut key_data = Vec::with_capacity(10000);
        for _ in 0..10000 {
            key_data.push(rng.gen_range(key_range.clone()));
        }
        
        // 创建测试批次
        let mut id_data = Vec::with_capacity(10000);
        for i in 0..10000 {
            id_data.push(i as i32);
        }
        let mut value_data = Vec::with_capacity(10000);
        for _ in 0..10000 {
            value_data.push(rng.gen_range(0.0..100.0));
        }
        
        let mut test_schema = BatchSchema::new();
        test_schema.fields.push(Field { name: "id".to_string(), data_type: DataType::Int32, nullable: false });
        test_schema.fields.push(Field { name: "key".to_string(), data_type: DataType::Int32, nullable: false });
        test_schema.fields.push(Field { name: "value".to_string(), data_type: DataType::Float64, nullable: false });
        
        let id_column = Column::new(DataType::Int32, 10000);
        let key_column = Column::new(DataType::Int32, 10000);
        let value_column = Column::new(DataType::Float64, 10000);
        
        let batch = Batch::from_columns(vec![id_column, key_column, value_column], test_schema.clone()).unwrap();
        
        group.bench_with_input(
            BenchmarkId::new("distribution", name),
            name,
            |b, _| {
                b.iter(|| {
                    let config = create_shuffle_config(4, test_schema.clone());
                    let mut operator = LocalShuffleOperator::new(config);
                    let _ = operator.process_input(&batch);
                    let _ = operator.finish_partitioning();
                });
            },
        );
    }
    
    group.finish();
}

criterion_group!(
    benches,
    bench_shuffle_processing,
    bench_shuffle_partitions,
    bench_shuffle_partition_columns,
    bench_shuffle_multiple_batches,
    bench_shuffle_data_distribution
);
criterion_main!(benches);
