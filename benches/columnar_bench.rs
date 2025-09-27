use criterion::{black_box, criterion_group, criterion_main, Criterion};
use oneengine::columnar::batch::{Batch, BatchSchema, Field};
use oneengine::columnar::column::{Column, AnyArray};
use oneengine::columnar::types::{DataType, Bitmap};
use anyhow::Result;

fn create_test_batch(rows: usize) -> Result<Batch> {
    let mut schema = BatchSchema::new();
    schema.add_field("id".to_string(), DataType::Int32, false);
    schema.add_field("value".to_string(), DataType::Float64, false);
    schema.add_field("name".to_string(), DataType::Utf8, true);

    let id_data = AnyArray::Int32((0..rows as i32).collect());
    let value_data = AnyArray::Float64((0..rows).map(|i| i as f64 * 1.5).collect());
    let name_data = AnyArray::Utf8((0..rows).map(|i| format!("name_{}", i)).collect());

    let id_column = Column::from_data(DataType::Int32, id_data, None)?;
    let value_column = Column::from_data(DataType::Float64, value_data, None)?;
    let name_column = Column::from_data(DataType::Utf8, name_data, None)?;

    Batch::from_columns(vec![id_column, value_column, name_column], schema)
}

fn bench_batch_creation(c: &mut Criterion) {
    c.bench_function("batch_creation_10k_rows", |b| {
        b.iter(|| {
            create_test_batch(black_box(10000)).unwrap()
        })
    });
}

fn bench_batch_slicing(c: &mut Criterion) {
    let batch = create_test_batch(10000).unwrap();
    
    c.bench_function("batch_slice_1k_rows", |b| {
        b.iter(|| {
            batch.slice(black_box(1000), black_box(1000)).unwrap()
        })
    });
}

fn bench_column_operations(c: &mut Criterion) {
    let batch = create_test_batch(10000).unwrap();
    let id_column = batch.column(0).unwrap();
    
    c.bench_function("column_memory_usage", |b| {
        b.iter(|| {
            black_box(id_column.memory_usage())
        })
    });
}

fn bench_null_bitmap_operations(c: &mut Criterion) {
    let mut bitmap = Bitmap::new(10000);
    
    c.bench_function("bitmap_set_operations", |b| {
        b.iter(|| {
            for i in 0..1000 {
                bitmap.set(black_box(i), black_box(i % 2 == 0));
            }
        })
    });
}

fn bench_batch_stats(c: &mut Criterion) {
    let batch = create_test_batch(10000).unwrap();
    
    c.bench_function("batch_stats_calculation", |b| {
        b.iter(|| {
            black_box(batch.stats())
        })
    });
}

criterion_group!(
    benches,
    bench_batch_creation,
    bench_batch_slicing,
    bench_column_operations,
    bench_null_bitmap_operations,
    bench_batch_stats
);
criterion_main!(benches);
