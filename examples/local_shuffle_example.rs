//! Local Shuffle算子示例
//! 
//! 演示Local Shuffle算子的使用方法和性能

use oneengine::columnar::{
    batch::{Batch, BatchSchema, Field},
    column::{Column, AnyArray},
    types::DataType,
};
use oneengine::execution::operators::local_shuffle::{LocalShuffleOperator, LocalShuffleConfig};
use oneengine::execution::operator::Operator;
use rand::Rng;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Local Shuffle算子示例 ===");
    
    // 创建示例数据
    let (input_schema, input_batches) = create_sample_data(1000, 3)?;
    
    // 配置Local Shuffle算子
    let config = LocalShuffleConfig {
        partition_cols: vec![1], // 按第2列分区
        num_partitions: 4, // 分成4个分区
        input_schema: input_schema.clone(),
    };
    
    // 创建Local Shuffle算子
    let mut operator = LocalShuffleOperator::new(config);
    
    // 处理输入批次
    println!("处理输入批次...");
    for batch in &input_batches {
        operator.process_input(batch)?;
    }
    
    // 完成分区
    println!("完成分区...");
    operator.finish_partitioning()?;
    
    // 获取结果
    println!("分区结果:");
    let mut result_count = 0;
    let waker = std::task::Waker::from(std::task::RawWaker::new(std::ptr::null(), &noop_waker_vtable()));
    let mut cx = std::task::Context::from_waker(&waker);
    
    while let Some(batch) = operator.poll_next(&mut cx).ready()? {
        println!("分区批次 {}: {} 行", result_count, batch.len());
        
        // 打印前几行数据
        for row in 0..std::cmp::min(3, batch.len()) {
            let mut row_data = Vec::new();
            for col in 0..batch.columns.len() {
                let value = match &batch.columns[col].data {
                    AnyArray::Int32(data) => format!("{}", data[row]),
                    AnyArray::Int64(data) => format!("{}", data[row]),
                    AnyArray::Float64(data) => format!("{:.2}", data[row]),
                    AnyArray::Utf8(data) => data[row].clone(),
                    AnyArray::Boolean(data) => format!("{}", data[row]),
                    _ => "N/A".to_string(),
                };
                row_data.push(value);
            }
            println!("  行 {}: {:?}", row, row_data);
        }
        
        result_count += 1;
    }
    
    println!("总共输出了 {} 个分区批次", result_count);
    
    Ok(())
}

fn create_sample_data(num_rows: usize, num_batches: usize) -> Result<(BatchSchema, Vec<Batch>), Box<dyn std::error::Error>> {
    let mut rng = rand::thread_rng();
    let rows_per_batch = num_rows / num_batches;
    
    // 创建schema
    let mut schema = BatchSchema::new();
    schema.fields.push(Field { name: "id".to_string(), data_type: DataType::Int32, nullable: false });
    schema.fields.push(Field { name: "key".to_string(), data_type: DataType::Int32, nullable: false });
    schema.fields.push(Field { name: "value".to_string(), data_type: DataType::Float64, nullable: false });
    
    let mut batches = Vec::new();
    
    for batch_idx in 0..num_batches {
        let start_row = batch_idx * rows_per_batch;
        let end_row = if batch_idx == num_batches - 1 {
            num_rows
        } else {
            (batch_idx + 1) * rows_per_batch
        };
        let batch_size = end_row - start_row;
        
        // 创建ID列
        let mut id_data = Vec::with_capacity(batch_size);
        for i in start_row..end_row {
            id_data.push(i as i32);
        }
        let id_column = Column::new(DataType::Int32, batch_size);
        
        // 创建key列（用于分区）
        let mut key_data = Vec::with_capacity(batch_size);
        for _ in 0..batch_size {
            key_data.push(rng.gen_range(0..10)); // 0-9的key值
        }
        let key_column = Column::new(DataType::Int32, batch_size);
        
        // 创建value列
        let mut value_data = Vec::with_capacity(batch_size);
        for _ in 0..batch_size {
            value_data.push(rng.gen_range(0.0..100.0));
        }
        let value_column = Column::new(DataType::Float64, batch_size);
        
        let batch = Batch::from_columns(vec![id_column, key_column, value_column], schema.clone())?;
        batches.push(batch);
    }
    
    Ok((schema, batches))
}

// 简单的Waker实现用于示例
unsafe fn noop_waker_vtable() -> &'static std::task::RawWakerVTable {
    &std::task::RawWakerVTable::new(
        |_| std::task::RawWaker::new(std::ptr::null(), &noop_waker_vtable()),
        |_| {},
        |_| {},
        |_| {},
    )
}
