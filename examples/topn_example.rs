//! Top-N算子示例
//! 
//! 演示Top-N算子的使用方法和性能

use oneengine::columnar::{
    batch::{Batch, BatchSchema, Field},
    column::{Column, AnyArray},
    types::DataType,
};
use oneengine::execution::operators::topn::{TopNOperator, TopNConfig};
use oneengine::execution::operator::Operator;
use rand::Rng;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Top-N算子示例 ===");
    
    // 创建示例数据
    let (input_schema, input_batches) = create_sample_data(1000, 3)?;
    
    // 配置Top-N算子
    let config = TopNConfig {
        sort_cols: vec![1, 2], // 按第2列和第3列排序
        ascending: vec![false, true], // 第2列降序，第3列升序
        limit: 10, // 取前10条
        input_schema: input_schema.clone(),
    };
    
    // 创建Top-N算子
    let mut operator = TopNOperator::new(config);
    
    // 处理输入批次
    println!("处理输入批次...");
    for batch in &input_batches {
        operator.process_input(batch)?;
    }
    
    // 完成排序
    println!("完成排序...");
    operator.finish_sorting()?;
    
    // 获取结果
    println!("Top-N结果:");
    let mut result_count = 0;
    let waker = std::task::Waker::from(std::task::RawWaker::new(std::ptr::null(), &noop_waker_vtable()));
    let mut cx = std::task::Context::from_waker(&waker);
    
    while let Some(batch) = operator.poll_next(&mut cx).ready()? {
        println!("批次 {}: {} 行", result_count, batch.len());
        
        // 打印前几行数据
        for row in 0..std::cmp::min(5, batch.len()) {
            let mut row_data = Vec::new();
            for col in 0..batch.columns.len() {
                let value = match &batch.columns[col].data {
                    AnyArray::Int32(data) => format!("{}", data[row]),
                    AnyArray::Int64(data) => format!("{}", data[row]),
                    AnyArray::Float64(data) => format!("{:.2}", data[row]),
                    AnyArray::Utf8(data) => data[row].clone(),
                    AnyArray::Boolean(data) => format!("{}", data[row]),
                };
                row_data.push(value);
            }
            println!("  行 {}: {:?}", row, row_data);
        }
        
        result_count += 1;
    }
    
    println!("总共输出了 {} 个批次", result_count);
    
    Ok(())
}

fn create_sample_data(num_rows: usize, num_batches: usize) -> Result<(BatchSchema, Vec<Batch>), Box<dyn std::error::Error>> {
    let mut rng = rand::thread_rng();
    let rows_per_batch = num_rows / num_batches;
    
    // 创建schema
    let mut schema = BatchSchema::new();
    schema.fields.push(Field { name: "id".to_string(), data_type: DataType::Int32, nullable: false });
    schema.fields.push(Field { name: "score".to_string(), data_type: DataType::Float64, nullable: false });
    schema.fields.push(Field { name: "category".to_string(), data_type: DataType::Utf8, nullable: false });
    
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
        
        // 创建分数列（随机分数）
        let mut score_data = Vec::with_capacity(batch_size);
        for _ in 0..batch_size {
            score_data.push(rng.gen_range(0.0..100.0));
        }
        let score_column = Column::new(DataType::Float64, batch_size);
        
        // 创建分类列
        let categories = vec!["A", "B", "C", "D", "E"];
        let mut category_data = Vec::with_capacity(batch_size);
        for _ in 0..batch_size {
            let category = categories[rng.gen_range(0..categories.len())];
            category_data.push(category.to_string());
        }
        let category_column = Column::new(DataType::Utf8, batch_size);
        
        let batch = Batch::from_columns(vec![id_column, score_column, category_column], schema.clone())?;
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
