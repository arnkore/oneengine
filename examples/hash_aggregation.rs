//! Hash聚合算子示例
//! 
//! 演示如何使用Hash聚合算子进行数据聚合

use oneengine::columnar::{
    batch::{Batch, BatchSchema, Field},
    column::{Column, AnyArray},
    types::{DataType, Bitmap},
};
use oneengine::execution::{
    operators::hash_agg::{HashAggOperator, HashAggConfig, AggExpr, AggFunction},
    context::ExecContext,
};
use anyhow::Result;

fn main() -> Result<()> {
    println!("OneEngine Hash Aggregation Example");

    // 1. 创建输入schema
    let input_schema = BatchSchema::new();
    input_schema.fields.push(Field {
        name: "department".to_string(),
        data_type: DataType::Utf8,
        nullable: false,
    });
    input_schema.fields.push(Field {
        name: "salary".to_string(),
        data_type: DataType::Float64,
        nullable: false,
    });
    input_schema.fields.push(Field {
        name: "age".to_string(),
        data_type: DataType::Int32,
        nullable: false,
    });

    // 2. 创建聚合配置
    let config = HashAggConfig {
        group_by_cols: vec![0], // 按department分组
        agg_exprs: vec![
            AggExpr {
                func: AggFunction::Count,
                input_col: 1, // salary列
                output_name: "employee_count".to_string(),
                output_type: DataType::Int64,
            },
            AggExpr {
                func: AggFunction::Sum,
                input_col: 1, // salary列
                output_name: "total_salary".to_string(),
                output_type: DataType::Float64,
            },
            AggExpr {
                func: AggFunction::Avg,
                input_col: 1, // salary列
                output_name: "avg_salary".to_string(),
                output_type: DataType::Float64,
            },
            AggExpr {
                func: AggFunction::Max,
                input_col: 1, // salary列
                output_name: "max_salary".to_string(),
                output_type: DataType::Float64,
            },
            AggExpr {
                func: AggFunction::Min,
                input_col: 1, // salary列
                output_name: "min_salary".to_string(),
                output_type: DataType::Float64,
            },
        ],
        input_schema: input_schema.clone(),
        two_phase: false,
        initial_capacity: 1024,
    };

    // 3. 创建Hash聚合算子
    let mut operator = HashAggOperator::new(config);

    // 4. 创建测试数据
    let test_data = create_test_data()?;
    
    // 5. 处理数据
    for batch in test_data {
        operator.process_input(&batch)?;
    }

    // 6. 完成聚合
    operator.finish_local_phase()?;

    // 7. 获取结果
    let context = ExecContext::default();
    let mut result_count = 0;
    
    loop {
        match operator.poll_next(&mut std::task::Context::from_waker(
            &std::task::Waker::noop()
        ))? {
            std::task::Poll::Ready(Some(batch)) => {
                result_count += 1;
                println!("Result batch {}: {} rows", result_count, batch.len);
                println!("Schema: {:?}", batch.schema);
                
                // 打印前几行结果
                for (i, field) in batch.schema.fields.iter().enumerate() {
                    println!("  {}: {}", i, field.name);
                }
            }
            std::task::Poll::Ready(None) => break,
            std::task::Poll::Pending => {
                // 在实际应用中，这里会等待更多数据
                break;
            }
        }
    }

    println!("Hash aggregation completed! Generated {} result batches.", result_count);
    Ok(())
}

fn create_test_data() -> Result<Vec<Batch>> {
    let mut batches = Vec::new();
    
    // 创建第一个批次
    let departments1 = AnyArray::Utf8(vec![
        "Engineering".to_string(),
        "Sales".to_string(),
        "Marketing".to_string(),
        "Engineering".to_string(),
        "Sales".to_string(),
    ]);
    
    let salaries1 = AnyArray::Float64(vec![
        75000.0,
        60000.0,
        55000.0,
        80000.0,
        65000.0,
    ]);
    
    let ages1 = AnyArray::Int32(vec![28, 32, 25, 35, 30]);
    
    let batch1 = Batch::from_columns(
        vec![
            Column::new(DataType::Utf8, 5),
            Column::new(DataType::Float64, 5),
            Column::new(DataType::Int32, 5),
        ],
        BatchSchema::new(),
    )?;
    
    batches.push(batch1);
    
    // 创建第二个批次
    let departments2 = AnyArray::Utf8(vec![
        "Marketing".to_string(),
        "Engineering".to_string(),
        "Sales".to_string(),
        "Marketing".to_string(),
        "Engineering".to_string(),
    ]);
    
    let salaries2 = AnyArray::Float64(vec![
        58000.0,
        85000.0,
        70000.0,
        52000.0,
        90000.0,
    ]);
    
    let ages2 = AnyArray::Int32(vec![27, 40, 33, 24, 38]);
    
    let batch2 = Batch::from_columns(
        vec![
            Column::new(DataType::Utf8, 5),
            Column::new(DataType::Float64, 5),
            Column::new(DataType::Int32, 5),
        ],
        BatchSchema::new(),
    )?;
    
    batches.push(batch2);
    
    Ok(batches)
}
