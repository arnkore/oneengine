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


//! Hash聚合算子基准测试
//! 
//! 测试Hash聚合算子的性能

use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};
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
use rand::Rng;

fn create_test_batch(row_count: usize) -> Result<Batch> {
    let mut rng = rand::thread_rng();
    
    // 创建部门数据
    let departments = vec!["Engineering", "Sales", "Marketing", "HR", "Finance"];
    let mut dept_data = Vec::new();
    for _ in 0..row_count {
        let dept = departments[rng.gen_range(0..departments.len())];
        dept_data.push(dept.to_string());
    }
    
    // 创建薪资数据
    let mut salary_data = Vec::new();
    for _ in 0..row_count {
        salary_data.push(rng.gen_range(40000.0..150000.0));
    }
    
    // 创建年龄数据
    let mut age_data = Vec::new();
    for _ in 0..row_count {
        age_data.push(rng.gen_range(22..65));
    }
    
    // 创建schema
    let mut schema = BatchSchema::new();
    schema.fields.push(Field {
        name: "department".to_string(),
        data_type: DataType::Utf8,
        nullable: false,
    });
    schema.fields.push(Field {
        name: "salary".to_string(),
        data_type: DataType::Float64,
        nullable: false,
    });
    schema.fields.push(Field {
        name: "age".to_string(),
        data_type: DataType::Int32,
        nullable: false,
    });
    
    // 创建列
    let dept_column = Column::new(DataType::Utf8, row_count);
    let salary_column = Column::new(DataType::Float64, row_count);
    let age_column = Column::new(DataType::Int32, row_count);
    
    // 创建批次
    let batch = Batch::from_columns(
        vec![dept_column, salary_column, age_column],
        schema,
    )?;
    
    Ok(batch)
}

fn create_agg_config(input_schema: BatchSchema) -> HashAggConfig {
    HashAggConfig {
        group_by_cols: vec![0], // 按department分组
        agg_exprs: vec![
            AggExpr {
                func: AggFunction::Count,
                input_col: 1, // salary列
                output_name: "count".to_string(),
                output_type: DataType::Int64,
            },
            AggExpr {
                func: AggFunction::Sum,
                input_col: 1, // salary列
                output_name: "sum".to_string(),
                output_type: DataType::Float64,
            },
            AggExpr {
                func: AggFunction::Avg,
                input_col: 1, // salary列
                output_name: "avg".to_string(),
                output_type: DataType::Float64,
            },
            AggExpr {
                func: AggFunction::Max,
                input_col: 1, // salary列
                output_name: "max".to_string(),
                output_type: DataType::Float64,
            },
            AggExpr {
                func: AggFunction::Min,
                input_col: 1, // salary列
                output_name: "min".to_string(),
                output_type: DataType::Float64,
            },
        ],
        input_schema,
        two_phase: false,
        initial_capacity: 1024,
    }
}

fn bench_hash_agg_processing(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash_agg_processing");
    
    for row_count in [1000, 10000, 100000].iter() {
        group.bench_with_input(
            BenchmarkId::new("process_batch", row_count),
            row_count,
            |b, &row_count| {
                b.iter(|| {
                    let batch = create_test_batch(row_count).unwrap();
                    let input_schema = batch.schema.clone();
                    let config = create_agg_config(input_schema);
                    let mut operator = HashAggOperator::new(config);
                    
                    // 处理批次
                    operator.process_input(&batch).unwrap();
                    
                    // 完成聚合
                    operator.finish_local_phase().unwrap();
                });
            },
        );
    }
    
    group.finish();
}

fn bench_hash_agg_different_group_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash_agg_group_sizes");
    
    let row_count = 10000;
    let batch = create_test_batch(row_count).unwrap();
    let input_schema = batch.schema.clone();
    
    // 测试不同的分组列数量
    for group_cols in [1, 2, 3].iter() {
        group.bench_with_input(
            BenchmarkId::new("group_cols", group_cols),
            group_cols,
            |b, &group_cols| {
                b.iter(|| {
                    let mut config = create_agg_config(input_schema.clone());
                    config.group_by_cols = (0..group_cols).collect();
                    let mut operator = HashAggOperator::new(config);
                    
                    operator.process_input(&batch).unwrap();
                    operator.finish_local_phase().unwrap();
                });
            },
        );
    }
    
    group.finish();
}

fn bench_hash_agg_aggregation_functions(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash_agg_functions");
    
    let row_count = 10000;
    let batch = create_test_batch(row_count).unwrap();
    let input_schema = batch.schema.clone();
    
    let functions = [
        ("count", AggFunction::Count),
        ("sum", AggFunction::Sum),
        ("avg", AggFunction::Avg),
        ("max", AggFunction::Max),
        ("min", AggFunction::Min),
    ];
    
    for (name, func) in functions.iter() {
        group.bench_with_input(
            BenchmarkId::new("function", name),
            func,
            |b, func| {
                b.iter(|| {
                    let mut config = create_agg_config(input_schema.clone());
                    config.agg_exprs = vec![AggExpr {
                        func: func.clone(),
                        input_col: 1,
                        output_name: "result".to_string(),
                        output_type: DataType::Float64,
                    }];
                    let mut operator = HashAggOperator::new(config);
                    
                    operator.process_input(&batch).unwrap();
                    operator.finish_local_phase().unwrap();
                });
            },
        );
    }
    
    group.finish();
}

criterion_group!(
    benches,
    bench_hash_agg_processing,
    bench_hash_agg_different_group_sizes,
    bench_hash_agg_aggregation_functions
);
criterion_main!(benches);
