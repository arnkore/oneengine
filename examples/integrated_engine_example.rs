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

//! Integrated Engine End-to-End Validation Example
//! 
//! This example demonstrates the complete execution flow:
//! Lake House Reading → MPP Operators → Pipeline → Driver → Task

use oneengine::execution::integrated_engine::*;
use oneengine::expression::ast::*;
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use anyhow::Result;
use tracing::{info, debug};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    info!("Starting Integrated Engine End-to-End Validation");

    // Create integrated execution engine
    let engine = IntegratedEngineFactory::create_default().await?;
    
    // Start the engine
    engine.start().await?;
    
    // Create a sample stage execution plan
    let stage_plan = create_sample_stage_plan()?;
    
    info!("Created stage execution plan: {}", stage_plan.stage_id);
    debug!("Stage plan details: {:?}", stage_plan);
    
    // Execute the stage plan
    let results = engine.execute_stage_plan(stage_plan).await?;
    
    info!("Stage execution completed successfully!");
    info!("Results: {} batches", results.len());
    
    // Print results summary
    for (i, batch) in results.iter().enumerate() {
        info!("Batch {}: {} rows, {} columns", 
              i, batch.num_rows(), batch.num_columns());
    }
    
    // Get execution statistics
    let stats = engine.get_execution_stats().await;
    info!("Execution Statistics:");
    info!("  Total Pipelines: {}", stats.total_pipelines);
    info!("  Completed Pipelines: {}", stats.completed_pipelines);
    info!("  Total Tasks: {}", stats.total_tasks);
    info!("  Total Execution Time: {}ms", stats.total_execution_time_ms);
    info!("  Total Rows Processed: {}", stats.total_rows_processed);
    
    // Stop the engine
    engine.stop().await?;
    
    info!("Integrated Engine End-to-End Validation completed successfully!");
    Ok(())
}

/// Create a sample stage execution plan for validation
fn create_sample_stage_plan() -> Result<StageExecutionPlan> {
    // Define schema
    let schema = Schema::new(vec![
        Field::new("id", ArrowDataType::Int64, false),
        Field::new("name", ArrowDataType::Utf8, false),
        Field::new("age", ArrowDataType::Int32, false),
        Field::new("salary", ArrowDataType::Float64, false),
        Field::new("department", ArrowDataType::Utf8, false),
    ]);
    
    // Create predicate expression: age > 25
    let predicate = Expression::Comparison(ComparisonExpr {
        left: Box::new(Expression::Column(ColumnRef {
            name: "age".to_string(),
            index: 0,
            data_type: ArrowDataType::Int32,
        })),
        op: ComparisonOp::GreaterThan,
        right: Box::new(Expression::Literal(Literal {
            value: datafusion_common::ScalarValue::Int32(Some(25)),
        })),
    });
    
    // Create projection expressions
    let projection_expressions = vec![
        Expression::Column(ColumnRef {
            name: "id".to_string(),
            index: 0,
            data_type: ArrowDataType::Int64,
        }),
        Expression::Column(ColumnRef {
            name: "name".to_string(),
            index: 1,
            data_type: ArrowDataType::Utf8,
        }),
        Expression::Column(ColumnRef {
            name: "salary".to_string(),
            index: 2,
            data_type: ArrowDataType::Float64,
        }),
    ];
    
    // Create operator nodes
    let operators = vec![
        // Scan operator
        OperatorNode {
            operator_id: "scan_1".to_string(),
            operator_type: OperatorType::MppScan { 
                table_path: "/data/employees.parquet".to_string(), 
                predicate: Some(predicate.clone())
            },
            dependencies: vec![],
            config: OperatorConfig::default(),
            input_schemas: vec![],
            output_schema: schema.clone(),
        },
        
        // Filter operator
        OperatorNode {
            operator_id: "filter_1".to_string(),
            operator_type: OperatorType::MppFilter { 
                predicate: predicate.clone()
            },
            dependencies: vec![0], // Depends on scan_1
            config: OperatorConfig::default(),
            input_schemas: vec![schema.clone()],
            output_schema: schema.clone(),
        },
        
        // Project operator
        OperatorNode {
            operator_id: "project_1".to_string(),
            operator_type: OperatorType::MppProject { 
                expressions: projection_expressions.clone()
            },
            dependencies: vec![1], // Depends on filter_1
            config: OperatorConfig::default(),
            input_schemas: vec![schema.clone()],
            output_schema: Schema::new(vec![
                Field::new("id", ArrowDataType::Int64, false),
                Field::new("name", ArrowDataType::Utf8, false),
                Field::new("salary", ArrowDataType::Float64, false),
            ]),
        },
        
        // Sort operator
        OperatorNode {
            operator_id: "sort_1".to_string(),
            operator_type: OperatorType::MppSort { 
                sort_columns: vec!["salary".to_string()] 
            },
            dependencies: vec![2], // Depends on project_1
            config: OperatorConfig::default(),
            input_schemas: vec![Schema::new(vec![
                Field::new("id", ArrowDataType::Int64, false),
                Field::new("name", ArrowDataType::Utf8, false),
                Field::new("salary", ArrowDataType::Float64, false),
            ])],
            output_schema: Schema::new(vec![
                Field::new("id", ArrowDataType::Int64, false),
                Field::new("name", ArrowDataType::Utf8, false),
                Field::new("salary", ArrowDataType::Float64, false),
            ]),
        },
    ];
    
    Ok(StageExecutionPlan {
        stage_id: "stage_1".to_string(),
        stage_name: "Employee Analysis Stage".to_string(),
        operators,
        dependencies: vec![],
        output_schema: Schema::new(vec![
            Field::new("id", ArrowDataType::Int64, false),
            Field::new("name", ArrowDataType::Utf8, false),
            Field::new("salary", ArrowDataType::Float64, false),
        ]),
        estimated_rows: Some(10000),
        estimated_memory_mb: Some(100),
    })
}

/// Create a complex stage execution plan with joins and aggregations
fn create_complex_stage_plan() -> Result<StageExecutionPlan> {
    // Define schemas
    let employees_schema = Schema::new(vec![
        Field::new("emp_id", ArrowDataType::Int64, false),
        Field::new("name", ArrowDataType::Utf8, false),
        Field::new("dept_id", ArrowDataType::Int32, false),
        Field::new("salary", ArrowDataType::Float64, false),
    ]);
    
    let departments_schema = Schema::new(vec![
        Field::new("dept_id", ArrowDataType::Int32, false),
        Field::new("dept_name", ArrowDataType::Utf8, false),
        Field::new("budget", ArrowDataType::Float64, false),
    ]);
    
    // Create operator nodes
    let operators = vec![
        // Scan employees table
        OperatorNode {
            operator_id: "scan_employees".to_string(),
            operator_type: OperatorType::MppScan {
                table_path: "/data/employees.parquet".to_string(),
                predicate: None,
            },
            dependencies: vec![],
            config: OperatorConfig::default(),
            input_schemas: vec![],
            output_schema: employees_schema.clone(),
        },
        
        // Scan departments table
        OperatorNode {
            operator_id: "scan_departments".to_string(),
            operator_type: OperatorType::MppScan {
                table_path: "/data/departments.parquet".to_string(),
                predicate: None,
            },
            dependencies: vec![],
            config: OperatorConfig::default(),
            input_schemas: vec![],
            output_schema: departments_schema.clone(),
        },
        
        // Join employees and departments
        OperatorNode {
            operator_id: "join_emp_dept".to_string(),
            operator_type: OperatorType::MppJoin {
                join_type: "inner".to_string(),
                condition: "employees.dept_id = departments.dept_id".to_string(),
            },
            dependencies: vec![0, 1], // Depends on both scans
            config: OperatorConfig::default(),
            input_schemas: vec![employees_schema.clone(), departments_schema.clone()],
            output_schema: Schema::new(vec![
                Field::new("emp_id", ArrowDataType::Int64, false),
                Field::new("name", ArrowDataType::Utf8, false),
                Field::new("dept_name", ArrowDataType::Utf8, false),
                Field::new("salary", ArrowDataType::Float64, false),
                Field::new("budget", ArrowDataType::Float64, false),
            ]),
        },
        
        // Aggregate by department
        OperatorNode {
            operator_id: "agg_by_dept".to_string(),
            operator_type: OperatorType::MppAggregate {
                functions: vec!["count".to_string(), "avg".to_string(), "sum".to_string()],
                group_by: vec!["dept_name".to_string()],
            },
            dependencies: vec![2], // Depends on join
            config: OperatorConfig::default(),
            input_schemas: vec![Schema::new(vec![
                Field::new("emp_id", ArrowDataType::Int64, false),
                Field::new("name", ArrowDataType::Utf8, false),
                Field::new("dept_name", ArrowDataType::Utf8, false),
                Field::new("salary", ArrowDataType::Float64, false),
                Field::new("budget", ArrowDataType::Float64, false),
            ])],
            output_schema: Schema::new(vec![
                Field::new("dept_name", ArrowDataType::Utf8, false),
                Field::new("emp_count", ArrowDataType::Int64, false),
                Field::new("avg_salary", ArrowDataType::Float64, false),
                Field::new("total_salary", ArrowDataType::Float64, false),
            ]),
        },
        
        // Sort by total salary
        OperatorNode {
            operator_id: "sort_by_total".to_string(),
            operator_type: OperatorType::MppSort {
                sort_columns: vec!["total_salary".to_string()],
            },
            dependencies: vec![3], // Depends on aggregation
            config: OperatorConfig::default(),
            input_schemas: vec![Schema::new(vec![
                Field::new("dept_name", ArrowDataType::Utf8, false),
                Field::new("emp_count", ArrowDataType::Int64, false),
                Field::new("avg_salary", ArrowDataType::Float64, false),
                Field::new("total_salary", ArrowDataType::Float64, false),
            ])],
            output_schema: Schema::new(vec![
                Field::new("dept_name", ArrowDataType::Utf8, false),
                Field::new("emp_count", ArrowDataType::Int64, false),
                Field::new("avg_salary", ArrowDataType::Float64, false),
                Field::new("total_salary", ArrowDataType::Float64, false),
            ]),
        },
    ];
    
    Ok(StageExecutionPlan {
        stage_id: "complex_stage_1".to_string(),
        stage_name: "Department Analysis Stage".to_string(),
        operators,
        dependencies: vec![],
        output_schema: Schema::new(vec![
            Field::new("dept_name", ArrowDataType::Utf8, false),
            Field::new("emp_count", ArrowDataType::Int64, false),
            Field::new("avg_salary", ArrowDataType::Float64, false),
            Field::new("total_salary", ArrowDataType::Float64, false),
        ]),
        estimated_rows: Some(50),
        estimated_memory_mb: Some(200),
    })
}
