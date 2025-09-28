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

//! Simple Validation Example
//! 
//! This example demonstrates basic functionality without complex dependencies

use oneengine::expression::ast::*;
use arrow::datatypes::*;
use arrow::array::*;
use arrow::record_batch::RecordBatch;
use anyhow::Result;
use tracing::{info, debug};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    info!("Starting Simple Validation Example");

    // Test 1: Basic expression evaluation
    test_basic_expressions()?;
    
    // Test 2: Arrow data types and arrays
    test_arrow_data_types()?;
    
    // Test 3: Record batch creation
    test_record_batch_creation()?;
    
    info!("Simple validation completed successfully!");
    Ok(())
}

fn test_basic_expressions() -> Result<()> {
    info!("Testing basic expressions...");
    
    // Create a simple literal expression
    let literal = Expression::Literal(Literal {
        value: datafusion_common::ScalarValue::Int32(Some(42)),
        data_type: ExprDataType::Int32,
    });
    
    // Create a column reference
    let column_ref = Expression::ColumnRef(ColumnRef {
        name: "age".to_string(),
        data_type: ExprDataType::Int32,
    });
    
    // Create a comparison expression
    let comparison = Expression::Comparison(ComparisonExpr {
        left: Box::new(column_ref),
        op: ComparisonOp::Gt,
        right: Box::new(literal),
        data_type: ExprDataType::Boolean,
    });
    
    info!("Created expressions successfully");
    debug!("Comparison expression: {:?}", comparison);
    
    Ok(())
}

fn test_arrow_data_types() -> Result<()> {
    info!("Testing Arrow data types...");
    
    // Create schema
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, false),
        Field::new("salary", DataType::Float64, false),
    ]);
    
    info!("Created schema with {} fields", schema.fields().len());
    
    // Create arrays
    let id_array = Int64Array::from(vec![1, 2, 3, 4, 5]);
    let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie", "David", "Eve"]);
    let age_array = Int32Array::from(vec![25, 30, 35, 28, 32]);
    let salary_array = Float64Array::from(vec![50000.0, 60000.0, 70000.0, 55000.0, 65000.0]);
    
    info!("Created arrays with {} rows each", id_array.len());
    
    Ok(())
}

fn test_record_batch_creation() -> Result<()> {
    info!("Testing RecordBatch creation...");
    
    // Create schema
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, false),
    ]);
    
    // Create arrays
    let id_array = Int64Array::from(vec![1, 2, 3]);
    let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie"]);
    let age_array = Int32Array::from(vec![25, 30, 35]);
    
    // Create record batch
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(id_array),
            Arc::new(name_array),
            Arc::new(age_array),
        ],
    )?;
    
    info!("Created RecordBatch with {} rows and {} columns", 
          batch.num_rows(), batch.num_columns());
    
    // Print some data
    for i in 0..batch.num_rows() {
        let id = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(i);
        let name = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap().value(i);
        let age = batch.column(2).as_any().downcast_ref::<Int32Array>().unwrap().value(i);
        
        info!("Row {}: id={}, name={}, age={}", i, id, name, age);
    }
    
    Ok(())
}
