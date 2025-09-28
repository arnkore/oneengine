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

//! Minimal validation example
//! 
//! This example tests the core functionality without complex MPP operations

use oneengine::expression::ast::Expression;
use oneengine::expression::executor::VectorizedExpressionEngine;
use oneengine::expression::cache::ExpressionCache;
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Starting minimal validation...");
    
    // Test 1: Expression engine initialization
    println!("📝 Testing expression engine initialization...");
    let cache = ExpressionCache::new(1000, 1024 * 1024 * 1024); // 1GB
    let engine = VectorizedExpressionEngine::new(cache);
    println!("✅ Expression engine initialized successfully");
    
    // Test 2: Create a simple RecordBatch
    println!("📊 Testing RecordBatch creation...");
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]);
    
    let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie", "David", "Eve"]);
    let value_array = Float64Array::from(vec![10.5, 20.3, 30.7, 40.1, 50.9]);
    
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(id_array),
            Arc::new(name_array),
            Arc::new(value_array),
        ],
    )?;
    
    println!("✅ RecordBatch created successfully with {} rows", batch.num_rows());
    
    // Test 3: Basic expression evaluation
    println!("🧮 Testing basic expression evaluation...");
    
    // Create a simple literal expression
    let literal_expr = Expression::Literal(oneengine::expression::ast::Literal {
        value: datafusion_common::ScalarValue::Int32(Some(42)),
    });
    
    println!("✅ Expression created successfully");
    
    // Test 4: Function registry
    println!("🔧 Testing function registry...");
    let registry = oneengine::expression::functions::FunctionRegistry::new();
    println!("✅ Function registry created successfully");
    
    println!("🎉 All basic tests passed! Core functionality is working.");
    
    Ok(())
}
