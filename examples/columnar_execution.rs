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


use oneengine::columnar::batch::{Batch, BatchSchema};
use oneengine::columnar::column::{Column, AnyArray};
use oneengine::columnar::types::{DataType, Bitmap};
use anyhow::Result;

fn main() -> Result<()> {
    println!("OneEngine Columnar Execution Example");

    // Create a schema
    let mut schema = BatchSchema::new();
    schema.add_field("id".to_string(), DataType::Int32, false);
    schema.add_field("name".to_string(), DataType::Utf8, true);
    schema.add_field("score".to_string(), DataType::Float64, false);

    // Create sample data
    let id_data = AnyArray::Int32(vec![1, 2, 3, 4, 5]);
    let name_data = AnyArray::Utf8(vec![
        "Alice".to_string(),
        "Bob".to_string(),
        "Charlie".to_string(),
        "David".to_string(),
        "Eve".to_string(),
    ]);
    let score_data = AnyArray::Float64(vec![85.5, 92.0, 78.5, 96.0, 88.5]);

    // Create columns
    let id_column = Column::from_data(DataType::Int32, id_data, None)?;
    let name_column = Column::from_data(DataType::Utf8, name_data, None)?;
    let score_column = Column::from_data(DataType::Float64, score_data, None)?;

    // Create a batch
    let batch = Batch::from_columns(
        vec![id_column, name_column, score_column],
        schema,
    )?;

    println!("Created batch: {}", batch);
    println!("Schema: {}", batch.schema);
    println!("Stats: {:?}", batch.stats());

    // Demonstrate slicing
    let sliced = batch.slice(1, 3)?;
    println!("Sliced batch (rows 1-3): {}", sliced);

    // Demonstrate null handling
    let mut nulls = Bitmap::new(3);
    nulls.set(1, true); // Set second row as null
    
    let name_data_with_nulls = AnyArray::Utf8(vec![
        "Alice".to_string(),
        "Bob".to_string(),
        "Charlie".to_string(),
    ]);
    let name_column_with_nulls = Column::from_data(
        DataType::Utf8, 
        name_data_with_nulls, 
        Some(nulls)
    )?;
    
    println!("Column with nulls: {}", name_column_with_nulls);
    println!("Null count: {}", name_column_with_nulls.null_count());

    Ok(())
}
