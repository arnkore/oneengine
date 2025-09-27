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


//! Parquet 和 Arrow IPC 集成示例
//! 
//! 演示 Parquet 文件读取、Arrow IPC 溢写和恢复

use oneengine::io::parquet_reader::{ParquetReader, ParquetReaderConfig, ColumnSelection};
use oneengine::io::arrow_ipc::{ArrowIpcWriter, ArrowIpcReader};
use arrow::record_batch::RecordBatch;
use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{Schema, Field, DataType};
use std::fs;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Parquet 和 Arrow IPC 集成示例 ===");
    
    // 创建临时目录
    let temp_dir = std::env::temp_dir();
    let parquet_path = temp_dir.join("test.parquet");
    let ipc_path = temp_dir.join("test.arrow");
    
    // 1. 创建测试数据
    println!("1. 创建测试数据...");
    create_test_parquet_file(&parquet_path)?;
    
    // 2. 读取 Parquet 文件
    println!("2. 读取 Parquet 文件...");
    let mut reader = ParquetReader::new(
        parquet_path.to_string_lossy().to_string(),
        ParquetReaderConfig {
            column_selection: ColumnSelection {
                select_all: true,
                columns: vec![],
            },
            predicates: vec![],
            enable_rowgroup_pruning: true,
            enable_page_index_selection: true,
            batch_size: 1000,
            max_rowgroups: Some(1000),
        }
    );
    
    reader.open()?;
    let batches = reader.read_batches()?;
    println!("   读取到 {} 个批次", batches.len());
    
    // 显示第一个批次的信息
    if let Some(first_batch) = batches.first() {
        println!("   第一个批次: {} 行, {} 列", 
                 first_batch.num_rows(), 
                 first_batch.num_columns());
    }
    
    // 3. 使用 Arrow IPC 溢写数据
    println!("3. 使用 Arrow IPC 溢写数据...");
    let mut writer = ArrowIpcWriter::new(ipc_path.to_string_lossy().to_string());
    
    // 使用第一个批次的 schema 初始化写入器
    if let Some(first_batch) = batches.first() {
        writer.initialize(first_batch.schema().as_ref().clone())?;
    } else {
        return Err("No batches to write".into());
    }
    
    for (i, batch) in batches.iter().enumerate() {
        writer.write_batch(batch)?;
        println!("   写入批次 {}: {} 行", i + 1, batch.num_rows());
    }
    
    writer.finish()?;
    println!("   溢写完成");
    
    // 4. 从 Arrow IPC 恢复数据
    println!("4. 从 Arrow IPC 恢复数据...");
    let mut ipc_reader = ArrowIpcReader::new(ipc_path.to_string_lossy().to_string());
    ipc_reader.open()?;
    
    let mut restored_batches: Vec<RecordBatch> = Vec::new();
    while let Some(batch) = ipc_reader.read_next_batch()? {
        restored_batches.push(batch);
    }
    
    println!("   恢复 {} 个批次", restored_batches.len());
    
    // 5. 验证数据一致性
    println!("5. 验证数据一致性...");
    assert_eq!(batches.len(), restored_batches.len());
    
    for (i, (original, restored)) in batches.iter().zip(restored_batches.iter()).enumerate() {
        assert_eq!(original.num_rows(), restored.num_rows());
        assert_eq!(original.num_columns(), restored.num_columns());
        println!("   批次 {} 验证通过: {} 行, {} 列", 
                 i + 1, 
                 original.num_rows(), 
                 original.num_columns());
    }
    
    println!("✅ 所有测试通过！");
    
    // 6. 显示文件统计信息
    println!("6. 文件统计信息:");
    let stats = reader.get_file_stats()?;
    println!("   Parquet 文件: {} 行, {} 列, {} 字节", 
             stats.num_rows, 
             stats.num_columns, 
             stats.file_size);
    
    Ok(())
}

/// 创建测试 Parquet 文件
fn create_test_parquet_file(path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    // 创建测试数据
    let id_data = Int32Array::from(vec![1, 2, 3, 4, 5]);
    let name_data = StringArray::from(vec!["Alice", "Bob", "Charlie", "David", "Eve"]);
    
    // 创建 Schema
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]);
    
    // 创建 RecordBatch
    let batch = RecordBatch::try_new(
        std::sync::Arc::new(schema),
        vec![
            std::sync::Arc::new(id_data),
            std::sync::Arc::new(name_data),
        ]
    )?;
    
    // 写入 Parquet 文件
    let file = std::fs::File::create(path)?;
    let mut writer = parquet::arrow::arrow_writer::ArrowWriter::try_new(file, batch.schema(), None)?;
    writer.write(&batch)?;
    writer.close()?;
    
    Ok(())
}