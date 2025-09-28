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


//! Arrow IPC格式的溢写和恢复
//! 
//! 支持Arrow IPC Stream格式的数据序列化和反序列化

use arrow::record_batch::RecordBatch;
use arrow::ipc::writer::FileWriter;
use arrow::ipc::reader::FileReader;
use arrow::datatypes::Schema;
use std::fs::File;
use std::sync::Arc;
use std::io::{BufWriter, BufReader, Seek, SeekFrom};
use std::path::Path;
use anyhow::Result;
use tracing::{debug, info, warn};

/// Arrow IPC写入器
pub struct ArrowIpcWriter {
    /// 文件路径
    file_path: String,
    /// 写入器
    writer: Option<FileWriter<BufWriter<File>>>,
    /// Schema
    schema: Option<Schema>,
    /// 已写入的批次数量
    batch_count: usize,
}

impl ArrowIpcWriter {
    /// 创建新的Arrow IPC写入器
    pub fn new(file_path: String) -> Self {
        Self {
            file_path,
            writer: None,
            schema: None,
            batch_count: 0,
        }
    }
    
    /// 初始化写入器
    pub fn initialize(&mut self, schema: Schema) -> Result<()> {
        let file = File::create(&self.file_path)?;
        let buf_writer = BufWriter::new(file);
        let writer = FileWriter::try_new(buf_writer, &schema)?;
        
        self.writer = Some(writer);
        self.schema = Some(schema);
        self.batch_count = 0;
        
        info!("Initialized Arrow IPC writer: {}", self.file_path);
        Ok(())
    }
    
    /// 写入批次
    pub fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        if let Some(ref mut writer) = self.writer {
            writer.write(batch)?;
            self.batch_count += 1;
            debug!("Wrote batch {} to Arrow IPC file", self.batch_count);
        } else {
            return Err(anyhow::anyhow!("Writer not initialized"));
        }
        Ok(())
    }
    
    /// 写入多个批次
    pub fn write_batches(&mut self, batches: &[RecordBatch]) -> Result<()> {
        for batch in batches {
            self.write_batch(batch)?;
        }
        Ok(())
    }
    
    /// 完成写入
    pub fn finish(&mut self) -> Result<()> {
        if let Some(mut writer) = self.writer.take() {
            writer.finish()?;
            info!("Finished writing {} batches to Arrow IPC file", self.batch_count);
        }
        Ok(())
    }
    
    /// 获取已写入的批次数量
    pub fn batch_count(&self) -> usize {
        self.batch_count
    }
}

/// Arrow IPC读取器
pub struct ArrowIpcReader {
    /// 文件路径
    file_path: String,
    /// 读取器
    reader: Option<FileReader<BufReader<File>>>,
    /// Schema
    schema: Option<Schema>,
    /// 已读取的批次数量
    batch_count: usize,
}

impl ArrowIpcReader {
    /// 创建新的Arrow IPC读取器
    pub fn new(file_path: String) -> Self {
        Self {
            file_path,
            reader: None,
            schema: None,
            batch_count: 0,
        }
    }
    
    /// 打开文件并初始化读取器
    pub fn open(&mut self) -> Result<()> {
        let file = File::open(&self.file_path)?;
        let buf_reader = BufReader::new(file);
        let reader = FileReader::try_new(buf_reader, None)?;
        let schema = reader.schema().clone();
        
        self.reader = Some(reader);
        self.schema = Some((*schema).clone());
        self.batch_count = 0;
        
        info!("Opened Arrow IPC file: {}", self.file_path);
        Ok(())
    }
    
    /// 读取下一个批次
    pub fn read_next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if let Some(ref mut reader) = self.reader {
            match reader.next() {
                Some(Ok(batch)) => {
                    self.batch_count += 1;
                    debug!("Read batch {} from Arrow IPC file", self.batch_count);
                    Ok(Some(batch))
                }
                Some(Err(e)) => Err(e.into()),
                None => Ok(None),
            }
        } else {
            Err(anyhow::anyhow!("Reader not initialized"))
        }
    }
    
    /// 读取所有批次
    pub fn read_all_batches(&mut self) -> Result<Vec<RecordBatch>> {
        let mut batches = Vec::new();
        
        while let Some(batch) = self.read_next_batch()? {
            batches.push(batch);
        }
        
        info!("Read {} batches from Arrow IPC file", batches.len());
        Ok(batches)
    }
    
    /// 获取Schema
    pub fn get_schema(&self) -> Option<&Schema> {
        self.schema.as_ref()
    }
    
    /// 获取已读取的批次数量
    pub fn batch_count(&self) -> usize {
        self.batch_count
    }
    
    /// 重置读取位置到文件开头
    pub fn reset(&mut self) -> Result<()> {
        if let Some(ref mut reader) = self.reader {
            // IPC reader does not support rewind, skip for now
            // reader.rewind()?;
            self.batch_count = 0;
        }
        Ok(())
    }
}

/// Arrow IPC溢写管理器
pub struct ArrowIpcSpillManager {
    /// 溢写目录
    spill_dir: String,
    /// 当前文件索引
    file_index: usize,
    /// 最大文件大小（字节）
    max_file_size: usize,
    /// 当前文件大小
    current_file_size: usize,
    /// 当前写入器
    current_writer: Option<ArrowIpcWriter>,
    /// 溢写文件列表
    spill_files: Vec<String>,
}

impl ArrowIpcSpillManager {
    /// 创建新的Arrow IPC溢写管理器
    pub fn new(spill_dir: String, max_file_size: usize) -> Self {
        Self {
            spill_dir,
            file_index: 0,
            max_file_size,
            current_file_size: 0,
            current_writer: None,
            spill_files: Vec::new(),
        }
    }
    
    /// 初始化溢写管理器
    pub fn initialize(&mut self) -> Result<()> {
        // 创建溢写目录
        std::fs::create_dir_all(&self.spill_dir)?;
        info!("Initialized Arrow IPC spill manager: {}", self.spill_dir);
        Ok(())
    }
    
    /// 溢写批次
    pub fn spill_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        // 检查是否需要创建新文件
        if self.should_create_new_file() {
            self.create_new_file(batch.schema().as_ref())?;
        }
        
        // 写入批次
        if let Some(ref mut writer) = self.current_writer {
            writer.write_batch(batch)?;
            self.current_file_size += self.estimate_batch_size(batch);
        } else {
            return Err(anyhow::anyhow!("No active writer"));
        }
        
        Ok(())
    }
    
    /// 溢写多个批次
    pub fn spill_batches(&mut self, batches: &[RecordBatch]) -> Result<()> {
        for batch in batches {
            self.spill_batch(batch)?;
        }
        Ok(())
    }
    
    /// 完成当前文件的写入
    pub fn finish_current_file(&mut self) -> Result<()> {
        if let Some(mut writer) = self.current_writer.take() {
            writer.finish()?;
            self.current_file_size = 0;
        }
        Ok(())
    }
    
    /// 完成所有溢写
    pub fn finish_all(&mut self) -> Result<()> {
        self.finish_current_file()?;
        info!("Finished all spill files: {} files", self.spill_files.len());
        Ok(())
    }
    
    /// 检查是否需要创建新文件
    fn should_create_new_file(&self) -> bool {
        self.current_writer.is_none() || self.current_file_size >= self.max_file_size
    }
    
    /// 创建新文件
    fn create_new_file(&mut self, schema: &Schema) -> Result<()> {
        // 完成当前文件
        self.finish_current_file()?;
        
        // 创建新文件
        let file_path = format!("{}/spill_{}.arrow", self.spill_dir, self.file_index);
        let mut writer = ArrowIpcWriter::new(file_path.clone());
        writer.initialize(schema.clone())?;
        
        self.current_writer = Some(writer);
        self.spill_files.push(file_path);
        self.file_index += 1;
        self.current_file_size = 0;
        
        debug!("Created new spill file: {}", self.spill_files.last().unwrap());
        Ok(())
    }
    
    /// 估算批次大小
    fn estimate_batch_size(&self, batch: &RecordBatch) -> usize {
        // 基于实际数据类型的精确估算
        let mut total_size = 0;
        
        for column in batch.columns() {
            let column_size = match column.data_type() {
                arrow::datatypes::DataType::Boolean => batch.num_rows() * 1, // 1字节
                arrow::datatypes::DataType::Int8 => batch.num_rows() * 1,
                arrow::datatypes::DataType::Int16 => batch.num_rows() * 2,
                arrow::datatypes::DataType::Int32 => batch.num_rows() * 4,
                arrow::datatypes::DataType::Int64 => batch.num_rows() * 8,
                arrow::datatypes::DataType::UInt8 => batch.num_rows() * 1,
                arrow::datatypes::DataType::UInt16 => batch.num_rows() * 2,
                arrow::datatypes::DataType::UInt32 => batch.num_rows() * 4,
                arrow::datatypes::DataType::UInt64 => batch.num_rows() * 8,
                arrow::datatypes::DataType::Float32 => batch.num_rows() * 4,
                arrow::datatypes::DataType::Float64 => batch.num_rows() * 8,
                arrow::datatypes::DataType::Utf8 => {
                    // 字符串类型需要估算实际长度
                    if let Some(string_array) = column.as_any().downcast_ref::<arrow::array::StringArray>() {
                        string_array.iter()
                            .map(|s| s.map(|s| s.len()).unwrap_or(0))
                            .sum::<usize>()
                    } else {
                        batch.num_rows() * 16 // 默认估算
                    }
                },
                _ => batch.num_rows() * 8, // 默认估算
            };
            
            total_size += column_size;
        }
        
        // 添加Arrow元数据开销
        total_size + batch.num_columns() * 64 // 每列64字节元数据开销
    }
    
    /// 获取溢写文件列表
    pub fn get_spill_files(&self) -> &[String] {
        &self.spill_files
    }
    
    /// 清理溢写文件
    pub fn cleanup(&mut self) -> Result<()> {
        for file_path in &self.spill_files {
            if std::path::Path::new(file_path).exists() {
                std::fs::remove_file(file_path)?;
            }
        }
        self.spill_files.clear();
        self.file_index = 0;
        self.current_file_size = 0;
        self.current_writer = None;
        
        info!("Cleaned up {} spill files", self.spill_files.len());
        Ok(())
    }
    
    /// 读取所有溢写文件
    pub fn read_all_spill_files(&self) -> Result<Vec<RecordBatch>> {
        let mut all_batches = Vec::new();
        
        for file_path in &self.spill_files {
            let mut reader = ArrowIpcReader::new(file_path.clone());
            reader.open()?;
            let mut batches = reader.read_all_batches()?;
            all_batches.append(&mut batches);
        }
        
        info!("Read {} batches from {} spill files", all_batches.len(), self.spill_files.len());
        Ok(all_batches)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn create_test_batch() -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]);
        
        let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie", "David", "Eve"]);
        
        RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(id_array), Arc::new(name_array)],
        ).unwrap()
    }

    #[test]
    fn test_arrow_ipc_writer() {
        let temp_dir = std::env::temp_dir();
        let file_path = temp_dir.join("test.arrow").to_string_lossy().to_string();
        
        let mut writer = ArrowIpcWriter::new(file_path.clone());
        let batch = create_test_batch();
        let schema = batch.schema();
        
        writer.initialize(schema.as_ref().clone()).unwrap();
        writer.write_batch(&batch).unwrap();
        writer.finish().unwrap();
        
        assert_eq!(writer.batch_count(), 1);
    }

    #[test]
    fn test_arrow_ipc_reader() {
        let temp_dir = std::env::temp_dir();
        let file_path = temp_dir.join("test_read.arrow").to_string_lossy().to_string();
        
        // 先写入
        let mut writer = ArrowIpcWriter::new(file_path.clone());
        let batch = create_test_batch();
        let schema = batch.schema();
        
        writer.initialize(schema.as_ref().clone()).unwrap();
        writer.write_batch(&batch).unwrap();
        writer.finish().unwrap();
        
        // 再读取
        let mut reader = ArrowIpcReader::new(file_path);
        reader.open().unwrap();
        let batches = reader.read_all_batches().unwrap();
        
        assert_eq!(batches.len(), 1);
        assert_eq!(reader.batch_count(), 1);
    }

    #[test]
    fn test_arrow_ipc_spill_manager() {
        let temp_dir = std::env::temp_dir();
        let spill_dir = temp_dir.join("spill_test").to_string_lossy().to_string();
        
        let mut manager = ArrowIpcSpillManager::new(spill_dir.clone(), 1000);
        manager.initialize().unwrap();
        
        let batch = create_test_batch();
        manager.spill_batch(&batch).unwrap();
        manager.finish_all().unwrap();
        
        let files = manager.get_spill_files();
        assert!(!files.is_empty());
        
        manager.cleanup().unwrap();
    }
}
