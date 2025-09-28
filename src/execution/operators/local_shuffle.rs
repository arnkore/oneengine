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

//! 本地Shuffle算子
//! 
//! 支持基于哈希的本地数据重分布和分区

use arrow::record_batch::RecordBatch;
use arrow::array::*;
use arrow::datatypes::*;
use std::sync::Arc;
use std::collections::HashMap;
use std::time::Instant;
use tracing::{debug, info, warn};
use crate::execution::push_runtime::{Operator, Event, OpStatus, Outbox, PortId};
use anyhow::Result;

/// 本地Shuffle配置
#[derive(Debug, Clone)]
pub struct VectorizedLocalShuffleConfig {
    /// 分区数量
    pub num_partitions: usize,
    /// 分区键列名
    pub partition_keys: Vec<String>,
    /// 哈希函数类型
    pub hash_function: HashFunction,
    /// 缓冲区大小
    pub buffer_size: usize,
    /// 是否启用压缩
    pub enable_compression: bool,
    /// 是否启用并行处理
    pub enable_parallel: bool,
}

/// 哈希函数类型
#[derive(Debug, Clone)]
pub enum HashFunction {
    /// 简单哈希
    Simple,
    /// Murmur3哈希
    Murmur3,
    /// CityHash
    CityHash,
    /// XXHash
    XXHash,
}

impl Default for VectorizedLocalShuffleConfig {
    fn default() -> Self {
        Self {
            num_partitions: 4,
            partition_keys: vec![],
            hash_function: HashFunction::Simple,
            buffer_size: 1024 * 1024, // 1MB
            enable_compression: false,
            enable_parallel: true,
        }
    }
}

/// 本地Shuffle算子
pub struct VectorizedLocalShuffle {
    config: VectorizedLocalShuffleConfig,
    input_ports: Vec<PortId>,
    output_ports: Vec<PortId>,
    operator_id: u32,
    name: String,
    
    // 分区缓冲区
    partition_buffers: HashMap<usize, Vec<RecordBatch>>,
    // 分区键列索引
    partition_key_indices: Vec<usize>,
    // 统计信息
    total_rows_processed: u64,
    total_bytes_processed: u64,
    shuffle_start_time: Option<Instant>,
}

impl VectorizedLocalShuffle {
    pub fn new(config: VectorizedLocalShuffleConfig, operator_id: u32) -> Self {
        Self {
            config,
            input_ports: vec![],
            output_ports: vec![],
            operator_id,
            name: format!("VectorizedLocalShuffle_{}", operator_id),
            partition_buffers: HashMap::new(),
            partition_key_indices: vec![],
            total_rows_processed: 0,
            total_bytes_processed: 0,
            shuffle_start_time: None,
        }
    }
    
    /// 设置输入输出端口
    pub fn set_ports(&mut self, input_ports: Vec<PortId>, output_ports: Vec<PortId>) {
        self.input_ports = input_ports;
        self.output_ports = output_ports;
    }
    
    /// 处理数据批次
    pub fn process_batch(&mut self, batch: &RecordBatch) -> Result<Vec<(usize, RecordBatch)>> {
        if self.shuffle_start_time.is_none() {
            self.shuffle_start_time = Some(Instant::now());
        }
        
        // 获取分区键列索引
        if self.partition_key_indices.is_empty() {
            self.partition_key_indices = self.get_partition_key_indices(&batch.schema())?;
        }
        
        // 计算分区
        let partitioned_batches = self.partition_batch(batch)?;
        
        // 更新统计信息
        self.total_rows_processed += batch.num_rows() as u64;
        self.total_bytes_processed += batch.get_array_memory_size() as u64;
        
        debug!("Processed batch: {} rows, partitioned into {} partitions", 
               batch.num_rows(), partitioned_batches.len());
        
        Ok(partitioned_batches)
    }
    
    /// 获取分区键列索引
    fn get_partition_key_indices(&self, schema: &Schema) -> Result<Vec<usize>> {
        let mut indices = Vec::new();
        
        for key in &self.config.partition_keys {
            if let Some(index) = schema.fields().iter().position(|f| f.name() == key) {
                indices.push(index);
            } else {
                return Err(anyhow::anyhow!("Partition key column '{}' not found in schema", key));
            }
        }
        
        Ok(indices)
    }
    
    /// 分区批次
    fn partition_batch(&self, batch: &RecordBatch) -> Result<Vec<(usize, RecordBatch)>> {
        let mut partition_map: HashMap<usize, Vec<usize>> = HashMap::new();
        
        // 计算每行的分区
        for row_idx in 0..batch.num_rows() {
            let partition_id = self.compute_partition(batch, row_idx)?;
            partition_map.entry(partition_id).or_insert_with(Vec::new).push(row_idx);
        }
        
        // 为每个分区创建批次
        let mut partitioned_batches = Vec::new();
        for (partition_id, row_indices) in partition_map {
            if !row_indices.is_empty() {
                let partition_batch = self.create_partition_batch(batch, &row_indices)?;
                partitioned_batches.push((partition_id, partition_batch));
            }
        }
        
        Ok(partitioned_batches)
    }
    
    /// 计算行的分区ID
    fn compute_partition(&self, batch: &RecordBatch, row_idx: usize) -> Result<usize> {
        let mut hash_value = 0u64;
        
        // 计算分区键的哈希值
        for &key_idx in &self.partition_key_indices {
            let array = batch.column(key_idx);
            let row_hash = self.compute_row_hash(array, row_idx)?;
            hash_value = self.combine_hashes(hash_value, row_hash);
        }
        
        // 如果没有任何分区键，使用行号
        if self.partition_key_indices.is_empty() {
            hash_value = row_idx as u64;
        }
        
        Ok((hash_value as usize) % self.config.num_partitions)
    }
    
    /// 计算行的哈希值
    fn compute_row_hash(&self, array: &ArrayRef, row_idx: usize) -> Result<u64> {
        match array.data_type() {
            DataType::Int8 => {
                let array = array.as_any().downcast_ref::<Int8Array>().unwrap();
                Ok(self.hash_value(array.value(row_idx) as i64))
            }
            DataType::Int16 => {
                let array = array.as_any().downcast_ref::<Int16Array>().unwrap();
                Ok(self.hash_value(array.value(row_idx) as i64))
            }
            DataType::Int32 => {
                let array = array.as_any().downcast_ref::<Int32Array>().unwrap();
                Ok(self.hash_value(array.value(row_idx) as i64))
            }
            DataType::Int64 => {
                let array = array.as_any().downcast_ref::<Int64Array>().unwrap();
                Ok(self.hash_value(array.value(row_idx)))
            }
            DataType::Utf8 => {
                let array = array.as_any().downcast_ref::<StringArray>().unwrap();
                Ok(self.hash_string(array.value(row_idx)))
            }
            DataType::Float32 => {
                let array = array.as_any().downcast_ref::<Float32Array>().unwrap();
                Ok(self.hash_value(array.value(row_idx).to_bits() as i64))
            }
            DataType::Float64 => {
                let array = array.as_any().downcast_ref::<Float64Array>().unwrap();
                Ok(self.hash_value(array.value(row_idx).to_bits() as i64))
            }
            DataType::Boolean => {
                let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                Ok(self.hash_value(if array.value(row_idx) { 1 } else { 0 }))
            }
            _ => {
                // 对于其他类型，使用字符串表示进行哈希
                let string_value = format!("{:?}", array);
                Ok(self.hash_string(&string_value))
            }
        }
    }
    
    /// 哈希整数值
    fn hash_value(&self, value: i64) -> u64 {
        match self.config.hash_function {
            HashFunction::Simple => self.simple_hash(value),
            HashFunction::Murmur3 => self.murmur3_hash(value),
            HashFunction::CityHash => self.city_hash(value),
            HashFunction::XXHash => self.xx_hash(value),
        }
    }
    
    /// 哈希字符串值
    fn hash_string(&self, value: &str) -> u64 {
        match self.config.hash_function {
            HashFunction::Simple => self.simple_hash_string(value),
            HashFunction::Murmur3 => self.murmur3_hash_string(value),
            HashFunction::CityHash => self.city_hash_string(value),
            HashFunction::XXHash => self.xx_hash_string(value),
        }
    }
    
    /// 简单哈希
    fn simple_hash(&self, value: i64) -> u64 {
        let mut hash = value as u64;
        hash = hash.wrapping_mul(0x9e3779b9);
        hash ^= hash >> 13;
        hash = hash.wrapping_mul(0x9e3779b9);
        hash ^= hash >> 13;
        hash
    }
    
    /// 简单字符串哈希
    fn simple_hash_string(&self, value: &str) -> u64 {
        let mut hash = 0u64;
        for byte in value.bytes() {
            hash = hash.wrapping_mul(31).wrapping_add(byte as u64);
        }
        hash
    }
    
    /// Murmur3哈希
    fn murmur3_hash(&self, value: i64) -> u64 {
        let mut hash = value as u64;
        hash ^= hash >> 33;
        hash = hash.wrapping_mul(0xff51afd7ed558ccd);
        hash ^= hash >> 33;
        hash = hash.wrapping_mul(0xc4ceb9fe1a85ec53);
        hash ^= hash >> 33;
        hash
    }
    
    /// Murmur3字符串哈希
    fn murmur3_hash_string(&self, value: &str) -> u64 {
        let mut hash = 0u64;
        for byte in value.bytes() {
            hash = hash.wrapping_mul(0xc4ceb9fe1a85ec53);
            hash ^= byte as u64;
        }
        hash
    }
    
    /// CityHash
    fn city_hash(&self, value: i64) -> u64 {
        let mut hash = value as u64;
        hash = hash.wrapping_mul(0x9ddfea08eb382d69);
        hash ^= hash >> 33;
        hash = hash.wrapping_mul(0xc4ceb9fe1a85ec53);
        hash ^= hash >> 33;
        hash
    }
    
    /// CityHash字符串
    fn city_hash_string(&self, value: &str) -> u64 {
        let mut hash = 0u64;
        for byte in value.bytes() {
            hash = hash.wrapping_mul(0x9ddfea08eb382d69);
            hash ^= byte as u64;
        }
        hash
    }
    
    /// XXHash
    fn xx_hash(&self, value: i64) -> u64 {
        let mut hash = value as u64;
        hash = hash.wrapping_mul(0x9e3779b97f4a7c15);
        hash ^= hash >> 33;
        hash = hash.wrapping_mul(0x85ebca77c2b2ae3d);
        hash ^= hash >> 33;
        hash
    }
    
    /// XXHash字符串
    fn xx_hash_string(&self, value: &str) -> u64 {
        let mut hash = 0u64;
        for byte in value.bytes() {
            hash = hash.wrapping_mul(0x85ebca77c2b2ae3d);
            hash ^= byte as u64;
        }
        hash
    }
    
    /// 合并哈希值
    fn combine_hashes(&self, hash1: u64, hash2: u64) -> u64 {
        hash1.wrapping_mul(0x9e3779b9).wrapping_add(hash2)
    }
    
    /// 创建分区批次
    fn create_partition_batch(&self, batch: &RecordBatch, row_indices: &[usize]) -> Result<RecordBatch> {
        let mut new_columns = Vec::new();
        
        for column in batch.columns() {
            let new_column = self.take_rows(column, row_indices)?;
            new_columns.push(new_column);
        }
        
        Ok(RecordBatch::try_new(batch.schema(), new_columns)?)
    }
    
    /// 提取指定行的数据
    fn take_rows(&self, array: &ArrayRef, row_indices: &[usize]) -> Result<ArrayRef> {
        let indices = UInt32Array::from(
            row_indices.iter().map(|&i| i as u32).collect::<Vec<_>>()
        );
        
        Ok(arrow::compute::take(array, &indices, None)?)
    }
    
    /// 获取统计信息
    pub fn get_stats(&self) -> ShuffleStats {
        ShuffleStats {
            total_rows_processed: self.total_rows_processed,
            total_bytes_processed: self.total_bytes_processed,
            num_partitions: self.config.num_partitions,
            processing_time: self.shuffle_start_time.map(|start| start.elapsed()).unwrap_or_default(),
        }
    }
}

/// Shuffle统计信息
#[derive(Debug, Clone)]
pub struct ShuffleStats {
    pub total_rows_processed: u64,
    pub total_bytes_processed: u64,
    pub num_partitions: usize,
    pub processing_time: std::time::Duration,
}

/// 实现Operator trait
impl Operator for VectorizedLocalShuffle {
    fn on_event(&mut self, ev: Event, out: &mut Outbox) -> OpStatus {
        match ev {
            Event::Data { port, batch } => {
                if self.input_ports.contains(&port) {
                    match self.process_batch(&batch) {
                        Ok(partitioned_batches) => {
                            // 发送分区后的数据到对应的输出端口
                            for (partition_id, partition_batch) in partitioned_batches {
                                if partition_id < self.output_ports.len() {
                                    let output_port = self.output_ports[partition_id];
                                    out.send(output_port, partition_batch);
                                }
                            }
                            OpStatus::Ready
                        }
                        Err(e) => {
                            warn!("Failed to process batch: {}", e);
                            OpStatus::Error(format!("Failed to process batch: {}", e))
                        }
                    }
                } else {
                    OpStatus::Ready
                }
            }
            Event::Flush(port) => {
                if self.input_ports.contains(&port) {
                    // 刷新所有分区的缓冲区
                    for &output_port in &self.output_ports {
                        out.emit_flush(output_port);
                    }
                    OpStatus::Ready
                } else {
                    OpStatus::Ready
                }
            }
            Event::Finish(port) => {
                if self.input_ports.contains(&port) {
                    // 完成所有输出
                    for &output_port in &self.output_ports {
                        out.emit_finish(output_port);
                    }
                    OpStatus::Finished
                } else {
                    OpStatus::Ready
                }
            }
            _ => OpStatus::Ready,
        }
    }
    
    fn is_finished(&self) -> bool {
        false // Shuffle算子通常不会自动完成
    }
    
    fn name(&self) -> &str {
        &self.name
    }
}
