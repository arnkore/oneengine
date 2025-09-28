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

//! Exchange算子
//! 
//! 支持分布式数据交换，包括shuffle、broadcast、repartition等操作

use arrow::record_batch::RecordBatch;
use arrow::array::*;
use arrow::datatypes::*;
use std::sync::Arc;
use std::collections::HashMap;
use std::time::Instant;
use tracing::{debug, info, warn};
use crate::execution::push_runtime::{Operator, Event, OpStatus, Outbox, PortId};
use anyhow::Result;

/// Exchange配置
#[derive(Debug, Clone)]
pub struct VectorizedExchangeConfig {
    /// 交换类型
    pub exchange_type: ExchangeType,
    /// 分区数量
    pub num_partitions: usize,
    /// 分区键列名
    pub partition_keys: Vec<String>,
    /// 目标节点列表
    pub target_nodes: Vec<String>,
    /// 缓冲区大小
    pub buffer_size: usize,
    /// 是否启用压缩
    pub enable_compression: bool,
    /// 是否启用并行处理
    pub enable_parallel: bool,
    /// 网络超时时间（毫秒）
    pub network_timeout_ms: u64,
}

/// 交换类型
#[derive(Debug, Clone)]
pub enum ExchangeType {
    /// Shuffle交换
    Shuffle,
    /// 广播交换
    Broadcast,
    /// 重新分区
    Repartition,
    /// 单分区
    SinglePartition,
    /// 随机分区
    RandomPartition,
}

impl Default for VectorizedExchangeConfig {
    fn default() -> Self {
        Self {
            exchange_type: ExchangeType::Shuffle,
            num_partitions: 4,
            partition_keys: vec![],
            target_nodes: vec![],
            buffer_size: 1024 * 1024, // 1MB
            enable_compression: false,
            enable_parallel: true,
            network_timeout_ms: 30000, // 30秒
        }
    }
}

/// Exchange算子
pub struct VectorizedExchange {
    config: VectorizedExchangeConfig,
    input_ports: Vec<PortId>,
    output_ports: Vec<PortId>,
    operator_id: u32,
    name: String,
    
    // 交换状态
    exchange_state: ExchangeState,
    // 分区键列索引
    partition_key_indices: Vec<usize>,
    // 分区缓冲区
    partition_buffers: HashMap<usize, Vec<RecordBatch>>,
    // 统计信息
    total_rows_processed: u64,
    total_bytes_processed: u64,
    exchange_start_time: Option<Instant>,
}

/// 交换状态
#[derive(Debug)]
enum ExchangeState {
    /// 收集数据阶段
    Collecting,
    /// 分区阶段
    Partitioning,
    /// 发送阶段
    Sending,
    /// 完成
    Finished,
}

impl VectorizedExchange {
    pub fn new(config: VectorizedExchangeConfig, operator_id: u32) -> Self {
        Self {
            config,
            input_ports: vec![],
            output_ports: vec![],
            operator_id,
            name: format!("VectorizedExchange_{}", operator_id),
            exchange_state: ExchangeState::Collecting,
            partition_key_indices: vec![],
            partition_buffers: HashMap::new(),
            total_rows_processed: 0,
            total_bytes_processed: 0,
            exchange_start_time: None,
        }
    }
    
    /// 设置输入输出端口
    pub fn set_ports(&mut self, input_ports: Vec<PortId>, output_ports: Vec<PortId>) {
        self.input_ports = input_ports;
        self.output_ports = output_ports;
    }
    
    /// 处理数据批次
    pub fn process_batch(&mut self, batch: &RecordBatch) -> Result<Vec<(usize, RecordBatch)>> {
        if self.exchange_start_time.is_none() {
            self.exchange_start_time = Some(Instant::now());
        }
        
        self.total_rows_processed += batch.num_rows() as u64;
        self.total_bytes_processed += batch.get_array_memory_size() as u64;
        
        match &self.exchange_state {
            ExchangeState::Collecting => {
                self.collect_batch(batch)?;
                Ok(vec![])
            }
            ExchangeState::Partitioning => {
                self.partition_batch(batch)
            }
            ExchangeState::Sending => {
                self.send_partitioned_data()
            }
            ExchangeState::Finished => {
                Ok(vec![])
            }
        }
    }
    
    /// 收集批次数据
    fn collect_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        // 获取分区键列索引
        if self.partition_key_indices.is_empty() {
            self.partition_key_indices = self.get_partition_key_indices(&batch.schema())?;
        }
        
        debug!("Collected batch: {} rows", batch.num_rows());
        
        // 根据交换类型决定是否立即分区
        match self.config.exchange_type {
            ExchangeType::Broadcast => {
                // 广播：立即发送到所有分区
                self.exchange_state = ExchangeState::Partitioning;
            }
            ExchangeType::SinglePartition => {
                // 单分区：发送到第一个分区
                self.exchange_state = ExchangeState::Partitioning;
            }
            _ => {
                // 其他类型：继续收集
                self.exchange_state = ExchangeState::Partitioning;
            }
        }
        
        Ok(())
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
    fn partition_batch(&mut self, batch: &RecordBatch) -> Result<Vec<(usize, RecordBatch)>> {
        let mut partitioned_batches = Vec::new();
        
        match self.config.exchange_type {
            ExchangeType::Shuffle => {
                partitioned_batches = self.shuffle_partition(batch)?;
            }
            ExchangeType::Broadcast => {
                partitioned_batches = self.broadcast_partition(batch)?;
            }
            ExchangeType::Repartition => {
                partitioned_batches = self.repartition_partition(batch)?;
            }
            ExchangeType::SinglePartition => {
                partitioned_batches = self.single_partition(batch)?;
            }
            ExchangeType::RandomPartition => {
                partitioned_batches = self.random_partition(batch)?;
            }
        }
        
        // 更新状态
        self.exchange_state = ExchangeState::Sending;
        
        Ok(partitioned_batches)
    }
    
    /// Shuffle分区
    fn shuffle_partition(&self, batch: &RecordBatch) -> Result<Vec<(usize, RecordBatch)>> {
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
    
    /// 广播分区
    fn broadcast_partition(&self, batch: &RecordBatch) -> Result<Vec<(usize, RecordBatch)>> {
        let mut partitioned_batches = Vec::new();
        
        // 发送到所有分区
        for partition_id in 0..self.config.num_partitions {
            partitioned_batches.push((partition_id, batch.clone()));
        }
        
        Ok(partitioned_batches)
    }
    
    /// 重新分区
    fn repartition_partition(&self, batch: &RecordBatch) -> Result<Vec<(usize, RecordBatch)>> {
        // 重新分区通常使用轮询方式
        let mut partitioned_batches = Vec::new();
        let mut partition_id = 0;
        
        for row_idx in 0..batch.num_rows() {
            let row_batch = self.create_single_row_batch(batch, row_idx)?;
            partitioned_batches.push((partition_id, row_batch));
            partition_id = (partition_id + 1) % self.config.num_partitions;
        }
        
        Ok(partitioned_batches)
    }
    
    /// 单分区
    fn single_partition(&self, batch: &RecordBatch) -> Result<Vec<(usize, RecordBatch)>> {
        Ok(vec![(0, batch.clone())])
    }
    
    /// 随机分区
    fn random_partition(&self, batch: &RecordBatch) -> Result<Vec<(usize, RecordBatch)>> {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut partitioned_batches = Vec::new();
        let mut partition_map: HashMap<usize, Vec<usize>> = HashMap::new();
        
        for row_idx in 0..batch.num_rows() {
            let mut hasher = DefaultHasher::new();
            row_idx.hash(&mut hasher);
            let partition_id = (hasher.finish() as usize) % self.config.num_partitions;
            partition_map.entry(partition_id).or_insert_with(Vec::new).push(row_idx);
        }
        
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
        let mut hash = value as u64;
        hash = hash.wrapping_mul(0x9e3779b9);
        hash ^= hash >> 13;
        hash = hash.wrapping_mul(0x9e3779b9);
        hash ^= hash >> 13;
        hash
    }
    
    /// 哈希字符串值
    fn hash_string(&self, value: &str) -> u64 {
        let mut hash = 0u64;
        for byte in value.bytes() {
            hash = hash.wrapping_mul(31).wrapping_add(byte as u64);
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
    
    /// 创建单行批次
    fn create_single_row_batch(&self, batch: &RecordBatch, row_idx: usize) -> Result<RecordBatch> {
        self.create_partition_batch(batch, &[row_idx])
    }
    
    /// 提取指定行的数据
    fn take_rows(&self, array: &ArrayRef, row_indices: &[usize]) -> Result<ArrayRef> {
        let indices = UInt32Array::from(
            row_indices.iter().map(|&i| i as u32).collect::<Vec<_>>()
        );
        
        Ok(arrow::compute::take(array, &indices, None)?)
    }
    
    /// 发送分区数据
    fn send_partitioned_data(&mut self) -> Result<Vec<(usize, RecordBatch)>> {
        // 这里应该实现实际的网络发送逻辑
        // 目前返回空向量
        self.exchange_state = ExchangeState::Finished;
        Ok(vec![])
    }
    
    /// 获取统计信息
    pub fn get_stats(&self) -> ExchangeStats {
        ExchangeStats {
            total_rows_processed: self.total_rows_processed,
            total_bytes_processed: self.total_bytes_processed,
            num_partitions: self.config.num_partitions,
            exchange_type: self.config.exchange_type.clone(),
            processing_time: self.exchange_start_time.map(|start| start.elapsed()).unwrap_or_default(),
        }
    }
}

/// Exchange统计信息
#[derive(Debug, Clone)]
pub struct ExchangeStats {
    pub total_rows_processed: u64,
    pub total_bytes_processed: u64,
    pub num_partitions: usize,
    pub exchange_type: ExchangeType,
    pub processing_time: std::time::Duration,
}

/// 实现Operator trait
impl Operator for VectorizedExchange {
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
        matches!(self.exchange_state, ExchangeState::Finished)
    }
    
    fn name(&self) -> &str {
        &self.name
    }
}
