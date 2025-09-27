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


//! 倾斜治理到算子层
//! 
//! Join/Agg识别重尾key后，局部盐化或单独通道处理

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use datafusion_common::ScalarValue;
use crate::push_runtime::{Event, OpStatus, Outbox, PortId};

/// 倾斜检测器
pub struct SkewDetector {
    /// 键值统计
    key_stats: HashMap<ScalarValue, KeyStatistics>,
    /// 倾斜阈值
    skew_threshold: f64,
    /// 采样率
    sampling_rate: f64,
    /// 重尾键集合
    heavy_tail_keys: HashSet<ScalarValue>,
}

/// 键值统计信息
#[derive(Debug, Clone)]
pub struct KeyStatistics {
    /// 出现次数
    count: u64,
    /// 首次出现时间
    first_seen: Instant,
    /// 最后出现时间
    last_seen: Instant,
    /// 数据大小（字节）
    data_size: u64,
    /// 是否被标记为重尾
    is_heavy_tail: bool,
}

/// 倾斜处理策略
pub enum SkewHandlingStrategy {
    /// 局部盐化
    LocalSalting,
    /// 单独通道处理
    SeparateChannel,
    /// 动态分区
    DynamicPartitioning,
    /// 混合策略
    Hybrid,
}

/// 盐化配置
pub struct SaltingConfig {
    /// 盐值数量
    salt_count: u32,
    /// 盐值分布策略
    distribution: SaltDistribution,
}

/// 盐值分布策略
pub enum SaltDistribution {
    /// 均匀分布
    Uniform,
    /// 加权分布（基于键值频率）
    Weighted,
    /// 哈希分布
    Hash,
}

/// 倾斜处理器
pub struct SkewHandler {
    /// 倾斜检测器
    detector: SkewDetector,
    /// 处理策略
    strategy: SkewHandlingStrategy,
    /// 盐化配置
    salting_config: SaltingConfig,
    /// 重尾键处理通道
    heavy_tail_channels: HashMap<ScalarValue, HeavyTailChannel>,
    /// 性能统计
    performance_stats: SkewPerformanceStats,
}

/// 重尾键处理通道
pub struct HeavyTailChannel {
    /// 键值
    key: ScalarValue,
    /// 通道ID
    channel_id: PortId,
    /// 缓冲区
    buffer: VecDeque<RecordBatch>,
    /// 处理状态
    status: ChannelStatus,
    /// 创建时间
    created_at: Instant,
}

/// 通道状态
#[derive(Debug, Clone, PartialEq)]
pub enum ChannelStatus {
    /// 空闲
    Idle,
    /// 处理中
    Processing,
    /// 堵塞
    Blocked,
    /// 完成
    Completed,
}

/// 倾斜性能统计
#[derive(Debug, Clone)]
pub struct SkewPerformanceStats {
    /// 检测到的重尾键数量
    heavy_tail_keys_detected: u64,
    /// 处理的倾斜数据量
    skewed_data_processed: u64,
    /// 平均处理延迟
    avg_processing_latency: Duration,
    /// 盐化效果（性能提升百分比）
    salting_effectiveness: f64,
    /// 通道利用率
    channel_utilization: f64,
}

impl SkewDetector {
    /// 创建新的倾斜检测器
    pub fn new(skew_threshold: f64, sampling_rate: f64) -> Self {
        Self {
            key_stats: HashMap::new(),
            skew_threshold,
            sampling_rate,
            heavy_tail_keys: HashSet::new(),
        }
    }
    
    /// 分析批次数据，检测倾斜
    pub fn analyze_batch(&mut self, batch: &RecordBatch, key_columns: &[usize]) -> Vec<ScalarValue> {
        let mut detected_heavy_tails = Vec::new();
        
        for &col_idx in key_columns {
            if col_idx >= batch.num_columns() {
                continue;
            }
            
            let array = batch.column(col_idx);
            let key_values = self.extract_key_values(array);
            
            for key_value in key_values {
                if self.should_sample() {
                    self.update_key_statistics(&key_value, batch.num_rows() as u64);
                }
            }
        }
        
        // 检测重尾键
        for (key, stats) in &self.key_stats {
            if self.is_heavy_tail_key(stats) && !self.heavy_tail_keys.contains(key) {
                self.heavy_tail_keys.insert(key.clone());
                detected_heavy_tails.push(key.clone());
            }
        }
        
        detected_heavy_tails
    }
    
    /// 提取键值
    fn extract_key_values(&self, array: &dyn Array) -> Vec<ScalarValue> {
        let mut values = Vec::new();
        
        match array.data_type() {
            DataType::Int32 => {
                let int_array = array.as_any().downcast_ref::<Int32Array>().unwrap();
                for i in 0..int_array.len() {
                    if !int_array.is_null(i) {
                        let val = int_array.value(i);
                        values.push(ScalarValue::Int32(Some(val)));
                    }
                }
            },
            DataType::Int64 => {
                let int_array = array.as_any().downcast_ref::<Int64Array>().unwrap();
                for i in 0..int_array.len() {
                    if !int_array.is_null(i) {
                        let val = int_array.value(i);
                        values.push(ScalarValue::Int64(Some(val)));
                    }
                }
            },
            DataType::Utf8 => {
                let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
                for i in 0..string_array.len() {
                    if !string_array.is_null(i) {
                        let val = string_array.value(i);
                        values.push(ScalarValue::Utf8(Some(val.to_string())));
                    }
                }
            },
            DataType::Float32 => {
                let float_array = array.as_any().downcast_ref::<Float32Array>().unwrap();
                for i in 0..float_array.len() {
                    if !float_array.is_null(i) {
                        let val = float_array.value(i);
                        values.push(ScalarValue::Float32(Some(val)));
                    }
                }
            },
            DataType::Float64 => {
                let float_array = array.as_any().downcast_ref::<Float64Array>().unwrap();
                for i in 0..float_array.len() {
                    if !float_array.is_null(i) {
                        let val = float_array.value(i);
                        values.push(ScalarValue::Float64(Some(val)));
                    }
                }
            },
            _ => {
                // 对于其他类型，使用通用方法
                for i in 0..array.len() {
                    if !array.is_null(i) {
                        // 这里需要根据具体类型进行转换
                        // 简化实现，实际需要更完整的类型支持
                    }
                }
            }
        }
        
        values
    }
    
    /// 判断是否应该采样
    fn should_sample(&self) -> bool {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        Instant::now().hash(&mut hasher);
        let hash = hasher.finish();
        (hash % 1000) as f64 / 1000.0 < self.sampling_rate
    }
    
    /// 更新键值统计
    fn update_key_statistics(&mut self, key: &ScalarValue, batch_size: u64) {
        let now = Instant::now();
        
        let key_size = self.estimate_key_size(key);
        match self.key_stats.get_mut(key) {
            Some(stats) => {
                stats.count += batch_size;
                stats.last_seen = now;
                stats.data_size += key_size * batch_size;
            },
            None => {
                self.key_stats.insert(key.clone(), KeyStatistics {
                    count: batch_size,
                    first_seen: now,
                    last_seen: now,
                    data_size: self.estimate_key_size(key) * batch_size,
                    is_heavy_tail: false,
                });
            }
        }
    }
    
    /// 估算键值大小
    fn estimate_key_size(&self, key: &ScalarValue) -> u64 {
        match key {
            ScalarValue::Int32(_) => 4,
            ScalarValue::Int64(_) => 8,
            ScalarValue::Float32(_) => 4,
            ScalarValue::Float64(_) => 8,
            ScalarValue::Utf8(Some(s)) => s.len() as u64,
            ScalarValue::Utf8(None) => 0,
            _ => 8, // 默认大小
        }
    }
    
    /// 判断是否为重尾键
    fn is_heavy_tail_key(&self, stats: &KeyStatistics) -> bool {
        let total_count: u64 = self.key_stats.values().map(|s| s.count).sum();
        if total_count == 0 {
            return false;
        }
        
        let key_ratio = stats.count as f64 / total_count as f64;
        key_ratio > self.skew_threshold
    }
    
    /// 获取重尾键列表
    pub fn get_heavy_tail_keys(&self) -> Vec<ScalarValue> {
        self.heavy_tail_keys.iter().cloned().collect()
    }
    
    /// 重置统计信息
    pub fn reset(&mut self) {
        self.key_stats.clear();
        self.heavy_tail_keys.clear();
    }
}

impl SkewHandler {
    /// 创建新的倾斜处理器
    pub fn new(
        skew_threshold: f64,
        sampling_rate: f64,
        strategy: SkewHandlingStrategy,
        salting_config: SaltingConfig,
    ) -> Self {
        Self {
            detector: SkewDetector::new(skew_threshold, sampling_rate),
            strategy,
            salting_config,
            heavy_tail_channels: HashMap::new(),
            performance_stats: SkewPerformanceStats::new(),
        }
    }
    
    /// 处理批次数据
    pub fn process_batch(
        &mut self,
        batch: RecordBatch,
        key_columns: &[usize],
        outbox: &mut Outbox<'_>,
    ) -> Result<OpStatus, String> {
        // 检测倾斜
        let heavy_tail_keys = self.detector.analyze_batch(&batch, key_columns);
        
        // 更新性能统计
        self.performance_stats.heavy_tail_keys_detected += heavy_tail_keys.len() as u64;
        
        // 根据策略处理
        match self.strategy {
            SkewHandlingStrategy::LocalSalting => {
                self.handle_with_salting(batch, &heavy_tail_keys, outbox)
            },
            SkewHandlingStrategy::SeparateChannel => {
                self.handle_with_separate_channels(batch, &heavy_tail_keys, outbox)
            },
            SkewHandlingStrategy::DynamicPartitioning => {
                self.handle_with_dynamic_partitioning(batch, &heavy_tail_keys, outbox)
            },
            SkewHandlingStrategy::Hybrid => {
                self.handle_with_hybrid_strategy(batch, &heavy_tail_keys, outbox)
            },
        }
    }
    
    /// 使用盐化策略处理
    fn handle_with_salting(
        &mut self,
        batch: RecordBatch,
        heavy_tail_keys: &[ScalarValue],
        outbox: &mut Outbox<'_>,
    ) -> Result<OpStatus, String> {
        let mut salted_batches = Vec::new();
        
        // 为每个重尾键创建盐化批次
        for heavy_key in heavy_tail_keys {
            for salt in 0..self.salting_config.salt_count {
                let salted_batch = self.apply_salting(&batch, heavy_key, salt)?;
                salted_batches.push(salted_batch);
            }
        }
        
        // 发送盐化后的批次
        for (i, salted_batch) in salted_batches.into_iter().enumerate() {
            outbox.send(0, Event::Data { port: i as PortId, batch: salted_batch });
        }
        
        Ok(OpStatus::Ready)
    }
    
    /// 使用单独通道策略处理
    fn handle_with_separate_channels(
        &mut self,
        batch: RecordBatch,
        heavy_tail_keys: &[ScalarValue],
        outbox: &mut Outbox<'_>,
    ) -> Result<OpStatus, String> {
        // 为每个重尾键创建或获取专用通道
        for heavy_key in heavy_tail_keys {
            if !self.heavy_tail_channels.contains_key(heavy_key) {
                let channel_id = self.heavy_tail_channels.len() as PortId + 1000; // 避免端口冲突
                let channel = HeavyTailChannel {
                    key: heavy_key.clone(),
                    channel_id,
                    buffer: VecDeque::new(),
                    status: ChannelStatus::Idle,
                    created_at: Instant::now(),
                };
                self.heavy_tail_channels.insert(heavy_key.clone(), channel);
            }
            
            // 将批次添加到对应通道
            if let Some(channel) = self.heavy_tail_channels.get_mut(heavy_key) {
                channel.buffer.push_back(batch.clone());
                channel.status = ChannelStatus::Processing;
            }
        }
        
        // 处理通道中的批次
        self.process_channels(outbox)?;
        
        Ok(OpStatus::Ready)
    }
    
    /// 使用动态分区策略处理
    fn handle_with_dynamic_partitioning(
        &mut self,
        batch: RecordBatch,
        heavy_tail_keys: &[ScalarValue],
        outbox: &mut Outbox<'_>,
    ) -> Result<OpStatus, String> {
        // 根据重尾键动态调整分区策略
        let partition_count = (heavy_tail_keys.len() + 1).max(2);
        
        // 将批次按重尾键分组
        let mut partitioned_batches = vec![Vec::new(); partition_count];
        
        // 实现基于重尾键的分区逻辑
        let mut key_to_partition = std::collections::HashMap::new();
        
        // 为每个重尾键分配一个专门的分区
        for (i, key) in heavy_tail_keys.iter().enumerate() {
            key_to_partition.insert(key.clone(), i);
        }
        
        // 将批次按键值分配到不同分区
        let batches_by_key = self.split_batch_by_keys(batch, &heavy_tail_keys)?;
        
        for (key, key_batches) in batches_by_key {
            if let Some(&partition_id) = key_to_partition.get(&key) {
                partitioned_batches[partition_id].extend(key_batches);
            } else {
                // 非重尾键分配到最后一个分区
                partitioned_batches[partition_count - 1].extend(key_batches);
            }
        }
        
        // 发送分区后的批次
        for (i, partition_batches) in partitioned_batches.into_iter().enumerate() {
            for batch in partition_batches {
                outbox.send(0, Event::Data { port: i as PortId, batch });
            }
        }
        
        Ok(OpStatus::Ready)
    }
    
    /// 使用混合策略处理
    fn handle_with_hybrid_strategy(
        &mut self,
        batch: RecordBatch,
        heavy_tail_keys: &[ScalarValue],
        outbox: &mut Outbox<'_>,
    ) -> Result<OpStatus, String> {
        // 根据重尾键的严重程度选择不同策略
        let mut light_skew_keys = Vec::new();
        let mut heavy_skew_keys = Vec::new();
        
        for key in heavy_tail_keys {
            if let Some(stats) = self.detector.key_stats.get(key) {
                let skew_ratio = stats.count as f64 / self.detector.key_stats.values().map(|s| s.count).sum::<u64>() as f64;
                if skew_ratio > 0.5 {
                    heavy_skew_keys.push(key.clone());
                } else {
                    light_skew_keys.push(key.clone());
                }
            }
        }
        
        // 轻度倾斜使用盐化
        if !light_skew_keys.is_empty() {
            self.handle_with_salting(batch.clone(), &light_skew_keys, outbox)?;
        }
        
        // 重度倾斜使用单独通道
        if !heavy_skew_keys.is_empty() {
            self.handle_with_separate_channels(batch, &heavy_skew_keys, outbox)?;
        }
        
        Ok(OpStatus::Ready)
    }
    
    /// 应用盐化
    fn apply_salting(&self, batch: &RecordBatch, key: &ScalarValue, salt: u32) -> Result<RecordBatch, String> {
        // 这里需要实现具体的盐化逻辑
        // 简化实现：返回原批次
        Ok(batch.clone())
    }
    
    /// 处理通道
    fn process_channels(&mut self, outbox: &mut Outbox<'_>) -> Result<(), String> {
        let mut completed_channels = Vec::new();
        
        for (key, channel) in &mut self.heavy_tail_channels {
            if channel.status == ChannelStatus::Processing && !channel.buffer.is_empty() {
                if let Some(batch) = channel.buffer.pop_front() {
                    outbox.send(0, Event::Data { port: channel.channel_id, batch });
                }
                
                if channel.buffer.is_empty() {
                    channel.status = ChannelStatus::Completed;
                    completed_channels.push(key.clone());
                }
            }
        }
        
        // 清理已完成的通道
        for key in completed_channels {
            self.heavy_tail_channels.remove(&key);
        }
        
        Ok(())
    }
    
    /// 按行分割批次
    fn split_batch_by_rows(&self, batch: RecordBatch, partition_count: usize) -> Vec<(usize, RecordBatch)> {
        // 将批次平均分配到各个分区
        let rows_per_partition = batch.num_rows() / partition_count;
        let mut result = Vec::new();
        
        for i in 0..partition_count {
            let start_row = i * rows_per_partition;
            let end_row = if i == partition_count - 1 {
                batch.num_rows()
            } else {
                (i + 1) * rows_per_partition
            };
            
            if start_row < end_row {
                // 实现具体的行分割逻辑
                let partition_batch = batch.slice(start_row, end_row - start_row);
                result.push((i, partition_batch));
            }
        }
        
        result
    }
    
    /// 按键值分割批次
    fn split_batch_by_keys(&self, batch: &RecordBatch, heavy_tail_keys: &[String]) -> Result<HashMap<String, Vec<RecordBatch>>, String> {
        use arrow::array::*;
        use arrow::datatypes::*;
        
        let mut key_to_batches: HashMap<String, Vec<RecordBatch>> = HashMap::new();
        
        // 假设第一列是键列
        if batch.num_columns() == 0 {
            return Ok(key_to_batches);
        }
        
        let key_column = batch.column(0);
        let schema = batch.schema();
        
        // 根据键列类型进行分组
        match key_column.data_type() {
            DataType::Utf8 => {
                if let Some(string_array) = key_column.as_any().downcast_ref::<StringArray>() {
                    for (row_idx, key_opt) in string_array.iter().enumerate() {
                        if let Some(key) = key_opt {
                            let key_str = key.to_string();
                            
                            // 检查是否在重尾键列表中
                            if heavy_tail_keys.contains(&key_str) {
                                // 创建单行批次
                                let single_row_batch = self.create_single_row_batch(batch, row_idx)?;
                                
                                key_to_batches.entry(key_str)
                                    .or_insert_with(Vec::new)
                                    .push(single_row_batch);
                            }
                        }
                    }
                }
            },
            DataType::Int32 => {
                if let Some(int_array) = key_column.as_any().downcast_ref::<Int32Array>() {
                    for (row_idx, key_opt) in int_array.iter().enumerate() {
                        if let Some(key) = key_opt {
                            let key_str = key.to_string();
                            
                            if heavy_tail_keys.contains(&key_str) {
                                let single_row_batch = self.create_single_row_batch(batch, row_idx)?;
                                
                                key_to_batches.entry(key_str)
                                    .or_insert_with(Vec::new)
                                    .push(single_row_batch);
                            }
                        }
                    }
                }
            },
            _ => {
                // 对于其他类型，使用默认分组
                let default_batch = batch.clone();
                key_to_batches.insert("default".to_string(), vec![default_batch]);
            }
        }
        
        Ok(key_to_batches)
    }
    
    /// 创建单行批次
    fn create_single_row_batch(&self, batch: &RecordBatch, row_idx: usize) -> Result<RecordBatch, String> {
        let mut single_row_columns = Vec::new();
        
        for column in batch.columns() {
            let single_row_array = column.slice(row_idx, 1);
            single_row_columns.push(single_row_array);
        }
        
        RecordBatch::try_new(batch.schema(), single_row_columns)
            .map_err(|e| format!("Failed to create single row batch: {}", e))
    }
    
    /// 获取性能统计
    pub fn get_performance_stats(&self) -> &SkewPerformanceStats {
        &self.performance_stats
    }
    
    /// 重置处理器
    pub fn reset(&mut self) {
        self.detector.reset();
        self.heavy_tail_channels.clear();
        self.performance_stats = SkewPerformanceStats::new();
    }
}

impl SkewPerformanceStats {
    /// 创建新的性能统计
    pub fn new() -> Self {
        Self {
            heavy_tail_keys_detected: 0,
            skewed_data_processed: 0,
            avg_processing_latency: Duration::from_micros(0),
            salting_effectiveness: 0.0,
            channel_utilization: 0.0,
        }
    }
}

impl Default for SkewDetector {
    fn default() -> Self {
        Self::new(0.1, 0.01) // 10%倾斜阈值，1%采样率
    }
}

impl Default for SkewHandler {
    fn default() -> Self {
        Self::new(
            0.1, // 10%倾斜阈值
            0.01, // 1%采样率
            SkewHandlingStrategy::Hybrid,
            SaltingConfig {
                salt_count: 4,
                distribution: SaltDistribution::Uniform,
            },
        )
    }
}

impl Default for SkewPerformanceStats {
    fn default() -> Self {
        Self::new()
    }
}
