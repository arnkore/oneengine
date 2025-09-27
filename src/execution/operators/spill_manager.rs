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


//! Arrow IPC Spill管理器
//! 
//! 为Agg/Sort/Join算子提供统一的spill功能

use crate::io::arrow_ipc::{ArrowIpcWriter, ArrowIpcReader};
use arrow::record_batch::RecordBatch;
use arrow::datatypes::Schema;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use anyhow::Result;
use tracing::{debug, info, warn};

/// Spill配置
#[derive(Debug, Clone)]
pub struct SpillConfig {
    /// 是否启用spill
    pub enabled: bool,
    /// spill目录
    pub spill_dir: String,
    /// 内存阈值（字节）
    pub memory_threshold: usize,
    /// 滞回阈值（字节）
    pub hysteresis_threshold: usize,
    /// 最大spill文件数
    pub max_spill_files: usize,
    /// 是否压缩
    pub compress: bool,
}

impl Default for SpillConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            spill_dir: "/tmp/oneengine_spill".to_string(),
            memory_threshold: 100 * 1024 * 1024, // 100MB
            hysteresis_threshold: 80 * 1024 * 1024, // 80MB
            max_spill_files: 1000,
            compress: true,
        }
    }
}

/// Spill文件信息
#[derive(Debug, Clone)]
pub struct SpillFileInfo {
    /// 文件路径
    pub file_path: String,
    /// 分区ID
    pub partition_id: u32,
    /// 批次数量
    pub batch_count: usize,
    /// 文件大小（字节）
    pub file_size: usize,
    /// 创建时间
    pub created_at: std::time::Instant,
}

/// 分区化spill管理器
pub struct PartitionedSpillManager {
    /// 配置
    config: SpillConfig,
    /// 当前内存使用量
    current_memory_usage: usize,
    /// 是否在spill状态
    is_spilling: bool,
    /// 分区spill文件信息
    partition_files: HashMap<u32, Vec<SpillFileInfo>>,
    /// 当前spill文件计数器
    spill_file_counter: usize,
    /// 算子ID
    operator_id: u32,
}

impl PartitionedSpillManager {
    /// 创建新的分区化spill管理器
    pub fn new(operator_id: u32, config: SpillConfig) -> Self {
        // 确保spill目录存在
        if let Err(e) = std::fs::create_dir_all(&config.spill_dir) {
            warn!("Failed to create spill directory {}: {}", config.spill_dir, e);
        }

        Self {
            config,
            current_memory_usage: 0,
            is_spilling: false,
            partition_files: HashMap::new(),
            spill_file_counter: 0,
            operator_id,
        }
    }

    /// 更新内存使用量
    pub fn update_memory_usage(&mut self, additional_bytes: usize) {
        self.current_memory_usage += additional_bytes;
    }

    /// 减少内存使用量
    pub fn decrease_memory_usage(&mut self, bytes: usize) {
        self.current_memory_usage = self.current_memory_usage.saturating_sub(bytes);
    }

    /// 检查是否应该spill
    pub fn should_spill(&self) -> bool {
        self.config.enabled && 
        !self.is_spilling && 
        self.current_memory_usage > self.config.memory_threshold
    }

    /// 检查是否可以停止spill
    pub fn can_stop_spilling(&self) -> bool {
        self.is_spilling && self.current_memory_usage < self.config.hysteresis_threshold
    }

    /// 获取当前内存使用量
    pub fn current_memory_usage(&self) -> usize {
        self.current_memory_usage
    }

    /// 是否正在spill
    pub fn is_spilling(&self) -> bool {
        self.is_spilling
    }

    /// 设置spill状态
    pub fn set_spilling(&mut self, spilling: bool) {
        self.is_spilling = spilling;
    }

    /// 创建spill文件路径
    fn create_spill_file_path(&mut self, partition_id: u32) -> String {
        self.spill_file_counter += 1;
        format!(
            "{}/operator_{}_partition_{}_spill_{}.arrow",
            self.config.spill_dir,
            self.operator_id,
            partition_id,
            self.spill_file_counter
        )
    }

    /// 将分区数据spill到磁盘
    pub fn spill_partition(&mut self, partition_id: u32, batches: Vec<RecordBatch>) -> Result<()> {
        if batches.is_empty() {
            return Ok(());
        }

        let file_path = self.create_spill_file_path(partition_id);
        let schema = batches[0].schema();
        
        // 创建IPC写入器
        let mut writer = ArrowIpcWriter::new(file_path.clone());
        writer.initialize((*schema).clone())?;
        
        let batch_count = batches.len();
        
        // 写入所有批次
        for batch in &batches {
            writer.write_batch(batch)?;
        }
        
        // 完成写入
        writer.finish()?;
        
        // 获取文件大小
        let file_size = std::fs::metadata(&file_path)?.len() as usize;
        
        // 记录文件信息
        let file_info = SpillFileInfo {
            file_path: file_path.clone(),
            partition_id,
            batch_count,
            file_size,
            created_at: std::time::Instant::now(),
        };
        
        self.partition_files
            .entry(partition_id)
            .or_insert_with(Vec::new)
            .push(file_info.clone());
        
        info!(
            "Spilled partition {} with {} batches ({} bytes) to {}",
            partition_id,
            batch_count,
            file_size,
            file_info.file_path
        );
        
        Ok(())
    }

    /// 从磁盘恢复分区数据
    pub fn restore_partition(&mut self, partition_id: u32) -> Result<Vec<RecordBatch>> {
        let files = self.partition_files
            .get(&partition_id)
            .ok_or_else(|| anyhow::anyhow!("No spill files found for partition {}", partition_id))?;
        
        let mut all_batches = Vec::new();
        
        for file_info in files {
            let mut reader = ArrowIpcReader::new(file_info.file_path.clone());
            reader.open()?;
            
            while let Some(batch) = reader.read_next_batch()? {
                all_batches.push(batch);
            }
            
            // reader.close()?; // ArrowIpcReader没有close方法
        }
        
        info!(
            "Restored partition {} with {} batches",
            partition_id,
            all_batches.len()
        );
        
        Ok(all_batches)
    }

    /// 清理分区spill文件
    pub fn cleanup_partition(&mut self, partition_id: u32) -> Result<()> {
        if let Some(files) = self.partition_files.remove(&partition_id) {
            for file_info in files {
                if let Err(e) = std::fs::remove_file(&file_info.file_path) {
                    warn!("Failed to remove spill file {}: {}", file_info.file_path, e);
                } else {
                    debug!("Removed spill file: {}", file_info.file_path);
                }
            }
        }
        Ok(())
    }

    /// 清理所有spill文件
    pub fn cleanup_all(&mut self) -> Result<()> {
        for (partition_id, files) in self.partition_files.drain() {
            for file_info in files {
                if let Err(e) = std::fs::remove_file(&file_info.file_path) {
                    warn!("Failed to remove spill file {}: {}", file_info.file_path, e);
                } else {
                    debug!("Removed spill file: {}", file_info.file_path);
                }
            }
        }
        Ok(())
    }

    /// 获取spill统计信息
    pub fn get_spill_stats(&self) -> SpillStats {
        let total_files = self.partition_files.values().map(|files| files.len()).sum();
        let total_size = self.partition_files
            .values()
            .flatten()
            .map(|file| file.file_size)
            .sum();
        
        SpillStats {
            total_files,
            total_size,
            partition_count: self.partition_files.len(),
            current_memory_usage: self.current_memory_usage,
            is_spilling: self.is_spilling,
        }
    }
}

/// Spill统计信息
#[derive(Debug, Clone)]
pub struct SpillStats {
    /// 总文件数
    pub total_files: usize,
    /// 总大小（字节）
    pub total_size: usize,
    /// 分区数量
    pub partition_count: usize,
    /// 当前内存使用量
    pub current_memory_usage: usize,
    /// 是否正在spill
    pub is_spilling: bool,
}

/// 滞回阈值管理器
pub struct HysteresisManager {
    /// 高阈值
    high_threshold: usize,
    /// 低阈值
    low_threshold: usize,
    /// 当前状态
    state: HysteresisState,
}

#[derive(Debug, Clone, PartialEq)]
enum HysteresisState {
    Normal,
    Spilling,
}

impl HysteresisManager {
    /// 创建新的滞回阈值管理器
    pub fn new(high_threshold: usize, low_threshold: usize) -> Self {
        assert!(high_threshold > low_threshold, "High threshold must be greater than low threshold");
        
        Self {
            high_threshold,
            low_threshold,
            state: HysteresisState::Normal,
        }
    }

    /// 检查是否应该开始spill
    pub fn should_start_spill(&mut self, current_usage: usize) -> bool {
        match self.state {
            HysteresisState::Normal => {
                if current_usage > self.high_threshold {
                    self.state = HysteresisState::Spilling;
                    true
                } else {
                    false
                }
            }
            HysteresisState::Spilling => false,
        }
    }

    /// 检查是否应该停止spill
    pub fn should_stop_spill(&mut self, current_usage: usize) -> bool {
        match self.state {
            HysteresisState::Normal => false,
            HysteresisState::Spilling => {
                if current_usage < self.low_threshold {
                    self.state = HysteresisState::Normal;
                    true
                } else {
                    false
                }
            }
        }
    }

    /// 获取当前状态
    pub fn is_spilling(&self) -> bool {
        self.state == HysteresisState::Spilling
    }
}
