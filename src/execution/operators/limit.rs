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

//! Limit算子
//! 
//! 支持LIMIT和OFFSET操作

use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, info, warn};
use crate::execution::push_runtime::{Operator, Event, OpStatus, Outbox, PortId};
use anyhow::Result;

/// Limit配置
#[derive(Debug, Clone)]
pub struct VectorizedLimitConfig {
    /// 限制行数
    pub limit: Option<usize>,
    /// 偏移行数
    pub offset: usize,
    /// 是否启用早期终止
    pub enable_early_termination: bool,
    /// 是否启用并行处理
    pub enable_parallel: bool,
}

impl Default for VectorizedLimitConfig {
    fn default() -> Self {
        Self {
            limit: None,
            offset: 0,
            enable_early_termination: true,
            enable_parallel: false,
        }
    }
}

/// Limit算子
pub struct VectorizedLimit {
    config: VectorizedLimitConfig,
    input_ports: Vec<PortId>,
    output_ports: Vec<PortId>,
    operator_id: u32,
    name: String,
    
    // Limit状态
    limit_state: LimitState,
    // 统计信息
    total_rows_processed: u64,
    total_rows_output: u64,
    limit_start_time: Option<Instant>,
}

/// Limit状态
#[derive(Debug)]
enum LimitState {
    /// 跳过OFFSET阶段
    Skipping,
    /// 输出LIMIT阶段
    Limiting,
    /// 完成
    Finished,
}

impl VectorizedLimit {
    pub fn new(config: VectorizedLimitConfig, operator_id: u32) -> Self {
        Self {
            config,
            input_ports: vec![],
            output_ports: vec![],
            operator_id,
            name: format!("VectorizedLimit_{}", operator_id),
            limit_state: LimitState::Skipping,
            total_rows_processed: 0,
            total_rows_output: 0,
            limit_start_time: None,
        }
    }
    
    /// 设置输入输出端口
    pub fn set_ports(&mut self, input_ports: Vec<PortId>, output_ports: Vec<PortId>) {
        self.input_ports = input_ports;
        self.output_ports = output_ports;
    }
    
    /// 处理数据批次
    pub fn process_batch(&mut self, batch: &RecordBatch) -> Result<Option<RecordBatch>> {
        if self.limit_start_time.is_none() {
            self.limit_start_time = Some(Instant::now());
        }
        
        self.total_rows_processed += batch.num_rows() as u64;
        
        match &self.limit_state {
            LimitState::Skipping => {
                self.handle_skipping_phase(batch)
            }
            LimitState::Limiting => {
                self.handle_limiting_phase(batch)
            }
            LimitState::Finished => {
                Ok(None)
            }
        }
    }
    
    /// 处理跳过阶段
    fn handle_skipping_phase(&mut self, batch: &RecordBatch) -> Result<Option<RecordBatch>> {
        let batch_rows = batch.num_rows();
        let remaining_offset = self.config.offset.saturating_sub(self.total_rows_processed as usize);
        
        if remaining_offset == 0 {
            // 跳过阶段完成，进入限制阶段
            self.limit_state = LimitState::Limiting;
            return self.handle_limiting_phase(batch);
        }
        
        if remaining_offset >= batch_rows {
            // 整个批次都需要跳过
            debug!("Skipping entire batch: {} rows", batch_rows);
            Ok(None)
        } else {
            // 部分跳过，部分输出
            let skip_rows = remaining_offset;
            let output_rows = batch_rows - skip_rows;
            
            debug!("Skipping {} rows, outputting {} rows", skip_rows, output_rows);
            
            // 跳过前skip_rows行，输出剩余行
            let limited_batch = self.skip_rows(batch, skip_rows)?;
            self.limit_state = LimitState::Limiting;
            
            if limited_batch.num_rows() > 0 {
                Ok(Some(limited_batch))
            } else {
                Ok(None)
            }
        }
    }
    
    /// 处理限制阶段
    fn handle_limiting_phase(&mut self, batch: &RecordBatch) -> Result<Option<RecordBatch>> {
        if let Some(limit) = self.config.limit {
            let remaining_limit = limit.saturating_sub(self.total_rows_output as usize);
            
            if remaining_limit == 0 {
                // 已达到限制，完成
                self.limit_state = LimitState::Finished;
                return Ok(None);
            }
            
            if remaining_limit >= batch.num_rows() {
                // 整个批次都可以输出
                self.total_rows_output += batch.num_rows() as u64;
                debug!("Outputting entire batch: {} rows", batch.num_rows());
                Ok(Some(batch.clone()))
            } else {
                // 部分输出
                let limited_batch = self.limit_rows(batch, remaining_limit)?;
                self.total_rows_output += limited_batch.num_rows() as u64;
                debug!("Outputting limited batch: {} rows", limited_batch.num_rows());
                
                // 达到限制，完成
                self.limit_state = LimitState::Finished;
                Ok(Some(limited_batch))
            }
        } else {
            // 没有限制，直接输出
            self.total_rows_output += batch.num_rows() as u64;
            Ok(Some(batch.clone()))
        }
    }
    
    /// 跳过指定行数
    fn skip_rows(&self, batch: &RecordBatch, skip_rows: usize) -> Result<RecordBatch> {
        if skip_rows >= batch.num_rows() {
            // 跳过所有行，返回空批次
            return Ok(self.create_empty_batch(&batch.schema()));
        }
        
        let remaining_rows = batch.num_rows() - skip_rows;
        self.take_rows(batch, skip_rows, remaining_rows)
    }
    
    /// 限制行数
    fn limit_rows(&self, batch: &RecordBatch, limit_rows: usize) -> Result<RecordBatch> {
        if limit_rows >= batch.num_rows() {
            // 不需要限制
            return Ok(batch.clone());
        }
        
        self.take_rows(batch, 0, limit_rows)
    }
    
    /// 提取指定行范围的数据
    fn take_rows(&self, batch: &RecordBatch, start_row: usize, num_rows: usize) -> Result<RecordBatch> {
        if num_rows == 0 {
            return Ok(self.create_empty_batch(&batch.schema()));
        }
        
        let mut new_columns = Vec::new();
        
        for column in batch.columns() {
            let new_column = self.take_column_rows(column, start_row, num_rows)?;
            new_columns.push(new_column);
        }
        
        Ok(RecordBatch::try_new(batch.schema(), new_columns)?)
    }
    
    /// 提取列中指定行范围的数据
    fn take_column_rows(&self, array: &arrow::array::ArrayRef, start_row: usize, num_rows: usize) -> Result<arrow::array::ArrayRef> {
        use arrow::array::*;
        use arrow::compute::*;
        
        // 创建索引数组
        let indices: Vec<u32> = (start_row..start_row + num_rows)
            .map(|i| i as u32)
            .collect();
        let indices_array = UInt32Array::from(indices);
        
        // 使用take操作提取数据
        Ok(take(array, &indices_array, None)?)
    }
    
    /// 创建空批次
    fn create_empty_batch(&self, schema: &arrow::datatypes::Schema) -> RecordBatch {
        let mut empty_columns = Vec::new();
        
        for field in schema.fields() {
            let empty_array = arrow::array::new_empty_array(field.data_type());
            empty_columns.push(empty_array);
        }
        
        RecordBatch::try_new(Arc::new(schema.clone()), empty_columns).unwrap()
    }
    
    /// 获取统计信息
    pub fn get_stats(&self) -> LimitStats {
        LimitStats {
            total_rows_processed: self.total_rows_processed,
            total_rows_output: self.total_rows_output,
            limit: self.config.limit,
            offset: self.config.offset,
            processing_time: self.limit_start_time.map(|start| start.elapsed()).unwrap_or_default(),
        }
    }
}

/// Limit统计信息
#[derive(Debug, Clone)]
pub struct LimitStats {
    pub total_rows_processed: u64,
    pub total_rows_output: u64,
    pub limit: Option<usize>,
    pub offset: usize,
    pub processing_time: std::time::Duration,
}

/// 实现Operator trait
impl Operator for VectorizedLimit {
    fn on_event(&mut self, ev: Event, out: &mut Outbox) -> OpStatus {
        match ev {
            Event::Data { port, batch } => {
                if self.input_ports.contains(&port) {
                    match self.process_batch(&batch) {
                        Ok(Some(limited_batch)) => {
                            // 发送限制后的数据
                            for &output_port in &self.output_ports {
                                out.send(output_port, limited_batch.clone());
                            }
                            OpStatus::Ready
                        }
                        Ok(None) => {
                            // 没有数据输出，继续处理
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
                    // 刷新输出端口
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
        matches!(self.limit_state, LimitState::Finished)
    }
    
    fn name(&self) -> &str {
        &self.name
    }
}
