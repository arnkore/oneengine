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


//! Arrow-based operator implementations
//! 
//! Leverages Arrow compute kernels and data structures for high-performance execution

// Legacy single-node operators (to be deprecated)
pub mod filter;
pub mod projector;
pub mod local_shuffle;
pub mod limit;

// Distributed MPP operators
pub mod mpp_operator;
pub mod mpp_exchange;
pub mod mpp_aggregator;
pub mod mpp_join;
pub mod mpp_sort;
pub mod mpp_scan;
pub mod mpp_window;
pub mod mpp_distinct;
pub mod mpp_union;
pub mod mpp_sink;

use arrow::record_batch::RecordBatch;
use arrow::datatypes::SchemaRef;
use crate::execution::push_runtime::{Operator, Event, OpStatus, Outbox, PortId};
use anyhow::Result;
use std::sync::Arc;

/// 算子配置
pub trait OperatorConfig: Send + Sync {
    /// 获取输出schema
    fn output_schema(&self) -> SchemaRef;
    
    /// 验证配置
    fn validate(&self) -> Result<()>;
}

/// 基础算子实现
pub struct BaseOperator {
    /// 算子ID
    operator_id: u32,
    /// 输入端口
    input_ports: Vec<PortId>,
    /// 输出端口
    output_ports: Vec<PortId>,
    /// 是否完成
    finished: bool,
    /// 算子名称
    name: String,
}

impl BaseOperator {
    /// 创建新的基础算子
    pub fn new(
        operator_id: u32,
        input_ports: Vec<PortId>,
        output_ports: Vec<PortId>,
        name: String,
    ) -> Self {
        Self {
            operator_id,
            input_ports,
            output_ports,
            finished: false,
            name,
        }
    }
    
    /// 获取算子ID
    pub fn operator_id(&self) -> u32 {
        self.operator_id
    }
    
    /// 获取输入端口
    pub fn input_ports(&self) -> &[PortId] {
        &self.input_ports
    }
    
    /// 获取输出端口
    pub fn output_ports(&self) -> &[PortId] {
        &self.output_ports
    }
    
    /// 设置完成状态
    pub fn set_finished(&mut self) {
        self.finished = true;
    }
    
    /// 检查是否完成
    pub fn is_finished(&self) -> bool {
        self.finished
    }
    
    /// 获取算子名称
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// 单输入算子
pub trait SingleInputOperator: Operator {
    /// 处理单个批次
    fn process_batch(&mut self, batch: RecordBatch, out: &mut Outbox) -> Result<OpStatus>;
}

/// 双输入算子
pub trait DualInputOperator: Operator {
    /// 处理左输入批次
    fn process_left_batch(&mut self, batch: RecordBatch, out: &mut Outbox) -> Result<OpStatus>;
    
    /// 处理右输入批次
    fn process_right_batch(&mut self, batch: RecordBatch, out: &mut Outbox) -> Result<OpStatus>;
    
    /// 完成处理
    fn finish(&mut self, out: &mut Outbox) -> Result<OpStatus>;
}

/// 多输出算子
pub trait MultiOutputOperator: Operator {
    /// 获取输出端口数量
    fn output_port_count(&self) -> usize;
    
    /// 选择输出端口
    fn select_output_port(&self, batch: &RecordBatch, row_idx: usize) -> PortId;
}

/// 可溢写算子
pub trait SpillableOperator: Operator {
    /// 检查是否需要溢写
    fn should_spill(&self) -> bool;
    
    /// 执行溢写
    fn spill(&mut self, out: &mut Outbox) -> Result<OpStatus>;
    
    /// 恢复数据
    fn restore(&mut self, out: &mut Outbox) -> Result<OpStatus>;
}

/// 运行时过滤器支持
pub trait RuntimeFilterSupport: Operator {
    /// 应用运行时过滤器
    fn apply_runtime_filter(&mut self, filter: &crate::execution::push_runtime::RuntimeFilter) -> Result<()>;
    
    /// 检查过滤器是否可用
    fn has_runtime_filter(&self) -> bool;
}

/// 统计信息收集
pub trait MetricsSupport: Operator {
    /// 记录处理统计
    fn record_metrics(&self, batch: &RecordBatch, duration: std::time::Duration);
    
    /// 获取统计信息
    fn get_metrics(&self) -> OperatorMetrics;
}

/// 算子统计信息
#[derive(Debug, Default, Clone)]
pub struct OperatorMetrics {
    /// 处理的行数
    pub rows_processed: u64,
    /// 处理的批次数
    pub batches_processed: u64,
    /// 总处理时间
    pub total_process_time: std::time::Duration,
    /// 平均批处理时间
    pub avg_batch_time: std::time::Duration,
    /// 阻塞次数
    pub block_count: u64,
    /// 溢写次数
    pub spill_count: u64,
    /// 溢写字节数
    pub spill_bytes: u64,
}

impl OperatorMetrics {
    /// 更新批处理统计
    pub fn update_batch(&mut self, rows: usize, duration: std::time::Duration) {
        self.rows_processed += rows as u64;
        self.batches_processed += 1;
        self.total_process_time += duration;
        self.avg_batch_time = if self.batches_processed > 0 {
            std::time::Duration::from_nanos(
                self.total_process_time.as_nanos() as u64 / self.batches_processed
            )
        } else {
            std::time::Duration::ZERO
        };
    }
    
    /// 记录阻塞
    pub fn record_block(&mut self) {
        self.block_count += 1;
    }
    
    /// 记录溢写
    pub fn record_spill(&mut self, bytes: u64) {
        self.spill_count += 1;
        self.spill_bytes += bytes;
    }
}
