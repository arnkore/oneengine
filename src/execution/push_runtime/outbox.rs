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


//! 输出邮箱
//! 
//! 管理算子输出和信用控制

use crate::execution::push_runtime::{Event, PortId, OperatorId, RuntimeFilter, CreditManager, WouldBlock, LockFreeEventQueue};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use anyhow::Result;
use std::time::{Duration, Instant};
use tracing::{debug, warn, error};

/// 输出邮箱统计信息
#[derive(Debug, Clone, Default)]
pub struct OutboxStats {
    /// 发送的批次数量
    pub batches_sent: u64,
    /// 发送的行数
    pub rows_sent: u64,
    /// 发送的字节数
    pub bytes_sent: u64,
    /// 阻塞次数
    pub block_count: u64,
    /// 信用不足次数
    pub credit_shortage_count: u64,
    /// 平均批次大小
    pub avg_batch_size: f64,
    /// 最大批次大小
    pub max_batch_size: usize,
    /// 最小批次大小
    pub min_batch_size: usize,
    /// 总发送时间
    pub total_send_time: Duration,
    /// 最后发送时间
    pub last_send_time: Option<Instant>,
}

/// 输出邮箱
pub struct Outbox<'a> {
    /// 算子ID
    operator_id: OperatorId,
    /// 信用管理器
    credit_manager: Option<&'a mut CreditManager>,
    /// 事件队列
    event_queue: Option<&'a mut Vec<Event>>,
    /// 端口映射
    port_mapping: Option<&'a HashMap<PortId, OperatorId>>,
    /// 内部事件队列（当没有外部事件队列时使用）
    internal_event_queue: Vec<Event>,
    /// 统计信息
    stats: OutboxStats,
}

impl<'a> Outbox<'a> {
    /// 创建新的输出邮箱（带信用管理器）
    pub fn new_with_credit_manager(
        operator_id: OperatorId,
        credit_manager: &'a mut CreditManager,
        event_queue: &'a mut Vec<Event>,
        port_mapping: &'a HashMap<PortId, OperatorId>,
    ) -> Self {
        Self {
            operator_id,
            credit_manager: Some(credit_manager),
            event_queue: Some(event_queue),
            port_mapping: Some(port_mapping),
            internal_event_queue: Vec::new(),
            stats: OutboxStats::default(),
        }
    }
    
    /// 创建新的输出邮箱（简化版本，用于Driver）
    pub fn new(operator_id: OperatorId) -> Self {
        Self {
            operator_id,
            credit_manager: None,
            event_queue: None,
            port_mapping: None,
            internal_event_queue: Vec::new(),
            stats: OutboxStats::default(),
        }
    }
    
    /// 推送数据（带信用控制）
    pub fn push(&mut self, to: PortId, batch: RecordBatch) -> Result<(), WouldBlock> {
        let start_time = Instant::now();
        
        // 检查信用（如果有信用管理器）
        if let Some(ref mut credit_manager) = self.credit_manager {
            if !credit_manager.has_credit(to) {
                self.stats.credit_shortage_count += 1;
                debug!("No credit available for port {}, operator {}", to, self.operator_id);
                return Err(WouldBlock);
            }
            
            // 扣减信用
            credit_manager.consume_credit(to, batch.num_rows() as u32);
        }
        
        // 更新统计信息
        self.update_stats(&batch, start_time);
        
        // 发送数据事件
        self.send_event(Event::Data { port: to, batch });
        
        debug!("Pushed batch to port {}, operator {}", to, self.operator_id);
        Ok(())
    }
    
    /// 发送数据（简化版本，用于兼容性）
    pub fn send(&mut self, to: PortId, batch: RecordBatch) {
        let start_time = Instant::now();
        
        // 更新统计信息
        self.update_stats(&batch, start_time);
        
        // 发送数据事件
        self.send_event(Event::Data { port: to, batch });
        
        debug!("Sent batch to port {}, operator {}", to, self.operator_id);
    }
    
    /// 发送控制事件
    pub fn emit_ctrl(&mut self, to: PortId, filter: RuntimeFilter) -> Result<()> {
        self.send_event(Event::Ctrl(filter));
        debug!("Emitted control event to port {}, operator {}", to, self.operator_id);
        Ok(())
    }
    
    /// 发送信用事件
    pub fn emit_credit(&mut self, to: PortId, credit: u32) {
        self.send_event(Event::Credit(to, credit));
        debug!("Emitted credit {} to port {}, operator {}", credit, to, self.operator_id);
    }
    
    /// 发送刷新事件
    pub fn emit_flush(&mut self, to: PortId) -> Result<()> {
        self.send_event(Event::Flush(to));
        debug!("Emitted flush event to port {}, operator {}", to, self.operator_id);
        Ok(())
    }
    
    /// 发送完成事件
    pub fn emit_finish(&mut self, to: PortId) -> Result<()> {
        self.send_event(Event::Finish(to));
        debug!("Emitted finish event to port {}, operator {}", to, self.operator_id);
        Ok(())
    }
    
    /// 发送流结束事件
    pub fn send_eos(&mut self, to: PortId) {
        self.send_event(Event::EndOfStream { port: to });
        debug!("Emitted end of stream event to port {}, operator {}", to, self.operator_id);
    }
    
    /// 批量发送数据
    pub fn send_batch(&mut self, to: PortId, batches: Vec<RecordBatch>) -> Result<(), WouldBlock> {
        let start_time = Instant::now();
        let mut total_rows = 0;
        let mut total_bytes = 0;
        
        // 计算总行数和字节数
        for batch in &batches {
            total_rows += batch.num_rows();
            total_bytes += self.estimate_batch_size(batch);
        }
        
        // 检查信用（如果有信用管理器）
        if let Some(ref mut credit_manager) = self.credit_manager {
            if !credit_manager.has_credit(to) {
                self.stats.credit_shortage_count += 1;
                return Err(WouldBlock);
            }
            
            // 扣减信用
            credit_manager.consume_credit(to, total_rows as u32);
        }
        
        // 发送所有批次
        for batch in batches {
            self.update_stats(&batch, start_time);
            self.send_event(Event::Data { port: to, batch });
        }
        
        debug!("Sent {} batches to port {}, operator {}", 
               self.stats.batches_sent, to, self.operator_id);
        Ok(())
    }
    
    /// 检查是否有信用
    pub fn has_credit(&self, to: PortId) -> bool {
        if let Some(ref credit_manager) = self.credit_manager {
            credit_manager.has_credit(to)
        } else {
            true // 没有信用管理器时总是有信用
        }
    }
    
    /// 获取可用信用
    pub fn get_available_credit(&self, to: PortId) -> u32 {
        if let Some(ref credit_manager) = self.credit_manager {
            credit_manager.credits.get(&to).map(|c| c.load(std::sync::atomic::Ordering::Relaxed)).unwrap_or(0)
        } else {
            u32::MAX // 没有信用管理器时返回最大信用
        }
    }
    
    /// 获取统计信息
    pub fn get_stats(&self) -> &OutboxStats {
        &self.stats
    }
    
    /// 重置统计信息
    pub fn reset_stats(&mut self) {
        self.stats = OutboxStats::default();
    }
    
    /// 获取内部事件队列
    pub fn get_internal_events(&mut self) -> Vec<Event> {
        std::mem::take(&mut self.internal_event_queue)
    }
    
    /// 设置信用管理器
    pub fn set_credit_manager(&mut self, credit_manager: &'a mut CreditManager) {
        self.credit_manager = Some(credit_manager);
    }
    
    /// 设置事件队列
    pub fn set_event_queue(&mut self, event_queue: &'a mut Vec<Event>) {
        self.event_queue = Some(event_queue);
    }
    
    /// 设置端口映射
    pub fn set_port_mapping(&mut self, port_mapping: &'a HashMap<PortId, OperatorId>) {
        self.port_mapping = Some(port_mapping);
    }
    
    /// 发送事件（内部方法）
    fn send_event(&mut self, event: Event) {
        if let Some(ref mut event_queue) = self.event_queue {
            event_queue.push(event);
        } else {
            self.internal_event_queue.push(event);
        }
    }
    
    /// 更新统计信息
    fn update_stats(&mut self, batch: &RecordBatch, start_time: Instant) {
        let batch_size = batch.num_rows();
        let batch_bytes = self.estimate_batch_size(batch);
        let send_time = start_time.elapsed();
        
        self.stats.batches_sent += 1;
        self.stats.rows_sent += batch_size as u64;
        self.stats.bytes_sent += batch_bytes as u64;
        self.stats.total_send_time += send_time;
        self.stats.last_send_time = Some(Instant::now());
        
        // 更新批次大小统计
        if batch_size > self.stats.max_batch_size {
            self.stats.max_batch_size = batch_size;
        }
        if batch_size < self.stats.min_batch_size || self.stats.min_batch_size == 0 {
            self.stats.min_batch_size = batch_size;
        }
        
        // 更新平均批次大小
        self.stats.avg_batch_size = self.stats.rows_sent as f64 / self.stats.batches_sent as f64;
    }
    
    /// 估算批次大小
    fn estimate_batch_size(&self, batch: &RecordBatch) -> usize {
        let mut total_size = 0;
        for column in batch.columns() {
            total_size += column.get_buffer_memory_size();
        }
        total_size
    }
}
