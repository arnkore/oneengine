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


//! 事件循环
//! 
//! 基于信用背压的纯push事件驱动执行

use super::{Event, Operator, OperatorId, PortId, CreditManager, MetricsCollector, Outbox};
use arrow::record_batch::RecordBatch;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use anyhow::Result;
use tracing::{debug, warn, error};

/// 事件循环
pub struct EventLoop {
    /// 算子注册表
    operators: HashMap<OperatorId, Box<dyn Operator>>,
    /// 信用管理器
    credit_manager: CreditManager,
    /// 事件队列
    event_queue: Vec<Event>,
    /// 端口到算子的映射
    port_mapping: HashMap<PortId, OperatorId>,
    /// 指标收集器
    metrics: Arc<dyn MetricsCollector>,
    /// 是否运行中
    running: bool,
    /// 最大事件处理数（防止饥饿）
    max_events_per_cycle: usize,
    /// 统计信息
    stats: EventLoopStats,
}

/// 事件循环统计信息
#[derive(Debug, Default)]
pub struct EventLoopStats {
    /// 处理的事件数
    pub events_processed: u64,
    /// 阻塞的算子数
    pub blocked_operators: u64,
    /// 平均处理时间
    pub avg_process_time: Duration,
    /// 总运行时间
    pub total_runtime: Duration,
}

impl EventLoop {
    /// 创建新的事件循环
    pub fn new(metrics: Arc<dyn MetricsCollector>) -> Self {
        Self {
            operators: HashMap::new(),
            credit_manager: CreditManager::new(),
            event_queue: Vec::new(),
            port_mapping: HashMap::new(),
            metrics,
            running: false,
            max_events_per_cycle: 1000,
            stats: EventLoopStats::default(),
        }
    }
    
    /// 注册算子
    pub fn register_operator(
        &mut self,
        operator_id: OperatorId,
        mut operator: Box<dyn Operator>,
        input_ports: Vec<PortId>,
        output_ports: Vec<PortId>,
    ) -> Result<()> {
        
        // 设置端口映射
        for port in input_ports.iter().chain(output_ports.iter()) {
            self.port_mapping.insert(*port, operator_id);
        }
        
        // 设置输出端口信用
        for port in &output_ports {
            self.credit_manager.set_credit(*port, 1000); // 默认1000行信用
        }
        
        self.operators.insert(operator_id, operator);
        debug!("Registered operator {} with {} input ports and {} output ports", 
               operator_id, input_ports.len(), output_ports.len());
        
        Ok(())
    }
    
    /// 设置端口信用
    pub fn set_port_credit(&mut self, port: PortId, credit: u32) {
        self.credit_manager.set_credit(port, credit);
    }
    
    /// 启动事件循环
    pub fn run(&mut self) -> Result<()> {
        self.running = true;
        let start_time = Instant::now();
        
        debug!("Starting event loop");
        
        while self.running {
            let cycle_start = Instant::now();
            let events_processed = self.process_events();
            
            // 更新统计信息
            let cycle_duration = cycle_start.elapsed();
            self.stats.events_processed += events_processed as u64;
            self.stats.total_runtime = start_time.elapsed();
            
            // 如果没有事件处理，短暂休眠
            if events_processed == 0 {
                std::thread::sleep(Duration::from_millis(1));
            }
            
            // 检查是否所有算子都完成
            if self.all_operators_finished() {
                debug!("All operators finished, stopping event loop");
                break;
            }
        }
        
        debug!("Event loop stopped after {:?}", self.stats.total_runtime);
        Ok(())
    }
    
    /// 停止事件循环
    pub fn stop(&mut self) {
        self.running = false;
    }
    
    /// 处理事件
    fn process_events(&mut self) -> usize {
        let mut events_processed = 0;
        let mut blocked_operators = 0;
        
        while events_processed < self.max_events_per_cycle {
            if let Some(event) = self.event_queue.pop() {
                let process_start = Instant::now();
                
                match self.handle_event(event) {
                    Ok(blocked) => {
                        if blocked {
                            blocked_operators += 1;
                        }
                        events_processed += 1;
                    }
                    Err(e) => {
                        error!("Error handling event: {}", e);
                        events_processed += 1;
                    }
                }
                
                // 记录处理时间
                let process_duration = process_start.elapsed();
                self.metrics.record_process_time(0, process_duration); // 使用0作为事件循环ID
            } else {
                break;
            }
        }
        
        self.stats.blocked_operators = blocked_operators as u64;
        events_processed
    }
    
    /// 处理单个事件
    fn handle_event(&mut self, event: Event) -> Result<bool> {
        match event {
            Event::Data { port, batch } => {
                if let Some(&op_id) = self.port_mapping.get(&port) {
                    self.handle_data_event(op_id, port, batch)
                } else {
                    warn!("No operator found for port {}", port);
                    Ok(false)
                }
            }
            Event::Credit(port, credit) => {
                self.credit_manager.return_credit(port, credit);
                self.metrics.record_credit_usage(port, 0, credit);
                debug!("Returned {} credit to port {}", credit, port);
                Ok(false)
            }
            Event::Ctrl(filter) => {
                // 广播控制事件到所有算子
                let mut blocked_count = 0;
                let operator_ids: Vec<_> = self.operators.keys().cloned().collect();
                for op_id in operator_ids {
                    // 为每个算子创建独立的outbox
                    let mut outbox = Outbox::new_with_credit_manager(
                        op_id,
                        &mut self.credit_manager,
                        &mut self.event_queue,
                        &self.port_mapping,
                    );
                    
                    if let Some(operator) = self.operators.get_mut(&op_id) {
                        match operator.on_event(Event::Ctrl(filter.clone()), &mut outbox) {
                            super::OpStatus::Blocked => blocked_count += 1,
                            _ => {}
                        }
                    }
                }
                Ok(blocked_count > 0)
            }
            Event::Flush(port) | Event::Finish(port) => {
                if let Some(&op_id) = self.port_mapping.get(&port) {
                    self.handle_control_event(op_id, event)
                } else {
                    warn!("No operator found for port {}", port);
                    Ok(false)
                }
            }
            Event::StartScan { .. } | Event::EndOfStream { .. } => {
                // 处理扫描开始和流结束事件
                Ok(false)
            }
        }
    }
    
    /// 处理数据事件
    fn handle_data_event(&mut self, operator_id: OperatorId, port: PortId, batch: RecordBatch) -> Result<bool> {
        if let Some(operator) = self.operators.get_mut(&operator_id) {
            // 创建outbox
            let mut outbox = Outbox::new_with_credit_manager(
                operator_id,
                &mut self.credit_manager,
                &mut self.event_queue,
                &self.port_mapping,
            );
            
            let status = operator.on_event(Event::Data { port, batch }, &mut outbox);
            
            match status {
                super::OpStatus::Blocked => {
                    self.metrics.record_block(operator_id, "no_credit");
                    Ok(true)
                }
                super::OpStatus::Error(msg) => {
                    error!("Operator {} error: {}", operator_id, msg);
                    Ok(false)
                }
                _ => Ok(false)
            }
        } else {
            warn!("Operator {} not found", operator_id);
            Ok(false)
        }
    }
    
    /// 处理控制事件
    fn handle_control_event(&mut self, operator_id: OperatorId, event: Event) -> Result<bool> {
        if let Some(operator) = self.operators.get_mut(&operator_id) {
            // 创建outbox
            let mut outbox = Outbox::new_with_credit_manager(
                operator_id,
                &mut self.credit_manager,
                &mut self.event_queue,
                &self.port_mapping,
            );
            
            let status = operator.on_event(event, &mut outbox);
            
            match status {
                super::OpStatus::Blocked => Ok(true),
                _ => Ok(false)
            }
        } else {
            warn!("Operator {} not found", operator_id);
            Ok(false)
        }
    }
    
    /// 创建输出邮箱
    fn create_outbox(&mut self, operator_id: OperatorId) -> Outbox {
        Outbox::new_with_credit_manager(
            operator_id,
            &mut self.credit_manager,
            &mut self.event_queue,
            &self.port_mapping,
        )
    }
    
    /// 检查所有算子是否完成
    fn all_operators_finished(&self) -> bool {
        self.operators.values().all(|op| op.is_finished())
    }
    
    /// 获取统计信息
    pub fn get_stats(&self) -> &EventLoopStats {
        &self.stats
    }
    
    /// 设置最大事件处理数
    pub fn set_max_events_per_cycle(&mut self, max: usize) {
        self.max_events_per_cycle = max;
    }
    
    /// 处理单个事件（用于外部调用）
    pub fn process_event(&mut self, event: Event) -> Result<()> {
        self.handle_event(event)?;
        Ok(())
    }
    
    /// 检查是否完成
    pub fn is_finished(&self) -> bool {
        !self.running || self.all_operators_finished()
    }
    
    /// 添加端口映射
    pub fn add_port_mapping(&mut self, port: PortId, operator_id: OperatorId) {
        self.port_mapping.insert(port, operator_id);
    }
    
    /// 获取下一个待处理的事件
    pub fn get_next_event(&mut self) -> Option<Event> {
        self.event_queue.pop()
    }
    
    /// 检查是否有待处理的事件
    pub fn has_pending_events(&self) -> bool {
        !self.event_queue.is_empty()
    }
    
    /// 添加事件到队列
    pub fn add_event(&mut self, event: Event) {
        self.event_queue.push(event);
    }
    
    /// 获取事件队列大小
    pub fn event_queue_size(&self) -> usize {
        self.event_queue.len()
    }
}

impl Default for EventLoop {
    fn default() -> Self {
        Self::new(Arc::new(NoOpMetricsCollector))
    }
}

/// 空操作指标收集器
struct NoOpMetricsCollector;

impl MetricsCollector for NoOpMetricsCollector {
    fn record_push(&self, _operator_id: OperatorId, _port: PortId, _rows: u32) {}
    fn record_block(&self, _operator_id: OperatorId, _reason: &str) {}
    fn record_process_time(&self, _operator_id: OperatorId, _duration: Duration) {}
    fn record_credit_usage(&self, _port: PortId, _used: u32, _remaining: u32) {}
}
