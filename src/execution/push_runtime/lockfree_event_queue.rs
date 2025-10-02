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

//! 无锁事件队列
//! 
//! 基于crossbeam的高性能无锁事件队列实现

use super::{Event, PortId, OperatorId};
use crossbeam::queue::{SegQueue, ArrayQueue};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, warn, error};
use anyhow::Result;

/// 无锁事件队列配置
#[derive(Debug, Clone)]
pub struct LockFreeEventQueueConfig {
    /// 队列容量
    pub capacity: usize,
    /// 是否启用批处理
    pub enable_batching: bool,
    /// 批处理大小
    pub batch_size: usize,
    /// 批处理超时时间（微秒）
    pub batch_timeout_us: u64,
    /// 是否启用优先级队列
    pub enable_priority: bool,
}

impl Default for LockFreeEventQueueConfig {
    fn default() -> Self {
        Self {
            capacity: 10000,
            enable_batching: true,
            batch_size: 100,
            batch_timeout_us: 1000, // 1ms
            enable_priority: false,
        }
    }
}

/// 优先级事件
#[derive(Debug, Clone)]
pub struct PriorityEvent {
    pub event: Event,
    pub priority: u8, // 0 = 最高优先级
    pub timestamp: Instant,
}

impl PriorityEvent {
    pub fn new(event: Event, priority: u8) -> Self {
        Self {
            event,
            priority,
            timestamp: Instant::now(),
        }
    }
}

/// 无锁事件队列
pub struct LockFreeEventQueue {
    /// 主事件队列
    main_queue: SegQueue<Event>,
    /// 高优先级事件队列
    priority_queue: Option<ArrayQueue<Event>>,
    /// 批处理队列
    batch_queue: Vec<Event>,
    /// 配置
    config: LockFreeEventQueueConfig,
    /// 统计信息
    stats: QueueStats,
    /// 最后批处理时间
    last_batch_time: Instant,
}

/// 队列统计信息
#[derive(Debug, Default)]
pub struct QueueStats {
    /// 入队次数
    pub enqueue_count: u64,
    /// 出队次数
    pub dequeue_count: u64,
    /// 批处理次数
    pub batch_count: u64,
    /// 队列满次数
    pub queue_full_count: u64,
    /// 平均队列长度
    pub avg_queue_length: f64,
    /// 最大队列长度
    pub max_queue_length: usize,
    /// 当前队列长度
    pub current_length: usize,
}

impl LockFreeEventQueue {
    /// 创建新的无锁事件队列
    pub fn new(config: LockFreeEventQueueConfig) -> Self {
        let priority_queue = if config.enable_priority {
            Some(ArrayQueue::new(config.capacity / 10)) // 高优先级队列占10%
        } else {
            None
        };

        Self {
            main_queue: SegQueue::new(),
            priority_queue,
            batch_queue: Vec::with_capacity(config.batch_size),
            config,
            stats: QueueStats::default(),
            last_batch_time: Instant::now(),
        }
    }

    /// 入队事件
    pub fn enqueue(&mut self, event: Event) -> Result<()> {
        // 尝试高优先级队列
        if let Some(ref priority_queue) = self.priority_queue {
            if priority_queue.push(event.clone()).is_ok() {
                self.update_stats(true, false);
                return Ok(());
            }
        }

        // 入队到主队列
        self.main_queue.push(event);
        self.update_stats(true, false);
        Ok(())
    }

    /// 批量入队事件
    pub fn enqueue_batch(&mut self, events: Vec<Event>) -> Result<usize> {
        let mut enqueued = 0;
        
        for event in events {
            if let Err(e) = self.enqueue(event) {
                warn!("Failed to enqueue event: {}", e);
                break;
            }
            enqueued += 1;
        }
        
        Ok(enqueued)
    }

    /// 出队事件
    pub fn dequeue(&mut self) -> Option<Event> {
        // 优先从高优先级队列获取
        if let Some(ref priority_queue) = self.priority_queue {
            if let Some(event) = priority_queue.pop() {
                self.update_stats(false, false);
                return Some(event);
            }
        }

        // 从主队列获取
        if let Some(event) = self.main_queue.pop() {
            self.update_stats(false, false);
            return Some(event);
        }

        None
    }

    /// 批量出队事件
    pub fn dequeue_batch(&mut self) -> Vec<Event> {
        let mut events = Vec::new();
        
        if self.config.enable_batching {
            // 检查是否需要批处理
            let should_batch = self.batch_queue.len() >= self.config.batch_size ||
                (self.batch_queue.len() > 0 && 
                 self.last_batch_time.elapsed().as_micros() as u64 >= self.config.batch_timeout_us);
            
            if should_batch {
                // 处理批处理队列中的事件
                events.extend(self.batch_queue.drain(..));
                self.stats.batch_count += 1;
                self.last_batch_time = Instant::now();
            }
        }

        // 获取新事件
        let mut batch_count = 0;
        while batch_count < self.config.batch_size {
            if let Some(event) = self.dequeue() {
                events.push(event);
                batch_count += 1;
            } else {
                break;
            }
        }

        events
    }

    /// 非阻塞批量出队
    pub fn try_dequeue_batch(&mut self) -> Vec<Event> {
        let mut events = Vec::new();
        let mut count = 0;
        
        while count < self.config.batch_size {
            if let Some(event) = self.dequeue() {
                events.push(event);
                count += 1;
            } else {
                break;
            }
        }
        
        events
    }

    /// 检查队列是否为空
    pub fn is_empty(&self) -> bool {
        let priority_empty = self.priority_queue.as_ref()
            .map(|q| q.is_empty())
            .unwrap_or(true);
        
        priority_empty && self.main_queue.is_empty()
    }

    /// 获取队列长度
    pub fn len(&self) -> usize {
        let priority_len = self.priority_queue.as_ref()
            .map(|q| q.len())
            .unwrap_or(0);
        
        priority_len + self.main_queue.len()
    }

    /// 清空队列
    pub fn clear(&self) {
        // 清空高优先级队列
        if let Some(ref priority_queue) = self.priority_queue {
            while priority_queue.pop().is_some() {}
        }
        
        // 清空主队列
        while self.main_queue.pop().is_some() {}
    }

    /// 更新统计信息
    fn update_stats(&mut self, is_enqueue: bool, is_batch: bool) {
        // 这里应该使用原子操作更新统计信息
        // 简化实现：直接更新
        if is_enqueue {
            self.stats.enqueue_count += 1;
        } else {
            self.stats.dequeue_count += 1;
        }
        
        if is_batch {
            self.stats.batch_count += 1;
        }
        
        let current_len = self.len();
        self.stats.current_length = current_len;
        
        if current_len > self.stats.max_queue_length {
            self.stats.max_queue_length = current_len;
        }
    }

    /// 获取统计信息
    pub fn get_stats(&self) -> &QueueStats {
        &self.stats
    }

    /// 重置统计信息
    pub fn reset_stats(&mut self) {
        self.stats = QueueStats::default();
    }
}

/// 无锁事件队列工厂
pub struct LockFreeEventQueueFactory;

impl LockFreeEventQueueFactory {
    /// 创建高性能事件队列
    pub fn create_high_performance_queue() -> LockFreeEventQueue {
        let config = LockFreeEventQueueConfig {
            capacity: 50000,
            enable_batching: true,
            batch_size: 200,
            batch_timeout_us: 500, // 0.5ms
            enable_priority: true,
        };
        LockFreeEventQueue::new(config)
    }

    /// 创建低延迟事件队列
    pub fn create_low_latency_queue() -> LockFreeEventQueue {
        let config = LockFreeEventQueueConfig {
            capacity: 10000,
            enable_batching: false,
            batch_size: 1,
            batch_timeout_us: 0,
            enable_priority: true,
        };
        LockFreeEventQueue::new(config)
    }

    /// 创建高吞吐量事件队列
    pub fn create_high_throughput_queue() -> LockFreeEventQueue {
        let config = LockFreeEventQueueConfig {
            capacity: 100000,
            enable_batching: true,
            batch_size: 500,
            batch_timeout_us: 2000, // 2ms
            enable_priority: false,
        };
        LockFreeEventQueue::new(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_queue_basic_operations() {
        let mut queue = LockFreeEventQueue::new(LockFreeEventQueueConfig::default());
        
        // 测试入队和出队
        let event = Event::Data { 
            port: 1, 
            batch: arrow::record_batch::RecordBatch::new_empty(Arc::new(arrow::datatypes::Schema::empty()))
        };
        
        assert!(queue.enqueue(event.clone()).is_ok());
        assert!(!queue.is_empty());
        assert_eq!(queue.len(), 1);
        
        let dequeued = queue.dequeue();
        assert!(dequeued.is_some());
        assert!(queue.is_empty());
    }

    #[test]
    fn test_batch_operations() {
        let config = LockFreeEventQueueConfig {
            capacity: 1000,
            enable_batching: true,
            batch_size: 5,
            batch_timeout_us: 1000,
            enable_priority: false,
        };
        let mut queue = LockFreeEventQueue::new(config);
        
        // 批量入队
        let events: Vec<Event> = (0..10).map(|i| {
            Event::Data { 
                port: i as u32, 
                batch: arrow::record_batch::RecordBatch::new_empty(Arc::new(arrow::datatypes::Schema::empty()))
            }
        }).collect();
        
        assert_eq!(queue.enqueue_batch(events).unwrap(), 10);
        assert_eq!(queue.len(), 10);
        
        // 批量出队
        let batch = queue.dequeue_batch();
        assert_eq!(batch.len(), 5); // 批处理大小
    }
}
