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


//! 信用管理器
//! 
//! 管理端口信用和背压控制，支持端到端背压控制

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use crate::execution::push_runtime::PortId;

/// 信用管理器 - 增强版背压控制
#[derive(Debug)]
pub struct CreditManager {
    /// 端口信用
    pub credits: HashMap<PortId, AtomicU32>,
    /// 最大信用
    pub max_credits: HashMap<PortId, u32>,
    /// 信用使用率阈值
    pub credit_usage_threshold: f64,
    /// 背压级别
    pub backpressure_level: BackpressureLevel,
    /// 全局信用池
    pub global_credit_pool: AtomicU32,
    /// 最大全局信用
    pub max_global_credits: u32,
}

/// 背压级别
#[derive(Debug, Clone, PartialEq)]
pub enum BackpressureLevel {
    /// 无背压
    None,
    /// 轻微背压
    Light,
    /// 中等背压
    Medium,
    /// 严重背压
    Severe,
    /// 完全阻塞
    Blocked,
}

impl CreditManager {
    /// 创建新的信用管理器
    pub fn new() -> Self {
        Self {
            credits: HashMap::new(),
            max_credits: HashMap::new(),
            credit_usage_threshold: 0.8, // 80%使用率触发背压
            backpressure_level: BackpressureLevel::None,
            global_credit_pool: AtomicU32::new(10000), // 10000全局信用
            max_global_credits: 10000,
        }
    }
    
    /// 创建高性能信用管理器
    pub fn new_high_performance() -> Self {
        Self {
            credits: HashMap::new(),
            max_credits: HashMap::new(),
            credit_usage_threshold: 0.9, // 90%使用率触发背压
            backpressure_level: BackpressureLevel::None,
            global_credit_pool: AtomicU32::new(50000), // 50000全局信用
            max_global_credits: 50000,
        }
    }
    
    /// 设置端口信用
    pub fn set_credit(&mut self, port: PortId, credit: u32) {
        self.credits.insert(port, AtomicU32::new(credit));
        self.max_credits.insert(port, credit);
    }
    
    /// 检查是否有信用 - 线程安全
    pub fn has_credit(&self, port: PortId) -> bool {
        self.credits.get(&port).map_or(false, |c| c.load(Ordering::Relaxed) > 0)
    }
    
    /// 消费信用 - 线程安全
    pub fn consume_credit(&mut self, port: PortId, amount: u32) -> bool {
        if let Some(credit) = self.credits.get(&port) {
            let current = credit.load(Ordering::Relaxed);
            if current >= amount {
                credit.fetch_sub(amount, Ordering::Relaxed);
                self.update_backpressure_level();
                true
            } else {
                false
            }
        } else {
            false
        }
    }
    
    /// 归还信用 - 线程安全
    pub fn return_credit(&mut self, port: PortId, amount: u32) {
        if let Some(credit) = self.credits.get(&port) {
            let max_credit = self.max_credits.get(&port).copied().unwrap_or(0);
            let current = credit.load(Ordering::Relaxed);
            let new_credit = (current + amount).min(max_credit);
            credit.store(new_credit, Ordering::Relaxed);
            self.update_backpressure_level();
        }
    }
    
    /// 重置信用
    pub fn reset_credit(&mut self, port: PortId) {
        if let Some(max_credit) = self.max_credits.get(&port) {
            self.credits.insert(port, AtomicU32::new(*max_credit));
        }
    }
    
    /// 更新背压级别
    fn update_backpressure_level(&mut self) {
        let total_credits: u32 = self.credits.values()
            .map(|c| c.load(Ordering::Relaxed))
            .sum();
        let total_max_credits: u32 = self.max_credits.values().sum();
        
        if total_max_credits == 0 {
            self.backpressure_level = BackpressureLevel::None;
            return;
        }
        
        let usage_ratio = 1.0 - (total_credits as f64 / total_max_credits as f64);
        
        self.backpressure_level = if usage_ratio >= 0.95 {
            BackpressureLevel::Blocked
        } else if usage_ratio >= 0.8 {
            BackpressureLevel::Severe
        } else if usage_ratio >= 0.6 {
            BackpressureLevel::Medium
        } else if usage_ratio >= self.credit_usage_threshold {
            BackpressureLevel::Light
        } else {
            BackpressureLevel::None
        };
    }
    
    /// 获取背压级别
    pub fn get_backpressure_level(&self) -> BackpressureLevel {
        self.backpressure_level.clone()
    }
    
    /// 检查是否应该应用背压
    pub fn should_apply_backpressure(&self) -> bool {
        matches!(self.backpressure_level, BackpressureLevel::Light | BackpressureLevel::Medium | BackpressureLevel::Severe | BackpressureLevel::Blocked)
    }
    
    /// 获取端口信用使用率
    pub fn get_port_credit_usage(&self, port: PortId) -> f64 {
        if let (Some(credit), Some(max_credit)) = (
            self.credits.get(&port).map(|c| c.load(Ordering::Relaxed)),
            self.max_credits.get(&port)
        ) {
            if *max_credit > 0 {
                1.0 - (credit as f64 / *max_credit as f64)
            } else {
                0.0
            }
        } else {
            0.0
        }
    }
    
    /// 动态调整信用分配
    pub fn rebalance_credits(&mut self) {
        let total_available = self.global_credit_pool.load(Ordering::Relaxed);
        let port_count = self.credits.len();
        
        if port_count > 0 {
            let credit_per_port = total_available / port_count as u32;
            
            for (port, credit) in &mut self.credits {
                let max_credit = self.max_credits.get(port).copied().unwrap_or(credit_per_port);
                let new_credit = credit_per_port.min(max_credit);
                credit.store(new_credit, Ordering::Relaxed);
            }
        }
    }
    
    /// 获取全局信用池状态
    pub fn get_global_credit_status(&self) -> (u32, u32) {
        let available = self.global_credit_pool.load(Ordering::Relaxed);
        (available, self.max_global_credits)
    }
}

impl Default for CreditManager {
    fn default() -> Self {
        Self::new()
    }
}
