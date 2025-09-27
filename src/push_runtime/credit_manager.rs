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
//! 管理端口信用和背压控制

use std::collections::HashMap;
use crate::push_runtime::PortId;

/// 信用管理器
#[derive(Debug, Default)]
pub struct CreditManager {
    /// 端口信用
    pub credits: HashMap<PortId, u32>,
    /// 最大信用
    pub max_credits: HashMap<PortId, u32>,
}

impl CreditManager {
    /// 创建新的信用管理器
    pub fn new() -> Self {
        Self {
            credits: HashMap::new(),
            max_credits: HashMap::new(),
        }
    }
    
    /// 设置端口信用
    pub fn set_credit(&mut self, port: PortId, credit: u32) {
        self.credits.insert(port, credit);
        self.max_credits.insert(port, credit);
    }
    
    /// 检查是否有信用
    pub fn has_credit(&self, port: PortId) -> bool {
        self.credits.get(&port).map_or(false, |&c| c > 0)
    }
    
    /// 消费信用
    pub fn consume_credit(&mut self, port: PortId, amount: u32) {
        if let Some(credit) = self.credits.get_mut(&port) {
            *credit = credit.saturating_sub(amount);
        }
    }
    
    /// 归还信用
    pub fn return_credit(&mut self, port: PortId, amount: u32) {
        if let Some(credit) = self.credits.get_mut(&port) {
            let max_credit = self.max_credits.get(&port).copied().unwrap_or(0);
            *credit = (*credit + amount).min(max_credit);
        }
    }
    
    /// 重置信用
    pub fn reset_credit(&mut self, port: PortId) {
        if let Some(max_credit) = self.max_credits.get(&port) {
            self.credits.insert(port, *max_credit);
        }
    }
}
