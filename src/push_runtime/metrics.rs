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


//! 指标收集器
//! 
//! 收集和报告运行时指标

use crate::push_runtime::{OperatorId, PortId};
use std::time::Duration;

/// 指标收集器trait
pub trait MetricsCollector: Send + Sync {
    /// 记录推送事件
    fn record_push(&self, operator_id: OperatorId, port: PortId, rows: u32);
    
    /// 记录阻塞事件
    fn record_block(&self, operator_id: OperatorId, reason: &str);
    
    /// 记录处理时间
    fn record_process_time(&self, operator_id: OperatorId, duration: Duration);
    
    /// 记录信用使用
    fn record_credit_usage(&self, port: PortId, used: u32, remaining: u32);
}

/// 简单的指标收集器实现
#[derive(Default)]
pub struct SimpleMetricsCollector;

impl MetricsCollector for SimpleMetricsCollector {
    fn record_push(&self, operator_id: OperatorId, port: PortId, rows: u32) {
        // 简单的日志记录
        tracing::debug!("Push: Operator {} -> Port {} ({} rows)", operator_id, port, rows);
    }
    
    fn record_block(&self, operator_id: OperatorId, reason: &str) {
        tracing::debug!("Block: Operator {} ({})", operator_id, reason);
    }
    
    fn record_process_time(&self, operator_id: OperatorId, duration: Duration) {
        tracing::debug!("Process: Operator {} took {:?}", operator_id, duration);
    }
    
    fn record_credit_usage(&self, port: PortId, used: u32, remaining: u32) {
        tracing::debug!("Credit: Port {} used {} remaining {}", port, used, remaining);
    }
}
