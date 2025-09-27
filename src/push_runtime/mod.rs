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


//! Push运行时核心
//! 
//! 基于Apache Arrow的纯push事件驱动执行架构

pub mod event_loop;
pub mod credit_manager;
pub mod outbox;
pub mod operator_context;
pub mod metrics;

use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;
use anyhow::Result;

/// 端口ID
pub type PortId = u32;

/// 算子ID
pub type OperatorId = u32;

/// 事件类型
#[derive(Debug, Clone)]
pub enum Event {
    /// 数据事件
    Data { port: PortId, batch: RecordBatch },
    /// 信用事件（行数或批数）
    Credit(PortId, u32),
    /// 控制事件（运行时过滤器等）
    Ctrl(RuntimeFilter),
    /// 刷新事件
    Flush(PortId),
    /// 完成事件
    Finish(PortId),
    /// 开始扫描事件
    StartScan { file_path: String },
    /// 流结束事件
    EndOfStream { port: PortId },
}

/// 运行时过滤器
#[derive(Debug, Clone)]
pub enum RuntimeFilter {
    /// Bloom过滤器
    Bloom { column: String, filter: Vec<u8> },
    /// IN过滤器
    In { column: String, values: Vec<String> },
    /// MinMax过滤器
    MinMax { column: String, min: String, max: String },
}

/// 算子状态
#[derive(Debug, Clone, PartialEq)]
pub enum OpStatus {
    /// 就绪
    Ready,
    /// 阻塞（等待信用）
    Blocked,
    /// 完成
    Finished,
    /// 有更多数据
    HasMore,
    /// 错误
    Error(String),
}

/// 算子接口
pub trait Operator: Send + Sync {
    /// 注册时调用
    fn on_register(&mut self, ctx: OperatorContext) -> Result<()>;
    
    /// 处理事件
    fn on_event(&mut self, ev: Event, out: &mut Outbox) -> OpStatus;
    
    /// 是否完成
    fn is_finished(&self) -> bool;
    
    /// 获取算子名称
    fn name(&self) -> &str;
}

// 重新导出模块中的类型
pub use credit_manager::CreditManager;
pub use outbox::Outbox;
pub use operator_context::OperatorContext;
pub use metrics::{MetricsCollector, SimpleMetricsCollector};


/// 阻塞错误
#[derive(Debug, Clone, PartialEq)]
pub struct WouldBlock;

impl std::fmt::Display for WouldBlock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Operation would block")
    }
}

impl std::error::Error for WouldBlock {}
