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
    Data(PortId, RecordBatch),
    /// 信用事件（行数或批数）
    Credit(PortId, u32),
    /// 控制事件（运行时过滤器等）
    Ctrl(RuntimeFilter),
    /// 刷新事件
    Flush(PortId),
    /// 完成事件
    Finish(PortId),
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
    /// 错误
    Error(String),
}

/// 算子接口
pub trait Operator: Send {
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
