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
