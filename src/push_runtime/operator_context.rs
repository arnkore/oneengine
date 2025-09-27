//! 算子上下文
//! 
//! 提供算子运行时的上下文信息

use crate::push_runtime::{PortId, OperatorId, MetricsCollector};
use std::sync::Arc;

/// 算子上下文
pub struct OperatorContext {
    /// 算子ID
    pub operator_id: OperatorId,
    /// 输入端口
    pub input_ports: Vec<PortId>,
    /// 输出端口
    pub output_ports: Vec<PortId>,
    /// 指标收集器
    pub metrics: Arc<dyn MetricsCollector>,
}

impl OperatorContext {
    /// 创建新的算子上下文
    pub fn new(
        operator_id: OperatorId,
        input_ports: Vec<PortId>,
        output_ports: Vec<PortId>,
        metrics: Arc<dyn MetricsCollector>,
    ) -> Self {
        Self {
            operator_id,
            input_ports,
            output_ports,
            metrics,
        }
    }
}
