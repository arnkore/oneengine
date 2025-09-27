//! 输出邮箱
//! 
//! 管理算子输出和信用控制

use crate::push_runtime::{Event, PortId, OperatorId, RuntimeFilter, CreditManager, WouldBlock};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use anyhow::Result;

/// 输出邮箱
pub struct Outbox<'a> {
    /// 信用管理器
    credit_manager: &'a mut CreditManager,
    /// 事件队列
    event_queue: &'a mut Vec<Event>,
    /// 端口映射
    port_mapping: &'a HashMap<PortId, OperatorId>,
}

impl<'a> Outbox<'a> {
    /// 创建新的输出邮箱
    pub fn new(
        credit_manager: &'a mut CreditManager,
        event_queue: &'a mut Vec<Event>,
        port_mapping: &'a HashMap<PortId, OperatorId>,
    ) -> Self {
        Self {
            credit_manager,
            event_queue,
            port_mapping,
        }
    }
    
    /// 推送数据
    pub fn push(&mut self, to: PortId, batch: RecordBatch) -> Result<(), WouldBlock> {
        // 检查信用
        if !self.credit_manager.has_credit(to) {
            return Err(WouldBlock);
        }
        
        // 扣减信用
        self.credit_manager.consume_credit(to, batch.num_rows() as u32);
        
        // 发送数据事件
        self.event_queue.push(Event::Data { port: to, batch });
        Ok(())
    }
    
    /// 发送控制事件
    pub fn emit_ctrl(&mut self, _to: PortId, filter: RuntimeFilter) -> Result<()> {
        self.event_queue.push(Event::Ctrl(filter));
        Ok(())
    }
    
    /// 发送信用事件
    pub fn emit_credit(&mut self, to: PortId, credit: u32) {
        self.event_queue.push(Event::Credit(to, credit));
    }
    
    /// 发送刷新事件
    pub fn emit_flush(&mut self, to: PortId) {
        self.event_queue.push(Event::Flush(to));
    }
    
    /// 发送完成事件
    pub fn emit_finish(&mut self, to: PortId) {
        self.event_queue.push(Event::Finish(to));
    }
    
    /// 发送数据（简化版本）
    pub fn send(&mut self, to: PortId, batch: RecordBatch) {
        self.event_queue.push(Event::Data { port: to, batch });
    }
    
    /// 发送流结束事件
    pub fn send_eos(&mut self, to: PortId) {
        self.event_queue.push(Event::EndOfStream { port: to });
    }
}
