//! 信用管理器
//! 
//! 管理端口信用和背压控制

use std::collections::HashMap;
use crate::push_runtime::PortId;

/// 信用管理器
#[derive(Debug, Default)]
pub struct CreditManager {
    /// 端口信用
    credits: HashMap<PortId, u32>,
    /// 最大信用
    max_credits: HashMap<PortId, u32>,
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
