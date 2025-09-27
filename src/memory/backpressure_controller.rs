//! 背压控制器
//! 
//! 处理内存压力和流量控制

use anyhow::Result;
use std::sync::{Arc, Mutex, Condvar};
use std::time::{Duration, Instant};
use std::collections::VecDeque;
use tracing::{debug, warn, error};
use serde::{Serialize, Deserialize};

/// 背压配置
#[derive(Debug, Clone)]
pub struct BackpressureConfig {
    /// 高水位线（触发背压的阈值）
    pub high_watermark: f64,
    /// 低水位线（解除背压的阈值）
    pub low_watermark: f64,
    /// 最大队列大小
    pub max_queue_size: usize,
    /// 背压检查间隔
    pub check_interval: Duration,
    /// 最大等待时间
    pub max_wait_time: Duration,
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self {
            high_watermark: 0.8,  // 80%
            low_watermark: 0.6,   // 60%
            max_queue_size: 1000,
            check_interval: Duration::from_millis(100),
            max_wait_time: Duration::from_secs(30),
        }
    }
}

/// 背压状态
#[derive(Debug, Clone, PartialEq)]
pub enum BackpressureState {
    /// 正常状态
    Normal,
    /// 背压状态
    Backpressure,
    /// 严重背压状态
    SevereBackpressure,
}

/// 队列水位信息
#[derive(Debug, Clone)]
pub struct QueueWatermark {
    /// 当前队列大小
    pub current_size: usize,
    /// 最大队列大小
    pub max_size: usize,
    /// 水位百分比
    pub watermark_ratio: f64,
    /// 是否超过高水位线
    pub exceeds_high_watermark: bool,
    /// 是否超过低水位线
    pub exceeds_low_watermark: bool,
}

/// 背压事件
#[derive(Debug, Clone)]
pub enum BackpressureEvent {
    /// 进入背压状态
    EnterBackpressure,
    /// 退出背压状态
    ExitBackpressure,
    /// 进入严重背压状态
    EnterSevereBackpressure,
    /// 队列满
    QueueFull,
    /// 内存不足
    MemoryPressure,
}

/// 背压监听器
pub trait BackpressureListener: Send + Sync {
    /// 处理背压事件
    fn on_backpressure_event(&self, event: BackpressureEvent);
}

/// 背压控制器
pub struct BackpressureController {
    /// 配置
    config: BackpressureConfig,
    /// 当前状态
    state: Arc<Mutex<BackpressureState>>,
    /// 队列大小
    queue_size: Arc<Mutex<usize>>,
    /// 内存使用量
    memory_usage: Arc<Mutex<usize>>,
    /// 最大内存量
    max_memory: Arc<Mutex<usize>>,
    /// 等待队列
    waiting_threads: Arc<Mutex<VecDeque<Arc<Condvar>>>>,
    /// 条件变量
    condvar: Arc<Condvar>,
    /// 监听器
    listeners: Arc<Mutex<Vec<Arc<dyn BackpressureListener>>>>,
    /// 统计信息
    stats: Arc<Mutex<BackpressureStats>>,
}

/// 背压统计信息
#[derive(Debug, Clone, Default)]
pub struct BackpressureStats {
    /// 背压事件次数
    pub backpressure_events: u64,
    /// 严重背压事件次数
    pub severe_backpressure_events: u64,
    /// 队列满事件次数
    pub queue_full_events: u64,
    /// 内存压力事件次数
    pub memory_pressure_events: u64,
    /// 总等待时间
    pub total_wait_time: Duration,
    /// 平均等待时间
    pub avg_wait_time: Duration,
    /// 最大等待时间
    pub max_wait_time: Duration,
}

impl BackpressureController {
    /// 创建新的背压控制器
    pub fn new(config: BackpressureConfig) -> Self {
        Self {
            state: Arc::new(Mutex::new(BackpressureState::Normal)),
            queue_size: Arc::new(Mutex::new(0)),
            memory_usage: Arc::new(Mutex::new(0)),
            max_memory: Arc::new(Mutex::new(1024 * 1024 * 1024)), // 1GB
            waiting_threads: Arc::new(Mutex::new(VecDeque::new())),
            condvar: Arc::new(Condvar::new()),
            listeners: Arc::new(Mutex::new(Vec::new())),
            stats: Arc::new(Mutex::new(BackpressureStats::default())),
            config,
        }
    }

    /// 设置最大内存量
    pub fn set_max_memory(&self, max_memory: usize) {
        let mut memory = self.max_memory.lock().unwrap();
        *memory = max_memory;
    }

    /// 更新队列大小
    pub fn update_queue_size(&self, size: usize) -> Result<()> {
        let mut queue_size = self.queue_size.lock().unwrap();
        *queue_size = size;

        // 检查是否需要触发背压
        self.check_backpressure()?;
        Ok(())
    }

    /// 更新内存使用量
    pub fn update_memory_usage(&self, usage: usize) -> Result<()> {
        let mut memory_usage = self.memory_usage.lock().unwrap();
        *memory_usage = usage;

        // 检查是否需要触发背压
        self.check_backpressure()?;
        Ok(())
    }

    /// 等待背压解除
    pub fn wait_for_backpressure_relief(&self) -> Result<()> {
        let start_time = Instant::now();
        let mut state = self.state.lock().unwrap();

        while *state == BackpressureState::Backpressure || *state == BackpressureState::SevereBackpressure {
            // 检查是否超时
            if start_time.elapsed() > self.config.max_wait_time {
                return Err(anyhow::anyhow!("Backpressure wait timeout"));
            }

            // 等待背压解除
            state = self.condvar.wait_timeout(state, self.config.check_interval).unwrap().0;
        }

        // 更新统计信息
        let wait_time = start_time.elapsed();
        self.update_wait_stats(wait_time);

        Ok(())
    }

    /// 检查是否可以处理新任务
    pub fn can_process(&self) -> bool {
        let state = self.state.lock().unwrap();
        *state == BackpressureState::Normal
    }

    /// 获取当前状态
    pub fn get_state(&self) -> BackpressureState {
        let state = self.state.lock().unwrap();
        state.clone()
    }

    /// 获取队列水位信息
    pub fn get_queue_watermark(&self) -> QueueWatermark {
        let queue_size = *self.queue_size.lock().unwrap();
        let max_size = self.config.max_queue_size;
        let watermark_ratio = queue_size as f64 / max_size as f64;

        QueueWatermark {
            current_size: queue_size,
            max_size,
            watermark_ratio,
            exceeds_high_watermark: watermark_ratio >= self.config.high_watermark,
            exceeds_low_watermark: watermark_ratio >= self.config.low_watermark,
        }
    }

    /// 获取内存水位信息
    pub fn get_memory_watermark(&self) -> QueueWatermark {
        let memory_usage = *self.memory_usage.lock().unwrap();
        let max_memory = *self.max_memory.lock().unwrap();
        let watermark_ratio = memory_usage as f64 / max_memory as f64;

        QueueWatermark {
            current_size: memory_usage,
            max_size: max_memory,
            watermark_ratio,
            exceeds_high_watermark: watermark_ratio >= self.config.high_watermark,
            exceeds_low_watermark: watermark_ratio >= self.config.low_watermark,
        }
    }

    /// 添加监听器
    pub fn add_listener(&self, listener: Arc<dyn BackpressureListener>) {
        let mut listeners = self.listeners.lock().unwrap();
        listeners.push(listener);
    }

    /// 获取统计信息
    pub fn get_stats(&self) -> BackpressureStats {
        let stats = self.stats.lock().unwrap();
        stats.clone()
    }

    /// 检查背压状态
    fn check_backpressure(&self) -> Result<()> {
        let queue_watermark = self.get_queue_watermark();
        let memory_watermark = self.get_memory_watermark();

        let old_state = self.get_state();
        let new_state = self.determine_backpressure_state(&queue_watermark, &memory_watermark);

        if old_state != new_state {
            self.update_state(new_state)?;
        }

        Ok(())
    }

    /// 确定背压状态
    fn determine_backpressure_state(&self, queue_watermark: &QueueWatermark, memory_watermark: &QueueWatermark) -> BackpressureState {
        // 检查严重背压条件
        if queue_watermark.exceeds_high_watermark && memory_watermark.exceeds_high_watermark {
            return BackpressureState::SevereBackpressure;
        }

        // 检查普通背压条件
        if queue_watermark.exceeds_high_watermark || memory_watermark.exceeds_high_watermark {
            return BackpressureState::Backpressure;
        }

        // 检查是否恢复正常
        if !queue_watermark.exceeds_low_watermark && !memory_watermark.exceeds_low_watermark {
            return BackpressureState::Normal;
        }

        // 保持当前状态
        self.get_state()
    }

    /// 更新状态
    fn update_state(&self, new_state: BackpressureState) -> Result<()> {
        let old_state = self.get_state();
        
        {
            let mut state = self.state.lock().unwrap();
            *state = new_state.clone();
        }

        // 触发事件
        self.trigger_state_change_event(&old_state, &new_state);

        // 通知等待的线程
        self.condvar.notify_all();

        debug!("Backpressure state changed: {:?} -> {:?}", old_state, new_state);
        Ok(())
    }

    /// 触发状态变化事件
    fn trigger_state_change_event(&self, old_state: &BackpressureState, new_state: &BackpressureState) {
        let event = match (old_state, new_state) {
            (BackpressureState::Normal, BackpressureState::Backpressure) => {
                self.increment_backpressure_events();
                BackpressureEvent::EnterBackpressure
            }
            (BackpressureState::Backpressure, BackpressureState::Normal) => {
                BackpressureEvent::ExitBackpressure
            }
            (_, BackpressureState::SevereBackpressure) => {
                self.increment_severe_backpressure_events();
                BackpressureEvent::EnterSevereBackpressure
            }
            _ => return,
        };

        self.notify_listeners(event);
    }

    /// 通知监听器
    fn notify_listeners(&self, event: BackpressureEvent) {
        let listeners = self.listeners.lock().unwrap();
        for listener in listeners.iter() {
            listener.on_backpressure_event(event.clone());
        }
    }

    /// 增加背压事件计数
    fn increment_backpressure_events(&self) {
        let mut stats = self.stats.lock().unwrap();
        stats.backpressure_events += 1;
    }

    /// 增加严重背压事件计数
    fn increment_severe_backpressure_events(&self) {
        let mut stats = self.stats.lock().unwrap();
        stats.severe_backpressure_events += 1;
    }

    /// 更新等待统计
    fn update_wait_stats(&self, wait_time: Duration) {
        let mut stats = self.stats.lock().unwrap();
        stats.total_wait_time += wait_time;
        
        // 更新平均等待时间
        let total_waits = stats.backpressure_events + stats.severe_backpressure_events;
        if total_waits > 0 {
            stats.avg_wait_time = Duration::from_nanos(
                stats.total_wait_time.as_nanos() as u64 / total_waits as u64
            );
        }

        // 更新最大等待时间
        if wait_time > stats.max_wait_time {
            stats.max_wait_time = wait_time;
        }
    }
}

/// 简单的背压监听器实现
pub struct SimpleBackpressureListener {
    pub name: String,
}

impl BackpressureListener for SimpleBackpressureListener {
    fn on_backpressure_event(&self, event: BackpressureEvent) {
        match event {
            BackpressureEvent::EnterBackpressure => {
                warn!("[{}] Entered backpressure state", self.name);
            }
            BackpressureEvent::ExitBackpressure => {
                debug!("[{}] Exited backpressure state", self.name);
            }
            BackpressureEvent::EnterSevereBackpressure => {
                error!("[{}] Entered severe backpressure state", self.name);
            }
            BackpressureEvent::QueueFull => {
                error!("[{}] Queue is full", self.name);
            }
            BackpressureEvent::MemoryPressure => {
                warn!("[{}] Memory pressure detected", self.name);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_backpressure_controller_creation() {
        let config = BackpressureConfig::default();
        let controller = BackpressureController::new(config);
        assert_eq!(controller.get_state(), BackpressureState::Normal);
    }

    #[test]
    fn test_queue_watermark() {
        let config = BackpressureConfig {
            max_queue_size: 100,
            high_watermark: 0.8,
            low_watermark: 0.6,
            ..Default::default()
        };
        let controller = BackpressureController::new(config);

        // 测试低水位线
        controller.update_queue_size(50).unwrap();
        let watermark = controller.get_queue_watermark();
        assert!(!watermark.exceeds_high_watermark);
        assert!(!watermark.exceeds_low_watermark);

        // 测试高水位线
        controller.update_queue_size(90).unwrap();
        let watermark = controller.get_queue_watermark();
        assert!(watermark.exceeds_high_watermark);
        assert!(watermark.exceeds_low_watermark);
    }

    #[test]
    fn test_memory_watermark() {
        let config = BackpressureConfig::default();
        let controller = BackpressureController::new(config);
        controller.set_max_memory(1000);

        // 测试低水位线
        controller.update_memory_usage(500).unwrap();
        let watermark = controller.get_memory_watermark();
        assert!(!watermark.exceeds_high_watermark);
        assert!(!watermark.exceeds_low_watermark);

        // 测试高水位线
        controller.update_memory_usage(900).unwrap();
        let watermark = controller.get_memory_watermark();
        assert!(watermark.exceeds_high_watermark);
        assert!(watermark.exceeds_low_watermark);
    }
}
