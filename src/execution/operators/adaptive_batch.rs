//! 自适应批次大小调整
//! 
//! 根据系统负载、内存使用情况和处理性能动态调整批次大小

use std::time::{Instant, Duration};
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, AtomicU64, Ordering};
use tracing::{debug, info, warn};

/// 批次性能统计
#[derive(Debug, Clone)]
pub struct BatchStats {
    /// 批次大小
    pub batch_size: usize,
    /// 处理时间（微秒）
    pub processing_time_us: u64,
    /// 内存使用量（字节）
    pub memory_usage_bytes: usize,
    /// 吞吐量（行/秒）
    pub throughput_rows_per_sec: f64,
    /// 内存效率（行/字节）
    pub memory_efficiency: f64,
    /// 时间戳
    pub timestamp: Instant,
}

/// 自适应批次配置
#[derive(Debug, Clone)]
pub struct AdaptiveBatchConfig {
    /// 初始批次大小
    pub initial_batch_size: usize,
    /// 最小批次大小
    pub min_batch_size: usize,
    /// 最大批次大小
    pub max_batch_size: usize,
    /// 批次大小调整步长
    pub adjustment_step: usize,
    /// 性能统计窗口大小
    pub stats_window_size: usize,
    /// 性能目标：目标吞吐量（行/秒）
    pub target_throughput: f64,
    /// 内存目标：目标内存使用量（字节）
    pub target_memory_usage: usize,
    /// 调整间隔（毫秒）
    pub adjustment_interval_ms: u64,
    /// 性能阈值：低于此值认为性能不佳
    pub performance_threshold: f64,
    /// 内存阈值：高于此值认为内存使用过多
    pub memory_threshold: f64,
}

impl Default for AdaptiveBatchConfig {
    fn default() -> Self {
        Self {
            initial_batch_size: 8192,
            min_batch_size: 1024,
            max_batch_size: 65536,
            adjustment_step: 1024,
            stats_window_size: 100,
            target_throughput: 1000000.0, // 100万行/秒
            target_memory_usage: 64 * 1024 * 1024, // 64MB
            adjustment_interval_ms: 1000, // 1秒
            performance_threshold: 0.8, // 80%
            memory_threshold: 1.2, // 120%
        }
    }
}

/// 自适应批次管理器
pub struct AdaptiveBatchManager {
    /// 配置
    config: AdaptiveBatchConfig,
    /// 当前批次大小
    current_batch_size: AtomicUsize,
    /// 性能统计历史
    stats_history: VecDeque<BatchStats>,
    /// 最后调整时间
    last_adjustment: Instant,
    /// 总处理行数
    total_rows_processed: AtomicU64,
    /// 总处理时间
    total_processing_time: AtomicU64,
    /// 当前内存使用量
    current_memory_usage: AtomicUsize,
    /// 性能趋势
    performance_trend: PerformanceTrend,
}

/// 性能趋势
#[derive(Debug, Clone, PartialEq)]
pub enum PerformanceTrend {
    /// 性能提升
    Improving,
    /// 性能稳定
    Stable,
    /// 性能下降
    Declining,
}

impl AdaptiveBatchManager {
    /// 创建新的自适应批次管理器
    pub fn new(config: AdaptiveBatchConfig) -> Self {
        Self {
            current_batch_size: AtomicUsize::new(config.initial_batch_size),
            stats_history: VecDeque::with_capacity(config.stats_window_size),
            last_adjustment: Instant::now(),
            total_rows_processed: AtomicU64::new(0),
            total_processing_time: AtomicU64::new(0),
            current_memory_usage: AtomicUsize::new(0),
            performance_trend: PerformanceTrend::Stable,
            config,
        }
    }
    
    /// 获取当前批次大小
    pub fn get_current_batch_size(&self) -> usize {
        self.current_batch_size.load(Ordering::Relaxed)
    }
    
    /// 记录批次处理统计
    pub fn record_batch_stats(&mut self, batch_size: usize, processing_time: Duration, memory_usage: usize) {
        let processing_time_us = processing_time.as_micros() as u64;
        let throughput = if processing_time_us > 0 {
            (batch_size as f64 * 1_000_000.0) / processing_time_us as f64
        } else {
            0.0
        };
        
        let memory_efficiency = if memory_usage > 0 {
            batch_size as f64 / memory_usage as f64
        } else {
            0.0
        };
        
        let stats = BatchStats {
            batch_size,
            processing_time_us,
            memory_usage_bytes: memory_usage,
            throughput_rows_per_sec: throughput,
            memory_efficiency,
            timestamp: Instant::now(),
        };
        
        // 添加到统计历史
        if self.stats_history.len() >= self.config.stats_window_size {
            self.stats_history.pop_front();
        }
        self.stats_history.push_back(stats);
        
        // 更新总统计
        self.total_rows_processed.fetch_add(batch_size as u64, Ordering::Relaxed);
        self.total_processing_time.fetch_add(processing_time_us, Ordering::Relaxed);
        self.current_memory_usage.store(memory_usage, Ordering::Relaxed);
        
        debug!("Recorded batch stats: size={}, time={}μs, memory={}B, throughput={:.2} rows/sec", 
               batch_size, processing_time_us, memory_usage, throughput);
    }
    
    /// 检查是否需要调整批次大小
    pub fn should_adjust_batch_size(&self) -> bool {
        let now = Instant::now();
        let time_since_last_adjustment = now.duration_since(self.last_adjustment);
        
        time_since_last_adjustment >= Duration::from_millis(self.config.adjustment_interval_ms)
            && self.stats_history.len() >= 10 // 至少需要10个样本
    }
    
    /// 调整批次大小
    pub fn adjust_batch_size(&mut self) -> Option<usize> {
        if !self.should_adjust_batch_size() {
            return None;
        }
        
        let current_size = self.current_batch_size.load(Ordering::Relaxed);
        let new_size = self.calculate_optimal_batch_size();
        
        if new_size != current_size {
            self.current_batch_size.store(new_size, Ordering::Relaxed);
            self.last_adjustment = Instant::now();
            
            info!("Adjusted batch size: {} -> {} (trend: {:?})", 
                  current_size, new_size, self.performance_trend);
            
            Some(new_size)
        } else {
            None
        }
    }
    
    /// 计算最优批次大小
    fn calculate_optimal_batch_size(&mut self) -> usize {
        let current_size = self.current_batch_size.load(Ordering::Relaxed);
        let recent_stats = self.get_recent_stats();
        
        if recent_stats.is_empty() {
            return current_size;
        }
        
        // 分析性能趋势
        self.analyze_performance_trend(&recent_stats);
        
        // 计算平均性能指标
        let avg_throughput = self.calculate_average_throughput(&recent_stats);
        let avg_memory_usage = self.calculate_average_memory_usage(&recent_stats);
        let avg_memory_efficiency = self.calculate_average_memory_efficiency(&recent_stats);
        
        debug!("Performance analysis: throughput={:.2}, memory={}B, efficiency={:.4}", 
               avg_throughput, avg_memory_usage, avg_memory_efficiency);
        
        // 根据性能指标调整批次大小
        let mut new_size = current_size;
        
        // 吞吐量优化
        if avg_throughput < self.config.target_throughput * self.config.performance_threshold {
            // 性能不佳，尝试增加批次大小
            new_size = (current_size + self.config.adjustment_step).min(self.config.max_batch_size);
            debug!("Increasing batch size for better throughput: {} -> {}", current_size, new_size);
        } else if avg_throughput > self.config.target_throughput * 1.2 {
            // 性能很好，可以尝试减少批次大小以节省内存
            if avg_memory_usage > self.config.target_memory_usage {
                new_size = (current_size - self.config.adjustment_step).max(self.config.min_batch_size);
                debug!("Decreasing batch size to save memory: {} -> {}", current_size, new_size);
            }
        }
        
        // 内存使用优化
        if avg_memory_usage > (self.config.target_memory_usage as f64 * self.config.memory_threshold) as usize {
            // 内存使用过多，减少批次大小
            new_size = (current_size - self.config.adjustment_step).max(self.config.min_batch_size);
            debug!("Decreasing batch size due to high memory usage: {} -> {}", current_size, new_size);
        } else if avg_memory_usage < (self.config.target_memory_usage as f64 * 0.5) as usize {
            // 内存使用较少，可以尝试增加批次大小
            if avg_throughput < self.config.target_throughput {
                new_size = (current_size + self.config.adjustment_step).min(self.config.max_batch_size);
                debug!("Increasing batch size due to low memory usage: {} -> {}", current_size, new_size);
            }
        }
        
        // 内存效率优化
        if avg_memory_efficiency < 0.001 { // 每字节少于0.001行
            // 内存效率低，减少批次大小
            new_size = (current_size - self.config.adjustment_step).max(self.config.min_batch_size);
            debug!("Decreasing batch size for better memory efficiency: {} -> {}", current_size, new_size);
        }
        
        new_size
    }
    
    /// 获取最近的统计信息
    fn get_recent_stats(&self) -> Vec<BatchStats> {
        let recent_count = (self.stats_history.len() / 2).max(5); // 至少5个，最多一半
        self.stats_history.iter()
            .rev()
            .take(recent_count)
            .cloned()
            .collect()
    }
    
    /// 分析性能趋势
    fn analyze_performance_trend(&mut self, stats: &[BatchStats]) {
        if stats.len() < 3 {
            self.performance_trend = PerformanceTrend::Stable;
            return;
        }
        
        let recent_throughput: Vec<f64> = stats.iter()
            .map(|s| s.throughput_rows_per_sec)
            .collect();
        
        // 计算趋势（简单的线性回归）
        let n = recent_throughput.len() as f64;
        let sum_x: f64 = (0..recent_throughput.len()).map(|i| i as f64).sum();
        let sum_y: f64 = recent_throughput.iter().sum();
        let sum_xy: f64 = recent_throughput.iter().enumerate()
            .map(|(i, &y)| i as f64 * y)
            .sum();
        let sum_x2: f64 = (0..recent_throughput.len()).map(|i| (i as f64).powi(2)).sum();
        
        let slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x * sum_x);
        
        if slope > 1000.0 {
            self.performance_trend = PerformanceTrend::Improving;
        } else if slope < -1000.0 {
            self.performance_trend = PerformanceTrend::Declining;
        } else {
            self.performance_trend = PerformanceTrend::Stable;
        }
    }
    
    /// 计算平均吞吐量
    fn calculate_average_throughput(&self, stats: &[BatchStats]) -> f64 {
        if stats.is_empty() {
            return 0.0;
        }
        
        stats.iter().map(|s| s.throughput_rows_per_sec).sum::<f64>() / stats.len() as f64
    }
    
    /// 计算平均内存使用量
    fn calculate_average_memory_usage(&self, stats: &[BatchStats]) -> usize {
        if stats.is_empty() {
            return 0;
        }
        
        stats.iter().map(|s| s.memory_usage_bytes).sum::<usize>() / stats.len()
    }
    
    /// 计算平均内存效率
    fn calculate_average_memory_efficiency(&self, stats: &[BatchStats]) -> f64 {
        if stats.is_empty() {
            return 0.0;
        }
        
        stats.iter().map(|s| s.memory_efficiency).sum::<f64>() / stats.len() as f64
    }
    
    /// 获取性能统计摘要
    pub fn get_performance_summary(&self) -> PerformanceSummary {
        let total_rows = self.total_rows_processed.load(Ordering::Relaxed);
        let total_time = self.total_processing_time.load(Ordering::Relaxed);
        let current_memory = self.current_memory_usage.load(Ordering::Relaxed);
        
        let overall_throughput = if total_time > 0 {
            (total_rows as f64 * 1_000_000.0) / total_time as f64
        } else {
            0.0
        };
        
        PerformanceSummary {
            current_batch_size: self.get_current_batch_size(),
            total_rows_processed: total_rows,
            overall_throughput,
            current_memory_usage: current_memory,
            performance_trend: self.performance_trend.clone(),
            stats_count: self.stats_history.len(),
        }
    }
    
    /// 重置统计信息
    pub fn reset_stats(&mut self) {
        self.stats_history.clear();
        self.total_rows_processed.store(0, Ordering::Relaxed);
        self.total_processing_time.store(0, Ordering::Relaxed);
        self.current_memory_usage.store(0, Ordering::Relaxed);
        self.performance_trend = PerformanceTrend::Stable;
        self.last_adjustment = Instant::now();
        
        info!("Reset adaptive batch statistics");
    }
}

/// 性能统计摘要
#[derive(Debug, Clone)]
pub struct PerformanceSummary {
    /// 当前批次大小
    pub current_batch_size: usize,
    /// 总处理行数
    pub total_rows_processed: u64,
    /// 整体吞吐量（行/秒）
    pub overall_throughput: f64,
    /// 当前内存使用量
    pub current_memory_usage: usize,
    /// 性能趋势
    pub performance_trend: PerformanceTrend,
    /// 统计样本数量
    pub stats_count: usize,
}

/// 自适应批次大小调整器（线程安全版本）
pub struct AdaptiveBatchAdjuster {
    manager: Arc<std::sync::Mutex<AdaptiveBatchManager>>,
}

impl AdaptiveBatchAdjuster {
    /// 创建新的自适应批次调整器
    pub fn new(config: AdaptiveBatchConfig) -> Self {
        Self {
            manager: Arc::new(std::sync::Mutex::new(AdaptiveBatchManager::new(config))),
        }
    }
    
    /// 获取当前批次大小
    pub fn get_current_batch_size(&self) -> usize {
        self.manager.lock().unwrap().get_current_batch_size()
    }
    
    /// 记录批次处理统计
    pub fn record_batch_stats(&self, batch_size: usize, processing_time: Duration, memory_usage: usize) {
        self.manager.lock().unwrap().record_batch_stats(batch_size, processing_time, memory_usage);
    }
    
    /// 调整批次大小
    pub fn adjust_batch_size(&self) -> Option<usize> {
        self.manager.lock().unwrap().adjust_batch_size()
    }
    
    /// 获取性能统计摘要
    pub fn get_performance_summary(&self) -> PerformanceSummary {
        self.manager.lock().unwrap().get_performance_summary()
    }
    
    /// 重置统计信息
    pub fn reset_stats(&self) {
        self.manager.lock().unwrap().reset_stats();
    }
}

impl Clone for AdaptiveBatchAdjuster {
    fn clone(&self) -> Self {
        Self {
            manager: Arc::clone(&self.manager),
        }
    }
}
