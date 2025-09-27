//! Join build-side自适应选择
//! 
//! 根据数据大小、内存使用情况和系统负载动态选择Join的build side

use std::collections::HashMap;
use std::time::{Instant, Duration};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, AtomicU64, Ordering};
use tracing::{debug, info, warn};

use arrow::record_batch::RecordBatch;
use arrow::datatypes::SchemaRef;

/// Join策略
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum JoinStrategy {
    /// 左表作为build side
    LeftBuild,
    /// 右表作为build side
    RightBuild,
    /// 自动选择build side
    Auto,
    /// 广播Join
    Broadcast,
    /// 分区Join
    Partitioned,
}

/// Join统计信息
#[derive(Debug, Clone)]
pub struct JoinStats {
    /// 左表大小（行数）
    pub left_size: usize,
    /// 右表大小（行数）
    pub right_size: usize,
    /// 左表内存使用量（字节）
    pub left_memory: usize,
    /// 右表内存使用量（字节）
    pub right_memory: usize,
    /// 选择的策略
    pub selected_strategy: JoinStrategy,
    /// 处理时间（微秒）
    pub processing_time_us: u64,
    /// 输出行数
    pub output_rows: usize,
    /// 内存效率（输出行数/内存使用量）
    pub memory_efficiency: f64,
    /// 时间戳
    pub timestamp: Instant,
}

/// Join自适应配置
#[derive(Debug, Clone)]
pub struct JoinAdaptationConfig {
    /// 内存阈值（字节）
    pub memory_threshold: usize,
    /// 广播Join阈值（行数）
    pub broadcast_threshold: usize,
    /// 分区Join阈值（行数）
    pub partition_threshold: usize,
    /// 内存效率阈值
    pub memory_efficiency_threshold: f64,
    /// 统计窗口大小
    pub stats_window_size: usize,
    /// 学习率（用于策略调整）
    pub learning_rate: f64,
    /// 最小样本数（用于策略选择）
    pub min_samples: usize,
    /// 策略切换阈值
    pub strategy_switch_threshold: f64,
}

impl Default for JoinAdaptationConfig {
    fn default() -> Self {
        Self {
            memory_threshold: 100 * 1024 * 1024, // 100MB
            broadcast_threshold: 10000, // 1万行
            partition_threshold: 1000000, // 100万行
            memory_efficiency_threshold: 0.01, // 每字节0.01行
            stats_window_size: 50,
            learning_rate: 0.1,
            min_samples: 5,
            strategy_switch_threshold: 0.2, // 20%性能提升
        }
    }
}

/// Join自适应选择器
pub struct JoinAdaptationSelector {
    /// 配置
    config: JoinAdaptationConfig,
    /// 统计历史
    stats_history: Vec<JoinStats>,
    /// 策略性能统计
    strategy_performance: HashMap<JoinStrategy, StrategyPerformance>,
    /// 总Join次数
    total_joins: AtomicUsize,
    /// 总处理时间
    total_processing_time: AtomicU64,
    /// 总输出行数
    total_output_rows: AtomicU64,
}

/// 策略性能统计
#[derive(Debug, Clone)]
pub struct StrategyPerformance {
    /// 使用次数
    pub usage_count: usize,
    /// 平均处理时间（微秒）
    pub avg_processing_time: f64,
    /// 平均内存效率
    pub avg_memory_efficiency: f64,
    /// 平均输出行数
    pub avg_output_rows: f64,
    /// 成功率
    pub success_rate: f64,
    /// 最后更新时间
    pub last_updated: Instant,
}

impl Default for StrategyPerformance {
    fn default() -> Self {
        Self {
            usage_count: 0,
            avg_processing_time: 0.0,
            avg_memory_efficiency: 0.0,
            avg_output_rows: 0.0,
            success_rate: 1.0,
            last_updated: Instant::now(),
        }
    }
}

impl JoinAdaptationSelector {
    /// 创建新的Join自适应选择器
    pub fn new(config: JoinAdaptationConfig) -> Self {
        Self {
            strategy_performance: HashMap::new(),
            stats_history: Vec::with_capacity(config.stats_window_size),
            total_joins: AtomicUsize::new(0),
            total_processing_time: AtomicU64::new(0),
            total_output_rows: AtomicU64::new(0),
            config,
        }
    }
    
    /// 选择Join策略
    pub fn select_join_strategy(
        &mut self,
        left_batch: &RecordBatch,
        right_batch: &RecordBatch,
        available_memory: usize,
    ) -> JoinStrategy {
        let left_size = left_batch.num_rows();
        let right_size = right_batch.num_rows();
        let left_memory = self.estimate_memory_usage(left_batch);
        let right_memory = self.estimate_memory_usage(right_batch);
        
        debug!("Selecting join strategy: left={} rows ({}B), right={} rows ({}B), available_memory={}B",
               left_size, left_memory, right_size, right_memory, available_memory);
        
        // 基于规则的初步选择
        let mut strategy = self.rule_based_selection(left_size, right_size, left_memory, right_memory, available_memory);
        
        // 基于历史性能的学习调整
        if self.stats_history.len() >= self.config.min_samples {
            strategy = self.learning_based_selection(strategy, left_size, right_size, left_memory, right_memory);
        }
        
        debug!("Selected join strategy: {:?}", strategy);
        strategy
    }
    
    /// 基于规则的策略选择
    fn rule_based_selection(
        &self,
        left_size: usize,
        right_size: usize,
        left_memory: usize,
        right_memory: usize,
        available_memory: usize,
    ) -> JoinStrategy {
        // 1. 检查是否可以广播
        if left_size <= self.config.broadcast_threshold && left_memory <= available_memory / 4 {
            return JoinStrategy::Broadcast;
        }
        if right_size <= self.config.broadcast_threshold && right_memory <= available_memory / 4 {
            return JoinStrategy::Broadcast;
        }
        
        // 2. 检查是否需要分区
        if left_size > self.config.partition_threshold || right_size > self.config.partition_threshold {
            return JoinStrategy::Partitioned;
        }
        
        // 3. 选择build side
        if left_memory <= right_memory {
            JoinStrategy::LeftBuild
        } else {
            JoinStrategy::RightBuild
        }
    }
    
    /// 基于学习的选择调整
    fn learning_based_selection(
        &self,
        initial_strategy: JoinStrategy,
        left_size: usize,
        right_size: usize,
        left_memory: usize,
        right_memory: usize,
    ) -> JoinStrategy {
        // 获取相似场景的历史性能
        let similar_stats = self.find_similar_scenarios(left_size, right_size, left_memory, right_memory);
        
        if similar_stats.is_empty() {
            return initial_strategy;
        }
        
        // 计算各策略的平均性能
        let mut strategy_scores = HashMap::new();
        
        for stats in &similar_stats {
            let score = self.calculate_strategy_score(stats);
            let entry = strategy_scores.entry(stats.selected_strategy.clone()).or_insert(Vec::new());
            entry.push(score);
        }
        
        // 选择最佳策略
        let mut best_strategy = initial_strategy.clone();
        let mut best_score = f64::NEG_INFINITY;
        
        for (strategy, scores) in strategy_scores {
            let avg_score = scores.iter().sum::<f64>() / scores.len() as f64;
            if avg_score > best_score {
                best_score = avg_score;
                best_strategy = strategy;
            }
        }
        
        // 如果性能提升超过阈值，则切换策略
        if best_score > self.calculate_strategy_score(&JoinStats {
            selected_strategy: best_strategy.clone(),
            processing_time_us: 0,
            memory_efficiency: 0.0,
            output_rows: 0,
            left_size,
            right_size,
            left_memory,
            right_memory,
            timestamp: Instant::now(),
        }) + self.config.strategy_switch_threshold {
            debug!("Switching strategy from {:?} to {:?} (score improvement: {:.2})", 
                   initial_strategy, best_strategy, best_score);
            best_strategy
        } else {
            initial_strategy
        }
    }
    
    /// 查找相似场景
    fn find_similar_scenarios(
        &self,
        left_size: usize,
        right_size: usize,
        left_memory: usize,
        right_memory: usize,
    ) -> Vec<JoinStats> {
        let mut similar = Vec::new();
        
        for stats in &self.stats_history {
            let size_similarity = self.calculate_size_similarity(
                left_size, right_size, stats.left_size, stats.right_size
            );
            let memory_similarity = self.calculate_memory_similarity(
                left_memory, right_memory, stats.left_memory, stats.right_memory
            );
            
            if size_similarity > 0.8 && memory_similarity > 0.8 {
                similar.push(stats.clone());
            }
        }
        
        similar
    }
    
    /// 计算大小相似度
    fn calculate_size_similarity(
        &self,
        left1: usize, right1: usize,
        left2: usize, right2: usize,
    ) -> f64 {
        let left_sim = 1.0 - (left1 as f64 - left2 as f64).abs() / (left1 + left2).max(1) as f64;
        let right_sim = 1.0 - (right1 as f64 - right2 as f64).abs() / (right1 + right2).max(1) as f64;
        (left_sim + right_sim) / 2.0
    }
    
    /// 计算内存相似度
    fn calculate_memory_similarity(
        &self,
        left1: usize, right1: usize,
        left2: usize, right2: usize,
    ) -> f64 {
        let left_sim = 1.0 - (left1 as f64 - left2 as f64).abs() / (left1 + left2).max(1) as f64;
        let right_sim = 1.0 - (right1 as f64 - right2 as f64).abs() / (right1 + right2).max(1) as f64;
        (left_sim + right_sim) / 2.0
    }
    
    /// 计算策略得分
    fn calculate_strategy_score(&self, stats: &JoinStats) -> f64 {
        let time_score = if stats.processing_time_us > 0 {
            1.0 / (stats.processing_time_us as f64 / 1_000_000.0) // 转换为秒
        } else {
            0.0
        };
        
        let memory_score = stats.memory_efficiency;
        let output_score = stats.output_rows as f64 / 1000.0; // 标准化
        
        time_score * 0.4 + memory_score * 0.4 + output_score * 0.2
    }
    
    /// 估算内存使用量
    fn estimate_memory_usage(&self, batch: &RecordBatch) -> usize {
        batch.get_array_memory_size()
    }
    
    /// 记录Join统计信息
    pub fn record_join_stats(&mut self, stats: JoinStats) {
        // 添加到统计历史
        if self.stats_history.len() >= self.config.stats_window_size {
            self.stats_history.remove(0);
        }
        self.stats_history.push(stats.clone());
        
        // 更新总统计
        self.total_joins.fetch_add(1, Ordering::Relaxed);
        self.total_processing_time.fetch_add(stats.processing_time_us, Ordering::Relaxed);
        self.total_output_rows.fetch_add(stats.output_rows as u64, Ordering::Relaxed);
        
        // 更新策略性能统计
        self.update_strategy_performance(&stats);
        
        debug!("Recorded join stats: strategy={:?}, time={}μs, output={} rows, efficiency={:.4}",
               stats.selected_strategy, stats.processing_time_us, stats.output_rows, stats.memory_efficiency);
    }
    
    /// 更新策略性能统计
    fn update_strategy_performance(&mut self, stats: &JoinStats) {
        let strategy = &stats.selected_strategy;
        let perf = self.strategy_performance.entry(strategy.clone()).or_insert_with(Default::default);
        
        // 使用指数移动平均更新
        let alpha = self.config.learning_rate;
        
        perf.avg_processing_time = alpha * stats.processing_time_us as f64 + (1.0 - alpha) * perf.avg_processing_time;
        perf.avg_memory_efficiency = alpha * stats.memory_efficiency + (1.0 - alpha) * perf.avg_memory_efficiency;
        perf.avg_output_rows = alpha * stats.output_rows as f64 + (1.0 - alpha) * perf.avg_output_rows;
        
        perf.usage_count += 1;
        perf.last_updated = Instant::now();
        
        // 更新成功率（简化版本）
        if stats.output_rows > 0 {
            perf.success_rate = alpha * 1.0 + (1.0 - alpha) * perf.success_rate;
        } else {
            perf.success_rate = alpha * 0.0 + (1.0 - alpha) * perf.success_rate;
        }
    }
    
    /// 获取策略性能摘要
    pub fn get_strategy_performance_summary(&self) -> HashMap<JoinStrategy, StrategyPerformance> {
        self.strategy_performance.clone()
    }
    
    /// 获取整体性能摘要
    pub fn get_overall_performance_summary(&self) -> OverallPerformanceSummary {
        let total_joins = self.total_joins.load(Ordering::Relaxed);
        let total_time = self.total_processing_time.load(Ordering::Relaxed);
        let total_output = self.total_output_rows.load(Ordering::Relaxed);
        
        let avg_processing_time = if total_joins > 0 {
            total_time as f64 / total_joins as f64
        } else {
            0.0
        };
        
        let avg_output_rows = if total_joins > 0 {
            total_output as f64 / total_joins as f64
        } else {
            0.0
        };
        
        OverallPerformanceSummary {
            total_joins,
            avg_processing_time,
            avg_output_rows,
            strategy_count: self.strategy_performance.len(),
            stats_count: self.stats_history.len(),
        }
    }
    
    /// 重置统计信息
    pub fn reset_stats(&mut self) {
        self.stats_history.clear();
        self.strategy_performance.clear();
        self.total_joins.store(0, Ordering::Relaxed);
        self.total_processing_time.store(0, Ordering::Relaxed);
        self.total_output_rows.store(0, Ordering::Relaxed);
        
        info!("Reset join adaptation statistics");
    }
}

/// 整体性能摘要
#[derive(Debug, Clone)]
pub struct OverallPerformanceSummary {
    /// 总Join次数
    pub total_joins: usize,
    /// 平均处理时间（微秒）
    pub avg_processing_time: f64,
    /// 平均输出行数
    pub avg_output_rows: f64,
    /// 策略数量
    pub strategy_count: usize,
    /// 统计样本数
    pub stats_count: usize,
}

/// Join自适应选择器（线程安全版本）
pub struct JoinAdaptationSelectorSync {
    selector: Arc<std::sync::Mutex<JoinAdaptationSelector>>,
}

impl JoinAdaptationSelectorSync {
    /// 创建新的Join自适应选择器
    pub fn new(config: JoinAdaptationConfig) -> Self {
        Self {
            selector: Arc::new(std::sync::Mutex::new(JoinAdaptationSelector::new(config))),
        }
    }
    
    /// 选择Join策略
    pub fn select_join_strategy(
        &self,
        left_batch: &RecordBatch,
        right_batch: &RecordBatch,
        available_memory: usize,
    ) -> JoinStrategy {
        self.selector.lock().unwrap().select_join_strategy(left_batch, right_batch, available_memory)
    }
    
    /// 记录Join统计信息
    pub fn record_join_stats(&self, stats: JoinStats) {
        self.selector.lock().unwrap().record_join_stats(stats);
    }
    
    /// 获取策略性能摘要
    pub fn get_strategy_performance_summary(&self) -> HashMap<JoinStrategy, StrategyPerformance> {
        self.selector.lock().unwrap().get_strategy_performance_summary()
    }
    
    /// 获取整体性能摘要
    pub fn get_overall_performance_summary(&self) -> OverallPerformanceSummary {
        self.selector.lock().unwrap().get_overall_performance_summary()
    }
    
    /// 重置统计信息
    pub fn reset_stats(&self) {
        self.selector.lock().unwrap().reset_stats();
    }
}

impl Clone for JoinAdaptationSelectorSync {
    fn clone(&self) -> Self {
        Self {
            selector: Arc::clone(&self.selector),
        }
    }
}
