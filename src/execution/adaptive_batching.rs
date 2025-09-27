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


//! 自适应批量
//! 
//! 基于L2/L3 miss和算子堵塞时间在线调节batch size

use std::collections::HashMap;
use std::time::{Duration, Instant};
use std::sync::atomic::{AtomicUsize, AtomicU64, Ordering};
use std::sync::Arc;
use arrow::record_batch::RecordBatch;
use crate::push_runtime::{Event, OpStatus, Outbox};

/// 自适应批量管理器
pub struct AdaptiveBatchingManager {
    /// 当前批量大小
    current_batch_size: AtomicUsize,
    /// 最小批量大小
    min_batch_size: usize,
    /// 最大批量大小
    max_batch_size: usize,
    /// 性能指标收集器
    metrics_collector: Arc<PerformanceMetricsCollector>,
    /// 自适应策略
    adaptation_strategy: AdaptationStrategy,
    /// 历史性能数据
    performance_history: Vec<PerformanceSnapshot>,
}

/// 性能指标收集器
pub struct PerformanceMetricsCollector {
    /// L2缓存未命中次数
    l2_misses: AtomicU64,
    /// L3缓存未命中次数
    l3_misses: AtomicU64,
    /// 算子堵塞时间（微秒）
    operator_block_time: AtomicU64,
    /// 处理时间（微秒）
    processing_time: AtomicU64,
    /// 批次数量
    batch_count: AtomicU64,
}

/// 自适应策略
pub enum AdaptationStrategy {
    /// 基于缓存未命中率
    CacheMissBased,
    /// 基于算子堵塞时间
    OperatorBlockTimeBased,
    /// 混合策略
    Hybrid,
    /// 机器学习策略
    MachineLearning,
}

/// 性能快照
#[derive(Debug, Clone)]
pub struct PerformanceSnapshot {
    /// 时间戳
    timestamp: Instant,
    /// 批量大小
    batch_size: usize,
    /// L2未命中率
    l2_miss_rate: f64,
    /// L3未命中率
    l3_miss_rate: f64,
    /// 平均堵塞时间
    avg_block_time: Duration,
    /// 平均处理时间
    avg_processing_time: Duration,
    /// 吞吐量（行/秒）
    throughput: f64,
    /// 算子堵塞时间
    operator_blocking_time: Duration,
    /// 内存使用
    memory_usage: usize,
    /// CPU使用率
    cpu_usage: f64,
    /// 内存压力
    memory_pressure: f64,
}

/// 批量调整建议
#[derive(Debug, Clone)]
pub enum BatchAdjustment {
    /// 增加批量大小
    Increase(usize),
    /// 减少批量大小
    Decrease(usize),
    /// 保持当前大小
    Keep,
}

impl AdaptiveBatchingManager {
    /// 创建新的自适应批量管理器
    pub fn new(
        initial_batch_size: usize,
        min_batch_size: usize,
        max_batch_size: usize,
        strategy: AdaptationStrategy,
    ) -> Self {
        Self {
            current_batch_size: AtomicUsize::new(initial_batch_size),
            min_batch_size,
            max_batch_size,
            metrics_collector: Arc::new(PerformanceMetricsCollector::new()),
            adaptation_strategy: strategy,
            performance_history: Vec::new(),
        }
    }
    
    /// 获取当前批量大小
    pub fn get_current_batch_size(&self) -> usize {
        self.current_batch_size.load(Ordering::Relaxed)
    }
    
    /// 记录性能指标
    pub fn record_metrics(&self, 
        l2_misses: u64, 
        l3_misses: u64, 
        block_time: Duration, 
        processing_time: Duration
    ) {
        self.metrics_collector.l2_misses.fetch_add(l2_misses, Ordering::Relaxed);
        self.metrics_collector.l3_misses.fetch_add(l3_misses, Ordering::Relaxed);
        self.metrics_collector.operator_block_time.fetch_add(block_time.as_micros() as u64, Ordering::Relaxed);
        self.metrics_collector.processing_time.fetch_add(processing_time.as_micros() as u64, Ordering::Relaxed);
        self.metrics_collector.batch_count.fetch_add(1, Ordering::Relaxed);
    }
    
    /// 分析性能并调整批量大小
    pub fn analyze_and_adjust(&mut self) -> BatchAdjustment {
        let current_metrics = self.collect_current_metrics();
        self.performance_history.push(current_metrics.clone());
        
        // 保持历史数据在合理范围内
        if self.performance_history.len() > 100 {
            self.performance_history.drain(0..50);
        }
        
        let adjustment = match self.adaptation_strategy {
            AdaptationStrategy::CacheMissBased => {
                self.adjust_based_on_cache_misses(&current_metrics)
            },
            AdaptationStrategy::OperatorBlockTimeBased => {
                self.adjust_based_on_block_time(&current_metrics)
            },
            AdaptationStrategy::Hybrid => {
                self.adjust_hybrid(&current_metrics)
            },
            AdaptationStrategy::MachineLearning => {
                self.adjust_with_ml(&current_metrics)
            },
        };
        
        self.apply_adjustment(&adjustment);
        adjustment
    }
    
    /// 收集当前性能指标
    fn collect_current_metrics(&self) -> PerformanceSnapshot {
        let batch_count = self.metrics_collector.batch_count.load(Ordering::Relaxed);
        let l2_misses = self.metrics_collector.l2_misses.load(Ordering::Relaxed);
        let l3_misses = self.metrics_collector.l3_misses.load(Ordering::Relaxed);
        let block_time = self.metrics_collector.operator_block_time.load(Ordering::Relaxed);
        let processing_time = self.metrics_collector.processing_time.load(Ordering::Relaxed);
        
        let l2_miss_rate = if batch_count > 0 { l2_misses as f64 / batch_count as f64 } else { 0.0 };
        let l3_miss_rate = if batch_count > 0 { l3_misses as f64 / batch_count as f64 } else { 0.0 };
        let avg_block_time = if batch_count > 0 { 
            Duration::from_micros(block_time / batch_count) 
        } else { 
            Duration::from_micros(0) 
        };
        let avg_processing_time = if batch_count > 0 { 
            Duration::from_micros(processing_time / batch_count) 
        } else { 
            Duration::from_micros(0) 
        };
        
        let current_batch_size = self.get_current_batch_size();
        let throughput = if avg_processing_time.as_micros() > 0 {
            current_batch_size as f64 / (avg_processing_time.as_micros() as f64 / 1_000_000.0)
        } else {
            0.0
        };
        
        PerformanceSnapshot {
            timestamp: Instant::now(),
            batch_size: current_batch_size,
            l2_miss_rate,
            l3_miss_rate,
            avg_block_time,
            avg_processing_time,
            throughput,
            operator_blocking_time: Duration::from_millis(0),
            memory_usage: 0,
            cpu_usage: 0.0,
            memory_pressure: 0.0,
        }
    }
    
    /// 基于缓存未命中率调整
    fn adjust_based_on_cache_misses(&self, metrics: &PerformanceSnapshot) -> BatchAdjustment {
        let l2_threshold = 0.1; // 10% L2未命中率阈值
        let l3_threshold = 0.05; // 5% L3未命中率阈值
        
        if metrics.l2_miss_rate > l2_threshold || metrics.l3_miss_rate > l3_threshold {
            // 缓存未命中率高，减少批量大小
            let new_size = (metrics.batch_size as f64 * 0.8) as usize;
            let new_size = new_size.max(self.min_batch_size);
            if new_size < metrics.batch_size {
                BatchAdjustment::Decrease(metrics.batch_size - new_size)
            } else {
                BatchAdjustment::Keep
            }
        } else if metrics.l2_miss_rate < l2_threshold * 0.5 && metrics.l3_miss_rate < l3_threshold * 0.5 {
            // 缓存未命中率低，可以增加批量大小
            let new_size = (metrics.batch_size as f64 * 1.2) as usize;
            let new_size = new_size.min(self.max_batch_size);
            if new_size > metrics.batch_size {
                BatchAdjustment::Increase(new_size - metrics.batch_size)
            } else {
                BatchAdjustment::Keep
            }
        } else {
            BatchAdjustment::Keep
        }
    }
    
    /// 基于算子堵塞时间调整
    fn adjust_based_on_block_time(&self, metrics: &PerformanceSnapshot) -> BatchAdjustment {
        let block_time_threshold = Duration::from_millis(10); // 10ms堵塞时间阈值
        
        if metrics.avg_block_time > block_time_threshold {
            // 堵塞时间长，减少批量大小
            let new_size = (metrics.batch_size as f64 * 0.7) as usize;
            let new_size = new_size.max(self.min_batch_size);
            if new_size < metrics.batch_size {
                BatchAdjustment::Decrease(metrics.batch_size - new_size)
            } else {
                BatchAdjustment::Keep
            }
        } else if metrics.avg_block_time < block_time_threshold / 2 {
            // 堵塞时间短，可以增加批量大小
            let new_size = (metrics.batch_size as f64 * 1.3) as usize;
            let new_size = new_size.min(self.max_batch_size);
            if new_size > metrics.batch_size {
                BatchAdjustment::Increase(new_size - metrics.batch_size)
            } else {
                BatchAdjustment::Keep
            }
        } else {
            BatchAdjustment::Keep
        }
    }
    
    /// 混合策略调整
    fn adjust_hybrid(&self, metrics: &PerformanceSnapshot) -> BatchAdjustment {
        let cache_adjustment = self.adjust_based_on_cache_misses(metrics);
        let block_adjustment = self.adjust_based_on_block_time(metrics);
        
        // 综合考虑两种策略
        match (cache_adjustment.clone(), block_adjustment.clone()) {
            (BatchAdjustment::Decrease(_), BatchAdjustment::Decrease(_)) => {
                // 两种策略都建议减少，取更大的减少量
                let cache_decrease = if let BatchAdjustment::Decrease(d) = cache_adjustment { d } else { 0 };
                let block_decrease = if let BatchAdjustment::Decrease(d) = block_adjustment { d } else { 0 };
                BatchAdjustment::Decrease(cache_decrease.max(block_decrease))
            },
            (BatchAdjustment::Increase(_), BatchAdjustment::Increase(_)) => {
                // 两种策略都建议增加，取更小的增加量（保守）
                let cache_increase = if let BatchAdjustment::Increase(d) = cache_adjustment { d } else { 0 };
                let block_increase = if let BatchAdjustment::Increase(d) = block_adjustment { d } else { 0 };
                BatchAdjustment::Increase(cache_increase.min(block_increase))
            },
            (BatchAdjustment::Decrease(d), _) | (_, BatchAdjustment::Decrease(d)) => {
                // 任一策略建议减少，优先减少
                BatchAdjustment::Decrease(d)
            },
            (BatchAdjustment::Increase(d), _) | (_, BatchAdjustment::Increase(d)) => {
                // 任一策略建议增加，保守增加
                BatchAdjustment::Increase(d / 2)
            },
            _ => BatchAdjustment::Keep,
        }
    }
    
    /// 机器学习策略调整
    fn adjust_with_ml(&self, metrics: &PerformanceSnapshot) -> BatchAdjustment {
        // 基于机器学习的批处理大小调整策略
        if self.performance_history.len() < 10 {
            return BatchAdjustment::Keep;
        }
        
        // 准备特征向量
        let features = self.extract_features(metrics);
        
        // 使用线性回归模型预测最佳批处理大小
        let predicted_batch_size = self.predict_optimal_batch_size(&features);
        let current_batch_size = self.current_batch_size.load(Ordering::Relaxed);
        
        // 计算调整比例
        let adjustment_ratio = predicted_batch_size as f64 / current_batch_size as f64;
        
        if adjustment_ratio > 1.2 {
            let increase = ((predicted_batch_size - current_batch_size) as f64 * 0.8) as usize;
            BatchAdjustment::Increase(increase)
        } else if adjustment_ratio < 0.8 {
            let decrease = ((current_batch_size - predicted_batch_size) as f64 * 0.8) as usize;
            BatchAdjustment::Decrease(decrease)
        } else {
            BatchAdjustment::Keep
        }
    }
    
    /// 提取特征向量
    fn extract_features(&self, metrics: &PerformanceSnapshot) -> Vec<f64> {
        let mut features = Vec::new();
        
        // 基础性能指标
        features.push(metrics.throughput);
        features.push(metrics.l2_miss_rate);
        features.push(metrics.l3_miss_rate);
        features.push(metrics.operator_blocking_time.as_millis() as f64);
        features.push(metrics.memory_usage as f64);
        
        // 历史趋势特征
        if self.performance_history.len() >= 5 {
            let recent_metrics = &self.performance_history[self.performance_history.len()-5..];
            let avg_throughput: f64 = recent_metrics.iter().map(|m| m.throughput).sum::<f64>() / recent_metrics.len() as f64;
            let throughput_trend = (metrics.throughput - avg_throughput) / avg_throughput;
            features.push(throughput_trend);
            
            let avg_l2_miss: f64 = recent_metrics.iter().map(|m| m.l2_miss_rate).sum::<f64>() / recent_metrics.len() as f64;
            let l2_trend = (metrics.l2_miss_rate - avg_l2_miss) / avg_l2_miss.max(0.001);
            features.push(l2_trend);
        } else {
            features.push(0.0); // 默认趋势
            features.push(0.0);
        }
        
        // 系统负载特征
        features.push(self.current_batch_size.load(Ordering::Relaxed) as f64);
        features.push(metrics.cpu_usage);
        features.push(metrics.memory_pressure);
        
        features
    }
    
    /// 预测最佳批处理大小
    fn predict_optimal_batch_size(&self, features: &[f64]) -> usize {
        // 简化的线性回归模型
        // 实际实现中应该使用训练好的模型
        let weights = vec![
            0.1,   // throughput
            -0.05, // l2_miss_rate
            -0.03, // l3_miss_rate
            -0.02, // operator_blocking_time
            -0.01, // memory_usage
            0.15,  // throughput_trend
            -0.08, // l2_trend
            0.2,   // current_batch_size
            -0.1,  // cpu_usage
            -0.05, // memory_pressure
        ];
        
        let bias = 1000.0; // 基础批处理大小
        
        let prediction = bias + features.iter()
            .zip(weights.iter())
            .map(|(f, w)| f * w)
            .sum::<f64>();
        
        // 限制在合理范围内
        prediction.max(100.0).min(10000.0) as usize
    }
    
    /// 应用调整
    fn apply_adjustment(&self, adjustment: &BatchAdjustment) {
        match adjustment {
            BatchAdjustment::Increase(amount) => {
                let current = self.current_batch_size.load(Ordering::Relaxed);
                let new_size = (current + amount).min(self.max_batch_size);
                self.current_batch_size.store(new_size, Ordering::Relaxed);
            },
            BatchAdjustment::Decrease(amount) => {
                let current = self.current_batch_size.load(Ordering::Relaxed);
                let new_size = current.saturating_sub(*amount).max(self.min_batch_size);
                self.current_batch_size.store(new_size, Ordering::Relaxed);
            },
            BatchAdjustment::Keep => {},
        }
    }
    
    /// 重置性能指标
    pub fn reset_metrics(&self) {
        self.metrics_collector.l2_misses.store(0, Ordering::Relaxed);
        self.metrics_collector.l3_misses.store(0, Ordering::Relaxed);
        self.metrics_collector.operator_block_time.store(0, Ordering::Relaxed);
        self.metrics_collector.processing_time.store(0, Ordering::Relaxed);
        self.metrics_collector.batch_count.store(0, Ordering::Relaxed);
    }
    
    /// 获取性能统计
    pub fn get_performance_stats(&self) -> PerformanceStats {
        let batch_count = self.metrics_collector.batch_count.load(Ordering::Relaxed);
        let l2_misses = self.metrics_collector.l2_misses.load(Ordering::Relaxed);
        let l3_misses = self.metrics_collector.l3_misses.load(Ordering::Relaxed);
        let block_time = self.metrics_collector.operator_block_time.load(Ordering::Relaxed);
        let processing_time = self.metrics_collector.processing_time.load(Ordering::Relaxed);
        
        PerformanceStats {
            total_batches: batch_count,
            l2_miss_rate: if batch_count > 0 { l2_misses as f64 / batch_count as f64 } else { 0.0 },
            l3_miss_rate: if batch_count > 0 { l3_misses as f64 / batch_count as f64 } else { 0.0 },
            avg_block_time: if batch_count > 0 { 
                Duration::from_micros(block_time / batch_count) 
            } else { 
                Duration::from_micros(0) 
            },
            avg_processing_time: if batch_count > 0 { 
                Duration::from_micros(processing_time / batch_count) 
            } else { 
                Duration::from_micros(0) 
            },
            current_batch_size: self.get_current_batch_size(),
        }
    }
}

impl PerformanceMetricsCollector {
    /// 创建新的性能指标收集器
    pub fn new() -> Self {
        Self {
            l2_misses: AtomicU64::new(0),
            l3_misses: AtomicU64::new(0),
            operator_block_time: AtomicU64::new(0),
            processing_time: AtomicU64::new(0),
            batch_count: AtomicU64::new(0),
        }
    }
}

/// 性能统计信息
#[derive(Debug, Clone)]
pub struct PerformanceStats {
    /// 总批次数
    pub total_batches: u64,
    /// L2未命中率
    pub l2_miss_rate: f64,
    /// L3未命中率
    pub l3_miss_rate: f64,
    /// 平均堵塞时间
    pub avg_block_time: Duration,
    /// 平均处理时间
    pub avg_processing_time: Duration,
    /// 当前批量大小
    pub current_batch_size: usize,
}

impl Default for AdaptiveBatchingManager {
    fn default() -> Self {
        Self::new(1000, 100, 10000, AdaptationStrategy::Hybrid)
    }
}

impl Default for PerformanceMetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}
