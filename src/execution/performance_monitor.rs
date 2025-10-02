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

//! 性能监控和指标收集
//! 
//! 提供全面的性能监控、指标收集和性能分析功能

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info, warn, error};

/// 性能指标类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MetricType {
    /// 执行时间
    ExecutionTime,
    /// 内存使用
    MemoryUsage,
    /// CPU使用率
    CpuUsage,
    /// 吞吐量
    Throughput,
    /// 延迟
    Latency,
    /// 错误率
    ErrorRate,
    /// 队列长度
    QueueLength,
    /// 缓存命中率
    CacheHitRate,
    /// 网络I/O
    NetworkIO,
    /// 磁盘I/O
    DiskIO,
}

/// 性能指标
#[derive(Debug, Clone)]
pub struct PerformanceMetric {
    /// 指标类型
    pub metric_type: MetricType,
    /// 指标名称
    pub name: String,
    /// 指标值
    pub value: f64,
    /// 时间戳
    pub timestamp: Instant,
    /// 标签
    pub tags: HashMap<String, String>,
}

/// 性能统计信息
#[derive(Debug, Default)]
pub struct PerformanceStats {
    /// 总执行时间
    pub total_execution_time: AtomicU64,
    /// 平均执行时间
    pub avg_execution_time: AtomicU64,
    /// 最大执行时间
    pub max_execution_time: AtomicU64,
    /// 最小执行时间
    pub min_execution_time: AtomicU64,
    /// 执行次数
    pub execution_count: AtomicU64,
    /// 错误次数
    pub error_count: AtomicU64,
    /// 内存使用峰值
    pub peak_memory_usage: AtomicU64,
    /// 当前内存使用
    pub current_memory_usage: AtomicU64,
    /// 吞吐量（每秒处理的项目数）
    pub throughput: AtomicU64,
    /// 平均延迟
    pub avg_latency: AtomicU64,
    /// 最大延迟
    pub max_latency: AtomicU64,
}

/// 算子性能统计
#[derive(Debug, Clone, Default)]
pub struct OperatorStats {
    /// 算子ID
    pub operator_id: u32,
    /// 算子名称
    pub operator_name: String,
    /// 性能统计
    pub stats: PerformanceStats,
    /// 子算子统计
    pub child_stats: Vec<OperatorStats>,
}

/// 查询性能统计
#[derive(Debug, Clone, Default)]
pub struct QueryStats {
    /// 查询ID
    pub query_id: String,
    /// 总执行时间
    pub total_time: Duration,
    /// 算子统计
    pub operator_stats: Vec<OperatorStats>,
    /// 内存使用峰值
    pub peak_memory: u64,
    /// 处理的行数
    pub rows_processed: u64,
    /// 处理的批次数量
    pub batches_processed: u64,
    /// 错误信息
    pub errors: Vec<String>,
}

/// 性能监控器
pub struct PerformanceMonitor {
    /// 指标收集器
    metrics: Arc<RwLock<Vec<PerformanceMetric>>>,
    /// 统计信息
    stats: Arc<RwLock<HashMap<String, PerformanceStats>>>,
    /// 查询统计
    query_stats: Arc<RwLock<HashMap<String, QueryStats>>>,
    /// 监控配置
    config: MonitorConfig,
    /// 是否启用监控
    enabled: bool,
}

/// 监控配置
#[derive(Debug, Clone)]
pub struct MonitorConfig {
    /// 指标保留时间
    pub retention_duration: Duration,
    /// 采样率（0.0-1.0）
    pub sampling_rate: f64,
    /// 是否启用详细监控
    pub detailed_monitoring: bool,
    /// 指标收集间隔
    pub collection_interval: Duration,
    /// 最大指标数量
    pub max_metrics: usize,
}

impl Default for MonitorConfig {
    fn default() -> Self {
        Self {
            retention_duration: Duration::from_secs(3600), // 1小时
            sampling_rate: 1.0,
            detailed_monitoring: true,
            collection_interval: Duration::from_millis(100),
            max_metrics: 10000,
        }
    }
}

impl PerformanceMonitor {
    /// 创建新的性能监控器
    pub fn new(config: MonitorConfig) -> Self {
        Self {
            metrics: Arc::new(RwLock::new(Vec::new())),
            stats: Arc::new(RwLock::new(HashMap::new())),
            query_stats: Arc::new(RwLock::new(HashMap::new())),
            config,
            enabled: true,
        }
    }

    /// 记录性能指标
    pub async fn record_metric(&self, metric: PerformanceMetric) {
        if !self.enabled {
            return;
        }

        // 采样率检查
        if rand::random::<f64>() > self.config.sampling_rate {
            return;
        }

        let mut metrics = self.metrics.write().await;
        
        // 检查指标数量限制
        if metrics.len() >= self.config.max_metrics {
            // 移除最旧的指标
            let remove_count = metrics.len() / 2;
            metrics.drain(0..remove_count);
        }
        
        metrics.push(metric);
    }

    /// 记录执行时间
    pub async fn record_execution_time(&self, name: &str, duration: Duration, tags: Option<HashMap<String, String>>) {
        let metric = PerformanceMetric {
            metric_type: MetricType::ExecutionTime,
            name: name.to_string(),
            value: duration.as_micros() as f64,
            timestamp: Instant::now(),
            tags: tags.unwrap_or_default(),
        };
        
        self.record_metric(metric).await;
        self.update_stats(name, |stats| {
            let micros = duration.as_micros() as u64;
            stats.total_execution_time.fetch_add(micros, Ordering::Relaxed);
            stats.execution_count.fetch_add(1, Ordering::Relaxed);
            
            // 更新平均时间
            let count = stats.execution_count.load(Ordering::Relaxed);
            let total = stats.total_execution_time.load(Ordering::Relaxed);
            stats.avg_execution_time.store(total / count, Ordering::Relaxed);
            
            // 更新最大和最小时间
            let current_max = stats.max_execution_time.load(Ordering::Relaxed);
            if micros > current_max {
                stats.max_execution_time.store(micros, Ordering::Relaxed);
            }
            
            let current_min = stats.min_execution_time.load(Ordering::Relaxed);
            if current_min == 0 || micros < current_min {
                stats.min_execution_time.store(micros, Ordering::Relaxed);
            }
        }).await;
    }

    /// 记录内存使用
    pub async fn record_memory_usage(&self, name: &str, usage: u64, tags: Option<HashMap<String, String>>) {
        let metric = PerformanceMetric {
            metric_type: MetricType::MemoryUsage,
            name: name.to_string(),
            value: usage as f64,
            timestamp: Instant::now(),
            tags: tags.unwrap_or_default(),
        };
        
        self.record_metric(metric).await;
        self.update_stats(name, |stats| {
            stats.current_memory_usage.store(usage, Ordering::Relaxed);
            
            let current_peak = stats.peak_memory_usage.load(Ordering::Relaxed);
            if usage > current_peak {
                stats.peak_memory_usage.store(usage, Ordering::Relaxed);
            }
        }).await;
    }

    /// 记录错误
    pub async fn record_error(&self, name: &str, error: &str) {
        self.update_stats(name, |stats| {
            stats.error_count.fetch_add(1, Ordering::Relaxed);
        }).await;
        
        debug!("Recorded error for {}: {}", name, error);
    }

    /// 记录吞吐量
    pub async fn record_throughput(&self, name: &str, items_per_second: u64) {
        let metric = PerformanceMetric {
            metric_type: MetricType::Throughput,
            name: name.to_string(),
            value: items_per_second as f64,
            timestamp: Instant::now(),
            tags: HashMap::new(),
        };
        
        self.record_metric(metric).await;
        self.update_stats(name, |stats| {
            stats.throughput.store(items_per_second, Ordering::Relaxed);
        }).await;
    }

    /// 记录延迟
    pub async fn record_latency(&self, name: &str, latency: Duration, tags: Option<HashMap<String, String>>) {
        let metric = PerformanceMetric {
            metric_type: MetricType::Latency,
            name: name.to_string(),
            value: latency.as_micros() as f64,
            timestamp: Instant::now(),
            tags: tags.unwrap_or_default(),
        };
        
        self.record_metric(metric).await;
        self.update_stats(name, |stats| {
            let micros = latency.as_micros() as u64;
            stats.avg_latency.store(micros, Ordering::Relaxed);
            
            let current_max = stats.max_latency.load(Ordering::Relaxed);
            if micros > current_max {
                stats.max_latency.store(micros, Ordering::Relaxed);
            }
        }).await;
    }

    /// 更新统计信息
    async fn update_stats<F>(&self, name: &str, updater: F)
    where
        F: FnOnce(&mut PerformanceStats),
    {
        let mut stats = self.stats.write().await;
        let entry = stats.entry(name.to_string()).or_insert_with(PerformanceStats::default);
        updater(entry);
    }

    /// 获取统计信息
    pub async fn get_stats(&self, name: &str) -> Option<PerformanceStats> {
        let stats = self.stats.read().await;
        stats.get(name).cloned()
    }

    /// 获取所有统计信息
    pub async fn get_all_stats(&self) -> HashMap<String, PerformanceStats> {
        self.stats.read().await.clone()
    }

    /// 获取指标
    pub async fn get_metrics(&self, metric_type: Option<MetricType>, limit: Option<usize>) -> Vec<PerformanceMetric> {
        let metrics = self.metrics.read().await;
        
        let filtered: Vec<PerformanceMetric> = if let Some(mt) = metric_type {
            metrics.iter()
                .filter(|m| m.metric_type == mt)
                .cloned()
                .collect()
        } else {
            metrics.clone()
        };
        
        if let Some(limit) = limit {
            filtered.into_iter().take(limit).collect()
        } else {
            filtered
        }
    }

    /// 清理过期指标
    pub async fn cleanup_expired_metrics(&self) {
        let cutoff = Instant::now() - self.config.retention_duration;
        let mut metrics = self.metrics.write().await;
        metrics.retain(|m| m.timestamp > cutoff);
        
        debug!("Cleaned up expired metrics, {} remaining", metrics.len());
    }

    /// 生成性能报告
    pub async fn generate_report(&self) -> PerformanceReport {
        let stats = self.get_all_stats().await;
        let metrics = self.get_metrics(None, None).await;
        
        let mut report = PerformanceReport {
            timestamp: Instant::now(),
            total_metrics: metrics.len(),
            stats_count: stats.len(),
            operator_stats: Vec::new(),
            summary: PerformanceSummary::default(),
        };
        
        // 计算汇总统计
        for (name, stat) in &stats {
            let operator_stat = OperatorStats {
                operator_id: 0, // 需要从名称中提取
                operator_name: name.clone(),
                stats: stat.clone(),
                child_stats: Vec::new(),
            };
            report.operator_stats.push(operator_stat);
            
            // 更新汇总统计
            report.summary.total_execution_time += stat.total_execution_time.load(Ordering::Relaxed);
            report.summary.total_executions += stat.execution_count.load(Ordering::Relaxed);
            report.summary.total_errors += stat.error_count.load(Ordering::Relaxed);
            report.summary.peak_memory_usage = report.summary.peak_memory_usage.max(
                stat.peak_memory_usage.load(Ordering::Relaxed)
            );
        }
        
        if report.summary.total_executions > 0 {
            report.summary.avg_execution_time = report.summary.total_execution_time / report.summary.total_executions;
            report.summary.error_rate = report.summary.total_errors as f64 / report.summary.total_executions as f64;
        }
        
        report
    }

    /// 启用/禁用监控
    pub fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
        info!("Performance monitoring {}", if enabled { "enabled" } else { "disabled" });
    }

    /// 重置统计信息
    pub async fn reset_stats(&self) {
        let mut stats = self.stats.write().await;
        stats.clear();
        
        let mut metrics = self.metrics.write().await;
        metrics.clear();
        
        info!("Performance statistics reset");
    }
}

/// 性能报告
#[derive(Debug, Clone)]
pub struct PerformanceReport {
    /// 报告时间戳
    pub timestamp: Instant,
    /// 总指标数量
    pub total_metrics: usize,
    /// 统计信息数量
    pub stats_count: usize,
    /// 算子统计
    pub operator_stats: Vec<OperatorStats>,
    /// 汇总统计
    pub summary: PerformanceSummary,
}

/// 性能汇总
#[derive(Debug, Clone, Default)]
pub struct PerformanceSummary {
    /// 总执行时间（微秒）
    pub total_execution_time: u64,
    /// 总执行次数
    pub total_executions: u64,
    /// 平均执行时间（微秒）
    pub avg_execution_time: u64,
    /// 总错误次数
    pub total_errors: u64,
    /// 错误率
    pub error_rate: f64,
    /// 内存使用峰值（字节）
    pub peak_memory_usage: u64,
}

/// 性能监控器工厂
pub struct PerformanceMonitorFactory;

impl Clone for PerformanceStats {
    fn clone(&self) -> Self {
        Self {
            total_execution_time: AtomicU64::new(self.total_execution_time.load(Ordering::Relaxed)),
            avg_execution_time: AtomicU64::new(self.avg_execution_time.load(Ordering::Relaxed)),
            max_execution_time: AtomicU64::new(self.max_execution_time.load(Ordering::Relaxed)),
            min_execution_time: AtomicU64::new(self.min_execution_time.load(Ordering::Relaxed)),
            execution_count: AtomicU64::new(self.execution_count.load(Ordering::Relaxed)),
            error_count: AtomicU64::new(self.error_count.load(Ordering::Relaxed)),
            peak_memory_usage: AtomicU64::new(self.peak_memory_usage.load(Ordering::Relaxed)),
            current_memory_usage: AtomicU64::new(self.current_memory_usage.load(Ordering::Relaxed)),
            throughput: AtomicU64::new(self.throughput.load(Ordering::Relaxed)),
            avg_latency: AtomicU64::new(self.avg_latency.load(Ordering::Relaxed)),
            max_latency: AtomicU64::new(self.max_latency.load(Ordering::Relaxed)),
        }
    }
}

impl PerformanceMonitorFactory {
    /// 创建默认监控器
    pub fn create_default() -> PerformanceMonitor {
        PerformanceMonitor::new(MonitorConfig::default())
    }
    
    /// 创建高性能监控器
    pub fn create_high_performance() -> PerformanceMonitor {
        let config = MonitorConfig {
            retention_duration: Duration::from_secs(1800), // 30分钟
            sampling_rate: 0.1, // 10%采样
            detailed_monitoring: false,
            collection_interval: Duration::from_millis(50),
            max_metrics: 5000,
        };
        PerformanceMonitor::new(config)
    }
    
    /// 创建详细监控器
    pub fn create_detailed() -> PerformanceMonitor {
        let config = MonitorConfig {
            retention_duration: Duration::from_secs(7200), // 2小时
            sampling_rate: 1.0, // 100%采样
            detailed_monitoring: true,
            collection_interval: Duration::from_millis(10),
            max_metrics: 50000,
        };
        PerformanceMonitor::new(config)
    }
}
