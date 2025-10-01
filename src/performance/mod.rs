//! Performance monitoring and metrics collection module
//! 
//! This module provides comprehensive performance monitoring capabilities
//! for the OneEngine MPP execution engine, including:
//! 
//! - Real-time performance metrics collection
//! - Memory usage tracking
//! - CPU utilization monitoring
//! - Network I/O statistics
//! - Query execution profiling
//! - Performance regression detection

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

// Declare the benchmark_runner module
mod benchmark_runner;

// Re-export benchmark runner types
pub use benchmark_runner::*;

/// Performance metrics collector
#[derive(Debug, Clone)]
pub struct PerformanceCollector {
    metrics: Arc<RwLock<HashMap<String, PerformanceMetric>>>,
    start_time: Instant,
}

/// Individual performance metric
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceMetric {
    pub name: String,
    pub value: f64,
    pub unit: String,
    pub timestamp: u64,
    pub tags: HashMap<String, String>,
}

/// Performance statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceStats {
    pub total_queries: u64,
    pub avg_execution_time: Duration,
    pub max_execution_time: Duration,
    pub min_execution_time: Duration,
    pub total_memory_used: u64,
    pub peak_memory_used: u64,
    pub cpu_utilization: f64,
    pub network_throughput: f64,
    pub cache_hit_rate: f64,
}

/// Query execution profile
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryProfile {
    pub query_id: String,
    pub execution_time: Duration,
    pub memory_usage: u64,
    pub cpu_time: Duration,
    pub io_time: Duration,
    pub operator_stats: Vec<OperatorProfile>,
    pub stage_stats: Vec<StageProfile>,
}

/// Operator execution profile
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperatorProfile {
    pub operator_id: String,
    pub operator_type: String,
    pub execution_time: Duration,
    pub memory_usage: u64,
    pub input_rows: u64,
    pub output_rows: u64,
    pub cpu_utilization: f64,
}

/// Stage execution profile
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageProfile {
    pub stage_id: String,
    pub execution_time: Duration,
    pub memory_usage: u64,
    pub parallelism: usize,
    pub operator_count: usize,
}

/// Memory usage tracker
#[derive(Debug, Clone)]
pub struct MemoryTracker {
    current_usage: Arc<RwLock<u64>>,
    peak_usage: Arc<RwLock<u64>>,
    allocations: Arc<RwLock<u64>>,
    deallocations: Arc<RwLock<u64>>,
}

/// CPU utilization monitor
#[derive(Debug, Clone)]
pub struct CpuMonitor {
    start_time: Instant,
    start_cpu_time: Duration,
    last_measurement: Instant,
    last_cpu_time: Duration,
}

/// Network I/O statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkStats {
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub packets_sent: u64,
    pub packets_received: u64,
    pub avg_latency: Duration,
    pub max_latency: Duration,
    pub min_latency: Duration,
}

/// Performance regression detector
#[derive(Debug, Clone)]
pub struct RegressionDetector {
    baseline_metrics: Arc<RwLock<HashMap<String, f64>>>,
    current_metrics: Arc<RwLock<HashMap<String, f64>>>,
    threshold: f64,
}

impl PerformanceCollector {
    /// Create a new performance collector
    pub fn new() -> Self {
        Self {
            metrics: Arc::new(RwLock::new(HashMap::new())),
            start_time: Instant::now(),
        }
    }

    /// Record a performance metric
    pub async fn record_metric(&self, name: String, value: f64, unit: String, tags: HashMap<String, String>) {
        let metric = PerformanceMetric {
            name: name.clone(),
            value,
            unit,
            timestamp: self.start_time.elapsed().as_millis() as u64,
            tags,
        };

        let mut metrics = self.metrics.write().await;
        metrics.insert(name, metric);
    }

    /// Get all metrics
    pub async fn get_metrics(&self) -> HashMap<String, PerformanceMetric> {
        self.metrics.read().await.clone()
    }

    /// Get performance statistics
    pub async fn get_stats(&self) -> PerformanceStats {
        let metrics = self.metrics.read().await;
        
        // Calculate statistics from collected metrics
        let mut total_queries = 0;
        let mut total_execution_time = Duration::ZERO;
        let mut max_execution_time = Duration::ZERO;
        let mut min_execution_time = Duration::MAX;
        let mut total_memory = 0;
        let mut peak_memory = 0;
        let mut total_cpu = 0.0;
        let mut total_network = 0.0;
        let mut total_cache_hits = 0.0;
        let mut total_cache_requests = 0.0;

        for metric in metrics.values() {
            match metric.name.as_str() {
                "query_count" => total_queries = metric.value as u64,
                "execution_time" => {
                    let duration = Duration::from_millis(metric.value as u64);
                    total_execution_time += duration;
                    if duration > max_execution_time {
                        max_execution_time = duration;
                    }
                    if duration < min_execution_time {
                        min_execution_time = duration;
                    }
                },
                "memory_usage" => {
                    total_memory += metric.value as u64;
                    if metric.value as u64 > peak_memory {
                        peak_memory = metric.value as u64;
                    }
                },
                "cpu_utilization" => total_cpu += metric.value,
                "network_throughput" => total_network += metric.value,
                "cache_hits" => total_cache_hits += metric.value,
                "cache_requests" => total_cache_requests += metric.value,
                _ => {}
            }
        }

        let avg_execution_time = if total_queries > 0 {
            total_execution_time / total_queries as u32
        } else {
            Duration::ZERO
        };

        let cpu_utilization = if total_queries > 0 {
            total_cpu / total_queries as f64
        } else {
            0.0
        };

        let network_throughput = if total_queries > 0 {
            total_network / total_queries as f64
        } else {
            0.0
        };

        let cache_hit_rate = if total_cache_requests > 0.0 {
            total_cache_hits / total_cache_requests
        } else {
            0.0
        };

        PerformanceStats {
            total_queries,
            avg_execution_time,
            max_execution_time,
            min_execution_time,
            total_memory_used: total_memory,
            peak_memory_used: peak_memory,
            cpu_utilization,
            network_throughput,
            cache_hit_rate,
        }
    }

    /// Clear all metrics
    pub async fn clear_metrics(&self) {
        let mut metrics = self.metrics.write().await;
        metrics.clear();
    }
}

impl MemoryTracker {
    /// Create a new memory tracker
    pub fn new() -> Self {
        Self {
            current_usage: Arc::new(RwLock::new(0)),
            peak_usage: Arc::new(RwLock::new(0)),
            allocations: Arc::new(RwLock::new(0)),
            deallocations: Arc::new(RwLock::new(0)),
        }
    }

    /// Record memory allocation
    pub async fn record_allocation(&self, size: u64) {
        let mut current = self.current_usage.write().await;
        let mut peak = self.peak_usage.write().await;
        let mut allocs = self.allocations.write().await;

        *current += size;
        if *current > *peak {
            *peak = *current;
        }
        *allocs += 1;
    }

    /// Record memory deallocation
    pub async fn record_deallocation(&self, size: u64) {
        let mut current = self.current_usage.write().await;
        let mut deallocs = self.deallocations.write().await;

        if *current >= size {
            *current -= size;
        }
        *deallocs += 1;
    }

    /// Get current memory usage
    pub async fn get_current_usage(&self) -> u64 {
        *self.current_usage.read().await
    }

    /// Get peak memory usage
    pub async fn get_peak_usage(&self) -> u64 {
        *self.peak_usage.read().await
    }

    /// Get allocation statistics
    pub async fn get_allocation_stats(&self) -> (u64, u64) {
        let allocs = *self.allocations.read().await;
        let deallocs = *self.deallocations.read().await;
        (allocs, deallocs)
    }
}

impl CpuMonitor {
    /// Create a new CPU monitor
    pub fn new() -> Self {
        Self {
            start_time: Instant::now(),
            start_cpu_time: Duration::ZERO, // Would need system-specific implementation
            last_measurement: Instant::now(),
            last_cpu_time: Duration::ZERO,
        }
    }

    /// Get current CPU utilization
    pub fn get_cpu_utilization(&mut self) -> f64 {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_measurement);
        
        // This is a simplified implementation
        // In a real implementation, you would use system APIs to get actual CPU time
        let cpu_time = Duration::ZERO; // Placeholder
        
        let cpu_utilization = if elapsed.as_secs_f64() > 0.0 {
            cpu_time.as_secs_f64() / elapsed.as_secs_f64()
        } else {
            0.0
        };

        self.last_measurement = now;
        self.last_cpu_time = cpu_time;

        cpu_utilization
    }
}

impl NetworkStats {
    /// Create new network statistics
    pub fn new() -> Self {
        Self {
            bytes_sent: 0,
            bytes_received: 0,
            packets_sent: 0,
            packets_received: 0,
            avg_latency: Duration::ZERO,
            max_latency: Duration::ZERO,
            min_latency: Duration::MAX,
        }
    }

    /// Record sent bytes
    pub fn record_sent(&mut self, bytes: u64) {
        self.bytes_sent += bytes;
        self.packets_sent += 1;
    }

    /// Record received bytes
    pub fn record_received(&mut self, bytes: u64) {
        self.bytes_received += bytes;
        self.packets_received += 1;
    }

    /// Record latency measurement
    pub fn record_latency(&mut self, latency: Duration) {
        if latency > self.max_latency {
            self.max_latency = latency;
        }
        if latency < self.min_latency {
            self.min_latency = latency;
        }
        
        // Update average latency (simplified)
        let total_packets = self.packets_sent + self.packets_received;
        if total_packets > 0 {
            self.avg_latency = Duration::from_millis(
                (self.avg_latency.as_millis() as u64 * (total_packets - 1) + latency.as_millis() as u64) / total_packets
            );
        }
    }
}

impl RegressionDetector {
    /// Create a new regression detector
    pub fn new(threshold: f64) -> Self {
        Self {
            baseline_metrics: Arc::new(RwLock::new(HashMap::new())),
            current_metrics: Arc::new(RwLock::new(HashMap::new())),
            threshold,
        }
    }

    /// Set baseline metrics
    pub async fn set_baseline(&self, metrics: HashMap<String, f64>) {
        let mut baseline = self.baseline_metrics.write().await;
        *baseline = metrics;
    }

    /// Update current metrics
    pub async fn update_current(&self, metrics: HashMap<String, f64>) {
        let mut current = self.current_metrics.write().await;
        *current = metrics;
    }

    /// Detect performance regressions
    pub async fn detect_regressions(&self) -> Vec<String> {
        let baseline = self.baseline_metrics.read().await;
        let current = self.current_metrics.read().await;
        let mut regressions = Vec::new();

        for (metric_name, baseline_value) in baseline.iter() {
            if let Some(current_value) = current.get(metric_name) {
                let improvement = (current_value - baseline_value) / baseline_value;
                if improvement > self.threshold {
                    regressions.push(format!(
                        "Performance regression detected in {}: {:.2}% slower",
                        metric_name,
                        improvement * 100.0
                    ));
                }
            }
        }

        regressions
    }
}

impl Default for PerformanceCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl Default for MemoryTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl Default for CpuMonitor {
    fn default() -> Self {
        Self::new()
    }
}

impl Default for NetworkStats {
    fn default() -> Self {
        Self::new()
    }
}
