//! Benchmark runner for performance testing
//! 
//! This module provides utilities for running performance benchmarks
//! and collecting detailed performance metrics.

use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;
use serde::{Serialize, Deserialize};

use crate::performance::{PerformanceCollector, PerformanceStats, QueryProfile, OperatorProfile, StageProfile};

/// Benchmark configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkConfig {
    pub warmup_iterations: usize,
    pub measurement_iterations: usize,
    pub timeout: Duration,
    pub memory_limit: Option<u64>,
    pub cpu_limit: Option<f64>,
}

impl Default for BenchmarkConfig {
    fn default() -> Self {
        Self {
            warmup_iterations: 3,
            measurement_iterations: 10,
            timeout: Duration::from_secs(300), // 5 minutes
            memory_limit: None,
            cpu_limit: None,
        }
    }
}

/// Benchmark result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkResult {
    pub name: String,
    pub iterations: usize,
    pub total_time: Duration,
    pub avg_time: Duration,
    pub min_time: Duration,
    pub max_time: Duration,
    pub std_dev: Duration,
    pub throughput: f64,
    pub memory_usage: u64,
    pub cpu_utilization: f64,
    pub error_count: usize,
}

/// Benchmark suite
#[derive(Debug, Clone)]
pub struct BenchmarkSuite {
    config: BenchmarkConfig,
    collector: PerformanceCollector,
    results: Vec<BenchmarkResult>,
}

impl BenchmarkSuite {
    /// Create a new benchmark suite
    pub fn new(config: BenchmarkConfig) -> Self {
        Self {
            config,
            collector: PerformanceCollector::new(),
            results: Vec::new(),
        }
    }

    /// Run a single benchmark
    pub async fn run_benchmark<F, Fut, R>(
        &mut self,
        name: String,
        benchmark_fn: F,
    ) -> Result<BenchmarkResult, Box<dyn std::error::Error + Send + Sync>>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<R, Box<dyn std::error::Error + Send + Sync>>>,
    {
        let start_time = Instant::now();
        let mut times = Vec::new();
        let mut error_count = 0;

        // Warmup iterations
        for _ in 0..self.config.warmup_iterations {
            let _ = benchmark_fn().await;
        }

        // Measurement iterations
        for _ in 0..self.config.measurement_iterations {
            let iteration_start = Instant::now();
            
            match benchmark_fn().await {
                Ok(_) => {
                    let iteration_time = iteration_start.elapsed();
                    times.push(iteration_time);
                }
                Err(_) => {
                    error_count += 1;
                }
            }
        }

        let total_time = start_time.elapsed();
        let avg_time = if !times.is_empty() {
            times.iter().sum::<Duration>() / times.len() as u32
        } else {
            Duration::ZERO
        };

        let min_time = times.iter().min().copied().unwrap_or(Duration::ZERO);
        let max_time = times.iter().max().copied().unwrap_or(Duration::ZERO);

        // Calculate standard deviation
        let variance = if times.len() > 1 {
            let mean = avg_time.as_nanos() as f64;
            let sum_squared_diff = times.iter()
                .map(|&t| {
                    let diff = t.as_nanos() as f64 - mean;
                    diff * diff
                })
                .sum::<f64>();
            sum_squared_diff / (times.len() - 1) as f64
        } else {
            0.0
        };
        let std_dev = Duration::from_nanos(variance.sqrt() as u64);

        // Calculate throughput (operations per second)
        let throughput = if avg_time.as_secs_f64() > 0.0 {
            1.0 / avg_time.as_secs_f64()
        } else {
            0.0
        };

        // Get performance metrics
        let stats = self.collector.get_stats().await;
        let memory_usage = stats.peak_memory_used;
        let cpu_utilization = stats.cpu_utilization;

        let result = BenchmarkResult {
            name,
            iterations: times.len(),
            total_time,
            avg_time,
            min_time,
            max_time,
            std_dev,
            throughput,
            memory_usage,
            cpu_utilization,
            error_count,
        };

        self.results.push(result.clone());
        Ok(result)
    }

    /// Run multiple benchmarks in parallel
    pub async fn run_parallel_benchmarks<F, Fut, R>(
        &mut self,
        benchmarks: Vec<(String, F)>,
    ) -> Result<Vec<BenchmarkResult>, Box<dyn std::error::Error + Send + Sync>>
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<R, Box<dyn std::error::Error + Send + Sync>>> + Send,
        R: Send,
    {
        let mut handles = Vec::new();
        let rt = Runtime::new()?;

        for (name, benchmark_fn) in benchmarks {
            let collector = self.collector.clone();
            let config = self.config.clone();
            
            let handle = rt.spawn(async move {
                Self::run_single_benchmark(name, benchmark_fn, config, collector).await
            });
            handles.push(handle);
        }

        let mut results = Vec::new();
        for handle in handles {
            let result = handle.await??;
            results.push(result);
        }

        Ok(results)
    }

    /// Run a single benchmark (internal helper)
    async fn run_single_benchmark<F, Fut, R>(
        name: String,
        benchmark_fn: F,
        config: BenchmarkConfig,
        collector: PerformanceCollector,
    ) -> Result<BenchmarkResult, Box<dyn std::error::Error + Send + Sync>>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<R, Box<dyn std::error::Error + Send + Sync>>>,
    {
        let start_time = Instant::now();
        let mut times = Vec::new();
        let mut error_count = 0;

        // Warmup iterations
        for _ in 0..config.warmup_iterations {
            let _ = benchmark_fn().await;
        }

        // Measurement iterations
        for _ in 0..config.measurement_iterations {
            let iteration_start = Instant::now();
            
            match benchmark_fn().await {
                Ok(_) => {
                    let iteration_time = iteration_start.elapsed();
                    times.push(iteration_time);
                }
                Err(_) => {
                    error_count += 1;
                }
            }
        }

        let total_time = start_time.elapsed();
        let avg_time = if !times.is_empty() {
            times.iter().sum::<Duration>() / times.len() as u32
        } else {
            Duration::ZERO
        };

        let min_time = times.iter().min().copied().unwrap_or(Duration::ZERO);
        let max_time = times.iter().max().copied().unwrap_or(Duration::ZERO);

        // Calculate standard deviation
        let variance = if times.len() > 1 {
            let mean = avg_time.as_nanos() as f64;
            let sum_squared_diff = times.iter()
                .map(|&t| {
                    let diff = t.as_nanos() as f64 - mean;
                    diff * diff
                })
                .sum::<f64>();
            sum_squared_diff / (times.len() - 1) as f64
        } else {
            0.0
        };
        let std_dev = Duration::from_nanos(variance.sqrt() as u64);

        // Calculate throughput
        let throughput = if avg_time.as_secs_f64() > 0.0 {
            1.0 / avg_time.as_secs_f64()
        } else {
            0.0
        };

        // Get performance metrics
        let stats = collector.get_stats().await;
        let memory_usage = stats.peak_memory_used;
        let cpu_utilization = stats.cpu_utilization;

        Ok(BenchmarkResult {
            name,
            iterations: times.len(),
            total_time,
            avg_time,
            min_time,
            max_time,
            std_dev,
            throughput,
            memory_usage,
            cpu_utilization,
            error_count,
        })
    }

    /// Get all benchmark results
    pub fn get_results(&self) -> &[BenchmarkResult] {
        &self.results
    }

    /// Generate performance report
    pub fn generate_report(&self) -> String {
        let mut report = String::new();
        report.push_str("# Performance Benchmark Report\n\n");

        for result in &self.results {
            report.push_str(&format!("## {}\n", result.name));
            report.push_str(&format!("- Iterations: {}\n", result.iterations));
            report.push_str(&format!("- Average Time: {:?}\n", result.avg_time));
            report.push_str(&format!("- Min Time: {:?}\n", result.min_time));
            report.push_str(&format!("- Max Time: {:?}\n", result.max_time));
            report.push_str(&format!("- Standard Deviation: {:?}\n", result.std_dev));
            report.push_str(&format!("- Throughput: {:.2} ops/sec\n", result.throughput));
            report.push_str(&format!("- Memory Usage: {} bytes\n", result.memory_usage));
            report.push_str(&format!("- CPU Utilization: {:.2}%\n", result.cpu_utilization * 100.0));
            report.push_str(&format!("- Error Count: {}\n", result.error_count));
            report.push_str("\n");
        }

        report
    }

    /// Export results to JSON
    pub fn export_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(&self.results)
    }

    /// Export results to CSV
    pub fn export_csv(&self) -> String {
        let mut csv = String::new();
        csv.push_str("name,iterations,avg_time_ms,min_time_ms,max_time_ms,std_dev_ms,throughput,memory_usage,cpu_utilization,error_count\n");

        for result in &self.results {
            csv.push_str(&format!(
                "{},{},{},{},{},{},{},{},{},{}\n",
                result.name,
                result.iterations,
                result.avg_time.as_millis(),
                result.min_time.as_millis(),
                result.max_time.as_millis(),
                result.std_dev.as_millis(),
                result.throughput,
                result.memory_usage,
                result.cpu_utilization,
                result.error_count
            ));
        }

        csv
    }
}

/// Performance comparison utility
pub struct PerformanceComparator {
    baseline_results: HashMap<String, BenchmarkResult>,
    current_results: HashMap<String, BenchmarkResult>,
}

impl PerformanceComparator {
    /// Create a new performance comparator
    pub fn new() -> Self {
        Self {
            baseline_results: HashMap::new(),
            current_results: HashMap::new(),
        }
    }

    /// Set baseline results
    pub fn set_baseline(&mut self, results: Vec<BenchmarkResult>) {
        self.baseline_results.clear();
        for result in results {
            self.baseline_results.insert(result.name.clone(), result);
        }
    }

    /// Set current results
    pub fn set_current(&mut self, results: Vec<BenchmarkResult>) {
        self.current_results.clear();
        for result in results {
            self.current_results.insert(result.name.clone(), result);
        }
    }

    /// Compare performance
    pub fn compare(&self) -> Vec<PerformanceComparison> {
        let mut comparisons = Vec::new();

        for (name, current) in &self.current_results {
            if let Some(baseline) = self.baseline_results.get(name) {
                let time_improvement = self.calculate_improvement(
                    baseline.avg_time.as_nanos() as f64,
                    current.avg_time.as_nanos() as f64,
                );

                let throughput_improvement = self.calculate_improvement(
                    baseline.throughput,
                    current.throughput,
                );

                let memory_improvement = self.calculate_improvement(
                    baseline.memory_usage as f64,
                    current.memory_usage as f64,
                );

                comparisons.push(PerformanceComparison {
                    name: name.clone(),
                    time_improvement,
                    throughput_improvement,
                    memory_improvement,
                    baseline_time: baseline.avg_time,
                    current_time: current.avg_time,
                    baseline_throughput: baseline.throughput,
                    current_throughput: current.throughput,
                });
            }
        }

        comparisons
    }

    fn calculate_improvement(&self, baseline: f64, current: f64) -> f64 {
        if baseline == 0.0 {
            0.0
        } else {
            (current - baseline) / baseline
        }
    }
}

/// Performance comparison result
#[derive(Debug, Clone)]
pub struct PerformanceComparison {
    pub name: String,
    pub time_improvement: f64,
    pub throughput_improvement: f64,
    pub memory_improvement: f64,
    pub baseline_time: Duration,
    pub current_time: Duration,
    pub baseline_throughput: f64,
    pub current_throughput: f64,
}


impl Default for PerformanceComparator {
    fn default() -> Self {
        Self::new()
    }
}
