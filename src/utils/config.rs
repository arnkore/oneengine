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


use serde::{Deserialize, Serialize};
use std::path::Path;
use anyhow::Result;

/// Main configuration for OneEngine
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub scheduler: SchedulerConfig,
    pub executor: ExecutorConfig,
    pub protocol: ProtocolConfig,
    pub memory: MemoryConfig,
    pub logging: LoggingConfig,
}

/// Scheduler configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchedulerConfig {
    pub queue_capacity: usize,
    pub max_concurrent_tasks: usize,
    pub scheduling_interval_ms: u64,
    pub resource_config: ResourceConfig,
}

/// Executor configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutorConfig {
    pub worker_threads: usize,
    pub max_task_retries: u32,
    pub task_timeout_seconds: u64,
    pub enable_metrics: bool,
}

/// Protocol configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtocolConfig {
    pub bind_address: String,
    pub port: u16,
    pub supported_engines: Vec<String>,
    pub max_connections: usize,
}

/// Memory configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryConfig {
    pub max_memory_mb: u64,
    pub page_size_mb: u64,
    pub enable_memory_pooling: bool,
    pub gc_threshold: f64,
}

/// Resource configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceConfig {
    pub max_cpu_utilization: f64,
    pub max_memory_utilization: f64,
    pub enable_gpu_scheduling: bool,
    pub enable_custom_resources: bool,
}

/// Logging configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    pub level: String,
    pub format: String,
    pub output: String,
}

impl Config {
    /// Load configuration from file or create default
    pub fn load() -> Result<Self> {
        let config_path = "config.yaml";
        
        if Path::new(config_path).exists() {
            let content = std::fs::read_to_string(config_path)?;
            let config: Config = serde_yaml::from_str(&content)?;
            Ok(config)
        } else {
            let config = Config::default();
            // Save default config for future use
            let content = serde_yaml::to_string(&config)?;
            std::fs::write(config_path, content)?;
            Ok(config)
        }
    }

    /// Save configuration to file
    pub fn save(&self, path: &str) -> Result<()> {
        let content = serde_yaml::to_string(self)?;
        std::fs::write(path, content)?;
        Ok(())
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            scheduler: SchedulerConfig::default(),
            executor: ExecutorConfig::default(),
            protocol: ProtocolConfig::default(),
            memory: MemoryConfig::default(),
            logging: LoggingConfig::default(),
        }
    }
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            queue_capacity: 10000,
            max_concurrent_tasks: 100,
            scheduling_interval_ms: 10,
            resource_config: ResourceConfig::default(),
        }
    }
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        Self {
            worker_threads: num_cpus::get(),
            max_task_retries: 3,
            task_timeout_seconds: 300,
            enable_metrics: true,
        }
    }
}

impl Default for ProtocolConfig {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0".to_string(),
            port: 8080,
            supported_engines: vec![
                "spark".to_string(),
                "flink".to_string(),
                "trino".to_string(),
                "presto".to_string(),
            ],
            max_connections: 1000,
        }
    }
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            max_memory_mb: 8192,
            page_size_mb: 64,
            enable_memory_pooling: true,
            gc_threshold: 0.8,
        }
    }
}

impl Default for ResourceConfig {
    fn default() -> Self {
        Self {
            max_cpu_utilization: 0.8,
            max_memory_utilization: 0.8,
            enable_gpu_scheduling: false,
            enable_custom_resources: false,
        }
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
            format: "json".to_string(),
            output: "stdout".to_string(),
        }
    }
}
