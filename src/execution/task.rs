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
use std::collections::HashMap;
use uuid::Uuid;
use chrono::{DateTime, Utc};

/// A task represents a unit of work in the pipeline
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub id: Uuid,
    pub name: String,
    pub task_type: TaskType,
    pub priority: Priority,
    pub resource_requirements: ResourceRequirements,
    pub dependencies: Vec<Uuid>,
    pub created_at: DateTime<Utc>,
    pub deadline: Option<DateTime<Utc>>,
    pub metadata: HashMap<String, String>,
}

/// Types of tasks that can be executed
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TaskType {
    /// Data processing task (map, filter, reduce, etc.)
    DataProcessing {
        operator: String,
        input_schema: Option<String>,
        output_schema: Option<String>,
    },
    /// Data source task (reading from external systems)
    DataSource {
        source_type: String,
        connection_info: HashMap<String, String>,
    },
    /// Data sink task (writing to external systems)
    DataSink {
        sink_type: String,
        connection_info: HashMap<String, String>,
    },
    /// Control flow task (conditionals, loops, etc.)
    ControlFlow {
        flow_type: String,
        condition: Option<String>,
    },
    /// Custom task
    Custom {
        handler: String,
        parameters: HashMap<String, String>,
    },
}

/// Task priority levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Priority {
    Low = 1,
    Normal = 2,
    High = 3,
    Critical = 4,
}

/// Resource requirements for a task
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceRequirements {
    pub cpu_cores: u32,
    pub memory_mb: u64,
    pub disk_mb: u64,
    pub network_bandwidth_mbps: Option<u32>,
    pub gpu_cores: Option<u32>,
    pub custom_resources: HashMap<String, u64>,
}

impl Task {
    /// Create a new task
    pub fn new(
        name: String,
        task_type: TaskType,
        priority: Priority,
        resource_requirements: ResourceRequirements,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            name,
            task_type,
            priority,
            resource_requirements,
            dependencies: Vec::new(),
            created_at: Utc::now(),
            deadline: None,
            metadata: HashMap::new(),
        }
    }

    /// Add a dependency to this task
    pub fn add_dependency(&mut self, task_id: Uuid) {
        self.dependencies.push(task_id);
    }

    /// Set the deadline for this task
    pub fn set_deadline(&mut self, deadline: DateTime<Utc>) {
        self.deadline = Some(deadline);
    }

    /// Add metadata to this task
    pub fn add_metadata(&mut self, key: String, value: String) {
        self.metadata.insert(key, value);
    }

    /// Check if this task is ready to execute (all dependencies satisfied)
    pub fn is_ready(&self, completed_tasks: &std::collections::HashSet<Uuid>) -> bool {
        self.dependencies.iter().all(|dep| completed_tasks.contains(dep))
    }

    /// Get the estimated execution time in milliseconds
    pub fn estimated_execution_time(&self) -> u64 {
        // Simple heuristic based on resource requirements
        let base_time = 1000; // 1 second base
        let cpu_factor = self.resource_requirements.cpu_cores as u64 * 100;
        let memory_factor = self.resource_requirements.memory_mb / 100;
        
        base_time + cpu_factor + memory_factor
    }
}

impl Default for ResourceRequirements {
    fn default() -> Self {
        Self {
            cpu_cores: 1,
            memory_mb: 512,
            disk_mb: 1024,
            network_bandwidth_mbps: None,
            gpu_cores: None,
            custom_resources: HashMap::new(),
        }
    }
}
