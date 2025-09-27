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


use crate::execution::task::Task;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;
use chrono::{DateTime, Utc};

/// A pipeline represents a directed acyclic graph of tasks
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pipeline {
    pub id: Uuid,
    pub name: String,
    pub description: Option<String>,
    pub tasks: Vec<Task>,
    pub edges: Vec<PipelineEdge>,
    pub created_at: DateTime<Utc>,
    pub metadata: HashMap<String, String>,
}

/// An edge in the pipeline DAG
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineEdge {
    pub from_task: Uuid,
    pub to_task: Uuid,
    pub edge_type: EdgeType,
    pub data_schema: Option<String>,
}

/// Types of edges in the pipeline
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EdgeType {
    /// Data flows from source to destination
    DataFlow,
    /// Control flow (task B depends on task A completing)
    ControlFlow,
    /// Conditional flow (task B executes only if task A succeeds)
    ConditionalFlow,
    /// Parallel execution (tasks can run in parallel)
    Parallel,
}

/// Pipeline execution status
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PipelineStatus {
    Created,
    Submitted,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl Pipeline {
    /// Create a new pipeline
    pub fn new(name: String, description: Option<String>) -> Self {
        Self {
            id: Uuid::new_v4(),
            name,
            description,
            tasks: Vec::new(),
            edges: Vec::new(),
            created_at: Utc::now(),
            metadata: HashMap::new(),
        }
    }

    /// Add a task to the pipeline
    pub fn add_task(&mut self, task: Task) -> Uuid {
        let task_id = task.id;
        self.tasks.push(task);
        task_id
    }

    /// Add an edge between two tasks
    pub fn add_edge(&mut self, from_task: Uuid, to_task: Uuid, edge_type: EdgeType) -> Result<(), String> {
        // Validate that both tasks exist
        let from_exists = self.tasks.iter().any(|t| t.id == from_task);
        let to_exists = self.tasks.iter().any(|t| t.id == to_task);
        
        if !from_exists {
            return Err(format!("Source task {} not found", from_task));
        }
        if !to_exists {
            return Err(format!("Destination task {} not found", to_task));
        }

        // Add the edge
        self.edges.push(PipelineEdge {
            from_task,
            to_task,
            edge_type,
            data_schema: None,
        });

        // Update task dependencies
        if let Some(task) = self.tasks.iter_mut().find(|t| t.id == to_task) {
            task.add_dependency(from_task);
        }

        Ok(())
    }

    /// Get all tasks that have no dependencies (entry points)
    pub fn get_entry_tasks(&self) -> Vec<&Task> {
        let task_ids: std::collections::HashSet<Uuid> = self.tasks.iter().map(|t| t.id).collect();
        
        self.tasks
            .iter()
            .filter(|task| {
                task.dependencies.is_empty() || 
                task.dependencies.iter().all(|dep| !task_ids.contains(dep))
            })
            .collect()
    }

    /// Get all tasks that no other tasks depend on (exit points)
    pub fn get_exit_tasks(&self) -> Vec<&Task> {
        let dependent_tasks: std::collections::HashSet<Uuid> = self.edges
            .iter()
            .map(|edge| edge.from_task)
            .collect();
        
        self.tasks
            .iter()
            .filter(|task| !dependent_tasks.contains(&task.id))
            .collect()
    }

    /// Get tasks that are ready to execute (all dependencies satisfied)
    pub fn get_ready_tasks(&self, completed_tasks: &std::collections::HashSet<Uuid>) -> Vec<&Task> {
        self.tasks
            .iter()
            .filter(|task| task.is_ready(completed_tasks))
            .collect()
    }

    /// Get the next tasks to execute based on the current state
    pub fn get_next_tasks(&self, completed_tasks: &std::collections::HashSet<Uuid>) -> Vec<&Task> {
        let ready_tasks = self.get_ready_tasks(completed_tasks);
        
        // Sort by priority (higher priority first)
        let mut sorted_tasks = ready_tasks;
        sorted_tasks.sort_by(|a, b| b.priority.cmp(&a.priority));
        
        sorted_tasks
    }

    /// Validate the pipeline structure
    pub fn validate(&self) -> Result<(), String> {
        // Check for cycles
        if self.has_cycle() {
            return Err("Pipeline contains cycles".to_string());
        }

        // Check that all edges reference valid tasks
        for edge in &self.edges {
            let from_exists = self.tasks.iter().any(|t| t.id == edge.from_task);
            let to_exists = self.tasks.iter().any(|t| t.id == edge.to_task);
            
            if !from_exists {
                return Err(format!("Edge references non-existent source task: {}", edge.from_task));
            }
            if !to_exists {
                return Err(format!("Edge references non-existent destination task: {}", edge.to_task));
            }
        }

        Ok(())
    }

    /// Check if the pipeline has cycles using DFS
    fn has_cycle(&self) -> bool {
        let mut visited = std::collections::HashSet::new();
        let mut rec_stack = std::collections::HashSet::new();

        for task in &self.tasks {
            if !visited.contains(&task.id) {
                if self.dfs_has_cycle(task.id, &mut visited, &mut rec_stack) {
                    return true;
                }
            }
        }

        false
    }

    fn dfs_has_cycle(
        &self,
        task_id: Uuid,
        visited: &mut std::collections::HashSet<Uuid>,
        rec_stack: &mut std::collections::HashSet<Uuid>,
    ) -> bool {
        visited.insert(task_id);
        rec_stack.insert(task_id);

        // Check all outgoing edges
        for edge in &self.edges {
            if edge.from_task == task_id {
                let next_task = edge.to_task;
                
                if !visited.contains(&next_task) {
                    if self.dfs_has_cycle(next_task, visited, rec_stack) {
                        return true;
                    }
                } else if rec_stack.contains(&next_task) {
                    return true;
                }
            }
        }

        rec_stack.remove(&task_id);
        false
    }

    /// Get the estimated total execution time
    pub fn estimated_execution_time(&self) -> u64 {
        // Simple estimation - sum of all task execution times
        // In a real implementation, this would consider parallelism
        self.tasks.iter().map(|task| task.estimated_execution_time()).sum()
    }
}
