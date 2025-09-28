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
use crate::execution::pipeline::Pipeline;
use crate::execution::scheduler::task_queue::TaskQueue;
use crate::execution::scheduler::pipeline_manager::PipelineManager;
use crate::execution::scheduler::resource_manager::{ResourceManager, ResourceConfig as ResourceManagerConfig};
use crate::execution::vectorized_driver::VectorizedDriver;
use crate::execution::operators::scan_operator::VectorizedScanConfig;
use crate::execution::operators::filter::{FilterPredicate, VectorizedFilterConfig};
use crate::execution::operators::projector::{ProjectionExpression, VectorizedProjectorConfig};
use crate::execution::operators::aggregator::{AggregationFunction, VectorizedAggregatorConfig};
use crate::utils::config::SchedulerConfig;
use anyhow::Result;
use std::sync::Arc;
use tokio::sync::{RwLock, mpsc};
use tracing::{info, error, debug};
use uuid::Uuid;

/// Push-based scheduler that proactively schedules tasks
pub struct PushScheduler {
    config: SchedulerConfig,
    task_queue: Arc<TaskQueue>,
    pipeline_manager: Arc<PipelineManager>,
    resource_manager: Arc<ResourceManager>,
    vectorized_driver: Arc<RwLock<Option<VectorizedDriver>>>,
    running: Arc<RwLock<bool>>,
    task_sender: mpsc::UnboundedSender<Task>,
    task_receiver: Arc<RwLock<Option<mpsc::UnboundedReceiver<Task>>>>,
}

impl PushScheduler {
    /// Create a new push scheduler
    pub async fn new(config: SchedulerConfig) -> Result<Self> {
        info!("Creating push scheduler with config: {:?}", config);

        let (task_sender, task_receiver) = mpsc::unbounded_channel();
        
        let task_queue = Arc::new(TaskQueue::new(config.queue_capacity));
        let pipeline_manager = Arc::new(PipelineManager::new());
        let resource_manager_config = ResourceManagerConfig {
            max_cpu_utilization: config.resource_config.max_cpu_utilization,
            max_memory_utilization: config.resource_config.max_memory_utilization,
            enable_gpu_scheduling: config.resource_config.enable_gpu_scheduling,
            enable_custom_resources: config.resource_config.enable_custom_resources,
        };
        let resource_manager = Arc::new(ResourceManager::new(resource_manager_config));

        Ok(Self {
            config,
            task_queue,
            pipeline_manager,
            resource_manager,
            vectorized_driver: Arc::new(RwLock::new(None)),
            running: Arc::new(RwLock::new(false)),
            task_sender,
            task_receiver: Arc::new(RwLock::new(Some(task_receiver))),
        })
    }

    /// Start the scheduler
    pub async fn start(&self) -> Result<()> {
        info!("Starting push scheduler...");

        *self.running.write().await = true;

        // Start the main scheduling loop
        let scheduler = self.clone();
        tokio::spawn(async move {
            if let Err(e) = scheduler.scheduling_loop().await {
                error!("Scheduling loop error: {}", e);
            }
        });

        info!("Push scheduler started");
        Ok(())
    }

    /// Stop the scheduler
    pub async fn stop(&self) -> Result<()> {
        info!("Stopping push scheduler...");
        *self.running.write().await = false;
        info!("Push scheduler stopped");
        Ok(())
    }

    /// Submit a task to the scheduler
    pub async fn submit_task(&self, task: Task) -> Result<()> {
        debug!("Submitting task: {}", task.id);
        
        // Add to task queue
        self.task_queue.enqueue(task.clone()).await?;
        
        // Send to task channel for immediate processing
        self.task_sender.send(task)?;
        
        Ok(())
    }

    /// Submit a pipeline to the scheduler
    pub async fn submit_pipeline(&self, pipeline: Pipeline) -> Result<()> {
        info!("Submitting pipeline: {}", pipeline.id);
        
        // Register pipeline with pipeline manager
        self.pipeline_manager.register_pipeline(pipeline.clone()).await?;
        
        // Submit all tasks in the pipeline
        for task in pipeline.tasks {
            self.submit_task(task).await?;
        }
        
        Ok(())
    }

    /// Main scheduling loop
    async fn scheduling_loop(&self) -> Result<()> {
        let mut task_receiver = self.task_receiver.write().await.take()
            .ok_or_else(|| anyhow::anyhow!("Task receiver already taken"))?;

        while *self.running.read().await {
            tokio::select! {
                // Process incoming tasks
                task = task_receiver.recv() => {
                    match task {
                        Some(task) => {
                            if let Err(e) = self.process_task(task).await {
                                error!("Error processing task: {}", e);
                            }
                        }
                        None => {
                            debug!("Task receiver closed");
                            break;
                        }
                    }
                }
                
                // Process queued tasks
                _ = tokio::time::sleep(tokio::time::Duration::from_millis(10)) => {
                    if let Err(e) = self.process_queued_tasks().await {
                        error!("Error processing queued tasks: {}", e);
                    }
                }
            }
        }

        Ok(())
    }

    /// Process a single task
    async fn process_task(&self, task: Task) -> Result<()> {
        debug!("Processing task: {}", task.id);

        // Check resource availability
        if !self.resource_manager.can_allocate(&task.resource_requirements).await? {
            debug!("Insufficient resources for task: {}, requeuing", task.id);
            self.task_queue.enqueue(task).await?;
            return Ok(());
        }

        // Allocate resources
        let allocation = self.resource_manager.allocate(&task.resource_requirements).await?;
        
        // Schedule task for execution
        self.schedule_task_for_execution(task, allocation).await?;
        
        Ok(())
    }

    /// Process queued tasks
    async fn process_queued_tasks(&self) -> Result<()> {
        // Try to process tasks from the queue
        while let Some(task) = self.task_queue.dequeue().await? {
            if let Err(e) = self.process_task(task).await {
                error!("Error processing queued task: {}", e);
            }
        }
        Ok(())
    }

    /// Schedule a task for execution
    async fn schedule_task_for_execution(&self, task: Task, allocation: crate::execution::scheduler::resource_manager::ResourceAllocation) -> Result<()> {
        debug!("Scheduling task {} for execution", task.id);
        
        // This would typically send the task to the executor
        // For now, we'll just log it
        info!("Task {} scheduled with allocation: {:?}", task.id, allocation);
        
        Ok(())
    }
    
    /// Set the vectorized driver
    pub async fn set_vectorized_driver(&self, driver: VectorizedDriver) -> Result<()> {
        let mut vectorized_driver = self.vectorized_driver.write().await;
        *vectorized_driver = Some(driver);
        info!("Vectorized driver set in push scheduler");
        Ok(())
    }
    
    /// Execute a pipeline using vectorized execution
    pub async fn execute_pipeline_vectorized(&self, pipeline: Pipeline) -> Result<Vec<arrow::record_batch::RecordBatch>> {
        let mut driver = self.vectorized_driver.write().await;
        if let Some(ref mut driver) = *driver {
            // Convert pipeline to query plan and execute
            let query_plan = self.convert_pipeline_to_query_plan(pipeline).await?;
            let results = driver.execute_query(query_plan).await
                .map_err(|e| anyhow::anyhow!("Query execution failed: {}", e))?;
            Ok(results)
        } else {
            Err(anyhow::anyhow!("Vectorized driver not set"))
        }
    }
    
    /// Execute a task using vectorized execution
    pub async fn execute_task_vectorized(&self, task: Task) -> Result<arrow::record_batch::RecordBatch> {
        let mut driver = self.vectorized_driver.write().await;
        if let Some(ref mut driver) = *driver {
            // Convert task to query plan and execute
            let query_plan = self.convert_task_to_query_plan(task).await?;
            let results = driver.execute_query(query_plan).await
                .map_err(|e| anyhow::anyhow!("Query execution failed: {}", e))?;
            if results.is_empty() {
                return Err(anyhow::anyhow!("No results from task execution"));
            }
            Ok(results[0].clone())
        } else {
            Err(anyhow::anyhow!("Vectorized driver not set"))
        }
    }
    
    /// Convert pipeline to query plan
    async fn convert_pipeline_to_query_plan(&self, pipeline: Pipeline) -> Result<crate::execution::vectorized_driver::QueryPlan> {
        use crate::execution::vectorized_driver::*;
        use arrow::datatypes::*;
        use datafusion_common::ScalarValue;
        
        let mut operators = Vec::new();
        let mut connections = Vec::new();
        let mut port_counter = 0;
        
        // Convert each task to operator
        for (i, task) in pipeline.tasks.iter().enumerate() {
            let operator_id = (i + 1) as u32;
            let input_ports = if i == 0 { vec![] } else { vec![port_counter - 1] };
            let output_ports = vec![port_counter];
            
            let operator_node = match &task.task_type {
                crate::execution::task::TaskType::DataSource { source_type, .. } => {
                    OperatorNode {
                        operator_id,
                        operator_type: OperatorType::Scan { 
                            file_path: source_type.clone() 
                        },
                        input_ports,
                        output_ports,
                        config: OperatorConfig::ScanConfig(VectorizedScanConfig::default()),
                    }
                },
                crate::execution::task::TaskType::DataProcessing { operator, .. } => {
                    match operator.as_str() {
                        "filter" => {
                            OperatorNode {
                                operator_id,
                                operator_type: OperatorType::Filter { 
                                    predicate: FilterPredicate::GreaterThan {
                                        column: "value".to_string(),
                                        value: ScalarValue::Int32(Some(0)),
                                    },
                                    column_index: 0,
                                },
                                input_ports,
                                output_ports,
                                config: OperatorConfig::FilterConfig(VectorizedFilterConfig::default()),
                            }
                        },
                        "project" => {
                            OperatorNode {
                                operator_id,
                                operator_type: OperatorType::Project { 
                                    expressions: vec![ProjectionExpression::column("id".to_string())],
                                    output_schema: Arc::new(Schema::new(vec![
                                        Field::new("id", DataType::Int32, false),
                                    ])),
                                },
                                input_ports,
                                output_ports,
                                config: OperatorConfig::ProjectorConfig(VectorizedProjectorConfig::default()),
                            }
                        },
                        "aggregate" => {
                            OperatorNode {
                                operator_id,
                                operator_type: OperatorType::Aggregate { 
                                    group_columns: vec![0],
                                    agg_functions: vec![AggregationFunction::Count {
                                        output_column: 0,
                                    }],
                                },
                                input_ports,
                                output_ports,
                                config: OperatorConfig::AggregatorConfig(VectorizedAggregatorConfig::default()),
                            }
                        },
                        _ => {
                            return Err(anyhow::anyhow!("Unsupported operator: {}", operator));
                        }
                    }
                },
                _ => {
                    return Err(anyhow::anyhow!("Unsupported task type"));
                }
            };
            
            operators.push(operator_node);
            port_counter += 1;
        }
        
        // Create connections based on pipeline edges
        for edge in &pipeline.edges {
            if let (Some(from_idx), Some(to_idx)) = (
                pipeline.tasks.iter().position(|t| t.id == edge.from_task),
                pipeline.tasks.iter().position(|t| t.id == edge.to_task)
            ) {
                connections.push(Connection {
                    from_operator: (from_idx + 1) as u32,
                    from_port: from_idx as u32,
                    to_operator: (to_idx + 1) as u32,
                    to_port: to_idx as u32,
                });
            }
        }
        
        Ok(QueryPlan {
            plan_id: 1,
            operators,
            connections,
            input_files: vec![],
            output_schema: Arc::new(Schema::new(vec![
                Field::new("result", DataType::Utf8, false),
            ])),
        })
    }
    
    /// Convert task to query plan
    async fn convert_task_to_query_plan(&self, task: Task) -> Result<crate::execution::vectorized_driver::QueryPlan> {
        // Create a simple pipeline with one task
        let pipeline = Pipeline {
            id: Uuid::new_v4(),
            name: task.name.clone(),
            description: None,
            tasks: vec![task],
            edges: vec![],
            created_at: chrono::Utc::now(),
            metadata: std::collections::HashMap::new(),
        };
        
        self.convert_pipeline_to_query_plan(pipeline).await
    }
}

impl Clone for PushScheduler {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            task_queue: self.task_queue.clone(),
            pipeline_manager: self.pipeline_manager.clone(),
            resource_manager: self.resource_manager.clone(),
            vectorized_driver: self.vectorized_driver.clone(),
            running: self.running.clone(),
            task_sender: self.task_sender.clone(),
            task_receiver: self.task_receiver.clone(),
        }
    }
}

// ResourceAllocation is now defined in resource_manager module
