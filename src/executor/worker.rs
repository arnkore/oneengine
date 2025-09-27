use crate::core::task::Task;
use anyhow::Result;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, debug, error};
use uuid::Uuid;

/// A worker thread that executes tasks
pub struct Worker {
    pub id: usize,
    running: Arc<RwLock<bool>>,
    current_load: Arc<RwLock<usize>>,
    task_queue: Arc<RwLock<Vec<ExecutableTask>>>,
}

/// A task ready for execution
#[derive(Debug, Clone)]
pub struct ExecutableTask {
    pub task: Task,
    pub allocation_id: Uuid,
    pub retry_count: u32,
}

impl Worker {
    /// Create a new worker
    pub fn new(id: usize) -> Self {
        Self {
            id,
            running: Arc::new(RwLock::new(false)),
            current_load: Arc::new(RwLock::new(0)),
            task_queue: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Start the worker
    pub async fn start(&self) -> Result<()> {
        *self.running.write().await = true;
        
        let worker = self.clone();
        tokio::spawn(async move {
            if let Err(e) = worker.worker_loop().await {
                error!("Worker {} error: {}", worker.id, e);
            }
        });

        Ok(())
    }

    /// Stop the worker
    pub async fn stop(&self) -> Result<()> {
        *self.running.write().await = false;
        Ok(())
    }

    /// Submit a task to this worker
    pub async fn submit_task(&self, task: ExecutableTask) -> Result<()> {
        let mut queue = self.task_queue.write().await;
        queue.push(task);
        
        let mut load = self.current_load.write().await;
        *load += 1;
        
        Ok(())
    }

    /// Get current load of this worker
    pub async fn get_current_load(&self) -> usize {
        *self.current_load.read().await
    }

    /// Main worker loop
    async fn worker_loop(&self) -> Result<()> {
        info!("Worker {} started", self.id);

        while *self.running.read().await {
            // Get next task
            let task = {
                let mut queue = self.task_queue.write().await;
                queue.pop()
            };

            if let Some(executable_task) = task {
                debug!("Worker {} executing task {}", self.id, executable_task.task.id);
                
                // Execute the task
                let result = self.execute_task(executable_task).await;
                
                // Update load
                {
                    let mut load = self.current_load.write().await;
                    *load = load.saturating_sub(1);
                }

                // Handle result
                match result {
                    Ok(task_result) => {
                        info!("Task {} completed successfully in {}ms", 
                              task_result.task_id, task_result.execution_time_ms);
                    }
                    Err(e) => {
                        error!("Task execution failed: {}", e);
                    }
                }
            } else {
                // No tasks available, yield
                tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
            }
        }

        info!("Worker {} stopped", self.id);
        Ok(())
    }

    /// Execute a single task
    async fn execute_task(&self, executable_task: ExecutableTask) -> Result<TaskResult> {
        let start_time = std::time::Instant::now();
        let task = &executable_task.task;

        debug!("Executing task {} of type {:?}", task.id, task.task_type);

        // Simulate task execution
        // In a real implementation, this would execute the actual task logic
        let result = match &task.task_type {
            crate::core::task::TaskType::DataProcessing { operator, .. } => {
                self.execute_data_processing_task(task, operator).await
            }
            crate::core::task::TaskType::DataSource { source_type, .. } => {
                self.execute_data_source_task(task, source_type).await
            }
            crate::core::task::TaskType::DataSink { sink_type, .. } => {
                self.execute_data_sink_task(task, sink_type).await
            }
            crate::core::task::TaskType::ControlFlow { flow_type, .. } => {
                self.execute_control_flow_task(task, flow_type).await
            }
            crate::core::task::TaskType::Custom { handler, .. } => {
                self.execute_custom_task(task, handler).await
            }
        };

        let execution_time = start_time.elapsed().as_millis() as u64;

        match result {
            Ok(output_data) => Ok(TaskResult {
                task_id: task.id,
                success: true,
                execution_time_ms: execution_time,
                error_message: None,
                output_data: Some(output_data),
            }),
            Err(e) => Ok(TaskResult {
                task_id: task.id,
                success: false,
                execution_time_ms: execution_time,
                error_message: Some(e.to_string()),
                output_data: None,
            }),
        }
    }

    /// Execute a data processing task
    async fn execute_data_processing_task(&self, task: &Task, operator: &str) -> Result<Vec<u8>> {
        debug!("Executing data processing task with operator: {}", operator);
        
        // Simulate processing time based on resource requirements
        let processing_time = task.estimated_execution_time();
        tokio::time::sleep(tokio::time::Duration::from_millis(processing_time.min(100))).await;
        
        // Return dummy output data
        Ok(format!("processed_data_{}", task.id).into_bytes())
    }

    /// Execute a data source task
    async fn execute_data_source_task(&self, task: &Task, source_type: &str) -> Result<Vec<u8>> {
        debug!("Executing data source task with type: {}", source_type);
        
        // Simulate data reading
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        
        Ok(format!("source_data_{}", task.id).into_bytes())
    }

    /// Execute a data sink task
    async fn execute_data_sink_task(&self, task: &Task, sink_type: &str) -> Result<Vec<u8>> {
        debug!("Executing data sink task with type: {}", sink_type);
        
        // Simulate data writing
        tokio::time::sleep(tokio::time::Duration::from_millis(30)).await;
        
        Ok(format!("sink_data_{}", task.id).into_bytes())
    }

    /// Execute a control flow task
    async fn execute_control_flow_task(&self, task: &Task, flow_type: &str) -> Result<Vec<u8>> {
        debug!("Executing control flow task with type: {}", flow_type);
        
        // Simulate control flow processing
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        
        Ok(format!("control_flow_{}", task.id).into_bytes())
    }

    /// Execute a custom task
    async fn execute_custom_task(&self, task: &Task, handler: &str) -> Result<Vec<u8>> {
        debug!("Executing custom task with handler: {}", handler);
        
        // Simulate custom task execution
        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
        
        Ok(format!("custom_data_{}", task.id).into_bytes())
    }
}

impl Clone for Worker {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            running: self.running.clone(),
            current_load: self.current_load.clone(),
            task_queue: self.task_queue.clone(),
        }
    }
}

/// Task execution result
#[derive(Debug, Clone)]
pub struct TaskResult {
    pub task_id: Uuid,
    pub success: bool,
    pub execution_time_ms: u64,
    pub error_message: Option<String>,
    pub output_data: Option<Vec<u8>>,
}
