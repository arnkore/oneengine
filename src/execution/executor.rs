use crate::execution::task::Task;
use crate::utils::config::ExecutorConfig;
use anyhow::Result;
use std::sync::Arc;
use tokio::sync::{RwLock, mpsc};
use tracing::{info, error, debug};
use uuid::Uuid;

/// High-performance task executor
pub struct Executor {
    config: ExecutorConfig,
    workers: Vec<Arc<Worker>>,
    running: Arc<RwLock<bool>>,
    task_sender: mpsc::UnboundedSender<ExecutableTask>,
    task_receiver: Arc<RwLock<Option<mpsc::UnboundedReceiver<ExecutableTask>>>>,
}

/// A task ready for execution
#[derive(Debug, Clone)]
pub struct ExecutableTask {
    pub task: Task,
    pub allocation_id: Uuid,
    pub retry_count: u32,
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

impl Executor {
    /// Create a new executor
    pub async fn new(config: ExecutorConfig) -> Result<Self> {
        info!("Creating executor with config: {:?}", config);

        let (task_sender, task_receiver) = mpsc::unbounded_channel();
        
        let workers = Self::create_workers(config.worker_threads).await?;

        Ok(Self {
            config,
            workers,
            running: Arc::new(RwLock::new(false)),
            task_sender,
            task_receiver: Arc::new(RwLock::new(Some(task_receiver))),
        })
    }

    /// Create worker threads
    async fn create_workers(worker_count: usize) -> Result<Vec<Arc<Worker>>> {
        let mut workers = Vec::new();
        
        for i in 0..worker_count {
            let worker = Arc::new(Worker::new(i));
            workers.push(worker);
        }
        
        info!("Created {} worker threads", worker_count);
        Ok(workers)
    }

    /// Start the executor
    pub async fn start(&self) -> Result<()> {
        info!("Starting executor...");

        *self.running.write().await = true;

        // Start worker threads
        for worker in &self.workers {
            worker.start().await?;
        }

        // Start task distribution loop
        let executor = self.clone();
        tokio::spawn(async move {
            if let Err(e) = executor.task_distribution_loop().await {
                error!("Task distribution loop error: {}", e);
            }
        });

        info!("Executor started with {} workers", self.workers.len());
        Ok(())
    }

    /// Stop the executor
    pub async fn stop(&self) -> Result<()> {
        info!("Stopping executor...");

        *self.running.write().await = false;

        // Stop all workers
        for worker in &self.workers {
            worker.stop().await?;
        }

        info!("Executor stopped");
        Ok(())
    }

    /// Submit a task for execution
    pub async fn submit_task(&self, task: Task, allocation_id: Uuid) -> Result<()> {
        debug!("Submitting task for execution: {}", task.id);

        let executable_task = ExecutableTask {
            task,
            allocation_id,
            retry_count: 0,
        };

        self.task_sender.send(executable_task)?;
        Ok(())
    }

    /// Main task distribution loop
    async fn task_distribution_loop(&self) -> Result<()> {
        let mut task_receiver = self.task_receiver.write().await.take()
            .ok_or_else(|| anyhow::anyhow!("Task receiver already taken"))?;

        while *self.running.read().await {
            tokio::select! {
                // Process incoming tasks
                task = task_receiver.recv() => {
                    match task {
                        Some(executable_task) => {
                            if let Err(e) = self.distribute_task(executable_task).await {
                                error!("Error distributing task: {}", e);
                            }
                        }
                        None => {
                            debug!("Task receiver closed");
                            break;
                        }
                    }
                }
                
                // Yield control
                _ = tokio::time::sleep(tokio::time::Duration::from_millis(1)) => {
                    // Continue
                }
            }
        }

        Ok(())
    }

    /// Distribute task to an available worker
    async fn distribute_task(&self, executable_task: ExecutableTask) -> Result<()> {
        // Find the least busy worker
        let mut best_worker = None;
        let mut min_load = usize::MAX;

        for worker in &self.workers {
            let load = worker.get_current_load().await;
            if load < min_load {
                min_load = load;
                best_worker = Some(worker);
            }
        }

        if let Some(worker) = best_worker {
            worker.submit_task(executable_task).await?;
            debug!("Task distributed to worker {}", worker.id);
        } else {
            error!("No available workers for task distribution");
            return Err(anyhow::anyhow!("No available workers"));
        }

        Ok(())
    }
}

impl Clone for Executor {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            workers: self.workers.clone(),
            running: self.running.clone(),
            task_sender: self.task_sender.clone(),
            task_receiver: self.task_receiver.clone(),
        }
    }
}

/// A worker thread that executes tasks
pub struct Worker {
    pub id: usize,
    running: Arc<RwLock<bool>>,
    current_load: Arc<RwLock<usize>>,
    task_queue: Arc<RwLock<Vec<ExecutableTask>>>,
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
            crate::execution::task::TaskType::DataProcessing { operator, .. } => {
                self.execute_data_processing_task(task, operator).await
            }
            crate::execution::task::TaskType::DataSource { source_type, .. } => {
                self.execute_data_source_task(task, source_type).await
            }
            crate::execution::task::TaskType::DataSink { sink_type, .. } => {
                self.execute_data_sink_task(task, sink_type).await
            }
            crate::execution::task::TaskType::ControlFlow { flow_type, .. } => {
                self.execute_control_flow_task(task, flow_type).await
            }
            crate::execution::task::TaskType::Custom { handler, .. } => {
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
