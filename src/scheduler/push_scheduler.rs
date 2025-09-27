use crate::core::task::Task;
use crate::core::pipeline::Pipeline;
use crate::scheduler::task_queue::TaskQueue;
use crate::scheduler::pipeline_manager::PipelineManager;
use crate::scheduler::resource_manager::{ResourceManager, ResourceConfig as ResourceManagerConfig};
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
    async fn schedule_task_for_execution(&self, task: Task, allocation: crate::scheduler::resource_manager::ResourceAllocation) -> Result<()> {
        debug!("Scheduling task {} for execution", task.id);
        
        // This would typically send the task to the executor
        // For now, we'll just log it
        info!("Task {} scheduled with allocation: {:?}", task.id, allocation);
        
        Ok(())
    }
}

impl Clone for PushScheduler {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            task_queue: self.task_queue.clone(),
            pipeline_manager: self.pipeline_manager.clone(),
            resource_manager: self.resource_manager.clone(),
            running: self.running.clone(),
            task_sender: self.task_sender.clone(),
            task_receiver: self.task_receiver.clone(),
        }
    }
}

// ResourceAllocation is now defined in resource_manager module
