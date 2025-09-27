use crate::execution::pipeline::{Pipeline, PipelineStatus};
use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, debug, error};
use uuid::Uuid;

/// Manages pipeline lifecycle and execution
pub struct PipelineManager {
    pipelines: Arc<RwLock<HashMap<Uuid, Pipeline>>>,
    pipeline_status: Arc<RwLock<HashMap<Uuid, PipelineStatus>>>,
    completed_tasks: Arc<RwLock<std::collections::HashSet<Uuid>>>,
}

impl PipelineManager {
    /// Create a new pipeline manager
    pub fn new() -> Self {
        Self {
            pipelines: Arc::new(RwLock::new(HashMap::new())),
            pipeline_status: Arc::new(RwLock::new(HashMap::new())),
            completed_tasks: Arc::new(RwLock::new(std::collections::HashSet::new())),
        }
    }

    /// Register a new pipeline
    pub async fn register_pipeline(&self, pipeline: Pipeline) -> Result<()> {
        info!("Registering pipeline: {}", pipeline.id);

        // Validate pipeline
        pipeline.validate().map_err(|e| anyhow::anyhow!("Pipeline validation failed: {}", e))?;

        let pipeline_id = pipeline.id;
        
        // Register pipeline
        {
            let mut pipelines = self.pipelines.write().await;
            pipelines.insert(pipeline_id, pipeline);
        }

        // Set initial status
        {
            let mut status = self.pipeline_status.write().await;
            status.insert(pipeline_id, PipelineStatus::Created);
        }

        info!("Pipeline {} registered successfully", pipeline_id);
        Ok(())
    }

    /// Get a pipeline by ID
    pub async fn get_pipeline(&self, pipeline_id: Uuid) -> Result<Option<Pipeline>> {
        let pipelines = self.pipelines.read().await;
        Ok(pipelines.get(&pipeline_id).cloned())
    }

    /// Get pipeline status
    pub async fn get_pipeline_status(&self, pipeline_id: Uuid) -> Result<Option<PipelineStatus>> {
        let status = self.pipeline_status.read().await;
        Ok(status.get(&pipeline_id).cloned())
    }

    /// Update pipeline status
    pub async fn update_pipeline_status(&self, pipeline_id: Uuid, status: PipelineStatus) -> Result<()> {
        let mut pipeline_status = self.pipeline_status.write().await;
        pipeline_status.insert(pipeline_id, status.clone());
        debug!("Pipeline {} status updated to {:?}", pipeline_id, status);
        Ok(())
    }

    /// Get ready tasks for a pipeline
    pub async fn get_ready_tasks(&self, pipeline_id: Uuid) -> Result<Vec<Uuid>> {
        let pipeline = self.get_pipeline(pipeline_id).await?
            .ok_or_else(|| anyhow::anyhow!("Pipeline not found: {}", pipeline_id))?;

        let completed_tasks = self.completed_tasks.read().await;
        let ready_tasks = pipeline.get_ready_tasks(&completed_tasks);
        
        Ok(ready_tasks.iter().map(|task| task.id).collect())
    }

    /// Mark a task as completed
    pub async fn mark_task_completed(&self, task_id: Uuid) -> Result<()> {
        let mut completed_tasks = self.completed_tasks.write().await;
        completed_tasks.insert(task_id);
        debug!("Task {} marked as completed", task_id);
        Ok(())
    }

    /// Check if a pipeline is completed
    pub async fn is_pipeline_completed(&self, pipeline_id: Uuid) -> Result<bool> {
        let pipeline = self.get_pipeline(pipeline_id).await?
            .ok_or_else(|| anyhow::anyhow!("Pipeline not found: {}", pipeline_id))?;

        let completed_tasks = self.completed_tasks.read().await;
        let all_tasks_completed = pipeline.tasks.iter().all(|task| completed_tasks.contains(&task.id));
        
        Ok(all_tasks_completed)
    }

    /// Get all active pipelines
    pub async fn get_active_pipelines(&self) -> Result<Vec<Uuid>> {
        let status = self.pipeline_status.read().await;
        let active_statuses = [
            PipelineStatus::Created,
            PipelineStatus::Submitted,
            PipelineStatus::Running,
        ];

        Ok(status
            .iter()
            .filter(|(_, status)| active_statuses.contains(status))
            .map(|(id, _)| *id)
            .collect())
    }

    /// Get pipeline statistics
    pub async fn get_pipeline_stats(&self) -> Result<PipelineStats> {
        let pipelines = self.pipelines.read().await;
        let status = self.pipeline_status.read().await;
        let completed_tasks = self.completed_tasks.read().await;

        let mut status_counts = HashMap::new();
        for (_, pipeline_status) in status.iter() {
            *status_counts.entry(pipeline_status.clone()).or_insert(0) += 1;
        }

        let total_tasks: usize = pipelines.values().map(|p| p.tasks.len()).sum();
        let completed_task_count = completed_tasks.len();

        Ok(PipelineStats {
            total_pipelines: pipelines.len(),
            status_counts,
            total_tasks,
            completed_tasks: completed_task_count,
            completion_rate: if total_tasks > 0 {
                completed_task_count as f64 / total_tasks as f64
            } else {
                0.0
            },
        })
    }

    /// Clean up completed pipelines
    pub async fn cleanup_completed_pipelines(&self) -> Result<usize> {
        let mut pipelines = self.pipelines.write().await;
        let mut status = self.pipeline_status.write().await;
        let mut cleaned_count = 0;

        let completed_pipelines: Vec<Uuid> = status
            .iter()
            .filter(|(_, s)| matches!(s, PipelineStatus::Completed | PipelineStatus::Failed | PipelineStatus::Cancelled))
            .map(|(id, _)| *id)
            .collect();

        for pipeline_id in completed_pipelines {
            pipelines.remove(&pipeline_id);
            status.remove(&pipeline_id);
            cleaned_count += 1;
        }

        if cleaned_count > 0 {
            info!("Cleaned up {} completed pipelines", cleaned_count);
        }

        Ok(cleaned_count)
    }
}

/// Pipeline statistics
#[derive(Debug, Clone)]
pub struct PipelineStats {
    pub total_pipelines: usize,
    pub status_counts: HashMap<PipelineStatus, usize>,
    pub total_tasks: usize,
    pub completed_tasks: usize,
    pub completion_rate: f64,
}
