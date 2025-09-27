use crate::scheduler::push_scheduler::PushScheduler;
use crate::executor::executor::Executor;
use crate::protocol::adapter::ProtocolAdapter;
use crate::utils::config::Config;
use anyhow::Result;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, error};

/// The main OneEngine that orchestrates all components
pub struct OneEngine {
    config: Config,
    scheduler: Arc<PushScheduler>,
    executor: Arc<Executor>,
    protocol_adapter: Arc<ProtocolAdapter>,
    running: Arc<RwLock<bool>>,
}

impl OneEngine {
    /// Create a new OneEngine instance
    pub async fn new(config: Config) -> Result<Self> {
        info!("Initializing OneEngine with config: {:?}", config);

        // Initialize components
        let scheduler = Arc::new(PushScheduler::new(config.scheduler.clone()).await?);
        let executor = Arc::new(Executor::new(config.executor.clone()).await?);
        let protocol_adapter = Arc::new(ProtocolAdapter::new(config.protocol.clone()).await?);

        Ok(Self {
            config,
            scheduler,
            executor,
            protocol_adapter,
            running: Arc::new(RwLock::new(false)),
        })
    }

    /// Start the engine
    pub async fn start(&mut self) -> Result<()> {
        info!("Starting OneEngine components...");

        // Start scheduler
        self.scheduler.start().await?;
        info!("Push scheduler started");

        // Start executor
        self.executor.start().await?;
        info!("Executor started");

        // Start protocol adapter
        self.protocol_adapter.start().await?;
        info!("Protocol adapter started");

        // Mark as running
        *self.running.write().await = true;
        info!("OneEngine started successfully");

        Ok(())
    }

    /// Run the engine (main event loop)
    pub async fn run(&self) -> Result<()> {
        info!("OneEngine is running...");

        // Main event loop
        loop {
            if !*self.running.read().await {
                info!("OneEngine stopping...");
                break;
            }

            // Process incoming tasks from protocol adapter
            if let Err(e) = self.process_tasks().await {
                error!("Error processing tasks: {}", e);
            }

            // Yield control to allow other tasks to run
            tokio::task::yield_now().await;
        }

        info!("OneEngine stopped");
        Ok(())
    }

    /// Process incoming tasks
    async fn process_tasks(&self) -> Result<()> {
        // This will be implemented to handle task processing
        // For now, just yield to prevent busy waiting
        tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
        Ok(())
    }

    /// Stop the engine
    pub async fn stop(&self) -> Result<()> {
        info!("Stopping OneEngine...");

        // Mark as not running
        *self.running.write().await = false;

        // Stop components
        self.scheduler.stop().await?;
        self.executor.stop().await?;
        self.protocol_adapter.stop().await?;

        info!("OneEngine stopped");
        Ok(())
    }
}
