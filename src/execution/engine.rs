use crate::scheduler::push_scheduler::PushScheduler;
use crate::execution::executor::Executor;
use crate::execution::vectorized_driver::VectorizedDriver;
use crate::io::vectorized_scan_operator::VectorizedScanConfig;
use crate::execution::operators::vectorized_filter::{FilterPredicate, VectorizedFilterConfig};
use crate::execution::operators::vectorized_projector::{ProjectionExpression, VectorizedProjectorConfig};
use crate::execution::operators::vectorized_aggregator::{AggregationFunction, VectorizedAggregatorConfig};
use crate::protocol::adapter::ProtocolAdapter;
use crate::utils::config::Config;
use crate::execution::pipeline::Pipeline;
use crate::execution::task::Task;
use anyhow::Result;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, error};

/// The main OneEngine that orchestrates all components
pub struct OneEngine {
    config: Config,
    scheduler: Arc<PushScheduler>,
    executor: Arc<Executor>,
    vectorized_driver: Arc<RwLock<VectorizedDriver>>,
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
        
        // Initialize vectorized driver
                let vectorized_driver_config = crate::execution::vectorized_driver::VectorizedDriverConfig {
            max_workers: config.executor.max_workers,
            memory_limit: config.executor.memory_limit,
            batch_size: config.executor.batch_size,
            enable_vectorization: true,
            enable_simd: true,
            enable_compression: true,
            enable_prefetch: true,
            enable_numa_aware: true,
            enable_adaptive_batching: true,
        };
        let vectorized_driver = Arc::new(RwLock::new(VectorizedDriver::new(vectorized_driver_config)));

        Ok(Self {
            config,
            scheduler,
            executor,
            vectorized_driver,
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

        // Start vectorized driver
        {
            let mut driver = self.vectorized_driver.write().await;
            driver.start().await?;
        }
        info!("Vectorized driver started");
        
        // Set vectorized driver in scheduler
        {
            let driver = self.vectorized_driver.read().await;
            // We need to clone the driver for the scheduler
            // This is a simplified approach - in production, we'd use Arc<VectorizedDriver>
            // For now, we'll create a new driver instance
                    let driver_config = crate::execution::vectorized_driver::VectorizedDriverConfig {
                max_workers: self.config.executor.max_workers,
                memory_limit: self.config.executor.memory_limit,
                batch_size: self.config.executor.batch_size,
                enable_vectorization: true,
                enable_simd: true,
                enable_compression: true,
                enable_prefetch: true,
                enable_numa_aware: true,
                enable_adaptive_batching: true,
            };
            let scheduler_driver = VectorizedDriver::new(driver_config);
            self.scheduler.set_vectorized_driver(scheduler_driver).await?;
        }
        info!("Vectorized driver set in scheduler");

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

    /// Execute a pipeline using vectorized execution
    pub async fn execute_pipeline(&self, pipeline: Pipeline) -> Result<Vec<arrow::record_batch::RecordBatch>> {
        info!("Executing pipeline: {}", pipeline.name);
        
        // Convert pipeline to query plan
        let query_plan = self.convert_pipeline_to_query_plan(pipeline).await?;
        
        // Execute using vectorized driver
        let mut driver = self.vectorized_driver.write().await;
        let results = driver.execute_query(query_plan).await?;
        
        info!("Pipeline execution completed: {} batches", results.len());
        Ok(results)
    }
    
    /// Execute a single task using vectorized execution
    pub async fn execute_task(&self, task: Task) -> Result<arrow::record_batch::RecordBatch> {
        info!("Executing task: {}", task.name);
        
        // Convert task to query plan
        let query_plan = self.convert_task_to_query_plan(task).await?;
        
        // Execute using vectorized driver
        let mut driver = self.vectorized_driver.write().await;
        let results = driver.execute_query(query_plan).await?;
        
        if results.is_empty() {
            return Err(anyhow::anyhow!("No results from task execution"));
        }
        
        info!("Task execution completed: {} batches", results.len());
        Ok(results[0].clone())
    }
    
    /// Convert pipeline to query plan
    async fn convert_pipeline_to_query_plan(&self, pipeline: Pipeline) -> Result<crate::execution::vectorized_driver::QueryPlan> {
        use crate::execution::vectorized_driver::*;
        use arrow::datatypes::*;
        
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
                                    predicate: FilterPredicate::Gt {
                                        column: "value".to_string(),
                                        value: datafusion_common::ScalarValue::Int32(Some(0)),
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
                                    expressions: vec![ProjectionExpression::Column("id".to_string())],
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
                                        column: "id".to_string(),
                                        output_column: "count".to_string(),
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
            id: uuid::Uuid::new_v4(),
            name: task.name.clone(),
            description: None,
            tasks: vec![task],
            edges: vec![],
            created_at: chrono::Utc::now(),
            metadata: std::collections::HashMap::new(),
        };
        
        self.convert_pipeline_to_query_plan(pipeline).await
    }

    /// Stop the engine
    pub async fn stop(&self) -> Result<()> {
        info!("Stopping OneEngine...");

        // Mark as not running
        *self.running.write().await = false;

        // Stop components
        self.scheduler.stop().await?;
        self.executor.stop().await?;
        
        // Stop vectorized driver
        {
            let mut driver = self.vectorized_driver.write().await;
            driver.stop().await?;
        }
        
        self.protocol_adapter.stop().await?;

        info!("OneEngine stopped");
        Ok(())
    }
}
