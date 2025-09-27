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


use oneengine::core::task::{Task, TaskType, Priority, ResourceRequirements};
use oneengine::core::pipeline::{Pipeline, EdgeType};
use oneengine::core::engine::OneEngine;
use oneengine::utils::config::Config;
use anyhow::Result;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    info!("OneEngine Basic Usage Example");

    // Load configuration
    let config = Config::load()?;
    info!("Configuration loaded");

    // Create and start the engine
    let mut engine = OneEngine::new(config).await?;
    engine.start().await?;
    info!("OneEngine started successfully");

    // Create a simple pipeline
    let mut pipeline = Pipeline::new(
        "example_pipeline".to_string(),
        Some("A simple example pipeline".to_string()),
    );

    // Create some tasks
    let mut source_task = Task::new(
        "data_source".to_string(),
        TaskType::DataSource {
            source_type: "file".to_string(),
            connection_info: std::collections::HashMap::new(),
        },
        Priority::High,
        ResourceRequirements::default(),
    );

    let mut processing_task = Task::new(
        "data_processing".to_string(),
        TaskType::DataProcessing {
            operator: "map".to_string(),
            input_schema: None,
            output_schema: None,
        },
        Priority::Normal,
        ResourceRequirements {
            cpu_cores: 2,
            memory_mb: 1024,
            ..Default::default()
        },
    );

    let mut sink_task = Task::new(
        "data_sink".to_string(),
        TaskType::DataSink {
            sink_type: "database".to_string(),
            connection_info: std::collections::HashMap::new(),
        },
        Priority::Normal,
        ResourceRequirements::default(),
    );

    // Add tasks to pipeline
    let source_id = pipeline.add_task(source_task);
    let processing_id = pipeline.add_task(processing_task);
    let sink_id = pipeline.add_task(sink_task);

    // Add edges to create a linear pipeline
    pipeline.add_edge(source_id, processing_id, EdgeType::DataFlow).map_err(|e| anyhow::anyhow!("Failed to add edge: {}", e))?;
    pipeline.add_edge(processing_id, sink_id, EdgeType::DataFlow).map_err(|e| anyhow::anyhow!("Failed to add edge: {}", e))?;

    info!("Pipeline created with {} tasks", pipeline.tasks.len());

    // Validate pipeline
    pipeline.validate().map_err(|e| anyhow::anyhow!("Pipeline validation failed: {}", e))?;
    info!("Pipeline validation passed");

    // In a real application, you would submit the pipeline to the engine
    // For this example, we'll just demonstrate the structure
    info!("Pipeline structure:");
    info!("  - Entry tasks: {}", pipeline.get_entry_tasks().len());
    info!("  - Exit tasks: {}", pipeline.get_exit_tasks().len());
    info!("  - Estimated execution time: {}ms", pipeline.estimated_execution_time());

    // Simulate some work
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Stop the engine
    engine.stop().await?;
    info!("OneEngine stopped");

    Ok(())
}
