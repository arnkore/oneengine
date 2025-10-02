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


use oneengine::execution::vectorized_driver::{UnifiedExecutionEngine, UnifiedExecutionEngineFactory};
use oneengine::execution::pipeline_executor::{PipelineExecutor, PipelineExecutorFactory};
use oneengine::utils::config::Config;
use tracing::{info, error};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    info!("Starting Unified Execution Engine - High Performance Query Processing");

    // Load configuration
    let config = Config::load()?;
    info!("Configuration loaded: {:?}", config);

    // Create unified execution engine
    let engine = UnifiedExecutionEngineFactory::create_high_performance();
    info!("Unified Execution Engine created successfully");
    
    // Create pipeline executor
    let pipeline_executor = PipelineExecutorFactory::create_high_performance();
    info!("Pipeline Executor created successfully");
    
    // For now, just demonstrate the engines are ready
    info!("Unified Execution Engine and Pipeline Executor are ready for high-performance query processing");

    Ok(())
}
