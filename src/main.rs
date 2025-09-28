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


use oneengine::execution::mpp_engine::{MppExecutionEngine, MppExecutionEngineFactory, MppExecutionConfig};
use oneengine::utils::config::Config;
use tracing::{info, error};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    info!("Starting MPP Execution Engine - Distributed Query Processing");

    // Load configuration
    let config = Config::load()?;
    info!("Configuration loaded: {:?}", config);

    // Create MPP execution engine configuration
    let mpp_config = MppExecutionEngineFactory::create_default_config(
        "worker-1".to_string(),
        vec!["worker-1".to_string(), "worker-2".to_string(), "worker-3".to_string()],
    );
    
    // Create and start the MPP engine
    let mut engine = MppExecutionEngineFactory::create_engine(mpp_config);
    info!("MPP Execution Engine created successfully");
    
    // For now, just demonstrate the engine is ready
    info!("MPP Execution Engine is ready for distributed query processing");

    Ok(())
}
