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


use oneengine::execution::engine::OneEngine;
use oneengine::utils::config::Config;
use tracing::{info, error};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    info!("Starting OneEngine - Unified Native Engine");

    // Load configuration
    let config = Config::load()?;
    info!("Configuration loaded: {:?}", config);

    // Create and start the engine
    let mut engine = OneEngine::new(config).await?;
    
    match engine.start().await {
        Ok(_) => {
            info!("OneEngine started successfully");
            engine.run().await?;
        }
        Err(e) => {
            error!("Failed to start OneEngine: {}", e);
            return Err(e);
        }
    }

    Ok(())
}
