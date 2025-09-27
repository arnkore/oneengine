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


use thiserror::Error;

/// OneEngine error types
#[derive(Error, Debug)]
pub enum OneEngineError {
    #[error("Configuration error: {0}")]
    ConfigError(String),
    
    #[error("Scheduler error: {0}")]
    SchedulerError(String),
    
    #[error("Executor error: {0}")]
    ExecutorError(String),
    
    #[error("Memory error: {0}")]
    MemoryError(String),
    
    #[error("Protocol error: {0}")]
    ProtocolError(String),
    
    #[error("Task error: {0}")]
    TaskError(String),
    
    #[error("Pipeline error: {0}")]
    PipelineError(String),
    
    #[error("Resource error: {0}")]
    ResourceError(String),
    
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    
    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),
    
    #[error("YAML error: {0}")]
    YamlError(#[from] serde_yaml::Error),
    
    #[error("Anyhow error: {0}")]
    AnyhowError(#[from] anyhow::Error),
}

/// Result type for OneEngine operations
pub type OneEngineResult<T> = Result<T, OneEngineError>;
