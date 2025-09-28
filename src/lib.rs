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


//! OneEngine - A unified native engine for Spark, Flink, Trino, and Presto workers
//! 
//! This crate provides a high-performance, unified execution engine that can serve
//! as a worker for multiple big data processing frameworks.

pub mod protocol;
pub mod memory;
pub mod utils;
pub mod execution;
pub mod datalake;
pub mod ipc;
pub mod simd;
pub mod network;
pub mod serialization;
pub mod expression;

// Re-export commonly used types
pub use execution::mpp_engine::{MppExecutionEngine, MppExecutionEngineFactory, MppExecutionConfig};
pub use execution::integrated_engine::{IntegratedEngine, IntegratedEngineFactory, IntegratedEngineConfig, StageExecutionPlan};
pub use utils::config::Config;
