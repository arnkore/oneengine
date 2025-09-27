//! OneEngine - A unified native engine for Spark, Flink, Trino, and Presto workers
//! 
//! This crate provides a high-performance, unified execution engine that can serve
//! as a worker for multiple big data processing frameworks.

pub mod scheduler;
pub mod protocol;
pub mod memory;
pub mod utils;
pub mod columnar;
pub mod execution;
pub mod push_runtime;
pub mod io;
pub mod simd;
pub mod concurrency;
pub mod network;
pub mod serialization;

// Re-export commonly used types
pub use execution::engine::OneEngine;
pub use utils::config::Config;
