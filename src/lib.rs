//! OneEngine - A unified native engine for Spark, Flink, Trino, and Presto workers
//! 
//! This crate provides a high-performance, unified execution engine that can serve
//! as a worker for multiple big data processing frameworks.

pub mod core;
pub mod scheduler;
pub mod executor;
pub mod protocol;
pub mod memory;
pub mod utils;

// Re-export commonly used types
pub use core::engine::OneEngine;
pub use utils::config::Config;
