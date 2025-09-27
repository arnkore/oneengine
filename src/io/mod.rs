//! IO模块
//! 
//! 提供文件I/O、Parquet读取、Arrow IPC等功能

pub mod parquet_reader;
pub mod arrow_ipc;
pub mod flight_server;
pub mod flight_exchange;
pub mod data_lake_reader;
pub mod orc_reader;
pub mod iceberg_integration;
pub mod vectorized_scan_operator;

// 重新导出常用类型
pub use parquet_reader::{ParquetReader, ParquetReaderConfig, ParquetFileStats, Predicate, ColumnSelection};
