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

//! MPP data sink operators
//! 
//! Handles data output in MPP execution

use std::sync::Arc;
use std::collections::HashMap;
use anyhow::Result;
use arrow::record_batch::RecordBatch;
use arrow::datatypes::SchemaRef;
use serde::{Serialize, Deserialize};
use uuid::Uuid;
use tracing::{debug, warn, error};
use crate::execution::operators::mpp_operator::{MppOperator, MppContext, MppOperatorStats, PartitionId, WorkerId};
use crate::execution::push_runtime::{Operator, Event, OpStatus, Outbox, PortId};

/// Data sink type
#[derive(Debug, Clone, PartialEq)]
pub enum DataSinkType {
    /// File sink (Parquet, CSV, etc.)
    File { format: String, path: String },
    /// Database sink
    Database { connection_string: String, table: String },
    /// Memory sink (for testing)
    Memory,
    /// Network sink (for distributed output)
    Network { target_workers: Vec<WorkerId> },
}

/// Data sink configuration
#[derive(Debug, Clone)]
pub struct MppDataSinkConfig {
    /// Sink type
    pub sink_type: DataSinkType,
    /// Output schema
    pub output_schema: SchemaRef,
    /// Batch size for writing
    pub batch_size: usize,
    /// Compression enabled
    pub compression_enabled: bool,
    /// Memory limit for buffering
    pub memory_limit: usize,
}

impl Default for MppDataSinkConfig {
    fn default() -> Self {
        Self {
            sink_type: DataSinkType::Memory,
            output_schema: Arc::new(arrow::datatypes::Schema::empty()),
            batch_size: 8192,
            compression_enabled: false,
            memory_limit: 1024 * 1024 * 1024, // 1GB
        }
    }
}

/// MPP data sink operator
pub struct MppDataSinkOperator {
    /// Operator ID
    operator_id: Uuid,
    /// Configuration
    config: MppDataSinkConfig,
    /// Buffered data for writing
    buffered_data: Vec<RecordBatch>,
    /// Statistics
    stats: DataSinkStats,
    /// Memory usage
    memory_usage: usize,
    /// Whether the operator is finished
    finished: bool,
}

/// Data sink statistics
#[derive(Debug, Clone, Default)]
pub struct DataSinkStats {
    /// Rows written
    pub rows_written: u64,
    /// Batches written
    pub batches_written: u64,
    /// Bytes written
    pub bytes_written: u64,
    /// Write time
    pub write_time: std::time::Duration,
    /// Memory usage
    pub memory_usage: usize,
    /// Error count
    pub error_count: u64,
}

impl MppDataSinkOperator {
    pub fn new(operator_id: Uuid, config: MppDataSinkConfig) -> Self {
        Self {
            operator_id,
            config,
            buffered_data: Vec::new(),
            stats: DataSinkStats::default(),
            memory_usage: 0,
            finished: false,
        }
    }
    
    /// Add batch to sink buffer
    pub fn add_batch(&mut self, batch: RecordBatch) -> Result<()> {
        let batch_size = batch.get_array_memory_size();
        
        // Check memory limit
        if self.memory_usage + batch_size > self.config.memory_limit {
            // Flush buffered data
            self.flush_buffered_data()?;
        }
        
        self.buffered_data.push(batch.clone());
        self.memory_usage += batch_size;
        self.stats.rows_written += batch.num_rows() as u64;
        
        debug!("Added batch to sink, total rows: {}", self.stats.rows_written);
        Ok(())
    }
    
    /// Flush buffered data to output
    pub fn flush_buffered_data(&mut self) -> Result<()> {
        if self.buffered_data.is_empty() {
            return Ok(());
        }
        
        let start = std::time::Instant::now();
        
        match &self.config.sink_type {
            DataSinkType::File { format, path } => {
                self.write_to_file(format, path)?;
            }
            DataSinkType::Database { connection_string, table } => {
                self.write_to_database(connection_string, table)?;
            }
            DataSinkType::Memory => {
                // For memory sink, just clear the buffer
                debug!("Memory sink: cleared {} batches", self.buffered_data.len());
            }
            DataSinkType::Network { target_workers } => {
                self.write_to_network(target_workers)?;
            }
        }
        
        self.stats.write_time += start.elapsed();
        self.stats.batches_written += self.buffered_data.len() as u64;
        self.buffered_data.clear();
        self.memory_usage = 0;
        
        Ok(())
    }
    
    /// Write data to file
    fn write_to_file(&self, format: &str, path: &str) -> Result<()> {
        debug!("Writing {} batches to {} file: {}", 
               self.buffered_data.len(), format, path);
        // TODO: Implement actual file writing
        Ok(())
    }
    
    /// Write data to database
    fn write_to_database(&self, connection_string: &str, table: &str) -> Result<()> {
        debug!("Writing {} batches to database table: {}", 
               self.buffered_data.len(), table);
        // TODO: Implement actual database writing
        Ok(())
    }
    
    /// Write data to network
    fn write_to_network(&self, target_workers: &[WorkerId]) -> Result<()> {
        debug!("Writing {} batches to {} workers", 
               self.buffered_data.len(), target_workers.len());
        // TODO: Implement actual network writing
        Ok(())
    }
}

impl MppOperator for MppDataSinkOperator {
    fn initialize(&mut self, _context: &MppContext) -> Result<()> {
        debug!("Initializing data sink operator: {}", self.operator_id);
        Ok(())
    }
    
    fn process_batch(&mut self, batch: RecordBatch, _context: &MppContext) -> Result<Vec<RecordBatch>> {
        self.add_batch(batch)?;
        Ok(vec![]) // Data sink doesn't produce output
    }
    
    fn exchange_data(&mut self, _data: Vec<RecordBatch>, _target_workers: Vec<WorkerId>) -> Result<()> {
        // Data sink doesn't exchange data
        Ok(())
    }
    
    fn process_partition(&mut self, _partition_id: PartitionId, batch: RecordBatch) -> Result<RecordBatch> {
        self.add_batch(batch)?;
        // Return empty batch as data sink doesn't produce output
        Ok(RecordBatch::new_empty(self.config.output_schema.clone()))
    }
    
    fn finish(&mut self, _context: &MppContext) -> Result<()> {
        // Flush any remaining data
        self.flush_buffered_data()?;
        debug!("Data sink operator finished, wrote {} rows", self.stats.rows_written);
        Ok(())
    }
    
    fn get_stats(&self) -> MppOperatorStats {
        MppOperatorStats {
            rows_processed: self.stats.rows_written,
            batches_processed: self.stats.batches_written,
            data_exchanged: 0,
            network_operations: 0,
            processing_time: self.stats.write_time,
            network_time: std::time::Duration::from_secs(0),
            retry_count: 0,
            error_count: self.stats.error_count,
            memory_usage: self.memory_usage,
        }
    }
    
    fn recover(&mut self, _context: &MppContext) -> Result<()> {
        Ok(())
    }
}

impl Operator for MppDataSinkOperator {
    fn on_event(&mut self, ev: Event, _out: &mut Outbox) -> OpStatus {
        match ev {
            Event::Data { batch, .. } => {
                // Add batch to sink
                match self.add_batch(batch) {
                    Ok(_) => OpStatus::HasMore,
                    Err(e) => {
                        error!("Failed to add batch to sink: {}", e);
                        self.stats.error_count += 1;
                        OpStatus::Error(format!("Failed to add batch to sink: {}", e))
                    }
                }
            }
            Event::Finish(_) => {
                // Flush remaining data and finish
                if let Err(e) = self.flush_buffered_data() {
                    error!("Failed to flush sink data: {}", e);
                    return OpStatus::Error(format!("Failed to flush sink data: {}", e));
                }
                self.finished = true;
                OpStatus::Finished
            }
            _ => OpStatus::Ready,
        }
    }
    
    fn is_finished(&self) -> bool {
        self.finished
    }
    
    fn name(&self) -> &str {
        "MppDataSinkOperator"
    }
}

/// Data sink operator factory
pub struct MppDataSinkOperatorFactory;

impl MppDataSinkOperatorFactory {
    /// Create a data sink operator
    pub fn create_sink(
        operator_id: Uuid,
        config: MppDataSinkConfig,
    ) -> Result<MppDataSinkOperator> {
        Ok(MppDataSinkOperator::new(operator_id, config))
    }
    
    /// Create a file sink
    pub fn create_file_sink(
        operator_id: Uuid,
        format: String,
        path: String,
        output_schema: SchemaRef,
    ) -> Result<MppDataSinkOperator> {
        let config = MppDataSinkConfig {
            sink_type: DataSinkType::File { format, path },
            output_schema,
            batch_size: 8192,
            compression_enabled: false,
            memory_limit: 1024 * 1024 * 1024, // 1GB
        };
        Ok(MppDataSinkOperator::new(operator_id, config))
    }
    
    /// Create a memory sink
    pub fn create_memory_sink(
        operator_id: Uuid,
        output_schema: SchemaRef,
    ) -> Result<MppDataSinkOperator> {
        let config = MppDataSinkConfig {
            sink_type: DataSinkType::Memory,
            output_schema,
            batch_size: 8192,
            compression_enabled: false,
            memory_limit: 1024 * 1024 * 1024, // 1GB
        };
        Ok(MppDataSinkOperator::new(operator_id, config))
    }
}
