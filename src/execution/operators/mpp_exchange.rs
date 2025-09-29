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

//! MPP data exchange operators
//! 
//! Handles data exchange between workers in distributed MPP execution

use std::sync::Arc;
use std::collections::HashMap;
use anyhow::Result;
use arrow::record_batch::RecordBatch;
use arrow::datatypes::SchemaRef;
use serde::{Serialize, Deserialize};
use tokio::sync::mpsc;
use uuid::Uuid;
use tracing::{debug, warn, error};
use super::mpp_operator::{MppOperator, MppContext, MppOperatorStats, PartitionId};

/// Worker node identifier
pub type WorkerId = String;

/// Exchange operator identifier
pub type ExchangeId = Uuid;

/// Data exchange strategy
#[derive(Debug, Clone, PartialEq)]
pub enum ExchangeStrategy {
    /// Broadcast data to all workers
    Broadcast,
    /// Hash partition data based on key
    HashPartition { key_columns: Vec<String> },
    /// Range partition data
    RangePartition { range_bounds: Vec<serde_json::Value> },
    /// Round-robin distribution
    RoundRobin,
    /// Single distribution (to one worker)
    Single { target_worker: WorkerId },
}

/// Exchange operator configuration
#[derive(Debug, Clone, Default)]
pub struct ExchangeConfig {
    /// Exchange strategy
    pub strategy: ExchangeStrategy,
    /// Output schema
    pub output_schema: SchemaRef,
    /// Buffer size for data exchange
    pub buffer_size: usize,
    /// Compression enabled
    pub compression_enabled: bool,
    /// Retry configuration
    pub retry_config: RetryConfig,
}

/// Retry configuration
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum retry attempts
    pub max_attempts: u32,
    /// Initial retry delay
    pub initial_delay: std::time::Duration,
    /// Maximum retry delay
    pub max_delay: std::time::Duration,
    /// Backoff multiplier
    pub backoff_multiplier: f64,
}

/// Data exchange operator
pub struct DataExchangeOperator {
    /// Exchange ID
    exchange_id: ExchangeId,
    /// Configuration
    config: ExchangeConfig,
    /// Target workers
    target_workers: Vec<WorkerId>,
    /// Exchange channels
    exchange_channels: HashMap<WorkerId, mpsc::UnboundedSender<RecordBatch>>,
    /// Statistics
    stats: ExchangeStats,
    /// Hash partitioner
    hash_partitioner: Option<HashPartitioner>,
}

/// Hash partitioner for data distribution
pub struct HashPartitioner {
    /// Key columns for hashing
    key_columns: Vec<String>,
    /// Number of partitions
    num_partitions: usize,
}

impl HashPartitioner {
    pub fn new(key_columns: Vec<String>, num_partitions: usize) -> Self {
        Self {
            key_columns,
            num_partitions,
        }
    }
    
    /// Get partition for a row
    pub fn get_partition(&self, batch: &RecordBatch, row_idx: usize) -> Result<usize> {
        // Calculate hash based on key columns
        let mut hash = 0u64;
        for column_name in &self.key_columns {
            if let Some(column) = batch.column_by_name(column_name) {
                let value = self.get_column_value(column, row_idx)?;
                hash = hash.wrapping_add(self.hash_value(&value));
            }
        }
        Ok((hash as usize) % self.num_partitions)
    }
    
    fn get_column_value(&self, column: &arrow::array::ArrayRef, row_idx: usize) -> Result<serde_json::Value> {
        use arrow::array::*;
        use serde_json::Value;
        
        if column.is_null(row_idx) {
            return Ok(Value::Null);
        }
        
        let value = match column.data_type() {
            arrow::datatypes::DataType::Int32 => {
                let array = column.as_any().downcast_ref::<Int32Array>().unwrap();
                Value::Number(array.value(row_idx).into())
            }
            arrow::datatypes::DataType::Int64 => {
                let array = column.as_any().downcast_ref::<Int64Array>().unwrap();
                Value::Number(array.value(row_idx).into())
            }
            arrow::datatypes::DataType::Utf8 => {
                let array = column.as_any().downcast_ref::<StringArray>().unwrap();
                Value::String(array.value(row_idx).to_string())
            }
            _ => return Err(anyhow::anyhow!("Unsupported column type for hashing")),
        };
        
        Ok(value)
    }
    
    fn hash_value(&self, value: &serde_json::Value) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }
}

/// Exchange statistics
#[derive(Debug, Clone, Default)]
pub struct ExchangeStats {
    /// Batches sent
    pub batches_sent: u64,
    /// Rows sent
    pub rows_sent: u64,
    /// Bytes sent
    pub bytes_sent: u64,
    /// Network operations
    pub network_operations: u64,
    /// Retry count
    pub retry_count: u64,
    /// Error count
    pub error_count: u64,
    /// Processing time
    pub processing_time: std::time::Duration,
    /// Network time
    pub network_time: std::time::Duration,
}

impl DataExchangeOperator {
    pub fn new(
        exchange_id: ExchangeId,
        config: ExchangeConfig,
        target_workers: Vec<WorkerId>,
        exchange_channels: HashMap<WorkerId, mpsc::UnboundedSender<RecordBatch>>,
    ) -> Self {
        let hash_partitioner = match &config.strategy {
            ExchangeStrategy::HashPartition { key_columns } => {
                Some(HashPartitioner::new(key_columns.clone(), target_workers.len()))
            }
            _ => None,
        };
        
        Self {
            exchange_id,
            config,
            target_workers,
            exchange_channels,
            stats: ExchangeStats::default(),
            hash_partitioner,
        }
    }
    
    /// Exchange data batch
    pub async fn exchange_batch(&mut self, batch: RecordBatch) -> Result<()> {
        let start = std::time::Instant::now();
        
        // Clone the strategy to avoid borrow checker issues
        let strategy = self.config.strategy.clone();
        match strategy {
            ExchangeStrategy::Broadcast => {
                self.broadcast_batch(batch).await?;
            }
            ExchangeStrategy::HashPartition { .. } => {
                self.hash_partition_batch(batch).await?;
            }
            ExchangeStrategy::RangePartition { .. } => {
                self.range_partition_batch(batch).await?;
            }
            ExchangeStrategy::RoundRobin => {
                self.round_robin_batch(batch).await?;
            }
            ExchangeStrategy::Single { target_worker } => {
                self.single_distribute_batch(batch, &target_worker).await?;
            }
        }
        
        let duration = start.elapsed();
        self.stats.processing_time += duration;
        
        debug!("Exchange batch completed in {:?}", duration);
        Ok(())
    }
    
    /// Broadcast batch to all workers
    async fn broadcast_batch(&mut self, batch: RecordBatch) -> Result<()> {
        let target_workers = self.target_workers.clone();
        let exchange_channels = self.exchange_channels.clone();
        
        for worker_id in &target_workers {
            if let Some(channel) = exchange_channels.get(worker_id) {
                self.send_batch_with_retry(channel, batch.clone(), worker_id).await?;
            }
        }
        self.stats.batches_sent += 1;
        self.stats.rows_sent += batch.num_rows() as u64;
        Ok(())
    }
    
    /// Hash partition batch
    async fn hash_partition_batch(&mut self, batch: RecordBatch) -> Result<()> {
        if let Some(partitioner) = &self.hash_partitioner {
            // Group rows by partition
            let mut partition_batches: HashMap<usize, Vec<RecordBatch>> = HashMap::new();
            
            for row_idx in 0..batch.num_rows() {
                let partition = partitioner.get_partition(&batch, row_idx)?;
                let worker_idx = partition % self.target_workers.len();
                
                // For simplicity, we'll send the entire batch to the determined worker
                // In a real implementation, you'd slice the batch by rows
                if let Some(worker_id) = self.target_workers.get(worker_idx) {
                    if let Some(channel) = self.exchange_channels.get(worker_id) {
                        self.send_batch_with_retry(channel, batch.clone(), worker_id).await?;
                    }
                }
            }
        }
        self.stats.batches_sent += 1;
        self.stats.rows_sent += batch.num_rows() as u64;
        Ok(())
    }
    
    /// Range partition batch
    async fn range_partition_batch(&mut self, batch: RecordBatch) -> Result<()> {
        // Implementation would depend on range bounds
        // For now, use round-robin as fallback
        self.round_robin_batch(batch).await
    }
    
    /// Round-robin distribute batch
    async fn round_robin_batch(&mut self, batch: RecordBatch) -> Result<()> {
        // Simple round-robin distribution
        let target_workers = self.target_workers.clone();
        let exchange_channels = self.exchange_channels.clone();
        let worker_idx = (self.stats.batches_sent as usize) % target_workers.len();
        if let Some(worker_id) = target_workers.get(worker_idx) {
            if let Some(channel) = exchange_channels.get(worker_id) {
                self.send_batch_with_retry(channel, batch.clone(), worker_id).await?;
            }
        }
        self.stats.batches_sent += 1;
        self.stats.rows_sent += batch.num_rows() as u64;
        Ok(())
    }
    
    /// Single distribution
    async fn single_distribute_batch(&mut self, batch: RecordBatch, target_worker: &WorkerId) -> Result<()> {
        let exchange_channels = self.exchange_channels.clone();
        if let Some(channel) = exchange_channels.get(target_worker) {
            self.send_batch_with_retry(channel, batch.clone(), target_worker).await?;
        }
        self.stats.batches_sent += 1;
        self.stats.rows_sent += batch.num_rows() as u64;
        Ok(())
    }
    
    /// Send batch with retry logic
    async fn send_batch_with_retry(
        &mut self,
        channel: &mpsc::UnboundedSender<RecordBatch>,
        batch: RecordBatch,
        worker_id: &WorkerId,
    ) -> Result<()> {
        let mut attempt = 0;
        let mut delay = self.config.retry_config.initial_delay;
        
        loop {
            match channel.send(batch.clone()) {
                Ok(_) => {
                    self.stats.bytes_sent += batch.get_array_memory_size() as u64;
                    self.stats.network_operations += 1;
                    return Ok(());
                }
                Err(e) => {
                    attempt += 1;
                    if attempt >= self.config.retry_config.max_attempts {
                        error!("Failed to send batch to worker {} after {} attempts: {}", worker_id, attempt, e);
                        self.stats.error_count += 1;
                        return Err(anyhow::anyhow!("Failed to send batch: {}", e));
                    }
                    
                    warn!("Failed to send batch to worker {} (attempt {}): {}, retrying in {:?}", 
                          worker_id, attempt, e, delay);
                    
                    self.stats.retry_count += 1;
                    tokio::time::sleep(delay).await;
                    
                    // Exponential backoff
                    delay = std::cmp::min(
                        delay * 2,
                        self.config.retry_config.max_delay,
                    );
                }
            }
        }
    }
    
    /// Get exchange statistics
    pub fn get_stats(&self) -> &ExchangeStats {
        &self.stats
    }
    
    /// Reset statistics
    pub fn reset_stats(&mut self) {
        self.stats = ExchangeStats::default();
    }
}

/// Exchange operator factory
pub struct ExchangeOperatorFactory;

impl ExchangeOperatorFactory {
    /// Create broadcast exchange operator
    pub fn create_broadcast(
        exchange_id: ExchangeId,
        output_schema: SchemaRef,
        target_workers: Vec<WorkerId>,
        exchange_channels: HashMap<WorkerId, mpsc::UnboundedSender<RecordBatch>>,
    ) -> DataExchangeOperator {
        let config = ExchangeConfig {
            strategy: ExchangeStrategy::Broadcast,
            output_schema,
            buffer_size: 1024,
            compression_enabled: true,
            retry_config: RetryConfig {
                max_attempts: 3,
                initial_delay: std::time::Duration::from_millis(100),
                max_delay: std::time::Duration::from_secs(5),
                backoff_multiplier: 2.0,
            },
        };
        
        DataExchangeOperator::new(exchange_id, config, target_workers, exchange_channels)
    }
    
    /// Create hash partition exchange operator
    pub fn create_hash_partition(
        exchange_id: ExchangeId,
        output_schema: SchemaRef,
        key_columns: Vec<String>,
        target_workers: Vec<WorkerId>,
        exchange_channels: HashMap<WorkerId, mpsc::UnboundedSender<RecordBatch>>,
    ) -> DataExchangeOperator {
        let config = ExchangeConfig {
            strategy: ExchangeStrategy::HashPartition { key_columns },
            output_schema,
            buffer_size: 1024,
            compression_enabled: true,
            retry_config: RetryConfig {
                max_attempts: 3,
                initial_delay: std::time::Duration::from_millis(100),
                max_delay: std::time::Duration::from_secs(5),
                backoff_multiplier: 2.0,
            },
        };
        
        DataExchangeOperator::new(exchange_id, config, target_workers, exchange_channels)
    }
    
    /// Create round-robin exchange operator
    pub fn create_round_robin(
        exchange_id: ExchangeId,
        output_schema: SchemaRef,
        target_workers: Vec<WorkerId>,
        exchange_channels: HashMap<WorkerId, mpsc::UnboundedSender<RecordBatch>>,
    ) -> DataExchangeOperator {
        let config = ExchangeConfig {
            strategy: ExchangeStrategy::RoundRobin,
            output_schema,
            buffer_size: 1024,
            compression_enabled: true,
            retry_config: RetryConfig {
                max_attempts: 3,
                initial_delay: std::time::Duration::from_millis(100),
                max_delay: std::time::Duration::from_secs(5),
                backoff_multiplier: 2.0,
            },
        };
        
        DataExchangeOperator::new(exchange_id, config, target_workers, exchange_channels)
    }
}

/// MPP data exchange operator implementation
impl MppOperator for DataExchangeOperator {
    fn initialize(&mut self, context: &MppContext) -> Result<()> {
        // Exchange channels are already set in constructor
        Ok(())
    }
    
    fn process_batch(&mut self, batch: RecordBatch, _context: &MppContext) -> Result<Vec<RecordBatch>> {
        // For data exchange, we don't return data locally
        // Data is sent to target workers via exchange_batch
        Ok(vec![])
    }
    
    fn exchange_data(&mut self, data: Vec<RecordBatch>, target_workers: Vec<WorkerId>) -> Result<()> {
        // Note: This is a synchronous method, but exchange_batch is async
        // In a real implementation, you'd need to handle this properly
        // For now, we'll just send the data directly
        for batch in data {
            for worker_id in &target_workers {
                if let Some(channel) = self.exchange_channels.get(worker_id) {
                    channel.send(batch.clone()).map_err(|e| anyhow::anyhow!("Failed to send data: {}", e))?;
                    self.stats.bytes_sent += batch.get_array_memory_size() as u64;
                    self.stats.network_operations += 1;
                }
            }
        }
        Ok(())
    }
    
    fn process_partition(&mut self, _partition_id: PartitionId, _data: RecordBatch) -> Result<RecordBatch> {
        // Data exchange doesn't process partitions locally
        Err(anyhow::anyhow!("Data exchange operator doesn't process partitions locally"))
    }
    
    fn finish(&mut self, _context: &MppContext) -> Result<()> {
        // Exchange channels are managed by the factory
        Ok(())
    }
    
    fn get_stats(&self) -> MppOperatorStats {
        MppOperatorStats {
            rows_processed: self.stats.rows_sent,
            batches_processed: self.stats.batches_sent,
            data_exchanged: self.stats.bytes_sent,
            network_operations: self.stats.network_operations,
            processing_time: self.stats.processing_time,
            network_time: self.stats.network_time,
            retry_count: self.stats.retry_count,
            error_count: self.stats.error_count,
        }
    }
    
    fn recover(&mut self, _context: &MppContext) -> Result<()> {
        // Reset statistics for recovery
        self.reset_stats();
        Ok(())
    }
}
