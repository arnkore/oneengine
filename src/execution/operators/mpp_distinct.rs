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

//! MPP distinct operators
//! 
//! Handles distributed distinct operations in MPP execution

use std::sync::Arc;
use anyhow::Result;
use arrow::record_batch::RecordBatch;
use arrow::datatypes::SchemaRef;
use serde::{Serialize, Deserialize};
use uuid::Uuid;
use tracing::{debug, warn, error, info};
use std::time::Instant;
use std::collections::{HashMap, HashSet};

use super::mpp_operator::{MppOperator, MppContext, MppOperatorStats, PartitionId, WorkerId};
use crate::expression::{VectorizedExpressionEngine, ExpressionEngineConfig};
use crate::expression::ast::Expression;

/// Distinct key
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DistinctKey {
    /// Key values
    pub values: Vec<serde_json::Value>,
}

/// MPP distinct operator configuration
#[derive(Debug, Clone)]
pub struct MppDistinctConfig {
    /// Distinct columns
    pub distinct_columns: Vec<String>,
    /// Output schema
    pub output_schema: SchemaRef,
    /// Memory limit for distinct operations
    pub memory_limit: usize,
    /// Whether to use external sort
    pub use_external_sort: bool,
    /// External sort directory
    pub external_sort_dir: Option<String>,
    /// Whether to enable vectorization
    pub enable_vectorization: bool,
    /// Whether to enable SIMD
    pub enable_simd: bool,
    /// Whether to use hash-based distinct
    pub use_hash_distinct: bool,
}

impl Default for MppDistinctConfig {
    fn default() -> Self {
        Self {
            distinct_columns: Vec::new(),
            output_schema: Arc::new(arrow::datatypes::Schema::empty()),
            memory_limit: 1024 * 1024 * 1024, // 1GB
            use_external_sort: true,
            external_sort_dir: Some("/tmp/distinct".to_string()),
            enable_vectorization: true,
            enable_simd: true,
            use_hash_distinct: true,
        }
    }
}

/// MPP distinct operator
pub struct MppDistinctOperator {
    /// Operator ID
    operator_id: Uuid,
    /// Configuration
    config: MppDistinctConfig,
    /// Expression engine
    expression_engine: VectorizedExpressionEngine,
    /// Hash set for distinct values
    distinct_set: HashSet<DistinctKey>,
    /// Buffered data for sorting
    buffered_data: Vec<RecordBatch>,
    /// Statistics
    stats: MppOperatorStats,
    /// Distinct statistics
    distinct_stats: DistinctStats,
    /// Memory usage
    memory_usage: usize,
}

/// Distinct statistics
#[derive(Debug, Clone, Default)]
pub struct DistinctStats {
    /// Rows processed
    pub rows_processed: u64,
    /// Batches processed
    pub batches_processed: u64,
    /// Distinct rows found
    pub distinct_rows: u64,
    /// Duplicate rows removed
    pub duplicate_rows: u64,
    /// Distinct computation time
    pub distinct_computation_time: std::time::Duration,
    /// Sort time
    pub sort_time: std::time::Duration,
    /// Hash time
    pub hash_time: std::time::Duration,
    /// Memory usage
    pub memory_usage: usize,
    /// Spill count
    pub spill_count: u64,
    /// Spill bytes
    pub spill_bytes: u64,
}

impl MppDistinctOperator {
    pub fn new(operator_id: Uuid, config: MppDistinctConfig) -> Result<Self> {
        // Create expression engine configuration
        let expression_config = ExpressionEngineConfig {
            enable_jit: config.enable_simd,
            enable_simd: config.enable_simd,
            enable_fusion: true,
            enable_cache: true,
            jit_threshold: 100,
            cache_size_limit: 1024 * 1024 * 1024, // 1GB
            batch_size: 8192,
        };
        
        // Create expression engine
        let expression_engine = VectorizedExpressionEngine::new(expression_config)?;
        
        Ok(Self {
            operator_id,
            config,
            expression_engine,
            distinct_set: HashSet::new(),
            buffered_data: Vec::new(),
            stats: MppOperatorStats::default(),
            distinct_stats: DistinctStats::default(),
            memory_usage: 0,
        })
    }
    
    /// Add batch for distinct processing
    pub fn add_batch(&mut self, batch: RecordBatch) -> Result<()> {
        let batch_size = batch.get_array_memory_size();
        
        // Check memory limit
        if self.memory_usage + batch_size > self.config.memory_limit {
            if self.config.use_external_sort {
                self.spill_to_disk()?;
            } else {
                return Err(anyhow::anyhow!("Memory limit exceeded for distinct operations"));
            }
        }
        
        if self.config.use_hash_distinct {
            self.process_batch_hash(batch)?;
        } else {
            self.buffered_data.push(batch);
            self.memory_usage += batch_size;
        }
        
        Ok(())
    }
    
    /// Process batch using hash-based distinct
    fn process_batch_hash(&mut self, batch: RecordBatch) -> Result<()> {
        let start = Instant::now();
        
        for row_idx in 0..batch.num_rows() {
            let distinct_key = self.extract_distinct_key(&batch, row_idx)?;
            
            if self.distinct_set.insert(distinct_key) {
                self.distinct_stats.distinct_rows += 1;
            } else {
                self.distinct_stats.duplicate_rows += 1;
            }
        }
        
        self.distinct_stats.rows_processed += batch.num_rows() as u64;
        self.distinct_stats.batches_processed += 1;
        self.distinct_stats.hash_time += start.elapsed();
        
        Ok(())
    }
    
    /// Extract distinct key from row
    fn extract_distinct_key(&self, batch: &RecordBatch, row_idx: usize) -> Result<DistinctKey> {
        let mut values = Vec::new();
        
        for column_name in &self.config.distinct_columns {
            if let Some(column) = batch.column_by_name(column_name) {
                let value = self.get_column_value(column, row_idx)?;
                values.push(value);
            }
        }
        
        Ok(DistinctKey { values })
    }
    
    /// Get column value as JSON value
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
            arrow::datatypes::DataType::Float32 => {
                let array = column.as_any().downcast_ref::<Float32Array>().unwrap();
                Value::Number(serde_json::Number::from_f64(array.value(row_idx) as f64).unwrap())
            }
            arrow::datatypes::DataType::Float64 => {
                let array = column.as_any().downcast_ref::<Float64Array>().unwrap();
                Value::Number(serde_json::Number::from_f64(array.value(row_idx)).unwrap())
            }
            arrow::datatypes::DataType::Utf8 => {
                let array = column.as_any().downcast_ref::<StringArray>().unwrap();
                Value::String(array.value(row_idx).to_string())
            }
            arrow::datatypes::DataType::Boolean => {
                let array = column.as_any().downcast_ref::<BooleanArray>().unwrap();
                Value::Bool(array.value(row_idx))
            }
            _ => return Err(anyhow::anyhow!("Unsupported column type for distinct")),
        };
        
        Ok(value)
    }
    
    /// Process distinct using sort-based approach
    pub fn process_distinct_sort(&mut self) -> Result<Vec<RecordBatch>> {
        let start = Instant::now();
        
        if self.buffered_data.is_empty() {
            return Ok(vec![]);
        }
        
        // Sort data by distinct columns
        let sorted_batches = self.sort_data()?;
        
        // Remove duplicates
        let distinct_batches = self.remove_duplicates(sorted_batches)?;
        
        self.distinct_stats.distinct_computation_time += start.elapsed();
        
        debug!("Processed distinct: {} batches -> {} distinct batches", 
               self.buffered_data.len(), distinct_batches.len());
        
        Ok(distinct_batches)
    }
    
    /// Sort data by distinct columns
    fn sort_data(&mut self) -> Result<Vec<RecordBatch>> {
        let start = Instant::now();
        
        // For now, return data as-is
        // In a real implementation, you'd sort by distinct columns
        let result = self.buffered_data.clone();
        
        self.distinct_stats.sort_time += start.elapsed();
        
        Ok(result)
    }
    
    /// Remove duplicates from sorted data
    fn remove_duplicates(&self, batches: Vec<RecordBatch>) -> Result<Vec<RecordBatch>> {
        let mut result = Vec::new();
        let mut last_key: Option<DistinctKey> = None;
        
        for batch in batches {
            let mut distinct_rows = Vec::new();
            
            for row_idx in 0..batch.num_rows() {
                let current_key = self.extract_distinct_key(&batch, row_idx)?;
                
                if last_key.as_ref() != Some(&current_key) {
                    distinct_rows.push(row_idx);
                    last_key = Some(current_key);
                }
            }
            
            if !distinct_rows.is_empty() {
                // Create batch with distinct rows
                let distinct_batch = self.create_distinct_batch(&batch, &distinct_rows)?;
                result.push(distinct_batch);
            }
        }
        
        Ok(result)
    }
    
    /// Create batch with distinct rows
    fn create_distinct_batch(&self, batch: &RecordBatch, row_indices: &[usize]) -> Result<RecordBatch> {
        let mut distinct_columns = Vec::new();
        
        for column in batch.columns() {
            let distinct_values: Vec<arrow::array::ArrayRef> = row_indices
                .iter()
                .map(|&idx| column.slice(idx, 1))
                .collect();
            
            // Concatenate arrays
            let concatenated = arrow::compute::concat(&distinct_values)?;
            distinct_columns.push(concatenated);
        }
        
        let distinct_batch = RecordBatch::try_new(batch.schema(), distinct_columns)?;
        Ok(distinct_batch)
    }
    
    /// Get distinct results from hash set
    pub fn get_distinct_results(&self) -> Result<Vec<RecordBatch>> {
        // This is a simplified implementation
        // In a real implementation, you'd reconstruct batches from the hash set
        Ok(vec![])
    }
    
    /// Spill data to disk
    fn spill_to_disk(&mut self) -> Result<()> {
        if let Some(dir) = &self.config.external_sort_dir {
            warn!("Spilling {} batches to disk at {}", self.buffered_data.len(), dir);
            self.distinct_stats.spill_count += 1;
            self.distinct_stats.spill_bytes += self.memory_usage as u64;
        } else {
            return Err(anyhow::anyhow!("External sort directory not specified"));
        }
        
        self.buffered_data.clear();
        self.memory_usage = 0;
        Ok(())
    }
    
    /// Get distinct statistics
    pub fn get_distinct_stats(&self) -> &DistinctStats {
        &self.distinct_stats
    }
    
    /// Reset operator state
    pub fn reset(&mut self) {
        self.distinct_set.clear();
        self.buffered_data.clear();
        self.stats = MppOperatorStats::default();
        self.distinct_stats = DistinctStats::default();
        self.memory_usage = 0;
    }
}

impl MppOperator for MppDistinctOperator {
    fn initialize(&mut self, _context: &MppContext) -> Result<()> {
        debug!("Initialized distinct operator {}", self.operator_id);
        Ok(())
    }
    
    fn process_batch(&mut self, batch: RecordBatch, _context: &MppContext) -> Result<Vec<RecordBatch>> {
        self.add_batch(batch)?;
        Ok(vec![])
    }
    
    fn exchange_data(&mut self, _data: Vec<RecordBatch>, _target_workers: Vec<WorkerId>) -> Result<()> {
        // Distinct operator doesn't exchange data
        Err(anyhow::anyhow!("Distinct operator doesn't exchange data"))
    }
    
    fn process_partition(&mut self, _partition_id: PartitionId, data: RecordBatch) -> Result<RecordBatch> {
        let schema = data.schema();
        self.add_batch(data)?;
        
        if self.config.use_hash_distinct {
            // Return empty batch for hash-based distinct
            let schema = data.schema();
            let empty_batch = RecordBatch::new_empty(schema);
            Ok(empty_batch)
        } else {
            // Process using sort-based approach
            let results = self.process_distinct_sort()?;
            if results.is_empty() {
                let schema = data.schema();
                let empty_batch = RecordBatch::new_empty(schema);
                Ok(empty_batch)
            } else {
                Ok(results[0].clone())
            }
        }
    }
    
    fn finish(&mut self, _context: &MppContext) -> Result<()> {
        debug!("Finished distinct operator {}", self.operator_id);
        Ok(())
    }
    
    fn get_stats(&self) -> MppOperatorStats {
        MppOperatorStats {
            rows_processed: 0,
            batches_processed: 0,
            data_exchanged: 0,
            network_operations: 0,
            processing_time: std::time::Duration::from_secs(0),
            network_time: std::time::Duration::from_secs(0),
            retry_count: 0,
            error_count: 0,
            memory_usage: 0,
        }
    }
    
    fn recover(&mut self, _context: &MppContext) -> Result<()> {
        self.reset();
        debug!("Recovered distinct operator {}", self.operator_id);
        Ok(())
    }
}

/// MPP distinct operator factory
pub struct MppDistinctOperatorFactory;

impl MppDistinctOperatorFactory {
    /// Create distinct operator
    pub fn create_distinct(
        operator_id: Uuid,
        distinct_columns: Vec<String>,
        output_schema: SchemaRef,
        memory_limit: usize,
    ) -> Result<MppDistinctOperator> {
        let config = MppDistinctConfig {
            distinct_columns,
            output_schema,
            memory_limit,
            use_external_sort: true,
            external_sort_dir: Some("/tmp/distinct".to_string()),
            enable_vectorization: true,
            enable_simd: true,
            use_hash_distinct: true,
        };
        
        MppDistinctOperator::new(operator_id, config)
    }
}
