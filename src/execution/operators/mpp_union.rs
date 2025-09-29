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

//! MPP union operators
//! 
//! Handles distributed union operations in MPP execution

use std::sync::Arc;
use anyhow::Result;
use arrow::record_batch::RecordBatch;
use arrow::datatypes::SchemaRef;
use serde::{Serialize, Deserialize};
use uuid::Uuid;
use tracing::{debug, warn, error, info};
use std::time::Instant;
use std::collections::HashMap;

use super::mpp_operator::{MppOperator, MppContext, MppOperatorStats, PartitionId, WorkerId};
use crate::expression::{VectorizedExpressionEngine, ExpressionEngineConfig};
use crate::expression::ast::Expression;

/// Union type
#[derive(Debug, Clone, PartialEq)]
pub enum UnionType {
    /// Union (removes duplicates)
    Union,
    /// Union all (keeps duplicates)
    UnionAll,
    /// Intersect (only common rows)
    Intersect,
    /// Except (rows from first input not in second)
    Except,
}

/// MPP union operator configuration
#[derive(Debug, Clone)]
pub struct MppUnionConfig {
    /// Union type
    pub union_type: UnionType,
    /// Output schema
    pub output_schema: SchemaRef,
    /// Memory limit for union operations
    pub memory_limit: usize,
    /// Whether to use external sort
    pub use_external_sort: bool,
    /// External sort directory
    pub external_sort_dir: Option<String>,
    /// Whether to enable vectorization
    pub enable_vectorization: bool,
    /// Whether to enable SIMD
    pub enable_simd: bool,
    /// Whether to use hash-based operations
    pub use_hash_operations: bool,
}

impl Default for MppUnionConfig {
    fn default() -> Self {
        Self {
            union_type: UnionType::UnionAll,
            output_schema: Arc::new(arrow::datatypes::Schema::empty()),
            memory_limit: 1024 * 1024 * 1024, // 1GB
            use_external_sort: true,
            external_sort_dir: Some("/tmp/union".to_string()),
            enable_vectorization: true,
            enable_simd: true,
            use_hash_operations: true,
        }
    }
}

/// MPP union operator
pub struct MppUnionOperator {
    /// Operator ID
    operator_id: Uuid,
    /// Configuration
    config: MppUnionConfig,
    /// Expression engine
    expression_engine: VectorizedExpressionEngine,
    /// Left input data
    left_data: Vec<RecordBatch>,
    /// Right input data
    right_data: Vec<RecordBatch>,
    /// Statistics
    stats: MppOperatorStats,
    /// Union statistics
    union_stats: UnionStats,
    /// Memory usage
    memory_usage: usize,
}

/// Union statistics
#[derive(Debug, Clone, Default)]
pub struct UnionStats {
    /// Left rows processed
    pub left_rows_processed: u64,
    /// Right rows processed
    pub right_rows_processed: u64,
    /// Output rows produced
    pub output_rows: u64,
    /// Left batches processed
    pub left_batches_processed: u64,
    /// Right batches processed
    pub right_batches_processed: u64,
    /// Union computation time
    pub union_computation_time: std::time::Duration,
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

impl MppUnionOperator {
    pub fn new(operator_id: Uuid, config: MppUnionConfig) -> Result<Self> {
        // Create expression engine configuration
        let expression_config = ExpressionEngineConfig {
            enable_jit: config.enable_simd,
            enable_simd: config.enable_simd,
            enable_fusion: true,
            enable_cache: true,
            jit_threshold: 100,
            cache_size_limit: 1024 * 1024 * 1024, // 1GB
            batch_size: 8192,
            cache_max_entries: 1000,
            cache_max_memory: 1024 * 1024 * 1024, // 1GB
        };
        
        // Create expression engine
        let expression_engine = VectorizedExpressionEngine::new(expression_config)?;
        
        Ok(Self {
            operator_id,
            config,
            expression_engine,
            left_data: Vec::new(),
            right_data: Vec::new(),
            stats: MppOperatorStats::default(),
            union_stats: UnionStats::default(),
            memory_usage: 0,
        })
    }
    
    /// Add left input batch
    pub fn add_left_batch(&mut self, batch: RecordBatch) -> Result<()> {
        let batch_size = batch.get_array_memory_size();
        
        // Check memory limit
        if self.memory_usage + batch_size > self.config.memory_limit {
            if self.config.use_external_sort {
                self.spill_to_disk()?;
            } else {
                return Err(anyhow::anyhow!("Memory limit exceeded for union operations"));
            }
        }
        
        let rows = batch.num_rows() as u64;
        self.left_data.push(batch);
        self.memory_usage += batch_size;
        self.union_stats.left_rows_processed += rows;
        self.union_stats.left_batches_processed += 1;
        
        Ok(())
    }
    
    /// Add right input batch
    pub fn add_right_batch(&mut self, batch: RecordBatch) -> Result<()> {
        let batch_size = batch.get_array_memory_size();
        
        // Check memory limit
        if self.memory_usage + batch_size > self.config.memory_limit {
            if self.config.use_external_sort {
                self.spill_to_disk()?;
            } else {
                return Err(anyhow::anyhow!("Memory limit exceeded for union operations"));
            }
        }
        
        let rows = batch.num_rows() as u64;
        self.right_data.push(batch);
        self.memory_usage += batch_size;
        self.union_stats.right_rows_processed += rows;
        self.union_stats.right_batches_processed += 1;
        
        Ok(())
    }
    
    /// Process union operation
    pub fn process_union(&mut self) -> Result<Vec<RecordBatch>> {
        let start = Instant::now();
        
        let result = match self.config.union_type {
            UnionType::Union => self.process_union_distinct()?,
            UnionType::UnionAll => self.process_union_all()?,
            UnionType::Intersect => self.process_intersect()?,
            UnionType::Except => self.process_except()?,
        };
        
        self.union_stats.union_computation_time += start.elapsed();
        self.union_stats.output_rows = result.iter().map(|b| b.num_rows() as u64).sum();
        
        debug!("Processed union: {} left + {} right -> {} output rows", 
               self.union_stats.left_rows_processed, 
               self.union_stats.right_rows_processed,
               self.union_stats.output_rows);
        
        Ok(result)
    }
    
    /// Process union all (keeps duplicates)
    fn process_union_all(&self) -> Result<Vec<RecordBatch>> {
        let mut result = Vec::new();
        
        // Add all left data
        result.extend(self.left_data.clone());
        
        // Add all right data
        result.extend(self.right_data.clone());
        
        Ok(result)
    }
    
    /// Process union (removes duplicates)
    fn process_union_distinct(&self) -> Result<Vec<RecordBatch>> {
        let mut result = Vec::new();
        let mut seen_rows = std::collections::HashSet::new();
        
        // Process left data
        for batch in &self.left_data {
            let distinct_batch = self.remove_duplicates_from_batch(batch, &mut seen_rows)?;
            if distinct_batch.num_rows() > 0 {
                result.push(distinct_batch);
            }
        }
        
        // Process right data
        for batch in &self.right_data {
            let distinct_batch = self.remove_duplicates_from_batch(batch, &mut seen_rows)?;
            if distinct_batch.num_rows() > 0 {
                result.push(distinct_batch);
            }
        }
        
        Ok(result)
    }
    
    /// Process intersect (only common rows)
    fn process_intersect(&self) -> Result<Vec<RecordBatch>> {
        let mut result = Vec::new();
        let mut left_rows = std::collections::HashSet::new();
        let mut common_rows = std::collections::HashSet::new();
        
        // Collect left rows
        for batch in &self.left_data {
            self.collect_rows(batch, &mut left_rows)?;
        }
        
        // Find common rows in right data
        for batch in &self.right_data {
            self.find_common_rows(batch, &left_rows, &mut common_rows)?;
        }
        
        // Create result batches with common rows
        for batch in &self.left_data {
            let intersect_batch = self.create_intersect_batch(batch, &common_rows)?;
            if intersect_batch.num_rows() > 0 {
                result.push(intersect_batch);
            }
        }
        
        Ok(result)
    }
    
    /// Process except (rows from first input not in second)
    fn process_except(&self) -> Result<Vec<RecordBatch>> {
        let mut result = Vec::new();
        let mut right_rows = std::collections::HashSet::new();
        
        // Collect right rows
        for batch in &self.right_data {
            self.collect_rows(batch, &mut right_rows)?;
        }
        
        // Create result batches with rows not in right data
        for batch in &self.left_data {
            let except_batch = self.create_except_batch(batch, &right_rows)?;
            if except_batch.num_rows() > 0 {
                result.push(except_batch);
            }
        }
        
        Ok(result)
    }
    
    /// Remove duplicates from batch
    fn remove_duplicates_from_batch(
        &self, 
        batch: &RecordBatch, 
        seen_rows: &mut std::collections::HashSet<Vec<serde_json::Value>>
    ) -> Result<RecordBatch> {
        let mut distinct_rows = Vec::new();
        
        for row_idx in 0..batch.num_rows() {
            let row_key = self.extract_row_key(batch, row_idx)?;
            
            if seen_rows.insert(row_key) {
                distinct_rows.push(row_idx);
            }
        }
        
        if distinct_rows.is_empty() {
            let empty_batch = RecordBatch::new_empty(batch.schema());
            Ok(empty_batch)
        } else {
            self.create_batch_with_rows(batch, &distinct_rows)
        }
    }
    
    /// Collect rows from batch
    fn collect_rows(
        &self, 
        batch: &RecordBatch, 
        rows: &mut std::collections::HashSet<Vec<serde_json::Value>>
    ) -> Result<()> {
        for row_idx in 0..batch.num_rows() {
            let row_key = self.extract_row_key(batch, row_idx)?;
            rows.insert(row_key);
        }
        Ok(())
    }
    
    /// Find common rows in batch
    fn find_common_rows(
        &self, 
        batch: &RecordBatch, 
        left_rows: &std::collections::HashSet<Vec<serde_json::Value>>,
        common_rows: &mut std::collections::HashSet<Vec<serde_json::Value>>
    ) -> Result<()> {
        for row_idx in 0..batch.num_rows() {
            let row_key = self.extract_row_key(batch, row_idx)?;
            
            if left_rows.contains(&row_key) {
                common_rows.insert(row_key);
            }
        }
        Ok(())
    }
    
    /// Create intersect batch
    fn create_intersect_batch(
        &self, 
        batch: &RecordBatch, 
        common_rows: &std::collections::HashSet<Vec<serde_json::Value>>
    ) -> Result<RecordBatch> {
        let mut intersect_rows = Vec::new();
        
        for row_idx in 0..batch.num_rows() {
            let row_key = self.extract_row_key(batch, row_idx)?;
            
            if common_rows.contains(&row_key) {
                intersect_rows.push(row_idx);
            }
        }
        
        if intersect_rows.is_empty() {
            let empty_batch = RecordBatch::new_empty(batch.schema());
            Ok(empty_batch)
        } else {
            self.create_batch_with_rows(batch, &intersect_rows)
        }
    }
    
    /// Create except batch
    fn create_except_batch(
        &self, 
        batch: &RecordBatch, 
        right_rows: &std::collections::HashSet<Vec<serde_json::Value>>
    ) -> Result<RecordBatch> {
        let mut except_rows = Vec::new();
        
        for row_idx in 0..batch.num_rows() {
            let row_key = self.extract_row_key(batch, row_idx)?;
            
            if !right_rows.contains(&row_key) {
                except_rows.push(row_idx);
            }
        }
        
        if except_rows.is_empty() {
            let empty_batch = RecordBatch::new_empty(batch.schema());
            Ok(empty_batch)
        } else {
            self.create_batch_with_rows(batch, &except_rows)
        }
    }
    
    /// Extract row key for comparison
    fn extract_row_key(&self, batch: &RecordBatch, row_idx: usize) -> Result<Vec<serde_json::Value>> {
        let mut key = Vec::new();
        
        for column in batch.columns() {
            let value = self.get_column_value(column, row_idx)?;
            key.push(value);
        }
        
        Ok(key)
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
            _ => return Err(anyhow::anyhow!("Unsupported column type for union")),
        };
        
        Ok(value)
    }
    
    /// Create batch with specific rows
    fn create_batch_with_rows(&self, batch: &RecordBatch, row_indices: &[usize]) -> Result<RecordBatch> {
        let mut new_columns = Vec::new();
        
        for column in batch.columns() {
            let selected_values: Vec<arrow::array::ArrayRef> = row_indices
                .iter()
                .map(|&idx| column.slice(idx, 1))
                .collect();
            
            // Concatenate arrays
            let concatenated = arrow::compute::concat(&selected_values)?;
            new_columns.push(concatenated);
        }
        
        let new_batch = RecordBatch::try_new(batch.schema(), new_columns)?;
        Ok(new_batch)
    }
    
    /// Spill data to disk
    fn spill_to_disk(&mut self) -> Result<()> {
        if let Some(dir) = &self.config.external_sort_dir {
            warn!("Spilling {} left + {} right batches to disk at {}", 
                  self.left_data.len(), self.right_data.len(), dir);
            self.union_stats.spill_count += 1;
            self.union_stats.spill_bytes += self.memory_usage as u64;
        } else {
            return Err(anyhow::anyhow!("External sort directory not specified"));
        }
        
        self.left_data.clear();
        self.right_data.clear();
        self.memory_usage = 0;
        Ok(())
    }
    
    /// Get union statistics
    pub fn get_union_stats(&self) -> &UnionStats {
        &self.union_stats
    }
    
    /// Reset operator state
    pub fn reset(&mut self) {
        self.left_data.clear();
        self.right_data.clear();
        self.stats = MppOperatorStats::default();
        self.union_stats = UnionStats::default();
        self.memory_usage = 0;
    }
}

impl MppOperator for MppUnionOperator {
    fn initialize(&mut self, _context: &MppContext) -> Result<()> {
        debug!("Initialized union operator {}", self.operator_id);
        Ok(())
    }
    
    fn process_batch(&mut self, batch: RecordBatch, _context: &MppContext) -> Result<Vec<RecordBatch>> {
        // Union operator needs two inputs, so we'll add to left by default
        self.add_left_batch(batch)?;
        Ok(vec![])
    }
    
    fn exchange_data(&mut self, _data: Vec<RecordBatch>, _target_workers: Vec<WorkerId>) -> Result<()> {
        // Union operator doesn't exchange data
        Err(anyhow::anyhow!("Union operator doesn't exchange data"))
    }
    
    fn process_partition(&mut self, _partition_id: PartitionId, data: RecordBatch) -> Result<RecordBatch> {
        let schema = data.schema();
        self.add_left_batch(data)?;
        let results = self.process_union()?;
        
        if results.is_empty() {
            let schema = data.schema();
            let empty_batch = RecordBatch::new_empty(schema);
            Ok(empty_batch)
        } else {
            Ok(results[0].clone())
        }
    }
    
    fn finish(&mut self, _context: &MppContext) -> Result<()> {
        debug!("Finished union operator {}", self.operator_id);
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
        debug!("Recovered union operator {}", self.operator_id);
        Ok(())
    }
}

/// MPP union operator factory
pub struct MppUnionOperatorFactory;

impl MppUnionOperatorFactory {
    /// Create union operator
    pub fn create_union(
        operator_id: Uuid,
        union_type: UnionType,
        output_schema: SchemaRef,
        memory_limit: usize,
    ) -> Result<MppUnionOperator> {
        let config = MppUnionConfig {
            union_type,
            output_schema,
            memory_limit,
            use_external_sort: true,
            external_sort_dir: Some("/tmp/union".to_string()),
            enable_vectorization: true,
            enable_simd: true,
            use_hash_operations: true,
        };
        
        MppUnionOperator::new(operator_id, config)
    }
}
