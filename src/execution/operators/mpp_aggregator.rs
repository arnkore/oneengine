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

//! MPP aggregation operators
//! 
//! Handles distributed aggregation in MPP execution

use std::sync::Arc;
use std::collections::HashMap;
use anyhow::Result;
use arrow::record_batch::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow::array::*;
use serde::{Serialize, Deserialize};
use uuid::Uuid;
use tracing::{debug, warn, error};

/// Worker node identifier
pub type WorkerId = String;

/// Aggregation function type
#[derive(Debug, Clone, PartialEq)]
pub enum AggregationFunction {
    Sum,
    Count,
    Avg,
    Min,
    Max,
    CountDistinct,
    StdDev,
    Variance,
}

/// Aggregation column specification
#[derive(Debug, Clone)]
pub struct AggregationColumn {
    /// Column name
    pub column_name: String,
    /// Aggregation function
    pub function: AggregationFunction,
    /// Output column name
    pub output_name: String,
    /// Whether to include nulls
    pub include_nulls: bool,
}

/// Group by column specification
#[derive(Debug, Clone)]
pub struct GroupByColumn {
    /// Column name
    pub column_name: String,
    /// Output column name
    pub output_name: String,
}

/// MPP aggregation configuration
#[derive(Debug, Clone)]
pub struct MppAggregationConfig {
    /// Group by columns
    pub group_by_columns: Vec<GroupByColumn>,
    /// Aggregation columns
    pub aggregation_columns: Vec<AggregationColumn>,
    /// Output schema
    pub output_schema: SchemaRef,
    /// Whether this is a final aggregation (after partial aggregation)
    pub is_final: bool,
    /// Memory limit for hash table
    pub memory_limit: usize,
    /// Spill threshold
    pub spill_threshold: f64,
}

/// MPP aggregation operator
pub struct MppAggregationOperator {
    /// Operator ID
    operator_id: Uuid,
    /// Configuration
    config: MppAggregationConfig,
    /// Hash table for grouping
    hash_table: HashMap<GroupKey, AggregationState>,
    /// Statistics
    stats: AggregationStats,
    /// Memory usage
    memory_usage: usize,
}

/// Group key for hash table
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GroupKey {
    /// Key values
    pub values: Vec<serde_json::Value>,
}

/// Aggregation state for a group
#[derive(Debug, Clone)]
pub struct AggregationState {
    /// Count of values
    pub count: u64,
    /// Sum for numeric aggregations
    pub sum: Option<f64>,
    /// Min value
    pub min: Option<serde_json::Value>,
    /// Max value
    pub max: Option<serde_json::Value>,
    /// Distinct values for count distinct
    pub distinct_values: Option<std::collections::HashSet<serde_json::Value>>,
    /// Sum of squares for variance/stddev
    pub sum_squares: Option<f64>,
}

impl AggregationState {
    pub fn new() -> Self {
        Self {
            count: 0,
            sum: None,
            min: None,
            max: None,
            distinct_values: None,
            sum_squares: None,
        }
    }
    
    /// Update state with new value
    pub fn update(&mut self, value: serde_json::Value, agg_col: &AggregationColumn) {
        self.count += 1;
        
        match agg_col.function {
            AggregationFunction::Sum | AggregationFunction::Avg => {
                if let Some(num) = self.value_to_f64(&value) {
                    self.sum = Some(self.sum.unwrap_or(0.0) + num);
                }
            }
            AggregationFunction::Min => {
                if self.min.is_none() || value < self.min.as_ref().unwrap() {
                    self.min = Some(value.clone());
                }
            }
            AggregationFunction::Max => {
                if self.max.is_none() || value > self.max.as_ref().unwrap() {
                    self.max = Some(value.clone());
                }
            }
            AggregationFunction::CountDistinct => {
                if self.distinct_values.is_none() {
                    self.distinct_values = Some(std::collections::HashSet::new());
                }
                self.distinct_values.as_mut().unwrap().insert(value);
            }
            AggregationFunction::StdDev | AggregationFunction::Variance => {
                if let Some(num) = self.value_to_f64(&value) {
                    self.sum = Some(self.sum.unwrap_or(0.0) + num);
                    self.sum_squares = Some(self.sum_squares.unwrap_or(0.0) + num * num);
                }
            }
            _ => {} // Count is handled by count field
        }
    }
    
    fn value_to_f64(&self, value: &serde_json::Value) -> Option<f64> {
        match value {
            serde_json::Value::Number(n) => n.as_f64(),
            _ => None,
        }
    }
    
    /// Get final result for a group
    pub fn get_result(&self, agg_col: &AggregationColumn) -> Option<serde_json::Value> {
        match agg_col.function {
            AggregationFunction::Count => Some(serde_json::Value::Number(self.count.into())),
            AggregationFunction::Sum => self.sum.map(|s| serde_json::Value::Number(serde_json::Number::from_f64(s).unwrap())),
            AggregationFunction::Avg => {
                if self.count > 0 {
                    self.sum.map(|s| serde_json::Value::Number(serde_json::Number::from_f64(s / self.count as f64).unwrap()))
                } else {
                    None
                }
            }
            AggregationFunction::Min => self.min.clone(),
            AggregationFunction::Max => self.max.clone(),
            AggregationFunction::CountDistinct => {
                self.distinct_values.as_ref().map(|v| serde_json::Value::Number(v.len().into()))
            }
            AggregationFunction::Variance => {
                if self.count > 1 {
                    let mean = self.sum.unwrap_or(0.0) / self.count as f64;
                    let variance = (self.sum_squares.unwrap_or(0.0) / self.count as f64) - (mean * mean);
                    Some(serde_json::Value::Number(serde_json::Number::from_f64(variance).unwrap()))
                } else {
                    None
                }
            }
            AggregationFunction::StdDev => {
                if self.count > 1 {
                    let mean = self.sum.unwrap_or(0.0) / self.count as f64;
                    let variance = (self.sum_squares.unwrap_or(0.0) / self.count as f64) - (mean * mean);
                    let stddev = variance.sqrt();
                    Some(serde_json::Value::Number(serde_json::Number::from_f64(stddev).unwrap()))
                } else {
                    None
                }
            }
        }
    }
}

/// Aggregation statistics
#[derive(Debug, Clone, Default)]
pub struct AggregationStats {
    /// Rows processed
    pub rows_processed: u64,
    /// Groups created
    pub groups_created: u64,
    /// Memory usage (bytes)
    pub memory_usage: usize,
    /// Spill count
    pub spill_count: u64,
    /// Processing time
    pub processing_time: std::time::Duration,
}

impl MppAggregationOperator {
    pub fn new(operator_id: Uuid, config: MppAggregationConfig) -> Self {
        Self {
            operator_id,
            config,
            hash_table: HashMap::new(),
            stats: AggregationStats::default(),
            memory_usage: 0,
        }
    }
    
    /// Process batch for aggregation
    pub fn process_batch(&mut self, batch: RecordBatch) -> Result<Vec<RecordBatch>> {
        let start = std::time::Instant::now();
        
        // Process each row in the batch
        for row_idx in 0..batch.num_rows() {
            let group_key = self.extract_group_key(&batch, row_idx)?;
            let agg_values = self.extract_aggregation_values(&batch, row_idx)?;
            
            // Update or create aggregation state
            let state = self.hash_table.entry(group_key).or_insert_with(AggregationState::new);
            
            // Update state with aggregation values
            for (i, value) in agg_values.into_iter().enumerate() {
                if i < self.config.aggregation_columns.len() {
                    state.update(value, &self.config.aggregation_columns[i]);
                }
            }
        }
        
        self.stats.rows_processed += batch.num_rows() as u64;
        self.stats.groups_created = self.hash_table.len() as u64;
        self.stats.processing_time += start.elapsed();
        
        // Check if we need to spill
        if self.should_spill() {
            self.spill()?;
        }
        
        Ok(vec![])
    }
    
    /// Extract group key from row
    fn extract_group_key(&self, batch: &RecordBatch, row_idx: usize) -> Result<GroupKey> {
        let mut values = Vec::new();
        
        for group_col in &self.config.group_by_columns {
            if let Some(column) = batch.column_by_name(&group_col.column_name) {
                let value = self.get_column_value(column, row_idx)?;
                values.push(value);
            }
        }
        
        Ok(GroupKey { values })
    }
    
    /// Extract aggregation values from row
    fn extract_aggregation_values(&self, batch: &RecordBatch, row_idx: usize) -> Result<Vec<serde_json::Value>> {
        let mut values = Vec::new();
        
        for agg_col in &self.config.aggregation_columns {
            if let Some(column) = batch.column_by_name(&agg_col.column_name) {
                let value = self.get_column_value(column, row_idx)?;
                values.push(value);
            }
        }
        
        Ok(values)
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
            _ => return Err(anyhow::anyhow!("Unsupported column type for aggregation")),
        };
        
        Ok(value)
    }
    
    /// Check if we should spill to disk
    fn should_spill(&self) -> bool {
        self.memory_usage as f64 > self.config.memory_limit as f64 * self.config.spill_threshold
    }
    
    /// Spill hash table to disk
    fn spill(&mut self) -> Result<()> {
        // In a real implementation, this would write the hash table to disk
        // and clear it from memory
        warn!("Spilling aggregation hash table to disk");
        self.stats.spill_count += 1;
        self.hash_table.clear();
        self.memory_usage = 0;
        Ok(())
    }
    
    /// Finalize aggregation and produce results
    pub fn finalize(&mut self) -> Result<RecordBatch> {
        let start = std::time::Instant::now();
        
        // Build result batch
        let mut group_columns = Vec::new();
        let mut agg_columns = Vec::new();
        
        // Add group by columns
        for group_col in &self.config.group_by_columns {
            let mut values = Vec::new();
            for (group_key, _) in &self.hash_table {
                if let Some(value) = group_key.values.get(0) {
                    values.push(Some(value.clone()));
                }
            }
            // Convert to Arrow array (simplified)
            let array = StringArray::from(values);
            group_columns.push(Arc::new(array) as Arc<dyn Array>);
        }
        
        // Add aggregation columns
        for agg_col in &self.config.aggregation_columns {
            let mut values = Vec::new();
            for (_, state) in &self.hash_table {
                if let Some(value) = state.get_result(agg_col) {
                    values.push(Some(value.to_string()));
                } else {
                    values.push(None);
                }
            }
            // Convert to Arrow array (simplified)
            let array = StringArray::from(values);
            agg_columns.push(Arc::new(array) as Arc<dyn Array>);
        }
        
        let mut all_columns = group_columns;
        all_columns.extend(agg_columns);
        
        let result_batch = RecordBatch::try_new(self.config.output_schema.clone(), all_columns)
            .map_err(|e| anyhow::anyhow!("Failed to create result batch: {}", e))?;
        
        self.stats.processing_time += start.elapsed();
        
        debug!("Aggregation finalized: {} groups, {} rows", 
               self.hash_table.len(), result_batch.num_rows());
        
        Ok(result_batch)
    }
    
    /// Get aggregation statistics
    pub fn get_stats(&self) -> &AggregationStats {
        &self.stats
    }
    
    /// Reset operator state
    pub fn reset(&mut self) {
        self.hash_table.clear();
        self.stats = AggregationStats::default();
        self.memory_usage = 0;
    }
}

/// MPP aggregation operator factory
pub struct MppAggregationOperatorFactory;

impl MppAggregationOperatorFactory {
    /// Create local aggregation operator
    pub fn create_local_aggregation(
        operator_id: Uuid,
        group_by_columns: Vec<GroupByColumn>,
        aggregation_columns: Vec<AggregationColumn>,
        output_schema: SchemaRef,
    ) -> MppAggregationOperator {
        let config = MppAggregationConfig {
            group_by_columns,
            aggregation_columns,
            output_schema,
            is_final: false,
            memory_limit: 1024 * 1024 * 1024, // 1GB
            spill_threshold: 0.8,
        };
        
        MppAggregationOperator::new(operator_id, config)
    }
    
    /// Create final aggregation operator
    pub fn create_final_aggregation(
        operator_id: Uuid,
        group_by_columns: Vec<GroupByColumn>,
        aggregation_columns: Vec<AggregationColumn>,
        output_schema: SchemaRef,
    ) -> MppAggregationOperator {
        let config = MppAggregationConfig {
            group_by_columns,
            aggregation_columns,
            output_schema,
            is_final: true,
            memory_limit: 1024 * 1024 * 1024, // 1GB
            spill_threshold: 0.8,
        };
        
        MppAggregationOperator::new(operator_id, config)
    }
}
