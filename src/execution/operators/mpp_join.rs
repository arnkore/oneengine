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

//! MPP join operators
//! 
//! Handles distributed joins in MPP execution

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

/// Join type
#[derive(Debug, Clone, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Semi,
    Anti,
}

/// Join condition
#[derive(Debug, Clone)]
pub struct JoinCondition {
    /// Left side columns
    pub left_columns: Vec<String>,
    /// Right side columns
    pub right_columns: Vec<String>,
    /// Join type
    pub join_type: JoinType,
}

/// Join key
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct JoinKey {
    /// Key values
    pub values: Vec<serde_json::Value>,
}

/// MPP hash join operator
pub struct MppHashJoinOperator {
    /// Operator ID
    operator_id: Uuid,
    /// Join condition
    join_condition: JoinCondition,
    /// Hash table for build side
    hash_table: HashMap<JoinKey, Vec<RecordBatch>>,
    /// Statistics
    stats: JoinStats,
    /// Memory usage
    memory_usage: usize,
    /// Memory limit
    memory_limit: usize,
    /// Whether the operator is finished
    finished: bool,
}

/// Join statistics
#[derive(Debug, Clone, Default)]
pub struct JoinStats {
    /// Rows processed from left side
    pub left_rows_processed: u64,
    /// Rows processed from right side
    pub right_rows_processed: u64,
    /// Rows produced
    pub rows_produced: u64,
    /// Hash table size
    pub hash_table_size: usize,
    /// Memory usage
    pub memory_usage: usize,
    /// Processing time
    pub processing_time: std::time::Duration,
    /// Build time
    pub build_time: std::time::Duration,
    /// Probe time
    pub probe_time: std::time::Duration,
}

impl MppHashJoinOperator {
    pub fn new(operator_id: Uuid, join_condition: JoinCondition, memory_limit: usize) -> Self {
        Self {
            operator_id,
            join_condition,
            hash_table: HashMap::new(),
            stats: JoinStats::default(),
            memory_usage: 0,
            memory_limit,
            finished: false,
        }
    }
    
    /// Build hash table from build side data
    pub fn build_hash_table(&mut self, batch: RecordBatch) -> Result<()> {
        let start = std::time::Instant::now();
        
        for row_idx in 0..batch.num_rows() {
            let join_key = self.extract_join_key(&batch, row_idx, true)?;
            let entry = self.hash_table.entry(join_key).or_insert_with(Vec::new);
            entry.push(batch.slice(row_idx, 1));
        }
        
        self.stats.left_rows_processed += batch.num_rows() as u64;
        self.stats.hash_table_size = self.hash_table.len();
        self.stats.build_time += start.elapsed();
        
        debug!("Built hash table with {} entries", self.hash_table.len());
        Ok(())
    }
    
    /// Probe hash table with probe side data
    pub fn probe_hash_table(&mut self, batch: RecordBatch) -> Result<Vec<RecordBatch>> {
        let start = std::time::Instant::now();
        let mut results = Vec::new();
        
        for row_idx in 0..batch.num_rows() {
            let join_key = self.extract_join_key(&batch, row_idx, false)?;
            
            if let Some(matching_rows) = self.hash_table.get(&join_key) {
                for matching_row in matching_rows {
                    let joined_batch = self.join_rows(&batch, row_idx, matching_row)?;
                    results.push(joined_batch);
                }
            } else if self.join_condition.join_type == JoinType::Left {
                // Left join: include left side even if no match
                let left_only_batch = self.create_left_only_batch(&batch, row_idx)?;
                results.push(left_only_batch);
            }
        }
        
        self.stats.right_rows_processed += batch.num_rows() as u64;
        self.stats.rows_produced += results.len() as u64;
        self.stats.probe_time += start.elapsed();
        
        Ok(results)
    }
    
    /// Extract join key from row
    fn extract_join_key(&self, batch: &RecordBatch, row_idx: usize, is_left: bool) -> Result<JoinKey> {
        let columns = if is_left {
            &self.join_condition.left_columns
        } else {
            &self.join_condition.right_columns
        };
        
        let mut values = Vec::new();
        for column_name in columns {
            if let Some(column) = batch.column_by_name(column_name) {
                let value = self.get_column_value(column, row_idx)?;
                values.push(value);
            }
        }
        
        Ok(JoinKey { values })
    }
    
    /// Join two rows
    fn join_rows(&self, right_batch: &RecordBatch, right_row_idx: usize, left_row: &RecordBatch) -> Result<RecordBatch> {
        // Combine left and right row data
        let mut left_columns = left_row.columns().to_vec();
        let mut right_columns = right_batch.columns().to_vec();
        
        // For simplicity, we'll concatenate the columns
        // In a real implementation, you'd handle schema merging properly
        left_columns.extend(right_columns);
        
        let joined_batch = RecordBatch::try_new(
            right_batch.schema(), // This should be the joined schema
            left_columns,
        ).map_err(|e| anyhow::anyhow!("Failed to create joined batch: {}", e))?;
        
        Ok(joined_batch)
    }
    
    /// Create left-only batch for left join
    fn create_left_only_batch(&self, right_batch: &RecordBatch, right_row_idx: usize) -> Result<RecordBatch> {
        // Create a batch with null values for right side
        // This is a simplified implementation
        Ok(right_batch.slice(right_row_idx, 1))
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
            arrow::datatypes::DataType::Utf8 => {
                let array = column.as_any().downcast_ref::<StringArray>().unwrap();
                Value::String(array.value(row_idx).to_string())
            }
            _ => return Err(anyhow::anyhow!("Unsupported column type for join")),
        };
        
        Ok(value)
    }
    
    /// Get join statistics
    pub fn get_stats(&self) -> &JoinStats {
        &self.stats
    }
    
    /// Reset operator state
    pub fn reset(&mut self) {
        self.hash_table.clear();
        self.stats = JoinStats::default();
        self.memory_usage = 0;
    }
}

/// MPP nested loop join operator
pub struct MppNestedLoopJoinOperator {
    /// Operator ID
    operator_id: Uuid,
    /// Join condition
    join_condition: JoinCondition,
    /// Left side data
    left_data: Vec<RecordBatch>,
    /// Right side data
    right_data: Vec<RecordBatch>,
    /// Statistics
    stats: JoinStats,
    /// Whether the operator is finished
    finished: bool,
}

impl MppNestedLoopJoinOperator {
    pub fn new(operator_id: Uuid, join_condition: JoinCondition) -> Self {
        Self {
            operator_id,
            join_condition,
            left_data: Vec::new(),
            right_data: Vec::new(),
            stats: JoinStats::default(),
            finished: false,
        }
    }
    
    /// Add left side data
    pub fn add_left_data(&mut self, batch: RecordBatch) {
        self.left_data.push(batch);
    }
    
    /// Add right side data
    pub fn add_right_data(&mut self, batch: RecordBatch) {
        self.right_data.push(batch);
    }
    
    /// Execute nested loop join
    pub fn execute_join(&mut self) -> Result<Vec<RecordBatch>> {
        let start = std::time::Instant::now();
        let mut results = Vec::new();
        
        for left_batch in &self.left_data {
            for right_batch in &self.right_data {
                for left_row_idx in 0..left_batch.num_rows() {
                    for right_row_idx in 0..right_batch.num_rows() {
                        if self.matches_condition(left_batch, left_row_idx, right_batch, right_row_idx)? {
                            let joined_batch = self.join_rows(left_batch, left_row_idx, right_batch, right_row_idx)?;
                            results.push(joined_batch);
                        }
                    }
                }
            }
        }
        
        self.stats.processing_time += start.elapsed();
        self.stats.rows_produced = results.len() as u64;
        
        Ok(results)
    }
    
    /// Check if rows match join condition
    fn matches_condition(&self, left_batch: &RecordBatch, left_row_idx: usize, right_batch: &RecordBatch, right_row_idx: usize) -> Result<bool> {
        // Simple equality check for now
        // In a real implementation, you'd support complex conditions
        for (left_col, right_col) in self.join_condition.left_columns.iter().zip(self.join_condition.right_columns.iter()) {
            let left_value = self.get_column_value(left_batch.column_by_name(left_col).unwrap(), left_row_idx)?;
            let right_value = self.get_column_value(right_batch.column_by_name(right_col).unwrap(), right_row_idx)?;
            
            if left_value != right_value {
                return Ok(false);
            }
        }
        Ok(true)
    }
    
    /// Join two specific rows
    fn join_rows(&self, left_batch: &RecordBatch, left_row_idx: usize, right_batch: &RecordBatch, right_row_idx: usize) -> Result<RecordBatch> {
        // Simplified row joining
        // In a real implementation, you'd properly merge schemas and data
        Ok(left_batch.slice(left_row_idx, 1))
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
            arrow::datatypes::DataType::Utf8 => {
                let array = column.as_any().downcast_ref::<StringArray>().unwrap();
                Value::String(array.value(row_idx).to_string())
            }
            _ => return Err(anyhow::anyhow!("Unsupported column type for join")),
        };
        
        Ok(value)
    }
    
    /// Get join statistics
    pub fn get_stats(&self) -> &JoinStats {
        &self.stats
    }
}

/// MPP join operator factory
pub struct MppJoinOperatorFactory;

impl MppJoinOperatorFactory {
    /// Create hash join operator
    pub fn create_hash_join(
        operator_id: Uuid,
        join_condition: JoinCondition,
        memory_limit: usize,
    ) -> MppHashJoinOperator {
        MppHashJoinOperator::new(operator_id, join_condition, memory_limit)
    }
    
    /// Create nested loop join operator
    pub fn create_nested_loop_join(
        operator_id: Uuid,
        join_condition: JoinCondition,
    ) -> MppNestedLoopJoinOperator {
        MppNestedLoopJoinOperator::new(operator_id, join_condition)
    }
}

impl MppOperator for MppHashJoinOperator {
    fn initialize(&mut self, _context: &MppContext) -> Result<()> {
        Ok(())
    }

    fn process_batch(&mut self, batch: RecordBatch, _context: &MppContext) -> Result<Vec<RecordBatch>> {
        // Simple pass-through for now
        Ok(vec![batch])
    }

    fn exchange_data(&mut self, _data: Vec<RecordBatch>, _target_workers: Vec<WorkerId>) -> Result<()> {
        Ok(())
    }

    fn process_partition(&mut self, _partition_id: PartitionId, data: RecordBatch) -> Result<RecordBatch> {
        // Simple pass-through for now
        Ok(data)
    }

    fn finish(&mut self, _context: &MppContext) -> Result<()> {
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
        Ok(())
    }
}

impl Operator for MppHashJoinOperator {
    fn on_event(&mut self, ev: Event, out: &mut Outbox) -> OpStatus {
        match ev {
            Event::Data { batch, .. } => {
                // 处理连接数据
                match self.process_join_batch(&batch) {
                    Ok(Some(result_batch)) => {
                        if let Err(e) = out.push(0, result_batch) {
                            error!("Failed to push join result: {}", e);
                            return OpStatus::Error(format!("Failed to push join result: {}", e));
                        }
                        OpStatus::HasMore
                    }
                    Ok(None) => OpStatus::Ready,
                    Err(e) => {
                        error!("Join processing error: {}", e);
                        OpStatus::Error(format!("Join processing error: {}", e))
                    }
                }
            }
            Event::Finish(_) => {
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
        "MppHashJoinOperator"
    }
}

impl MppHashJoinOperator {
    /// 处理连接批次
    fn process_join_batch(&mut self, batch: &RecordBatch) -> Result<Option<RecordBatch>> {
        // 简化实现：直接返回原批次
        Ok(Some(batch.clone()))
    }
}

impl Operator for MppNestedLoopJoinOperator {
    fn on_event(&mut self, ev: Event, out: &mut Outbox) -> OpStatus {
        match ev {
            Event::Data { batch, .. } => {
                // 根据端口号决定是左输入还是右输入
                // 这里简化处理，假设端口0是左输入，端口1是右输入
                if let Err(e) = self.add_left_data(batch) {
                    error!("Failed to add left data for nested loop join: {}", e);
                    return OpStatus::Error(format!("Failed to add left data for nested loop join: {}", e));
                }
                
                // 检查是否可以执行嵌套循环连接
                if !self.left_data.is_empty() && !self.right_data.is_empty() {
                    match self.process_batch() {
                        Ok(join_batches) => {
                            for batch in join_batches {
                                if let Err(e) = out.push(0, batch) {
                                    error!("Failed to push nested loop join batch: {}", e);
                                    return OpStatus::Error(format!("Failed to push nested loop join batch: {}", e));
                                }
                            }
                        }
                        Err(e) => {
                            error!("Failed to perform nested loop join: {}", e);
                            return OpStatus::Error(format!("Failed to perform nested loop join: {}", e));
                        }
                    }
                }
                OpStatus::HasMore
            }
            Event::Finish(_) => {
                // 输出所有剩余的连接数据
                match self.process_batch() {
                    Ok(final_batches) => {
                        for batch in final_batches {
                            if let Err(e) = out.push(0, batch) {
                                error!("Failed to push final nested loop join batch: {}", e);
                                return OpStatus::Error(format!("Failed to push final nested loop join batch: {}", e));
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to finalize nested loop join: {}", e);
                        return OpStatus::Error(format!("Failed to finalize nested loop join: {}", e));
                    }
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
        "MppNestedLoopJoinOperator"
    }
}
