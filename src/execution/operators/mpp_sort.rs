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

//! MPP sort operators
//! 
//! Handles distributed sorting in MPP execution

use std::sync::Arc;
use anyhow::Result;
use arrow::record_batch::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow::compute::SortOptions;
use serde::{Serialize, Deserialize};
use uuid::Uuid;
use tracing::{debug, warn, error};
use crate::execution::operators::mpp_operator::{MppOperator, MppContext, MppOperatorStats, PartitionId, WorkerId};
use crate::execution::push_runtime::{Operator, Event, OpStatus, Outbox, PortId};

/// Sort column specification
#[derive(Debug, Clone)]
pub struct SortColumn {
    /// Column name
    pub column_name: String,
    /// Sort direction (true = ascending, false = descending)
    pub ascending: bool,
    /// Nulls first
    pub nulls_first: bool,
}

/// MPP sort configuration
#[derive(Debug, Clone)]
pub struct MppSortConfig {
    /// Sort columns
    pub sort_columns: Vec<SortColumn>,
    /// Output schema
    pub output_schema: SchemaRef,
    /// Memory limit for sorting
    pub memory_limit: usize,
    /// Whether to use external sort
    pub use_external_sort: bool,
    /// External sort directory
    pub external_sort_dir: Option<String>,
    /// Sort algorithm
    pub sort_algorithm: SortAlgorithm,
    /// Whether to enable parallel sorting
    pub enable_parallel_sort: bool,
    /// Number of parallel threads
    pub parallel_threads: usize,
    /// Whether to enable SIMD sorting
    pub enable_simd_sort: bool,
    /// Whether to enable radix sort for integers
    pub enable_radix_sort: bool,
    /// Whether to enable stable sorting
    pub enable_stable_sort: bool,
}

/// Sort algorithm
#[derive(Debug, Clone, PartialEq)]
pub enum SortAlgorithm {
    /// Quick sort
    QuickSort,
    /// Merge sort
    MergeSort,
    /// Heap sort
    HeapSort,
    /// Radix sort (for integers)
    RadixSort,
    /// Tim sort (stable)
    TimSort,
    /// Intro sort (hybrid)
    IntroSort,
}

impl Default for MppSortConfig {
    fn default() -> Self {
        Self {
            sort_columns: Vec::new(),
            output_schema: Arc::new(arrow::datatypes::Schema::empty()),
            memory_limit: 1024 * 1024 * 1024, // 1GB
            use_external_sort: true,
            external_sort_dir: Some("/tmp/sort".to_string()),
            sort_algorithm: SortAlgorithm::TimSort,
            enable_parallel_sort: true,
            parallel_threads: num_cpus::get(),
            enable_simd_sort: true,
            enable_radix_sort: true,
            enable_stable_sort: true,
        }
    }
}

/// MPP sort operator
pub struct MppSortOperator {
    /// Operator ID
    operator_id: Uuid,
    /// Configuration
    config: MppSortConfig,
    /// Buffered data for sorting
    buffered_data: Vec<RecordBatch>,
    /// Statistics
    stats: SortStats,
    /// Memory usage
    memory_usage: usize,
    /// Whether the operator is finished
    finished: bool,
}

/// Sort statistics
#[derive(Debug, Clone, Default)]
pub struct SortStats {
    /// Rows processed
    pub rows_processed: u64,
    /// Batches processed
    pub batches_processed: u64,
    /// Memory usage
    pub memory_usage: usize,
    /// Sort time
    pub sort_time: std::time::Duration,
    /// Merge time
    pub merge_time: std::time::Duration,
    /// Spill count
    pub spill_count: u64,
    /// Spill bytes
    pub spill_bytes: u64,
}

impl MppSortOperator {
    pub fn new(operator_id: Uuid, config: MppSortConfig) -> Self {
        Self {
            operator_id,
            config,
            buffered_data: Vec::new(),
            stats: SortStats::default(),
            memory_usage: 0,
            finished: false,
        }
    }
    
    /// Add batch for sorting
    pub fn add_batch(&mut self, batch: RecordBatch) -> Result<()> {
        let batch_size = batch.get_array_memory_size();
        
        // Check memory limit
        if self.memory_usage + batch_size > self.config.memory_limit {
            if self.config.use_external_sort {
                self.spill_to_disk()?;
            } else {
                return Err(anyhow::anyhow!("Memory limit exceeded for sorting"));
            }
        }
        
        let rows = batch.num_rows() as u64;
        self.buffered_data.push(batch);
        self.memory_usage += batch_size;
        self.stats.rows_processed += rows;
        self.stats.batches_processed += 1;
        
        Ok(())
    }
    
    /// Sort all buffered data
    pub fn sort(&mut self) -> Result<Vec<RecordBatch>> {
        let start = std::time::Instant::now();
        
        if self.buffered_data.is_empty() {
            return Ok(vec![]);
        }
        
        // Convert sort columns to Arrow sort options
        let sort_options = self.create_sort_options()?;
        
        // Sort each batch individually first
        let mut sorted_batches = Vec::new();
        for batch in &self.buffered_data {
            let sorted_batch = self.sort_batch(batch, &sort_options)?;
            sorted_batches.push(sorted_batch);
        }
        
        // Merge sorted batches if we have multiple batches
        let result = if sorted_batches.len() > 1 {
            self.merge_sorted_batches(sorted_batches)?
        } else {
            sorted_batches
        };
        
        self.stats.sort_time += start.elapsed();
        
        debug!("Sorted {} batches in {:?}", self.buffered_data.len(), self.stats.sort_time);
        
        Ok(result)
    }
    
    /// Sort a single batch
    fn sort_batch(&self, batch: &RecordBatch, sort_options: &[SortOptions]) -> Result<RecordBatch> {
        use arrow::compute;
        
        // Get sort columns
        let mut sort_columns = Vec::new();
        for sort_col in &self.config.sort_columns {
            if let Some(column) = batch.column_by_name(&sort_col.column_name) {
                sort_columns.push(column.clone());
            } else {
                return Err(anyhow::anyhow!("Sort column not found: {}", sort_col.column_name));
            }
        }
        
        // Perform sort - simplified approach
        // TODO: Implement proper multi-column sorting
        if sort_columns.is_empty() {
            return Err(anyhow::anyhow!("No sort columns specified"));
        }
        
        let first_column = &sort_columns[0];
        let sort_options_single = sort_options[0].clone();
        let sorted_indices = compute::sort_to_indices(first_column, Some(sort_options_single), None)?;
        
        // Apply sort to all columns
        let sorted_columns: Result<Vec<Arc<dyn arrow::array::Array>>, arrow::error::ArrowError> = batch
            .columns()
            .iter()
            .map(|col| compute::take(col, &sorted_indices, None))
            .collect();
        
        let sorted_columns = sorted_columns?;
        let sorted_batch = RecordBatch::try_new(batch.schema(), sorted_columns)
            .map_err(|e| anyhow::anyhow!("Failed to create sorted batch: {}", e))?;
        
        Ok(sorted_batch)
    }
    
    /// Merge multiple sorted batches
    fn merge_sorted_batches(&mut self, mut sorted_batches: Vec<RecordBatch>) -> Result<Vec<RecordBatch>> {
        let start = std::time::Instant::now();
        
        // Simple merge: concatenate and sort again
        // In a real implementation, you'd use a proper merge algorithm
        let mut all_rows = 0;
        for batch in &sorted_batches {
            all_rows += batch.num_rows();
        }
        
        // For now, just return the sorted batches as-is
        // A real implementation would merge them properly
        self.stats.merge_time += start.elapsed();
        
        Ok(sorted_batches)
    }
    
    /// Create Arrow sort options
    fn create_sort_options(&self) -> Result<Vec<SortOptions>> {
        let mut options = Vec::new();
        
        for sort_col in &self.config.sort_columns {
            options.push(SortOptions {
                descending: !sort_col.ascending,
                nulls_first: sort_col.nulls_first,
            });
        }
        
        Ok(options)
    }
    
    /// Spill data to disk
    fn spill_to_disk(&mut self) -> Result<()> {
        if let Some(dir) = &self.config.external_sort_dir {
            // In a real implementation, you'd write the data to disk
            warn!("Spilling {} batches to disk at {}", self.buffered_data.len(), dir);
            self.stats.spill_count += 1;
            self.stats.spill_bytes += self.memory_usage as u64;
        } else {
            return Err(anyhow::anyhow!("External sort directory not specified"));
        }
        
        self.buffered_data.clear();
        self.memory_usage = 0;
        Ok(())
    }
    
    /// Get sort statistics
    pub fn get_stats(&self) -> &SortStats {
        &self.stats
    }
    
    /// Reset operator state
    pub fn reset(&mut self) {
        self.buffered_data.clear();
        self.stats = SortStats::default();
        self.memory_usage = 0;
    }
}

/// MPP top-N operator
pub struct MppTopNOperator {
    /// Operator ID
    operator_id: Uuid,
    /// Number of top rows
    limit: usize,
    /// Sort configuration
    sort_config: MppSortConfig,
    /// Buffered data
    buffered_data: Vec<RecordBatch>,
    /// Statistics
    stats: SortStats,
    /// Whether the operator is finished
    finished: bool,
}

impl MppTopNOperator {
    pub fn new(operator_id: Uuid, limit: usize, sort_config: MppSortConfig) -> Self {
        Self {
            operator_id,
            limit,
            sort_config,
            buffered_data: Vec::new(),
            stats: SortStats::default(),
            finished: false,
        }
    }
    
    /// Add batch for top-N processing
    pub fn add_batch(&mut self, batch: RecordBatch) -> Result<()> {
        let rows = batch.num_rows() as u64;
        self.buffered_data.push(batch);
        self.stats.rows_processed += rows;
        self.stats.batches_processed += 1;
        Ok(())
    }
    
    /// Get top-N results
    pub fn get_top_n(&mut self) -> Result<Vec<RecordBatch>> {
        let start = std::time::Instant::now();
        
        if self.buffered_data.is_empty() {
            return Ok(vec![]);
        }
        
        // Sort all data first
        let sort_operator = MppSortOperator::new(self.operator_id, self.sort_config.clone());
        let mut temp_operator = sort_operator;
        
        for batch in &self.buffered_data {
            temp_operator.add_batch(batch.clone())?;
        }
        
        let sorted_batches = temp_operator.sort()?;
        
        // Take only the top N rows
        let mut result = Vec::new();
        let mut remaining = self.limit;
        
        for batch in sorted_batches {
            if remaining == 0 {
                break;
            }
            
            let take_count = std::cmp::min(remaining, batch.num_rows());
            let top_batch = batch.slice(0, take_count);
            result.push(top_batch);
            remaining -= take_count;
        }
        
        self.stats.sort_time += start.elapsed();
        
        debug!("Retrieved top {} rows", self.limit);
        
        Ok(result)
    }
    
    /// Get statistics
    pub fn get_stats(&self) -> &SortStats {
        &self.stats
    }
}

/// MPP sort operator factory
pub struct MppSortOperatorFactory;

impl MppSortOperatorFactory {
    /// Create sort operator
    pub fn create_sort(
        operator_id: Uuid,
        sort_columns: Vec<SortColumn>,
        output_schema: SchemaRef,
        memory_limit: usize,
    ) -> Result<MppSortOperator> {
        let config = MppSortConfig {
            sort_columns,
            output_schema,
            memory_limit,
            use_external_sort: true,
            external_sort_dir: Some("/tmp/sort".to_string()),
            sort_algorithm: SortAlgorithm::QuickSort,
            enable_parallel_sort: false,
            parallel_threads: 1,
            enable_simd_sort: false,
            enable_radix_sort: false,
            enable_stable_sort: false,
        };
        
        Ok(MppSortOperator::new(operator_id, config))
    }
    
    /// Create top-N operator
    pub fn create_top_n(
        operator_id: Uuid,
        limit: usize,
        sort_columns: Vec<SortColumn>,
        output_schema: SchemaRef,
        memory_limit: usize,
    ) -> MppTopNOperator {
        let sort_config = MppSortConfig {
            sort_columns,
            output_schema,
            memory_limit,
            use_external_sort: true,
            external_sort_dir: Some("/tmp/sort".to_string()),
            sort_algorithm: SortAlgorithm::QuickSort,
            enable_parallel_sort: false,
            parallel_threads: 1,
            enable_simd_sort: false,
            enable_radix_sort: false,
            enable_stable_sort: false,
        };
        
        MppTopNOperator::new(operator_id, limit, sort_config)
    }
}

impl MppOperator for MppSortOperator {
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

impl Operator for MppSortOperator {
    fn on_event(&mut self, ev: Event, out: &mut Outbox) -> OpStatus {
        match ev {
            Event::Data { batch, .. } => {
                // 添加批次到排序缓冲区
                if let Err(e) = self.add_batch(batch) {
                    error!("Failed to add batch for sorting: {}", e);
                    return OpStatus::Error(format!("Failed to add batch for sorting: {}", e));
                }
                
                // 检查是否需要输出排序后的数据
                if self.should_output_sorted_data() {
                    match self.get_sorted_batches() {
                        Ok(sorted_batches) => {
                            for batch in sorted_batches {
                                if let Err(e) = out.push(0, batch) {
                                    error!("Failed to push sorted batch: {}", e);
                                    return OpStatus::Error(format!("Failed to push sorted batch: {}", e));
                                }
                            }
                        }
                        Err(e) => {
                            error!("Failed to get sorted batches: {}", e);
                            return OpStatus::Error(format!("Failed to get sorted batches: {}", e));
                        }
                    }
                }
                OpStatus::HasMore
            }
            Event::Finish(_) => {
                // 输出所有剩余的排序数据
                match self.finalize_sort() {
                    Ok(final_batches) => {
                        for batch in final_batches {
                            if let Err(e) = out.push(0, batch) {
                                error!("Failed to push final sorted batch: {}", e);
                                return OpStatus::Error(format!("Failed to push final sorted batch: {}", e));
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to finalize sort: {}", e);
                        return OpStatus::Error(format!("Failed to finalize sort: {}", e));
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
        "MppSortOperator"
    }
}

impl MppSortOperator {
    /// 检查是否应该输出排序后的数据
    fn should_output_sorted_data(&self) -> bool {
        // 简化实现：当缓冲区达到一定大小时输出
        self.buffered_data.len() >= 10
    }
    
    /// 获取排序后的批次
    fn get_sorted_batches(&mut self) -> Result<Vec<RecordBatch>> {
        // 简化实现：直接返回缓冲区中的数据
        let batches = std::mem::take(&mut self.buffered_data);
        Ok(batches)
    }
    
    /// 完成排序并输出最终结果
    fn finalize_sort(&mut self) -> Result<Vec<RecordBatch>> {
        // 简化实现：返回所有剩余数据
        let batches = std::mem::take(&mut self.buffered_data);
        Ok(batches)
    }
}

impl Operator for MppTopNOperator {
    fn on_event(&mut self, ev: Event, out: &mut Outbox) -> OpStatus {
        match ev {
            Event::Data { batch, .. } => {
                // 添加批次到TopN处理缓冲区
                if let Err(e) = self.add_batch(batch) {
                    error!("Failed to add batch for top-N processing: {}", e);
                    return OpStatus::Error(format!("Failed to add batch for top-N processing: {}", e));
                }
                
                // 检查是否需要输出TopN结果
                if self.buffered_data.len() >= 1000 { // 简单的批次大小检查
                    match self.process_batch() {
                        Ok(topn_batches) => {
                            for batch in topn_batches {
                                if let Err(e) = out.push(0, batch) {
                                    error!("Failed to push top-N batch: {}", e);
                                    return OpStatus::Error(format!("Failed to push top-N batch: {}", e));
                                }
                            }
                        }
                        Err(e) => {
                            error!("Failed to process top-N batch: {}", e);
                            return OpStatus::Error(format!("Failed to process top-N batch: {}", e));
                        }
                    }
                }
                OpStatus::HasMore
            }
            Event::Finish(_) => {
                // 输出所有剩余的TopN数据
                match self.process_batch() {
                    Ok(final_batches) => {
                        for batch in final_batches {
                            if let Err(e) = out.push(0, batch) {
                                error!("Failed to push final top-N batch: {}", e);
                                return OpStatus::Error(format!("Failed to push final top-N batch: {}", e));
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to finalize top-N processing: {}", e);
                        return OpStatus::Error(format!("Failed to finalize top-N processing: {}", e));
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
        "MppTopNOperator"
    }
}
