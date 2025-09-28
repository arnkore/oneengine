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

//! MPP scan operators
//! 
//! Handles distributed data scanning in MPP execution

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
use crate::datalake::unified_lake_reader::{
    UnifiedLakeReader, UnifiedLakeReaderConfig, LakeFormat, UnifiedPredicate,
    TimeTravelConfig, IncrementalReadConfig, PartitionPruningInfo, ColumnProjection,
    TableMetadata, TableStatistics
};
use crate::expression::{VectorizedExpressionEngine, ExpressionEngineConfig};
use crate::expression::ast::{Expression, ColumnRef, Literal, ComparisonExpr, ComparisonOp, LogicalExpr, LogicalOp};

/// MPP scan operator with expression engine and lake reader integration
pub struct MppScanOperator {
    /// Operator ID
    operator_id: Uuid,
    /// Partition ID to scan
    partition_id: PartitionId,
    /// Unified lake reader
    lake_reader: UnifiedLakeReader,
    /// Expression engine for predicate evaluation
    expression_engine: VectorizedExpressionEngine,
    /// Compiled predicate expression
    compiled_predicate: Option<Expression>,
    /// Statistics
    stats: MppOperatorStats,
    /// Scan configuration
    config: MppScanConfig,
    /// Current table path
    current_table_path: Option<String>,
    /// Current batch index
    current_batch_index: usize,
    /// Total batches
    total_batches: usize,
    /// Current snapshot ID
    current_snapshot_id: Option<i64>,
    /// Table metadata
    table_metadata: Option<TableMetadata>,
    /// Scan statistics
    scan_stats: ScanStats,
}

/// MPP scan configuration
#[derive(Debug, Clone)]
pub struct MppScanConfig {
    /// Lake reader configuration
    pub lake_reader_config: UnifiedLakeReaderConfig,
    /// Output schema
    pub output_schema: SchemaRef,
    /// Whether to enable vectorization
    pub enable_vectorization: bool,
    /// Whether to enable SIMD
    pub enable_simd: bool,
    /// Whether to enable prefetch
    pub enable_prefetch: bool,
    /// Whether to use column pruning
    pub enable_column_pruning: bool,
    /// Whether to use predicate pushdown
    pub enable_predicate_pushdown: bool,
    /// Whether to use partition pruning
    pub enable_partition_pruning: bool,
    /// Whether to use time travel
    pub enable_time_travel: bool,
    /// Whether to use incremental read
    pub enable_incremental_read: bool,
}

/// Scan statistics
#[derive(Debug, Clone, Default)]
pub struct ScanStats {
    /// Total rows scanned
    pub total_rows_scanned: u64,
    /// Total batches scanned
    pub total_batches_scanned: u64,
    /// Total scan time
    pub total_scan_time: std::time::Duration,
    /// Average scan time
    pub avg_scan_time: std::time::Duration,
    /// File read time
    pub file_read_time: std::time::Duration,
    /// Predicate pushdown time
    pub predicate_pushdown_time: std::time::Duration,
    /// Partition pruning time
    pub partition_pruning_time: std::time::Duration,
    /// Column projection time
    pub column_projection_time: std::time::Duration,
    /// Time travel time
    pub time_travel_time: std::time::Duration,
    /// Incremental read time
    pub incremental_read_time: std::time::Duration,
    /// Snapshot ID
    pub snapshot_id: Option<i64>,
    /// Files scanned
    pub files_scanned: u64,
}

impl Default for MppScanConfig {
    fn default() -> Self {
        Self {
            lake_reader_config: UnifiedLakeReaderConfig::default(),
            output_schema: Arc::new(arrow::datatypes::Schema::empty()),
            enable_vectorization: true,
            enable_simd: true,
            enable_prefetch: true,
            enable_column_pruning: true,
            enable_predicate_pushdown: true,
            enable_partition_pruning: true,
            enable_time_travel: false,
            enable_incremental_read: false,
        }
    }
}

impl MppScanOperator {
    pub fn new(
        operator_id: Uuid,
        partition_id: PartitionId,
        config: MppScanConfig,
    ) -> Result<Self> {
        let lake_reader = UnifiedLakeReader::new(config.lake_reader_config.clone());
        
        // Create expression engine configuration
        let expression_config = ExpressionEngineConfig {
            enable_jit: config.enable_simd,
            enable_simd: config.enable_simd,
            enable_fusion: true,
            enable_cache: true,
            jit_threshold: 100,
            cache_size_limit: 1024 * 1024 * 1024, // 1GB
            batch_size: config.lake_reader_config.batch_size,
        };
        
        // Create expression engine
        let expression_engine = VectorizedExpressionEngine::new(expression_config)?;
        
        Ok(Self {
            operator_id,
            partition_id,
            lake_reader,
            expression_engine,
            compiled_predicate: None,
            stats: MppOperatorStats::default(),
            config,
            current_table_path: None,
            current_batch_index: 0,
            total_batches: 0,
            current_snapshot_id: None,
            table_metadata: None,
            scan_stats: ScanStats::default(),
        })
    }
    
    /// Set table path to scan
    pub fn set_table_path(&mut self, table_path: String) {
        self.current_table_path = Some(table_path);
        self.current_batch_index = 0;
        self.total_batches = 0;
        self.scan_stats = ScanStats::default();
        self.current_snapshot_id = None;
    }
    
    /// Set predicate expression
    pub fn set_predicate(&mut self, predicate: Expression) -> Result<()> {
        let compiled = self.expression_engine.compile(&predicate)?;
        self.compiled_predicate = Some(compiled);
        Ok(())
    }
    
    /// Apply predicate to batch
    fn apply_predicate(&mut self, batch: &RecordBatch) -> Result<RecordBatch> {
        if let Some(ref compiled_predicate) = self.compiled_predicate {
            // Use expression engine to execute filtering
            let mask_result = self.expression_engine.execute(compiled_predicate, batch)?;
            
            // Convert result to BooleanArray
            let mask = mask_result.as_any().downcast_ref::<arrow::array::BooleanArray>()
                .ok_or_else(|| anyhow::anyhow!("Expression result is not a boolean array"))?;
            
            // Use Arrow compute kernel for filtering
            let filtered_columns: Result<Vec<Arc<dyn arrow::array::Array>>, arrow::error::ArrowError> = batch
                .columns()
                .iter()
                .map(|col| arrow::compute::filter(col, mask))
                .collect();
            
            let filtered_columns = filtered_columns?;
            let filtered_schema = batch.schema();
            
            Ok(RecordBatch::try_new(filtered_schema, filtered_columns)?)
        } else {
            // No predicate, return original batch
            Ok(batch.clone())
        }
    }
    
    /// Scan next batch from partition
    pub fn scan_next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.current_table_path.is_none() {
            return Ok(None);
        }

        let table_path = self.current_table_path.as_ref().unwrap();
        let start = Instant::now();
        
        // Open table if not already opened
        if self.table_metadata.is_none() {
            self.lake_reader.open()
                .map_err(|e| anyhow::anyhow!("Failed to open table: {}", e))?;
            self.table_metadata = Some(self.lake_reader.get_metadata()
                .map_err(|e| anyhow::anyhow!("Failed to get metadata: {}", e))?
                .clone());
        }
        
        // Use unified lake reader to read data
        let batches = self.lake_reader.read_data()
            .map_err(|e| anyhow::anyhow!("Failed to read data: {}", e))?;
        
        if self.current_batch_index >= batches.len() {
            return Ok(None);
        }

        let batch = batches[self.current_batch_index].clone();
        self.current_batch_index += 1;
        
        // Apply predicate
        let filtered_batch = self.apply_predicate(&batch)
            .map_err(|e| anyhow::anyhow!("Failed to apply predicate: {}", e))?;
        
        let duration = start.elapsed();
        self.update_stats(filtered_batch.num_rows(), duration);
        
        debug!("Scanned batch {}: {} rows -> {} rows ({}μs)", 
               self.current_batch_index, batch.num_rows(), filtered_batch.num_rows(), duration.as_micros());
        
        Ok(Some(filtered_batch))
    }
    
    /// Apply predicate pushdown
    pub fn apply_predicate_pushdown(&mut self, predicates: Vec<UnifiedPredicate>) -> Result<()> {
        self.lake_reader.set_predicates(predicates);
        let start = Instant::now();
        let filtered_files = self.lake_reader.apply_predicate_pushdown()
            .map_err(|e| anyhow::anyhow!("Failed to apply predicate pushdown: {}", e))?;
        let duration = start.elapsed();
        self.scan_stats.predicate_pushdown_time += duration;
        debug!("Applied predicate pushdown: {} files filtered in {:?}", filtered_files, duration);
        Ok(())
    }
    
    /// Apply column projection
    pub fn apply_column_projection(&mut self, columns: Vec<String>) -> Result<()> {
        let projection = ColumnProjection {
            columns,
            select_all: false,
        };
        self.lake_reader.set_column_projection(projection);
        let start = Instant::now();
        self.lake_reader.apply_column_projection()
            .map_err(|e| anyhow::anyhow!("Failed to apply column projection: {}", e))?;
        let duration = start.elapsed();
        self.scan_stats.column_projection_time += duration;
        debug!("Applied column projection in {:?}", duration);
        Ok(())
    }
    
    /// Apply partition pruning
    pub fn apply_partition_pruning(&mut self, pruning_info: PartitionPruningInfo) -> Result<()> {
        self.lake_reader.set_partition_pruning(pruning_info);
        let start = Instant::now();
        let pruned_files = self.lake_reader.apply_partition_pruning()
            .map_err(|e| anyhow::anyhow!("Failed to apply partition pruning: {}", e))?;
        let duration = start.elapsed();
        self.scan_stats.partition_pruning_time += duration;
        debug!("Applied partition pruning: {} files pruned in {:?}", pruned_files, duration);
        Ok(())
    }
    
    /// Apply time travel
    pub fn apply_time_travel(&mut self, time_travel: TimeTravelConfig) -> Result<()> {
        self.lake_reader.set_time_travel(time_travel);
        let start = Instant::now();
        self.lake_reader.apply_time_travel()
            .map_err(|e| anyhow::anyhow!("Failed to apply time travel: {}", e))?;
        let duration = start.elapsed();
        self.scan_stats.time_travel_time += duration;
        debug!("Applied time travel in {:?}", duration);
        Ok(())
    }
    
    /// Apply incremental read
    pub fn apply_incremental_read(&mut self, incremental: IncrementalReadConfig) -> Result<()> {
        self.lake_reader.set_incremental_read(incremental);
        let start = Instant::now();
        self.lake_reader.apply_incremental_read()
            .map_err(|e| anyhow::anyhow!("Failed to apply incremental read: {}", e))?;
        let duration = start.elapsed();
        self.scan_stats.incremental_read_time += duration;
        debug!("Applied incremental read in {:?}", duration);
        Ok(())
    }
    
    /// Update scan statistics
    fn update_stats(&mut self, rows: usize, duration: std::time::Duration) {
        self.scan_stats.total_rows_scanned += rows as u64;
        self.scan_stats.total_batches_scanned += 1;
        self.scan_stats.total_scan_time += duration;
        self.scan_stats.avg_scan_time = self.scan_stats.total_scan_time / 
            self.scan_stats.total_batches_scanned.max(1) as u32;
        
        // Update MPP operator stats
        self.stats.rows_processed += rows as u64;
        self.stats.batches_processed += 1;
        self.stats.processing_time += duration;
    }
    
    /// Get scan statistics
    pub fn get_scan_stats(&self) -> &ScanStats {
        &self.scan_stats
    }
    
}

impl MppOperator for MppScanOperator {
    fn initialize(&mut self, _context: &MppContext) -> Result<()> {
        if let Some(ref table_path) = self.current_table_path {
            self.lake_reader.open()
                .map_err(|e| anyhow::anyhow!("Failed to open table: {}", e))?;
            self.table_metadata = Some(self.lake_reader.get_metadata()
                .map_err(|e| anyhow::anyhow!("Failed to get metadata: {}", e))?
                .clone());
        }
        debug!("Initialized scan operator for partition {}", self.partition_id);
        Ok(())
    }
    
    fn process_batch(&mut self, _batch: RecordBatch, _context: &MppContext) -> Result<Vec<RecordBatch>> {
        // Scan operator generates data, doesn't process input
        Err(anyhow::anyhow!("Scan operator doesn't process input batches"))
    }
    
    fn exchange_data(&mut self, _data: Vec<RecordBatch>, _target_workers: Vec<WorkerId>) -> Result<()> {
        // Scan operator doesn't exchange data
        Err(anyhow::anyhow!("Scan operator doesn't exchange data"))
    }
    
    fn process_partition(&mut self, partition_id: PartitionId, _data: RecordBatch) -> Result<RecordBatch> {
        if partition_id == self.partition_id {
            match self.scan_next_batch()? {
                Some(batch) => Ok(batch),
                None => Err(anyhow::anyhow!("No more data in partition {}", partition_id))
            }
        } else {
            Err(anyhow::anyhow!("Partition mismatch: expected {}, got {}", self.partition_id, partition_id))
        }
    }
    
    fn finish(&mut self, _context: &MppContext) -> Result<()> {
        self.lake_reader.close()
            .map_err(|e| anyhow::anyhow!("Failed to close lake reader: {}", e))?;
        debug!("Finished scan operator for partition {}", self.partition_id);
        Ok(())
    }
    
    fn get_stats(&self) -> MppOperatorStats {
        self.stats.clone()
    }
    
    fn recover(&mut self, _context: &MppContext) -> Result<()> {
        self.lake_reader.recover()
            .map_err(|e| anyhow::anyhow!("Failed to recover lake reader: {}", e))?;
        self.stats = MppOperatorStats::default();
        self.scan_stats = ScanStats::default();
        debug!("Recovered scan operator for partition {}", self.partition_id);
        Ok(())
    }
}


/// MPP scan operator factory
pub struct MppScanOperatorFactory;

impl MppScanOperatorFactory {
    /// Create Iceberg scan operator
    pub fn create_iceberg_scan(
        operator_id: Uuid,
        partition_id: PartitionId,
        table_path: String,
        config: MppScanConfig,
    ) -> Result<MppScanOperator> {
        let mut lake_config = config.lake_reader_config.clone();
        lake_config.format = LakeFormat::Iceberg;
        lake_config.table_path = table_path;
        
        let mut scan_config = config;
        scan_config.lake_reader_config = lake_config;
        
        let mut operator = MppScanOperator::new(operator_id, partition_id, scan_config)?;
        operator.set_table_path(table_path);
        Ok(operator)
    }
    
    /// Create Parquet scan operator
    pub fn create_parquet_scan(
        operator_id: Uuid,
        partition_id: PartitionId,
        table_path: String,
        config: MppScanConfig,
    ) -> Result<MppScanOperator> {
        let mut lake_config = config.lake_reader_config.clone();
        lake_config.format = LakeFormat::Parquet;
        lake_config.table_path = table_path;
        
        let mut scan_config = config;
        scan_config.lake_reader_config = lake_config;
        
        let mut operator = MppScanOperator::new(operator_id, partition_id, scan_config)?;
        operator.set_table_path(table_path);
        Ok(operator)
    }
    
    /// Create ORC scan operator
    pub fn create_orc_scan(
        operator_id: Uuid,
        partition_id: PartitionId,
        table_path: String,
        config: MppScanConfig,
    ) -> Result<MppScanOperator> {
        let mut lake_config = config.lake_reader_config.clone();
        lake_config.format = LakeFormat::Orc;
        lake_config.table_path = table_path;
        
        let mut scan_config = config;
        scan_config.lake_reader_config = lake_config;
        
        let mut operator = MppScanOperator::new(operator_id, partition_id, scan_config)?;
        operator.set_table_path(table_path);
        Ok(operator)
    }
    
    /// Create scan operator with custom configuration
    pub fn create_scan(
        operator_id: Uuid,
        partition_id: PartitionId,
        config: MppScanConfig,
    ) -> Result<MppScanOperator> {
        MppScanOperator::new(operator_id, partition_id, config)
    }
}
