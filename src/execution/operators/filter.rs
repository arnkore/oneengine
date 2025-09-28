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

//! Columnar filter
//! 
//! Fully columnar, fully vectorized, extremely optimized filter operator implementation based on expression engine

use super::super::push_runtime::{Operator, Event, OpStatus, Outbox, PortId};
use crate::expression::VectorizedExpressionEngine;
use crate::expression::ExpressionEngineConfig;
use crate::expression::ast::Expression;
use arrow::array::*;
use arrow::compute::filter;
use arrow::record_batch::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use anyhow::Result;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, warn};

/// Columnar vectorized filter configuration
#[derive(Debug, Clone)]
pub struct VectorizedFilterConfig {
    /// Batch size
    pub batch_size: usize,
    /// Whether to enable SIMD optimization
    pub enable_simd: bool,
    /// Whether to enable dictionary column optimization
    pub enable_dict_optimization: bool,
    /// Whether to enable compressed column optimization
    pub enable_compressed_optimization: bool,
    /// Whether to enable zero-copy optimization
    pub enable_zero_copy: bool,
    /// Whether to enable prefetch optimization
    pub enable_prefetch: bool,
}

impl Default for VectorizedFilterConfig {
    fn default() -> Self {
        Self {
            batch_size: 1024,
            enable_simd: true,
            enable_dict_optimization: true,
            enable_compressed_optimization: true,
            enable_zero_copy: true,
            enable_prefetch: true,
        }
    }
}

/// Columnar vectorized filter
pub struct VectorizedFilter {
    config: VectorizedFilterConfig,
    /// Expression engine
    expression_engine: VectorizedExpressionEngine,
    /// Filter expression (unified using Expression AST)
    predicate: Expression,
    column_index: Option<usize>,
    cached_mask: Option<BooleanArray>,
    stats: FilterStats,
    /// Operator ID
    operator_id: u32,
    /// Input ports
    input_ports: Vec<PortId>,
    /// Output ports
    output_ports: Vec<PortId>,
    /// Whether finished
    finished: bool,
    /// Operator name
    name: String,
}

impl VectorizedFilter {
    pub fn new(
        config: VectorizedFilterConfig, 
        predicate: Expression, // Directly use Expression
        operator_id: u32,
        input_ports: Vec<PortId>,
        output_ports: Vec<PortId>,
        name: String,
    ) -> Result<Self> {
        // Create expression engine configuration
        let expression_config = ExpressionEngineConfig {
            enable_jit: config.enable_simd,
            enable_simd: config.enable_simd,
            enable_fusion: true,
            enable_cache: true,
            jit_threshold: 100,
            cache_size_limit: 1024 * 1024 * 1024, // 1GB
            batch_size: config.batch_size,
        };
        
        // Create expression engine
        let expression_engine = VectorizedExpressionEngine::new(expression_config)?;
        
        Ok(Self {
            config,
            expression_engine,
            predicate,
            column_index: None,
            cached_mask: None,
            stats: FilterStats::default(),
            operator_id,
            input_ports,
            output_ports,
            finished: false,
            name,
        })
    }

    /// Set column index
    pub fn set_column_index(&mut self, index: usize) {
        self.column_index = Some(index);
    }

    /// Vectorized filtering
    pub fn filter(&mut self, batch: &RecordBatch) -> Result<RecordBatch, String> {
        let start = Instant::now();
        
        // Directly use expression engine for filtering
        let mask_result = self.expression_engine.execute(&self.predicate, batch)
            .map_err(|e| e.to_string())?;
        
        // Convert result to BooleanArray
        let mask = mask_result.as_any().downcast_ref::<BooleanArray>()
            .ok_or_else(|| "Expression result is not a boolean array".to_string())?;
        
        // Use Arrow compute kernel for filtering
        let filtered_columns: Result<Vec<ArrayRef>, ArrowError> = batch
            .columns()
            .iter()
            .map(|col| filter(col, mask))
            .collect();
        
        let filtered_columns = filtered_columns.map_err(|e| e.to_string())?;
        let filtered_schema = batch.schema();
        
        let result = RecordBatch::try_new(filtered_schema, filtered_columns)
            .map_err(|e| e.to_string())?;
        
        let duration = start.elapsed();
        self.update_stats(batch.num_rows(), result.num_rows(), duration);
        
        debug!("Vectorized filtering completed: {} rows -> {} rows ({}μs)", 
               batch.num_rows(), result.num_rows(), duration.as_micros());
        
        Ok(result)
    }

    /// Update statistics
    fn update_stats(&mut self, input_rows: usize, output_rows: usize, duration: std::time::Duration) {
        self.stats.total_rows_processed += input_rows as u64;
        self.stats.total_rows_filtered += output_rows as u64;
        self.stats.total_batches_processed += 1;
        self.stats.total_filter_time += duration;
        
        if self.stats.total_batches_processed > 0 {
            self.stats.avg_filter_time = std::time::Duration::from_nanos(
                self.stats.total_filter_time.as_nanos() as u64 / self.stats.total_batches_processed
            );
        }
        
        if self.stats.total_rows_processed > 0 {
            self.stats.selectivity = self.stats.total_rows_filtered as f64 / self.stats.total_rows_processed as f64;
        }
    }

    /// Get statistics
    pub fn get_stats(&self) -> &FilterStats {
        &self.stats
    }

    /// Reset statistics
    pub fn reset_stats(&mut self) {
        self.stats = FilterStats::default();
    }
}

/// Filter statistics
#[derive(Debug, Clone, Default)]
pub struct FilterStats {
    pub total_rows_processed: u64,
    pub total_rows_filtered: u64,
    pub total_batches_processed: u64,
    pub total_filter_time: std::time::Duration,
    pub avg_filter_time: std::time::Duration,
    pub selectivity: f64,
}

impl Operator for VectorizedFilter {
    fn on_event(&mut self, ev: Event, out: &mut Outbox) -> OpStatus {
        match ev {
            Event::Data { port, batch } => {
                if self.input_ports.contains(&port) {
                    match self.filter(&batch) {
                        Ok(filtered_batch) => {
                            // Send to all output ports
                            for &output_port in &self.output_ports {
                                out.send(output_port, filtered_batch.clone());
                            }
                            OpStatus::Ready
                        },
                        Err(e) => {
                            warn!("Vectorized filtering failed: {}", e);
                            OpStatus::Error("Vectorized filtering failed".to_string())
                        }
                    }
                } else {
                    warn!("Unknown input port: {}", port);
                    OpStatus::Error("Unknown input port".to_string())
                }
            },
            Event::EndOfStream { port } => {
                if self.input_ports.contains(&port) {
                    self.finished = true;
                    // Forward EndOfStream event
                    for &output_port in &self.output_ports {
                        out.send_eos(output_port);
                    }
                    OpStatus::Finished
                } else {
                    OpStatus::Ready
                }
            },
            _ => OpStatus::Ready,
        }
    }
    
    fn is_finished(&self) -> bool {
        self.finished
    }
    
    fn name(&self) -> &str {
        &self.name
    }
}