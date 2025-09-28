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

//! Columnar projector
//! 
//! Fully columnar, fully vectorized, extremely optimized projection operator implementation based on expression engine

use super::super::push_runtime::{Operator, Event, OpStatus, Outbox, PortId};
use crate::expression::VectorizedExpressionEngine;
use crate::expression::ExpressionEngineConfig;
use crate::expression::ast::Expression;
use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;
use arrow::datatypes::SchemaRef;
use anyhow::Result;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, warn};

/// Columnar vectorized projector configuration
#[derive(Debug, Clone)]
pub struct VectorizedProjectorConfig {
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
    /// Whether to enable column reordering optimization
    pub enable_column_reordering: bool,
    /// Whether to enable expression computation optimization
    pub enable_expression_optimization: bool,
}

impl Default for VectorizedProjectorConfig {
    fn default() -> Self {
        Self {
            batch_size: 1024,
            enable_simd: true,
            enable_dict_optimization: true,
            enable_compressed_optimization: true,
            enable_zero_copy: true,
            enable_prefetch: true,
            enable_column_reordering: true,
            enable_expression_optimization: true,
        }
    }
}

/// Columnar vectorized projector
pub struct VectorizedProjector {
    config: VectorizedProjectorConfig,
    /// Expression engine
    expression_engine: VectorizedExpressionEngine,
    /// Projection expressions (unified using Expression AST)
    expressions: Vec<Expression>,
    output_schema: SchemaRef,
    column_indices: Vec<usize>,
    stats: ProjectorStats,
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

impl VectorizedProjector {
    pub fn new(
        config: VectorizedProjectorConfig,
        expressions: Vec<Expression>, // Directly use Expression
        output_schema: SchemaRef,
        operator_id: u32,
        input_ports: Vec<PortId>,
        output_ports: Vec<PortId>,
        name: String,
    ) -> Result<Self> {
        let column_indices = Self::extract_column_indices(&expressions);
        
        let expression_config = ExpressionEngineConfig {
            enable_jit: config.enable_simd,
            enable_simd: config.enable_simd,
            enable_fusion: true,
            enable_cache: true,
            jit_threshold: 100,
            cache_size_limit: 1024 * 1024 * 1024, // 1GB
            batch_size: config.batch_size,
        };
        
        let expression_engine = VectorizedExpressionEngine::new(expression_config)?;
        
        Ok(Self {
            config,
            expression_engine,
            expressions,
            output_schema,
            column_indices,
            stats: ProjectorStats::default(),
            operator_id,
            input_ports,
            output_ports,
            finished: false,
            name,
        })
    }

    /// Extract column indices from Expression
    fn extract_column_indices(expressions: &[Expression]) -> Vec<usize> {
        let mut indices = Vec::new();
        for expr in expressions {
            Self::collect_column_indices(expr, &mut indices);
        }
        indices.sort();
        indices.dedup();
        indices
    }

    /// Recursively collect column indices from expression
    fn collect_column_indices(expr: &Expression, indices: &mut Vec<usize>) {
        match expr {
            Expression::Column(column_ref) => {
                indices.push(column_ref.index);
            }
            Expression::Arithmetic(arithmetic) => {
                Self::collect_column_indices(&arithmetic.left, indices);
                Self::collect_column_indices(&arithmetic.right, indices);
            }
            Expression::Comparison(comparison) => {
                Self::collect_column_indices(&comparison.left, indices);
                Self::collect_column_indices(&comparison.right, indices);
            }
            Expression::Logical(logical) => {
                Self::collect_column_indices(&logical.left, indices);
                Self::collect_column_indices(&logical.right, indices);
            }
            Expression::Function(function_call) => {
                for arg in &function_call.args {
                    Self::collect_column_indices(arg, indices);
                }
            }
            Expression::Case(case_expr) => {
                Self::collect_column_indices(&case_expr.condition, indices);
                Self::collect_column_indices(&case_expr.then_expr, indices);
                Self::collect_column_indices(&case_expr.else_expr, indices);
            }
            Expression::Cast(cast_expr) => {
                Self::collect_column_indices(&cast_expr.expr, indices);
            }
            _ => {} // Other expression types don't contain column references
        }
    }

    /// Vectorized projection
    pub fn project(&mut self, batch: &RecordBatch) -> Result<RecordBatch, String> {
        let start = Instant::now();
        
        // Directly use expression engine to compute projection expressions
        let projected_columns: Result<Vec<ArrayRef>, String> = self.expressions
            .iter()
            .map(|expr| {
                self.expression_engine.execute(expr, batch)
                    .map_err(|e| e.to_string())
            })
            .collect();
        
        let projected_columns = projected_columns?;
        
        let result = RecordBatch::try_new(self.output_schema.clone(), projected_columns)
            .map_err(|e| e.to_string())?;
        
        let duration = start.elapsed();
        self.update_stats(batch.num_rows(), duration);
        
        debug!("Vectorized projection completed: {} columns -> {} columns ({}μs)", 
               batch.num_columns(), result.num_columns(), duration.as_micros());
        
        Ok(result)
    }

    /// Update statistics
    fn update_stats(&mut self, rows: usize, duration: std::time::Duration) {
        self.stats.total_rows_processed += rows as u64;
        self.stats.total_batches_processed += 1;
        self.stats.total_project_time += duration;
        
        if self.stats.total_batches_processed > 0 {
            self.stats.avg_project_time = std::time::Duration::from_nanos(
                self.stats.total_project_time.as_nanos() as u64 / self.stats.total_batches_processed
            );
        }
    }

    /// Get statistics
    pub fn get_stats(&self) -> &ProjectorStats {
        &self.stats
    }

    /// Reset statistics
    pub fn reset_stats(&mut self) {
        self.stats = ProjectorStats::default();
    }
}

/// Projector statistics
#[derive(Debug, Clone, Default)]
pub struct ProjectorStats {
    pub total_rows_processed: u64,
    pub total_batches_processed: u64,
    pub total_project_time: std::time::Duration,
    pub avg_project_time: std::time::Duration,
}

impl Operator for VectorizedProjector {
    fn on_event(&mut self, ev: Event, out: &mut Outbox) -> OpStatus {
        match ev {
            Event::Data { port, batch } => {
                if self.input_ports.contains(&port) {
                    match self.project(&batch) {
                        Ok(projected_batch) => {
                            // Send to all output ports
                            for &output_port in &self.output_ports {
                                out.send(output_port, projected_batch.clone());
                            }
                            OpStatus::Ready
                        },
                        Err(e) => {
                            warn!("Vectorized projection failed: {}", e);
                            OpStatus::Error("Vectorized projection failed".to_string())
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