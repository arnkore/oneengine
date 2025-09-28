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


//! 列式投影器
//! 
//! 基于表达式引擎的完全面向列式的、全向量化极致优化的投影算子实现

use arrow::array::*;
use arrow::compute::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use arrow::error::ArrowError;
use std::sync::Arc;
use std::time::Instant;
use std::collections::HashMap;
use tracing::{debug, info, warn};
use crate::execution::push_runtime::{Operator, Event, OpStatus, Outbox, PortId};
use crate::expression::{VectorizedExpressionEngine, ExpressionEngineConfig};
use crate::expression::ast::{Expression, ColumnRef, Literal, ArithmeticExpr, ArithmeticOp as ExprArithmeticOp, FunctionCall, CastExpr, ComparisonExpr, LogicalExpr, CaseExpr};
use datafusion_common::ScalarValue;
use anyhow::Result;

/// 列式向量化投影器配置
#[derive(Debug, Clone)]
pub struct VectorizedProjectorConfig {
    /// 批次大小
    pub batch_size: usize,
    /// 是否启用SIMD优化
    pub enable_simd: bool,
    /// 是否启用字典列优化
    pub enable_dictionary_optimization: bool,
    /// 是否启用压缩列优化
    pub enable_compressed_optimization: bool,
    /// 是否启用零拷贝优化
    pub enable_zero_copy: bool,
    /// 是否启用预取优化
    pub enable_prefetch: bool,
    /// 是否启用列重排序优化
    pub enable_column_reordering: bool,
    /// 是否启用表达式计算优化
    pub enable_expression_optimization: bool,
}

impl Default for VectorizedProjectorConfig {
    fn default() -> Self {
        Self {
            batch_size: 8192,
            enable_simd: true,
            enable_dictionary_optimization: true,
            enable_compressed_optimization: true,
            enable_zero_copy: true,
            enable_prefetch: true,
            enable_column_reordering: true,
            enable_expression_optimization: true,
        }
    }
}

// 删除ProjectionExpression，直接使用统一的Expression AST

// 删除ProjectionExpression和相关的操作符定义，直接使用统一的Expression AST

/// 列式向量化投影器
pub struct VectorizedProjector {
    config: VectorizedProjectorConfig,
    /// 表达式引擎
    expression_engine: VectorizedExpressionEngine,
    /// 投影表达式（统一使用Expression AST）
    expressions: Vec<Expression>,
    output_schema: SchemaRef,
    column_indices: Vec<usize>,
    stats: ProjectorStats,
    /// 算子ID
    operator_id: u32,
    /// 输入端口
    input_ports: Vec<PortId>,
    /// 输出端口
    output_ports: Vec<PortId>,
    /// 是否完成
    finished: bool,
    /// 算子名称
    name: String,
}

#[derive(Debug, Default)]
pub struct ProjectorStats {
    pub total_rows_processed: u64,
    pub total_batches_processed: u64,
    pub total_project_time: std::time::Duration,
    pub avg_project_time: std::time::Duration,
    pub column_access_count: HashMap<usize, u64>,
    pub expression_eval_count: HashMap<String, u64>,
}

impl VectorizedProjector {
    pub fn new(
        config: VectorizedProjectorConfig,
        expressions: Vec<Expression>,
        output_schema: SchemaRef,
        operator_id: u32,
        input_ports: Vec<PortId>,
        output_ports: Vec<PortId>,
        name: String,
    ) -> Result<Self> {
        let column_indices = Self::extract_column_indices(&expressions);
        
        // 创建表达式引擎配置
        let expression_config = ExpressionEngineConfig {
            enable_jit: config.enable_simd,
            enable_simd: config.enable_simd,
            enable_fusion: true,
            enable_cache: true,
            jit_threshold: 100,
            cache_size_limit: 1024 * 1024 * 1024, // 1GB
            batch_size: config.batch_size,
        };
        
        // 创建表达式引擎
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

    /// 从Expression中提取列索引
    fn extract_column_indices(expressions: &[Expression]) -> Vec<usize> {
        let mut indices = Vec::new();
        for expr in expressions {
            Self::collect_column_indices(expr, &mut indices);
        }
        indices.sort();
        indices.dedup();
        indices
    }

    /// 递归收集表达式中的列索引
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
            _ => {} // 其他表达式类型不包含列引用
        }
    }



    /// 创建字面量数组
    fn create_literal_array_static(value: &ScalarValue, len: usize) -> Result<ArrayRef, String> {
        match value {
            ScalarValue::Int32(Some(v)) => {
                let array = Int32Array::from(vec![*v; len]);
                Ok(Arc::new(array))
            },
            ScalarValue::Int64(Some(v)) => {
                let array = Int64Array::from(vec![*v; len]);
                Ok(Arc::new(array))
            },
            ScalarValue::Float32(Some(v)) => {
                let array = Float32Array::from(vec![*v; len]);
                Ok(Arc::new(array))
            },
            ScalarValue::Float64(Some(v)) => {
                let array = Float64Array::from(vec![*v; len]);
                Ok(Arc::new(array))
            },
            ScalarValue::Utf8(Some(v)) => {
                let array = StringArray::from(vec![v.as_str(); len]);
                Ok(Arc::new(array))
            },
            ScalarValue::Boolean(Some(v)) => {
                let array = BooleanArray::from(vec![*v; len]);
                Ok(Arc::new(array))
            },
            _ => Err("Unsupported literal type".to_string()),
        }
    }



    /// 向量化投影
    pub fn project(&mut self, batch: &RecordBatch) -> Result<RecordBatch, String> {
        let start = Instant::now();
        
        // 直接使用表达式引擎计算投影表达式
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
        
        debug!("向量化投影完成: {} columns -> {} columns ({}μs)", 
               batch.num_columns(), result.num_columns(), duration.as_micros());
        
        Ok(result)
    }

    

    /// 创建常量数组
    fn create_literal_array(&self, value: &ScalarValue, len: usize) -> Result<ArrayRef, String> {
        match value {
            ScalarValue::Int32(Some(val)) => {
                Ok(Arc::new(Int32Array::from(vec![*val; len])))
            },
            ScalarValue::Int64(Some(val)) => {
                Ok(Arc::new(Int64Array::from(vec![*val; len])))
            },
            ScalarValue::Float32(Some(val)) => {
                Ok(Arc::new(Float32Array::from(vec![*val; len])))
            },
            ScalarValue::Float64(Some(val)) => {
                Ok(Arc::new(Float64Array::from(vec![*val; len])))
            },
            ScalarValue::Utf8(Some(val)) => {
                Ok(Arc::new(StringArray::from(vec![val.as_str(); len])))
            },
            ScalarValue::Boolean(Some(val)) => {
                Ok(Arc::new(BooleanArray::from(vec![*val; len])))
            },
            _ => Err(format!("Unsupported literal type: {:?}", value))
        }
    }


    /// 更新统计信息
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

    /// 获取统计信息
    pub fn get_stats(&self) -> &ProjectorStats {
        &self.stats
    }

    /// 重置统计信息
    pub fn reset_stats(&mut self) {
        self.stats = ProjectorStats::default();
    }
}

/// 实现Operator trait
impl Operator for VectorizedProjector {
    
    fn on_event(&mut self, ev: Event, out: &mut Outbox) -> OpStatus {
        match ev {
            Event::Data { port, batch } => {
                if self.input_ports.contains(&port) {
                    match self.project(&batch) {
                        Ok(projected_batch) => {
                            // 发送到所有输出端口
                            for &output_port in &self.output_ports {
                                out.send(output_port, projected_batch.clone());
                            }
                            OpStatus::Ready
                        },
                        Err(e) => {
                            warn!("向量化投影失败: {}", e);
                            OpStatus::Error("向量化投影失败".to_string())
                        }
                    }
                } else {
                    warn!("未知的输入端口: {}", port);
                    OpStatus::Error("未知的输入端口".to_string())
                }
            },
            Event::EndOfStream { port } => {
                if self.input_ports.contains(&port) {
                    self.finished = true;
                    // 转发EndOfStream事件
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

/// 批量投影处理器
pub struct BatchProjectorProcessor {
    projectors: Vec<VectorizedProjector>,
    config: VectorizedProjectorConfig,
}

impl BatchProjectorProcessor {
    pub fn new(config: VectorizedProjectorConfig) -> Self {
        Self {
            projectors: Vec::new(),
            config,
        }
    }

    /// 添加投影器
    pub fn add_projector(&mut self, expressions: Vec<Expression>, output_schema: SchemaRef) -> Result<()> {
        let projector = VectorizedProjector::new(
            self.config.clone(), 
            expressions, 
            output_schema, 
            0, // operator_id
            vec![], // input_ports
            vec![], // output_ports
            "projector".to_string() // name
        );
        self.projectors.push(projector?);
        Ok(())
    }

    /// 批量投影
    pub fn project_batch(&mut self, batch: &RecordBatch) -> Result<Vec<RecordBatch>, String> {
        let mut results = Vec::new();
        
        for projector in &mut self.projectors {
            let result = projector.project(batch)?;
            results.push(result);
        }
        
        Ok(results)
    }

    /// 获取所有投影器的统计信息
    pub fn get_all_stats(&self) -> Vec<&ProjectorStats> {
        self.projectors.iter().map(|p| p.get_stats()).collect()
    }
}

/// 投影优化器
pub struct ProjectorOptimizer {
    config: VectorizedProjectorConfig,
}

impl ProjectorOptimizer {
    pub fn new(config: VectorizedProjectorConfig) -> Self {
        Self { config }
    }
    

    /// 优化投影表达式
    pub fn optimize_expressions(&self, expressions: &[Expression]) -> Vec<Expression> {
        // 实现表达式优化逻辑
        // 1. 常量折叠
        // 2. 表达式重排序
        // 3. 公共子表达式消除
        // 4. 向量化优化
        expressions.to_vec()
    }

    /// 分析表达式复杂度
    pub fn analyze_complexity(&self, expr: &Expression) -> usize {
        match expr {
            Expression::Column(_) => 1,
            Expression::Literal(_) => 1,
            Expression::Arithmetic(arithmetic) => {
                1 + self.analyze_complexity(&arithmetic.left) + self.analyze_complexity(&arithmetic.right)
            },
            Expression::Comparison(comparison) => {
                1 + self.analyze_complexity(&comparison.left) + self.analyze_complexity(&comparison.right)
            },
            Expression::Logical(logical) => {
                1 + self.analyze_complexity(&logical.left) + self.analyze_complexity(&logical.right)
            },
            Expression::Function(function_call) => {
                1 + function_call.args.iter().map(|arg| self.analyze_complexity(arg)).sum::<usize>()
            },
            Expression::Case(case_expr) => {
                1 + self.analyze_complexity(&case_expr.condition) + self.analyze_complexity(&case_expr.then_expr) + self.analyze_complexity(&case_expr.else_expr)
            },
            Expression::Cast(cast_expr) => {
                1 + self.analyze_complexity(&cast_expr.expr)
            },
            _ => 1, // 其他表达式类型
        }
    }
    
    // 静态版本的函数，用于在静态上下文中调用
    fn create_literal_array_static(value: &ScalarValue, len: usize) -> Result<ArrayRef, String> {
        match value {
            ScalarValue::Int32(Some(v)) => {
                let array = Int32Array::from(vec![*v; len]);
                Ok(Arc::new(array))
            },
            ScalarValue::Int64(Some(v)) => {
                let array = Int64Array::from(vec![*v; len]);
                Ok(Arc::new(array))
            },
            ScalarValue::Float32(Some(v)) => {
                let array = Float32Array::from(vec![*v; len]);
                Ok(Arc::new(array))
            },
            ScalarValue::Float64(Some(v)) => {
                let array = Float64Array::from(vec![*v; len]);
                Ok(Arc::new(array))
            },
            ScalarValue::Utf8(Some(v)) => {
                let array = StringArray::from(vec![v.as_str(); len]);
                Ok(Arc::new(array))
            },
            ScalarValue::Boolean(Some(v)) => {
                let array = BooleanArray::from(vec![*v; len]);
                Ok(Arc::new(array))
            },
            _ => Err("Unsupported literal type".to_string()),
        }
    }
    
}
