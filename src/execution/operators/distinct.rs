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

//! 向量化去重算子
//! 
//! 支持基于哈希表的去重操作

use arrow::record_batch::RecordBatch;
use arrow::array::*;
use arrow::datatypes::*;
use arrow::compute::*;
use std::sync::Arc;
use std::time::Instant;
use std::collections::HashSet;
use tracing::{debug, info, warn};
use crate::execution::push_runtime::{Operator, Event, OpStatus, Outbox, PortId};
use crate::expression::{VectorizedExpressionEngine, ExpressionEngineConfig};
use crate::expression::ast::{Expression, ColumnRef, Literal};
use datafusion_common::ScalarValue;
use anyhow::Result;

/// 去重配置
#[derive(Debug, Clone)]
pub struct DistinctConfig {
    /// 输入schema
    pub input_schema: SchemaRef,
    /// 去重列
    pub distinct_columns: Vec<String>,
    /// 是否启用向量化处理
    pub enable_vectorization: bool,
    /// 批处理大小
    pub batch_size: usize,
    /// 内存限制
    pub memory_limit: usize,
    /// 是否保持顺序
    pub preserve_order: bool,
}

impl DistinctConfig {
    /// 创建新的配置
    pub fn new(
        input_schema: SchemaRef,
        distinct_columns: Vec<String>,
    ) -> Self {
        Self {
            input_schema,
            distinct_columns,
            enable_vectorization: true,
            batch_size: 1024,
            memory_limit: 100 * 1024 * 1024, // 100MB
            preserve_order: true,
        }
    }
    
    /// 获取输出schema
    pub fn output_schema(&self) -> SchemaRef {
        self.input_schema.clone()
    }
}

/// 去重键
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct DistinctKey {
    /// 键值
    values: Vec<ScalarValue>,
}

/// 向量化去重算子
pub struct VectorizedDistinct {
    /// 基础算子信息
    base: crate::execution::operators::BaseOperator,
    /// 配置
    config: DistinctConfig,
    /// 表达式引擎
    expression_engine: VectorizedExpressionEngine,
    /// 去重集合
    distinct_set: HashSet<DistinctKey>,
    /// 结果批次缓存
    result_batches: Vec<RecordBatch>,
    /// 当前批次索引
    current_batch: usize,
    /// 统计信息
    metrics: crate::execution::operators::OperatorMetrics,
}

impl VectorizedDistinct {
    /// 创建新的去重算子
    pub fn new(
        config: DistinctConfig,
        operator_id: u32,
        input_ports: Vec<PortId>,
        output_ports: Vec<PortId>,
        name: String,
    ) -> Result<Self> {
        let expression_config = ExpressionEngineConfig {
            enable_jit: true,
            enable_simd: true,
            enable_optimization: true,
            cache_size: 1000,
        };
        
        let expression_engine = VectorizedExpressionEngine::new(expression_config)?;
        
        Ok(Self {
            base: crate::execution::operators::BaseOperator::new(
                operator_id,
                input_ports,
                output_ports,
                name,
            ),
            config,
            expression_engine,
            distinct_set: HashSet::new(),
            result_batches: Vec::new(),
            current_batch: 0,
            metrics: crate::execution::operators::OperatorMetrics::default(),
        })
    }
    
    /// 处理输入批次
    fn process_batch(&mut self, batch: RecordBatch, out: &mut Outbox) -> Result<OpStatus> {
        let start_time = Instant::now();
        
        // 处理批次进行去重
        let distinct_batch = self.process_distinct_batch(&batch)?;
        
        if distinct_batch.num_rows() > 0 {
            out.send(0, distinct_batch)?;
        }
        
        self.metrics.update_batch(batch.num_rows(), start_time.elapsed());
        Ok(OpStatus::NeedMoreData)
    }
    
    /// 处理去重批次
    fn process_distinct_batch(&mut self, batch: &RecordBatch) -> Result<RecordBatch> {
        let mut distinct_rows = Vec::new();
        let mut distinct_indices = Vec::new();
        
        for row_idx in 0..batch.num_rows() {
            let distinct_key = self.create_distinct_key(batch, row_idx)?;
            
            if self.distinct_set.insert(distinct_key) {
                // 新行，添加到结果
                distinct_indices.push(row_idx as u32);
            }
        }
        
        if distinct_indices.is_empty() {
            // 没有新行，返回空批次
            return Ok(RecordBatch::new_empty(self.config.input_schema.clone()));
        }
        
        // 构建去重后的批次
        self.build_distinct_batch(batch, &distinct_indices)
    }
    
    /// 创建去重键
    fn create_distinct_key(&self, batch: &RecordBatch, row_idx: usize) -> Result<DistinctKey> {
        let mut values = Vec::new();
        
        for column_name in &self.config.distinct_columns {
            let column_index = batch.schema().fields.iter()
                .position(|f| f.name() == column_name)
                .ok_or_else(|| anyhow::anyhow!("Distinct column {} not found", column_name))?;
            
            let column = batch.column(column_index);
            let scalar = ScalarValue::try_from_array(column, row_idx)?;
            values.push(scalar);
        }
        
        Ok(DistinctKey { values })
    }
    
    /// 构建去重批次
    fn build_distinct_batch(
        &self,
        batch: &RecordBatch,
        distinct_indices: &[u32],
    ) -> Result<RecordBatch> {
        let mut result_columns = Vec::new();
        
        for field in batch.schema().fields() {
            let column = batch.column_by_name(field.name()).unwrap();
            let selected_column = take(column, &UInt32Array::from(distinct_indices), None)?;
            result_columns.push(selected_column);
        }
        
        let result_batch = RecordBatch::try_new(
            batch.schema().clone(),
            result_columns,
        )?;
        
        Ok(result_batch)
    }
    
    /// 完成处理
    fn finish(&mut self, out: &mut Outbox) -> Result<OpStatus> {
        out.emit_finish(0);
        self.base.set_finished();
        Ok(OpStatus::Finished)
    }
}

impl Operator for VectorizedDistinct {
    fn on_event(&mut self, event: Event, out: &mut Outbox) -> Result<OpStatus> {
        match event {
            Event::Data { port: _, batch } => {
                self.process_batch(batch, out)
            }
            Event::Flush { port: _ } => {
                Ok(OpStatus::NeedMoreData)
            }
            Event::Finish { port: _ } => {
                self.finish(out)
            }
        }
    }
    
    
    fn is_finished(&self) -> bool {
        self.base.is_finished()
    }
    
    fn name(&self) -> &str {
        self.base.name()
    }
}

