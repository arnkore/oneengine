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

//! 向量化TopN算子
//! 
//! 支持获取排序后的前N项数据，使用堆排序优化

use arrow::record_batch::RecordBatch;
use arrow::array::*;
use arrow::datatypes::*;
use arrow::compute::*;
use std::sync::Arc;
use std::time::Instant;
use std::collections::BinaryHeap;
use std::cmp::Ordering;
use tracing::{debug, info, warn};
use crate::execution::push_runtime::{Operator, Event, OpStatus, Outbox, PortId};
use crate::expression::{VectorizedExpressionEngine, ExpressionEngineConfig};
use crate::expression::ast::{Expression, ColumnRef, Literal};
use datafusion_common::ScalarValue;
use anyhow::Result;

/// 排序选项
#[derive(Debug, Clone)]
pub struct SortOptions {
    /// 排序列
    pub column: String,
    /// 是否升序
    pub ascending: bool,
    /// 空值处理
    pub nulls_first: bool,
}

/// TopN配置
#[derive(Debug, Clone)]
pub struct TopNConfig {
    /// 输入schema
    pub input_schema: SchemaRef,
    /// 排序选项
    pub sort_options: Vec<SortOptions>,
    /// 返回行数
    pub limit: usize,
    /// 是否启用向量化处理
    pub enable_vectorization: bool,
    /// 批处理大小
    pub batch_size: usize,
    /// 内存限制
    pub memory_limit: usize,
}

impl TopNConfig {
    /// 创建新的配置
    pub fn new(
        input_schema: SchemaRef,
        sort_options: Vec<SortOptions>,
        limit: usize,
    ) -> Self {
        Self {
            input_schema,
            sort_options,
            limit,
            enable_vectorization: true,
            batch_size: 1024,
            memory_limit: 100 * 1024 * 1024, // 100MB
        }
    }
    
    /// 获取输出schema
    pub fn output_schema(&self) -> SchemaRef {
        self.input_schema.clone()
    }
}

/// 排序键
#[derive(Debug, Clone, PartialEq)]
struct SortKey {
    /// 键值
    values: Vec<ScalarValue>,
    /// 原始行索引
    row_index: usize,
}

impl Eq for SortKey {}

impl PartialOrd for SortKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SortKey {
    fn cmp(&self, other: &Self) -> Ordering {
        for (i, (self_val, other_val)) in self.values.iter().zip(other.values.iter()).enumerate() {
            let ordering = self.compare_values(self_val, other_val, i);
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        Ordering::Equal
    }
}

impl SortKey {
    /// 比较两个值
    fn compare_values(&self, a: &ScalarValue, b: &ScalarValue, sort_index: usize) -> Ordering {
        // 处理NULL值
        if a.is_null() && b.is_null() {
            return Ordering::Equal;
        }
        if a.is_null() {
            return Ordering::Less; // NULL值较小
        }
        if b.is_null() {
            return Ordering::Greater;
        }
        
        // 比较非NULL值
        let ordering = match (a, b) {
            (ScalarValue::Int8(Some(a)), ScalarValue::Int8(Some(b))) => a.cmp(b),
            (ScalarValue::Int16(Some(a)), ScalarValue::Int16(Some(b))) => a.cmp(b),
            (ScalarValue::Int32(Some(a)), ScalarValue::Int32(Some(b))) => a.cmp(b),
            (ScalarValue::Int64(Some(a)), ScalarValue::Int64(Some(b))) => a.cmp(b),
            (ScalarValue::UInt8(Some(a)), ScalarValue::UInt8(Some(b))) => a.cmp(b),
            (ScalarValue::UInt16(Some(a)), ScalarValue::UInt16(Some(b))) => a.cmp(b),
            (ScalarValue::UInt32(Some(a)), ScalarValue::UInt32(Some(b))) => a.cmp(b),
            (ScalarValue::UInt64(Some(a)), ScalarValue::UInt64(Some(b))) => a.cmp(b),
            (ScalarValue::Float32(Some(a)), ScalarValue::Float32(Some(b))) => {
                a.partial_cmp(b).unwrap_or(Ordering::Equal)
            }
            (ScalarValue::Float64(Some(a)), ScalarValue::Float64(Some(b))) => {
                a.partial_cmp(b).unwrap_or(Ordering::Equal)
            }
            (ScalarValue::Utf8(Some(a)), ScalarValue::Utf8(Some(b))) => a.cmp(b),
            (ScalarValue::LargeUtf8(Some(a)), ScalarValue::LargeUtf8(Some(b))) => a.cmp(b),
            (ScalarValue::Boolean(Some(a)), ScalarValue::Boolean(Some(b))) => a.cmp(b),
            _ => Ordering::Equal,
        };
        
        // 根据排序选项调整顺序
        // 这里简化处理，实际需要根据sort_options调整
        ordering
    }
}

/// 向量化TopN算子
pub struct VectorizedTopN {
    /// 基础算子信息
    base: crate::execution::operators::BaseOperator,
    /// 配置
    config: TopNConfig,
    /// 表达式引擎
    expression_engine: VectorizedExpressionEngine,
    /// 输入数据缓存
    input_batches: Vec<RecordBatch>,
    /// 排序堆
    sort_heap: BinaryHeap<SortKey>,
    /// 统计信息
    metrics: crate::execution::operators::OperatorMetrics,
}

impl VectorizedTopN {
    /// 创建新的TopN算子
    pub fn new(
        config: TopNConfig,
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
            input_batches: Vec::new(),
            sort_heap: BinaryHeap::new(),
            metrics: crate::execution::operators::OperatorMetrics::default(),
        })
    }
    
    /// 处理输入批次
    fn process_batch(&mut self, batch: RecordBatch, out: &mut Outbox) -> Result<OpStatus> {
        let start_time = Instant::now();
        
        // 缓存输入批次
        self.input_batches.push(batch);
        
        self.metrics.update_batch(batch.num_rows(), start_time.elapsed());
        Ok(OpStatus::NeedMoreData)
    }
    
    /// 完成处理
    fn finish(&mut self, out: &mut Outbox) -> Result<OpStatus> {
        let start_time = Instant::now();
        
        // 构建排序堆
        self.build_sort_heap()?;
        
        // 提取TopN结果
        let result_batch = self.extract_topn_results()?;
        
        if result_batch.num_rows() > 0 {
            out.send(0, result_batch)?;
        }
        
        out.emit_finish(0);
        self.base.set_finished();
        
        let duration = start_time.elapsed();
        debug!("TopN processing completed in {:?}", duration);
        
        Ok(OpStatus::Finished)
    }
    
    /// 构建排序堆
    fn build_sort_heap(&mut self) -> Result<()> {
        for (batch_idx, batch) in self.input_batches.iter().enumerate() {
            for row_idx in 0..batch.num_rows() {
                let sort_key = self.create_sort_key(batch, row_idx, batch_idx)?;
                
                if self.sort_heap.len() < self.config.limit {
                    self.sort_heap.push(sort_key);
                } else {
                    // 堆已满，比较并可能替换
                    if let Some(peek) = self.sort_heap.peek() {
                        if sort_key < *peek {
                            self.sort_heap.pop();
                            self.sort_heap.push(sort_key);
                        }
                    }
                }
            }
        }
        
        debug!("Built sort heap with {} entries", self.sort_heap.len());
        Ok(())
    }
    
    /// 创建排序键
    fn create_sort_key(&self, batch: &RecordBatch, row_idx: usize, batch_idx: usize) -> Result<SortKey> {
        let mut values = Vec::new();
        
        for sort_option in &self.config.sort_options {
            let column_index = batch.schema().fields.iter()
                .position(|f| f.name() == &sort_option.column)
                .ok_or_else(|| anyhow::anyhow!("Sort column {} not found", sort_option.column))?;
            
            let column = batch.column(column_index);
            let scalar = ScalarValue::try_from_array(column, row_idx)?;
            values.push(scalar);
        }
        
        Ok(SortKey {
            values,
            row_index: batch_idx * 10000 + row_idx, // 简单的全局行索引
        })
    }
    
    /// 提取TopN结果
    fn extract_topn_results(&mut self) -> Result<RecordBatch> {
        let mut result_rows = Vec::new();
        
        // 从堆中提取结果
        while let Some(sort_key) = self.sort_heap.pop() {
            result_rows.push(sort_key);
        }
        
        // 反转顺序以获得正确的排序
        result_rows.reverse();
        
        if result_rows.is_empty() {
            // 返回空批次
            let empty_batch = RecordBatch::new_empty(self.config.input_schema.clone());
            return Ok(empty_batch);
        }
        
        // 构建结果批次
        self.build_result_batch(&result_rows)
    }
    
    /// 构建结果批次
    fn build_result_batch(&self, result_rows: &[SortKey]) -> Result<RecordBatch> {
        let mut result_columns = Vec::new();
        
        for field in self.config.input_schema.fields() {
            let mut column_values = Vec::new();
            
            for sort_key in result_rows {
                let batch_idx = sort_key.row_index / 10000;
                let row_idx = sort_key.row_index % 10000;
                let batch = &self.input_batches[batch_idx];
                let column = batch.column_by_name(field.name()).unwrap();
                let scalar = ScalarValue::try_from_array(column, row_idx)?;
                column_values.push(scalar);
            }
            
            let array = ScalarValue::iter_to_array(column_values.into_iter())?;
            result_columns.push(array);
        }
        
        let result_batch = RecordBatch::try_new(
            self.config.input_schema.clone(),
            result_columns,
        )?;
        
        Ok(result_batch)
    }
}

impl Operator for VectorizedTopN {
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

/// TopN优化器
pub struct TopNOptimizer {
    /// 统计信息收集器
    stats_collector: std::collections::HashMap<String, TopNStats>,
}

/// TopN统计信息
#[derive(Debug, Clone)]
struct TopNStats {
    /// 输入行数
    input_rows: u64,
    /// 输出行数
    output_rows: u64,
    /// 排序列数
    sort_columns: usize,
    /// 处理时间
    process_time: std::time::Duration,
}

impl TopNOptimizer {
    /// 创建新的优化器
    pub fn new() -> Self {
        Self {
            stats_collector: std::collections::HashMap::new(),
        }
    }
    
    /// 选择最佳TopN算法
    pub fn choose_topn_algorithm(
        &self,
        input_rows: u64,
        limit: usize,
        sort_columns: usize,
    ) -> TopNAlgorithm {
        if input_rows <= limit as u64 * 2 {
            TopNAlgorithm::FullSort
        } else if sort_columns == 1 {
            TopNAlgorithm::HeapSort
        } else {
            TopNAlgorithm::PartialSort
        }
    }
    
    /// 更新统计信息
    pub fn update_stats(&mut self, topn_id: String, stats: TopNStats) {
        self.stats_collector.insert(topn_id, stats);
    }
}

/// TopN算法
#[derive(Debug, Clone)]
pub enum TopNAlgorithm {
    /// 完全排序
    FullSort,
    /// 堆排序
    HeapSort,
    /// 部分排序
    PartialSort,
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::*;
    use arrow::datatypes::*;
    
    #[test]
    fn test_topn_basic() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]);
        
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int32Array::from(vec![3, 1, 4, 1, 5])),
                Arc::new(Int32Array::from(vec![30, 10, 40, 10, 50])),
            ],
        ).unwrap();
        
        let sort_options = vec![SortOptions {
            column: "value".to_string(),
            ascending: false, // 降序
            nulls_first: true,
        }];
        
        let config = TopNConfig::new(
            Arc::new(schema),
            sort_options,
            3, // 前3项
        );
        
        let mut topn = VectorizedTopN::new(
            config,
            1,
            vec![0],
            vec![0],
            "test_topn".to_string(),
        ).unwrap();
        
        let mut out = crate::execution::push_runtime::Outbox::new();
        let status = topn.process_batch(batch, &mut out);
        assert!(status.is_ok());
    }
}
