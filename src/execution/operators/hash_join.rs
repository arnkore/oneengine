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

//! 向量化哈希连接算子
//! 
//! 支持等值连接，使用哈希表进行高效连接

use arrow::record_batch::RecordBatch;
use arrow::array::*;
use arrow::datatypes::*;
use arrow::compute::*;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, info, warn};
use crate::execution::push_runtime::{Operator, Event, OpStatus, Outbox, PortId};
use crate::expression::{VectorizedExpressionEngine, ExpressionEngineConfig};
use crate::expression::ast::{Expression, ColumnRef, Literal, ComparisonExpr, ComparisonOp};
use datafusion_common::ScalarValue;
use anyhow::Result;

/// 连接类型
#[derive(Debug, Clone, PartialEq)]
pub enum JoinType {
    /// 内连接
    Inner,
    /// 左外连接
    Left,
    /// 右外连接
    Right,
    /// 全外连接
    Full,
    /// 左半连接
    LeftSemi,
    /// 左反连接
    LeftAnti,
    /// 右半连接
    RightSemi,
    /// 右反连接
    RightAnti,
}

/// 连接条件
#[derive(Debug, Clone)]
pub enum JoinCondition {
    /// 等值连接条件
    EquiJoin {
        left_columns: Vec<String>,
        right_columns: Vec<String>,
    },
    /// 表达式连接条件
    Expression {
        condition: Expression,
    },
}

/// 哈希连接配置
#[derive(Debug, Clone)]
pub struct HashJoinConfig {
    /// 连接类型
    pub join_type: JoinType,
    /// 连接条件
    pub condition: JoinCondition,
    /// 左表schema
    pub left_schema: SchemaRef,
    /// 右表schema
    pub right_schema: SchemaRef,
    /// 是否启用布隆过滤器
    pub enable_bloom_filter: bool,
    /// 哈希表大小限制（字节）
    pub hash_table_size_limit: usize,
    /// 是否启用向量化处理
    pub enable_vectorization: bool,
    /// 批处理大小
    pub batch_size: usize,
}

impl HashJoinConfig {
    /// 创建新的配置
    pub fn new(
        join_type: JoinType,
        condition: JoinCondition,
        left_schema: SchemaRef,
        right_schema: SchemaRef,
    ) -> Self {
        Self {
            join_type,
            condition,
            left_schema,
            right_schema,
            enable_bloom_filter: true,
            hash_table_size_limit: 100 * 1024 * 1024, // 100MB
            enable_vectorization: true,
            batch_size: 1024,
        }
    }
    
    /// 获取输出schema
    pub fn output_schema(&self) -> SchemaRef {
        let mut fields = Vec::new();
        
        // 添加左表字段
        for field in self.left_schema.fields() {
            fields.push(field.clone());
        }
        
        // 添加右表字段（根据连接类型）
        match self.join_type {
            JoinType::Inner | JoinType::Left | JoinType::Full | JoinType::Right => {
                for field in self.right_schema.fields() {
                    fields.push(field.clone());
                }
            }
            JoinType::LeftSemi | JoinType::LeftAnti => {
                // 半连接只返回左表字段
            }
            JoinType::RightSemi | JoinType::RightAnti => {
                // 右半连接只返回右表字段
                for field in self.right_schema.fields() {
                    fields.push(field.clone());
                }
            }
        }
        
        Arc::new(Schema::new(fields))
    }
}

/// 哈希表条目
#[derive(Debug, Clone)]
struct HashTableEntry {
    /// 行索引
    row_indices: Vec<u32>,
    /// 批次索引
    batch_indices: Vec<u32>,
}

/// 向量化哈希连接算子
pub struct VectorizedHashJoin {
    /// 基础算子信息
    base: crate::execution::operators::BaseOperator,
    /// 配置
    config: HashJoinConfig,
    /// 表达式引擎
    expression_engine: VectorizedExpressionEngine,
    /// 右表哈希表
    hash_table: HashMap<Vec<ScalarValue>, HashTableEntry>,
    /// 左表批次缓存
    left_batches: Vec<RecordBatch>,
    /// 右表批次缓存
    right_batches: Vec<RecordBatch>,
    /// 左表处理完成标志
    left_finished: bool,
    /// 右表处理完成标志
    right_finished: bool,
    /// 当前处理的左表批次索引
    current_left_batch: usize,
    /// 当前处理的左表行索引
    current_left_row: usize,
    /// 统计信息
    metrics: crate::execution::operators::OperatorMetrics,
}

impl VectorizedHashJoin {
    /// 创建新的哈希连接算子
    pub fn new(
        config: HashJoinConfig,
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
            hash_table: HashMap::new(),
            left_batches: Vec::new(),
            right_batches: Vec::new(),
            left_finished: false,
            right_finished: false,
            current_left_batch: 0,
            current_left_row: 0,
            metrics: crate::execution::operators::OperatorMetrics::default(),
        })
    }
    
    /// 构建右表哈希表
    fn build_hash_table(&mut self) -> Result<()> {
        let start_time = Instant::now();
        
        for (batch_idx, batch) in self.right_batches.iter().enumerate() {
            let key_columns = self.get_join_key_columns(batch, true)?;
            let key_arrays = self.extract_key_arrays(&key_columns, batch)?;
            
            for row_idx in 0..batch.num_rows() {
                let key = self.extract_key_values(&key_arrays, row_idx)?;
                
                let entry = self.hash_table.entry(key).or_insert_with(|| HashTableEntry {
                    row_indices: Vec::new(),
                    batch_indices: Vec::new(),
                });
                
                entry.row_indices.push(row_idx as u32);
                entry.batch_indices.push(batch_idx as u32);
            }
        }
        
        let duration = start_time.elapsed();
        debug!("Built hash table with {} entries in {:?}", self.hash_table.len(), duration);
        
        Ok(())
    }
    
    /// 获取连接键列
    fn get_join_key_columns(&self, batch: &RecordBatch, is_right: bool) -> Result<Vec<String>> {
        match &self.config.condition {
            JoinCondition::EquiJoin { left_columns, right_columns } => {
                Ok(if is_right { right_columns.clone() } else { left_columns.clone() })
            }
            JoinCondition::Expression { .. } => {
                // 对于表达式连接，需要从表达式中提取列
                todo!("Expression join key extraction not implemented yet")
            }
        }
    }
    
    /// 提取键数组
    fn extract_key_arrays(&self, key_columns: &[String], batch: &RecordBatch) -> Result<Vec<ArrayRef>> {
        let mut key_arrays = Vec::new();
        
        for column_name in key_columns {
            let column_index = batch.schema().fields.iter()
                .position(|f| f.name() == column_name)
                .ok_or_else(|| anyhow::anyhow!("Column {} not found", column_name))?;
            
            key_arrays.push(batch.column(column_index).clone());
        }
        
        Ok(key_arrays)
    }
    
    /// 提取键值
    fn extract_key_values(&self, key_arrays: &[ArrayRef], row_idx: usize) -> Result<Vec<ScalarValue>> {
        let mut key_values = Vec::new();
        
        for array in key_arrays {
            let scalar = ScalarValue::try_from_array(array, row_idx)?;
            key_values.push(scalar);
        }
        
        Ok(key_values)
    }
    
    /// 处理左表批次
    fn process_left_batch(&mut self, batch: RecordBatch, out: &mut Outbox) -> Result<OpStatus> {
        let start_time = Instant::now();
        
        // 如果右表还未处理完成，缓存左表批次
        if !self.right_finished {
            self.left_batches.push(batch);
            return Ok(OpStatus::NeedMoreData);
        }
        
        // 如果哈希表还未构建，先构建
        if self.hash_table.is_empty() {
            self.build_hash_table()?;
        }
        
        // 处理左表批次
        let result_batches = self.join_left_batch(&batch)?;
        
        for result_batch in result_batches {
            out.send(0, result_batch)?;
        }
        
        self.metrics.update_batch(batch.num_rows(), start_time.elapsed());
        Ok(OpStatus::NeedMoreData)
    }
    
    /// 处理右表批次
    fn process_right_batch(&mut self, batch: RecordBatch, out: &mut Outbox) -> Result<OpStatus> {
        // 缓存右表批次
        self.right_batches.push(batch);
        Ok(OpStatus::NeedMoreData)
    }
    
    /// 连接左表批次
    fn join_left_batch(&self, left_batch: &RecordBatch) -> Result<Vec<RecordBatch>> {
        let mut result_batches = Vec::new();
        let key_columns = self.get_join_key_columns(left_batch, false)?;
        let key_arrays = self.extract_key_arrays(&key_columns, left_batch)?;
        
        let mut matched_rows = Vec::new();
        let mut matched_right_rows = Vec::new();
        let mut matched_right_batches = Vec::new();
        
        for row_idx in 0..left_batch.num_rows() {
            let key = self.extract_key_values(&key_arrays, row_idx)?;
            
            if let Some(entry) = self.hash_table.get(&key) {
                // 找到匹配
                for (i, &right_row_idx) in entry.row_indices.iter().enumerate() {
                    matched_rows.push(row_idx as u32);
                    matched_right_rows.push(right_row_idx);
                    matched_right_batches.push(entry.batch_indices[i]);
                }
            } else if matches!(self.config.join_type, JoinType::Left | JoinType::Full) {
                // 左外连接或全外连接，左表行没有匹配
                matched_rows.push(row_idx as u32);
                matched_right_rows.push(u32::MAX); // 标记为NULL
                matched_right_batches.push(u32::MAX);
            }
        }
        
        if !matched_rows.is_empty() {
            let result_batch = self.build_result_batch(
                left_batch,
                &matched_rows,
                &matched_right_rows,
                &matched_right_batches,
            )?;
            result_batches.push(result_batch);
        }
        
        Ok(result_batches)
    }
    
    /// 构建结果批次
    fn build_result_batch(
        &self,
        left_batch: &RecordBatch,
        matched_rows: &[u32],
        matched_right_rows: &[u32],
        matched_right_batches: &[u32],
    ) -> Result<RecordBatch> {
        let mut left_columns = Vec::new();
        let mut right_columns = Vec::new();
        
        // 提取左表列
        for field in left_batch.schema().fields() {
            let column = left_batch.column_by_name(field.name()).unwrap();
            let selected_column = take(column, &UInt32Array::from(matched_rows), None)?;
            left_columns.push(selected_column);
        }
        
        // 提取右表列
        if !matches!(self.config.join_type, JoinType::LeftSemi | JoinType::LeftAnti) {
            for field in self.config.right_schema.fields() {
                let mut right_values = Vec::new();
                
                for (i, &right_row_idx) in matched_right_rows.iter().enumerate() {
                    if right_row_idx == u32::MAX {
                        // NULL值
                        right_values.push(ScalarValue::try_from(field.data_type())?);
                    } else {
                        let right_batch = &self.right_batches[matched_right_batches[i] as usize];
                        let right_column = right_batch.column_by_name(field.name()).unwrap();
                        let scalar = ScalarValue::try_from_array(right_column, right_row_idx as usize)?;
                        right_values.push(scalar);
                    }
                }
                
                let right_array = ScalarValue::iter_to_array(right_values.into_iter())?;
                right_columns.push(right_array);
            }
        }
        
        // 合并列
        let mut all_columns = left_columns;
        all_columns.extend(right_columns);
        
        let output_schema = self.config.output_schema();
        let result_batch = RecordBatch::try_new(output_schema, all_columns)?;
        
        Ok(result_batch)
    }
    
    /// 完成处理
    fn finish(&mut self, out: &mut Outbox) -> Result<OpStatus> {
        if !self.left_finished {
            // 处理剩余的左表批次
            while self.current_left_batch < self.left_batches.len() {
                let batch = &self.left_batches[self.current_left_batch];
                let result_batches = self.join_left_batch(batch)?;
                
                for result_batch in result_batches {
                    out.send(0, result_batch)?;
                }
                
                self.current_left_batch += 1;
            }
            self.left_finished = true;
        }
        
        if self.left_finished && self.right_finished {
            out.emit_finish(0);
            self.base.set_finished();
            Ok(OpStatus::Finished)
        } else {
            Ok(OpStatus::NeedMoreData)
        }
    }
}

impl Operator for VectorizedHashJoin {
    fn on_event(&mut self, event: Event, out: &mut Outbox) -> Result<OpStatus> {
        match event {
            Event::Data { port, batch } => {
                if port == 0 {
                    // 左表数据
                    self.process_left_batch(batch, out)
                } else {
                    // 右表数据
                    self.process_right_batch(batch, out)
                }
            }
            Event::Flush { port } => {
                if port == 1 {
                    // 右表完成，开始处理左表
                    self.right_finished = true;
                    if !self.left_batches.is_empty() {
                        self.build_hash_table()?;
                    }
                }
                Ok(OpStatus::NeedMoreData)
            }
            Event::Finish { port } => {
                if port == 0 {
                    self.left_finished = true;
                } else {
                    self.right_finished = true;
                }
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

/// 哈希连接优化器
pub struct HashJoinOptimizer {
    /// 统计信息收集器
    stats_collector: HashMap<String, JoinStats>,
}

/// 连接统计信息
#[derive(Debug, Clone)]
struct JoinStats {
    /// 左表行数
    left_rows: u64,
    /// 右表行数
    right_rows: u64,
    /// 连接选择性
    selectivity: f64,
    /// 平均匹配数
    avg_matches: f64,
}

impl HashJoinOptimizer {
    /// 创建新的优化器
    pub fn new() -> Self {
        Self {
            stats_collector: HashMap::new(),
        }
    }
    
    /// 选择最佳连接算法
    pub fn choose_join_algorithm(
        &self,
        left_rows: u64,
        right_rows: u64,
        memory_limit: usize,
    ) -> JoinAlgorithm {
        let total_memory = (left_rows + right_rows) * 100; // 估算每行100字节
        
        if total_memory < memory_limit {
            JoinAlgorithm::HashJoin
        } else if left_rows < right_rows {
            JoinAlgorithm::NestedLoopJoin
        } else {
            JoinAlgorithm::SortMergeJoin
        }
    }
    
    /// 更新统计信息
    pub fn update_stats(&mut self, join_id: String, stats: JoinStats) {
        self.stats_collector.insert(join_id, stats);
    }
}

/// 连接算法
#[derive(Debug, Clone)]
pub enum JoinAlgorithm {
    /// 哈希连接
    HashJoin,
    /// 嵌套循环连接
    NestedLoopJoin,
    /// 排序合并连接
    SortMergeJoin,
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::*;
    use arrow::datatypes::*;
    
    #[test]
    fn test_hash_join_inner() {
        // 创建测试数据
        let left_schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]);
        
        let right_schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]);
        
        let left_batch = RecordBatch::try_new(
            Arc::new(left_schema.clone()),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            ],
        ).unwrap();
        
        let right_batch = RecordBatch::try_new(
            Arc::new(right_schema.clone()),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 4])),
                Arc::new(Int32Array::from(vec![100, 200, 400])),
            ],
        ).unwrap();
        
        let config = HashJoinConfig::new(
            JoinType::Inner,
            JoinCondition::EquiJoin {
                left_columns: vec!["id".to_string()],
                right_columns: vec!["id".to_string()],
            },
            Arc::new(left_schema),
            Arc::new(right_schema),
        );
        
        let mut join = VectorizedHashJoin::new(
            config,
            1,
            vec![0, 1],
            vec![0],
            "test_join".to_string(),
        ).unwrap();
        
        // 测试右表处理
        let mut out = crate::execution::push_runtime::Outbox::new();
        let status = join.process_right_batch(right_batch, &mut out);
        assert!(status.is_ok());
        
        // 测试左表处理
        let status = join.process_left_batch(left_batch, &mut out);
        assert!(status.is_ok());
    }
}
