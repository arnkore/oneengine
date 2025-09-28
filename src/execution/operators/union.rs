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

//! 向量化集合并集算子
//! 
//! 支持UNION和UNION ALL操作

use arrow::record_batch::RecordBatch;
use arrow::array::*;
use arrow::datatypes::*;
use arrow::compute::*;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, info, warn};
use crate::execution::push_runtime::{Operator, Event, OpStatus, Outbox, PortId};
use crate::expression::{VectorizedExpressionEngine, ExpressionEngineConfig};
use anyhow::Result;

/// 联合类型
#[derive(Debug, Clone, PartialEq)]
pub enum UnionType {
    /// UNION - 去重
    Union,
    /// UNION ALL - 不去重
    UnionAll,
}

/// 联合配置
#[derive(Debug, Clone)]
pub struct UnionConfig {
    /// 输入schema列表
    pub input_schemas: Vec<SchemaRef>,
    /// 输出schema
    pub output_schema: SchemaRef,
    /// 联合类型
    pub union_type: UnionType,
    /// 是否启用向量化处理
    pub enable_vectorization: bool,
    /// 批处理大小
    pub batch_size: usize,
    /// 内存限制
    pub memory_limit: usize,
}

impl UnionConfig {
    /// 创建新的配置
    pub fn new(
        input_schemas: Vec<SchemaRef>,
        union_type: UnionType,
    ) -> Result<Self> {
        if input_schemas.is_empty() {
            return Err(anyhow::anyhow!("At least one input schema required"));
        }
        
        // 验证所有schema兼容
        let output_schema = Self::validate_and_merge_schemas(&input_schemas)?;
        
        Ok(Self {
            input_schemas,
            output_schema,
            union_type,
            enable_vectorization: true,
            batch_size: 1024,
            memory_limit: 100 * 1024 * 1024, // 100MB
        })
    }
    
    /// 验证并合并schema
    fn validate_and_merge_schemas(input_schemas: &[SchemaRef]) -> Result<SchemaRef> {
        let first_schema = &input_schemas[0];
        let mut merged_fields = first_schema.fields().clone();
        
        for schema in input_schemas.iter().skip(1) {
            if schema.fields().len() != first_schema.fields().len() {
                return Err(anyhow::anyhow!(
                    "Schema field count mismatch: {} vs {}",
                    schema.fields().len(),
                    first_schema.fields().len()
                ));
            }
            
            for (i, (field1, field2)) in first_schema.fields().iter().zip(schema.fields().iter()).enumerate() {
                if field1.name() != field2.name() {
                    return Err(anyhow::anyhow!(
                        "Schema field name mismatch at position {}: {} vs {}",
                        i,
                        field1.name(),
                        field2.name()
                    ));
                }
                
                // 合并数据类型
                let merged_type = Self::merge_data_types(field1.data_type(), field2.data_type())?;
                merged_fields[i] = Field::new(
                    field1.name(),
                    merged_type,
                    field1.is_nullable() || field2.is_nullable(),
                );
            }
        }
        
        Ok(Arc::new(Schema::new(merged_fields)))
    }
    
    /// 合并数据类型
    fn merge_data_types(left: &DataType, right: &DataType) -> Result<DataType> {
        match (left, right) {
            (DataType::Int8, DataType::Int8) => Ok(DataType::Int8),
            (DataType::Int16, DataType::Int16) => Ok(DataType::Int16),
            (DataType::Int32, DataType::Int32) => Ok(DataType::Int32),
            (DataType::Int64, DataType::Int64) => Ok(DataType::Int64),
            (DataType::UInt8, DataType::UInt8) => Ok(DataType::UInt8),
            (DataType::UInt16, DataType::UInt16) => Ok(DataType::UInt16),
            (DataType::UInt32, DataType::UInt32) => Ok(DataType::UInt32),
            (DataType::UInt64, DataType::UInt64) => Ok(DataType::UInt64),
            (DataType::Float32, DataType::Float32) => Ok(DataType::Float32),
            (DataType::Float64, DataType::Float64) => Ok(DataType::Float64),
            (DataType::Utf8, DataType::Utf8) => Ok(DataType::Utf8),
            (DataType::LargeUtf8, DataType::LargeUtf8) => Ok(DataType::LargeUtf8),
            (DataType::Boolean, DataType::Boolean) => Ok(DataType::Boolean),
            (DataType::Date32, DataType::Date32) => Ok(DataType::Date32),
            (DataType::Date64, DataType::Date64) => Ok(DataType::Date64),
            (DataType::Timestamp(unit1, tz1), DataType::Timestamp(unit2, tz2)) => {
                if unit1 == unit2 && tz1 == tz2 {
                    Ok(DataType::Timestamp(*unit1, tz1.clone()))
                } else {
                    // 转换为更通用的类型
                    Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
                }
            }
            (DataType::Int8, DataType::Int16) | (DataType::Int16, DataType::Int8) => Ok(DataType::Int16),
            (DataType::Int16, DataType::Int32) | (DataType::Int32, DataType::Int16) => Ok(DataType::Int32),
            (DataType::Int32, DataType::Int64) | (DataType::Int64, DataType::Int32) => Ok(DataType::Int64),
            (DataType::Float32, DataType::Float64) | (DataType::Float64, DataType::Float32) => Ok(DataType::Float64),
            (DataType::Utf8, DataType::LargeUtf8) | (DataType::LargeUtf8, DataType::Utf8) => Ok(DataType::LargeUtf8),
            _ => {
                // 默认转换为字符串类型
                Ok(DataType::Utf8)
            }
        }
    }
}

/// 向量化集合并集算子
pub struct VectorizedUnion {
    /// 基础算子信息
    base: crate::execution::operators::BaseOperator,
    /// 配置
    config: UnionConfig,
    /// 表达式引擎
    expression_engine: VectorizedExpressionEngine,
    /// 输入批次缓存
    input_batches: Vec<Vec<RecordBatch>>,
    /// 当前输入索引
    current_input: usize,
    /// 当前批次索引
    current_batch: usize,
    /// 去重集合（用于UNION）
    distinct_set: std::collections::HashSet<Vec<u8>>,
    /// 统计信息
    metrics: crate::execution::operators::OperatorMetrics,
}

impl VectorizedUnion {
    /// 创建新的集合并集算子
    pub fn new(
        config: UnionConfig,
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
        
        let input_count = config.input_schemas.len();
        
        Ok(Self {
            base: crate::execution::operators::BaseOperator::new(
                operator_id,
                input_ports,
                output_ports,
                name,
            ),
            config,
            expression_engine,
            input_batches: vec![Vec::new(); input_count],
            current_input: 0,
            current_batch: 0,
            distinct_set: std::collections::HashSet::new(),
            metrics: crate::execution::operators::OperatorMetrics::default(),
        })
    }
    
    /// 处理输入批次
    fn process_batch(&mut self, batch: RecordBatch, input_port: PortId, out: &mut Outbox) -> Result<OpStatus> {
        let start_time = Instant::now();
        
        // 缓存输入批次
        let input_idx = input_port as usize;
        if input_idx < self.input_batches.len() {
            self.input_batches[input_idx].push(batch);
        }
        
        self.metrics.update_batch(batch.num_rows(), start_time.elapsed());
        Ok(OpStatus::NeedMoreData)
    }
    
    /// 完成处理
    fn finish(&mut self, out: &mut Outbox) -> Result<OpStatus> {
        let start_time = Instant::now();
        
        // 合并所有输入
        let result_batches = self.merge_all_inputs()?;
        
        // 输出结果
        for batch in result_batches {
            out.send(0, batch)?;
        }
        
        out.emit_finish(0);
        self.base.set_finished();
        
        let duration = start_time.elapsed();
        debug!("Union processing completed in {:?}", duration);
        
        Ok(OpStatus::Finished)
    }
    
    /// 合并所有输入
    fn merge_all_inputs(&mut self) -> Result<Vec<RecordBatch>> {
        let mut result_batches = Vec::new();
        
        for input_idx in 0..self.input_batches.len() {
            for batch in &self.input_batches[input_idx] {
                let converted_batch = self.convert_batch_schema(batch)?;
                
                match self.config.union_type {
                    UnionType::Union => {
                        // 去重合并
                        let distinct_batch = self.apply_distinct(&converted_batch)?;
                        if distinct_batch.num_rows() > 0 {
                            result_batches.push(distinct_batch);
                        }
                    }
                    UnionType::UnionAll => {
                        // 直接合并
                        result_batches.push(converted_batch);
                    }
                }
            }
        }
        
        Ok(result_batches)
    }
    
    /// 转换批次schema
    fn convert_batch_schema(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let mut converted_columns = Vec::new();
        
        for (i, field) in self.config.output_schema.fields().iter().enumerate() {
            let input_field = &batch.schema().fields()[i];
            
            if input_field.data_type() == field.data_type() {
                // 类型相同，直接使用
                converted_columns.push(batch.column(i).clone());
            } else {
                // 需要类型转换
                let converted_column = self.convert_column_type(
                    batch.column(i),
                    input_field.data_type(),
                    field.data_type(),
                )?;
                converted_columns.push(converted_column);
            }
        }
        
        let result_batch = RecordBatch::try_new(
            self.config.output_schema.clone(),
            converted_columns,
        )?;
        
        Ok(result_batch)
    }
    
    /// 转换列类型
    fn convert_column_type(
        &self,
        column: &ArrayRef,
        from_type: &DataType,
        to_type: &DataType,
    ) -> Result<ArrayRef> {
        match (from_type, to_type) {
            (DataType::Int8, DataType::Int16) => {
                let array = column.as_any().downcast_ref::<Int8Array>().unwrap();
                let converted: Int16Array = array.iter().map(|v| v.map(|v| v as i16)).collect();
                Ok(Arc::new(converted))
            }
            (DataType::Int16, DataType::Int32) => {
                let array = column.as_any().downcast_ref::<Int16Array>().unwrap();
                let converted: Int32Array = array.iter().map(|v| v.map(|v| v as i32)).collect();
                Ok(Arc::new(converted))
            }
            (DataType::Int32, DataType::Int64) => {
                let array = column.as_any().downcast_ref::<Int32Array>().unwrap();
                let converted: Int64Array = array.iter().map(|v| v.map(|v| v as i64)).collect();
                Ok(Arc::new(converted))
            }
            (DataType::Float32, DataType::Float64) => {
                let array = column.as_any().downcast_ref::<Float32Array>().unwrap();
                let converted: Float64Array = array.iter().map(|v| v.map(|v| v as f64)).collect();
                Ok(Arc::new(converted))
            }
            (DataType::Utf8, DataType::LargeUtf8) => {
                let array = column.as_any().downcast_ref::<StringArray>().unwrap();
                let converted: LargeStringArray = array.iter().map(|v| v.map(|v| v.to_string())).collect();
                Ok(Arc::new(converted))
            }
            _ => {
                // 默认转换为字符串
                let string_array = cast(column, &DataType::Utf8)?;
                Ok(string_array)
            }
        }
    }
    
    /// 应用去重
    fn apply_distinct(&mut self, batch: &RecordBatch) -> Result<RecordBatch> {
        let mut distinct_rows = Vec::new();
        let mut distinct_indices = Vec::new();
        
        for row_idx in 0..batch.num_rows() {
            let row_key = self.create_row_key(batch, row_idx)?;
            
            if self.distinct_set.insert(row_key) {
                // 新行，添加到结果
                distinct_indices.push(row_idx as u32);
            }
        }
        
        if distinct_indices.is_empty() {
            // 没有新行，返回空批次
            return Ok(RecordBatch::new_empty(self.config.output_schema.clone()));
        }
        
        // 构建去重后的批次
        self.build_distinct_batch(batch, &distinct_indices)
    }
    
    /// 创建行键
    fn create_row_key(&self, batch: &RecordBatch, row_idx: usize) -> Result<Vec<u8>> {
        let mut key_bytes = Vec::new();
        
        for column in batch.columns() {
            let scalar = datafusion_common::ScalarValue::try_from_array(column, row_idx)?;
            let value_bytes = self.serialize_scalar_value(&scalar)?;
            key_bytes.extend_from_slice(&value_bytes);
            key_bytes.push(0); // 分隔符
        }
        
        Ok(key_bytes)
    }
    
    /// 序列化标量值
    fn serialize_scalar_value(&self, scalar: &datafusion_common::ScalarValue) -> Result<Vec<u8>> {
        match scalar {
            datafusion_common::ScalarValue::Int8(Some(v)) => Ok(v.to_le_bytes().to_vec()),
            datafusion_common::ScalarValue::Int16(Some(v)) => Ok(v.to_le_bytes().to_vec()),
            datafusion_common::ScalarValue::Int32(Some(v)) => Ok(v.to_le_bytes().to_vec()),
            datafusion_common::ScalarValue::Int64(Some(v)) => Ok(v.to_le_bytes().to_vec()),
            datafusion_common::ScalarValue::UInt8(Some(v)) => Ok(v.to_le_bytes().to_vec()),
            datafusion_common::ScalarValue::UInt16(Some(v)) => Ok(v.to_le_bytes().to_vec()),
            datafusion_common::ScalarValue::UInt32(Some(v)) => Ok(v.to_le_bytes().to_vec()),
            datafusion_common::ScalarValue::UInt64(Some(v)) => Ok(v.to_le_bytes().to_vec()),
            datafusion_common::ScalarValue::Float32(Some(v)) => Ok(v.to_le_bytes().to_vec()),
            datafusion_common::ScalarValue::Float64(Some(v)) => Ok(v.to_le_bytes().to_vec()),
            datafusion_common::ScalarValue::Utf8(Some(v)) => Ok(v.as_bytes().to_vec()),
            datafusion_common::ScalarValue::LargeUtf8(Some(v)) => Ok(v.as_bytes().to_vec()),
            datafusion_common::ScalarValue::Boolean(Some(v)) => Ok(vec![if *v { 1 } else { 0 }]),
            _ => Ok(vec![]), // NULL值
        }
    }
    
    /// 构建去重批次
    fn build_distinct_batch(
        &self,
        batch: &RecordBatch,
        distinct_indices: &[u32],
    ) -> Result<RecordBatch> {
        let mut result_columns = Vec::new();
        
        for column in batch.columns() {
            let selected_column = take(column, &UInt32Array::from(distinct_indices), None)?;
            result_columns.push(selected_column);
        }
        
        let result_batch = RecordBatch::try_new(
            batch.schema().clone(),
            result_columns,
        )?;
        
        Ok(result_batch)
    }
}

impl Operator for VectorizedUnion {
    fn on_event(&mut self, event: Event, out: &mut Outbox) -> Result<OpStatus> {
        match event {
            Event::Data { port, batch } => {
                self.process_batch(batch, port, out)
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::*;
    use arrow::datatypes::*;
    
    #[test]
    fn test_union_all() {
        let schema1 = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]);
        
        let schema2 = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]);
        
        let batch1 = RecordBatch::try_new(
            Arc::new(schema1),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["Alice", "Bob"])),
            ],
        ).unwrap();
        
        let batch2 = RecordBatch::try_new(
            Arc::new(schema2),
            vec![
                Arc::new(Int32Array::from(vec![3, 4])),
                Arc::new(StringArray::from(vec!["Charlie", "David"])),
            ],
        ).unwrap();
        
        let config = UnionConfig::new(
            vec![batch1.schema().clone(), batch2.schema().clone()],
            UnionType::UnionAll,
        ).unwrap();
        
        let mut union = VectorizedUnion::new(
            config,
            1,
            vec![0, 1],
            vec![0],
            "test_union".to_string(),
        ).unwrap();
        
        let mut out = crate::execution::push_runtime::Outbox::new();
        let status = union.process_batch(batch1, 0, &mut out);
        assert!(status.is_ok());
    }
}
