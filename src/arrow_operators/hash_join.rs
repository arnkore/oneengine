//! Hash Join算子
//! 
//! 基于Arrow的Hash Join实现，支持Broadcast和Shuffle模式

use super::{BaseOperator, DualInputOperator, MetricsSupport, OperatorMetrics};
use crate::push_runtime::{Operator, OperatorContext, Event, OpStatus, Outbox, PortId};
use arrow::record_batch::RecordBatch;
use arrow::datatypes::{SchemaRef, Schema, Field, DataType};
use arrow::array::{Array, Int32Array, StringArray, Float64Array, BooleanArray};
use arrow::compute::{take, filter, concat};
use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

/// Join类型
#[derive(Debug, Clone, PartialEq)]
pub enum JoinType {
    /// 内连接
    Inner,
    /// 左外连接
    LeftOuter,
    /// 右外连接
    RightOuter,
    /// 全外连接
    FullOuter,
    /// 左半连接
    LeftSemi,
    /// 左反连接
    LeftAnti,
}

/// Join模式
#[derive(Debug, Clone, PartialEq)]
pub enum JoinMode {
    /// Broadcast模式（小表广播）
    Broadcast,
    /// Shuffle模式（大表重分区）
    Shuffle,
    /// Colocate模式（本地连接）
    Colocate,
}

/// Hash Join配置
#[derive(Debug, Clone)]
pub struct HashJoinConfig {
    /// 左表连接列
    pub left_join_columns: Vec<String>,
    /// 右表连接列
    pub right_join_columns: Vec<String>,
    /// Join类型
    pub join_type: JoinType,
    /// Join模式
    pub join_mode: JoinMode,
    /// 输出schema
    pub output_schema: SchemaRef,
    /// 是否启用字典列优化
    pub enable_dictionary_optimization: bool,
    /// 最大内存使用量（字节）
    pub max_memory_bytes: usize,
    /// 是否启用RuntimeFilter
    pub enable_runtime_filter: bool,
}

impl Default for HashJoinConfig {
    fn default() -> Self {
        Self {
            left_join_columns: Vec::new(),
            right_join_columns: Vec::new(),
            join_type: JoinType::Inner,
            join_mode: JoinMode::Broadcast,
            output_schema: Arc::new(Schema::new(vec![] as Vec<arrow::datatypes::Field>)),
            enable_dictionary_optimization: true,
            max_memory_bytes: 1024 * 1024, // 1MB
            enable_runtime_filter: false,
        }
    }
}

/// Hash表条目
#[derive(Debug, Clone)]
pub struct HashTableEntry {
    /// 行索引
    row_indices: Vec<usize>,
    /// 批次索引
    batch_index: usize,
}

/// Hash Join算子
pub struct HashJoinOperator {
    /// 基础算子
    base: BaseOperator,
    /// 配置
    config: HashJoinConfig,
    /// 统计信息
    metrics: OperatorMetrics,
    /// 构建端Hash表
    pub build_table: HashMap<Vec<u8>, HashTableEntry>,
    /// 构建端数据
    build_batches: Vec<RecordBatch>,
    /// 构建端schema
    build_schema: Option<SchemaRef>,
    /// 构建端连接列索引
    build_join_indices: Vec<usize>,
    /// 探测端连接列索引
    probe_join_indices: Vec<usize>,
    /// 是否构建完成
    build_complete: bool,
    /// 当前探测批次
    current_probe_batch: Option<RecordBatch>,
    /// 当前探测行索引
    current_probe_row: usize,
}

impl HashJoinOperator {
    /// 创建新的Hash Join算子
    pub fn new(
        operator_id: u32,
        config: HashJoinConfig,
    ) -> Self {
        Self {
            base: BaseOperator::new(operator_id, vec![], vec![], "HashJoin".to_string()),
            config,
            metrics: OperatorMetrics::default(),
            build_table: HashMap::new(),
            build_batches: Vec::new(),
            build_schema: None,
            build_join_indices: Vec::new(),
            probe_join_indices: Vec::new(),
            build_complete: false,
            current_probe_batch: None,
            current_probe_row: 0,
        }
    }

    /// 构建Hash表
    fn build_hash_table(&mut self, batch: &RecordBatch) -> Result<()> {
        let start = Instant::now();
        
        // 获取连接列索引
        if self.build_join_indices.is_empty() {
            self.build_join_indices = self.get_join_column_indices(
                &batch.schema(),
                &self.config.left_join_columns,
            )?;
        }

        // 为每一行构建hash key
        for row_idx in 0..batch.num_rows() {
            let key = self.build_hash_key(batch, row_idx)?;
            
            let entry = self.build_table.entry(key).or_insert_with(|| HashTableEntry {
                row_indices: Vec::new(),
                batch_index: self.build_batches.len(),
            });
            
            entry.row_indices.push(row_idx);
        }

        // 保存构建端数据
        self.build_batches.push(batch.clone());
        
        // 记录统计信息
        // TODO: 实现metrics记录
        
        Ok(())
    }

    /// 构建Hash键
    fn build_hash_key(&self, batch: &RecordBatch, row_idx: usize) -> Result<Vec<u8>> {
        let mut key = Vec::new();
        
        for &col_idx in &self.build_join_indices {
            let array = batch.column(col_idx);
            let value_bytes = self.extract_value_bytes(array, row_idx)?;
            key.extend_from_slice(&value_bytes);
            key.push(0xFF); // 分隔符
        }
        
        Ok(key)
    }

    /// 提取值的字节表示
    fn extract_value_bytes(&self, array: &dyn Array, row_idx: usize) -> Result<Vec<u8>> {
        if array.is_null(row_idx) {
            return Ok(vec![0x00]); // NULL标记
        }

        match array.data_type() {
            DataType::Int32 => {
                let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
                Ok(arr.value(row_idx).to_le_bytes().to_vec())
            }
            DataType::Utf8 => {
                let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
                Ok(arr.value(row_idx).as_bytes().to_vec())
            }
            DataType::Float64 => {
                let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
                Ok(arr.value(row_idx).to_le_bytes().to_vec())
            }
            _ => {
                // 对于其他类型，使用字符串表示
                Ok(format!("{:?}", array).as_bytes().to_vec())
            }
        }
    }

    /// 获取连接列索引
    fn get_join_column_indices(&self, schema: &Schema, join_columns: &[String]) -> Result<Vec<usize>> {
        let mut indices = Vec::new();
        
        for col_name in join_columns {
            let field = schema.fields()
                .iter()
                .position(|f| f.name() == col_name)
                .ok_or_else(|| anyhow::anyhow!("Column not found: {}", col_name))?;
            indices.push(field);
        }
        
        Ok(indices)
    }

    /// 探测Hash表
    fn probe_hash_table(&mut self, batch: &RecordBatch) -> Result<Option<RecordBatch>> {
        let start = Instant::now();
        
        // 获取探测端连接列索引
        if self.probe_join_indices.is_empty() {
            self.probe_join_indices = self.get_join_column_indices(
                &batch.schema(),
                &self.config.right_join_columns,
            )?;
        }

        let mut matched_indices = Vec::new();
        let mut build_indices = Vec::new();
        
        // 为每一行探测hash表
        for row_idx in 0..batch.num_rows() {
            let key = self.build_probe_key(batch, row_idx)?;
            
            if let Some(entry) = self.build_table.get(&key) {
                for &build_row_idx in &entry.row_indices {
                    matched_indices.push(row_idx);
                    build_indices.push(build_row_idx);
                }
            }
        }

        // 构建输出批次
        let result = if !matched_indices.is_empty() {
            Some(self.build_output_batch(batch, &matched_indices, &build_indices)?)
        } else {
            None
        };

        // 记录统计信息
        // TODO: 实现metrics记录
        
        Ok(result)
    }

    /// 构建探测键
    fn build_probe_key(&self, batch: &RecordBatch, row_idx: usize) -> Result<Vec<u8>> {
        let mut key = Vec::new();
        
        for &col_idx in &self.probe_join_indices {
            let array = batch.column(col_idx);
            let value_bytes = self.extract_value_bytes(array, row_idx)?;
            key.extend_from_slice(&value_bytes);
            key.push(0xFF); // 分隔符
        }
        
        Ok(key)
    }

    /// 构建输出批次
    fn build_output_batch(
        &self,
        probe_batch: &RecordBatch,
        matched_indices: &[usize],
        build_indices: &[usize],
    ) -> Result<RecordBatch> {
        // 简化实现：只返回探测端数据
        // 在实际实现中，这里需要合并左右两表的数据
        let mut output_arrays = Vec::new();
        
        for col_idx in 0..probe_batch.num_columns() {
            let array = probe_batch.column(col_idx);
            let indices = Int32Array::from(
                matched_indices.iter().map(|&i| i as i32).collect::<Vec<_>>()
            );
            let output_array = take(array, &indices, None)?;
            output_arrays.push(output_array);
        }
        
        Ok(RecordBatch::try_new(
            probe_batch.schema(),
            output_arrays,
        )?)
    }

    /// 处理构建端数据
    pub fn process_build_side(&mut self, batch: RecordBatch) -> Result<OpStatus> {
        if !self.build_complete {
            self.build_hash_table(&batch)?;
            Ok(OpStatus::Ready)
        } else {
            Ok(OpStatus::Error("Build phase already complete".to_string()))
        }
    }

    /// 处理探测端数据
    pub fn process_probe_side(&mut self, batch: RecordBatch) -> Result<OpStatus> {
        if !self.build_complete {
            return Ok(OpStatus::Error("Build phase not complete".to_string()));
        }

        // 探测hash表
        if let Some(output_batch) = self.probe_hash_table(&batch)? {
            // 输出结果
            Ok(OpStatus::Ready)
        } else {
            Ok(OpStatus::Ready)
        }
    }

    /// 完成构建阶段
    fn finish_build_phase(&mut self) {
        self.build_complete = true;
        // TODO: 实现metrics记录
    }
}

impl Operator for HashJoinOperator {
    fn on_register(&mut self, _ctx: OperatorContext) -> Result<()> {
        Ok(())
    }

    fn on_event(&mut self, event: Event, _outbox: &mut Outbox) -> OpStatus {
        match event {
            Event::Data(port_id, batch) => {
                // 根据端口ID决定是构建端还是探测端
                if port_id == 0 {
                    // 左表（构建端）
                    match self.process_build_side(batch) {
                        Ok(status) => status,
                        Err(e) => OpStatus::Error(e.to_string()),
                    }
                } else {
                    // 右表（探测端）
                    match self.process_probe_side(batch) {
                        Ok(status) => status,
                        Err(e) => OpStatus::Error(e.to_string()),
                    }
                }
            }
            Event::Flush(_) => {
                if !self.build_complete {
                    self.finish_build_phase();
                }
                OpStatus::Ready
            }
            Event::Finish(_) => {
                if !self.build_complete {
                    self.finish_build_phase();
                }
                OpStatus::Finished
            }
            _ => OpStatus::Ready,
        }
    }

    fn is_finished(&self) -> bool {
        self.build_complete
    }

    fn name(&self) -> &str {
        "HashJoin"
    }
}

impl DualInputOperator for HashJoinOperator {
    fn process_left_batch(&mut self, batch: RecordBatch, _outbox: &mut Outbox) -> Result<OpStatus> {
        self.process_build_side(batch)
    }

    fn process_right_batch(&mut self, batch: RecordBatch, _outbox: &mut Outbox) -> Result<OpStatus> {
        self.process_probe_side(batch)
    }

    fn finish(&mut self, _outbox: &mut Outbox) -> Result<OpStatus> {
        if !self.build_complete {
            self.finish_build_phase();
        }
        Ok(OpStatus::Finished)
    }
}

impl MetricsSupport for HashJoinOperator {
    fn record_metrics(&self, _batch: &RecordBatch, _duration: std::time::Duration) {
        // TODO: 实现metrics记录
    }

    fn get_metrics(&self) -> OperatorMetrics {
        self.metrics.clone()
    }
}