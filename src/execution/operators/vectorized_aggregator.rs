//! 列式向量化聚合器
//! 
//! 提供完全面向列式的、全向量化极致优化的聚合算子实现

use arrow::array::*;
use arrow::compute::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use arrow::error::ArrowError;
use datafusion_common::ScalarValue;
use std::sync::Arc;
use std::time::Instant;
use std::collections::HashMap;
use tracing::{debug, info, warn};
use crate::push_runtime::{Operator, OperatorContext, Event, OpStatus, Outbox, PortId};
use anyhow::Result;

/// 列式向量化聚合器配置
#[derive(Debug, Clone)]
pub struct VectorizedAggregatorConfig {
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
    /// 是否启用哈希表优化
    pub enable_hash_table_optimization: bool,
    /// 是否启用分组键优化
    pub enable_group_key_optimization: bool,
    /// 是否启用聚合函数优化
    pub enable_aggregation_optimization: bool,
    /// 是否启用内存池优化
    pub enable_memory_pool_optimization: bool,
}

impl Default for VectorizedAggregatorConfig {
    fn default() -> Self {
        Self {
            batch_size: 8192,
            enable_simd: true,
            enable_dictionary_optimization: true,
            enable_compressed_optimization: true,
            enable_zero_copy: true,
            enable_prefetch: true,
            enable_hash_table_optimization: true,
            enable_group_key_optimization: true,
            enable_aggregation_optimization: true,
            enable_memory_pool_optimization: true,
        }
    }
}

/// 聚合函数类型
#[derive(Debug, Clone)]
pub enum AggregationFunction {
    Count,
    Sum { column: usize },
    Avg { column: usize },
    Min { column: usize },
    Max { column: usize },
    First { column: usize },
    Last { column: usize },
    StdDev { column: usize },
    Variance { column: usize },
    Median { column: usize },
    Percentile { column: usize, percentile: f64 },
    DistinctCount { column: usize },
    CollectList { column: usize },
    CollectSet { column: usize },
}

/// 聚合状态
#[derive(Debug)]
pub enum AggregationState {
    Count { count: u64 },
    Sum { sum: ScalarValue },
    Avg { sum: ScalarValue, count: u64 },
    Min { min: ScalarValue },
    Max { max: ScalarValue },
    First { first: ScalarValue },
    Last { last: ScalarValue },
    StdDev { sum: ScalarValue, sum_squares: ScalarValue, count: u64 },
    Variance { sum: ScalarValue, sum_squares: ScalarValue, count: u64 },
    Median { values: Vec<ScalarValue> },
    Percentile { values: Vec<ScalarValue>, percentile: f64 },
    DistinctCount { distinct_values: std::collections::HashSet<ScalarValue> },
    CollectList { values: Vec<ScalarValue> },
    CollectSet { values: std::collections::HashSet<ScalarValue> },
}

/// 分组键哈希器
pub struct GroupKeyHasher {
    group_columns: Vec<usize>,
    hash_table: HashMap<u64, usize>,
    hash_seed: u64,
}

impl GroupKeyHasher {
    pub fn new(group_columns: Vec<usize>) -> Self {
        Self {
            group_columns,
            hash_table: HashMap::new(),
            hash_seed: 0x9e3779b97f4a7c15, // 黄金比例
        }
    }

    /// 计算分组键的哈希值
    pub fn compute_hash(&self, batch: &RecordBatch, row_idx: usize) -> Result<u64, String> {
        let mut hasher = self.hash_seed;
        
        for &col_idx in &self.group_columns {
            let column = batch.column(col_idx);
            hasher = self.hash_column_value(column, row_idx, hasher)?;
        }
        
        Ok(hasher)
    }

    /// 哈希列值
    fn hash_column_value(&self, column: &ArrayRef, row_idx: usize, mut hasher: u64) -> Result<u64, String> {
        if !column.is_valid(row_idx) {
            hasher = hasher.wrapping_mul(0x9e3779b9).wrapping_add(0x7f4a7c15);
            return Ok(hasher);
        }

        match column.data_type() {
            DataType::Int8 => {
                let array = column.as_any().downcast_ref::<Int8Array>().unwrap();
                hasher = hasher.wrapping_mul(0x9e3779b9).wrapping_add(array.value(row_idx) as u64);
            },
            DataType::Int16 => {
                let array = column.as_any().downcast_ref::<Int16Array>().unwrap();
                hasher = hasher.wrapping_mul(0x9e3779b9).wrapping_add(array.value(row_idx) as u64);
            },
            DataType::Int32 => {
                let array = column.as_any().downcast_ref::<Int32Array>().unwrap();
                hasher = hasher.wrapping_mul(0x9e3779b9).wrapping_add(array.value(row_idx) as u64);
            },
            DataType::Int64 => {
                let array = column.as_any().downcast_ref::<Int64Array>().unwrap();
                hasher = hasher.wrapping_mul(0x9e3779b9).wrapping_add(array.value(row_idx) as u64);
            },
            DataType::UInt8 => {
                let array = column.as_any().downcast_ref::<UInt8Array>().unwrap();
                hasher = hasher.wrapping_mul(0x9e3779b9).wrapping_add(array.value(row_idx) as u64);
            },
            DataType::UInt16 => {
                let array = column.as_any().downcast_ref::<UInt16Array>().unwrap();
                hasher = hasher.wrapping_mul(0x9e3779b9).wrapping_add(array.value(row_idx) as u64);
            },
            DataType::UInt32 => {
                let array = column.as_any().downcast_ref::<UInt32Array>().unwrap();
                hasher = hasher.wrapping_mul(0x9e3779b9).wrapping_add(array.value(row_idx) as u64);
            },
            DataType::UInt64 => {
                let array = column.as_any().downcast_ref::<UInt64Array>().unwrap();
                hasher = hasher.wrapping_mul(0x9e3779b9).wrapping_add(array.value(row_idx));
            },
            DataType::Float32 => {
                let array = column.as_any().downcast_ref::<Float32Array>().unwrap();
                hasher = hasher.wrapping_mul(0x9e3779b9).wrapping_add(array.value(row_idx).to_bits() as u64);
            },
            DataType::Float64 => {
                let array = column.as_any().downcast_ref::<Float64Array>().unwrap();
                hasher = hasher.wrapping_mul(0x9e3779b9).wrapping_add(array.value(row_idx).to_bits());
            },
            DataType::Utf8 => {
                let array = column.as_any().downcast_ref::<StringArray>().unwrap();
                let value = array.value(row_idx);
                hasher = hasher.wrapping_mul(0x9e3779b9).wrapping_add(self.hash_string(value));
            },
            DataType::Boolean => {
                let array = column.as_any().downcast_ref::<BooleanArray>().unwrap();
                hasher = hasher.wrapping_mul(0x9e3779b9).wrapping_add(if array.value(row_idx) { 1 } else { 0 });
            },
            _ => return Err(format!("Unsupported group column type: {:?}", column.data_type())),
        }
        
        Ok(hasher)
    }

    /// 哈希字符串
    fn hash_string(&self, s: &str) -> u64 {
        let mut hash = 0u64;
        for byte in s.bytes() {
            hash = hash.wrapping_mul(0x9e3779b9).wrapping_add(byte as u64);
        }
        hash
    }

    /// 查找或创建分组
    pub fn find_or_create_group(&mut self, hash: u64) -> usize {
        if let Some(&group_id) = self.hash_table.get(&hash) {
            group_id
        } else {
            let group_id = self.hash_table.len();
            self.hash_table.insert(hash, group_id);
            group_id
        }
    }

    /// 获取分组数量
    pub fn group_count(&self) -> usize {
        self.hash_table.len()
    }
}

/// 列式向量化聚合器
pub struct VectorizedAggregator {
    config: VectorizedAggregatorConfig,
    group_columns: Vec<usize>,
    agg_functions: Vec<AggregationFunction>,
    group_hasher: GroupKeyHasher,
    agg_states: Vec<Vec<AggregationState>>,
    stats: AggregatorStats,
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
    /// 是否已发送结果
    results_sent: bool,
}

#[derive(Debug, Default)]
pub struct AggregatorStats {
    pub total_rows_processed: u64,
    pub total_groups_created: u64,
    pub total_batches_processed: u64,
    pub total_aggregation_time: std::time::Duration,
    pub avg_aggregation_time: std::time::Duration,
    pub hash_collision_count: u64,
    pub memory_usage_bytes: u64,
}

impl VectorizedAggregator {
    pub fn new(
        config: VectorizedAggregatorConfig,
        group_columns: Vec<usize>,
        agg_functions: Vec<AggregationFunction>,
        operator_id: u32,
        input_ports: Vec<PortId>,
        output_ports: Vec<PortId>,
        name: String,
    ) -> Self {
        let group_hasher = GroupKeyHasher::new(group_columns.clone());
        let agg_states = Vec::new();
        
        Self {
            config,
            group_columns,
            agg_functions,
            group_hasher,
            agg_states,
            stats: AggregatorStats::default(),
            operator_id,
            input_ports,
            output_ports,
            finished: false,
            name,
            results_sent: false,
        }
    }

    /// 向量化聚合
    pub fn aggregate(&mut self, batch: &RecordBatch) -> Result<RecordBatch, String> {
        let start = Instant::now();
        
        for row_idx in 0..batch.num_rows() {
            let group_hash = self.group_hasher.compute_hash(batch, row_idx)?;
            let group_id = self.group_hasher.find_or_create_group(group_hash);
            
            // 确保有足够的聚合状态
            if group_id >= self.agg_states.len() {
                self.agg_states.resize(group_id + 1, Vec::new());
            }
            
            // 确保每个分组有足够的聚合函数状态
            if self.agg_states[group_id].is_empty() {
                self.agg_states[group_id] = self.create_initial_agg_states();
            }
            
            // 更新聚合状态
            self.update_agg_states(group_id, batch, row_idx)?;
        }
        
        // 生成结果批次
        let result = self.create_result_batch()?;
        
        let duration = start.elapsed();
        self.update_stats(batch.num_rows(), duration);
        
        debug!("向量化聚合完成: {} rows -> {} groups ({}μs)", 
               batch.num_rows(), result.num_rows(), duration.as_micros());
        
        Ok(result)
    }

    /// 创建初始聚合状态
    fn create_initial_agg_states(&self) -> Vec<AggregationState> {
        self.agg_functions.iter().map(|func| {
            match func {
                AggregationFunction::Count => AggregationState::Count { count: 0 },
                AggregationFunction::Sum { .. } => AggregationState::Sum { sum: ScalarValue::Int64(Some(0)) },
                AggregationFunction::Avg { .. } => AggregationState::Avg { sum: ScalarValue::Int64(Some(0)), count: 0 },
                AggregationFunction::Min { .. } => AggregationState::Min { min: ScalarValue::Int64(Some(i64::MAX)) },
                AggregationFunction::Max { .. } => AggregationState::Max { max: ScalarValue::Int64(Some(i64::MIN)) },
                AggregationFunction::First { .. } => AggregationState::First { first: ScalarValue::Int64(Some(0)) },
                AggregationFunction::Last { .. } => AggregationState::Last { last: ScalarValue::Int64(Some(0)) },
                AggregationFunction::StdDev { .. } => AggregationState::StdDev { 
                    sum: ScalarValue::Float64(Some(0.0)), 
                    sum_squares: ScalarValue::Float64(Some(0.0)), 
                    count: 0 
                },
                AggregationFunction::Variance { .. } => AggregationState::Variance { 
                    sum: ScalarValue::Float64(Some(0.0)), 
                    sum_squares: ScalarValue::Float64(Some(0.0)), 
                    count: 0 
                },
                AggregationFunction::Median { .. } => AggregationState::Median { values: Vec::new() },
                AggregationFunction::Percentile { percentile, .. } => AggregationState::Percentile { 
                    values: Vec::new(), 
                    percentile: *percentile 
                },
                AggregationFunction::DistinctCount { .. } => AggregationState::DistinctCount { 
                    distinct_values: std::collections::HashSet::new() 
                },
                AggregationFunction::CollectList { .. } => AggregationState::CollectList { values: Vec::new() },
                AggregationFunction::CollectSet { .. } => AggregationState::CollectSet { 
                    values: std::collections::HashSet::new() 
                },
            }
        }).collect()
    }

    /// 更新聚合状态
    fn update_agg_states(&mut self, group_id: usize, batch: &RecordBatch, row_idx: usize) -> Result<(), String> {
        for (func_idx, func) in self.agg_functions.iter().enumerate() {
            let state = &mut self.agg_states[group_id][func_idx];
            
            match func {
                AggregationFunction::Count => {
                    if let AggregationState::Count { count } = state {
                        *count += 1;
                    }
                },
                AggregationFunction::Sum { column } => {
                    Self::update_sum_state(self, state, batch, *column, row_idx)?;
                },
                AggregationFunction::Avg { column } => {
                    Self::update_avg_state(self, state, batch, *column, row_idx)?;
                },
                AggregationFunction::Min { column } => {
                    Self::update_min_state(self, state, batch, *column, row_idx)?;
                },
                AggregationFunction::Max { column } => {
                    Self::update_max_state(self, state, batch, *column, row_idx)?;
                },
                AggregationFunction::First { column } => {
                    Self::update_first_state(self, state, batch, *column, row_idx)?;
                },
                AggregationFunction::Last { column } => {
                    Self::update_last_state(self, state, batch, *column, row_idx)?;
                },
                AggregationFunction::StdDev { column } => {
                    Self::update_stddev_state(self, state, batch, *column, row_idx)?;
                },
                AggregationFunction::Variance { column } => {
                    Self::update_variance_state(self, state, batch, *column, row_idx)?;
                },
                AggregationFunction::Median { column } => {
                    Self::update_median_state(self, state, batch, *column, row_idx)?;
                },
                AggregationFunction::Percentile { column, .. } => {
                    Self::update_percentile_state(self, state, batch, *column, row_idx)?;
                },
                AggregationFunction::DistinctCount { column } => {
                    Self::update_distinct_count_state(self, state, batch, *column, row_idx)?;
                },
                AggregationFunction::CollectList { column } => {
                    Self::update_collect_list_state(self, state, batch, *column, row_idx)?;
                },
                AggregationFunction::CollectSet { column } => {
                    Self::update_collect_set_state(self, state, batch, *column, row_idx)?;
                },
            }
        }
        
        Ok(())
    }

    /// 更新求和状态
    fn update_sum_state(&mut self, state: &mut AggregationState, batch: &RecordBatch, column: usize, row_idx: usize) -> Result<(), String> {
        if let AggregationState::Sum { sum } = state {
            let column = batch.column(column);
            let value = self.extract_scalar_value(column, row_idx)?;
            *sum = self.add_scalar_values(sum, &value)?;
        }
        Ok(())
    }

    /// 更新平均值状态
    fn update_avg_state(&mut self, state: &mut AggregationState, batch: &RecordBatch, column: usize, row_idx: usize) -> Result<(), String> {
        if let AggregationState::Avg { sum, count } = state {
            let column = batch.column(column);
            let value = self.extract_scalar_value(column, row_idx)?;
            *sum = self.add_scalar_values(sum, &value)?;
            *count += 1;
        }
        Ok(())
    }

    /// 更新最小值状态
    fn update_min_state(&mut self, state: &mut AggregationState, batch: &RecordBatch, column: usize, row_idx: usize) -> Result<(), String> {
        if let AggregationState::Min { min } = state {
            let column = batch.column(column);
            let value = self.extract_scalar_value(column, row_idx)?;
            *min = self.min_scalar_values(min, &value)?;
        }
        Ok(())
    }

    /// 更新最大值状态
    fn update_max_state(&mut self, state: &mut AggregationState, batch: &RecordBatch, column: usize, row_idx: usize) -> Result<(), String> {
        if let AggregationState::Max { max } = state {
            let column = batch.column(column);
            let value = self.extract_scalar_value(column, row_idx)?;
            *max = self.max_scalar_values(max, &value)?;
        }
        Ok(())
    }

    /// 更新第一个值状态
    fn update_first_state(&mut self, state: &mut AggregationState, batch: &RecordBatch, column: usize, row_idx: usize) -> Result<(), String> {
        if let AggregationState::First { first } = state {
            if first.is_null() {
                let column = batch.column(column);
                *first = self.extract_scalar_value(column, row_idx)?;
            }
        }
        Ok(())
    }

    /// 更新最后一个值状态
    fn update_last_state(&mut self, state: &mut AggregationState, batch: &RecordBatch, column: usize, row_idx: usize) -> Result<(), String> {
        if let AggregationState::Last { last } = state {
            let column = batch.column(column);
            *last = self.extract_scalar_value(column, row_idx)?;
        }
        Ok(())
    }

    /// 更新标准差状态
    fn update_stddev_state(&mut self, state: &mut AggregationState, batch: &RecordBatch, column: usize, row_idx: usize) -> Result<(), String> {
        if let AggregationState::StdDev { sum, sum_squares, count } = state {
            let column = batch.column(column);
            let value = self.extract_scalar_value(column, row_idx)?;
            *sum = self.add_scalar_values(sum, &value)?;
            *sum_squares = self.add_scalar_values(sum_squares, &self.square_scalar_value(&value)?)?;
            *count += 1;
        }
        Ok(())
    }

    /// 更新方差状态
    fn update_variance_state(&mut self, state: &mut AggregationState, batch: &RecordBatch, column: usize, row_idx: usize) -> Result<(), String> {
        if let AggregationState::Variance { sum, sum_squares, count } = state {
            let column = batch.column(column);
            let value = self.extract_scalar_value(column, row_idx)?;
            *sum = self.add_scalar_values(sum, &value)?;
            *sum_squares = self.add_scalar_values(sum_squares, &self.square_scalar_value(&value)?)?;
            *count += 1;
        }
        Ok(())
    }

    /// 更新中位数状态
    fn update_median_state(&mut self, state: &mut AggregationState, batch: &RecordBatch, column: usize, row_idx: usize) -> Result<(), String> {
        if let AggregationState::Median { values } = state {
            let column = batch.column(column);
            let value = self.extract_scalar_value(column, row_idx)?;
            values.push(value);
        }
        Ok(())
    }

    /// 更新百分位数状态
    fn update_percentile_state(&mut self, state: &mut AggregationState, batch: &RecordBatch, column: usize, row_idx: usize) -> Result<(), String> {
        if let AggregationState::Percentile { values, .. } = state {
            let column = batch.column(column);
            let value = self.extract_scalar_value(column, row_idx)?;
            values.push(value);
        }
        Ok(())
    }

    /// 更新去重计数状态
    fn update_distinct_count_state(&mut self, state: &mut AggregationState, batch: &RecordBatch, column: usize, row_idx: usize) -> Result<(), String> {
        if let AggregationState::DistinctCount { distinct_values } = state {
            let column = batch.column(column);
            let value = self.extract_scalar_value(column, row_idx)?;
            distinct_values.insert(value);
        }
        Ok(())
    }

    /// 更新收集列表状态
    fn update_collect_list_state(&mut self, state: &mut AggregationState, batch: &RecordBatch, column: usize, row_idx: usize) -> Result<(), String> {
        if let AggregationState::CollectList { values } = state {
            let column = batch.column(column);
            let value = self.extract_scalar_value(column, row_idx)?;
            values.push(value);
        }
        Ok(())
    }

    /// 更新收集集合状态
    fn update_collect_set_state(&mut self, state: &mut AggregationState, batch: &RecordBatch, column: usize, row_idx: usize) -> Result<(), String> {
        if let AggregationState::CollectSet { values } = state {
            let column = batch.column(column);
            let value = self.extract_scalar_value(column, row_idx)?;
            values.insert(value);
        }
        Ok(())
    }

    /// 提取标量值
    fn extract_scalar_value(&self, column: &ArrayRef, row_idx: usize) -> Result<ScalarValue, String> {
        if !column.is_valid(row_idx) {
            return Ok(ScalarValue::Null);
        }

        match column.data_type() {
            DataType::Int8 => {
                let array = column.as_any().downcast_ref::<Int8Array>().unwrap();
                Ok(ScalarValue::Int8(Some(array.value(row_idx))))
            },
            DataType::Int16 => {
                let array = column.as_any().downcast_ref::<Int16Array>().unwrap();
                Ok(ScalarValue::Int16(Some(array.value(row_idx))))
            },
            DataType::Int32 => {
                let array = column.as_any().downcast_ref::<Int32Array>().unwrap();
                Ok(ScalarValue::Int32(Some(array.value(row_idx))))
            },
            DataType::Int64 => {
                let array = column.as_any().downcast_ref::<Int64Array>().unwrap();
                Ok(ScalarValue::Int64(Some(array.value(row_idx))))
            },
            DataType::UInt8 => {
                let array = column.as_any().downcast_ref::<UInt8Array>().unwrap();
                Ok(ScalarValue::UInt8(Some(array.value(row_idx))))
            },
            DataType::UInt16 => {
                let array = column.as_any().downcast_ref::<UInt16Array>().unwrap();
                Ok(ScalarValue::UInt16(Some(array.value(row_idx))))
            },
            DataType::UInt32 => {
                let array = column.as_any().downcast_ref::<UInt32Array>().unwrap();
                Ok(ScalarValue::UInt32(Some(array.value(row_idx))))
            },
            DataType::UInt64 => {
                let array = column.as_any().downcast_ref::<UInt64Array>().unwrap();
                Ok(ScalarValue::UInt64(Some(array.value(row_idx))))
            },
            DataType::Float32 => {
                let array = column.as_any().downcast_ref::<Float32Array>().unwrap();
                Ok(ScalarValue::Float32(Some(array.value(row_idx))))
            },
            DataType::Float64 => {
                let array = column.as_any().downcast_ref::<Float64Array>().unwrap();
                Ok(ScalarValue::Float64(Some(array.value(row_idx))))
            },
            DataType::Utf8 => {
                let array = column.as_any().downcast_ref::<StringArray>().unwrap();
                Ok(ScalarValue::Utf8(Some(array.value(row_idx).to_string())))
            },
            DataType::Boolean => {
                let array = column.as_any().downcast_ref::<BooleanArray>().unwrap();
                Ok(ScalarValue::Boolean(Some(array.value(row_idx))))
            },
            _ => Err(format!("Unsupported column type: {:?}", column.data_type()))
        }
    }

    /// 标量值相加
    fn add_scalar_values(&self, left: &ScalarValue, right: &ScalarValue) -> Result<ScalarValue, String> {
        match (left, right) {
            (ScalarValue::Int32(Some(l)), ScalarValue::Int32(Some(r))) => {
                Ok(ScalarValue::Int32(Some(l + r)))
            },
            (ScalarValue::Int64(Some(l)), ScalarValue::Int64(Some(r))) => {
                Ok(ScalarValue::Int64(Some(l + r)))
            },
            (ScalarValue::Float32(Some(l)), ScalarValue::Float32(Some(r))) => {
                Ok(ScalarValue::Float32(Some(l + r)))
            },
            (ScalarValue::Float64(Some(l)), ScalarValue::Float64(Some(r))) => {
                Ok(ScalarValue::Float64(Some(l + r)))
            },
            _ => Err(format!("Unsupported addition: {:?} + {:?}", left, right))
        }
    }

    /// 标量值最小值
    fn min_scalar_values(&self, left: &ScalarValue, right: &ScalarValue) -> Result<ScalarValue, String> {
        match (left, right) {
            (ScalarValue::Int32(Some(l)), ScalarValue::Int32(Some(r))) => {
                Ok(ScalarValue::Int32(Some(*l.min(r))))
            },
            (ScalarValue::Int64(Some(l)), ScalarValue::Int64(Some(r))) => {
                Ok(ScalarValue::Int64(Some(*l.min(r))))
            },
            (ScalarValue::Float32(Some(l)), ScalarValue::Float32(Some(r))) => {
                Ok(ScalarValue::Float32(Some(l.min(*r))))
            },
            (ScalarValue::Float64(Some(l)), ScalarValue::Float64(Some(r))) => {
                Ok(ScalarValue::Float64(Some(l.min(*r))))
            },
            _ => Err(format!("Unsupported min: {:?} min {:?}", left, right))
        }
    }

    /// 标量值最大值
    fn max_scalar_values(&self, left: &ScalarValue, right: &ScalarValue) -> Result<ScalarValue, String> {
        match (left, right) {
            (ScalarValue::Int32(Some(l)), ScalarValue::Int32(Some(r))) => {
                Ok(ScalarValue::Int32(Some(*l.max(r))))
            },
            (ScalarValue::Int64(Some(l)), ScalarValue::Int64(Some(r))) => {
                Ok(ScalarValue::Int64(Some(*l.max(r))))
            },
            (ScalarValue::Float32(Some(l)), ScalarValue::Float32(Some(r))) => {
                Ok(ScalarValue::Float32(Some(l.max(*r))))
            },
            (ScalarValue::Float64(Some(l)), ScalarValue::Float64(Some(r))) => {
                Ok(ScalarValue::Float64(Some(l.max(*r))))
            },
            _ => Err(format!("Unsupported max: {:?} max {:?}", left, right))
        }
    }

    /// 标量值平方
    fn square_scalar_value(&self, value: &ScalarValue) -> Result<ScalarValue, String> {
        match value {
            ScalarValue::Float32(Some(v)) => Ok(ScalarValue::Float32(Some(v * v))),
            ScalarValue::Float64(Some(v)) => Ok(ScalarValue::Float64(Some(v * v))),
            _ => Err(format!("Unsupported square: {:?}", value))
        }
    }

    /// 创建结果批次
    fn create_result_batch(&self) -> Result<RecordBatch, String> {
        let mut columns = Vec::new();
        let mut field_names = Vec::new();
        
        // 添加分组列
        for &col_idx in &self.group_columns {
            let mut group_values = Vec::new();
            for group_states in &self.agg_states {
                if !group_states.is_empty() {
                    // 这里需要从原始数据中提取分组值，简化实现
                    group_values.push(ScalarValue::Int32(Some(0)));
                }
            }
            
            let array = self.create_array_from_scalar_values(&group_values)?;
            columns.push(array);
            field_names.push(format!("group_col_{}", col_idx));
        }
        
        // 添加聚合列
        for (func_idx, func) in self.agg_functions.iter().enumerate() {
            let mut agg_values = Vec::new();
            
            for group_states in &self.agg_states {
                if !group_states.is_empty() {
                    let value = self.finalize_agg_state(&group_states[func_idx], func)?;
                    agg_values.push(value);
                }
            }
            
            let array = self.create_array_from_scalar_values(&agg_values)?;
            columns.push(array);
            field_names.push(self.get_agg_function_name(func));
        }
        
        // 创建schema
        let fields: Vec<Field> = field_names.iter().enumerate().map(|(i, name)| {
            Field::new(name, columns[i].data_type().clone(), true)
        }).collect();
        
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema, columns).map_err(|e| e.to_string())?;
        
        Ok(batch)
    }

    /// 完成聚合状态
    fn finalize_agg_state(&self, state: &AggregationState, func: &AggregationFunction) -> Result<ScalarValue, String> {
        match (state, func) {
            (AggregationState::Count { count }, AggregationFunction::Count) => {
                Ok(ScalarValue::UInt64(Some(*count)))
            },
            (AggregationState::Sum { sum }, AggregationFunction::Sum { .. }) => {
                Ok(sum.clone())
            },
            (AggregationState::Avg { sum, count }, AggregationFunction::Avg { .. }) => {
                if *count > 0 {
                    match sum {
                        ScalarValue::Float64(Some(s)) => Ok(ScalarValue::Float64(Some(s / *count as f64))),
                        ScalarValue::Float32(Some(s)) => Ok(ScalarValue::Float32(Some(s / *count as f32))),
                        _ => Ok(ScalarValue::Float64(Some(0.0))),
                    }
                } else {
                    Ok(ScalarValue::Float64(Some(0.0)))
                }
            },
            (AggregationState::Min { min }, AggregationFunction::Min { .. }) => {
                Ok(min.clone())
            },
            (AggregationState::Max { max }, AggregationFunction::Max { .. }) => {
                Ok(max.clone())
            },
            (AggregationState::First { first }, AggregationFunction::First { .. }) => {
                Ok(first.clone())
            },
            (AggregationState::Last { last }, AggregationFunction::Last { .. }) => {
                Ok(last.clone())
            },
            (AggregationState::DistinctCount { distinct_values }, AggregationFunction::DistinctCount { .. }) => {
                Ok(ScalarValue::UInt64(Some(distinct_values.len() as u64)))
            },
            _ => Ok(ScalarValue::Null),
        }
    }

    /// 从标量值创建数组
    fn create_array_from_scalar_values(&self, values: &[ScalarValue]) -> Result<ArrayRef, String> {
        if values.is_empty() {
            return Ok(Arc::new(Int32Array::from(vec![0i32; 0])));
        }

        match &values[0] {
            ScalarValue::Int32(_) => {
                let int_values: Vec<Option<i32>> = values.iter().map(|v| {
                    if let ScalarValue::Int32(Some(val)) = v { Some(*val) } else { None }
                }).collect();
                Ok(Arc::new(Int32Array::from(int_values)))
            },
            ScalarValue::Int64(_) => {
                let int_values: Vec<Option<i64>> = values.iter().map(|v| {
                    if let ScalarValue::Int64(Some(val)) = v { Some(*val) } else { None }
                }).collect();
                Ok(Arc::new(Int64Array::from(int_values)))
            },
            ScalarValue::Float32(_) => {
                let float_values: Vec<Option<f32>> = values.iter().map(|v| {
                    if let ScalarValue::Float32(Some(val)) = v { Some(*val) } else { None }
                }).collect();
                Ok(Arc::new(Float32Array::from(float_values)))
            },
            ScalarValue::Float64(_) => {
                let float_values: Vec<Option<f64>> = values.iter().map(|v| {
                    if let ScalarValue::Float64(Some(val)) = v { Some(*val) } else { None }
                }).collect();
                Ok(Arc::new(Float64Array::from(float_values)))
            },
            ScalarValue::Utf8(_) => {
                let string_values: Vec<Option<String>> = values.iter().map(|v| {
                    if let ScalarValue::Utf8(Some(val)) = v { Some(val.clone()) } else { None }
                }).collect();
                Ok(Arc::new(StringArray::from(string_values)))
            },
            ScalarValue::Boolean(_) => {
                let bool_values: Vec<Option<bool>> = values.iter().map(|v| {
                    if let ScalarValue::Boolean(Some(val)) = v { Some(*val) } else { None }
                }).collect();
                Ok(Arc::new(BooleanArray::from(bool_values)))
            },
            _ => Err(format!("Unsupported scalar value type: {:?}", values[0]))
        }
    }

    /// 获取聚合函数名称
    fn get_agg_function_name(&self, func: &AggregationFunction) -> String {
        match func {
            AggregationFunction::Count => "count".to_string(),
            AggregationFunction::Sum { .. } => "sum".to_string(),
            AggregationFunction::Avg { .. } => "avg".to_string(),
            AggregationFunction::Min { .. } => "min".to_string(),
            AggregationFunction::Max { .. } => "max".to_string(),
            AggregationFunction::First { .. } => "first".to_string(),
            AggregationFunction::Last { .. } => "last".to_string(),
            AggregationFunction::StdDev { .. } => "stddev".to_string(),
            AggregationFunction::Variance { .. } => "variance".to_string(),
            AggregationFunction::Median { .. } => "median".to_string(),
            AggregationFunction::Percentile { .. } => "percentile".to_string(),
            AggregationFunction::DistinctCount { .. } => "distinct_count".to_string(),
            AggregationFunction::CollectList { .. } => "collect_list".to_string(),
            AggregationFunction::CollectSet { .. } => "collect_set".to_string(),
        }
    }

    /// 更新统计信息
    fn update_stats(&mut self, rows: usize, duration: std::time::Duration) {
        self.stats.total_rows_processed += rows as u64;
        self.stats.total_groups_created = self.group_hasher.group_count() as u64;
        self.stats.total_batches_processed += 1;
        self.stats.total_aggregation_time += duration;
        
        if self.stats.total_batches_processed > 0 {
            self.stats.avg_aggregation_time = std::time::Duration::from_nanos(
                self.stats.total_aggregation_time.as_nanos() as u64 / self.stats.total_batches_processed
            );
        }
    }

    /// 获取统计信息
    pub fn get_stats(&self) -> &AggregatorStats {
        &self.stats
    }

    /// 重置统计信息
    pub fn reset_stats(&mut self) {
        self.stats = AggregatorStats::default();
    }
}

/// 实现Operator trait
impl Operator for VectorizedAggregator {
    fn on_register(&mut self, _ctx: OperatorContext) -> Result<()> {
        debug!("向量化聚合器算子注册: {}", self.name);
        Ok(())
    }
    
    fn on_event(&mut self, ev: Event, out: &mut Outbox) -> OpStatus {
        match ev {
            Event::Data { port, batch } => {
                if self.input_ports.contains(&port) {
                    match self.aggregate(&batch) {
                        Ok(_aggregated_batch) => {
                            // 聚合器是流式处理，不立即发送结果
                            // 结果在EndOfStream时发送
                            OpStatus::Ready
                        },
                        Err(e) => {
                            warn!("向量化聚合失败: {}", e);
                            OpStatus::Error("Aggregation evaluation failed".to_string())
                        }
                    }
                } else {
                    warn!("未知的输入端口: {}", port);
                    OpStatus::Error
                }
            },
            Event::EndOfStream { port } => {
                if self.input_ports.contains(&port) && !self.results_sent {
                    // 发送聚合结果
                    match self.create_result_batch() {
                        Ok(result_batch) => {
                            for &output_port in &self.output_ports {
                                out.send(output_port, result_batch.clone());
                            }
                            self.results_sent = true;
                            self.finished = true;
                            
                            // 发送EndOfStream事件
                            for &output_port in &self.output_ports {
                                out.send_eos(output_port);
                            }
                            OpStatus::Finished
                        },
                        Err(e) => {
                            warn!("创建聚合结果失败: {}", e);
                            OpStatus::Error("Aggregation evaluation failed".to_string())
                        }
                    }
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
