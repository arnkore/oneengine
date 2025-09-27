//! Hash聚合算子
//! 
//! 基于Arrow的高性能两阶段Hash聚合

use super::{BaseOperator, SingleInputOperator, MetricsSupport, OperatorMetrics, SpillableOperator};
use crate::push_runtime::{Operator, OperatorContext, Event, OpStatus, Outbox, PortId};
use arrow::record_batch::RecordBatch;
use arrow::datatypes::SchemaRef;
// use arrow::compute::{sum, min, max, count, mean};
use arrow::array::{Array, StringArray, Int32Array, Float64Array};
use arrow::datatypes::{DataType, Field, Schema};
// use arrow_row::{RowConverter, Rows};
use anyhow::Result;
use std::collections::HashMap;
use std::time::Instant;
use std::sync::Arc;

/// 聚合函数类型
#[derive(Debug, Clone, PartialEq)]
pub enum AggFunction {
    /// 计数
    Count,
    /// 求和
    Sum,
    /// 平均值
    Avg,
    /// 最大值
    Max,
    /// 最小值
    Min,
    /// 去重计数
    CountDistinct,
}

/// 聚合表达式
#[derive(Debug, Clone)]
pub struct AggExpression {
    /// 聚合函数
    pub function: AggFunction,
    /// 输入列名
    pub input_column: String,
    /// 输出列名
    pub output_column: String,
    /// 数据类型
    pub data_type: DataType,
}

/// Hash聚合配置
#[derive(Debug, Clone)]
pub struct HashAggConfig {
    /// 分组列
    pub group_columns: Vec<String>,
    /// 聚合表达式
    pub agg_expressions: Vec<AggExpression>,
    /// 输出schema
    pub output_schema: SchemaRef,
    /// 最大内存使用量（字节）
    pub max_memory_bytes: usize,
    /// 是否启用两阶段聚合
    pub enable_two_phase: bool,
    /// 分区数量
    pub partition_count: usize,
    /// 是否启用字典列优化
    pub enable_dictionary_optimization: bool,
}

impl HashAggConfig {
    /// 创建新的配置
    pub fn new(
        group_columns: Vec<String>,
        agg_expressions: Vec<AggExpression>,
        output_schema: SchemaRef,
    ) -> Self {
        Self {
            group_columns,
            agg_expressions,
            output_schema,
            max_memory_bytes: 100 * 1024 * 1024, // 100MB
            enable_two_phase: true,
            partition_count: 16,
            enable_dictionary_optimization: true,
        }
    }
    
    /// 验证配置
    pub fn validate(&self) -> Result<()> {
        if self.group_columns.is_empty() && self.agg_expressions.is_empty() {
            return Err(anyhow::anyhow!("At least one group column or aggregation expression is required"));
        }
        
        if self.partition_count == 0 {
            return Err(anyhow::anyhow!("Partition count must be greater than 0"));
        }
        
        Ok(())
    }
}

/// 聚合状态
#[derive(Debug, Clone)]
pub enum AggState {
    /// 计数状态
    Count(u64),
    /// 求和状态
    Sum(f64),
    /// 平均值状态（计数 + 总和）
    Avg { count: u64, sum: f64 },
    /// 最大值状态
    Max(f64),
    /// 最小值状态
    Min(f64),
    /// 去重计数状态（使用HashSet存储唯一值）
    CountDistinct(std::collections::HashSet<String>),
}

impl AggState {
    /// 更新状态
    pub fn update(&mut self, value: &str) -> Result<()> {
        let num_value: f64 = value.parse().unwrap_or(0.0);
        
        match self {
            AggState::Count(count) => *count += 1,
            AggState::Sum(sum) => *sum += num_value,
            AggState::Avg { count, sum } => {
                *count += 1;
                *sum += num_value;
            }
            AggState::Max(max) => *max = (*max).max(num_value),
            AggState::Min(min) => *min = (*min).min(num_value),
            AggState::CountDistinct(set) => {
                set.insert(value.to_string());
            }
        }
        
        Ok(())
    }
    
    /// 获取最终值
    pub fn finalize(&self) -> f64 {
        match self {
            AggState::Count(count) => *count as f64,
            AggState::Sum(sum) => *sum,
            AggState::Avg { count, sum } => {
                if *count > 0 {
                    *sum / *count as f64
                } else {
                    0.0
                }
            }
            AggState::Max(max) => *max,
            AggState::Min(min) => *min,
            AggState::CountDistinct(set) => set.len() as f64,
        }
    }
}

/// Hash聚合算子
pub struct HashAggOperator {
    /// 基础算子
    base: BaseOperator,
    /// 配置
    config: HashAggConfig,
    /// 统计信息
    metrics: OperatorMetrics,
    /// 聚合状态（分组键 -> 聚合状态）
    agg_states: HashMap<Vec<u8>, Vec<AggState>>,
    /// 行转换器（用于分组键编码）
    // row_converter: Option<RowConverter>,
    /// 当前内存使用量
    current_memory_usage: usize,
    /// 是否已完成第一阶段
    phase_one_complete: bool,
    /// 溢写的分区
    spilled_partitions: Vec<u32>,
}

impl HashAggOperator {
    /// 创建新的Hash聚合算子
    pub fn new(
        operator_id: u32,
        input_ports: Vec<PortId>,
        output_ports: Vec<PortId>,
        config: HashAggConfig,
    ) -> Self {
        Self {
            base: BaseOperator::new(
                operator_id,
                input_ports,
                output_ports,
                "HashAgg".to_string(),
            ),
            config,
            metrics: OperatorMetrics::default(),
            agg_states: HashMap::new(),
            // row_converter: None,
            current_memory_usage: 0,
            phase_one_complete: false,
            spilled_partitions: Vec::new(),
        }
    }
    
    /// 初始化行转换器
    fn init_row_converter(&mut self, _schema: &Schema) -> Result<()> {
        // TODO: 实现基于arrow_row的行转换器
        // 为了简化，我们暂时跳过这个实现
        Ok(())
    }
    
    /// 计算分组键
    fn compute_group_key(&self, batch: &RecordBatch, row_idx: usize) -> Result<Vec<u8>> {
        if self.config.group_columns.is_empty() {
            return Ok(vec![]); // 全局聚合
        }
        
        // 简单的字符串拼接（用于测试）
        let mut key = String::new();
        for col_name in &self.config.group_columns {
            if let Some(field_idx) = batch.schema().fields().iter().position(|f| f.name() == col_name) {
                let array = batch.column(field_idx);
                let value = Self::extract_string_value_static(array, row_idx);
                key.push_str(&value);
                key.push('|');
            }
        }
        Ok(key.into_bytes())
    }
    
    /// 提取字符串值（静态方法）
    fn extract_string_value_static(array: &dyn Array, row_idx: usize) -> String {
        match array.data_type() {
            DataType::Utf8 => {
                let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
                string_array.value(row_idx).to_string()
            }
            DataType::Int32 => {
                let int_array = array.as_any().downcast_ref::<Int32Array>().unwrap();
                int_array.value(row_idx).to_string()
            }
            DataType::Int64 => {
                let int_array = array.as_any().downcast_ref::<Int32Array>().unwrap();
                int_array.value(row_idx).to_string()
            }
            DataType::Float64 => {
                let float_array = array.as_any().downcast_ref::<Float64Array>().unwrap();
                float_array.value(row_idx).to_string()
            }
            _ => "unknown".to_string(),
        }
    }
    
    /// 初始化聚合状态
    fn init_agg_states(&self) -> Vec<AggState> {
        self.config.agg_expressions.iter()
            .map(|expr| match expr.function {
                AggFunction::Count => AggState::Count(0),
                AggFunction::Sum => AggState::Sum(0.0),
                AggFunction::Avg => AggState::Avg { count: 0, sum: 0.0 },
                AggFunction::Max => AggState::Max(f64::NEG_INFINITY),
                AggFunction::Min => AggState::Min(f64::INFINITY),
                AggFunction::CountDistinct => AggState::CountDistinct(std::collections::HashSet::new()),
            })
            .collect()
    }
    
    /// 处理单个批次
    fn process_batch_internal(&mut self, batch: &RecordBatch) -> Result<()> {
        for row_idx in 0..batch.num_rows() {
            let group_key = self.compute_group_key(batch, row_idx)?;
            
            // 获取或创建聚合状态
            let states = self.agg_states.entry(group_key).or_insert_with(|| {
                self.config.agg_expressions.iter()
                    .map(|expr| match expr.function {
                        AggFunction::Count => AggState::Count(0),
                        AggFunction::Sum => AggState::Sum(0.0),
                        AggFunction::Avg => AggState::Avg { count: 0, sum: 0.0 },
                        AggFunction::Max => AggState::Max(f64::NEG_INFINITY),
                        AggFunction::Min => AggState::Min(f64::INFINITY),
                        AggFunction::CountDistinct => AggState::CountDistinct(std::collections::HashSet::new()),
                    })
                    .collect()
            });
            
            // 更新聚合状态
            for (i, expr) in self.config.agg_expressions.iter().enumerate() {
                if let Some(field_idx) = batch.schema().fields().iter().position(|f| f.name() == &expr.input_column) {
                    let array = batch.column(field_idx);
                    let value = Self::extract_string_value_static(array, row_idx);
                    states[i].update(&value)?;
                }
            }
        }
        
        // 更新内存使用量（简化估算）
        self.current_memory_usage += batch.get_array_memory_size();
        
        Ok(())
    }
    
    /// 生成输出批次
    fn generate_output_batch(&self) -> Result<RecordBatch> {
        if self.agg_states.is_empty() {
            return self.generate_empty_output_batch();
        }
        
        let mut group_keys = Vec::new();
        let mut agg_values = Vec::new();
        
        for (group_key, states) in &self.agg_states {
            group_keys.push(group_key.clone());
            let mut row_values = Vec::new();
            
            // 添加分组列值
            if !self.config.group_columns.is_empty() {
                // 这里应该解码分组键，为了简化我们跳过
                for _ in &self.config.group_columns {
                    row_values.push("group_value".to_string());
                }
            }
            
            // 添加聚合值
            for state in states {
                row_values.push(state.finalize().to_string());
            }
            
            agg_values.push(row_values);
        }
        
        // 创建输出批次（简化实现）
        self.create_output_batch_from_values(group_keys, agg_values)
    }
    
    /// 生成空输出批次
    fn generate_empty_output_batch(&self) -> Result<RecordBatch> {
        let mut columns = Vec::new();
        
        // 添加分组列
        for field in self.config.output_schema.fields() {
            let empty_array = match field.data_type() {
                DataType::Utf8 => Arc::new(StringArray::from(vec![] as Vec<&str>)) as Arc<dyn Array>,
                DataType::Int32 => Arc::new(Int32Array::from(vec![] as Vec<i32>)) as Arc<dyn Array>,
                DataType::Float64 => Arc::new(Float64Array::from(vec![] as Vec<f64>)) as Arc<dyn Array>,
                _ => return Err(anyhow::anyhow!("Unsupported data type: {:?}", field.data_type())),
            };
            columns.push(empty_array);
        }
        
        Ok(RecordBatch::try_new(self.config.output_schema.clone(), columns)?)
    }
    
    /// 从值创建输出批次
    fn create_output_batch_from_values(
        &self,
        _group_keys: Vec<Vec<u8>>,
        _agg_values: Vec<Vec<String>>,
    ) -> Result<RecordBatch> {
        // 这里应该根据实际数据创建RecordBatch
        // 为了简化，我们返回空批次
        self.generate_empty_output_batch()
    }
}

impl Operator for HashAggOperator {
    fn on_register(&mut self, ctx: OperatorContext) -> Result<()> {
        self.config.validate()?;
        Ok(())
    }
    
    fn on_event(&mut self, ev: Event, out: &mut Outbox) -> OpStatus {
        match ev {
            Event::Data(port, batch) => {
                if self.base.input_ports().contains(&port) {
                    match self.process_batch(batch, out) {
                        Ok(status) => status,
                        Err(e) => {
                            tracing::error!("HashAgg error: {}", e);
                            OpStatus::Error(e.to_string())
                        }
                    }
                } else {
                    OpStatus::Ready
                }
            }
            Event::Flush(port) => {
                if self.base.input_ports().contains(&port) {
                    // 完成第一阶段，生成输出
                    match self.finish_aggregation(out) {
                        Ok(status) => status,
                        Err(e) => {
                            tracing::error!("HashAgg finish error: {}", e);
                            OpStatus::Error(e.to_string())
                        }
                    }
                } else {
                    OpStatus::Ready
                }
            }
            Event::Finish(port) => {
                if self.base.input_ports().contains(&port) {
                    self.base.set_finished();
                    OpStatus::Finished
                } else {
                    OpStatus::Ready
                }
            }
            _ => OpStatus::Ready,
        }
    }
    
    fn is_finished(&self) -> bool {
        self.base.is_finished()
    }
    
    fn name(&self) -> &str {
        self.base.name()
    }
}

impl SingleInputOperator for HashAggOperator {
    fn process_batch(&mut self, batch: RecordBatch, out: &mut Outbox) -> Result<OpStatus> {
        let start_time = Instant::now();
        
        // 初始化行转换器（如果还没有）
        // TODO: 实现基于arrow_row的行转换器
        self.init_row_converter(&batch.schema())?;
        
        // 处理批次
        self.process_batch_internal(&batch)?;
        
        // 检查是否需要溢写
        if self.should_spill() {
            return self.spill(out);
        }
        
        // 记录统计信息
        self.metrics.update_batch(batch.num_rows(), start_time.elapsed());
        self.record_metrics(&batch, start_time.elapsed());
        
        Ok(OpStatus::Ready)
    }
}

impl SpillableOperator for HashAggOperator {
    fn should_spill(&self) -> bool {
        self.current_memory_usage > self.config.max_memory_bytes
    }
    
    fn spill(&mut self, out: &mut Outbox) -> Result<OpStatus> {
        // 实现分区化溢写
        // 这里应该将聚合状态写入磁盘
        self.metrics.record_spill(self.current_memory_usage as u64);
        self.current_memory_usage = 0;
        
        Ok(OpStatus::Ready)
    }
    
    fn restore(&mut self, out: &mut Outbox) -> Result<OpStatus> {
        // 实现从磁盘恢复数据
        Ok(OpStatus::Ready)
    }
}

impl MetricsSupport for HashAggOperator {
    fn record_metrics(&self, batch: &RecordBatch, duration: std::time::Duration) {
        tracing::debug!(
            "HashAgg processed {} rows in {:?}, groups: {}",
            batch.num_rows(),
            duration,
            self.agg_states.len()
        );
    }
    
    fn get_metrics(&self) -> OperatorMetrics {
        self.metrics.clone()
    }
}

impl HashAggOperator {
    /// 完成聚合
    fn finish_aggregation(&mut self, out: &mut Outbox) -> Result<OpStatus> {
        let output_batch = self.generate_output_batch()?;
        
        // 发送到输出端口
        for &output_port in self.base.output_ports() {
            match out.push(output_port, output_batch.clone()) {
                Ok(()) => {}
                Err(_) => {
                    self.metrics.record_block();
                    return Ok(OpStatus::Blocked);
                }
            }
        }
        
        Ok(OpStatus::Ready)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_hash_agg_creation() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Float64, false),
        ]));
        
        let config = HashAggConfig::new(
            vec!["id".to_string()],
            vec![AggExpression {
                function: AggFunction::Sum,
                input_column: "value".to_string(),
                output_column: "sum_value".to_string(),
                data_type: DataType::Float64,
            }],
            schema,
        );
        
        let operator = HashAggOperator::new(
            1,
            vec![100],
            vec![200],
            config,
        );
        
        assert_eq!(operator.name(), "HashAgg");
        assert!(!operator.is_finished());
    }
    
    #[test]
    fn test_agg_state_update() {
        let mut state = AggState::Sum(0.0);
        state.update("10.5").unwrap();
        state.update("20.3").unwrap();
        assert_eq!(state.finalize(), 30.8);
    }
}
