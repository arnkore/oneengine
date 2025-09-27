//! Local Shuffle算子
//! 
//! 基于Arrow的本地重分区实现，支持N路输出端口

use super::{BaseOperator, SingleInputOperator, MultiOutputOperator, MetricsSupport, OperatorMetrics};
use crate::push_runtime::{Operator, OperatorContext, Event, OpStatus, Outbox, PortId};
use arrow::record_batch::RecordBatch;
use arrow::datatypes::{SchemaRef, Schema, Field, DataType};
use arrow::array::{Array, Int32Array, StringArray, Float64Array};
use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

/// 分区策略
#[derive(Debug, Clone, PartialEq)]
pub enum PartitionStrategy {
    /// 哈希分区
    Hash,
    /// 范围分区
    Range,
    /// 轮询分区
    RoundRobin,
    /// 随机分区
    Random,
}

/// Local Shuffle配置
#[derive(Debug, Clone)]
pub struct LocalShuffleConfig {
    /// 分区列
    pub partition_columns: Vec<String>,
    /// 分区数量
    pub partition_count: usize,
    /// 分区策略
    pub strategy: PartitionStrategy,
    /// 是否启用数据倾斜检测
    pub enable_skew_detection: bool,
    /// 倾斜阈值（行数）
    pub skew_threshold: usize,
    /// 是否启用自适应分区
    pub enable_adaptive_partitioning: bool,
}

impl Default for LocalShuffleConfig {
    fn default() -> Self {
        Self {
            partition_columns: Vec::new(),
            partition_count: 4,
            strategy: PartitionStrategy::Hash,
            enable_skew_detection: true,
            skew_threshold: 10000,
            enable_adaptive_partitioning: false,
        }
    }
}

/// 分区统计信息
#[derive(Debug, Clone)]
struct PartitionStats {
    /// 行数
    row_count: usize,
    /// 字节数
    byte_count: usize,
    /// 最后更新时间
    last_update: Instant,
}

impl Default for PartitionStats {
    fn default() -> Self {
        Self {
            row_count: 0,
            byte_count: 0,
            last_update: Instant::now(),
        }
    }
}

/// Local Shuffle算子
pub struct LocalShuffleOperator {
    /// 基础算子
    base: BaseOperator,
    /// 配置
    config: LocalShuffleConfig,
    /// 统计信息
    metrics: OperatorMetrics,
    /// 分区统计
    partition_stats: Vec<PartitionStats>,
    /// 分区列索引
    partition_column_indices: Vec<usize>,
    /// 当前轮询索引
    round_robin_index: usize,
    /// 是否已完成
    finished: bool,
}

impl LocalShuffleOperator {
    /// 创建新的Local Shuffle算子
    pub fn new(
        operator_id: u32,
        input_ports: Vec<PortId>,
        output_ports: Vec<PortId>,
        config: LocalShuffleConfig,
    ) -> Self {
        let partition_count = config.partition_count;
        Self {
            base: BaseOperator::new(
                operator_id,
                input_ports,
                output_ports,
                "LocalShuffle".to_string(),
            ),
            config,
            metrics: OperatorMetrics::default(),
            partition_stats: vec![PartitionStats::default(); partition_count],
            partition_column_indices: Vec::new(),
            round_robin_index: 0,
            finished: false,
        }
    }

    /// 初始化分区列索引
    fn initialize_partition_columns(&mut self, schema: &Schema) -> Result<()> {
        self.partition_column_indices.clear();
        
        for col_name in &self.config.partition_columns {
            let field = schema.fields()
                .iter()
                .position(|f| f.name() == col_name)
                .ok_or_else(|| anyhow::anyhow!("Partition column not found: {}", col_name))?;
            self.partition_column_indices.push(field);
        }
        
        Ok(())
    }

    /// 计算分区ID
    fn calculate_partition_id(&self, batch: &RecordBatch, row_idx: usize) -> Result<usize> {
        match self.config.strategy {
            PartitionStrategy::Hash => {
                self.calculate_hash_partition(batch, row_idx)
            }
            PartitionStrategy::Range => {
                self.calculate_range_partition(batch, row_idx)
            }
            PartitionStrategy::RoundRobin => {
                // 简化实现：使用行索引作为轮询基础
                let partition_id = row_idx % self.config.partition_count;
                Ok(partition_id)
            }
            PartitionStrategy::Random => {
                use std::collections::hash_map::DefaultHasher;
                use std::hash::{Hash, Hasher};
                
                let mut hasher = DefaultHasher::new();
                row_idx.hash(&mut hasher);
                Ok(hasher.finish() as usize % self.config.partition_count)
            }
        }
    }

    /// 计算哈希分区
    fn calculate_hash_partition(&self, batch: &RecordBatch, row_idx: usize) -> Result<usize> {
        if self.partition_column_indices.is_empty() {
            return Ok(0);
        }

        let mut hash_value = 0u64;
        
        for &col_idx in &self.partition_column_indices {
            let array = batch.column(col_idx);
            let value_hash = self.hash_array_value(array, row_idx)?;
            hash_value = hash_value.wrapping_mul(31).wrapping_add(value_hash);
        }
        
        Ok((hash_value as usize) % self.config.partition_count)
    }

    /// 计算范围分区
    fn calculate_range_partition(&self, batch: &RecordBatch, row_idx: usize) -> Result<usize> {
        if self.partition_column_indices.is_empty() {
            return Ok(0);
        }

        // 简化实现：使用第一个分区列的值
        let col_idx = self.partition_column_indices[0];
        let array = batch.column(col_idx);
        let value = self.extract_numeric_value(array, row_idx)?;
        
        Ok((value as usize) % self.config.partition_count)
    }

    /// 哈希数组值
    fn hash_array_value(&self, array: &dyn Array, row_idx: usize) -> Result<u64> {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        
        if array.is_null(row_idx) {
            "NULL".hash(&mut hasher);
        } else {
            match array.data_type() {
                DataType::Int32 => {
                    let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
                    arr.value(row_idx).hash(&mut hasher);
                }
                DataType::Utf8 => {
                    let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
                    arr.value(row_idx).hash(&mut hasher);
                }
                DataType::Float64 => {
                    let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
                    arr.value(row_idx).to_bits().hash(&mut hasher);
                }
                _ => {
                    // 对于其他类型，使用字符串表示
                    format!("{:?}", array).hash(&mut hasher);
                }
            }
        }
        
        Ok(hasher.finish())
    }

    /// 提取数值
    fn extract_numeric_value(&self, array: &dyn Array, row_idx: usize) -> Result<i64> {
        if array.is_null(row_idx) {
            return Ok(0);
        }

        match array.data_type() {
            DataType::Int32 => {
                let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
                Ok(arr.value(row_idx) as i64)
            }
            DataType::Float64 => {
                let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
                Ok(arr.value(row_idx) as i64)
            }
            _ => Ok(0),
        }
    }


    /// 提取单行数据
    fn extract_single_row(&self, batch: &RecordBatch, row_idx: usize) -> Result<RecordBatch> {
        let mut single_row_arrays = Vec::new();
        
        for col_idx in 0..batch.num_columns() {
            let array = batch.column(col_idx);
            let single_row_array = self.extract_single_row_from_array(array, row_idx)?;
            single_row_arrays.push(single_row_array);
        }
        
        Ok(RecordBatch::try_new(
            batch.schema(),
            single_row_arrays,
        )?)
    }

    /// 从数组中提取单行
    fn extract_single_row_from_array(&self, array: &dyn Array, row_idx: usize) -> Result<Arc<dyn Array>> {
        // 简化实现：直接返回原数组的切片
        // 在实际实现中，这里需要创建只包含单行的新数组
        Ok(array.slice(row_idx, 1))
    }

    /// 合并批次
    fn merge_batches(&self, batches: Vec<RecordBatch>) -> Result<RecordBatch> {
        if batches.is_empty() {
            return Err(anyhow::anyhow!("No batches to merge"));
        }

        if batches.len() == 1 {
            return Ok(batches.into_iter().next().unwrap());
        }

        // 简化实现：使用第一个批次的schema
        let schema = batches[0].schema();
        let mut merged_arrays = Vec::new();
        
        for col_idx in 0..schema.fields().len() {
            let mut column_arrays = Vec::new();
            for batch in &batches {
                column_arrays.push(batch.column(col_idx));
            }
            
            // 合并列数组
            let column_refs: Vec<&dyn Array> = column_arrays.iter().map(|arr| arr.as_ref()).collect();
            let merged_array = arrow::compute::concat(&column_refs)?;
            merged_arrays.push(merged_array);
        }
        
        Ok(RecordBatch::try_new(schema, merged_arrays)?)
    }

    /// 检测数据倾斜
    fn detect_skew(&self) -> bool {
        if !self.config.enable_skew_detection {
            return false;
        }

        let max_rows = self.partition_stats.iter()
            .map(|stats| stats.row_count)
            .max()
            .unwrap_or(0);
        
        let min_rows = self.partition_stats.iter()
            .map(|stats| stats.row_count)
            .min()
            .unwrap_or(0);
        
        max_rows > self.config.skew_threshold && (max_rows - min_rows) > self.config.skew_threshold / 2
    }
}

impl Operator for LocalShuffleOperator {
    fn on_register(&mut self, _ctx: OperatorContext) -> Result<()> {
        Ok(())
    }
    
    fn on_event(&mut self, ev: Event, out: &mut Outbox) -> OpStatus {
        match ev {
            Event::Data(_, batch) => {
                match self.process_batch(batch, out) {
                    Ok(status) => status,
                    Err(e) => OpStatus::Error(format!("Failed to process batch: {}", e)),
                }
            }
            Event::Flush(_) => {
                self.finished = true;
                OpStatus::Ready
            }
            Event::Finish(_) => {
                self.finished = true;
                OpStatus::Finished
            }
            _ => OpStatus::Ready,
        }
    }
    
    fn is_finished(&self) -> bool {
        self.finished
    }
    
    fn name(&self) -> &str {
        "LocalShuffle"
    }
}

impl SingleInputOperator for LocalShuffleOperator {
    fn process_batch(&mut self, batch: RecordBatch, out: &mut Outbox) -> Result<OpStatus> {
        // 初始化分区列索引（如果还没有初始化）
        if self.partition_column_indices.is_empty() {
            self.initialize_partition_columns(&batch.schema())?;
        }

        // 按行处理数据
        let mut partition_batches: Vec<Vec<RecordBatch>> = vec![vec![]; self.config.partition_count];
        
        for row_idx in 0..batch.num_rows() {
            // 计算分区ID
            let partition_id = self.calculate_partition_id(&batch, row_idx)?;
            
            // 提取单行数据
            let single_row_batch = self.extract_single_row(&batch, row_idx)?;
            
            // 更新分区统计
            self.partition_stats[partition_id].row_count += 1;
            self.partition_stats[partition_id].byte_count += single_row_batch.get_array_memory_size();
            self.partition_stats[partition_id].last_update = Instant::now();
            
            // 添加到对应分区
            partition_batches[partition_id].push(single_row_batch);
        }
        
        // 合并每个分区的批次并推送到下游
        for (partition_id, batches) in partition_batches.into_iter().enumerate() {
            if !batches.is_empty() {
                let merged_batch = self.merge_batches(batches)?;
                out.push(partition_id as u32, merged_batch)?;
            }
        }
        
        // 记录指标
        let duration = std::time::Duration::from_millis(1); // 简化实现
        self.record_metrics(&batch, duration);
        
        Ok(OpStatus::Ready)
    }
}

impl MultiOutputOperator for LocalShuffleOperator {
    fn output_port_count(&self) -> usize {
        self.config.partition_count
    }
    
    fn select_output_port(&self, batch: &RecordBatch, row_idx: usize) -> PortId {
        // 计算分区ID
        match self.calculate_partition_id(batch, row_idx) {
            Ok(partition_id) => {
                let output_ports = self.base.output_ports();
                output_ports[partition_id % output_ports.len()]
            }
            Err(_) => {
                // 出错时使用第一个输出端口
                self.base.output_ports()[0]
            }
        }
    }
}

impl MetricsSupport for LocalShuffleOperator {
    fn record_metrics(&self, batch: &RecordBatch, duration: std::time::Duration) {
        tracing::debug!(
            "LocalShuffle: processed batch with {} rows, {} columns in {:?}",
            batch.num_rows(),
            batch.num_columns(),
            duration
        );
    }
    
    fn get_metrics(&self) -> OperatorMetrics {
        self.metrics.clone()
    }
}
