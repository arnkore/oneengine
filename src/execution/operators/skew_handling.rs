use std::collections::HashMap;
use std::time::Instant;
use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use tracing::{debug, info};

/// 数据倾斜检测和处理配置
#[derive(Debug, Clone)]
pub struct SkewHandlingConfig {
    /// 倾斜检测阈值（标准差倍数）
    pub skew_threshold: f64,
    /// 最小分区数量
    pub min_partitions: usize,
    /// 最大分区数量
    pub max_partitions: usize,
    /// 重分区阈值（数据量倍数）
    pub repartition_threshold: f64,
    /// 采样率（0.0-1.0）
    pub sampling_rate: f64,
    /// 统计窗口大小
    pub window_size: usize,
}

impl Default for SkewHandlingConfig {
    fn default() -> Self {
        Self {
            skew_threshold: 2.0,      // 2倍标准差
            min_partitions: 2,
            max_partitions: 64,
            repartition_threshold: 1.5, // 1.5倍平均数据量
            sampling_rate: 0.1,       // 10%采样
            window_size: 100,         // 100个批次
        }
    }
}

/// 分区统计信息
#[derive(Debug, Clone)]
pub struct PartitionStats {
    /// 分区ID
    pub partition_id: usize,
    /// 数据行数
    pub row_count: usize,
    /// 数据大小（字节）
    pub data_size: usize,
    /// 处理时间（微秒）
    pub processing_time_us: u64,
    /// 最后更新时间
    pub last_update: Instant,
}

impl Default for PartitionStats {
    fn default() -> Self {
        Self {
            partition_id: 0,
            row_count: 0,
            data_size: 0,
            processing_time_us: 0,
            last_update: Instant::now(),
        }
    }
}

/// 倾斜检测结果
#[derive(Debug, Clone)]
pub enum SkewDetectionResult {
    /// 无倾斜
    NoSkew,
    /// 轻微倾斜
    LightSkew {
        /// 倾斜分区ID
        skewed_partitions: Vec<usize>,
        /// 倾斜程度（0.0-1.0）
        skew_ratio: f64,
    },
    /// 严重倾斜
    HeavySkew {
        /// 倾斜分区ID
        skewed_partitions: Vec<usize>,
        /// 倾斜程度（0.0-1.0）
        skew_ratio: f64,
        /// 建议的重分区数量
        suggested_partitions: usize,
    },
}

/// 重分区策略
#[derive(Debug, Clone)]
pub enum RepartitionStrategy {
    /// 基于哈希的重分区
    HashRepartition {
        /// 新的分区数量
        new_partition_count: usize,
        /// 分区键列
        partition_columns: Vec<String>,
    },
    /// 基于范围的重分区
    RangeRepartition {
        /// 新的分区数量
        new_partition_count: usize,
        /// 分区键列
        partition_columns: Vec<String>,
        /// 范围边界
        range_bounds: Vec<f64>,
    },
    /// 基于轮询的重分区
    RoundRobinRepartition {
        /// 新的分区数量
        new_partition_count: usize,
    },
}

/// 数据倾斜检测和处理器
pub struct SkewHandler {
    /// 配置
    config: SkewHandlingConfig,
    /// 分区统计信息
    partition_stats: HashMap<usize, PartitionStats>,
    /// 统计窗口
    stats_window: Vec<PartitionStats>,
    /// 当前分区数量
    current_partition_count: usize,
    /// 是否启用倾斜处理
    enabled: bool,
}

impl SkewHandler {
    /// 创建新的倾斜处理器
    pub fn new(config: SkewHandlingConfig) -> Self {
        Self {
            config,
            partition_stats: HashMap::new(),
            stats_window: Vec::new(),
            current_partition_count: 0,
            enabled: true,
        }
    }

    /// 记录分区统计信息
    pub fn record_partition_stats(&mut self, partition_id: usize, row_count: usize, data_size: usize, processing_time_us: u64) {
        let stats = PartitionStats {
            partition_id,
            row_count,
            data_size,
            processing_time_us,
            last_update: Instant::now(),
        };

        self.partition_stats.insert(partition_id, stats.clone());
        self.stats_window.push(stats);

        // 保持窗口大小
        if self.stats_window.len() > self.config.window_size {
            self.stats_window.remove(0);
        }

        debug!("记录分区统计: partition_id={}, row_count={}, data_size={}, processing_time_us={}", 
               partition_id, row_count, data_size, processing_time_us);
    }

    /// 检测数据倾斜
    pub fn detect_skew(&self) -> SkewDetectionResult {
        if self.stats_window.len() < 2 {
            return SkewDetectionResult::NoSkew;
        }

        // 计算每个分区的平均数据量
        let mut partition_avg_sizes: Vec<f64> = Vec::new();
        let mut partition_counts: HashMap<usize, (usize, usize)> = HashMap::new();

        for stats in &self.stats_window {
            let entry = partition_counts.entry(stats.partition_id).or_insert((0, 0));
            entry.0 += stats.data_size;
            entry.1 += 1;
        }

        for (_, (total_size, count)) in partition_counts {
            if count > 0 {
                partition_avg_sizes.push(total_size as f64 / count as f64);
            }
        }

        if partition_avg_sizes.len() < 2 {
            return SkewDetectionResult::NoSkew;
        }

        // 计算统计信息
        let mean = partition_avg_sizes.iter().sum::<f64>() / partition_avg_sizes.len() as f64;
        let variance = partition_avg_sizes.iter()
            .map(|&x| (x - mean).powi(2))
            .sum::<f64>() / partition_avg_sizes.len() as f64;
        let std_dev = variance.sqrt();

        // 检测倾斜
        let skew_threshold = mean + self.config.skew_threshold * std_dev;
        let mut skewed_partitions = Vec::new();
        let mut max_skew_ratio: f64 = 0.0;

        for (i, &size) in partition_avg_sizes.iter().enumerate() {
            if size > skew_threshold {
                skewed_partitions.push(i);
                let skew_ratio = (size - mean) / mean;
                max_skew_ratio = max_skew_ratio.max(skew_ratio);
            }
        }

        if skewed_partitions.is_empty() {
            SkewDetectionResult::NoSkew
        } else if max_skew_ratio < 0.5 {
            SkewDetectionResult::LightSkew {
                skewed_partitions,
                skew_ratio: max_skew_ratio,
            }
        } else {
        let suggested_partitions = ((self.current_partition_count as f64 * (1.0 + max_skew_ratio)) as usize)
            .min(self.config.max_partitions)
            .max(self.config.min_partitions);
            
            SkewDetectionResult::HeavySkew {
                skewed_partitions,
                skew_ratio: max_skew_ratio,
                suggested_partitions,
            }
        }
    }

    /// 生成重分区策略
    pub fn generate_repartition_strategy(&self, detection_result: &SkewDetectionResult, schema: &Schema) -> Option<RepartitionStrategy> {
        match detection_result {
            SkewDetectionResult::NoSkew => None,
            SkewDetectionResult::LightSkew { .. } => {
                // 轻微倾斜，使用轮询重分区
                Some(RepartitionStrategy::RoundRobinRepartition {
                    new_partition_count: self.current_partition_count * 2,
                })
            }
            SkewDetectionResult::HeavySkew { suggested_partitions, .. } => {
                // 严重倾斜，使用哈希重分区
                let partition_columns = self.select_partition_columns(schema);
                Some(RepartitionStrategy::HashRepartition {
                    new_partition_count: *suggested_partitions,
                    partition_columns,
                })
            }
        }
    }

    /// 选择分区键列
    fn select_partition_columns(&self, schema: &Schema) -> Vec<String> {
        // 简单实现：选择前几个数值列作为分区键
        let mut partition_columns = Vec::new();
        for field in schema.fields() {
            if partition_columns.len() >= 3 {
                break;
            }
            match field.data_type() {
                arrow::datatypes::DataType::Int32
                | arrow::datatypes::DataType::Int64
                | arrow::datatypes::DataType::Float32
                | arrow::datatypes::DataType::Float64
                | arrow::datatypes::DataType::Utf8 => {
                    partition_columns.push(field.name().clone());
                }
                _ => {}
            }
        }
        partition_columns
    }

    /// 应用重分区策略
    pub fn apply_repartition_strategy(&mut self, strategy: &RepartitionStrategy, batch: &RecordBatch) -> Result<RecordBatch, String> {
        match strategy {
            RepartitionStrategy::HashRepartition { new_partition_count, partition_columns } => {
                self.apply_hash_repartition(*new_partition_count, partition_columns, batch)
            }
            RepartitionStrategy::RangeRepartition { new_partition_count, partition_columns, range_bounds } => {
                self.apply_range_repartition(*new_partition_count, partition_columns, range_bounds, batch)
            }
            RepartitionStrategy::RoundRobinRepartition { new_partition_count } => {
                self.apply_round_robin_repartition(*new_partition_count, batch)
            }
        }
    }

    /// 应用哈希重分区
    fn apply_hash_repartition(&mut self, new_partition_count: usize, partition_columns: &[String], batch: &RecordBatch) -> Result<RecordBatch, String> {
        // 简化实现：直接返回原批次
        // 在实际实现中，这里应该根据分区键列进行哈希分区
        self.current_partition_count = new_partition_count;
        info!("应用哈希重分区: new_partition_count={}, partition_columns={:?}", new_partition_count, partition_columns);
        Ok(batch.clone())
    }

    /// 应用范围重分区
    fn apply_range_repartition(&mut self, new_partition_count: usize, partition_columns: &[String], range_bounds: &[f64], batch: &RecordBatch) -> Result<RecordBatch, String> {
        // 简化实现：直接返回原批次
        self.current_partition_count = new_partition_count;
        info!("应用范围重分区: new_partition_count={}, partition_columns={:?}, range_bounds={:?}", 
              new_partition_count, partition_columns, range_bounds);
        Ok(batch.clone())
    }

    /// 应用轮询重分区
    fn apply_round_robin_repartition(&mut self, new_partition_count: usize, batch: &RecordBatch) -> Result<RecordBatch, String> {
        // 简化实现：直接返回原批次
        self.current_partition_count = new_partition_count;
        info!("应用轮询重分区: new_partition_count={}", new_partition_count);
        Ok(batch.clone())
    }

    /// 获取分区统计摘要
    pub fn get_partition_summary(&self) -> String {
        let mut summary = String::new();
        summary.push_str(&format!("分区统计摘要 (共{}个分区):\n", self.partition_stats.len()));
        
        for (partition_id, stats) in &self.partition_stats {
            summary.push_str(&format!("  分区{}: 行数={}, 大小={}B, 处理时间={}μs\n", 
                partition_id, stats.row_count, stats.data_size, stats.processing_time_us));
        }
        
        summary
    }

    /// 重置统计信息
    pub fn reset_stats(&mut self) {
        self.partition_stats.clear();
        self.stats_window.clear();
        info!("重置倾斜检测统计信息");
    }

    /// 启用/禁用倾斜处理
    pub fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
        info!("倾斜处理{}", if enabled { "启用" } else { "禁用" });
    }

    /// 检查是否启用
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }
}

/// 简化的倾斜处理器（用于示例）
pub struct SkewHandlerSync {
    handler: SkewHandler,
}

impl SkewHandlerSync {
    pub fn new(config: SkewHandlingConfig) -> Self {
        Self {
            handler: SkewHandler::new(config),
        }
    }

    pub fn record_partition_stats(&mut self, partition_id: usize, row_count: usize, data_size: usize, processing_time_us: u64) {
        self.handler.record_partition_stats(partition_id, row_count, data_size, processing_time_us);
    }

    pub fn detect_skew(&self) -> SkewDetectionResult {
        self.handler.detect_skew()
    }

    pub fn generate_repartition_strategy(&self, detection_result: &SkewDetectionResult, schema: &Schema) -> Option<RepartitionStrategy> {
        self.handler.generate_repartition_strategy(detection_result, schema)
    }

    pub fn apply_repartition_strategy(&mut self, strategy: &RepartitionStrategy, batch: &RecordBatch) -> Result<RecordBatch, String> {
        self.handler.apply_repartition_strategy(strategy, batch)
    }

    pub fn get_partition_summary(&self) -> String {
        self.handler.get_partition_summary()
    }

    pub fn reset_stats(&mut self) {
        self.handler.reset_stats();
    }

    pub fn set_enabled(&mut self, enabled: bool) {
        self.handler.set_enabled(enabled);
    }

    pub fn is_enabled(&self) -> bool {
        self.handler.is_enabled()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{Field, DataType, Schema};

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ])
    }

    fn create_test_batch() -> RecordBatch {
        let schema = create_test_schema();
        let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie", "David", "Eve"]);
        let value_array = Int32Array::from(vec![10, 20, 30, 40, 50]);
        
        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(id_array),
                Arc::new(name_array),
                Arc::new(value_array),
        ]).unwrap()
    }

    #[test]
    fn test_skew_detection_no_skew() {
        let config = SkewHandlingConfig::default();
        let mut handler = SkewHandler::new(config);
        
        // 添加均匀分布的数据
        for i in 0..10 {
            handler.record_partition_stats(i, 100, 1000, 1000);
        }
        
        let result = handler.detect_skew();
        match result {
            SkewDetectionResult::NoSkew => assert!(true),
            _ => assert!(false, "应该检测到无倾斜"),
        }
    }

    #[test]
    fn test_skew_detection_light_skew() {
        let config = SkewHandlingConfig {
            skew_threshold: 1.5,
            ..Default::default()
        };
        let mut handler = SkewHandler::new(config);
        
        // 添加轻微倾斜的数据
        for i in 0..8 {
            handler.record_partition_stats(i, 100, 1000, 1000);
        }
        handler.record_partition_stats(8, 100, 2000, 2000); // 倾斜分区
        handler.record_partition_stats(9, 100, 1000, 1000);
        
        let result = handler.detect_skew();
        match result {
            SkewDetectionResult::LightSkew { skewed_partitions, skew_ratio } => {
                assert!(!skewed_partitions.is_empty());
                assert!(skew_ratio > 0.0);
            }
            _ => assert!(false, "应该检测到轻微倾斜"),
        }
    }

    #[test]
    fn test_skew_detection_heavy_skew() {
        let config = SkewHandlingConfig {
            skew_threshold: 1.0,
            ..Default::default()
        };
        let mut handler = SkewHandler::new(config);
        
        // 添加严重倾斜的数据
        for i in 0..8 {
            handler.record_partition_stats(i, 100, 1000, 1000);
        }
        handler.record_partition_stats(8, 100, 5000, 5000); // 严重倾斜分区
        handler.record_partition_stats(9, 100, 1000, 1000);
        
        let result = handler.detect_skew();
        match result {
            SkewDetectionResult::HeavySkew { skewed_partitions, skew_ratio, suggested_partitions } => {
                assert!(!skewed_partitions.is_empty());
                assert!(skew_ratio > 0.5);
                assert!(suggested_partitions > 0);
            }
            _ => assert!(false, "应该检测到严重倾斜"),
        }
    }

    #[test]
    fn test_repartition_strategy_generation() {
        let config = SkewHandlingConfig::default();
        let handler = SkewHandler::new(config);
        let schema = create_test_schema();
        
        let detection_result = SkewDetectionResult::HeavySkew {
            skewed_partitions: vec![0, 1],
            skew_ratio: 0.8,
            suggested_partitions: 16,
        };
        
        let strategy = handler.generate_repartition_strategy(&detection_result, &schema);
        assert!(strategy.is_some());
        
        match strategy.unwrap() {
            RepartitionStrategy::HashRepartition { new_partition_count, partition_columns } => {
                assert_eq!(new_partition_count, 16);
                assert!(!partition_columns.is_empty());
            }
            _ => assert!(false, "应该生成哈希重分区策略"),
        }
    }
}
