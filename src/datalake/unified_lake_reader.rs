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

//! 统一湖仓读取器
//! 
//! 提供统一的湖仓格式读取接口，支持Iceberg、Paimon、Hudi等格式
//! 通过格式适配器实现不同湖仓格式的特定优化

use arrow::record_batch::RecordBatch;
use arrow::datatypes::Schema;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use anyhow::Result;
use tracing::{debug, info, warn};

use crate::datalake::adapters::*;

/// 湖仓格式类型
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LakeFormat {
    /// Apache Iceberg
    Iceberg,
    /// Apache Paimon
    Paimon,
    /// Apache Hudi
    Hudi,
    /// 原生Parquet
    Parquet,
    /// 原生ORC
    Orc,
}

/// 统一谓词类型
#[derive(Debug, Clone)]
pub enum UnifiedPredicate {
    /// 等值比较
    Equal { column: String, value: String },
    /// 大于比较
    GreaterThan { column: String, value: String },
    /// 小于比较
    LessThan { column: String, value: String },
    /// 范围比较
    Between { column: String, min: String, max: String },
    /// IN谓词
    In { column: String, values: Vec<String> },
    /// 空值检查
    IsNull { column: String },
    /// 非空值检查
    IsNotNull { column: String },
    /// 模糊匹配
    Like { column: String, pattern: String },
}

/// 时间旅行配置
#[derive(Debug, Clone)]
pub struct TimeTravelConfig {
    /// 快照ID
    pub snapshot_id: Option<i64>,
    /// 时间戳（毫秒）
    pub timestamp_ms: Option<i64>,
    /// 分支名称
    pub branch: Option<String>,
    /// 标签名称
    pub tag: Option<String>,
}

/// 增量读取配置
#[derive(Debug, Clone)]
pub struct IncrementalReadConfig {
    /// 起始快照ID
    pub start_snapshot_id: Option<i64>,
    /// 结束快照ID
    pub end_snapshot_id: Option<i64>,
    /// 起始时间戳
    pub start_timestamp_ms: Option<i64>,
    /// 结束时间戳
    pub end_timestamp_ms: Option<i64>,
}

/// 分区剪枝信息
#[derive(Debug, Clone)]
pub struct PartitionPruningInfo {
    /// 分区列
    pub partition_columns: Vec<String>,
    /// 分区值
    pub partition_values: HashMap<String, String>,
    /// 是否匹配
    pub matches: bool,
}

/// 列投影信息
#[derive(Debug, Clone)]
pub struct ColumnProjection {
    /// 投影列
    pub columns: Vec<String>,
    /// 是否选择所有列
    pub select_all: bool,
}

/// 统一湖仓读取器配置
#[derive(Debug, Clone)]
pub struct UnifiedLakeReaderConfig {
    /// 湖仓格式
    pub format: LakeFormat,
    /// 表路径
    pub table_path: String,
    /// 仓库路径
    pub warehouse_path: Option<String>,
    /// 谓词列表
    pub predicates: Vec<UnifiedPredicate>,
    /// 列投影
    pub column_projection: Option<ColumnProjection>,
    /// 时间旅行配置
    pub time_travel: Option<TimeTravelConfig>,
    /// 增量读取配置
    pub incremental_read: Option<IncrementalReadConfig>,
    /// 分区剪枝信息
    pub partition_pruning: Option<PartitionPruningInfo>,
    /// 批次大小
    pub batch_size: usize,
    /// 是否启用谓词下推
    pub enable_predicate_pushdown: bool,
    /// 是否启用列投影
    pub enable_column_projection: bool,
    /// 是否启用分区剪枝
    pub enable_partition_pruning: bool,
    /// 是否启用时间旅行
    pub enable_time_travel: bool,
    /// 是否启用增量读取
    pub enable_incremental_read: bool,
    /// 最大并发读取数
    pub max_concurrent_reads: usize,
}

impl Default for UnifiedLakeReaderConfig {
    fn default() -> Self {
        Self {
            format: LakeFormat::Parquet,
            table_path: "".to_string(),
            warehouse_path: None,
            predicates: Vec::new(),
            column_projection: None,
            time_travel: None,
            incremental_read: None,
            partition_pruning: None,
            batch_size: 8192,
            enable_predicate_pushdown: true,
            enable_column_projection: true,
            enable_partition_pruning: true,
            enable_time_travel: false,
            enable_incremental_read: false,
            max_concurrent_reads: 4,
        }
    }
}

/// 表元数据信息
#[derive(Debug, Clone)]
pub struct TableMetadata {
    /// 表ID
    pub table_id: String,
    /// 表名
    pub table_name: String,
    /// 命名空间
    pub namespace: String,
    /// 当前快照ID
    pub current_snapshot_id: Option<i64>,
    /// 快照列表
    pub snapshots: Vec<SnapshotInfo>,
    /// 分区规范
    pub partition_spec: Option<PartitionSpec>,
    /// 排序规范
    pub sort_order: Option<SortOrder>,
    /// 表属性
    pub properties: HashMap<String, String>,
    /// 创建时间
    pub created_at: i64,
    /// 更新时间
    pub updated_at: i64,
}

/// 快照信息
#[derive(Debug, Clone)]
pub struct SnapshotInfo {
    /// 快照ID
    pub snapshot_id: i64,
    /// 时间戳
    pub timestamp_ms: i64,
    /// 父快照ID
    pub parent_snapshot_id: Option<i64>,
    /// 操作类型
    pub operation: String,
    /// 摘要信息
    pub summary: HashMap<String, String>,
}

/// 分区规范
#[derive(Debug, Clone)]
pub struct PartitionSpec {
    /// 分区字段
    pub partition_fields: Vec<PartitionField>,
    /// 规范ID
    pub spec_id: i32,
}

/// 分区字段
#[derive(Debug, Clone)]
pub struct PartitionField {
    /// 字段名
    pub field_name: String,
    /// 字段ID
    pub field_id: i32,
    /// 转换类型
    pub transform: String,
}

/// 排序规范
#[derive(Debug, Clone)]
pub struct SortOrder {
    /// 排序字段
    pub sort_fields: Vec<SortField>,
    /// 排序ID
    pub order_id: i32,
}

/// 排序字段
#[derive(Debug, Clone)]
pub struct SortField {
    /// 字段名
    pub field_name: String,
    /// 字段ID
    pub field_id: i32,
    /// 排序方向
    pub direction: String,
    /// 空值位置
    pub null_order: String,
}

/// 表统计信息
#[derive(Debug, Clone)]
pub struct TableStatistics {
    /// 总文件数
    pub total_files: u64,
    /// 总记录数
    pub total_records: u64,
    /// 总文件大小（字节）
    pub total_size_bytes: u64,
    /// 分区数
    pub partition_count: u64,
    /// 快照数
    pub snapshot_count: u64,
    /// 平均文件大小
    pub avg_file_size_bytes: u64,
    /// 平均记录数
    pub avg_records_per_file: u64,
}

/// 格式适配器trait
pub trait FormatAdapter {
    /// 打开表
    fn open_table(&mut self, table_path: &str) -> Result<()>;
    
    /// 获取表元数据
    fn get_metadata(&self) -> Result<TableMetadata>;
    
    /// 获取表统计信息
    fn get_statistics(&self) -> Result<TableStatistics>;
    
    /// 应用谓词下推
    fn apply_predicate_pushdown(&mut self, predicates: &[UnifiedPredicate]) -> Result<u64>;
    
    /// 应用列投影
    fn apply_column_projection(&mut self, projection: &ColumnProjection) -> Result<()>;
    
    /// 应用分区剪枝
    fn apply_partition_pruning(&mut self, pruning: &PartitionPruningInfo) -> Result<u64>;
    
    /// 应用时间旅行
    fn apply_time_travel(&mut self, time_travel: &TimeTravelConfig) -> Result<()>;
    
    /// 应用增量读取
    fn apply_incremental_read(&mut self, incremental: &IncrementalReadConfig) -> Result<()>;
    
    /// 读取数据
    fn read_data(&mut self) -> Result<Vec<RecordBatch>>;
    
    /// 获取表Schema
    fn get_schema(&self) -> Result<Schema>;
}

/// 统一湖仓读取器
pub struct UnifiedLakeReader {
    config: UnifiedLakeReaderConfig,
    adapter: Box<dyn FormatAdapter + Send + Sync>,
    /// 是否已打开
    opened: bool,
    /// 当前快照ID
    current_snapshot_id: Option<i64>,
    /// 表元数据
    table_metadata: Option<TableMetadata>,
    /// 统计信息
    statistics: Option<TableStatistics>,
}

impl UnifiedLakeReader {
    /// 创建新的统一湖仓读取器
    pub fn new(config: UnifiedLakeReaderConfig) -> Self {
        let adapter = Self::create_adapter(&config.format);
        
        Self {
            config,
            adapter,
            opened: false,
            current_snapshot_id: None,
            table_metadata: None,
            statistics: None,
        }
    }
    
    /// 创建格式适配器
    fn create_adapter(format: &LakeFormat) -> Box<dyn FormatAdapter + Send + Sync> {
        match format {
            LakeFormat::Iceberg => {
                // 使用Iceberg适配器
                Box::new(IcebergAdapter::new())
            },
            LakeFormat::Paimon => {
                // 使用Paimon适配器
                Box::new(PaimonAdapter::new())
            },
            LakeFormat::Hudi => {
                // 使用Hudi适配器
                Box::new(HudiAdapter::new())
            },
            LakeFormat::Parquet => {
                // 使用Parquet适配器
                Box::new(ParquetAdapter::new())
            },
            LakeFormat::Orc => {
                // 对于原生ORC文件，我们使用现有的OrcDataLakeReader
                // 这里可以返回一个包装器或者直接使用OrcDataLakeReader
                todo!("ORC format should use existing OrcDataLakeReader directly")
            },
        }
    }
    
    /// 打开表
    pub fn open(&mut self) -> Result<()> {
        self.adapter.open_table(&self.config.table_path)?;
        self.opened = true;
        
        // 获取元数据
        self.table_metadata = Some(self.adapter.get_metadata()?);
        self.statistics = Some(self.adapter.get_statistics()?);
        
        info!("Opened {} table: {}", 
              format!("{:?}", self.config.format).to_lowercase(), 
              self.config.table_path);
        
        Ok(())
    }
    
    /// 获取表元数据
    pub fn get_metadata(&self) -> Result<&TableMetadata> {
        self.table_metadata.as_ref()
            .ok_or_else(|| anyhow::anyhow!("Table not opened"))
    }
    
    /// 获取表统计信息
    pub fn get_statistics(&self) -> Result<&TableStatistics> {
        self.statistics.as_ref()
            .ok_or_else(|| anyhow::anyhow!("Table not opened"))
    }
    
    /// 应用谓词下推
    pub fn apply_predicate_pushdown(&mut self) -> Result<u64> {
        if !self.config.enable_predicate_pushdown || self.config.predicates.is_empty() {
            return Ok(0);
        }
        
        let start = Instant::now();
        let filtered_files = self.adapter.apply_predicate_pushdown(&self.config.predicates)?;
        let duration = start.elapsed();
        
        debug!("Applied predicate pushdown: {} files filtered in {:?}", 
               filtered_files, duration);
        
        Ok(filtered_files)
    }
    
    /// 应用列投影
    pub fn apply_column_projection(&mut self) -> Result<()> {
        if !self.config.enable_column_projection {
            return Ok(());
        }
        
        if let Some(ref projection) = self.config.column_projection {
            let start = Instant::now();
            self.adapter.apply_column_projection(projection)?;
            let duration = start.elapsed();
            
            debug!("Applied column projection in {:?}", duration);
        }
        
        Ok(())
    }
    
    /// 应用分区剪枝
    pub fn apply_partition_pruning(&mut self) -> Result<u64> {
        if !self.config.enable_partition_pruning {
            return Ok(0);
        }
        
        if let Some(ref pruning) = self.config.partition_pruning {
            let start = Instant::now();
            let pruned_files = self.adapter.apply_partition_pruning(pruning)?;
            let duration = start.elapsed();
            
            debug!("Applied partition pruning: {} files pruned in {:?}", 
                   pruned_files, duration);
            
            Ok(pruned_files)
        } else {
            Ok(0)
        }
    }
    
    /// 应用时间旅行
    pub fn apply_time_travel(&mut self) -> Result<()> {
        if !self.config.enable_time_travel {
            return Ok(());
        }
        
        if let Some(ref time_travel) = self.config.time_travel {
            let start = Instant::now();
            self.adapter.apply_time_travel(time_travel)?;
            let duration = start.elapsed();
            
            debug!("Applied time travel in {:?}", duration);
        }
        
        Ok(())
    }
    
    /// 应用增量读取
    pub fn apply_incremental_read(&mut self) -> Result<()> {
        if !self.config.enable_incremental_read {
            return Ok(());
        }
        
        if let Some(ref incremental) = self.config.incremental_read {
            let start = Instant::now();
            self.adapter.apply_incremental_read(incremental)?;
            let duration = start.elapsed();
            
            debug!("Applied incremental read in {:?}", duration);
        }
        
        Ok(())
    }
    
    /// 读取数据
    pub fn read_data(&mut self) -> Result<Vec<RecordBatch>> {
        if !self.opened {
            return Err(anyhow::anyhow!("Table not opened"));
        }
        
        let start = Instant::now();
        let batches = self.adapter.read_data()?;
        let duration = start.elapsed();
        
        info!("Read {} batches in {:?}", batches.len(), duration);
        
        Ok(batches)
    }
    
    /// 获取表Schema
    pub fn get_schema(&self) -> Result<Schema> {
        self.adapter.get_schema()
    }
    
    /// 设置谓词
    pub fn set_predicates(&mut self, predicates: Vec<UnifiedPredicate>) {
        self.config.predicates = predicates;
    }
    
    /// 设置列投影
    pub fn set_column_projection(&mut self, projection: ColumnProjection) {
        self.config.column_projection = Some(projection);
    }
    
    /// 设置分区剪枝
    pub fn set_partition_pruning(&mut self, pruning: PartitionPruningInfo) {
        self.config.partition_pruning = Some(pruning);
    }
    
    /// 设置时间旅行
    pub fn set_time_travel(&mut self, time_travel: TimeTravelConfig) {
        self.config.time_travel = Some(time_travel);
    }
    
    /// 设置增量读取
    pub fn set_incremental_read(&mut self, incremental: IncrementalReadConfig) {
        self.config.incremental_read = Some(incremental);
    }
}

