//! Parquet扫描算子
//! 
//! 基于Arrow的Parquet文件扫描实现，支持RowGroup剪枝和PageIndex选择

use super::{BaseOperator, SingleInputOperator, MetricsSupport, OperatorMetrics};
use crate::push_runtime::{Operator, OperatorContext, Event, OpStatus, Outbox, PortId};
use crate::io::parquet_reader::{ParquetReader, ParquetReaderConfig, ColumnSelection, Predicate};
use arrow::record_batch::RecordBatch;
use arrow::datatypes::SchemaRef;
use anyhow::Result;
use std::sync::Arc;
use std::collections::VecDeque;

/// Parquet扫描配置
#[derive(Debug, Clone)]
pub struct ScanParquetConfig {
    /// 文件路径
    pub file_path: String,
    /// 列选择
    pub column_selection: ColumnSelection,
    /// 谓词列表
    pub predicates: Vec<Predicate>,
    /// 批次大小
    pub batch_size: usize,
    /// 是否启用RowGroup剪枝
    pub enable_rowgroup_pruning: bool,
    /// 是否启用PageIndex选择
    pub enable_page_index_selection: bool,
}

impl Default for ScanParquetConfig {
    fn default() -> Self {
        Self {
            file_path: String::new(),
            column_selection: ColumnSelection::all(),
            predicates: Vec::new(),
            batch_size: 8192,
            enable_rowgroup_pruning: true,
            enable_page_index_selection: true,
        }
    }
}

/// Parquet扫描算子
pub struct ScanParquetOperator {
    /// 基础算子
    base: BaseOperator,
    /// 配置
    config: ScanParquetConfig,
    /// 统计信息
    metrics: OperatorMetrics,
    /// Parquet读取器
    reader: Option<ParquetReader>,
    /// 待处理的批次队列
    batch_queue: VecDeque<RecordBatch>,
    /// 是否已完成扫描
    scan_completed: bool,
}

impl ScanParquetOperator {
    /// 创建新的Parquet扫描算子
    pub fn new(
        operator_id: u32,
        input_ports: Vec<PortId>,
        output_ports: Vec<PortId>,
        config: ScanParquetConfig,
    ) -> Self {
        Self {
            base: BaseOperator::new(
                operator_id,
                input_ports,
                output_ports,
                "ScanParquet".to_string(),
            ),
            config,
            metrics: OperatorMetrics::default(),
            reader: None,
            batch_queue: VecDeque::new(),
            scan_completed: false,
        }
    }
    
    /// 初始化Parquet读取器
    fn initialize_reader(&mut self) -> Result<()> {
        let parquet_config = ParquetReaderConfig {
            column_selection: self.config.column_selection.clone(),
            predicates: self.config.predicates.clone(),
            batch_size: self.config.batch_size,
            enable_rowgroup_pruning: self.config.enable_rowgroup_pruning,
            enable_page_index_selection: self.config.enable_page_index_selection,
            max_rowgroups: None,
        };
        
        let mut reader = ParquetReader::new(self.config.file_path.clone(), parquet_config);
        reader.open()?;
        
        // 预读取所有批次到队列中
        let batches = reader.read_batches()?;
        for batch in batches {
            self.batch_queue.push_back(batch);
        }
        
        self.reader = Some(reader);
        Ok(())
    }
    
    /// 处理扫描逻辑
    fn process_scan(&mut self, out: &mut Outbox) -> OpStatus {
        if let Some(batch) = self.batch_queue.pop_front() {
            // 发送数据到下游
            if let Err(e) = out.push(0, batch.clone()) {
                tracing::error!("Failed to push batch: {}", e);
                return OpStatus::Error(format!("Failed to push batch: {}", e));
            }
            
            // 记录指标
            self.record_metrics(&batch, std::time::Duration::from_millis(0));
            
            OpStatus::Ready
        } else {
            // 没有更多数据，标记完成
            self.scan_completed = true;
            OpStatus::Finished
        }
    }
}

impl Operator for ScanParquetOperator {
    fn on_register(&mut self, _ctx: OperatorContext) -> Result<()> {
        // 初始化Parquet读取器
        self.initialize_reader()?;
        Ok(())
    }
    
    fn on_event(&mut self, ev: Event, out: &mut Outbox) -> OpStatus {
        match ev {
            Event::Data { .. } => {
                // 扫描算子不需要处理输入数据事件
                OpStatus::Ready
            }
            Event::Ctrl { .. } => {
                // 处理控制事件，开始扫描
                if !self.scan_completed {
                    self.process_scan(out)
                } else {
                    OpStatus::Ready
                }
            }
            Event::Credit { .. } => {
                // 处理credit事件，继续扫描
                if !self.scan_completed {
                    self.process_scan(out)
                } else {
                    OpStatus::Ready
                }
            }
            Event::Flush(_) => {
                // 处理flush事件
                if !self.scan_completed {
                    self.process_scan(out)
                } else {
                    OpStatus::Ready
                }
            }
            Event::Finish(_) => {
                // 标记扫描完成
                self.scan_completed = true;
                OpStatus::Finished
            }
        }
    }
    
    fn is_finished(&self) -> bool {
        self.scan_completed && self.batch_queue.is_empty()
    }
    
    fn name(&self) -> &str {
        self.base.name()
    }
}

impl SingleInputOperator for ScanParquetOperator {
    fn process_batch(&mut self, _batch: RecordBatch, _out: &mut Outbox) -> Result<OpStatus> {
        // 扫描算子不需要处理输入批次，它生成数据
        Ok(OpStatus::Ready)
    }
}

impl MetricsSupport for ScanParquetOperator {
    fn record_metrics(&self, batch: &RecordBatch, _duration: std::time::Duration) {
        // 记录扫描指标
        tracing::debug!(
            "ScanParquet: processed batch with {} rows, {} columns",
            batch.num_rows(),
            batch.num_columns()
        );
    }
    
    fn get_metrics(&self) -> OperatorMetrics {
        self.metrics.clone()
    }
}
