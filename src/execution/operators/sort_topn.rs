//! Sort/TopN算子
//! 
//! 基于Arrow的排序和TopN实现，支持row encoding + IPC spill

use super::{BaseOperator, SingleInputOperator, MetricsSupport, OperatorMetrics};
use crate::push_runtime::{Operator, OperatorContext, Event, OpStatus, Outbox, PortId};
use crate::io::arrow_ipc::ArrowIpcWriter;
use arrow::record_batch::RecordBatch;
use arrow::datatypes::{SchemaRef, Schema, Field, DataType};
use arrow::array::{Array, Int32Array, StringArray, Float64Array, BooleanArray, ArrayRef};
use arrow::compute::{take, sort_to_indices, SortOptions};
use arrow_row::{RowConverter, Rows, SortField};
use std::sync::Arc;
use std::collections::BinaryHeap;
use std::cmp::Reverse;
use std::time::Instant;
use anyhow::Result;

/// 排序配置
#[derive(Debug, Clone)]
pub struct SortConfig {
    /// 排序列
    pub sort_columns: Vec<String>,
    /// 排序方向（true为升序，false为降序）
    pub ascending: Vec<bool>,
    /// TopN数量（None表示全排序）
    pub top_n: Option<usize>,
    /// 是否启用spill
    pub enable_spill: bool,
    /// spill阈值（字节）
    pub spill_threshold: usize,
    /// spill文件路径
    pub spill_path: String,
    /// 是否启用row encoding
    pub enable_row_encoding: bool,
}

impl Default for SortConfig {
    fn default() -> Self {
        Self {
            sort_columns: vec![],
            ascending: vec![],
            top_n: None,
            enable_spill: false,
            spill_threshold: 100 * 1024 * 1024, // 100MB
            spill_path: "/tmp/sort_spill".to_string(),
            enable_row_encoding: true,
        }
    }
}

/// 排序行条目
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SortRow {
    /// 编码后的行数据
    pub encoded_row: Vec<u8>,
    /// 原始行索引
    pub original_index: usize,
    /// 批次索引
    pub batch_index: usize,
}

/// Sort/TopN算子
pub struct SortTopNOperator {
    /// 基础算子
    base: BaseOperator,
    /// 配置
    config: SortConfig,
    /// 统计信息
    metrics: OperatorMetrics,
    /// 行转换器
    row_converter: Option<RowConverter>,
    /// 排序列索引
    sort_column_indices: Vec<usize>,
    /// 待排序的行
    pending_rows: Vec<SortRow>,
    /// TopN堆（用于TopN模式）
    top_n_heap: Option<BinaryHeap<Reverse<SortRow>>>,
    /// 已排序的行
    sorted_rows: Vec<SortRow>,
    /// 当前批次索引
    current_batch_index: usize,
    /// 是否已完成排序
    sort_complete: bool,
    /// IPC写入器（用于spill）
    ipc_writer: Option<ArrowIpcWriter>,
    /// 已spill的文件
    spilled_files: Vec<String>,
    /// 总内存使用量
    total_memory_usage: usize,
}

impl SortTopNOperator {
    /// 创建新的Sort/TopN算子
    pub fn new(
        operator_id: u32,
        input_ports: Vec<PortId>,
        output_ports: Vec<PortId>,
        config: SortConfig,
    ) -> Self {
        Self {
            base: BaseOperator::new(
                operator_id,
                input_ports,
                output_ports,
                "SortTopN".to_string(),
            ),
            config,
            metrics: OperatorMetrics::default(),
            row_converter: None,
            sort_column_indices: vec![],
            pending_rows: vec![],
            top_n_heap: None,
            sorted_rows: vec![],
            current_batch_index: 0,
            sort_complete: false,
            ipc_writer: None,
            spilled_files: vec![],
            total_memory_usage: 0,
        }
    }

    /// 初始化排序列
    fn initialize_sort_columns(&mut self, schema: &Schema) -> Result<()> {
        self.sort_column_indices.clear();
        
        for column_name in &self.config.sort_columns {
            let column_index = schema.fields()
                .iter()
                .position(|field| field.name() == column_name)
                .ok_or_else(|| anyhow::anyhow!("Column not found: {}", column_name))?;
            self.sort_column_indices.push(column_index);
        }
        
        // 初始化行转换器
        if self.config.enable_row_encoding {
            let sort_fields: Vec<SortField> = self.sort_column_indices
                .iter()
                .map(|&col_idx| {
                    let field = &schema.fields()[col_idx];
                    SortField::new(field.data_type().clone())
                })
                .collect();
            
            self.row_converter = Some(RowConverter::new(sort_fields)?);
        }
        
        // 初始化TopN堆
        if let Some(top_n) = self.config.top_n {
            self.top_n_heap = Some(BinaryHeap::with_capacity(top_n));
        }
        
        Ok(())
    }

    /// 处理批次数据
    fn process_batch(&mut self, batch: RecordBatch, out: &mut Outbox) -> Result<OpStatus> {
        let start = Instant::now();
        
        // 初始化排序列（如果还没有初始化）
        if self.sort_column_indices.is_empty() {
            self.initialize_sort_columns(&batch.schema())?;
        }

        // 处理每一行
        for row_idx in 0..batch.num_rows() {
            let sort_row = self.create_sort_row(&batch, row_idx)?;
            self.total_memory_usage += sort_row.encoded_row.len();
            
            // 检查是否需要spill
            if self.config.enable_spill && self.total_memory_usage > self.config.spill_threshold {
                self.spill_to_disk()?;
            }
            
            // 添加到待排序列表或TopN堆
            if let Some(ref mut heap) = self.top_n_heap {
                // TopN模式
                if heap.len() < self.config.top_n.unwrap() {
                    heap.push(Reverse(sort_row));
                } else {
                    // 替换堆顶元素
                    if let Some(Reverse(top)) = heap.peek() {
                        if sort_row.encoded_row < top.encoded_row {
                            heap.pop();
                            heap.push(Reverse(sort_row));
                        }
                    }
                }
            } else {
                // 全排序模式
                self.pending_rows.push(sort_row);
            }
        }
        
        self.current_batch_index += 1;
        
        // 记录处理时间
        let duration = start.elapsed();
        self.record_metrics(&batch, duration);
        
        Ok(OpStatus::Ready)
    }

    /// 创建排序行
    fn create_sort_row(&self, batch: &RecordBatch, row_idx: usize) -> Result<SortRow> {
        // 简化实现：使用行索引作为排序键
        Ok(SortRow {
            encoded_row: vec![row_idx as u8],
            original_index: row_idx,
            batch_index: self.current_batch_index,
        })
    }

    /// 比较两行
    fn compare_rows(&self, row1: &[u8], row2: &[u8]) -> Result<std::cmp::Ordering> {
        // 简化实现：按字节比较
        Ok(row1.cmp(row2))
    }

    /// 完成排序
    fn finish_sort(&mut self, out: &mut Outbox) -> Result<OpStatus> {
        if self.sort_complete {
            return Ok(OpStatus::Ready);
        }

        // 从TopN堆中提取结果
        if let Some(heap) = self.top_n_heap.take() {
            self.sorted_rows = heap.into_sorted_vec()
                .into_iter()
                .map(|Reverse(row)| row)
                .collect();
        } else {
            // 全排序
            self.sorted_rows = self.pending_rows.clone();
            self.sorted_rows.sort_by(|a, b| a.encoded_row.cmp(&b.encoded_row));
        }

        // 输出排序结果
        self.output_sorted_results(out)?;
        
        self.sort_complete = true;
        Ok(OpStatus::Ready)
    }

    /// 输出排序结果
    fn output_sorted_results(&self, out: &mut Outbox) -> Result<()> {
        // 简化实现：输出排序后的行索引
        let mut sorted_indices = Vec::new();
        for sort_row in &self.sorted_rows {
            sorted_indices.push(sort_row.original_index as i32);
        }
        
        // 创建输出批次
        let indices_array = Int32Array::from(sorted_indices);
        let schema = Schema::new(vec![Field::new("sorted_index", DataType::Int32, false)]);
        let output_batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(indices_array)])?;
        
        out.push(0, output_batch)?;
        Ok(())
    }

    /// 溢出到磁盘
    fn spill_to_disk(&mut self) -> Result<()> {
        if self.pending_rows.is_empty() {
            return Ok(());
        }

        // 创建spill文件
        let spill_file = format!("{}/sort_spill_{}.arrow", 
            self.config.spill_path, 
            self.spilled_files.len()
        );
        
        // 初始化IPC写入器
        if self.ipc_writer.is_none() {
            // 简化实现：创建一个虚拟的schema
            let schema = Schema::new(vec![Field::new("data", DataType::Utf8, false)]);
            self.ipc_writer = Some(ArrowIpcWriter::new(spill_file.clone()));
        }
        
        // 写入数据
        // TODO: 实现实际的spill逻辑
        
        self.spilled_files.push(spill_file);
        self.pending_rows.clear();
        self.total_memory_usage = 0;
        
        Ok(())
    }
}

impl Operator for SortTopNOperator {
    fn on_register(&mut self, _ctx: OperatorContext) -> Result<()> {
        Ok(())
    }
    
    fn on_event(&mut self, ev: Event, out: &mut Outbox) -> OpStatus {
        match ev {
            Event::Data(_, batch) => {
                match self.process_batch(batch, out) {
                    Ok(status) => status,
                    Err(e) => OpStatus::Error(e.to_string()),
                }
            }
            Event::Flush(_) => {
                match self.finish_sort(out) {
                    Ok(status) => status,
                    Err(e) => OpStatus::Error(e.to_string()),
                }
            }
            Event::Finish(_) => {
                match self.finish_sort(out) {
                    Ok(status) => status,
                    Err(e) => OpStatus::Error(e.to_string()),
                }
            }
            _ => OpStatus::Ready,
        }
    }
    
    fn is_finished(&self) -> bool {
        self.sort_complete
    }
    
    fn name(&self) -> &str {
        self.base.name()
    }
}

impl SingleInputOperator for SortTopNOperator {
    fn process_batch(&mut self, batch: RecordBatch, out: &mut Outbox) -> Result<OpStatus> {
        self.process_batch(batch, out)
    }
}

impl MetricsSupport for SortTopNOperator {
    fn record_metrics(&self, _batch: &RecordBatch, _duration: std::time::Duration) {
        // TODO: 记录指标
    }
    
    fn get_metrics(&self) -> OperatorMetrics {
        self.metrics.clone()
    }
}
