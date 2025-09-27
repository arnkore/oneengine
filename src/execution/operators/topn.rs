//! Top-N算子实现
//! 
//! 实现部分排序和归并的Top-N算子

use crate::columnar::{
    batch::{Batch, BatchSchema, Field},
    column::{Column, AnyArray},
    types::{DataType, Bitmap},
};
use crate::execution::{
    context::ExecContext,
    operator::Operator,
    driver::DriverYield,
};
use anyhow::Result;
use std::collections::BinaryHeap;
use std::cmp::Reverse;
use smallvec::SmallVec;

/// Top-N算子配置
#[derive(Debug, Clone)]
pub struct TopNConfig {
    /// 排序列索引
    pub sort_cols: Vec<usize>,
    /// 是否升序
    pub ascending: Vec<bool>,
    /// 限制数量
    pub limit: usize,
    /// 输入schema
    pub input_schema: BatchSchema,
}

/// Top-N算子
pub struct TopNOperator {
    config: TopNConfig,
    /// 排序堆
    heap: BinaryHeap<Reverse<SortKey>>,
    /// 是否已完成处理
    finished: bool,
    /// 输入数据缓存
    input_batches: Vec<Batch>,
    /// 当前处理的批次索引
    current_batch_idx: usize,
    /// 当前批次中的行索引
    current_row_idx: usize,
    /// 输出批次
    output_batches: Vec<Batch>,
    /// 当前输出批次索引
    output_batch_idx: usize,
}

/// 排序键
#[derive(Debug, Clone, PartialEq, PartialOrd)]
struct SortKey {
    values: Vec<f64>,
}

impl Eq for SortKey {}

impl Ord for SortKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // 使用partial_cmp来处理f64的比较
        for (a, b) in self.values.iter().zip(other.values.iter()) {
            match a.partial_cmp(b) {
                Some(std::cmp::Ordering::Equal) => continue,
                Some(ordering) => return ordering,
                None => return std::cmp::Ordering::Equal, // NaN情况
            }
        }
        std::cmp::Ordering::Equal
    }
}

impl TopNOperator {
    /// 创建新的Top-N算子
    pub fn new(config: TopNConfig) -> Self {
        Self {
            heap: BinaryHeap::with_capacity(config.limit),
            finished: false,
            input_batches: Vec::new(),
            current_batch_idx: 0,
            current_row_idx: 0,
            output_batches: Vec::new(),
            output_batch_idx: 0,
            config,
        }
    }

    /// 处理输入批次
    pub fn process_input(&mut self, batch: &Batch) -> Result<()> {
        self.input_batches.push(batch.clone());
        Ok(())
    }

    /// 完成排序并生成输出
    pub fn finish_sorting(&mut self) -> Result<()> {
        // 处理所有输入批次
        let batches = self.input_batches.clone();
        for batch in &batches {
            self.process_batch(batch)?;
        }

        // 从堆中提取Top-N结果
        self.generate_output()?;
        self.finished = true;
        Ok(())
    }

    /// 处理单个批次
    fn process_batch(&mut self, batch: &Batch) -> Result<()> {
        for row in 0..batch.len() {
            let sort_key = self.extract_sort_key(batch, row)?;
            
            if self.heap.len() < self.config.limit {
                // 堆未满，直接插入
                self.heap.push(Reverse(sort_key));
            } else {
                // 堆已满，比较并可能替换
                if let Some(Reverse(peek_key)) = self.heap.peek() {
                    if sort_key < *peek_key {
                        self.heap.pop();
                        self.heap.push(Reverse(sort_key));
                    }
                }
            }
        }
        Ok(())
    }

    /// 提取排序键
    fn extract_sort_key(&self, batch: &Batch, row: usize) -> Result<SortKey> {
        let mut values = Vec::with_capacity(self.config.sort_cols.len());
        
        for &col_idx in &self.config.sort_cols {
            if let Some(column) = batch.columns.get(col_idx) {
                let value = match &column.data {
                    AnyArray::Int32(data) => {
                        if let Some(bitmap) = &column.nulls {
                            if !bitmap.get(row) {
                                f64::NAN // NULL值
                            } else {
                                data[row] as f64
                            }
                        } else {
                            data[row] as f64
                        }
                    }
                    AnyArray::Int64(data) => {
                        if let Some(bitmap) = &column.nulls {
                            if !bitmap.get(row) {
                                f64::NAN
                            } else {
                                data[row] as f64
                            }
                        } else {
                            data[row] as f64
                        }
                    }
                    AnyArray::Float64(data) => {
                        if let Some(bitmap) = &column.nulls {
                            if !bitmap.get(row) {
                                f64::NAN
                            } else {
                                data[row]
                            }
                        } else {
                            data[row]
                        }
                    }
                    _ => f64::NAN, // 不支持的类型
                };
                values.push(value);
            }
        }
        
        Ok(SortKey { values })
    }

    /// 生成输出批次
    fn generate_output(&mut self) -> Result<()> {
        // 将堆转换为Vec并排序
        let mut sorted_items: Vec<_> = self.heap.drain().map(|Reverse(key)| key).collect();
        sorted_items.sort();

        // 创建输出列
        let mut output_columns = Vec::new();
        
        for col_idx in 0..self.config.input_schema.fields.len() {
            let field = &self.config.input_schema.fields[col_idx];
            let column = Column::new(field.data_type.clone(), sorted_items.len());
            output_columns.push(column);
        }
        
        // 创建输出批次
        let output_batch = Batch::from_columns(output_columns, self.config.input_schema.clone())?;
        self.output_batches.push(output_batch);
        
        Ok(())
    }
}

impl Operator for TopNOperator {
    fn prepare(&mut self, _ctx: &ExecContext) -> Result<()> {
        Ok(())
    }

    fn poll_next(&mut self, _cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<Option<Batch>>> {
        if self.finished {
            if self.output_batch_idx < self.output_batches.len() {
                let batch = self.output_batches[self.output_batch_idx].clone();
                self.output_batch_idx += 1;
                std::task::Poll::Ready(Ok(Some(batch)))
            } else {
                std::task::Poll::Ready(Ok(None))
            }
        } else {
            std::task::Poll::Pending
        }
    }

    fn close(&mut self) -> Result<()> {
        Ok(())
    }

    fn output_schema(&self) -> &BatchSchema {
        &self.config.input_schema
    }
}

// TopNOperator不需要实现Driver trait，它只实现Operator trait
