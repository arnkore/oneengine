//! Local Shuffle算子实现
//! 
//! 实现单机多分区重分布

use crate::columnar::{
    batch::{Batch, BatchSchema, Field},
    column::{Column, AnyArray},
    types::{DataType, Bitmap},
};
use crate::execution::{
    context::ExecContext,
    operator::Operator,
};
use anyhow::Result;
use hashbrown::HashMap;
use smallvec::SmallVec;

/// Local Shuffle配置
#[derive(Debug, Clone)]
pub struct LocalShuffleConfig {
    /// 分区列索引
    pub partition_cols: Vec<usize>,
    /// 分区数量
    pub num_partitions: usize,
    /// 输入schema
    pub input_schema: BatchSchema,
}

/// Local Shuffle算子
pub struct LocalShuffleOperator {
    config: LocalShuffleConfig,
    /// 分区数据
    partitions: Vec<Vec<Batch>>,
    /// 当前分区索引
    current_partition: usize,
    /// 当前分区内的批次索引
    current_batch_in_partition: usize,
    /// 是否已完成处理
    finished: bool,
    /// 输入数据缓存
    input_batches: Vec<Batch>,
    /// 是否已完成分区
    partitioning_done: bool,
}

impl LocalShuffleOperator {
    /// 创建新的Local Shuffle算子
    pub fn new(config: LocalShuffleConfig) -> Self {
        Self {
            partitions: vec![Vec::new(); config.num_partitions],
            current_partition: 0,
            current_batch_in_partition: 0,
            finished: false,
            input_batches: Vec::new(),
            partitioning_done: false,
            config,
        }
    }

    /// 处理输入批次
    pub fn process_input(&mut self, batch: &Batch) -> Result<()> {
        self.input_batches.push(batch.clone());
        Ok(())
    }

    /// 完成分区处理
    pub fn finish_partitioning(&mut self) -> Result<()> {
        // 处理所有输入批次
        let batches = self.input_batches.clone();
        for batch in &batches {
            self.partition_batch(batch)?;
        }
        self.partitioning_done = true;
        Ok(())
    }

    /// 分区单个批次
    fn partition_batch(&mut self, batch: &Batch) -> Result<()> {
        let mut partition_rows: Vec<Vec<usize>> = vec![Vec::new(); self.config.num_partitions];
        
        // 为每一行计算分区
        for row in 0..batch.len() {
            let partition_id = self.compute_partition(batch, row)?;
            partition_rows[partition_id].push(row);
        }

        // 为每个分区创建批次
        for (partition_id, rows) in partition_rows.into_iter().enumerate() {
            if !rows.is_empty() {
                let partition_batch = self.create_partition_batch(batch, &rows)?;
                self.partitions[partition_id].push(partition_batch);
            }
        }

        Ok(())
    }

    /// 计算行的分区ID
    fn compute_partition(&self, batch: &Batch, row: usize) -> Result<usize> {
        let mut hash = 0u64;
        
        for &col_idx in &self.config.partition_cols {
            if let Some(column) = batch.columns.get(col_idx) {
                let value_hash = match &column.data {
                    AnyArray::Int32(data) => {
                        if let Some(bitmap) = &column.nulls {
                            if !bitmap.get(row) {
                                0 // NULL值
                            } else {
                                data[row] as u64
                            }
                        } else {
                            data[row] as u64
                        }
                    }
                    AnyArray::Int64(data) => {
                        if let Some(bitmap) = &column.nulls {
                            if !bitmap.get(row) {
                                0
                            } else {
                                data[row] as u64
                            }
                        } else {
                            data[row] as u64
                        }
                    }
                    AnyArray::Float64(data) => {
                        if let Some(bitmap) = &column.nulls {
                            if !bitmap.get(row) {
                                0
                            } else {
                                data[row].to_bits()
                            }
                        } else {
                            data[row].to_bits()
                        }
                    }
                    AnyArray::Utf8(data) => {
                        if let Some(bitmap) = &column.nulls {
                            if !bitmap.get(row) {
                                0
                            } else {
                                // 简单的字符串哈希
                                data[row].as_bytes().iter().fold(0u64, |acc, &b| {
                                    acc.wrapping_mul(31).wrapping_add(b as u64)
                                })
                            }
                        } else {
                            data[row].as_bytes().iter().fold(0u64, |acc, &b| {
                                acc.wrapping_mul(31).wrapping_add(b as u64)
                            })
                        }
                    }
                    _ => 0, // 不支持的类型
                };
                hash = hash.wrapping_mul(31).wrapping_add(value_hash);
            }
        }
        
        Ok((hash as usize) % self.config.num_partitions)
    }

    /// 创建分区批次
    fn create_partition_batch(&self, original_batch: &Batch, row_indices: &[usize]) -> Result<Batch> {
        let mut partition_columns = Vec::new();
        
        for col_idx in 0..original_batch.columns.len() {
            let original_column = &original_batch.columns[col_idx];
            let mut partition_data = match &original_column.data {
                AnyArray::Int32(data) => {
                    let mut partition_data = Vec::with_capacity(row_indices.len());
                    for &row_idx in row_indices {
                        partition_data.push(data[row_idx]);
                    }
                    AnyArray::Int32(partition_data)
                }
                AnyArray::Int64(data) => {
                    let mut partition_data = Vec::with_capacity(row_indices.len());
                    for &row_idx in row_indices {
                        partition_data.push(data[row_idx]);
                    }
                    AnyArray::Int64(partition_data)
                }
                AnyArray::Float64(data) => {
                    let mut partition_data = Vec::with_capacity(row_indices.len());
                    for &row_idx in row_indices {
                        partition_data.push(data[row_idx]);
                    }
                    AnyArray::Float64(partition_data)
                }
                AnyArray::Utf8(data) => {
                    let mut partition_data = Vec::with_capacity(row_indices.len());
                    for &row_idx in row_indices {
                        partition_data.push(data[row_idx].clone());
                    }
                    AnyArray::Utf8(partition_data)
                }
                AnyArray::Boolean(data) => {
                    let mut partition_data = Vec::with_capacity(row_indices.len());
                    for &row_idx in row_indices {
                        partition_data.push(data[row_idx]);
                    }
                    AnyArray::Boolean(partition_data)
                }
                _ => {
                    // 对于不支持的类型，创建一个空的批次
                    AnyArray::Boolean(Vec::new())
                }
            };
            
            let mut partition_nulls = if let Some(original_nulls) = &original_column.nulls {
                let mut nulls = Bitmap::new(row_indices.len());
                for (i, &row_idx) in row_indices.iter().enumerate() {
                    nulls.set(i, original_nulls.get(row_idx));
                }
                Some(nulls)
            } else {
                None
            };
            
            let partition_column = Column::new(original_column.data_type.clone(), row_indices.len());
            partition_columns.push(partition_column);
        }
        
        let partition_batch = Batch::from_columns(partition_columns, original_batch.schema.clone())?;
        Ok(partition_batch)
    }
}

impl Operator for LocalShuffleOperator {
    fn prepare(&mut self, _ctx: &ExecContext) -> Result<()> {
        Ok(())
    }

    fn poll_next(&mut self, _cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<Option<Batch>>> {
        if !self.partitioning_done {
            return std::task::Poll::Pending;
        }

        if self.finished {
            return std::task::Poll::Ready(Ok(None));
        }

        // 查找下一个有数据的分区
        while self.current_partition < self.config.num_partitions {
            if self.current_batch_in_partition < self.partitions[self.current_partition].len() {
                let batch = self.partitions[self.current_partition][self.current_batch_in_partition].clone();
                self.current_batch_in_partition += 1;
                return std::task::Poll::Ready(Ok(Some(batch)));
            } else {
                self.current_partition += 1;
                self.current_batch_in_partition = 0;
            }
        }

        // 所有分区都已处理完毕
        self.finished = true;
        std::task::Poll::Ready(Ok(None))
    }

    fn close(&mut self) -> Result<()> {
        Ok(())
    }

    fn output_schema(&self) -> &BatchSchema {
        &self.config.input_schema
    }
}

// LocalShuffleOperator不需要实现Driver trait，它只实现Operator trait
