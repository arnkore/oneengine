//! Local Shuffle算子实现
//! 
//! 实现单机多分区重分布

use crate::columnar::{
    batch::{Batch, BatchSchema},
    column::{Column, AnyArray},
    types::{DataType, Bitmap},
};
use crate::execution::{
    context::ExecContext,
    operator::Operator,
    driver::DriverYield,
};
use anyhow::Result;
use hashbrown::HashMap;

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
    /// 是否已完成处理
    finished: bool,
}

impl LocalShuffleOperator {
    /// 创建新的Local Shuffle算子
    pub fn new(config: LocalShuffleConfig) -> Self {
        Self {
            partitions: vec![Vec::new(); config.num_partitions],
            current_partition: 0,
            finished: false,
            config,
        }
    }
}

impl Operator for LocalShuffleOperator {
    fn prepare(&mut self, _ctx: &ExecContext) -> Result<()> {
        Ok(())
    }

    fn poll_next(&mut self, _cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<Option<Batch>>> {
        if self.finished {
            std::task::Poll::Ready(Ok(None))
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

// LocalShuffleOperator不需要实现Driver trait，它只实现Operator trait
