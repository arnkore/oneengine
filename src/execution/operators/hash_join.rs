//! Hash Join算子实现
//! 
//! 实现Broadcast/Shuffle模式的Hash Join算子

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

/// Hash Join算子配置
#[derive(Debug, Clone)]
pub struct HashJoinConfig {
    /// 左表连接列索引
    pub left_join_cols: Vec<usize>,
    /// 右表连接列索引
    pub right_join_cols: Vec<usize>,
    /// 连接类型
    pub join_type: JoinType,
    /// 左表schema
    pub left_schema: BatchSchema,
    /// 右表schema
    pub right_schema: BatchSchema,
}

/// 连接类型
#[derive(Debug, Clone, PartialEq)]
pub enum JoinType {
    /// 内连接
    Inner,
    /// 左外连接
    LeftOuter,
    /// 右外连接
    RightOuter,
    /// 全外连接
    FullOuter,
}

/// Hash Join算子
pub struct HashJoinOperator {
    config: HashJoinConfig,
    /// 构建表（右表）的哈希表
    build_table: HashMap<Vec<u64>, Vec<usize>>,
    /// 是否已完成构建
    build_done: bool,
}

impl HashJoinOperator {
    /// 创建新的Hash Join算子
    pub fn new(config: HashJoinConfig) -> Self {
        Self {
            build_table: HashMap::new(),
            build_done: false,
            config,
        }
    }
}

impl Operator for HashJoinOperator {
    fn prepare(&mut self, _ctx: &ExecContext) -> Result<()> {
        Ok(())
    }

    fn poll_next(&mut self, _cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<Option<Batch>>> {
        std::task::Poll::Pending
    }

    fn close(&mut self) -> Result<()> {
        Ok(())
    }

    fn output_schema(&self) -> &BatchSchema {
        &self.config.left_schema
    }
}

// HashJoinOperator不需要实现Driver trait，它只实现Operator trait
