//! Top-N算子实现
//! 
//! 实现部分排序和归并的Top-N算子

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
use std::collections::BinaryHeap;
use std::cmp::Reverse;

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
            config,
        }
    }
}

impl Operator for TopNOperator {
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

// TopNOperator不需要实现Driver trait，它只实现Operator trait
