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

//! 表达式执行器模块
//! 
//! 提供向量化表达式执行功能

pub mod vectorized_executor;
pub mod expression_executor_impl;

use crate::expression::ast::Expression;
use crate::expression::ExpressionEngineConfig;
use anyhow::Result;
use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

/// 向量化执行器
pub struct VectorizedExecutor {
    config: ExpressionEngineConfig,
    execution_count: std::sync::atomic::AtomicU64,
}

impl VectorizedExecutor {
    /// 创建新的向量化执行器
    pub fn new(config: ExpressionEngineConfig) -> Result<Self> {
        Ok(Self {
            config,
            execution_count: std::sync::atomic::AtomicU64::new(0),
        })
    }

    /// 执行表达式
    pub fn execute_interpreted(&self, expression: &Expression, batch: &RecordBatch) -> Result<ArrayRef> {
        self.execution_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        use crate::expression::executor::expression_executor_impl::ExpressionExecutorImpl;
        ExpressionExecutorImpl::execute(expression, batch)
    }

    /// 获取执行次数
    pub fn get_execution_count(&self) -> u64 {
        self.execution_count.load(std::sync::atomic::Ordering::Relaxed)
    }
}
