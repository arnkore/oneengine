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

//! 向量化执行器
//! 
//! 提供高效的向量化表达式执行

use crate::expression::ast::Expression;
use anyhow::Result;
use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use super::expression_executor_impl::ExpressionExecutorImpl;

/// 向量化执行器实现
pub struct VectorizedExecutorImpl;

impl VectorizedExecutorImpl {
    /// 创建新的向量化执行器
    pub fn new() -> Self {
        Self
    }

    /// 执行表达式
    pub fn execute(&self, expression: &Expression, batch: &RecordBatch) -> Result<ArrayRef> {
        ExpressionExecutorImpl::execute(expression, batch)
    }
}
