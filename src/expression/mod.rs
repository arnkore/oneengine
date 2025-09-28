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

//! 简化的向量化表达式引擎
//! 
//! 提供高效的向量化表达式执行，专注于：
//! - 向量化计算
//! - 类型特化
//! - 简单高效的执行

pub mod ast;
pub mod executor;

use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use anyhow::Result;

/// 表达式引擎配置
#[derive(Debug, Clone)]
pub struct ExpressionEngineConfig {
    /// 批处理大小
    pub batch_size: usize,
}

impl Default for ExpressionEngineConfig {
    fn default() -> Self {
        Self {
            batch_size: 8192,
        }
    }
}

/// 向量化表达式引擎
pub struct VectorizedExpressionEngine {
    config: ExpressionEngineConfig,
    executor: executor::VectorizedExecutor,
}

impl VectorizedExpressionEngine {
    /// 创建新的表达式引擎
    pub fn new(config: ExpressionEngineConfig) -> Result<Self> {
        let executor = executor::VectorizedExecutor::new(config.clone())?;

        Ok(Self {
            config,
            executor,
        })
    }

    /// 执行表达式
    pub fn execute(&mut self, expression: &ast::Expression, batch: &RecordBatch) -> Result<ArrayRef> {
        self.executor.execute_interpreted(expression, batch)
    }

    /// 批量执行表达式
    pub fn execute_batch(&mut self, expression: &ast::Expression, batches: &[RecordBatch]) -> Result<Vec<ArrayRef>> {
        let mut results = Vec::with_capacity(batches.len());
        
        for batch in batches {
            let result = self.execute(expression, batch)?;
            results.push(result);
        }
        
        Ok(results)
    }

    /// 获取引擎统计信息
    pub fn get_stats(&self) -> ExpressionEngineStats {
        ExpressionEngineStats {
            total_executions: self.executor.get_execution_count(),
        }
    }
}

/// 表达式引擎统计信息
#[derive(Debug, Clone)]
pub struct ExpressionEngineStats {
    pub total_executions: u64,
}
