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
pub mod cache;
pub mod functions;

use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use anyhow::Result;

/// 表达式引擎配置
#[derive(Debug, Clone)]
pub struct ExpressionEngineConfig {
    /// 批处理大小
    pub batch_size: usize,
    /// 是否启用表达式缓存
    pub enable_cache: bool,
    /// 缓存最大条目数
    pub cache_max_entries: usize,
    /// 缓存最大内存使用量（字节）
    pub cache_max_memory: usize,
}

impl Default for ExpressionEngineConfig {
    fn default() -> Self {
        Self {
            batch_size: 8192,
            enable_cache: true,
            cache_max_entries: 1000,
            cache_max_memory: 1024 * 1024 * 1024, // 1GB
        }
    }
}

/// 向量化表达式引擎
pub struct VectorizedExpressionEngine {
    config: ExpressionEngineConfig,
    executor: executor::VectorizedExecutor,
    cache: Option<cache::ExpressionCache>,
    function_engine: functions::FunctionEvaluatorEngine,
}

impl VectorizedExpressionEngine {
    /// 创建新的表达式引擎
    pub fn new(config: ExpressionEngineConfig) -> Result<Self> {
        let executor = executor::VectorizedExecutor::new(config.clone())?;
        let cache = if config.enable_cache {
            Some(cache::ExpressionCache::new(config.cache_max_entries))
        } else {
            None
        };
        let function_engine = functions::FunctionEvaluatorEngine::new();

        Ok(Self {
            config,
            executor,
            cache,
            function_engine,
        })
    }

    /// 执行表达式
    pub fn execute(&mut self, expression: &ast::Expression, batch: &RecordBatch) -> Result<ArrayRef> {
        // 如果启用缓存，先尝试从缓存获取
        if let Some(cache) = &mut self.cache {
            if let Some(cached_result) = cache.get(expression, batch) {
                return Ok(cached_result);
            }
        }

        // 执行表达式
        let result = self.executor.execute_interpreted(expression, batch)?;

        // 如果启用缓存，将结果存入缓存
        if let Some(cache) = &mut self.cache {
            cache.put(expression, batch, result.clone());
        }

        Ok(result)
    }

    /// 执行函数
    pub fn execute_function(&mut self, name: &str, args: &[ArrayRef], batch: &RecordBatch) -> Result<ArrayRef> {
        let context = functions::FunctionContext {
            batch: Arc::new(batch.clone()),
            args: args.to_vec(),
            stats: functions::FunctionStats::default(),
        };
        
        let result = self.function_engine.evaluate(name, &context)?;
        Ok(result.result)
    }

    /// 列出所有可用函数
    pub fn list_functions(&self) -> Vec<&str> {
        self.function_engine.list_functions()
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
        let cache_stats = self.cache.as_ref().map(|c| c.get_stats()).unwrap_or_default();
        ExpressionEngineStats {
            total_executions: self.executor.get_execution_count(),
            cache_hits: cache_stats.hit_count,
            cache_misses: cache_stats.miss_count,
            cache_hit_rate: cache_stats.hit_rate,
            cache_size: cache_stats.size,
        }
    }
}

/// 表达式引擎统计信息
#[derive(Debug, Clone)]
pub struct ExpressionEngineStats {
    pub total_executions: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub cache_hit_rate: f64,
    pub cache_size: usize,
}

impl Default for ExpressionEngineStats {
    fn default() -> Self {
        Self {
            total_executions: 0,
            cache_hits: 0,
            cache_misses: 0,
            cache_hit_rate: 0.0,
            cache_size: 0,
        }
    }
}
