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

//! 表达式函数求值框架
//! 
//! 提供可扩展的函数求值框架，每个函数单独实现

pub mod arithmetic;
pub mod comparison;
pub mod logical;
pub mod math;
pub mod string;
pub mod date;
pub mod aggregate;

use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;
use anyhow::Result;
use std::sync::Arc;

/// 函数求值上下文
#[derive(Debug, Clone)]
pub struct FunctionContext {
    /// 输入批次
    pub batch: Arc<RecordBatch>,
    /// 函数参数
    pub args: Vec<ArrayRef>,
    /// 执行统计
    pub stats: FunctionStats,
}

/// 函数执行统计
#[derive(Debug, Clone, Default)]
pub struct FunctionStats {
    pub execution_count: u64,
    pub execution_time: std::time::Duration,
    pub memory_used: usize,
}

/// 函数求值结果
#[derive(Debug, Clone)]
pub struct FunctionResult {
    /// 计算结果
    pub result: ArrayRef,
    /// 执行统计
    pub stats: FunctionStats,
}

/// 函数求值trait
pub trait FunctionEvaluator: Send + Sync {
    /// 函数名称
    fn name(&self) -> &str;
    
    /// 函数描述
    fn description(&self) -> &str;
    
    /// 参数数量
    fn arity(&self) -> FunctionArity;
    
    /// 返回类型
    fn return_type(&self, arg_types: &[arrow::datatypes::DataType]) -> Result<arrow::datatypes::DataType>;
    
    /// 求值函数
    fn evaluate(&self, context: &FunctionContext) -> Result<FunctionResult>;
    
    /// 是否支持向量化
    fn supports_vectorization(&self) -> bool {
        true
    }
    
    /// 是否支持SIMD优化
    fn supports_simd(&self) -> bool {
        false
    }
}

/// 函数参数数量
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FunctionArity {
    /// 固定参数数量
    Fixed(usize),
    /// 可变参数数量（最小，最大）
    Variable(usize, Option<usize>),
}

/// 函数注册表
pub struct FunctionRegistry {
    functions: std::collections::HashMap<String, Box<dyn FunctionEvaluator>>,
}

impl FunctionRegistry {
    /// 创建新的函数注册表
    pub fn new() -> Self {
        Self {
            functions: std::collections::HashMap::new(),
        }
    }
    
    /// 注册函数
    pub fn register<F>(&mut self, function: F) 
    where 
        F: FunctionEvaluator + 'static,
    {
        self.functions.insert(function.name().to_string(), Box::new(function));
    }
    
    /// 获取函数
    pub fn get(&self, name: &str) -> Option<&dyn FunctionEvaluator> {
        self.functions.get(name).map(|f| f.as_ref())
    }
    
    /// 列出所有函数
    pub fn list_functions(&self) -> Vec<&str> {
        self.functions.keys().map(|k| k.as_str()).collect()
    }
    
    /// 获取函数数量
    pub fn function_count(&self) -> usize {
        self.functions.len()
    }
}

impl Default for FunctionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// 函数求值器
pub struct FunctionEvaluatorEngine {
    registry: FunctionRegistry,
    stats: FunctionStats,
}

impl FunctionEvaluatorEngine {
    /// 创建新的函数求值器
    pub fn new() -> Self {
        let mut registry = FunctionRegistry::new();
        
        // 注册算术函数
        registry.register(arithmetic::AddFunction::new());
        registry.register(arithmetic::SubtractFunction::new());
        registry.register(arithmetic::MultiplyFunction::new());
        registry.register(arithmetic::DivideFunction::new());
        registry.register(arithmetic::ModuloFunction::new());
        registry.register(arithmetic::PowerFunction::new());
        
        // 注册比较函数
        registry.register(comparison::EqualFunction::new());
        registry.register(comparison::NotEqualFunction::new());
        registry.register(comparison::LessThanFunction::new());
        registry.register(comparison::LessThanOrEqualFunction::new());
        registry.register(comparison::GreaterThanFunction::new());
        registry.register(comparison::GreaterThanOrEqualFunction::new());
        
        // 注册逻辑函数
        registry.register(logical::AndFunction::new());
        registry.register(logical::OrFunction::new());
        registry.register(logical::NotFunction::new());
        
        // 注册数学函数
        registry.register(math::AbsFunction::new());
        registry.register(math::SqrtFunction::new());
        registry.register(math::SinFunction::new());
        registry.register(math::CosFunction::new());
        registry.register(math::ExpFunction::new());
        registry.register(math::LnFunction::new());
        
        Self {
            registry,
            stats: FunctionStats::default(),
        }
    }
    
    /// 求值函数
    pub fn evaluate(&mut self, name: &str, context: &FunctionContext) -> Result<FunctionResult> {
        let function = self.registry.get(name)
            .ok_or_else(|| anyhow::anyhow!("Function not found: {}", name))?;
        
        let start_time = std::time::Instant::now();
        let result = function.evaluate(context)?;
        let execution_time = start_time.elapsed();
        
        // 更新统计信息
        self.stats.execution_count += 1;
        self.stats.execution_time += execution_time;
        
        Ok(result)
    }
    
    /// 获取统计信息
    pub fn get_stats(&self) -> &FunctionStats {
        &self.stats
    }
    
    /// 列出所有函数
    pub fn list_functions(&self) -> Vec<&str> {
        self.registry.list_functions()
    }
}

impl Default for FunctionEvaluatorEngine {
    fn default() -> Self {
        Self::new()
    }
}
