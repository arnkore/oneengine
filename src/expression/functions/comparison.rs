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

//! 比较函数实现
//! 
//! 提供基本的比较运算函数

use super::{FunctionEvaluator, FunctionContext, FunctionResult, FunctionArity, FunctionStats};
use arrow::array::*;
use arrow::datatypes::*;
use arrow::compute::kernels::cmp;
use anyhow::Result;
use std::sync::Arc;

/// 等于函数
pub struct EqualFunction;

impl EqualFunction {
    pub fn new() -> Self {
        Self
    }
}

impl FunctionEvaluator for EqualFunction {
    fn name(&self) -> &str {
        "equal"
    }
    
    fn description(&self) -> &str {
        "Check if two values are equal"
    }
    
    fn arity(&self) -> FunctionArity {
        FunctionArity::Fixed(2)
    }
    
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }
    
    fn evaluate(&self, context: &FunctionContext) -> Result<FunctionResult> {
        if context.args.len() != 2 {
            return Err(anyhow::anyhow!("Equal function requires exactly 2 arguments"));
        }
        
        let left = &context.args[0];
        let right = &context.args[1];
        
        let result = Arc::new(cmp::eq(left, right)?) as ArrayRef;
        
        Ok(FunctionResult {
            result,
            stats: FunctionStats::default(),
        })
    }
}

/// 不等于函数
pub struct NotEqualFunction;

impl NotEqualFunction {
    pub fn new() -> Self {
        Self
    }
}

impl FunctionEvaluator for NotEqualFunction {
    fn name(&self) -> &str {
        "not_equal"
    }
    
    fn description(&self) -> &str {
        "Check if two values are not equal"
    }
    
    fn arity(&self) -> FunctionArity {
        FunctionArity::Fixed(2)
    }
    
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }
    
    fn evaluate(&self, context: &FunctionContext) -> Result<FunctionResult> {
        if context.args.len() != 2 {
            return Err(anyhow::anyhow!("Not equal function requires exactly 2 arguments"));
        }
        
        let left = &context.args[0];
        let right = &context.args[1];
        
        let result = Arc::new(cmp::neq(left, right)?) as ArrayRef;
        
        Ok(FunctionResult {
            result,
            stats: FunctionStats::default(),
        })
    }
}

/// 小于函数
pub struct LessThanFunction;

impl LessThanFunction {
    pub fn new() -> Self {
        Self
    }
}

impl FunctionEvaluator for LessThanFunction {
    fn name(&self) -> &str {
        "less_than"
    }
    
    fn description(&self) -> &str {
        "Check if left value is less than right value"
    }
    
    fn arity(&self) -> FunctionArity {
        FunctionArity::Fixed(2)
    }
    
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }
    
    fn evaluate(&self, context: &FunctionContext) -> Result<FunctionResult> {
        if context.args.len() != 2 {
            return Err(anyhow::anyhow!("Less than function requires exactly 2 arguments"));
        }
        
        let left = &context.args[0];
        let right = &context.args[1];
        
        let result = Arc::new(cmp::lt(left, right)?) as ArrayRef;
        
        Ok(FunctionResult {
            result,
            stats: FunctionStats::default(),
        })
    }
}

/// 小于等于函数
pub struct LessThanOrEqualFunction;

impl LessThanOrEqualFunction {
    pub fn new() -> Self {
        Self
    }
}

impl FunctionEvaluator for LessThanOrEqualFunction {
    fn name(&self) -> &str {
        "less_than_or_equal"
    }
    
    fn description(&self) -> &str {
        "Check if left value is less than or equal to right value"
    }
    
    fn arity(&self) -> FunctionArity {
        FunctionArity::Fixed(2)
    }
    
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }
    
    fn evaluate(&self, context: &FunctionContext) -> Result<FunctionResult> {
        if context.args.len() != 2 {
            return Err(anyhow::anyhow!("Less than or equal function requires exactly 2 arguments"));
        }
        
        let left = &context.args[0];
        let right = &context.args[1];
        
        let result = Arc::new(cmp::lt_eq(left, right)?) as ArrayRef;
        
        Ok(FunctionResult {
            result,
            stats: FunctionStats::default(),
        })
    }
}

/// 大于函数
pub struct GreaterThanFunction;

impl GreaterThanFunction {
    pub fn new() -> Self {
        Self
    }
}

impl FunctionEvaluator for GreaterThanFunction {
    fn name(&self) -> &str {
        "greater_than"
    }
    
    fn description(&self) -> &str {
        "Check if left value is greater than right value"
    }
    
    fn arity(&self) -> FunctionArity {
        FunctionArity::Fixed(2)
    }
    
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }
    
    fn evaluate(&self, context: &FunctionContext) -> Result<FunctionResult> {
        if context.args.len() != 2 {
            return Err(anyhow::anyhow!("Greater than function requires exactly 2 arguments"));
        }
        
        let left = &context.args[0];
        let right = &context.args[1];
        
        let result = Arc::new(cmp::gt(left, right)?) as ArrayRef;
        
        Ok(FunctionResult {
            result,
            stats: FunctionStats::default(),
        })
    }
}

/// 大于等于函数
pub struct GreaterThanOrEqualFunction;

impl GreaterThanOrEqualFunction {
    pub fn new() -> Self {
        Self
    }
}

impl FunctionEvaluator for GreaterThanOrEqualFunction {
    fn name(&self) -> &str {
        "greater_than_or_equal"
    }
    
    fn description(&self) -> &str {
        "Check if left value is greater than or equal to right value"
    }
    
    fn arity(&self) -> FunctionArity {
        FunctionArity::Fixed(2)
    }
    
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }
    
    fn evaluate(&self, context: &FunctionContext) -> Result<FunctionResult> {
        if context.args.len() != 2 {
            return Err(anyhow::anyhow!("Greater than or equal function requires exactly 2 arguments"));
        }
        
        let left = &context.args[0];
        let right = &context.args[1];
        
        let result = Arc::new(cmp::gt_eq(left, right)?) as ArrayRef;
        
        Ok(FunctionResult {
            result,
            stats: FunctionStats::default(),
        })
    }
}
