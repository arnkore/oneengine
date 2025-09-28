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

//! 逻辑函数实现
//! 
//! 提供基本的逻辑运算函数

use super::{FunctionEvaluator, FunctionContext, FunctionResult, FunctionArity, FunctionStats};
use arrow::array::*;
use arrow::datatypes::*;
use arrow::compute::kernels::boolean;
use anyhow::Result;
use std::sync::Arc;

/// 逻辑与函数
pub struct AndFunction;

impl AndFunction {
    pub fn new() -> Self {
        Self
    }
}

impl FunctionEvaluator for AndFunction {
    fn name(&self) -> &str {
        "and"
    }
    
    fn description(&self) -> &str {
        "Logical AND operation"
    }
    
    fn arity(&self) -> FunctionArity {
        FunctionArity::Fixed(2)
    }
    
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }
    
    fn evaluate(&self, context: &FunctionContext) -> Result<FunctionResult> {
        if context.args.len() != 2 {
            return Err(anyhow::anyhow!("And function requires exactly 2 arguments"));
        }
        
        let left = context.args[0].as_any().downcast_ref::<BooleanArray>()
            .ok_or_else(|| anyhow::anyhow!("Left operand must be boolean"))?;
        let right = context.args[1].as_any().downcast_ref::<BooleanArray>()
            .ok_or_else(|| anyhow::anyhow!("Right operand must be boolean"))?;
        
        let result = Arc::new(boolean::and(left, right)?) as ArrayRef;
        
        Ok(FunctionResult {
            result,
            stats: FunctionStats::default(),
        })
    }
}

/// 逻辑或函数
pub struct OrFunction;

impl OrFunction {
    pub fn new() -> Self {
        Self
    }
}

impl FunctionEvaluator for OrFunction {
    fn name(&self) -> &str {
        "or"
    }
    
    fn description(&self) -> &str {
        "Logical OR operation"
    }
    
    fn arity(&self) -> FunctionArity {
        FunctionArity::Fixed(2)
    }
    
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }
    
    fn evaluate(&self, context: &FunctionContext) -> Result<FunctionResult> {
        if context.args.len() != 2 {
            return Err(anyhow::anyhow!("Or function requires exactly 2 arguments"));
        }
        
        let left = context.args[0].as_any().downcast_ref::<BooleanArray>()
            .ok_or_else(|| anyhow::anyhow!("Left operand must be boolean"))?;
        let right = context.args[1].as_any().downcast_ref::<BooleanArray>()
            .ok_or_else(|| anyhow::anyhow!("Right operand must be boolean"))?;
        
        let result = Arc::new(boolean::or(left, right)?) as ArrayRef;
        
        Ok(FunctionResult {
            result,
            stats: FunctionStats::default(),
        })
    }
}

/// 逻辑非函数
pub struct NotFunction;

impl NotFunction {
    pub fn new() -> Self {
        Self
    }
}

impl FunctionEvaluator for NotFunction {
    fn name(&self) -> &str {
        "not"
    }
    
    fn description(&self) -> &str {
        "Logical NOT operation"
    }
    
    fn arity(&self) -> FunctionArity {
        FunctionArity::Fixed(1)
    }
    
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }
    
    fn evaluate(&self, context: &FunctionContext) -> Result<FunctionResult> {
        if context.args.len() != 1 {
            return Err(anyhow::anyhow!("Not function requires exactly 1 argument"));
        }
        
        let operand = context.args[0].as_any().downcast_ref::<BooleanArray>()
            .ok_or_else(|| anyhow::anyhow!("Operand must be boolean"))?;
        
        let result = Arc::new(boolean::not(operand)?) as ArrayRef;
        
        Ok(FunctionResult {
            result,
            stats: FunctionStats::default(),
        })
    }
}
