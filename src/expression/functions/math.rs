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

//! 数学函数实现
//! 
//! 提供基本的数学运算函数

use super::{FunctionEvaluator, FunctionContext, FunctionResult, FunctionArity, FunctionStats};
use arrow::array::*;
use arrow::datatypes::*;
use anyhow::Result;
use std::sync::Arc;

/// 绝对值函数
pub struct AbsFunction;

impl AbsFunction {
    pub fn new() -> Self {
        Self
    }
}

impl FunctionEvaluator for AbsFunction {
    fn name(&self) -> &str {
        "abs"
    }
    
    fn description(&self) -> &str {
        "Absolute value"
    }
    
    fn arity(&self) -> FunctionArity {
        FunctionArity::Fixed(1)
    }
    
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return Err(anyhow::anyhow!("Abs function requires exactly 1 argument"));
        }
        
        Ok(arg_types[0].clone())
    }
    
    fn evaluate(&self, context: &FunctionContext) -> Result<FunctionResult> {
        if context.args.len() != 1 {
            return Err(anyhow::anyhow!("Abs function requires exactly 1 argument"));
        }
        
        let arg = &context.args[0];
        
        let result = match arg.data_type() {
            DataType::Int32 => {
                let array = arg.as_any().downcast_ref::<Int32Array>().unwrap();
                let abs_values: Vec<Option<i32>> = array.iter()
                    .map(|v| v.map(|x| x.abs()))
                    .collect();
                Arc::new(Int32Array::from(abs_values)) as ArrayRef
            }
            DataType::Int64 => {
                let array = arg.as_any().downcast_ref::<Int64Array>().unwrap();
                let abs_values: Vec<Option<i64>> = array.iter()
                    .map(|v| v.map(|x| x.abs()))
                    .collect();
                Arc::new(Int64Array::from(abs_values)) as ArrayRef
            }
            DataType::Float32 => {
                let array = arg.as_any().downcast_ref::<Float32Array>().unwrap();
                let abs_values: Vec<Option<f32>> = array.iter()
                    .map(|v| v.map(|x| x.abs()))
                    .collect();
                Arc::new(Float32Array::from(abs_values)) as ArrayRef
            }
            DataType::Float64 => {
                let array = arg.as_any().downcast_ref::<Float64Array>().unwrap();
                let abs_values: Vec<Option<f64>> = array.iter()
                    .map(|v| v.map(|x| x.abs()))
                    .collect();
                Arc::new(Float64Array::from(abs_values)) as ArrayRef
            }
            _ => return Err(anyhow::anyhow!("Unsupported abs operation for type: {:?}", arg.data_type())),
        };
        
        Ok(FunctionResult {
            result,
            stats: FunctionStats::default(),
        })
    }
}

/// 平方根函数
pub struct SqrtFunction;

impl SqrtFunction {
    pub fn new() -> Self {
        Self
    }
}

impl FunctionEvaluator for SqrtFunction {
    fn name(&self) -> &str {
        "sqrt"
    }
    
    fn description(&self) -> &str {
        "Square root"
    }
    
    fn arity(&self) -> FunctionArity {
        FunctionArity::Fixed(1)
    }
    
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return Err(anyhow::anyhow!("Sqrt function requires exactly 1 argument"));
        }
        
        match arg_types[0] {
            DataType::Float32 => Ok(DataType::Float32),
            DataType::Float64 => Ok(DataType::Float64),
            _ => Ok(DataType::Float64), // 默认返回Float64
        }
    }
    
    fn evaluate(&self, context: &FunctionContext) -> Result<FunctionResult> {
        if context.args.len() != 1 {
            return Err(anyhow::anyhow!("Sqrt function requires exactly 1 argument"));
        }
        
        let arg = &context.args[0];
        
        let result = match arg.data_type() {
            DataType::Float32 => {
                let array = arg.as_any().downcast_ref::<Float32Array>().unwrap();
                let sqrt_values: Vec<Option<f32>> = array.iter()
                    .map(|v| v.map(|x| x.sqrt()))
                    .collect();
                Arc::new(Float32Array::from(sqrt_values)) as ArrayRef
            }
            DataType::Float64 => {
                let array = arg.as_any().downcast_ref::<Float64Array>().unwrap();
                let sqrt_values: Vec<Option<f64>> = array.iter()
                    .map(|v| v.map(|x| x.sqrt()))
                    .collect();
                Arc::new(Float64Array::from(sqrt_values)) as ArrayRef
            }
            _ => return Err(anyhow::anyhow!("Unsupported sqrt operation for type: {:?}", arg.data_type())),
        };
        
        Ok(FunctionResult {
            result,
            stats: FunctionStats::default(),
        })
    }
}

/// 正弦函数
pub struct SinFunction;

impl SinFunction {
    pub fn new() -> Self {
        Self
    }
}

impl FunctionEvaluator for SinFunction {
    fn name(&self) -> &str {
        "sin"
    }
    
    fn description(&self) -> &str {
        "Sine function"
    }
    
    fn arity(&self) -> FunctionArity {
        FunctionArity::Fixed(1)
    }
    
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return Err(anyhow::anyhow!("Sin function requires exactly 1 argument"));
        }
        
        match arg_types[0] {
            DataType::Float32 => Ok(DataType::Float32),
            DataType::Float64 => Ok(DataType::Float64),
            _ => Ok(DataType::Float64),
        }
    }
    
    fn evaluate(&self, context: &FunctionContext) -> Result<FunctionResult> {
        if context.args.len() != 1 {
            return Err(anyhow::anyhow!("Sin function requires exactly 1 argument"));
        }
        
        let arg = &context.args[0];
        
        let result = match arg.data_type() {
            DataType::Float32 => {
                let array = arg.as_any().downcast_ref::<Float32Array>().unwrap();
                let sin_values: Vec<Option<f32>> = array.iter()
                    .map(|v| v.map(|x| x.sin()))
                    .collect();
                Arc::new(Float32Array::from(sin_values)) as ArrayRef
            }
            DataType::Float64 => {
                let array = arg.as_any().downcast_ref::<Float64Array>().unwrap();
                let sin_values: Vec<Option<f64>> = array.iter()
                    .map(|v| v.map(|x| x.sin()))
                    .collect();
                Arc::new(Float64Array::from(sin_values)) as ArrayRef
            }
            _ => return Err(anyhow::anyhow!("Unsupported sin operation for type: {:?}", arg.data_type())),
        };
        
        Ok(FunctionResult {
            result,
            stats: FunctionStats::default(),
        })
    }
}

/// 余弦函数
pub struct CosFunction;

impl CosFunction {
    pub fn new() -> Self {
        Self
    }
}

impl FunctionEvaluator for CosFunction {
    fn name(&self) -> &str {
        "cos"
    }
    
    fn description(&self) -> &str {
        "Cosine function"
    }
    
    fn arity(&self) -> FunctionArity {
        FunctionArity::Fixed(1)
    }
    
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return Err(anyhow::anyhow!("Cos function requires exactly 1 argument"));
        }
        
        match arg_types[0] {
            DataType::Float32 => Ok(DataType::Float32),
            DataType::Float64 => Ok(DataType::Float64),
            _ => Ok(DataType::Float64),
        }
    }
    
    fn evaluate(&self, context: &FunctionContext) -> Result<FunctionResult> {
        if context.args.len() != 1 {
            return Err(anyhow::anyhow!("Cos function requires exactly 1 argument"));
        }
        
        let arg = &context.args[0];
        
        let result = match arg.data_type() {
            DataType::Float32 => {
                let array = arg.as_any().downcast_ref::<Float32Array>().unwrap();
                let cos_values: Vec<Option<f32>> = array.iter()
                    .map(|v| v.map(|x| x.cos()))
                    .collect();
                Arc::new(Float32Array::from(cos_values)) as ArrayRef
            }
            DataType::Float64 => {
                let array = arg.as_any().downcast_ref::<Float64Array>().unwrap();
                let cos_values: Vec<Option<f64>> = array.iter()
                    .map(|v| v.map(|x| x.cos()))
                    .collect();
                Arc::new(Float64Array::from(cos_values)) as ArrayRef
            }
            _ => return Err(anyhow::anyhow!("Unsupported cos operation for type: {:?}", arg.data_type())),
        };
        
        Ok(FunctionResult {
            result,
            stats: FunctionStats::default(),
        })
    }
}

/// 指数函数
pub struct ExpFunction;

impl ExpFunction {
    pub fn new() -> Self {
        Self
    }
}

impl FunctionEvaluator for ExpFunction {
    fn name(&self) -> &str {
        "exp"
    }
    
    fn description(&self) -> &str {
        "Exponential function (e^x)"
    }
    
    fn arity(&self) -> FunctionArity {
        FunctionArity::Fixed(1)
    }
    
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return Err(anyhow::anyhow!("Exp function requires exactly 1 argument"));
        }
        
        match arg_types[0] {
            DataType::Float32 => Ok(DataType::Float32),
            DataType::Float64 => Ok(DataType::Float64),
            _ => Ok(DataType::Float64),
        }
    }
    
    fn evaluate(&self, context: &FunctionContext) -> Result<FunctionResult> {
        if context.args.len() != 1 {
            return Err(anyhow::anyhow!("Exp function requires exactly 1 argument"));
        }
        
        let arg = &context.args[0];
        
        let result = match arg.data_type() {
            DataType::Float32 => {
                let array = arg.as_any().downcast_ref::<Float32Array>().unwrap();
                let exp_values: Vec<Option<f32>> = array.iter()
                    .map(|v| v.map(|x| x.exp()))
                    .collect();
                Arc::new(Float32Array::from(exp_values)) as ArrayRef
            }
            DataType::Float64 => {
                let array = arg.as_any().downcast_ref::<Float64Array>().unwrap();
                let exp_values: Vec<Option<f64>> = array.iter()
                    .map(|v| v.map(|x| x.exp()))
                    .collect();
                Arc::new(Float64Array::from(exp_values)) as ArrayRef
            }
            _ => return Err(anyhow::anyhow!("Unsupported exp operation for type: {:?}", arg.data_type())),
        };
        
        Ok(FunctionResult {
            result,
            stats: FunctionStats::default(),
        })
    }
}

/// 自然对数函数
pub struct LnFunction;

impl LnFunction {
    pub fn new() -> Self {
        Self
    }
}

impl FunctionEvaluator for LnFunction {
    fn name(&self) -> &str {
        "ln"
    }
    
    fn description(&self) -> &str {
        "Natural logarithm (ln)"
    }
    
    fn arity(&self) -> FunctionArity {
        FunctionArity::Fixed(1)
    }
    
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return Err(anyhow::anyhow!("Ln function requires exactly 1 argument"));
        }
        
        match arg_types[0] {
            DataType::Float32 => Ok(DataType::Float32),
            DataType::Float64 => Ok(DataType::Float64),
            _ => Ok(DataType::Float64),
        }
    }
    
    fn evaluate(&self, context: &FunctionContext) -> Result<FunctionResult> {
        if context.args.len() != 1 {
            return Err(anyhow::anyhow!("Ln function requires exactly 1 argument"));
        }
        
        let arg = &context.args[0];
        
        let result = match arg.data_type() {
            DataType::Float32 => {
                let array = arg.as_any().downcast_ref::<Float32Array>().unwrap();
                let ln_values: Vec<Option<f32>> = array.iter()
                    .map(|v| v.map(|x| x.ln()))
                    .collect();
                Arc::new(Float32Array::from(ln_values)) as ArrayRef
            }
            DataType::Float64 => {
                let array = arg.as_any().downcast_ref::<Float64Array>().unwrap();
                let ln_values: Vec<Option<f64>> = array.iter()
                    .map(|v| v.map(|x| x.ln()))
                    .collect();
                Arc::new(Float64Array::from(ln_values)) as ArrayRef
            }
            _ => return Err(anyhow::anyhow!("Unsupported ln operation for type: {:?}", arg.data_type())),
        };
        
        Ok(FunctionResult {
            result,
            stats: FunctionStats::default(),
        })
    }
}
