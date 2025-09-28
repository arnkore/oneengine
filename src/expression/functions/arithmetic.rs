/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache Software Foundation (ASF) under one or more
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

//! 算术函数实现
//! 
//! 提供基本的算术运算函数

use super::{FunctionEvaluator, FunctionContext, FunctionResult, FunctionArity, FunctionStats};
use arrow::array::*;
use arrow::datatypes::*;
use anyhow::Result;
use std::sync::Arc;

/// 加法函数
pub struct AddFunction;

impl AddFunction {
    pub fn new() -> Self {
        Self
    }
}

impl FunctionEvaluator for AddFunction {
    fn name(&self) -> &str {
        "add"
    }
    
    fn description(&self) -> &str {
        "Add two numbers"
    }
    
    fn arity(&self) -> FunctionArity {
        FunctionArity::Fixed(2)
    }
    
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 {
            return Err(anyhow::anyhow!("Add function requires exactly 2 arguments"));
        }
        
        // 返回两个参数中精度更高的类型
        match (arg_types[0].clone(), &arg_types[1]) {
            (DataType::Int32, DataType::Int32) => Ok(DataType::Int32),
            (DataType::Int64, DataType::Int64) => Ok(DataType::Int64),
            (DataType::Float32, DataType::Float32) => Ok(DataType::Float32),
            (DataType::Float64, DataType::Float64) => Ok(DataType::Float64),
            _ => Ok(DataType::Float64), // 默认返回Float64
        }
    }
    
    fn evaluate(&self, context: &FunctionContext) -> Result<FunctionResult> {
        if context.args.len() != 2 {
            return Err(anyhow::anyhow!("Add function requires exactly 2 arguments"));
        }
        
        let left = &context.args[0];
        let right = &context.args[1];
        
        let result = match (left.data_type(), right.data_type()) {
            (DataType::Int32, DataType::Int32) => {
                let left_array = left.as_any().downcast_ref::<Int32Array>().unwrap();
                let right_array = right.as_any().downcast_ref::<Int32Array>().unwrap();
                let add_values: Vec<Option<i32>> = left_array.iter()
                    .zip(right_array.iter())
                    .map(|(l, r)| match (l, r) {
                        (Some(lv), Some(rv)) => Some(lv + rv),
                        _ => None,
                    })
                    .collect();
                Arc::new(Int32Array::from(add_values)) as ArrayRef
            }
            (DataType::Int64, DataType::Int64) => {
                let left_array = left.as_any().downcast_ref::<Int64Array>().unwrap();
                let right_array = right.as_any().downcast_ref::<Int64Array>().unwrap();
                let add_values: Vec<Option<i64>> = left_array.iter()
                    .zip(right_array.iter())
                    .map(|(l, r)| match (l, r) {
                        (Some(lv), Some(rv)) => Some(lv + rv),
                        _ => None,
                    })
                    .collect();
                Arc::new(Int64Array::from(add_values)) as ArrayRef
            }
            (DataType::Float32, DataType::Float32) => {
                let left_array = left.as_any().downcast_ref::<Float32Array>().unwrap();
                let right_array = right.as_any().downcast_ref::<Float32Array>().unwrap();
                let add_values: Vec<Option<f32>> = left_array.iter()
                    .zip(right_array.iter())
                    .map(|(l, r)| match (l, r) {
                        (Some(lv), Some(rv)) => Some(lv + rv),
                        _ => None,
                    })
                    .collect();
                Arc::new(Float32Array::from(add_values)) as ArrayRef
            }
            (DataType::Float64, DataType::Float64) => {
                let left_array = left.as_any().downcast_ref::<Float64Array>().unwrap();
                let right_array = right.as_any().downcast_ref::<Float64Array>().unwrap();
                let add_values: Vec<Option<f64>> = left_array.iter()
                    .zip(right_array.iter())
                    .map(|(l, r)| match (l, r) {
                        (Some(lv), Some(rv)) => Some(lv + rv),
                        _ => None,
                    })
                    .collect();
                Arc::new(Float64Array::from(add_values)) as ArrayRef
            }
            _ => return Err(anyhow::anyhow!("Unsupported add operation: {:?} + {:?}", left.data_type(), right.data_type())),
        };
        
        Ok(FunctionResult {
            result,
            stats: FunctionStats::default(),
        })
    }
}

/// 减法函数
pub struct SubtractFunction;

impl SubtractFunction {
    pub fn new() -> Self {
        Self
    }
}

impl FunctionEvaluator for SubtractFunction {
    fn name(&self) -> &str {
        "subtract"
    }
    
    fn description(&self) -> &str {
        "Subtract two numbers"
    }
    
    fn arity(&self) -> FunctionArity {
        FunctionArity::Fixed(2)
    }
    
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 {
            return Err(anyhow::anyhow!("Subtract function requires exactly 2 arguments"));
        }
        
        match (arg_types[0].clone(), &arg_types[1]) {
            (DataType::Int32, DataType::Int32) => Ok(DataType::Int32),
            (DataType::Int64, DataType::Int64) => Ok(DataType::Int64),
            (DataType::Float32, DataType::Float32) => Ok(DataType::Float32),
            (DataType::Float64, DataType::Float64) => Ok(DataType::Float64),
            _ => Ok(DataType::Float64),
        }
    }
    
    fn evaluate(&self, context: &FunctionContext) -> Result<FunctionResult> {
        if context.args.len() != 2 {
            return Err(anyhow::anyhow!("Subtract function requires exactly 2 arguments"));
        }
        
        let left = &context.args[0];
        let right = &context.args[1];
        
        let result = match (left.data_type(), right.data_type()) {
            (DataType::Int32, DataType::Int32) => {
                let left_array = left.as_any().downcast_ref::<Int32Array>().unwrap();
                let right_array = right.as_any().downcast_ref::<Int32Array>().unwrap();
                let sub_values: Vec<Option<i32>> = left_array.iter()
                    .zip(right_array.iter())
                    .map(|(l, r)| match (l, r) {
                        (Some(lv), Some(rv)) => Some(lv - rv),
                        _ => None,
                    })
                    .collect();
                Arc::new(Int32Array::from(sub_values)) as ArrayRef
            }
            (DataType::Int64, DataType::Int64) => {
                let left_array = left.as_any().downcast_ref::<Int64Array>().unwrap();
                let right_array = right.as_any().downcast_ref::<Int64Array>().unwrap();
                let sub_values: Vec<Option<i64>> = left_array.iter()
                    .zip(right_array.iter())
                    .map(|(l, r)| match (l, r) {
                        (Some(lv), Some(rv)) => Some(lv - rv),
                        _ => None,
                    })
                    .collect();
                Arc::new(Int64Array::from(sub_values)) as ArrayRef
            }
            (DataType::Float32, DataType::Float32) => {
                let left_array = left.as_any().downcast_ref::<Float32Array>().unwrap();
                let right_array = right.as_any().downcast_ref::<Float32Array>().unwrap();
                let sub_values: Vec<Option<f32>> = left_array.iter()
                    .zip(right_array.iter())
                    .map(|(l, r)| match (l, r) {
                        (Some(lv), Some(rv)) => Some(lv - rv),
                        _ => None,
                    })
                    .collect();
                Arc::new(Float32Array::from(sub_values)) as ArrayRef
            }
            (DataType::Float64, DataType::Float64) => {
                let left_array = left.as_any().downcast_ref::<Float64Array>().unwrap();
                let right_array = right.as_any().downcast_ref::<Float64Array>().unwrap();
                let sub_values: Vec<Option<f64>> = left_array.iter()
                    .zip(right_array.iter())
                    .map(|(l, r)| match (l, r) {
                        (Some(lv), Some(rv)) => Some(lv - rv),
                        _ => None,
                    })
                    .collect();
                Arc::new(Float64Array::from(sub_values)) as ArrayRef
            }
            _ => return Err(anyhow::anyhow!("Unsupported subtract operation: {:?} - {:?}", left.data_type(), right.data_type())),
        };
        
        Ok(FunctionResult {
            result,
            stats: FunctionStats::default(),
        })
    }
}

/// 乘法函数
pub struct MultiplyFunction;

impl MultiplyFunction {
    pub fn new() -> Self {
        Self
    }
}

impl FunctionEvaluator for MultiplyFunction {
    fn name(&self) -> &str {
        "multiply"
    }
    
    fn description(&self) -> &str {
        "Multiply two numbers"
    }
    
    fn arity(&self) -> FunctionArity {
        FunctionArity::Fixed(2)
    }
    
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 {
            return Err(anyhow::anyhow!("Multiply function requires exactly 2 arguments"));
        }
        
        match (arg_types[0].clone(), &arg_types[1]) {
            (DataType::Int32, DataType::Int32) => Ok(DataType::Int32),
            (DataType::Int64, DataType::Int64) => Ok(DataType::Int64),
            (DataType::Float32, DataType::Float32) => Ok(DataType::Float32),
            (DataType::Float64, DataType::Float64) => Ok(DataType::Float64),
            _ => Ok(DataType::Float64),
        }
    }
    
    fn evaluate(&self, context: &FunctionContext) -> Result<FunctionResult> {
        if context.args.len() != 2 {
            return Err(anyhow::anyhow!("Multiply function requires exactly 2 arguments"));
        }
        
        let left = &context.args[0];
        let right = &context.args[1];
        
        let result = match (left.data_type(), right.data_type()) {
            (DataType::Int32, DataType::Int32) => {
                let left_array = left.as_any().downcast_ref::<Int32Array>().unwrap();
                let right_array = right.as_any().downcast_ref::<Int32Array>().unwrap();
                let mul_values: Vec<Option<i32>> = left_array.iter()
                    .zip(right_array.iter())
                    .map(|(l, r)| match (l, r) {
                        (Some(lv), Some(rv)) => Some(lv * rv),
                        _ => None,
                    })
                    .collect();
                Arc::new(Int32Array::from(mul_values)) as ArrayRef
            }
            (DataType::Int64, DataType::Int64) => {
                let left_array = left.as_any().downcast_ref::<Int64Array>().unwrap();
                let right_array = right.as_any().downcast_ref::<Int64Array>().unwrap();
                let mul_values: Vec<Option<i64>> = left_array.iter()
                    .zip(right_array.iter())
                    .map(|(l, r)| match (l, r) {
                        (Some(lv), Some(rv)) => Some(lv * rv),
                        _ => None,
                    })
                    .collect();
                Arc::new(Int64Array::from(mul_values)) as ArrayRef
            }
            (DataType::Float32, DataType::Float32) => {
                let left_array = left.as_any().downcast_ref::<Float32Array>().unwrap();
                let right_array = right.as_any().downcast_ref::<Float32Array>().unwrap();
                let mul_values: Vec<Option<f32>> = left_array.iter()
                    .zip(right_array.iter())
                    .map(|(l, r)| match (l, r) {
                        (Some(lv), Some(rv)) => Some(lv * rv),
                        _ => None,
                    })
                    .collect();
                Arc::new(Float32Array::from(mul_values)) as ArrayRef
            }
            (DataType::Float64, DataType::Float64) => {
                let left_array = left.as_any().downcast_ref::<Float64Array>().unwrap();
                let right_array = right.as_any().downcast_ref::<Float64Array>().unwrap();
                let mul_values: Vec<Option<f64>> = left_array.iter()
                    .zip(right_array.iter())
                    .map(|(l, r)| match (l, r) {
                        (Some(lv), Some(rv)) => Some(lv * rv),
                        _ => None,
                    })
                    .collect();
                Arc::new(Float64Array::from(mul_values)) as ArrayRef
            }
            _ => return Err(anyhow::anyhow!("Unsupported multiply operation: {:?} * {:?}", left.data_type(), right.data_type())),
        };
        
        Ok(FunctionResult {
            result,
            stats: FunctionStats::default(),
        })
    }
}

/// 除法函数
pub struct DivideFunction;

impl DivideFunction {
    pub fn new() -> Self {
        Self
    }
}

impl FunctionEvaluator for DivideFunction {
    fn name(&self) -> &str {
        "divide"
    }
    
    fn description(&self) -> &str {
        "Divide two numbers"
    }
    
    fn arity(&self) -> FunctionArity {
        FunctionArity::Fixed(2)
    }
    
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 {
            return Err(anyhow::anyhow!("Divide function requires exactly 2 arguments"));
        }
        
        match (arg_types[0].clone(), &arg_types[1]) {
            (DataType::Int32, DataType::Int32) => Ok(DataType::Int32),
            (DataType::Int64, DataType::Int64) => Ok(DataType::Int64),
            (DataType::Float32, DataType::Float32) => Ok(DataType::Float32),
            (DataType::Float64, DataType::Float64) => Ok(DataType::Float64),
            _ => Ok(DataType::Float64),
        }
    }
    
    fn evaluate(&self, context: &FunctionContext) -> Result<FunctionResult> {
        if context.args.len() != 2 {
            return Err(anyhow::anyhow!("Divide function requires exactly 2 arguments"));
        }
        
        let left = &context.args[0];
        let right = &context.args[1];
        
        let result = match (left.data_type(), right.data_type()) {
            (DataType::Int32, DataType::Int32) => {
                let left_array = left.as_any().downcast_ref::<Int32Array>().unwrap();
                let right_array = right.as_any().downcast_ref::<Int32Array>().unwrap();
                let div_values: Vec<Option<i32>> = left_array.iter()
                    .zip(right_array.iter())
                    .map(|(l, r)| match (l, r) {
                        (Some(lv), Some(rv)) if rv != 0 => Some(lv / rv),
                        _ => None,
                    })
                    .collect();
                Arc::new(Int32Array::from(div_values)) as ArrayRef
            }
            (DataType::Int64, DataType::Int64) => {
                let left_array = left.as_any().downcast_ref::<Int64Array>().unwrap();
                let right_array = right.as_any().downcast_ref::<Int64Array>().unwrap();
                let div_values: Vec<Option<i64>> = left_array.iter()
                    .zip(right_array.iter())
                    .map(|(l, r)| match (l, r) {
                        (Some(lv), Some(rv)) if rv != 0 => Some(lv / rv),
                        _ => None,
                    })
                    .collect();
                Arc::new(Int64Array::from(div_values)) as ArrayRef
            }
            (DataType::Float32, DataType::Float32) => {
                let left_array = left.as_any().downcast_ref::<Float32Array>().unwrap();
                let right_array = right.as_any().downcast_ref::<Float32Array>().unwrap();
                let div_values: Vec<Option<f32>> = left_array.iter()
                    .zip(right_array.iter())
                    .map(|(l, r)| match (l, r) {
                        (Some(lv), Some(rv)) if rv != 0.0 => Some(lv / rv),
                        _ => None,
                    })
                    .collect();
                Arc::new(Float32Array::from(div_values)) as ArrayRef
            }
            (DataType::Float64, DataType::Float64) => {
                let left_array = left.as_any().downcast_ref::<Float64Array>().unwrap();
                let right_array = right.as_any().downcast_ref::<Float64Array>().unwrap();
                let div_values: Vec<Option<f64>> = left_array.iter()
                    .zip(right_array.iter())
                    .map(|(l, r)| match (l, r) {
                        (Some(lv), Some(rv)) if rv != 0.0 => Some(lv / rv),
                        _ => None,
                    })
                    .collect();
                Arc::new(Float64Array::from(div_values)) as ArrayRef
            }
            _ => return Err(anyhow::anyhow!("Unsupported divide operation: {:?} / {:?}", left.data_type(), right.data_type())),
        };
        
        Ok(FunctionResult {
            result,
            stats: FunctionStats::default(),
        })
    }
}

/// 取模函数
pub struct ModuloFunction;

impl ModuloFunction {
    pub fn new() -> Self {
        Self
    }
}

impl FunctionEvaluator for ModuloFunction {
    fn name(&self) -> &str {
        "modulo"
    }
    
    fn description(&self) -> &str {
        "Modulo operation"
    }
    
    fn arity(&self) -> FunctionArity {
        FunctionArity::Fixed(2)
    }
    
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 {
            return Err(anyhow::anyhow!("Modulo function requires exactly 2 arguments"));
        }
        
        match (arg_types[0].clone(), &arg_types[1]) {
            (DataType::Int32, DataType::Int32) => Ok(DataType::Int32),
            (DataType::Int64, DataType::Int64) => Ok(DataType::Int64),
            _ => Err(anyhow::anyhow!("Modulo operation only supported for integer types")),
        }
    }
    
    fn evaluate(&self, context: &FunctionContext) -> Result<FunctionResult> {
        if context.args.len() != 2 {
            return Err(anyhow::anyhow!("Modulo function requires exactly 2 arguments"));
        }
        
        let left = &context.args[0];
        let right = &context.args[1];
        
        let result = match (left.data_type(), right.data_type()) {
            (DataType::Int32, DataType::Int32) => {
                let left_array = left.as_any().downcast_ref::<Int32Array>().unwrap();
                let right_array = right.as_any().downcast_ref::<Int32Array>().unwrap();
                let mod_values: Vec<Option<i32>> = left_array.iter()
                    .zip(right_array.iter())
                    .map(|(l, r)| match (l, r) {
                        (Some(lv), Some(rv)) if rv != 0 => Some(lv % rv),
                        _ => None,
                    })
                    .collect();
                Arc::new(Int32Array::from(mod_values)) as ArrayRef
            }
            (DataType::Int64, DataType::Int64) => {
                let left_array = left.as_any().downcast_ref::<Int64Array>().unwrap();
                let right_array = right.as_any().downcast_ref::<Int64Array>().unwrap();
                let mod_values: Vec<Option<i64>> = left_array.iter()
                    .zip(right_array.iter())
                    .map(|(l, r)| match (l, r) {
                        (Some(lv), Some(rv)) if rv != 0 => Some(lv % rv),
                        _ => None,
                    })
                    .collect();
                Arc::new(Int64Array::from(mod_values)) as ArrayRef
            }
            _ => return Err(anyhow::anyhow!("Unsupported modulo operation: {:?} % {:?}", left.data_type(), right.data_type())),
        };
        
        Ok(FunctionResult {
            result,
            stats: FunctionStats::default(),
        })
    }
}

/// 幂运算函数
pub struct PowerFunction;

impl PowerFunction {
    pub fn new() -> Self {
        Self
    }
}

impl FunctionEvaluator for PowerFunction {
    fn name(&self) -> &str {
        "power"
    }
    
    fn description(&self) -> &str {
        "Power operation (x^y)"
    }
    
    fn arity(&self) -> FunctionArity {
        FunctionArity::Fixed(2)
    }
    
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 {
            return Err(anyhow::anyhow!("Power function requires exactly 2 arguments"));
        }
        
        match (arg_types[0].clone(), &arg_types[1]) {
            (DataType::Float32, DataType::Float32) => Ok(DataType::Float32),
            (DataType::Float64, DataType::Float64) => Ok(DataType::Float64),
            _ => Ok(DataType::Float64), // 默认返回Float64
        }
    }
    
    fn evaluate(&self, context: &FunctionContext) -> Result<FunctionResult> {
        if context.args.len() != 2 {
            return Err(anyhow::anyhow!("Power function requires exactly 2 arguments"));
        }
        
        let left = &context.args[0];
        let right = &context.args[1];
        
        let result = match (left.data_type(), right.data_type()) {
            (DataType::Float32, DataType::Float32) => {
                let left_array = left.as_any().downcast_ref::<Float32Array>().unwrap();
                let right_array = right.as_any().downcast_ref::<Float32Array>().unwrap();
                let pow_values: Vec<Option<f32>> = left_array.iter()
                    .zip(right_array.iter())
                    .map(|(l, r)| match (l, r) {
                        (Some(lv), Some(rv)) => Some(lv.powf(rv)),
                        _ => None,
                    })
                    .collect();
                Arc::new(Float32Array::from(pow_values)) as ArrayRef
            }
            (DataType::Float64, DataType::Float64) => {
                let left_array = left.as_any().downcast_ref::<Float64Array>().unwrap();
                let right_array = right.as_any().downcast_ref::<Float64Array>().unwrap();
                let pow_values: Vec<Option<f64>> = left_array.iter()
                    .zip(right_array.iter())
                    .map(|(l, r)| match (l, r) {
                        (Some(lv), Some(rv)) => Some(lv.powf(rv)),
                        _ => None,
                    })
                    .collect();
                Arc::new(Float64Array::from(pow_values)) as ArrayRef
            }
            _ => return Err(anyhow::anyhow!("Unsupported power operation: {:?} ^ {:?}", left.data_type(), right.data_type())),
        };
        
        Ok(FunctionResult {
            result,
            stats: FunctionStats::default(),
        })
    }
}
