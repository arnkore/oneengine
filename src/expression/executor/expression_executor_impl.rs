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

//! 表达式执行器具体实现
//! 
//! 提供各种表达式的具体计算逻辑

use crate::expression::ast::Expression;
use anyhow::Result;
use arrow::array::*;
use arrow::record_batch::RecordBatch;
use arrow::compute::*;
use arrow::datatypes::*;
use std::sync::Arc;
use datafusion_common::ScalarValue;

/// 表达式执行器实现
pub struct ExpressionExecutorImpl;

impl ExpressionExecutorImpl {
    /// 执行字面量
    pub fn execute_literal(literal: &crate::expression::ast::Literal, batch: &RecordBatch) -> Result<ArrayRef> {
        let len = batch.num_rows();
        match &literal.value {
            ScalarValue::Int8(Some(v)) => Ok(Arc::new(Int8Array::from(vec![*v; len])) as ArrayRef),
            ScalarValue::Int16(Some(v)) => Ok(Arc::new(Int16Array::from(vec![*v; len])) as ArrayRef),
            ScalarValue::Int32(Some(v)) => Ok(Arc::new(Int32Array::from(vec![*v; len])) as ArrayRef),
            ScalarValue::Int64(Some(v)) => Ok(Arc::new(Int64Array::from(vec![*v; len])) as ArrayRef),
            ScalarValue::UInt8(Some(v)) => Ok(Arc::new(UInt8Array::from(vec![*v; len])) as ArrayRef),
            ScalarValue::UInt16(Some(v)) => Ok(Arc::new(UInt16Array::from(vec![*v; len])) as ArrayRef),
            ScalarValue::UInt32(Some(v)) => Ok(Arc::new(UInt32Array::from(vec![*v; len])) as ArrayRef),
            ScalarValue::UInt64(Some(v)) => Ok(Arc::new(UInt64Array::from(vec![*v; len])) as ArrayRef),
            ScalarValue::Float32(Some(v)) => Ok(Arc::new(Float32Array::from(vec![*v; len])) as ArrayRef),
            ScalarValue::Float64(Some(v)) => Ok(Arc::new(Float64Array::from(vec![*v; len])) as ArrayRef),
            ScalarValue::Utf8(Some(v)) => Ok(Arc::new(StringArray::from(vec![v.as_str(); len])) as ArrayRef),
            ScalarValue::LargeUtf8(Some(v)) => Ok(Arc::new(LargeStringArray::from(vec![v.as_str(); len])) as ArrayRef),
            ScalarValue::Boolean(Some(v)) => Ok(Arc::new(BooleanArray::from(vec![*v; len])) as ArrayRef),
            _ => {
                // 处理NULL值
                let null_array = match literal.value.data_type() {
                    DataType::Int8 => Arc::new(Int8Array::from(vec![None; len])) as ArrayRef,
                    DataType::Int16 => Arc::new(Int16Array::from(vec![None; len])) as ArrayRef,
                    DataType::Int32 => Arc::new(Int32Array::from(vec![None; len])) as ArrayRef,
                    DataType::Int64 => Arc::new(Int64Array::from(vec![None; len])) as ArrayRef,
                    DataType::UInt8 => Arc::new(UInt8Array::from(vec![None; len])) as ArrayRef,
                    DataType::UInt16 => Arc::new(UInt16Array::from(vec![None; len])) as ArrayRef,
                    DataType::UInt32 => Arc::new(UInt32Array::from(vec![None; len])) as ArrayRef,
                    DataType::UInt64 => Arc::new(UInt64Array::from(vec![None; len])) as ArrayRef,
                    DataType::Float32 => Arc::new(Float32Array::from(vec![None; len])) as ArrayRef,
                    DataType::Float64 => Arc::new(Float64Array::from(vec![None; len])) as ArrayRef,
                    DataType::Utf8 => Arc::new(StringArray::from(vec![None; len])) as ArrayRef,
                    DataType::LargeUtf8 => Arc::new(LargeStringArray::from(vec![None; len])) as ArrayRef,
                    DataType::Boolean => Arc::new(BooleanArray::from(vec![None; len])) as ArrayRef,
                    _ => return Err(anyhow::anyhow!("Unsupported literal type: {:?}", literal.value.data_type())),
                };
                Ok(null_array)
            }
        }
    }

    /// 执行列引用
    pub fn execute_column(column_ref: &crate::expression::ast::ColumnRef, batch: &RecordBatch) -> Result<ArrayRef> {
        if column_ref.index < batch.num_columns() {
            Ok(batch.column(column_ref.index).clone())
        } else {
            Err(anyhow::anyhow!("Column index {} out of bounds", column_ref.index))
        }
    }

    /// 执行算术表达式
    pub fn execute_arithmetic(arithmetic: &crate::expression::ast::ArithmeticExpr, batch: &RecordBatch) -> Result<ArrayRef> {
        let left = Self::execute(&arithmetic.left, batch)?;
        let right = Self::execute(&arithmetic.right, batch)?;
        
        match arithmetic.op {
            crate::expression::ast::ArithmeticOp::Add => {
                Self::execute_add(&left, &right)
            }
            crate::expression::ast::ArithmeticOp::Subtract => {
                Self::execute_subtract(&left, &right)
            }
            crate::expression::ast::ArithmeticOp::Multiply => {
                Self::execute_multiply(&left, &right)
            }
            crate::expression::ast::ArithmeticOp::Divide => {
                Self::execute_divide(&left, &right)
            }
            crate::expression::ast::ArithmeticOp::Modulo => {
                Self::execute_modulo(&left, &right)
            }
            crate::expression::ast::ArithmeticOp::Power => {
                Self::execute_power(&left, &right)
            }
        }
    }

    /// 执行加法
    fn execute_add(left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef> {
        match (left.data_type(), right.data_type()) {
            (DataType::Int32, DataType::Int32) => {
                let left_array = left.as_any().downcast_ref::<Int32Array>().unwrap();
                let right_array = right.as_any().downcast_ref::<Int32Array>().unwrap();
                Ok(Arc::new(kernels::numeric::add(left_array, right_array)?) as ArrayRef)
            }
            (DataType::Int64, DataType::Int64) => {
                let left_array = left.as_any().downcast_ref::<Int64Array>().unwrap();
                let right_array = right.as_any().downcast_ref::<Int64Array>().unwrap();
                Ok(Arc::new(kernels::numeric::add(left_array, right_array)?) as ArrayRef)
            }
            (DataType::Float32, DataType::Float32) => {
                let left_array = left.as_any().downcast_ref::<Float32Array>().unwrap();
                let right_array = right.as_any().downcast_ref::<Float32Array>().unwrap();
                Ok(Arc::new(kernels::numeric::add(left_array, right_array)?) as ArrayRef)
            }
            (DataType::Float64, DataType::Float64) => {
                let left_array = left.as_any().downcast_ref::<Float64Array>().unwrap();
                let right_array = right.as_any().downcast_ref::<Float64Array>().unwrap();
                Ok(Arc::new(kernels::numeric::add(left_array, right_array)?) as ArrayRef)
            }
            _ => Err(anyhow::anyhow!("Unsupported add operation: {:?} + {:?}", left.data_type(), right.data_type()))
        }
    }

    /// 执行减法
    fn execute_subtract(left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef> {
        match (left.data_type(), right.data_type()) {
            (DataType::Int32, DataType::Int32) => {
                let left_array = left.as_any().downcast_ref::<Int32Array>().unwrap();
                let right_array = right.as_any().downcast_ref::<Int32Array>().unwrap();
                let subtract_values: Vec<Option<i32>> = left_array.iter()
                    .zip(right_array.iter())
                    .map(|(l, r)| match (l, r) {
                        (Some(lv), Some(rv)) => Some(lv - rv),
                        _ => None,
                    })
                    .collect();
                Ok(Arc::new(Int32Array::from(subtract_values)) as ArrayRef)
            }
            (DataType::Int64, DataType::Int64) => {
                let left_array = left.as_any().downcast_ref::<Int64Array>().unwrap();
                let right_array = right.as_any().downcast_ref::<Int64Array>().unwrap();
                let subtract_values: Vec<Option<i64>> = left_array.iter()
                    .zip(right_array.iter())
                    .map(|(l, r)| match (l, r) {
                        (Some(lv), Some(rv)) => Some(lv - rv),
                        _ => None,
                    })
                    .collect();
                Ok(Arc::new(Int64Array::from(subtract_values)) as ArrayRef)
            }
            (DataType::Float32, DataType::Float32) => {
                let left_array = left.as_any().downcast_ref::<Float32Array>().unwrap();
                let right_array = right.as_any().downcast_ref::<Float32Array>().unwrap();
                let subtract_values: Vec<Option<f32>> = left_array.iter()
                    .zip(right_array.iter())
                    .map(|(l, r)| match (l, r) {
                        (Some(lv), Some(rv)) => Some(lv - rv),
                        _ => None,
                    })
                    .collect();
                Ok(Arc::new(Float32Array::from(subtract_values)) as ArrayRef)
            }
            (DataType::Float64, DataType::Float64) => {
                let left_array = left.as_any().downcast_ref::<Float64Array>().unwrap();
                let right_array = right.as_any().downcast_ref::<Float64Array>().unwrap();
                let subtract_values: Vec<Option<f64>> = left_array.iter()
                    .zip(right_array.iter())
                    .map(|(l, r)| match (l, r) {
                        (Some(lv), Some(rv)) => Some(lv - rv),
                        _ => None,
                    })
                    .collect();
                Ok(Arc::new(Float64Array::from(subtract_values)) as ArrayRef)
            }
            _ => Err(anyhow::anyhow!("Unsupported subtract operation: {:?} - {:?}", left.data_type(), right.data_type()))
        }
    }

    /// 执行乘法
    fn execute_multiply(left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef> {
        match (left.data_type(), right.data_type()) {
            (DataType::Int32, DataType::Int32) => {
                let left_array = left.as_any().downcast_ref::<Int32Array>().unwrap();
                let right_array = right.as_any().downcast_ref::<Int32Array>().unwrap();
                let multiply_values: Vec<Option<i32>> = left_array.iter()
                    .zip(right_array.iter())
                    .map(|(l, r)| match (l, r) {
                        (Some(lv), Some(rv)) => Some(lv * rv),
                        _ => None,
                    })
                    .collect();
                Ok(Arc::new(Int32Array::from(multiply_values)) as ArrayRef)
            }
            (DataType::Int64, DataType::Int64) => {
                let left_array = left.as_any().downcast_ref::<Int64Array>().unwrap();
                let right_array = right.as_any().downcast_ref::<Int64Array>().unwrap();
                let multiply_values: Vec<Option<i64>> = left_array.iter()
                    .zip(right_array.iter())
                    .map(|(l, r)| match (l, r) {
                        (Some(lv), Some(rv)) => Some(lv * rv),
                        _ => None,
                    })
                    .collect();
                Ok(Arc::new(Int64Array::from(multiply_values)) as ArrayRef)
            }
            (DataType::Float32, DataType::Float32) => {
                let left_array = left.as_any().downcast_ref::<Float32Array>().unwrap();
                let right_array = right.as_any().downcast_ref::<Float32Array>().unwrap();
                let multiply_values: Vec<Option<f32>> = left_array.iter()
                    .zip(right_array.iter())
                    .map(|(l, r)| match (l, r) {
                        (Some(lv), Some(rv)) => Some(lv * rv),
                        _ => None,
                    })
                    .collect();
                Ok(Arc::new(Float32Array::from(multiply_values)) as ArrayRef)
            }
            (DataType::Float64, DataType::Float64) => {
                let left_array = left.as_any().downcast_ref::<Float64Array>().unwrap();
                let right_array = right.as_any().downcast_ref::<Float64Array>().unwrap();
                let multiply_values: Vec<Option<f64>> = left_array.iter()
                    .zip(right_array.iter())
                    .map(|(l, r)| match (l, r) {
                        (Some(lv), Some(rv)) => Some(lv * rv),
                        _ => None,
                    })
                    .collect();
                Ok(Arc::new(Float64Array::from(multiply_values)) as ArrayRef)
            }
            _ => Err(anyhow::anyhow!("Unsupported multiply operation: {:?} * {:?}", left.data_type(), right.data_type()))
        }
    }

    /// 执行除法
    fn execute_divide(left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef> {
        match (left.data_type(), right.data_type()) {
            (DataType::Int32, DataType::Int32) => {
                let left_array = left.as_any().downcast_ref::<Int32Array>().unwrap();
                let right_array = right.as_any().downcast_ref::<Int32Array>().unwrap();
                let divide_values: Vec<Option<i32>> = left_array.iter()
                    .zip(right_array.iter())
                    .map(|(l, r)| match (l, r) {
                        (Some(lv), Some(rv)) if rv != 0 => Some(lv / rv),
                        _ => None,
                    })
                    .collect();
                Ok(Arc::new(Int32Array::from(divide_values)) as ArrayRef)
            }
            (DataType::Int64, DataType::Int64) => {
                let left_array = left.as_any().downcast_ref::<Int64Array>().unwrap();
                let right_array = right.as_any().downcast_ref::<Int64Array>().unwrap();
                let divide_values: Vec<Option<i64>> = left_array.iter()
                    .zip(right_array.iter())
                    .map(|(l, r)| match (l, r) {
                        (Some(lv), Some(rv)) if rv != 0 => Some(lv / rv),
                        _ => None,
                    })
                    .collect();
                Ok(Arc::new(Int64Array::from(divide_values)) as ArrayRef)
            }
            (DataType::Float32, DataType::Float32) => {
                let left_array = left.as_any().downcast_ref::<Float32Array>().unwrap();
                let right_array = right.as_any().downcast_ref::<Float32Array>().unwrap();
                let divide_values: Vec<Option<f32>> = left_array.iter()
                    .zip(right_array.iter())
                    .map(|(l, r)| match (l, r) {
                        (Some(lv), Some(rv)) if rv != 0.0 => Some(lv / rv),
                        _ => None,
                    })
                    .collect();
                Ok(Arc::new(Float32Array::from(divide_values)) as ArrayRef)
            }
            (DataType::Float64, DataType::Float64) => {
                let left_array = left.as_any().downcast_ref::<Float64Array>().unwrap();
                let right_array = right.as_any().downcast_ref::<Float64Array>().unwrap();
                let divide_values: Vec<Option<f64>> = left_array.iter()
                    .zip(right_array.iter())
                    .map(|(l, r)| match (l, r) {
                        (Some(lv), Some(rv)) if rv != 0.0 => Some(lv / rv),
                        _ => None,
                    })
                    .collect();
                Ok(Arc::new(Float64Array::from(divide_values)) as ArrayRef)
            }
            _ => Err(anyhow::anyhow!("Unsupported divide operation: {:?} / {:?}", left.data_type(), right.data_type()))
        }
    }

    /// 执行取模
    fn execute_modulo(left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef> {
        match (left.data_type(), right.data_type()) {
            (DataType::Int32, DataType::Int32) => {
                let left_array = left.as_any().downcast_ref::<Int32Array>().unwrap();
                let right_array = right.as_any().downcast_ref::<Int32Array>().unwrap();
                let modulo_values: Vec<Option<i32>> = left_array.iter()
                    .zip(right_array.iter())
                    .map(|(l, r)| match (l, r) {
                        (Some(lv), Some(rv)) if rv != 0 => Some(lv % rv),
                        _ => None,
                    })
                    .collect();
                Ok(Arc::new(Int32Array::from(modulo_values)) as ArrayRef)
            }
            (DataType::Int64, DataType::Int64) => {
                let left_array = left.as_any().downcast_ref::<Int64Array>().unwrap();
                let right_array = right.as_any().downcast_ref::<Int64Array>().unwrap();
                let modulo_values: Vec<Option<i64>> = left_array.iter()
                    .zip(right_array.iter())
                    .map(|(l, r)| match (l, r) {
                        (Some(lv), Some(rv)) if rv != 0 => Some(lv % rv),
                        _ => None,
                    })
                    .collect();
                Ok(Arc::new(Int64Array::from(modulo_values)) as ArrayRef)
            }
            _ => Err(anyhow::anyhow!("Unsupported modulo operation: {:?} % {:?}", left.data_type(), right.data_type()))
        }
    }

    /// 执行幂运算
    fn execute_power(left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef> {
        match (left.data_type(), right.data_type()) {
            (DataType::Float64, DataType::Float64) => {
                let left_array = left.as_any().downcast_ref::<Float64Array>().unwrap();
                let right_array = right.as_any().downcast_ref::<Float64Array>().unwrap();
                let power_values: Vec<Option<f64>> = left_array.iter()
                    .zip(right_array.iter())
                    .map(|(l, r)| match (l, r) {
                        (Some(lv), Some(rv)) => Some(lv.powf(rv)),
                        _ => None,
                    })
                    .collect();
                Ok(Arc::new(Float64Array::from(power_values)) as ArrayRef)
            }
            _ => Err(anyhow::anyhow!("Unsupported power operation: {:?} ^ {:?}", left.data_type(), right.data_type()))
        }
    }

    /// 执行比较表达式
    pub fn execute_comparison(comparison: &crate::expression::ast::ComparisonExpr, batch: &RecordBatch) -> Result<ArrayRef> {
        let left = Self::execute(&comparison.left, batch)?;
        let right = Self::execute(&comparison.right, batch)?;
        
        match comparison.op {
            crate::expression::ast::ComparisonOp::Equal => {
                Ok(Arc::new(kernels::cmp::eq(&left, &right)?) as ArrayRef)
            }
            crate::expression::ast::ComparisonOp::NotEqual => {
                Ok(Arc::new(kernels::cmp::neq(&left, &right)?) as ArrayRef)
            }
            crate::expression::ast::ComparisonOp::LessThan => {
                Ok(Arc::new(kernels::cmp::lt(&left, &right)?) as ArrayRef)
            }
            crate::expression::ast::ComparisonOp::LessThanOrEqual => {
                Ok(Arc::new(kernels::cmp::lt_eq(&left, &right)?) as ArrayRef)
            }
            crate::expression::ast::ComparisonOp::GreaterThan => {
                Ok(Arc::new(kernels::cmp::gt(&left, &right)?) as ArrayRef)
            }
            crate::expression::ast::ComparisonOp::GreaterThanOrEqual => {
                Ok(Arc::new(kernels::cmp::gt_eq(&left, &right)?) as ArrayRef)
            }
            _ => Err(anyhow::anyhow!("Unsupported comparison operation: {:?}", comparison.op))
        }
    }

    /// 执行逻辑表达式
    pub fn execute_logical(logical: &crate::expression::ast::LogicalExpr, batch: &RecordBatch) -> Result<ArrayRef> {
        let left = Self::execute(&logical.left, batch)?;
        let right = Self::execute(&logical.right, batch)?;
        
        let left_bool = left.as_any().downcast_ref::<BooleanArray>()
            .ok_or_else(|| anyhow::anyhow!("Left operand is not boolean"))?;
        let right_bool = right.as_any().downcast_ref::<BooleanArray>()
            .ok_or_else(|| anyhow::anyhow!("Right operand is not boolean"))?;
        
        match logical.op {
            crate::expression::ast::LogicalOp::And => {
                Ok(Arc::new(kernels::boolean::and(left_bool, right_bool)?) as ArrayRef)
            }
            crate::expression::ast::LogicalOp::Or => {
                Ok(Arc::new(kernels::boolean::or(left_bool, right_bool)?) as ArrayRef)
            }
            crate::expression::ast::LogicalOp::Not => {
                Ok(Arc::new(kernels::boolean::not(left_bool)?) as ArrayRef)
            }
        }
    }

    /// 执行函数调用
    pub fn execute_function(function_call: &crate::expression::ast::FunctionCall, batch: &RecordBatch) -> Result<ArrayRef> {
        // 先执行所有参数
        let mut arg_arrays = Vec::new();
        for arg in &function_call.args {
            arg_arrays.push(Self::execute(arg, batch)?);
        }
        
        match function_call.name.as_str() {
            "abs" => Self::execute_abs(&arg_arrays[0]),
            "sqrt" => Self::execute_sqrt(&arg_arrays[0]),
            "sin" => Self::execute_sin(&arg_arrays[0]),
            "cos" => Self::execute_cos(&arg_arrays[0]),
            "exp" => Self::execute_exp(&arg_arrays[0]),
            "ln" => Self::execute_ln(&arg_arrays[0]),
            _ => Err(anyhow::anyhow!("Unknown function: {}", function_call.name))
        }
    }

    /// 执行绝对值函数
    fn execute_abs(array: &ArrayRef) -> Result<ArrayRef> {
        match array.data_type() {
            DataType::Int32 => {
                let array = array.as_any().downcast_ref::<Int32Array>().unwrap();
                let abs_values: Vec<Option<i32>> = array.iter().map(|v| v.map(|x| x.abs())).collect();
                Ok(Arc::new(Int32Array::from(abs_values)) as ArrayRef)
            }
            DataType::Int64 => {
                let array = array.as_any().downcast_ref::<Int64Array>().unwrap();
                let abs_values: Vec<Option<i64>> = array.iter().map(|v| v.map(|x| x.abs())).collect();
                Ok(Arc::new(Int64Array::from(abs_values)) as ArrayRef)
            }
            DataType::Float32 => {
                let array = array.as_any().downcast_ref::<Float32Array>().unwrap();
                let abs_values: Vec<Option<f32>> = array.iter().map(|v| v.map(|x| x.abs())).collect();
                Ok(Arc::new(Float32Array::from(abs_values)) as ArrayRef)
            }
            DataType::Float64 => {
                let array = array.as_any().downcast_ref::<Float64Array>().unwrap();
                let abs_values: Vec<Option<f64>> = array.iter().map(|v| v.map(|x| x.abs())).collect();
                Ok(Arc::new(Float64Array::from(abs_values)) as ArrayRef)
            }
            _ => Err(anyhow::anyhow!("Unsupported abs operation for type: {:?}", array.data_type()))
        }
    }

    /// 执行平方根函数
    fn execute_sqrt(array: &ArrayRef) -> Result<ArrayRef> {
        match array.data_type() {
            DataType::Float32 => {
                let array = array.as_any().downcast_ref::<Float32Array>().unwrap();
                let sqrt_values: Vec<Option<f32>> = array.iter().map(|v| v.map(|x| x.sqrt())).collect();
                Ok(Arc::new(Float32Array::from(sqrt_values)) as ArrayRef)
            }
            DataType::Float64 => {
                let array = array.as_any().downcast_ref::<Float64Array>().unwrap();
                let sqrt_values: Vec<Option<f64>> = array.iter().map(|v| v.map(|x| x.sqrt())).collect();
                Ok(Arc::new(Float64Array::from(sqrt_values)) as ArrayRef)
            }
            _ => Err(anyhow::anyhow!("Unsupported sqrt operation for type: {:?}", array.data_type()))
        }
    }

    /// 执行正弦函数
    fn execute_sin(array: &ArrayRef) -> Result<ArrayRef> {
        match array.data_type() {
            DataType::Float32 => {
                let array = array.as_any().downcast_ref::<Float32Array>().unwrap();
                let sin_values: Vec<Option<f32>> = array.iter().map(|v| v.map(|x| x.sin())).collect();
                Ok(Arc::new(Float32Array::from(sin_values)) as ArrayRef)
            }
            DataType::Float64 => {
                let array = array.as_any().downcast_ref::<Float64Array>().unwrap();
                let sin_values: Vec<Option<f64>> = array.iter().map(|v| v.map(|x| x.sin())).collect();
                Ok(Arc::new(Float64Array::from(sin_values)) as ArrayRef)
            }
            _ => Err(anyhow::anyhow!("Unsupported sin operation for type: {:?}", array.data_type()))
        }
    }

    /// 执行余弦函数
    fn execute_cos(array: &ArrayRef) -> Result<ArrayRef> {
        match array.data_type() {
            DataType::Float32 => {
                let array = array.as_any().downcast_ref::<Float32Array>().unwrap();
                let cos_values: Vec<Option<f32>> = array.iter().map(|v| v.map(|x| x.cos())).collect();
                Ok(Arc::new(Float32Array::from(cos_values)) as ArrayRef)
            }
            DataType::Float64 => {
                let array = array.as_any().downcast_ref::<Float64Array>().unwrap();
                let cos_values: Vec<Option<f64>> = array.iter().map(|v| v.map(|x| x.cos())).collect();
                Ok(Arc::new(Float64Array::from(cos_values)) as ArrayRef)
            }
            _ => Err(anyhow::anyhow!("Unsupported cos operation for type: {:?}", array.data_type()))
        }
    }

    /// 执行指数函数
    fn execute_exp(array: &ArrayRef) -> Result<ArrayRef> {
        match array.data_type() {
            DataType::Float32 => {
                let array = array.as_any().downcast_ref::<Float32Array>().unwrap();
                let exp_values: Vec<Option<f32>> = array.iter().map(|v| v.map(|x| x.exp())).collect();
                Ok(Arc::new(Float32Array::from(exp_values)) as ArrayRef)
            }
            DataType::Float64 => {
                let array = array.as_any().downcast_ref::<Float64Array>().unwrap();
                let exp_values: Vec<Option<f64>> = array.iter().map(|v| v.map(|x| x.exp())).collect();
                Ok(Arc::new(Float64Array::from(exp_values)) as ArrayRef)
            }
            _ => Err(anyhow::anyhow!("Unsupported exp operation for type: {:?}", array.data_type()))
        }
    }

    /// 执行自然对数函数
    fn execute_ln(array: &ArrayRef) -> Result<ArrayRef> {
        match array.data_type() {
            DataType::Float32 => {
                let array = array.as_any().downcast_ref::<Float32Array>().unwrap();
                let ln_values: Vec<Option<f32>> = array.iter().map(|v| v.map(|x| x.ln())).collect();
                Ok(Arc::new(Float32Array::from(ln_values)) as ArrayRef)
            }
            DataType::Float64 => {
                let array = array.as_any().downcast_ref::<Float64Array>().unwrap();
                let ln_values: Vec<Option<f64>> = array.iter().map(|v| v.map(|x| x.ln())).collect();
                Ok(Arc::new(Float64Array::from(ln_values)) as ArrayRef)
            }
            _ => Err(anyhow::anyhow!("Unsupported ln operation for type: {:?}", array.data_type()))
        }
    }

    /// 执行类型转换
    pub fn execute_cast(cast_expr: &crate::expression::ast::CastExpr, batch: &RecordBatch) -> Result<ArrayRef> {
        let array = Self::execute(&cast_expr.expr, batch)?;
        Ok(Arc::new(cast(&array, &cast_expr.target_type)?) as ArrayRef)
    }

    /// 执行条件表达式
    pub fn execute_case(case_expr: &crate::expression::ast::CaseExpr, batch: &RecordBatch) -> Result<ArrayRef> {
        let condition = Self::execute(&case_expr.condition, batch)?;
        let then_expr = Self::execute(&case_expr.then_expr, batch)?;
        let else_expr = Self::execute(&case_expr.else_expr, batch)?;
        
        let condition_bool = condition.as_any().downcast_ref::<BooleanArray>()
            .ok_or_else(|| anyhow::anyhow!("Condition must be boolean"))?;
        
        // 手动实现if_then_else逻辑
        let then_array = then_expr.as_any().downcast_ref::<Int32Array>().unwrap();
        let else_array = else_expr.as_any().downcast_ref::<Int32Array>().unwrap();
        let result_values: Vec<Option<i32>> = condition_bool.iter()
            .zip(then_array.iter())
            .zip(else_array.iter())
            .map(|((cond, then_val), else_val)| {
                match (cond, then_val, else_val) {
                    (Some(true), Some(tv), _) => Some(tv),
                    (Some(false), _, Some(ev)) => Some(ev),
                    _ => None,
                }
            })
            .collect();
        Ok(Arc::new(Int32Array::from(result_values)) as ArrayRef)
    }

    /// 执行聚合表达式（在投影中通常不支持，但保留接口）
    pub fn execute_aggregate(_agg_expr: &crate::expression::ast::AggregateExpr, _batch: &RecordBatch) -> Result<ArrayRef> {
        Err(anyhow::anyhow!("Aggregate expressions not supported in projection"))
    }

    /// 通用执行方法
    pub fn execute(expression: &Expression, batch: &RecordBatch) -> Result<ArrayRef> {
        match expression {
            Expression::Literal(literal) => {
                Self::execute_literal(literal, batch)
            }
            Expression::Column(column_ref) => {
                Self::execute_column(column_ref, batch)
            }
            Expression::Arithmetic(arithmetic) => {
                Self::execute_arithmetic(arithmetic, batch)
            }
            Expression::Comparison(comparison) => {
                Self::execute_comparison(comparison, batch)
            }
            Expression::Logical(logical) => {
                Self::execute_logical(logical, batch)
            }
            Expression::Function(function_call) => {
                Self::execute_function(function_call, batch)
            }
            Expression::Cast(cast_expr) => {
                Self::execute_cast(cast_expr, batch)
            }
            Expression::Case(case_expr) => {
                Self::execute_case(case_expr, batch)
            }
            Expression::Aggregate(agg_expr) => {
                Self::execute_aggregate(agg_expr, batch)
            }
            _ => {
                // TODO: Implement other expression types
                Err(anyhow::anyhow!("Expression type not implemented"))
            }
        }
    }
}
