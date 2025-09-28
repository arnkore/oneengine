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

//! String functions implementation
//! 
//! Provides string processing functions

use super::{FunctionEvaluator, FunctionContext, FunctionResult, FunctionArity, FunctionStats};
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use datafusion_common::ScalarValue;
use anyhow::Result;
use std::sync::Arc;
use regex::Regex;

/// String concatenation function
pub struct ConcatFunction;

impl ConcatFunction {
    pub fn new() -> Self {
        Self
    }
}

impl FunctionEvaluator for ConcatFunction {
    fn name(&self) -> &str { "concat" }
    fn description(&self) -> &str { "Concatenates multiple strings" }
    fn arity(&self) -> FunctionArity { FunctionArity::Variadic }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn evaluate(&self, context: &FunctionContext) -> Result<FunctionResult> {
        if context.args.is_empty() {
            return Err(anyhow::anyhow!("concat requires at least one argument"));
        }
        
        let mut result_values = Vec::with_capacity(context.batch.num_rows());
        
        for row_idx in 0..context.batch.num_rows() {
            let mut concat_result = String::new();
            let mut has_null = false;
            
            for arg in &context.args {
                if let Some(string_array) = arg.as_any().downcast_ref::<StringArray>() {
                    if string_array.is_null(row_idx) {
                        has_null = true;
                        break;
                    } else {
                        concat_result.push_str(string_array.value(row_idx));
                    }
                } else {
                    return Err(anyhow::anyhow!("All arguments must be String"));
                }
            }
            
            if has_null {
                result_values.push(None);
            } else {
                result_values.push(Some(concat_result));
            }
        }
        
        let result_array = StringArray::from(result_values);
        Ok(FunctionResult { result: Arc::new(result_array) })
    }
    fn supports_vectorization(&self) -> bool { true }
    fn supports_simd(&self) -> bool { false }
}

/// Substring function
pub struct SubstringFunction;

impl SubstringFunction {
    pub fn new() -> Self {
        Self
    }
}

impl FunctionEvaluator for SubstringFunction {
    fn name(&self) -> &str { "substring" }
    fn description(&self) -> &str { "Extracts substring" }
    fn arity(&self) -> FunctionArity { FunctionArity::Exact(3) }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn evaluate(&self, context: &FunctionContext) -> Result<FunctionResult> {
        if context.args.len() != 3 {
            return Err(anyhow::anyhow!("substring requires exactly 3 arguments"));
        }
        
        let string_array = context.args[0].as_any().downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("First argument must be String"))?;
        let start_array = context.args[1].as_any().downcast_ref::<Int32Array>()
            .ok_or_else(|| anyhow::anyhow!("Second argument must be Int32"))?;
        let length_array = context.args[2].as_any().downcast_ref::<Int32Array>()
            .ok_or_else(|| anyhow::anyhow!("Third argument must be Int32"))?;
        
        let mut result_values = Vec::with_capacity(string_array.len());
        for i in 0..string_array.len() {
            if string_array.is_null(i) || start_array.is_null(i) || length_array.is_null(i) {
                result_values.push(None);
            } else {
                let string = string_array.value(i);
                let start = start_array.value(i) as usize;
                let length = length_array.value(i) as usize;
                
                // Handle negative indices
                let start = if start > 0 { start - 1 } else { 0 }; // Convert to 0-based index
                let end = std::cmp::min(start + length, string.len());
                
                if start < string.len() {
                    let substring = &string[start..end];
                    result_values.push(Some(substring.to_string()));
                } else {
                    result_values.push(Some(String::new()));
                }
            }
        }
        
        let result_array = StringArray::from(result_values);
        Ok(FunctionResult { result: Arc::new(result_array) })
    }
    fn supports_vectorization(&self) -> bool { true }
    fn supports_simd(&self) -> bool { false }
}

/// String length function
pub struct LengthFunction;

impl LengthFunction {
    pub fn new() -> Self {
        Self
    }
}

impl FunctionEvaluator for LengthFunction {
    fn name(&self) -> &str { "length" }
    fn description(&self) -> &str { "Calculates string length" }
    fn arity(&self) -> FunctionArity { FunctionArity::Exact(1) }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> { Ok(DataType::Int32) }
    fn evaluate(&self, context: &FunctionContext) -> Result<FunctionResult> {
        if context.args.len() != 1 {
            return Err(anyhow::anyhow!("length requires exactly 1 argument"));
        }
        
        let string_array = context.args[0].as_any().downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("Argument must be String"))?;
        
        let mut result_values = Vec::with_capacity(string_array.len());
        for i in 0..string_array.len() {
            if string_array.is_null(i) {
                result_values.push(None);
            } else {
                let length = string_array.value(i).len() as i32;
                result_values.push(Some(length));
            }
        }
        
        let result_array = Int32Array::from(result_values);
        Ok(FunctionResult { result: Arc::new(result_array) })
    }
    fn supports_vectorization(&self) -> bool { true }
    fn supports_simd(&self) -> bool { true }
}

/// Convert to uppercase function
pub struct UpperFunction;

impl UpperFunction {
    pub fn new() -> Self {
        Self
    }
}

impl FunctionEvaluator for UpperFunction {
    fn name(&self) -> &str { "upper" }
    fn description(&self) -> &str { "Converts to uppercase" }
    fn arity(&self) -> FunctionArity { FunctionArity::Exact(1) }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn evaluate(&self, context: &FunctionContext) -> Result<FunctionResult> {
        if context.args.len() != 1 {
            return Err(anyhow::anyhow!("upper requires exactly 1 argument"));
        }
        
        let string_array = context.args[0].as_any().downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("Argument must be String"))?;
        
        let mut result_values = Vec::with_capacity(string_array.len());
        for i in 0..string_array.len() {
            if string_array.is_null(i) {
                result_values.push(None);
            } else {
                let upper = string_array.value(i).to_uppercase();
                result_values.push(Some(upper));
            }
        }
        
        let result_array = StringArray::from(result_values);
        Ok(FunctionResult { result: Arc::new(result_array) })
    }
    fn supports_vectorization(&self) -> bool { true }
    fn supports_simd(&self) -> bool { false }
}

/// Convert to lowercase function
pub struct LowerFunction;

impl LowerFunction {
    pub fn new() -> Self {
        Self
    }
}

impl FunctionEvaluator for LowerFunction {
    fn name(&self) -> &str { "lower" }
    fn description(&self) -> &str { "Converts to lowercase" }
    fn arity(&self) -> FunctionArity { FunctionArity::Exact(1) }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn evaluate(&self, context: &FunctionContext) -> Result<FunctionResult> {
        if context.args.len() != 1 {
            return Err(anyhow::anyhow!("lower requires exactly 1 argument"));
        }
        
        let string_array = context.args[0].as_any().downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("Argument must be String"))?;
        
        let mut result_values = Vec::with_capacity(string_array.len());
        for i in 0..string_array.len() {
            if string_array.is_null(i) {
                result_values.push(None);
            } else {
                let lower = string_array.value(i).to_lowercase();
                result_values.push(Some(lower));
            }
        }
        
        let result_array = StringArray::from(result_values);
        Ok(FunctionResult { result: Arc::new(result_array) })
    }
    fn supports_vectorization(&self) -> bool { true }
    fn supports_simd(&self) -> bool { false }
}

/// Trim whitespace function
pub struct TrimFunction;

impl TrimFunction {
    pub fn new() -> Self {
        Self
    }
}

impl FunctionEvaluator for TrimFunction {
    fn name(&self) -> &str { "trim" }
    fn description(&self) -> &str { "Trims leading and trailing whitespace" }
    fn arity(&self) -> FunctionArity { FunctionArity::Exact(1) }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn evaluate(&self, context: &FunctionContext) -> Result<FunctionResult> {
        if context.args.len() != 1 {
            return Err(anyhow::anyhow!("trim requires exactly 1 argument"));
        }
        
        let string_array = context.args[0].as_any().downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("Argument must be String"))?;
        
        let mut result_values = Vec::with_capacity(string_array.len());
        for i in 0..string_array.len() {
            if string_array.is_null(i) {
                result_values.push(None);
            } else {
                let trimmed = string_array.value(i).trim().to_string();
                result_values.push(Some(trimmed));
            }
        }
        
        let result_array = StringArray::from(result_values);
        Ok(FunctionResult { result: Arc::new(result_array) })
    }
    fn supports_vectorization(&self) -> bool { true }
    fn supports_simd(&self) -> bool { false }
}

/// String replacement function
pub struct ReplaceFunction;

impl ReplaceFunction {
    pub fn new() -> Self {
        Self
    }
}

impl FunctionEvaluator for ReplaceFunction {
    fn name(&self) -> &str { "replace" }
    fn description(&self) -> &str { "Replaces substring in string" }
    fn arity(&self) -> FunctionArity { FunctionArity::Exact(3) }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn evaluate(&self, context: &FunctionContext) -> Result<FunctionResult> {
        if context.args.len() != 3 {
            return Err(anyhow::anyhow!("replace requires exactly 3 arguments"));
        }
        
        let string_array = context.args[0].as_any().downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("First argument must be String"))?;
        let from_array = context.args[1].as_any().downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("Second argument must be String"))?;
        let to_array = context.args[2].as_any().downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("Third argument must be String"))?;
        
        let mut result_values = Vec::with_capacity(string_array.len());
        for i in 0..string_array.len() {
            if string_array.is_null(i) || from_array.is_null(i) || to_array.is_null(i) {
                result_values.push(None);
            } else {
                let string = string_array.value(i);
                let from = from_array.value(i);
                let to = to_array.value(i);
                
                let replaced = string.replace(from, to);
                result_values.push(Some(replaced));
            }
        }
        
        let result_array = StringArray::from(result_values);
        Ok(FunctionResult { result: Arc::new(result_array) })
    }
    fn supports_vectorization(&self) -> bool { true }
    fn supports_simd(&self) -> bool { false }
}

/// Regular expression matching function
pub struct RegexMatchFunction;

impl RegexMatchFunction {
    pub fn new() -> Self {
        Self
    }
}

impl FunctionEvaluator for RegexMatchFunction {
    fn name(&self) -> &str { "regex_match" }
    fn description(&self) -> &str { "Regular expression matching" }
    fn arity(&self) -> FunctionArity { FunctionArity::Exact(2) }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> { Ok(DataType::Boolean) }
    fn evaluate(&self, context: &FunctionContext) -> Result<FunctionResult> {
        if context.args.len() != 2 {
            return Err(anyhow::anyhow!("regex_match requires exactly 2 arguments"));
        }
        
        let string_array = context.args[0].as_any().downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("First argument must be String"))?;
        let pattern_array = context.args[1].as_any().downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("Second argument must be String"))?;
        
        let mut result_values = Vec::with_capacity(string_array.len());
        for i in 0..string_array.len() {
            if string_array.is_null(i) || pattern_array.is_null(i) {
                result_values.push(None);
            } else {
                let string = string_array.value(i);
                let pattern = pattern_array.value(i);
                
                match Regex::new(pattern) {
                    Ok(regex) => {
                        let matches = regex.is_match(string);
                        result_values.push(Some(matches));
                    },
                    Err(_) => {
                        // Regex compilation failed, return false
                        result_values.push(Some(false));
                    }
                }
            }
        }
        
        let result_array = BooleanArray::from(result_values);
        Ok(FunctionResult { result: Arc::new(result_array) })
    }
    fn supports_vectorization(&self) -> bool { true }
    fn supports_simd(&self) -> bool { false }
}