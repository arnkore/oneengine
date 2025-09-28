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

//! Date and time functions implementation
//! 
//! Provides date and time processing functions

use super::{FunctionEvaluator, FunctionContext, FunctionResult, FunctionArity, FunctionStats};
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use datafusion_common::ScalarValue;
use anyhow::Result;
use std::sync::Arc;
use chrono::{DateTime, Utc, NaiveDate, NaiveTime, NaiveDateTime, Timelike, Datelike};

/// Current date function
pub struct CurrentDateFunction;

impl CurrentDateFunction {
    pub fn new() -> Self {
        Self
    }
}

impl FunctionEvaluator for CurrentDateFunction {
    fn name(&self) -> &str { "current_date" }
    fn description(&self) -> &str { "Returns current date" }
    fn arity(&self) -> FunctionArity { FunctionArity::Exact(0) }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> { Ok(DataType::Date32) }
    fn evaluate(&self, context: &FunctionContext) -> Result<FunctionResult> {
        let now = Utc::now().date_naive();
        let days_since_epoch = now.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32;
        let array = Date32Array::from(vec![days_since_epoch; context.batch.num_rows()]);
        Ok(FunctionResult { result: Arc::new(array) })
    }
    fn supports_vectorization(&self) -> bool { true }
    fn supports_simd(&self) -> bool { true }
}

/// Current time function
pub struct CurrentTimeFunction;

impl CurrentTimeFunction {
    pub fn new() -> Self {
        Self
    }
}

impl FunctionEvaluator for CurrentTimeFunction {
    fn name(&self) -> &str { "current_time" }
    fn description(&self) -> &str { "Returns current time" }
    fn arity(&self) -> FunctionArity { FunctionArity::Exact(0) }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> { Ok(DataType::Time64(TimeUnit::Microsecond)) }
    fn evaluate(&self, context: &FunctionContext) -> Result<FunctionResult> {
        let now = Utc::now().time();
        let microseconds = now.num_seconds_from_midnight() as i64 * 1_000_000 + now.nanosecond() as i64 / 1000;
        let array = Time64MicrosecondArray::from(vec![microseconds; context.batch.num_rows()]);
        Ok(FunctionResult { result: Arc::new(array) })
    }
    fn supports_vectorization(&self) -> bool { true }
    fn supports_simd(&self) -> bool { true }
}

/// Current timestamp function
pub struct CurrentTimestampFunction;

impl CurrentTimestampFunction {
    pub fn new() -> Self {
        Self
    }
}

impl FunctionEvaluator for CurrentTimestampFunction {
    fn name(&self) -> &str { "current_timestamp" }
    fn description(&self) -> &str { "Returns current timestamp" }
    fn arity(&self) -> FunctionArity { FunctionArity::Exact(0) }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> { Ok(DataType::Timestamp(TimeUnit::Microsecond, None)) }
    fn evaluate(&self, context: &FunctionContext) -> Result<FunctionResult> {
        let now = Utc::now();
        let microseconds = now.timestamp_micros();
        let array = TimestampMicrosecondArray::from(vec![microseconds; context.batch.num_rows()]);
        Ok(FunctionResult { result: Arc::new(array) })
    }
    fn supports_vectorization(&self) -> bool { true }
    fn supports_simd(&self) -> bool { true }
}

/// Date addition function
pub struct DateAddFunction;

impl DateAddFunction {
    pub fn new() -> Self {
        Self
    }
}

impl FunctionEvaluator for DateAddFunction {
    fn name(&self) -> &str { "date_add" }
    fn description(&self) -> &str { "Adds days to a date" }
    fn arity(&self) -> FunctionArity { FunctionArity::Exact(2) }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> { Ok(DataType::Date32) }
    fn evaluate(&self, context: &FunctionContext) -> Result<FunctionResult> {
        if context.args.len() != 2 {
            return Err(anyhow::anyhow!("date_add requires exactly 2 arguments"));
        }
        
        let date_array = context.args[0].as_any().downcast_ref::<Date32Array>()
            .ok_or_else(|| anyhow::anyhow!("First argument must be Date32"))?;
        let days_array = context.args[1].as_any().downcast_ref::<Int32Array>()
            .ok_or_else(|| anyhow::anyhow!("Second argument must be Int32"))?;
        
        let mut result_values = Vec::with_capacity(date_array.len());
        for i in 0..date_array.len() {
            if date_array.is_null(i) || days_array.is_null(i) {
                result_values.push(None);
            } else {
                let date_days = date_array.value(i);
                let days_to_add = days_array.value(i);
                result_values.push(Some(date_days + days_to_add));
            }
        }
        
        let result_array = Date32Array::from(result_values);
        Ok(FunctionResult { result: Arc::new(result_array) })
    }
    fn supports_vectorization(&self) -> bool { true }
    fn supports_simd(&self) -> bool { true }
}

/// Date subtraction function
pub struct DateSubFunction;

impl DateSubFunction {
    pub fn new() -> Self {
        Self
    }
}

impl FunctionEvaluator for DateSubFunction {
    fn name(&self) -> &str { "date_sub" }
    fn description(&self) -> &str { "Subtracts days from a date" }
    fn arity(&self) -> FunctionArity { FunctionArity::Exact(2) }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> { Ok(DataType::Date32) }
    fn evaluate(&self, context: &FunctionContext) -> Result<FunctionResult> {
        if context.args.len() != 2 {
            return Err(anyhow::anyhow!("date_sub requires exactly 2 arguments"));
        }
        
        let date_array = context.args[0].as_any().downcast_ref::<Date32Array>()
            .ok_or_else(|| anyhow::anyhow!("First argument must be Date32"))?;
        let days_array = context.args[1].as_any().downcast_ref::<Int32Array>()
            .ok_or_else(|| anyhow::anyhow!("Second argument must be Int32"))?;
        
        let mut result_values = Vec::with_capacity(date_array.len());
        for i in 0..date_array.len() {
            if date_array.is_null(i) || days_array.is_null(i) {
                result_values.push(None);
            } else {
                let date_days = date_array.value(i);
                let days_to_sub = days_array.value(i);
                result_values.push(Some(date_days - days_to_sub));
            }
        }
        
        let result_array = Date32Array::from(result_values);
        Ok(FunctionResult { result: Arc::new(result_array) })
    }
    fn supports_vectorization(&self) -> bool { true }
    fn supports_simd(&self) -> bool { true }
}

/// Date difference function
pub struct DateDiffFunction;

impl DateDiffFunction {
    pub fn new() -> Self {
        Self
    }
}

impl FunctionEvaluator for DateDiffFunction {
    fn name(&self) -> &str { "date_diff" }
    fn description(&self) -> &str { "Calculates difference between two dates in days" }
    fn arity(&self) -> FunctionArity { FunctionArity::Exact(2) }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> { Ok(DataType::Int32) }
    fn evaluate(&self, context: &FunctionContext) -> Result<FunctionResult> {
        if context.args.len() != 2 {
            return Err(anyhow::anyhow!("date_diff requires exactly 2 arguments"));
        }
        
        let date1_array = context.args[0].as_any().downcast_ref::<Date32Array>()
            .ok_or_else(|| anyhow::anyhow!("First argument must be Date32"))?;
        let date2_array = context.args[1].as_any().downcast_ref::<Date32Array>()
            .ok_or_else(|| anyhow::anyhow!("Second argument must be Date32"))?;
        
        let mut result_values = Vec::with_capacity(date1_array.len());
        for i in 0..date1_array.len() {
            if date1_array.is_null(i) || date2_array.is_null(i) {
                result_values.push(None);
            } else {
                let date1_days = date1_array.value(i);
                let date2_days = date2_array.value(i);
                result_values.push(Some(date2_days - date1_days));
            }
        }
        
        let result_array = Int32Array::from(result_values);
        Ok(FunctionResult { result: Arc::new(result_array) })
    }
    fn supports_vectorization(&self) -> bool { true }
    fn supports_simd(&self) -> bool { true }
}

/// Extract date/time part function
pub struct ExtractFunction;

impl ExtractFunction {
    pub fn new() -> Self {
        Self
    }
}

impl FunctionEvaluator for ExtractFunction {
    fn name(&self) -> &str { "extract" }
    fn description(&self) -> &str { "Extracts specified part from date/time" }
    fn arity(&self) -> FunctionArity { FunctionArity::Exact(2) }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> { Ok(DataType::Int32) }
    fn evaluate(&self, context: &FunctionContext) -> Result<FunctionResult> {
        if context.args.len() != 2 {
            return Err(anyhow::anyhow!("extract requires exactly 2 arguments"));
        }
        
        // First argument is the part to extract (string), second is date/time
        let part_array = context.args[0].as_any().downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("First argument must be String"))?;
        
        let mut result_values = Vec::with_capacity(part_array.len());
        
        // Process based on the type of second argument
        if let Some(date_array) = context.args[1].as_any().downcast_ref::<Date32Array>() {
            for i in 0..part_array.len() {
                if part_array.is_null(i) || date_array.is_null(i) {
                    result_values.push(None);
                } else {
                    let part = part_array.value(i);
                    let date_days = date_array.value(i);
                    let date = NaiveDate::from_num_days_from_ce_opt(date_days as i32 + 719163)
                        .ok_or_else(|| anyhow::anyhow!("Invalid date"))?;
                    
                    let value = match part {
                        "year" => date.year() as i32,
                        "month" => date.month() as i32,
                        "day" => date.day() as i32,
                        "dow" => date.weekday().num_days_from_sunday() as i32,
                        "doy" => date.ordinal() as i32,
                        _ => return Err(anyhow::anyhow!("Unsupported date part: {}", part)),
                    };
                    result_values.push(Some(value));
                }
            }
        } else {
            return Err(anyhow::anyhow!("Second argument must be Date32"));
        }
        
        let result_array = Int32Array::from(result_values);
        Ok(FunctionResult { result: Arc::new(result_array) })
    }
    fn supports_vectorization(&self) -> bool { true }
    fn supports_simd(&self) -> bool { false }
}

/// Format date function
pub struct FormatDateFunction;

impl FormatDateFunction {
    pub fn new() -> Self {
        Self
    }
}

impl FunctionEvaluator for FormatDateFunction {
    fn name(&self) -> &str { "format_date" }
    fn description(&self) -> &str { "Formats a date" }
    fn arity(&self) -> FunctionArity { FunctionArity::Exact(2) }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn evaluate(&self, context: &FunctionContext) -> Result<FunctionResult> {
        if context.args.len() != 2 {
            return Err(anyhow::anyhow!("format_date requires exactly 2 arguments"));
        }
        
        let format_array = context.args[0].as_any().downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("First argument must be String"))?;
        let date_array = context.args[1].as_any().downcast_ref::<Date32Array>()
            .ok_or_else(|| anyhow::anyhow!("Second argument must be Date32"))?;
        
        let mut result_values = Vec::with_capacity(format_array.len());
        for i in 0..format_array.len() {
            if format_array.is_null(i) || date_array.is_null(i) {
                result_values.push(None);
            } else {
                let format = format_array.value(i);
                let date_days = date_array.value(i);
                let date = NaiveDate::from_num_days_from_ce_opt(date_days as i32 + 719163)
                    .ok_or_else(|| anyhow::anyhow!("Invalid date"))?;
                
                let formatted = match format {
                    "YYYY-MM-DD" => date.format("%Y-%m-%d").to_string(),
                    "MM/DD/YYYY" => date.format("%m/%d/%Y").to_string(),
                    "DD-MM-YYYY" => date.format("%d-%m-%Y").to_string(),
                    _ => date.format("%Y-%m-%d").to_string(), // Default format
                };
                result_values.push(Some(formatted));
            }
        }
        
        let result_array = StringArray::from(result_values);
        Ok(FunctionResult { result: Arc::new(result_array) })
    }
    fn supports_vectorization(&self) -> bool { true }
    fn supports_simd(&self) -> bool { false }
}
