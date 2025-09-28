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

//! 日期时间函数实现
//! 
//! 提供日期时间处理函数

use super::{FunctionEvaluator, FunctionContext, FunctionResult, FunctionArity, FunctionStats};
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use datafusion_common::ScalarValue;
use anyhow::Result;
use std::sync::Arc;
use chrono::{DateTime, Utc, NaiveDate, NaiveTime, NaiveDateTime, Timelike, Datelike};

/// 当前日期函数
pub struct CurrentDateFunction;

impl CurrentDateFunction {
    pub fn new() -> Self {
        Self
    }
}

impl FunctionEvaluator for CurrentDateFunction {
    fn name(&self) -> &str { "current_date" }
    fn description(&self) -> &str { "返回当前日期" }
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

/// 当前时间函数
pub struct CurrentTimeFunction;

impl CurrentTimeFunction {
    pub fn new() -> Self {
        Self
    }
}

impl FunctionEvaluator for CurrentTimeFunction {
    fn name(&self) -> &str { "current_time" }
    fn description(&self) -> &str { "返回当前时间" }
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

/// 当前时间戳函数
pub struct CurrentTimestampFunction;

impl CurrentTimestampFunction {
    pub fn new() -> Self {
        Self
    }
}

impl FunctionEvaluator for CurrentTimestampFunction {
    fn name(&self) -> &str { "current_timestamp" }
    fn description(&self) -> &str { "返回当前时间戳" }
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

/// 日期加法函数
pub struct DateAddFunction;

impl DateAddFunction {
    pub fn new() -> Self {
        Self
    }
}

impl FunctionEvaluator for DateAddFunction {
    fn name(&self) -> &str { "date_add" }
    fn description(&self) -> &str { "日期加法" }
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

/// 日期减法函数
pub struct DateSubFunction;

impl DateSubFunction {
    pub fn new() -> Self {
        Self
    }
}

impl FunctionEvaluator for DateSubFunction {
    fn name(&self) -> &str { "date_sub" }
    fn description(&self) -> &str { "日期减法" }
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

/// 日期差值函数
pub struct DateDiffFunction;

impl DateDiffFunction {
    pub fn new() -> Self {
        Self
    }
}

impl FunctionEvaluator for DateDiffFunction {
    fn name(&self) -> &str { "date_diff" }
    fn description(&self) -> &str { "计算两个日期的差值（天数）" }
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

/// 提取日期时间部分函数
pub struct ExtractFunction;

impl ExtractFunction {
    pub fn new() -> Self {
        Self
    }
}

impl FunctionEvaluator for ExtractFunction {
    fn name(&self) -> &str { "extract" }
    fn description(&self) -> &str { "提取日期时间的指定部分" }
    fn arity(&self) -> FunctionArity { FunctionArity::Exact(2) }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> { Ok(DataType::Int32) }
    fn evaluate(&self, context: &FunctionContext) -> Result<FunctionResult> {
        if context.args.len() != 2 {
            return Err(anyhow::anyhow!("extract requires exactly 2 arguments"));
        }
        
        // 第一个参数是提取的部分（字符串），第二个参数是日期时间
        let part_array = context.args[0].as_any().downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("First argument must be String"))?;
        
        let mut result_values = Vec::with_capacity(part_array.len());
        
        // 根据第二个参数的类型进行处理
        if let Ok(date_array) = context.args[1].as_any().downcast_ref::<Date32Array>() {
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

/// 格式化日期函数
pub struct FormatDateFunction;

impl FormatDateFunction {
    pub fn new() -> Self {
        Self
    }
}

impl FunctionEvaluator for FormatDateFunction {
    fn name(&self) -> &str { "format_date" }
    fn description(&self) -> &str { "格式化日期" }
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
                    _ => date.format("%Y-%m-%d").to_string(), // 默认格式
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
