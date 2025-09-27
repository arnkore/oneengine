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


use crate::columnar::types::{DataType, Bitmap, Encoding};
use anyhow::Result;
use std::fmt;
use std::sync::Arc;

/// A columnar data structure optimized for vectorized execution
#[derive(Debug, Clone)]
pub struct Column {
    pub data_type: DataType,
    pub data: AnyArray,
    pub nulls: Option<Bitmap>,
    pub encoding: Encoding,
    pub len: usize,
}

/// Type-erased array data
#[derive(Debug, Clone)]
pub enum AnyArray {
    Boolean(Vec<bool>),
    Int8(Vec<i8>),
    Int16(Vec<i16>),
    Int32(Vec<i32>),
    Int64(Vec<i64>),
    UInt8(Vec<u8>),
    UInt16(Vec<u16>),
    UInt32(Vec<u32>),
    UInt64(Vec<u64>),
    Float32(Vec<f32>),
    Float64(Vec<f64>),
    Utf8(Vec<String>),
    Binary(Vec<Vec<u8>>),
    Date32(Vec<i32>),
    Date64(Vec<i64>),
    Timestamp(Vec<i64>),
    Decimal(Vec<i128>, u8, u8), // data, precision, scale
}

impl Column {
    /// Create a new column with the specified data type and length
    pub fn new(data_type: DataType, len: usize) -> Self {
        let data = match data_type {
            DataType::Boolean => AnyArray::Boolean(vec![false; len]),
            DataType::Int8 => AnyArray::Int8(vec![0; len]),
            DataType::Int16 => AnyArray::Int16(vec![0; len]),
            DataType::Int32 => AnyArray::Int32(vec![0; len]),
            DataType::Int64 => AnyArray::Int64(vec![0; len]),
            DataType::UInt8 => AnyArray::UInt8(vec![0; len]),
            DataType::UInt16 => AnyArray::UInt16(vec![0; len]),
            DataType::UInt32 => AnyArray::UInt32(vec![0; len]),
            DataType::UInt64 => AnyArray::UInt64(vec![0; len]),
            DataType::Float32 => AnyArray::Float32(vec![0.0; len]),
            DataType::Float64 => AnyArray::Float64(vec![0.0; len]),
            DataType::Utf8 => AnyArray::Utf8(vec![String::new(); len]),
            DataType::Binary => AnyArray::Binary(vec![Vec::new(); len]),
            DataType::Date32 => AnyArray::Date32(vec![0; len]),
            DataType::Date64 => AnyArray::Date64(vec![0; len]),
            DataType::Timestamp => AnyArray::Timestamp(vec![0; len]),
            DataType::Decimal(precision, scale) => AnyArray::Decimal(vec![0; len], precision, scale),
        };

        Self {
            data_type,
            data,
            nulls: Some(Bitmap::new(len)),
            encoding: Encoding::Plain,
            len,
        }
    }

    /// Create a column from existing data
    pub fn from_data(data_type: DataType, data: AnyArray, nulls: Option<Bitmap>) -> Result<Self> {
        let len = match &data {
            AnyArray::Boolean(v) => v.len(),
            AnyArray::Int8(v) => v.len(),
            AnyArray::Int16(v) => v.len(),
            AnyArray::Int32(v) => v.len(),
            AnyArray::Int64(v) => v.len(),
            AnyArray::UInt8(v) => v.len(),
            AnyArray::UInt16(v) => v.len(),
            AnyArray::UInt32(v) => v.len(),
            AnyArray::UInt64(v) => v.len(),
            AnyArray::Float32(v) => v.len(),
            AnyArray::Float64(v) => v.len(),
            AnyArray::Utf8(v) => v.len(),
            AnyArray::Binary(v) => v.len(),
            AnyArray::Date32(v) => v.len(),
            AnyArray::Date64(v) => v.len(),
            AnyArray::Timestamp(v) => v.len(),
            AnyArray::Decimal(v, _, _) => v.len(),
        };

        // Validate nulls bitmap length
        if let Some(ref nulls) = nulls {
            if nulls.len() != len {
                return Err(anyhow::anyhow!("Nulls bitmap length {} doesn't match data length {}", nulls.len(), len));
            }
        }

        Ok(Self {
            data_type,
            data,
            nulls,
            encoding: Encoding::Plain,
            len,
        })
    }

    /// Get the length of the column
    pub fn len(&self) -> usize {
        self.len
    }

    /// Check if the column is empty
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Check if a value at the specified index is null
    pub fn is_null(&self, index: usize) -> bool {
        if index >= self.len {
            return true;
        }
        self.nulls.as_ref().map_or(false, |nulls| nulls.get(index))
    }

    /// Get the number of null values
    pub fn null_count(&self) -> usize {
        self.nulls.as_ref().map_or(0, |nulls| nulls.count_ones())
    }

    /// Get the number of non-null values
    pub fn non_null_count(&self) -> usize {
        self.len - self.null_count()
    }

    /// Slice the column to get a subset
    pub fn slice(&self, offset: usize, length: usize) -> Result<Self> {
        if offset + length > self.len {
            return Err(anyhow::anyhow!("Slice out of bounds"));
        }

        let data = match &self.data {
            AnyArray::Boolean(v) => AnyArray::Boolean(v[offset..offset + length].to_vec()),
            AnyArray::Int8(v) => AnyArray::Int8(v[offset..offset + length].to_vec()),
            AnyArray::Int16(v) => AnyArray::Int16(v[offset..offset + length].to_vec()),
            AnyArray::Int32(v) => AnyArray::Int32(v[offset..offset + length].to_vec()),
            AnyArray::Int64(v) => AnyArray::Int64(v[offset..offset + length].to_vec()),
            AnyArray::UInt8(v) => AnyArray::UInt8(v[offset..offset + length].to_vec()),
            AnyArray::UInt16(v) => AnyArray::UInt16(v[offset..offset + length].to_vec()),
            AnyArray::UInt32(v) => AnyArray::UInt32(v[offset..offset + length].to_vec()),
            AnyArray::UInt64(v) => AnyArray::UInt64(v[offset..offset + length].to_vec()),
            AnyArray::Float32(v) => AnyArray::Float32(v[offset..offset + length].to_vec()),
            AnyArray::Float64(v) => AnyArray::Float64(v[offset..offset + length].to_vec()),
            AnyArray::Utf8(v) => AnyArray::Utf8(v[offset..offset + length].to_vec()),
            AnyArray::Binary(v) => AnyArray::Binary(v[offset..offset + length].to_vec()),
            AnyArray::Date32(v) => AnyArray::Date32(v[offset..offset + length].to_vec()),
            AnyArray::Date64(v) => AnyArray::Date64(v[offset..offset + length].to_vec()),
            AnyArray::Timestamp(v) => AnyArray::Timestamp(v[offset..offset + length].to_vec()),
            AnyArray::Decimal(v, precision, scale) => {
                AnyArray::Decimal(v[offset..offset + length].to_vec(), *precision, *scale)
            }
        };

        let nulls = self.nulls.as_ref().map(|nulls| {
            let mut new_null_bits = Vec::new();
            let chunks = (length + 63) / 64;
            for i in 0..chunks {
                let chunk_offset = offset + i * 64;
                let chunk_length = std::cmp::min(64, length - i * 64);
                let mut chunk = 0u64;
                for j in 0..chunk_length {
                    if nulls.get(chunk_offset + j) {
                        chunk |= 1 << j;
                    }
                }
                new_null_bits.push(chunk);
            }
            Bitmap::from_bits(new_null_bits, length)
        });

        Ok(Self {
            data_type: self.data_type,
            data,
            nulls,
            encoding: self.encoding,
            len: length,
        })
    }

    /// Get the memory usage of the column in bytes
    pub fn memory_usage(&self) -> usize {
        let data_size = match &self.data {
            AnyArray::Boolean(v) => v.len() * std::mem::size_of::<bool>(),
            AnyArray::Int8(v) => v.len() * std::mem::size_of::<i8>(),
            AnyArray::Int16(v) => v.len() * std::mem::size_of::<i16>(),
            AnyArray::Int32(v) => v.len() * std::mem::size_of::<i32>(),
            AnyArray::Int64(v) => v.len() * std::mem::size_of::<i64>(),
            AnyArray::UInt8(v) => v.len() * std::mem::size_of::<u8>(),
            AnyArray::UInt16(v) => v.len() * std::mem::size_of::<u16>(),
            AnyArray::UInt32(v) => v.len() * std::mem::size_of::<u32>(),
            AnyArray::UInt64(v) => v.len() * std::mem::size_of::<u64>(),
            AnyArray::Float32(v) => v.len() * std::mem::size_of::<f32>(),
            AnyArray::Float64(v) => v.len() * std::mem::size_of::<f64>(),
            AnyArray::Utf8(v) => v.iter().map(|s| s.len()).sum::<usize>() + v.len() * std::mem::size_of::<String>(),
            AnyArray::Binary(v) => v.iter().map(|b| b.len()).sum::<usize>() + v.len() * std::mem::size_of::<Vec<u8>>(),
            AnyArray::Date32(v) => v.len() * std::mem::size_of::<i32>(),
            AnyArray::Date64(v) => v.len() * std::mem::size_of::<i64>(),
            AnyArray::Timestamp(v) => v.len() * std::mem::size_of::<i64>(),
            AnyArray::Decimal(v, _, _) => v.len() * std::mem::size_of::<i128>(),
        };

        let nulls_size = self.nulls.as_ref().map_or(0, |nulls| {
            nulls.as_slice().len() * std::mem::size_of::<u64>()
        });

        data_size + nulls_size
    }
}

impl fmt::Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Column({}, len={}, nulls={})", self.data_type, self.len, self.null_count())
    }
}
