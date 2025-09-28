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

//! 表达式类型系统
//! 
//! 提供完整的类型定义和类型推断功能

use std::fmt;

/// 数据类型定义
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DataType {
    /// 空类型
    Null,
    /// 布尔类型
    Boolean,
    /// 整数类型
    Int8,
    Int16,
    Int32,
    Int64,
    /// 无符号整数类型
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    /// 浮点类型
    Float32,
    Float64,
    /// 字符串类型
    String,
    /// 二进制类型
    Binary,
    /// 日期时间类型
    Date,
    Time,
    Timestamp,
    Interval,
    /// 数组类型
    Array(Box<DataType>),
    /// 结构体类型
    Struct(Vec<StructField>),
    /// 映射类型
    Map(Box<DataType>, Box<DataType>),
    /// 联合类型
    Union(Vec<DataType>),
    /// 用户定义类型
    Custom(String),
}

impl DataType {
    /// 获取类型的字节大小
    pub fn size(&self) -> Option<usize> {
        match self {
            DataType::Null => Some(0),
            DataType::Boolean => Some(1),
            DataType::Int8 | DataType::UInt8 => Some(1),
            DataType::Int16 | DataType::UInt16 => Some(2),
            DataType::Int32 | DataType::UInt32 => Some(4),
            DataType::Int64 | DataType::UInt64 => Some(8),
            DataType::Float32 => Some(4),
            DataType::Float64 => Some(8),
            DataType::Date => Some(4),
            DataType::Time => Some(8),
            DataType::Timestamp => Some(8),
            DataType::Interval => Some(8),
            DataType::String | DataType::Binary => None, // 变长类型
            DataType::Array(_) | DataType::Struct(_) | DataType::Map(_, _) | DataType::Union(_) => None,
            DataType::Custom(_) => None,
        }
    }

    /// 检查类型是否可比较
    pub fn is_comparable(&self) -> bool {
        match self {
            DataType::Null | DataType::Boolean => true,
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => true,
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => true,
            DataType::Float32 | DataType::Float64 => true,
            DataType::String | DataType::Binary => true,
            DataType::Date | DataType::Time | DataType::Timestamp | DataType::Interval => true,
            DataType::Array(element_type) => element_type.is_comparable(),
            DataType::Struct(fields) => fields.iter().all(|f| f.data_type.is_comparable()),
            DataType::Map(key_type, value_type) => key_type.is_comparable() && value_type.is_comparable(),
            DataType::Union(types) => types.iter().all(|t| t.is_comparable()),
            DataType::Custom(_) => false, // 用户定义类型需要特殊处理
        }
    }

    /// 检查类型是否可排序
    pub fn is_sortable(&self) -> bool {
        self.is_comparable()
    }

    /// 检查类型是否可聚合
    pub fn is_aggregatable(&self) -> bool {
        match self {
            DataType::Null | DataType::Boolean => false,
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => true,
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => true,
            DataType::Float32 | DataType::Float64 => true,
            DataType::String | DataType::Binary => false,
            DataType::Date | DataType::Time | DataType::Timestamp | DataType::Interval => true,
            DataType::Array(_) | DataType::Struct(_) | DataType::Map(_, _) | DataType::Union(_) => false,
            DataType::Custom(_) => false,
        }
    }

    /// 获取公共类型（用于类型推断）
    pub fn common_type(&self, other: &DataType) -> Option<DataType> {
        match (self, other) {
            // 相同类型
            (a, b) if a == b => Some(a.clone()),
            
            // 数值类型提升
            (DataType::Int8, DataType::Int16) | (DataType::Int16, DataType::Int8) => Some(DataType::Int16),
            (DataType::Int8, DataType::Int32) | (DataType::Int32, DataType::Int8) => Some(DataType::Int32),
            (DataType::Int8, DataType::Int64) | (DataType::Int64, DataType::Int8) => Some(DataType::Int64),
            (DataType::Int16, DataType::Int32) | (DataType::Int32, DataType::Int16) => Some(DataType::Int32),
            (DataType::Int16, DataType::Int64) | (DataType::Int64, DataType::Int16) => Some(DataType::Int64),
            (DataType::Int32, DataType::Int64) | (DataType::Int64, DataType::Int32) => Some(DataType::Int64),
            
            // 整数到浮点
            (DataType::Int8, DataType::Float32) | (DataType::Float32, DataType::Int8) => Some(DataType::Float32),
            (DataType::Int16, DataType::Float32) | (DataType::Float32, DataType::Int16) => Some(DataType::Float32),
            (DataType::Int32, DataType::Float32) | (DataType::Float32, DataType::Int32) => Some(DataType::Float32),
            (DataType::Int64, DataType::Float32) | (DataType::Float32, DataType::Int64) => Some(DataType::Float32),
            (DataType::Int8, DataType::Float64) | (DataType::Float64, DataType::Int8) => Some(DataType::Float64),
            (DataType::Int16, DataType::Float64) | (DataType::Float64, DataType::Int16) => Some(DataType::Float64),
            (DataType::Int32, DataType::Float64) | (DataType::Float64, DataType::Int32) => Some(DataType::Float64),
            (DataType::Int64, DataType::Float64) | (DataType::Float64, DataType::Int64) => Some(DataType::Float64),
            (DataType::Float32, DataType::Float64) | (DataType::Float64, DataType::Float32) => Some(DataType::Float64),
            
            // 无符号整数类型
            (DataType::UInt8, DataType::UInt16) | (DataType::UInt16, DataType::UInt8) => Some(DataType::UInt16),
            (DataType::UInt8, DataType::UInt32) | (DataType::UInt32, DataType::UInt8) => Some(DataType::UInt32),
            (DataType::UInt8, DataType::UInt64) | (DataType::UInt64, DataType::UInt8) => Some(DataType::UInt64),
            (DataType::UInt16, DataType::UInt32) | (DataType::UInt32, DataType::UInt16) => Some(DataType::UInt32),
            (DataType::UInt16, DataType::UInt64) | (DataType::UInt64, DataType::UInt16) => Some(DataType::UInt64),
            (DataType::UInt32, DataType::UInt64) | (DataType::UInt64, DataType::UInt32) => Some(DataType::UInt64),
            
            // 数组类型
            (DataType::Array(left_element), DataType::Array(right_element)) => {
                if let Some(common_element) = left_element.common_type(right_element) {
                    Some(DataType::Array(Box::new(common_element)))
                } else {
                    None
                }
            }
            
            // 结构体类型
            (DataType::Struct(left_fields), DataType::Struct(right_fields)) => {
                if left_fields.len() != right_fields.len() {
                    return None;
                }
                
                let mut common_fields = Vec::new();
                for (left_field, right_field) in left_fields.iter().zip(right_fields.iter()) {
                    if left_field.name != right_field.name {
                        return None;
                    }
                    if let Some(common_type) = left_field.data_type.common_type(&right_field.data_type) {
                        common_fields.push(StructField {
                            name: left_field.name.clone(),
                            data_type: common_type,
                        });
                    } else {
                        return None;
                    }
                }
                Some(DataType::Struct(common_fields))
            }
            
            // 其他情况不兼容
            _ => None,
        }
    }

    /// 检查类型是否可以隐式转换
    pub fn can_implicitly_cast_to(&self, target: &DataType) -> bool {
        match (self, target) {
            // 相同类型
            (a, b) if a == b => true,
            
            // 数值类型转换
            (DataType::Int8, DataType::Int16) => true,
            (DataType::Int8, DataType::Int32) => true,
            (DataType::Int8, DataType::Int64) => true,
            (DataType::Int16, DataType::Int32) => true,
            (DataType::Int16, DataType::Int64) => true,
            (DataType::Int32, DataType::Int64) => true,
            
            // 整数到浮点
            (DataType::Int8, DataType::Float32) => true,
            (DataType::Int16, DataType::Float32) => true,
            (DataType::Int32, DataType::Float32) => true,
            (DataType::Int64, DataType::Float32) => true,
            (DataType::Int8, DataType::Float64) => true,
            (DataType::Int16, DataType::Float64) => true,
            (DataType::Int32, DataType::Float64) => true,
            (DataType::Int64, DataType::Float64) => true,
            (DataType::Float32, DataType::Float64) => true,
            
            // 无符号整数转换
            (DataType::UInt8, DataType::UInt16) => true,
            (DataType::UInt8, DataType::UInt32) => true,
            (DataType::UInt8, DataType::UInt64) => true,
            (DataType::UInt16, DataType::UInt32) => true,
            (DataType::UInt16, DataType::UInt64) => true,
            (DataType::UInt32, DataType::UInt64) => true,
            
            // 字符串转换
            (DataType::String, DataType::Binary) => true,
            
            // 日期时间转换
            (DataType::Date, DataType::Timestamp) => true,
            (DataType::Time, DataType::Timestamp) => true,
            
            _ => false,
        }
    }

    /// 获取类型的默认值
    pub fn default_value(&self) -> Option<DefaultValue> {
        match self {
            DataType::Null => Some(DefaultValue::Null),
            DataType::Boolean => Some(DefaultValue::Boolean(false)),
            DataType::Int8 => Some(DefaultValue::Int8(0)),
            DataType::Int16 => Some(DefaultValue::Int16(0)),
            DataType::Int32 => Some(DefaultValue::Int32(0)),
            DataType::Int64 => Some(DefaultValue::Int64(0)),
            DataType::UInt8 => Some(DefaultValue::UInt8(0)),
            DataType::UInt16 => Some(DefaultValue::UInt16(0)),
            DataType::UInt32 => Some(DefaultValue::UInt32(0)),
            DataType::UInt64 => Some(DefaultValue::UInt64(0)),
            DataType::Float32 => Some(DefaultValue::Float32(0.0)),
            DataType::Float64 => Some(DefaultValue::Float64(0.0)),
            DataType::String => Some(DefaultValue::String(String::new())),
            DataType::Binary => Some(DefaultValue::Binary(Vec::new())),
            DataType::Date => Some(DefaultValue::Date(0)),
            DataType::Time => Some(DefaultValue::Time(0)),
            DataType::Timestamp => Some(DefaultValue::Timestamp(0)),
            DataType::Interval => Some(DefaultValue::Interval(0)),
            DataType::Array(element_type) => {
                element_type.default_value().map(|_| DefaultValue::Array(Vec::new()))
            }
            DataType::Struct(fields) => {
                let mut field_values = Vec::new();
                for field in fields {
                    if let Some(value) = field.data_type.default_value() {
                        field_values.push((field.name.clone(), value));
                    } else {
                        return None;
                    }
                }
                Some(DefaultValue::Struct(field_values))
            }
            DataType::Map(key_type, value_type) => {
                if key_type.default_value().is_some() && value_type.default_value().is_some() {
                    Some(DefaultValue::Map(Vec::new()))
                } else {
                    None
                }
            }
            DataType::Union(_) | DataType::Custom(_) => None,
        }
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Null => write!(f, "NULL"),
            DataType::Boolean => write!(f, "BOOLEAN"),
            DataType::Int8 => write!(f, "TINYINT"),
            DataType::Int16 => write!(f, "SMALLINT"),
            DataType::Int32 => write!(f, "INTEGER"),
            DataType::Int64 => write!(f, "BIGINT"),
            DataType::UInt8 => write!(f, "TINYINT UNSIGNED"),
            DataType::UInt16 => write!(f, "SMALLINT UNSIGNED"),
            DataType::UInt32 => write!(f, "INTEGER UNSIGNED"),
            DataType::UInt64 => write!(f, "BIGINT UNSIGNED"),
            DataType::Float32 => write!(f, "FLOAT"),
            DataType::Float64 => write!(f, "DOUBLE"),
            DataType::String => write!(f, "VARCHAR"),
            DataType::Binary => write!(f, "VARBINARY"),
            DataType::Date => write!(f, "DATE"),
            DataType::Time => write!(f, "TIME"),
            DataType::Timestamp => write!(f, "TIMESTAMP"),
            DataType::Interval => write!(f, "INTERVAL"),
            DataType::Array(element_type) => write!(f, "ARRAY<{}>", element_type),
            DataType::Struct(fields) => {
                write!(f, "STRUCT<");
                for (i, field) in fields.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}: {}", field.name, field.data_type)?;
                }
                write!(f, ">")
            }
            DataType::Map(key_type, value_type) => write!(f, "MAP<{}, {}>", key_type, value_type),
            DataType::Union(types) => {
                write!(f, "UNION<");
                for (i, t) in types.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", t)?;
                }
                write!(f, ">")
            }
            DataType::Custom(name) => write!(f, "{}", name),
        }
    }
}

/// 结构体字段
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StructField {
    pub name: String,
    pub data_type: DataType,
}

/// 默认值
#[derive(Debug, Clone, PartialEq)]
pub enum DefaultValue {
    Null,
    Boolean(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Float32(f32),
    Float64(f64),
    String(String),
    Binary(Vec<u8>),
    Date(i32),
    Time(i64),
    Timestamp(i64),
    Interval(i64),
    Array(Vec<DefaultValue>),
    Struct(Vec<(String, DefaultValue)>),
    Map(Vec<(DefaultValue, DefaultValue)>),
}

/// 类型推断器
pub struct TypeInferencer {
    /// 已知的列类型映射
    column_types: std::collections::HashMap<String, DataType>,
}

impl TypeInferencer {
    /// 创建新的类型推断器
    pub fn new() -> Self {
        Self {
            column_types: std::collections::HashMap::new(),
        }
    }

    /// 添加列类型信息
    pub fn add_column_type(&mut self, name: String, data_type: DataType) {
        self.column_types.insert(name, data_type);
    }

    /// 推断表达式类型
    pub fn infer_type(&self, expr: &crate::expression::ast::expression::Expression) -> Result<DataType, TypeInferenceError> {
        use crate::expression::ast::expression::Expression;
        
        match expr {
            Expression::Column(col) => {
                self.column_types.get(&col.name)
                    .cloned()
                    .ok_or_else(|| TypeInferenceError::UnknownColumn(col.name.clone()))
            }
            Expression::Literal(lit) => Ok(lit.data_type.clone()),
            Expression::Arithmetic(expr) => {
                let left_type = self.infer_type(&expr.left)?;
                let right_type = self.infer_type(&expr.right)?;
                left_type.common_type(&right_type)
                    .ok_or_else(|| TypeInferenceError::IncompatibleTypes(left_type, right_type))
            }
            Expression::Comparison(_) => Ok(DataType::Boolean),
            Expression::Logical(_) => Ok(DataType::Boolean),
            Expression::Function(func) => Ok(func.return_type.clone()),
            Expression::Case(expr) => {
                let then_type = self.infer_type(&expr.then_expr)?;
                let else_type = self.infer_type(&expr.else_expr)?;
                then_type.common_type(&else_type)
                    .ok_or_else(|| TypeInferenceError::IncompatibleTypes(then_type, else_type))
            }
            Expression::Cast(expr) => Ok(expr.target_type.clone()),
            Expression::Subquery(expr) => Ok(expr.return_type.clone()),
            Expression::Aggregate(expr) => Ok(expr.return_type.clone()),
            Expression::Window(expr) => Ok(expr.return_type.clone()),
            Expression::Array(expr) => Ok(expr.element_type.clone()),
            Expression::Struct(expr) => Ok(expr.struct_type.clone()),
        }
    }
}

/// 类型推断错误
#[derive(Debug, Clone, PartialEq)]
pub enum TypeInferenceError {
    UnknownColumn(String),
    IncompatibleTypes(DataType, DataType),
    CircularReference(String),
    InvalidCast(DataType, DataType),
}
