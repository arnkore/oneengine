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
//! 使用Arrow的数据类型系统，提供完整的类型定义和类型推断功能

use std::fmt;
use arrow::datatypes::DataType as ArrowDataType;

/// 使用Arrow的数据类型
pub type DataType = ArrowDataType;

/// DataType扩展trait
pub trait DataTypeExt {
    /// 检查类型是否可比较
    fn is_comparable(&self) -> bool;
    /// 检查类型是否可排序
    fn is_sortable(&self) -> bool;
    /// 检查类型是否可聚合
    fn is_aggregatable(&self) -> bool;
    /// 获取公共类型（用于类型推断）
    fn common_type(&self, other: &DataType) -> Option<DataType>;
    /// 检查类型是否可以隐式转换
    fn can_implicitly_cast_to(&self, target: &DataType) -> bool;
}

// 为Arrow DataType添加扩展方法
impl DataTypeExt for DataType {
    /// 检查类型是否可比较
    fn is_comparable(&self) -> bool {
        match self {
            DataType::Null | DataType::Boolean => true,
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => true,
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => true,
            DataType::Float32 | DataType::Float64 => true,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary => true,
            DataType::Date32 | DataType::Date64 => true,
            DataType::Time32(_) | DataType::Time64(_) => true,
            DataType::Timestamp(_, _) => true,
            DataType::Duration(_) => true,
            DataType::Interval(_) => true,
            DataType::List(_) => true,
            DataType::LargeList(_) => true,
            DataType::Struct(_) => true,
            DataType::Map(_, _) => true,
            DataType::Union(_, _) => true,
            _ => false,
        }
    }

    /// 检查类型是否可排序
    fn is_sortable(&self) -> bool {
        self.is_comparable()
    }

    /// 检查类型是否可聚合
    fn is_aggregatable(&self) -> bool {
        match self {
            DataType::Null | DataType::Boolean => false,
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => true,
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => true,
            DataType::Float32 | DataType::Float64 => true,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary => false,
            DataType::Date32 | DataType::Date64 => true,
            DataType::Time32(_) | DataType::Time64(_) => true,
            DataType::Timestamp(_, _) => true,
            DataType::Duration(_) => true,
            DataType::Interval(_) => true,
            DataType::List(_) | DataType::LargeList(_) | DataType::Struct(_) | DataType::Map(_, _) | DataType::Union(_, _) => false,
            _ => false,
        }
    }

    /// 获取公共类型（用于类型推断）
    fn common_type(&self, other: &DataType) -> Option<DataType> {
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
            (DataType::List(left_element), DataType::List(right_element)) => {
                if let Some(common_element) = DataTypeExt::common_type(&left_element.data_type(), &right_element.data_type()) {
                    Some(DataType::List(arrow::datatypes::Field::new("item", common_element, true).into()))
                } else {
                    None
                }
            }
            
            // 其他情况不兼容
            _ => None,
        }
    }

    /// 检查类型是否可以隐式转换
    fn can_implicitly_cast_to(&self, target: &DataType) -> bool {
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
            (DataType::Utf8, DataType::LargeUtf8) => true,
            (DataType::Binary, DataType::LargeBinary) => true,
            
            // 日期时间转换
            (DataType::Date32, DataType::Date64) => true,
            (DataType::Date32, DataType::Timestamp(_, _)) => true,
            (DataType::Date64, DataType::Timestamp(_, _)) => true,
            
            _ => false,
        }
    }
}

/// 结构体字段 - 使用Arrow的Field
pub type StructField = arrow::datatypes::Field;

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
            Expression::Literal(lit) => Ok(lit.data_type()),
            Expression::Arithmetic(expr) => {
                let left_type = self.infer_type(&expr.left)?;
                let right_type = self.infer_type(&expr.right)?;
                DataTypeExt::common_type(&left_type, &right_type)
                    .ok_or_else(|| TypeInferenceError::IncompatibleTypes(left_type, right_type))
            }
            Expression::Comparison(_) => Ok(DataType::Boolean),
            Expression::Logical(_) => Ok(DataType::Boolean),
            Expression::Function(func) => Ok(func.return_type.clone()),
            Expression::Case(expr) => {
                let then_type = self.infer_type(&expr.then_expr)?;
                let else_type = self.infer_type(&expr.else_expr)?;
                DataTypeExt::common_type(&then_type, &else_type)
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
