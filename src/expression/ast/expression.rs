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

//! 表达式AST节点定义
//! 
//! 提供完整的表达式抽象语法树表示

use crate::expression::ast::types::DataType;
use datafusion_common::ScalarValue;
use std::fmt;

/// 表达式AST节点
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Expression {
    /// 列引用
    Column(ColumnRef),
    /// 字面量
    Literal(Literal),
    /// 算术表达式
    Arithmetic(ArithmeticExpr),
    /// 比较表达式
    Comparison(ComparisonExpr),
    /// 逻辑表达式
    Logical(LogicalExpr),
    /// 函数调用
    Function(FunctionCall),
    /// 条件表达式 (CASE WHEN)
    Case(CaseExpr),
    /// 类型转换
    Cast(CastExpr),
    /// 子查询
    Subquery(SubqueryExpr),
    /// 聚合函数
    Aggregate(AggregateExpr),
    /// 窗口函数
    Window(WindowExpr),
    /// 数组表达式
    Array(ArrayExpr),
    /// 结构体表达式
    Struct(StructExpr),
}

impl Expression {
    /// 获取表达式的返回类型
    pub fn get_type(&self) -> DataType {
        match self {
            Expression::Column(col) => col.data_type.clone(),
            Expression::Literal(lit) => lit.data_type(),
            Expression::Arithmetic(expr) => expr.get_type(),
            Expression::Comparison(expr) => DataType::Boolean,
            Expression::Logical(expr) => DataType::Boolean,
            Expression::Function(func) => func.return_type.clone(),
            Expression::Case(expr) => expr.get_type(),
            Expression::Cast(expr) => expr.target_type.clone(),
            Expression::Subquery(expr) => expr.return_type.clone(),
            Expression::Aggregate(expr) => expr.return_type.clone(),
            Expression::Window(expr) => expr.return_type.clone(),
            Expression::Array(expr) => expr.element_type.clone(),
            Expression::Struct(expr) => expr.struct_type.clone(),
        }
    }

    /// 检查表达式是否包含聚合函数
    pub fn contains_aggregate(&self) -> bool {
        match self {
            Expression::Aggregate(_) | Expression::Window(_) => true,
            Expression::Arithmetic(expr) => expr.left.contains_aggregate() || expr.right.contains_aggregate(),
            Expression::Comparison(expr) => expr.left.contains_aggregate() || expr.right.contains_aggregate(),
            Expression::Logical(expr) => expr.left.contains_aggregate() || expr.right.contains_aggregate(),
            Expression::Function(func) => func.args.iter().any(|arg| arg.contains_aggregate()),
            Expression::Case(expr) => {
                expr.condition.contains_aggregate() ||
                expr.then_expr.contains_aggregate() ||
                expr.else_expr.contains_aggregate()
            }
            Expression::Cast(expr) => expr.expr.contains_aggregate(),
            Expression::Subquery(expr) => expr.expr.contains_aggregate(),
            Expression::Array(expr) => expr.elements.iter().any(|elem| elem.contains_aggregate()),
            Expression::Struct(expr) => expr.fields.iter().any(|field| field.expr.contains_aggregate()),
            _ => false,
        }
    }

    /// 获取表达式中引用的列
    pub fn get_columns(&self) -> Vec<ColumnRef> {
        match self {
            Expression::Column(col) => vec![col.clone()],
            Expression::Arithmetic(expr) => {
                let mut cols = expr.left.get_columns();
                cols.extend(expr.right.get_columns());
                cols
            }
            Expression::Comparison(expr) => {
                let mut cols = expr.left.get_columns();
                cols.extend(expr.right.get_columns());
                cols
            }
            Expression::Logical(expr) => {
                let mut cols = expr.left.get_columns();
                cols.extend(expr.right.get_columns());
                cols
            }
            Expression::Function(func) => {
                func.args.iter().flat_map(|arg| arg.get_columns()).collect()
            }
            Expression::Case(expr) => {
                let mut cols = expr.condition.get_columns();
                cols.extend(expr.then_expr.get_columns());
                cols.extend(expr.else_expr.get_columns());
                cols
            }
            Expression::Cast(expr) => expr.expr.get_columns(),
            Expression::Subquery(expr) => expr.expr.get_columns(),
            Expression::Aggregate(expr) => expr.expr.get_columns(),
            Expression::Window(expr) => expr.expr.get_columns(),
            Expression::Array(expr) => {
                expr.elements.iter().flat_map(|elem| elem.get_columns()).collect()
            }
            Expression::Struct(expr) => {
                expr.fields.iter().flat_map(|field| field.expr.get_columns()).collect()
            }
            _ => vec![],
        }
    }
}

impl fmt::Display for Expression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expression::Column(col) => write!(f, "{}", col),
            Expression::Literal(lit) => write!(f, "{}", lit),
            Expression::Arithmetic(expr) => write!(f, "{}", expr),
            Expression::Comparison(expr) => write!(f, "{}", expr),
            Expression::Logical(expr) => write!(f, "{}", expr),
            Expression::Function(func) => write!(f, "{}", func),
            Expression::Case(expr) => write!(f, "{}", expr),
            Expression::Cast(expr) => write!(f, "{}", expr),
            Expression::Subquery(expr) => write!(f, "{}", expr),
            Expression::Aggregate(expr) => write!(f, "{}", expr),
            Expression::Window(expr) => write!(f, "{}", expr),
            Expression::Array(expr) => write!(f, "{}", expr),
            Expression::Struct(expr) => write!(f, "{}", expr),
        }
    }
}

/// 列引用
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnRef {
    pub name: String,
    pub index: usize,
    pub data_type: DataType,
}

impl fmt::Display for ColumnRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

/// 字面量 - 使用DataFusion的ScalarValue
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Literal {
    pub value: ScalarValue,
}

impl Literal {
    /// 获取字面量的数据类型
    pub fn data_type(&self) -> DataType {
        use arrow::datatypes::DataType as ArrowDataType;
        match self.value.data_type() {
            arrow::datatypes::DataType::Null => ArrowDataType::Null,
            arrow::datatypes::DataType::Boolean => ArrowDataType::Boolean,
            arrow::datatypes::DataType::Int8 => ArrowDataType::Int8,
            arrow::datatypes::DataType::Int16 => ArrowDataType::Int16,
            arrow::datatypes::DataType::Int32 => ArrowDataType::Int32,
            arrow::datatypes::DataType::Int64 => ArrowDataType::Int64,
            arrow::datatypes::DataType::UInt8 => ArrowDataType::UInt8,
            arrow::datatypes::DataType::UInt16 => ArrowDataType::UInt16,
            arrow::datatypes::DataType::UInt32 => ArrowDataType::UInt32,
            arrow::datatypes::DataType::UInt64 => ArrowDataType::UInt64,
            arrow::datatypes::DataType::Float32 => ArrowDataType::Float32,
            arrow::datatypes::DataType::Float64 => ArrowDataType::Float64,
            arrow::datatypes::DataType::Utf8 => ArrowDataType::Utf8,
            arrow::datatypes::DataType::LargeUtf8 => ArrowDataType::LargeUtf8,
            arrow::datatypes::DataType::Binary => ArrowDataType::Binary,
            arrow::datatypes::DataType::LargeBinary => ArrowDataType::LargeBinary,
            arrow::datatypes::DataType::Date32 => ArrowDataType::Date32,
            arrow::datatypes::DataType::Date64 => ArrowDataType::Date64,
            arrow::datatypes::DataType::Time32(unit) => ArrowDataType::Time32(unit),
            arrow::datatypes::DataType::Time64(unit) => ArrowDataType::Time64(unit),
            arrow::datatypes::DataType::Timestamp(unit, tz) => ArrowDataType::Timestamp(unit, tz),
            arrow::datatypes::DataType::Duration(unit) => ArrowDataType::Duration(unit),
            arrow::datatypes::DataType::Interval(unit) => ArrowDataType::Interval(unit),
            arrow::datatypes::DataType::List(field) => ArrowDataType::List(field),
            arrow::datatypes::DataType::LargeList(field) => ArrowDataType::LargeList(field),
            arrow::datatypes::DataType::Struct(fields) => ArrowDataType::Struct(fields),
            arrow::datatypes::DataType::Map(field, sorted) => ArrowDataType::Map(field, sorted),
            arrow::datatypes::DataType::Union(fields, mode) => ArrowDataType::Union(fields, mode),
            arrow::datatypes::DataType::Dictionary(key_type, value_type) => ArrowDataType::Dictionary(key_type, value_type),
            // arrow::datatypes::DataType::Decimal(precision, scale) => ArrowDataType::Decimal(precision, scale),
            arrow::datatypes::DataType::FixedSizeBinary(size) => ArrowDataType::FixedSizeBinary(size),
            arrow::datatypes::DataType::FixedSizeList(field, size) => ArrowDataType::FixedSizeList(field, size),
            arrow::datatypes::DataType::RunEndEncoded(run_ends_type, values_type) => ArrowDataType::RunEndEncoded(run_ends_type, values_type),
        }
    }
}

impl fmt::Display for Literal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}

/// 算术表达式
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ArithmeticExpr {
    pub left: Box<Expression>,
    pub op: ArithmeticOp,
    pub right: Box<Expression>,
}

impl ArithmeticExpr {
    pub fn get_type(&self) -> DataType {
        // 算术运算的结果类型通常是两个操作数中精度更高的类型
        match (&self.left.get_type(), &self.right.get_type()) {
            (DataType::Float64, _) | (_, DataType::Float64) => DataType::Float64,
            (DataType::Float32, _) | (_, DataType::Float32) => DataType::Float32,
            (DataType::Int64, _) | (_, DataType::Int64) => DataType::Int64,
            (DataType::Int32, _) | (_, DataType::Int32) => DataType::Int32,
            (DataType::Int16, _) | (_, DataType::Int16) => DataType::Int16,
            (DataType::Int8, _) | (_, DataType::Int8) => DataType::Int8,
            _ => DataType::Int32, // 默认类型
        }
    }
}

impl fmt::Display for ArithmeticExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({} {} {})", self.left, self.op, self.right)
    }
}

/// 算术操作符
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ArithmeticOp {
    Add,      // +
    Subtract, // -
    Multiply, // *
    Divide,   // /
    Modulo,   // %
    Power,    // ^
}

impl fmt::Display for ArithmeticOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ArithmeticOp::Add => write!(f, "+"),
            ArithmeticOp::Subtract => write!(f, "-"),
            ArithmeticOp::Multiply => write!(f, "*"),
            ArithmeticOp::Divide => write!(f, "/"),
            ArithmeticOp::Modulo => write!(f, "%"),
            ArithmeticOp::Power => write!(f, "^"),
        }
    }
}

/// 比较表达式
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ComparisonExpr {
    pub left: Box<Expression>,
    pub op: ComparisonOp,
    pub right: Box<Expression>,
}

impl fmt::Display for ComparisonExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({} {} {})", self.left, self.op, self.right)
    }
}

/// 比较操作符
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ComparisonOp {
    Equal,              // =
    NotEqual,           // <>
    LessThan,           // <
    LessThanOrEqual,    // <=
    GreaterThan,        // >
    GreaterThanOrEqual, // >=
    Like,               // LIKE
    NotLike,            // NOT LIKE
    In,                 // IN
    NotIn,              // NOT IN
    IsNull,             // IS NULL
    IsNotNull,          // IS NOT NULL
}

impl fmt::Display for ComparisonOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ComparisonOp::Equal => write!(f, "="),
            ComparisonOp::NotEqual => write!(f, "<>"),
            ComparisonOp::LessThan => write!(f, "<"),
            ComparisonOp::LessThanOrEqual => write!(f, "<="),
            ComparisonOp::GreaterThan => write!(f, ">"),
            ComparisonOp::GreaterThanOrEqual => write!(f, ">="),
            ComparisonOp::Like => write!(f, "LIKE"),
            ComparisonOp::NotLike => write!(f, "NOT LIKE"),
            ComparisonOp::In => write!(f, "IN"),
            ComparisonOp::NotIn => write!(f, "NOT IN"),
            ComparisonOp::IsNull => write!(f, "IS NULL"),
            ComparisonOp::IsNotNull => write!(f, "IS NOT NULL"),
        }
    }
}

/// 逻辑表达式
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalExpr {
    pub left: Box<Expression>,
    pub op: LogicalOp,
    pub right: Box<Expression>,
}

impl fmt::Display for LogicalExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({} {} {})", self.left, self.op, self.right)
    }
}

/// 逻辑操作符
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LogicalOp {
    And, // AND
    Or,  // OR
    Not, // NOT
}

impl fmt::Display for LogicalOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogicalOp::And => write!(f, "AND"),
            LogicalOp::Or => write!(f, "OR"),
            LogicalOp::Not => write!(f, "NOT"),
        }
    }
}

/// 函数调用
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FunctionCall {
    pub name: String,
    pub args: Vec<Expression>,
    pub return_type: DataType,
    pub is_aggregate: bool,
    pub is_window: bool,
}

impl fmt::Display for FunctionCall {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}({})", self.name, 
               self.args.iter().map(|arg| format!("{}", arg)).collect::<Vec<_>>().join(", "))
    }
}

/// 条件表达式
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CaseExpr {
    pub condition: Box<Expression>,
    pub then_expr: Box<Expression>,
    pub else_expr: Box<Expression>,
}

impl CaseExpr {
    pub fn get_type(&self) -> DataType {
        // CASE表达式的类型是then_expr和else_expr的公共类型
        // 这里简化处理，实际应该做类型推断
        self.then_expr.get_type()
    }
}

impl fmt::Display for CaseExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CASE WHEN {} THEN {} ELSE {} END", 
               self.condition, self.then_expr, self.else_expr)
    }
}

/// 类型转换
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CastExpr {
    pub expr: Box<Expression>,
    pub target_type: DataType,
}

impl fmt::Display for CastExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CAST({} AS {})", self.expr, self.target_type)
    }
}

/// 子查询表达式
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SubqueryExpr {
    pub expr: Box<Expression>,
    pub return_type: DataType,
}

impl fmt::Display for SubqueryExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({})", self.expr)
    }
}

/// 聚合表达式
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AggregateExpr {
    pub func: String,
    pub expr: Box<Expression>,
    pub return_type: DataType,
    pub distinct: bool,
}

impl fmt::Display for AggregateExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let distinct_str = if self.distinct { "DISTINCT " } else { "" };
        write!(f, "{}({}{})", self.func, distinct_str, self.expr)
    }
}

/// 窗口表达式
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WindowExpr {
    pub func: String,
    pub expr: Box<Expression>,
    pub return_type: DataType,
    pub partition_by: Vec<Expression>,
    pub order_by: Vec<OrderByExpr>,
    pub window_frame: Option<WindowFrame>,
}

impl fmt::Display for WindowExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}({}) OVER (", self.func, self.expr);
        if !self.partition_by.is_empty() {
            write!(f, "PARTITION BY {}", 
                   self.partition_by.iter().map(|e| format!("{}", e)).collect::<Vec<_>>().join(", "));
        }
        if !self.order_by.is_empty() {
            if !self.partition_by.is_empty() {
                write!(f, " ");
            }
            write!(f, "ORDER BY {}", 
                   self.order_by.iter().map(|e| format!("{}", e)).collect::<Vec<_>>().join(", "));
        }
        write!(f, ")")
    }
}

/// 排序表达式
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OrderByExpr {
    pub expr: Expression,
    pub ascending: bool,
    pub nulls_first: bool,
}

impl fmt::Display for OrderByExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.expr);
        if !self.ascending {
            write!(f, " DESC");
        }
        if self.nulls_first {
            write!(f, " NULLS FIRST");
        }
        Ok(())
    }
}

/// 窗口框架
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WindowFrame {
    pub start: FrameBound,
    pub end: FrameBound,
}

/// 框架边界
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FrameBound {
    UnboundedPreceding,
    Preceding(usize),
    CurrentRow,
    Following(usize),
    UnboundedFollowing,
}

/// 数组表达式
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ArrayExpr {
    pub elements: Vec<Expression>,
    pub element_type: DataType,
}

impl fmt::Display for ArrayExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}]", 
               self.elements.iter().map(|e| format!("{}", e)).collect::<Vec<_>>().join(", "))
    }
}

/// 结构体表达式
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StructExpr {
    pub fields: Vec<StructField>,
    pub struct_type: DataType,
}

impl fmt::Display for StructExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{{{}", 
               self.fields.iter().map(|f| format!("{}: {}", f.name, f.expr)).collect::<Vec<_>>().join(", "));
        write!(f, "}}")
    }
}

/// 结构体字段
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StructField {
    pub name: String,
    pub expr: Expression,
}
