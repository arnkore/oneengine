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

//! AST访问者模式实现
//! 
//! 提供表达式AST的访问者模式，用于遍历和转换表达式

use super::expression::Expression;
use anyhow::Result;

/// 表达式访问者trait
pub trait ExpressionVisitor<T> {
    /// 访问表达式
    fn visit_expression(&mut self, expr: &Expression) -> Result<T> {
        match expr {
            Expression::Column(col) => self.visit_column(col),
            Expression::Literal(lit) => self.visit_literal(lit),
            Expression::Arithmetic(expr) => self.visit_arithmetic(expr),
            Expression::Comparison(expr) => self.visit_comparison(expr),
            Expression::Logical(expr) => self.visit_logical(expr),
            Expression::Function(func) => self.visit_function(func),
            Expression::Case(expr) => self.visit_case(expr),
            Expression::Cast(expr) => self.visit_cast(expr),
            Expression::Subquery(expr) => self.visit_subquery(expr),
            Expression::Aggregate(expr) => self.visit_aggregate(expr),
            Expression::Window(expr) => self.visit_window(expr),
            Expression::Array(expr) => self.visit_array(expr),
            Expression::Struct(expr) => self.visit_struct(expr),
        }
    }

    /// 访问列引用
    fn visit_column(&mut self, col: &crate::expression::ast::expression::ColumnRef) -> Result<T> {
        self.default_visit_column(col)
    }

    /// 访问字面量
    fn visit_literal(&mut self, lit: &crate::expression::ast::expression::Literal) -> Result<T> {
        self.default_visit_literal(lit)
    }

    /// 访问算术表达式
    fn visit_arithmetic(&mut self, expr: &crate::expression::ast::expression::ArithmeticExpr) -> Result<T> {
        self.default_visit_arithmetic(expr)
    }

    /// 访问比较表达式
    fn visit_comparison(&mut self, expr: &crate::expression::ast::expression::ComparisonExpr) -> Result<T> {
        self.default_visit_comparison(expr)
    }

    /// 访问逻辑表达式
    fn visit_logical(&mut self, expr: &crate::expression::ast::expression::LogicalExpr) -> Result<T> {
        self.default_visit_logical(expr)
    }

    /// 访问函数调用
    fn visit_function(&mut self, func: &crate::expression::ast::expression::FunctionCall) -> Result<T> {
        self.default_visit_function(func)
    }

    /// 访问条件表达式
    fn visit_case(&mut self, expr: &crate::expression::ast::expression::CaseExpr) -> Result<T> {
        self.default_visit_case(expr)
    }

    /// 访问类型转换
    fn visit_cast(&mut self, expr: &crate::expression::ast::expression::CastExpr) -> Result<T> {
        self.default_visit_cast(expr)
    }

    /// 访问子查询
    fn visit_subquery(&mut self, expr: &crate::expression::ast::expression::SubqueryExpr) -> Result<T> {
        self.default_visit_subquery(expr)
    }

    /// 访问聚合表达式
    fn visit_aggregate(&mut self, expr: &crate::expression::ast::expression::AggregateExpr) -> Result<T> {
        self.default_visit_aggregate(expr)
    }

    /// 访问窗口表达式
    fn visit_window(&mut self, expr: &crate::expression::ast::expression::WindowExpr) -> Result<T> {
        self.default_visit_window(expr)
    }

    /// 访问数组表达式
    fn visit_array(&mut self, expr: &crate::expression::ast::expression::ArrayExpr) -> Result<T> {
        self.default_visit_array(expr)
    }

    /// 访问结构体表达式
    fn visit_struct(&mut self, expr: &crate::expression::ast::expression::StructExpr) -> Result<T> {
        self.default_visit_struct(expr)
    }

    // 默认实现
    fn default_visit_column(&mut self, _col: &crate::expression::ast::expression::ColumnRef) -> Result<T> {
        self.default_visit()
    }

    fn default_visit_literal(&mut self, _lit: &crate::expression::ast::expression::Literal) -> Result<T> {
        self.default_visit()
    }

    fn default_visit_arithmetic(&mut self, expr: &crate::expression::ast::expression::ArithmeticExpr) -> Result<T> {
        self.visit_expression(&expr.left)?;
        self.visit_expression(&expr.right)?;
        self.default_visit()
    }

    fn default_visit_comparison(&mut self, expr: &crate::expression::ast::expression::ComparisonExpr) -> Result<T> {
        self.visit_expression(&expr.left)?;
        self.visit_expression(&expr.right)?;
        self.default_visit()
    }

    fn default_visit_logical(&mut self, expr: &crate::expression::ast::expression::LogicalExpr) -> Result<T> {
        self.visit_expression(&expr.left)?;
        self.visit_expression(&expr.right)?;
        self.default_visit()
    }

    fn default_visit_function(&mut self, func: &crate::expression::ast::expression::FunctionCall) -> Result<T> {
        for arg in &func.args {
            self.visit_expression(arg)?;
        }
        self.default_visit()
    }

    fn default_visit_case(&mut self, expr: &crate::expression::ast::expression::CaseExpr) -> Result<T> {
        self.visit_expression(&expr.condition)?;
        self.visit_expression(&expr.then_expr)?;
        self.visit_expression(&expr.else_expr)?;
        self.default_visit()
    }

    fn default_visit_cast(&mut self, expr: &crate::expression::ast::expression::CastExpr) -> Result<T> {
        self.visit_expression(&expr.expr)?;
        self.default_visit()
    }

    fn default_visit_subquery(&mut self, expr: &crate::expression::ast::expression::SubqueryExpr) -> Result<T> {
        self.visit_expression(&expr.expr)?;
        self.default_visit()
    }

    fn default_visit_aggregate(&mut self, expr: &crate::expression::ast::expression::AggregateExpr) -> Result<T> {
        self.visit_expression(&expr.expr)?;
        self.default_visit()
    }

    fn default_visit_window(&mut self, expr: &crate::expression::ast::expression::WindowExpr) -> Result<T> {
        self.visit_expression(&expr.expr)?;
        for partition_expr in &expr.partition_by {
            self.visit_expression(partition_expr)?;
        }
        for order_expr in &expr.order_by {
            self.visit_expression(&order_expr.expr)?;
        }
        self.default_visit()
    }

    fn default_visit_array(&mut self, expr: &crate::expression::ast::expression::ArrayExpr) -> Result<T> {
        for element in &expr.elements {
            self.visit_expression(element)?;
        }
        self.default_visit()
    }

    fn default_visit_struct(&mut self, expr: &crate::expression::ast::expression::StructExpr) -> Result<T> {
        for field in &expr.fields {
            self.visit_expression(&field.expr)?;
        }
        self.default_visit()
    }

    fn default_visit(&mut self) -> Result<T>;
}

/// 表达式转换器trait
pub trait ExpressionTransformer {
    /// 转换表达式
    fn transform_expression(&mut self, expr: Expression) -> Result<Expression> {
        match expr {
            Expression::Column(col) => self.transform_column(col),
            Expression::Literal(lit) => self.transform_literal(lit),
            Expression::Arithmetic(expr) => self.transform_arithmetic(expr),
            Expression::Comparison(expr) => self.transform_comparison(expr),
            Expression::Logical(expr) => self.transform_logical(expr),
            Expression::Function(func) => self.transform_function(func),
            Expression::Case(expr) => self.transform_case(expr),
            Expression::Cast(expr) => self.transform_cast(expr),
            Expression::Subquery(expr) => self.transform_subquery(expr),
            Expression::Aggregate(expr) => self.transform_aggregate(expr),
            Expression::Window(expr) => self.transform_window(expr),
            Expression::Array(expr) => self.transform_array(expr),
            Expression::Struct(expr) => self.transform_struct(expr),
        }
    }

    /// 转换列引用
    fn transform_column(&mut self, col: crate::expression::ast::expression::ColumnRef) -> Result<Expression> {
        Ok(Expression::Column(col))
    }

    /// 转换字面量
    fn transform_literal(&mut self, lit: crate::expression::ast::expression::Literal) -> Result<Expression> {
        Ok(Expression::Literal(lit))
    }

    /// 转换算术表达式
    fn transform_arithmetic(&mut self, mut expr: crate::expression::ast::expression::ArithmeticExpr) -> Result<Expression> {
        expr.left = Box::new(self.transform_expression(*expr.left)?);
        expr.right = Box::new(self.transform_expression(*expr.right)?);
        Ok(Expression::Arithmetic(expr))
    }

    /// 转换比较表达式
    fn transform_comparison(&mut self, mut expr: crate::expression::ast::expression::ComparisonExpr) -> Result<Expression> {
        expr.left = Box::new(self.transform_expression(*expr.left)?);
        expr.right = Box::new(self.transform_expression(*expr.right)?);
        Ok(Expression::Comparison(expr))
    }

    /// 转换逻辑表达式
    fn transform_logical(&mut self, mut expr: crate::expression::ast::expression::LogicalExpr) -> Result<Expression> {
        expr.left = Box::new(self.transform_expression(*expr.left)?);
        expr.right = Box::new(self.transform_expression(*expr.right)?);
        Ok(Expression::Logical(expr))
    }

    /// 转换函数调用
    fn transform_function(&mut self, mut func: crate::expression::ast::expression::FunctionCall) -> Result<Expression> {
        func.args = func.args.into_iter()
            .map(|arg| self.transform_expression(arg))
            .collect::<Result<Vec<_>>>()?;
        Ok(Expression::Function(func))
    }

    /// 转换条件表达式
    fn transform_case(&mut self, mut expr: crate::expression::ast::expression::CaseExpr) -> Result<Expression> {
        expr.condition = Box::new(self.transform_expression(*expr.condition)?);
        expr.then_expr = Box::new(self.transform_expression(*expr.then_expr)?);
        expr.else_expr = Box::new(self.transform_expression(*expr.else_expr)?);
        Ok(Expression::Case(expr))
    }

    /// 转换类型转换
    fn transform_cast(&mut self, mut expr: crate::expression::ast::expression::CastExpr) -> Result<Expression> {
        expr.expr = Box::new(self.transform_expression(*expr.expr)?);
        Ok(Expression::Cast(expr))
    }

    /// 转换子查询
    fn transform_subquery(&mut self, mut expr: crate::expression::ast::expression::SubqueryExpr) -> Result<Expression> {
        expr.expr = Box::new(self.transform_expression(*expr.expr)?);
        Ok(Expression::Subquery(expr))
    }

    /// 转换聚合表达式
    fn transform_aggregate(&mut self, mut expr: crate::expression::ast::expression::AggregateExpr) -> Result<Expression> {
        expr.expr = Box::new(self.transform_expression(*expr.expr)?);
        Ok(Expression::Aggregate(expr))
    }

    /// 转换窗口表达式
    fn transform_window(&mut self, mut expr: crate::expression::ast::expression::WindowExpr) -> Result<Expression> {
        expr.expr = Box::new(self.transform_expression(*expr.expr)?);
        expr.partition_by = expr.partition_by.into_iter()
            .map(|e| self.transform_expression(e))
            .collect::<Result<Vec<_>>>()?;
        for order_expr in &mut expr.order_by {
            order_expr.expr = self.transform_expression(order_expr.expr.clone())?;
        }
        Ok(Expression::Window(expr))
    }

    /// 转换数组表达式
    fn transform_array(&mut self, mut expr: crate::expression::ast::expression::ArrayExpr) -> Result<Expression> {
        expr.elements = expr.elements.into_iter()
            .map(|e| self.transform_expression(e))
            .collect::<Result<Vec<_>>>()?;
        Ok(Expression::Array(expr))
    }

    /// 转换结构体表达式
    fn transform_struct(&mut self, mut expr: crate::expression::ast::expression::StructExpr) -> Result<Expression> {
        for field in &mut expr.fields {
            field.expr = self.transform_expression(field.expr.clone())?;
        }
        Ok(Expression::Struct(expr))
    }
}

/// 表达式收集器 - 收集满足条件的表达式
pub struct ExpressionCollector<F> {
    predicate: F,
    collected: Vec<Expression>,
}

impl<F> ExpressionCollector<F>
where
    F: Fn(&Expression) -> bool,
{
    pub fn new(predicate: F) -> Self {
        Self {
            predicate,
            collected: Vec::new(),
        }
    }

    pub fn collect(mut self, expr: &Expression) -> Result<Vec<Expression>> {
        self.visit_expression(expr)?;
        Ok(self.collected)
    }
}

impl<F> ExpressionVisitor<()> for ExpressionCollector<F>
where
    F: Fn(&Expression) -> bool,
{
    fn visit_expression(&mut self, expr: &Expression) -> Result<()> {
        if (self.predicate)(expr) {
            self.collected.push(expr.clone());
        }
        self.default_visit_arithmetic(&crate::expression::ast::expression::ArithmeticExpr {
            left: Box::new(expr.clone()),
            op: crate::expression::ast::expression::ArithmeticOp::Add,
            right: Box::new(expr.clone()),
        })?;
        Ok(())
    }

    fn default_visit(&mut self) -> Result<()> {
        Ok(())
    }
}

/// 表达式深度计算器
pub struct ExpressionDepthCalculator;

impl ExpressionDepthCalculator {
    pub fn calculate_depth(expr: &Expression) -> usize {
        match expr {
            Expression::Column(_) | Expression::Literal(_) => 1,
            Expression::Arithmetic(expr) => {
                1 + std::cmp::max(
                    Self::calculate_depth(&expr.left),
                    Self::calculate_depth(&expr.right)
                )
            }
            Expression::Comparison(expr) => {
                1 + std::cmp::max(
                    Self::calculate_depth(&expr.left),
                    Self::calculate_depth(&expr.right)
                )
            }
            Expression::Logical(expr) => {
                1 + std::cmp::max(
                    Self::calculate_depth(&expr.left),
                    Self::calculate_depth(&expr.right)
                )
            }
            Expression::Function(func) => {
                1 + func.args.iter()
                    .map(|arg| Self::calculate_depth(arg))
                    .max()
                    .unwrap_or(0)
            }
            Expression::Case(expr) => {
                1 + std::cmp::max(
                    Self::calculate_depth(&expr.condition),
                    std::cmp::max(
                        Self::calculate_depth(&expr.then_expr),
                        Self::calculate_depth(&expr.else_expr)
                    )
                )
            }
            Expression::Cast(expr) => 1 + Self::calculate_depth(&expr.expr),
            Expression::Subquery(expr) => 1 + Self::calculate_depth(&expr.expr),
            Expression::Aggregate(expr) => 1 + Self::calculate_depth(&expr.expr),
            Expression::Window(expr) => {
                1 + std::cmp::max(
                    Self::calculate_depth(&expr.expr),
                    expr.partition_by.iter()
                        .map(|e| Self::calculate_depth(e))
                        .max()
                        .unwrap_or(0)
                )
            }
            Expression::Array(expr) => {
                1 + expr.elements.iter()
                    .map(|e| Self::calculate_depth(e))
                    .max()
                    .unwrap_or(0)
            }
            Expression::Struct(expr) => {
                1 + expr.fields.iter()
                    .map(|f| Self::calculate_depth(&f.expr))
                    .max()
                    .unwrap_or(0)
            }
        }
    }
}
