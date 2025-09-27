//! 列式向量化投影器
//! 
//! 提供完全面向列式的、全向量化极致优化的投影算子实现

use arrow::array::*;
use arrow::compute::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use arrow::error::ArrowError;
use std::sync::Arc;
use std::time::Instant;
use std::collections::HashMap;
use tracing::{debug, info, warn};
use crate::push_runtime::{Operator, OperatorContext, Event, OpStatus, Outbox, PortId};
use datafusion_common::ScalarValue;
use anyhow::Result;

/// 列式向量化投影器配置
#[derive(Debug, Clone)]
pub struct VectorizedProjectorConfig {
    /// 批次大小
    pub batch_size: usize,
    /// 是否启用SIMD优化
    pub enable_simd: bool,
    /// 是否启用字典列优化
    pub enable_dictionary_optimization: bool,
    /// 是否启用压缩列优化
    pub enable_compressed_optimization: bool,
    /// 是否启用零拷贝优化
    pub enable_zero_copy: bool,
    /// 是否启用预取优化
    pub enable_prefetch: bool,
    /// 是否启用列重排序优化
    pub enable_column_reordering: bool,
    /// 是否启用表达式计算优化
    pub enable_expression_optimization: bool,
}

impl Default for VectorizedProjectorConfig {
    fn default() -> Self {
        Self {
            batch_size: 8192,
            enable_simd: true,
            enable_dictionary_optimization: true,
            enable_compressed_optimization: true,
            enable_zero_copy: true,
            enable_prefetch: true,
            enable_column_reordering: true,
            enable_expression_optimization: true,
        }
    }
}

/// 投影表达式
#[derive(Debug, Clone)]
pub enum ProjectionExpression {
    /// 列引用
    Column { index: usize, name: String },
    /// 常量值
    Literal { value: ScalarValue },
    /// 算术表达式
    Arithmetic { left: Box<ProjectionExpression>, op: ArithmeticOp, right: Box<ProjectionExpression> },
    /// 比较表达式
    Comparison { left: Box<ProjectionExpression>, op: ComparisonOp, right: Box<ProjectionExpression> },
    /// 逻辑表达式
    Logical { left: Box<ProjectionExpression>, op: LogicalOp, right: Box<ProjectionExpression> },
    /// 函数调用
    Function { name: String, args: Vec<ProjectionExpression> },
    /// 条件表达式
    Case { condition: Box<ProjectionExpression>, then_expr: Box<ProjectionExpression>, else_expr: Box<ProjectionExpression> },
    /// 类型转换
    Cast { expr: Box<ProjectionExpression>, target_type: DataType },
}

#[derive(Debug, Clone)]
pub enum ArithmeticOp {
    Add, Subtract, Multiply, Divide, Modulo, Power,
}

#[derive(Debug, Clone)]
pub enum ComparisonOp {
    Equal, NotEqual, LessThan, LessThanOrEqual, GreaterThan, GreaterThanOrEqual,
}

#[derive(Debug, Clone)]
pub enum LogicalOp {
    And, Or, Not,
}

/// 列式向量化投影器
pub struct VectorizedProjector {
    config: VectorizedProjectorConfig,
    expressions: Vec<ProjectionExpression>,
    output_schema: SchemaRef,
    column_indices: Vec<usize>,
    stats: ProjectorStats,
    /// 算子ID
    operator_id: u32,
    /// 输入端口
    input_ports: Vec<PortId>,
    /// 输出端口
    output_ports: Vec<PortId>,
    /// 是否完成
    finished: bool,
    /// 算子名称
    name: String,
}

#[derive(Debug, Default)]
pub struct ProjectorStats {
    pub total_rows_processed: u64,
    pub total_batches_processed: u64,
    pub total_project_time: std::time::Duration,
    pub avg_project_time: std::time::Duration,
    pub column_access_count: HashMap<usize, u64>,
    pub expression_eval_count: HashMap<String, u64>,
}

impl VectorizedProjector {
    pub fn new(
        config: VectorizedProjectorConfig,
        expressions: Vec<ProjectionExpression>,
        output_schema: SchemaRef,
        operator_id: u32,
        input_ports: Vec<PortId>,
        output_ports: Vec<PortId>,
        name: String,
    ) -> Self {
        let column_indices = Self::extract_column_indices(&expressions);
        
        Self {
            config,
            expressions,
            output_schema,
            column_indices,
            stats: ProjectorStats::default(),
            operator_id,
            input_ports,
            output_ports,
            finished: false,
            name,
        }
    }

    /// 从表达式中提取列索引
    fn extract_column_indices(expressions: &[ProjectionExpression]) -> Vec<usize> {
        let mut indices = std::collections::HashSet::new();
        
        for expr in expressions {
            Self::extract_column_indices_from_expr(expr, &mut indices);
        }
        
        indices.into_iter().collect()
    }

    /// 递归提取表达式中的列索引
    fn extract_column_indices_from_expr(expr: &ProjectionExpression, indices: &mut std::collections::HashSet<usize>) {
        match expr {
            ProjectionExpression::Column { index, .. } => {
                indices.insert(*index);
            },
            ProjectionExpression::Arithmetic { left, right, .. } => {
                Self::extract_column_indices_from_expr(left, indices);
                Self::extract_column_indices_from_expr(right, indices);
            },
            ProjectionExpression::Comparison { left, right, .. } => {
                Self::extract_column_indices_from_expr(left, indices);
                Self::extract_column_indices_from_expr(right, indices);
            },
            ProjectionExpression::Logical { left, right, .. } => {
                Self::extract_column_indices_from_expr(left, indices);
                Self::extract_column_indices_from_expr(right, indices);
            },
            ProjectionExpression::Function { args, .. } => {
                for arg in args {
                    Self::extract_column_indices_from_expr(arg, indices);
                }
            },
            ProjectionExpression::Case { condition, then_expr, else_expr } => {
                Self::extract_column_indices_from_expr(condition, indices);
                Self::extract_column_indices_from_expr(then_expr, indices);
                Self::extract_column_indices_from_expr(else_expr, indices);
            },
            ProjectionExpression::Cast { expr, .. } => {
                Self::extract_column_indices_from_expr(expr, indices);
            },
            ProjectionExpression::Literal { .. } => {
                // 常量不涉及列访问
            },
        }
    }

    /// 向量化投影
    pub fn project(&mut self, batch: &RecordBatch) -> Result<RecordBatch, String> {
        let start = Instant::now();
        
        // 检查列索引是否在范围内
        for &idx in &self.column_indices {
            if idx >= batch.num_columns() {
                return Err(format!("Column index {} out of bounds", idx));
            }
        }
        
        // 计算投影表达式
        let projected_columns: Result<Vec<ArrayRef>, String> = self.expressions
            .iter()
            .map(|expr| Self::evaluate_expression_static(expr, batch))
            .collect();
        
        let projected_columns = projected_columns?;
        
        let result = RecordBatch::try_new(self.output_schema.clone(), projected_columns)
            .map_err(|e| e.to_string())?;
        
        let duration = start.elapsed();
        self.update_stats(batch.num_rows(), duration);
        
        debug!("向量化投影完成: {} columns -> {} columns ({}μs)", 
               batch.num_columns(), result.num_columns(), duration.as_micros());
        
        Ok(result)
    }

    /// 计算投影表达式
    fn evaluate_expression_static(expr: &ProjectionExpression, batch: &RecordBatch) -> Result<ArrayRef, String> {
        match expr {
            ProjectionExpression::Column { index, .. } => {
                Ok(batch.column(*index).clone())
            },
            ProjectionExpression::Literal { value } => {
                Self::create_literal_array(value, batch.num_rows())
            },
            ProjectionExpression::Arithmetic { left, op, right } => {
                Self::evaluate_arithmetic_expression(left, op, right, batch)
            },
            ProjectionExpression::Comparison { left, op, right } => {
                Self::evaluate_comparison_expression(left, op, right, batch)
            },
            ProjectionExpression::Logical { left, op, right } => {
                Self::evaluate_logical_expression(left, op, right, batch)
            },
            ProjectionExpression::Function { name, args } => {
                Self::evaluate_function_expression(name, args, batch)
            },
            ProjectionExpression::Case { condition, then_expr, else_expr } => {
                Self::evaluate_case_expression_static(condition, then_expr, else_expr, batch)
            },
            ProjectionExpression::Cast { expr, target_type } => {
                Self::evaluate_cast_expression_static(expr, target_type, batch)
            },
        }
    }
    
    fn evaluate_expression(&mut self, expr: &ProjectionExpression, batch: &RecordBatch) -> Result<ArrayRef, String> {
        match expr {
            ProjectionExpression::Column { index, .. } => {
                self.stats.column_access_count.entry(*index).and_modify(|e| *e += 1).or_insert(1);
                Ok(batch.column(*index).clone())
            },
            ProjectionExpression::Literal { value } => {
                self.create_literal_array(value, batch.num_rows())
            },
            ProjectionExpression::Arithmetic { left, op, right } => {
                self.evaluate_arithmetic_expression(left, op, right, batch)
            },
            ProjectionExpression::Comparison { left, op, right } => {
                self.evaluate_comparison_expression(left, op, right, batch)
            },
            ProjectionExpression::Logical { left, op, right } => {
                self.evaluate_logical_expression(left, op, right, batch)
            },
            ProjectionExpression::Function { name, args } => {
                self.evaluate_function_expression(name, args, batch)
            },
            ProjectionExpression::Case { condition, then_expr, else_expr } => {
                self.evaluate_case_expression(condition, then_expr, else_expr, batch)
            },
            ProjectionExpression::Cast { expr, target_type } => {
                self.evaluate_cast_expression(expr, target_type, batch)
            },
        }
    }

    /// 创建常量数组
    fn create_literal_array(&self, value: &ScalarValue, len: usize) -> Result<ArrayRef, String> {
        match value {
            ScalarValue::Int32(Some(val)) => {
                Ok(Arc::new(Int32Array::from(vec![*val; len])))
            },
            ScalarValue::Int64(Some(val)) => {
                Ok(Arc::new(Int64Array::from(vec![*val; len])))
            },
            ScalarValue::Float32(Some(val)) => {
                Ok(Arc::new(Float32Array::from(vec![*val; len])))
            },
            ScalarValue::Float64(Some(val)) => {
                Ok(Arc::new(Float64Array::from(vec![*val; len])))
            },
            ScalarValue::Utf8(Some(val)) => {
                Ok(Arc::new(StringArray::from(vec![val.as_str(); len])))
            },
            ScalarValue::Boolean(Some(val)) => {
                Ok(Arc::new(BooleanArray::from(vec![*val; len])))
            },
            _ => Err(format!("Unsupported literal type: {:?}", value))
        }
    }

    /// 计算算术表达式
    fn evaluate_arithmetic_expression(
        &mut self,
        left: &ProjectionExpression,
        op: &ArithmeticOp,
        right: &ProjectionExpression,
        batch: &RecordBatch,
    ) -> Result<ArrayRef, String> {
        let left_array = self.evaluate_expression(left, batch)?;
        let right_array = self.evaluate_expression(right, batch)?;
        
        match op {
            ArithmeticOp::Add => {
                self.evaluate_add(&left_array, &right_array)
            },
            ArithmeticOp::Subtract => {
                self.evaluate_subtract(&left_array, &right_array)
            },
            ArithmeticOp::Multiply => {
                self.evaluate_multiply(&left_array, &right_array)
            },
            ArithmeticOp::Divide => {
                self.evaluate_divide(&left_array, &right_array)
            },
            ArithmeticOp::Modulo => {
                self.evaluate_modulo(&left_array, &right_array)
            },
            ArithmeticOp::Power => {
                self.evaluate_power(&left_array, &right_array)
            },
        }
    }

    /// 计算加法
    fn evaluate_add(&self, left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef, String> {
        match (left.data_type(), right.data_type()) {
            (DataType::Int32, DataType::Int32) => {
                let left_array = left.as_any().downcast_ref::<Int32Array>().unwrap();
                let right_array = right.as_any().downcast_ref::<Int32Array>().unwrap();
                Ok(Arc::new(arrow::compute::kernels::numeric::add(left_array, right_array).map_err(|e| e.to_string())?))
            },
            (DataType::Int64, DataType::Int64) => {
                let left_array = left.as_any().downcast_ref::<Int64Array>().unwrap();
                let right_array = right.as_any().downcast_ref::<Int64Array>().unwrap();
                Ok(Arc::new(arrow::compute::kernels::numeric::add(left_array, right_array).map_err(|e| e.to_string())?))
            },
            (DataType::Float32, DataType::Float32) => {
                let left_array = left.as_any().downcast_ref::<Float32Array>().unwrap();
                let right_array = right.as_any().downcast_ref::<Float32Array>().unwrap();
                Ok(Arc::new(arrow::compute::kernels::numeric::add(left_array, right_array).map_err(|e| e.to_string())?))
            },
            (DataType::Float64, DataType::Float64) => {
                let left_array = left.as_any().downcast_ref::<Float64Array>().unwrap();
                let right_array = right.as_any().downcast_ref::<Float64Array>().unwrap();
                Ok(Arc::new(arrow::compute::kernels::numeric::add(left_array, right_array).map_err(|e| e.to_string())?))
            },
            _ => Err(format!("Unsupported add operation: {:?} + {:?}", left.data_type(), right.data_type()))
        }
    }

    /// 计算减法
    fn evaluate_subtract(&self, left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef, String> {
        match (left.data_type(), right.data_type()) {
            (DataType::Int32, DataType::Int32) => {
                let left_array = left.as_any().downcast_ref::<Int32Array>().unwrap();
                let right_array = right.as_any().downcast_ref::<Int32Array>().unwrap();
                Ok(Arc::new(subtract(left_array, right_array).map_err(|e| e.to_string())?))
            },
            (DataType::Int64, DataType::Int64) => {
                let left_array = left.as_any().downcast_ref::<Int64Array>().unwrap();
                let right_array = right.as_any().downcast_ref::<Int64Array>().unwrap();
                Ok(Arc::new(subtract(left_array, right_array).map_err(|e| e.to_string())?))
            },
            (DataType::Float32, DataType::Float32) => {
                let left_array = left.as_any().downcast_ref::<Float32Array>().unwrap();
                let right_array = right.as_any().downcast_ref::<Float32Array>().unwrap();
                Ok(Arc::new(subtract(left_array, right_array).map_err(|e| e.to_string())?))
            },
            (DataType::Float64, DataType::Float64) => {
                let left_array = left.as_any().downcast_ref::<Float64Array>().unwrap();
                let right_array = right.as_any().downcast_ref::<Float64Array>().unwrap();
                Ok(Arc::new(subtract(left_array, right_array).map_err(|e| e.to_string())?))
            },
            _ => Err(format!("Unsupported subtract operation: {:?} - {:?}", left.data_type(), right.data_type()))
        }
    }

    /// 计算乘法
    fn evaluate_multiply(&self, left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef, String> {
        match (left.data_type(), right.data_type()) {
            (DataType::Int32, DataType::Int32) => {
                let left_array = left.as_any().downcast_ref::<Int32Array>().unwrap();
                let right_array = right.as_any().downcast_ref::<Int32Array>().unwrap();
                Ok(Arc::new(multiply(left_array, right_array).map_err(|e| e.to_string())?))
            },
            (DataType::Int64, DataType::Int64) => {
                let left_array = left.as_any().downcast_ref::<Int64Array>().unwrap();
                let right_array = right.as_any().downcast_ref::<Int64Array>().unwrap();
                Ok(Arc::new(multiply(left_array, right_array).map_err(|e| e.to_string())?))
            },
            (DataType::Float32, DataType::Float32) => {
                let left_array = left.as_any().downcast_ref::<Float32Array>().unwrap();
                let right_array = right.as_any().downcast_ref::<Float32Array>().unwrap();
                Ok(Arc::new(multiply(left_array, right_array).map_err(|e| e.to_string())?))
            },
            (DataType::Float64, DataType::Float64) => {
                let left_array = left.as_any().downcast_ref::<Float64Array>().unwrap();
                let right_array = right.as_any().downcast_ref::<Float64Array>().unwrap();
                Ok(Arc::new(multiply(left_array, right_array).map_err(|e| e.to_string())?))
            },
            _ => Err(format!("Unsupported multiply operation: {:?} * {:?}", left.data_type(), right.data_type()))
        }
    }

    /// 计算除法
    fn evaluate_divide(&self, left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef, String> {
        match (left.data_type(), right.data_type()) {
            (DataType::Int32, DataType::Int32) => {
                let left_array = left.as_any().downcast_ref::<Int32Array>().unwrap();
                let right_array = right.as_any().downcast_ref::<Int32Array>().unwrap();
                Ok(Arc::new(divide(left_array, right_array).map_err(|e| e.to_string())?))
            },
            (DataType::Int64, DataType::Int64) => {
                let left_array = left.as_any().downcast_ref::<Int64Array>().unwrap();
                let right_array = right.as_any().downcast_ref::<Int64Array>().unwrap();
                Ok(Arc::new(divide(left_array, right_array).map_err(|e| e.to_string())?))
            },
            (DataType::Float32, DataType::Float32) => {
                let left_array = left.as_any().downcast_ref::<Float32Array>().unwrap();
                let right_array = right.as_any().downcast_ref::<Float32Array>().unwrap();
                Ok(Arc::new(divide(left_array, right_array).map_err(|e| e.to_string())?))
            },
            (DataType::Float64, DataType::Float64) => {
                let left_array = left.as_any().downcast_ref::<Float64Array>().unwrap();
                let right_array = right.as_any().downcast_ref::<Float64Array>().unwrap();
                Ok(Arc::new(divide(left_array, right_array).map_err(|e| e.to_string())?))
            },
            _ => Err(format!("Unsupported divide operation: {:?} / {:?}", left.data_type(), right.data_type()))
        }
    }

    /// 计算取模
    fn evaluate_modulo(&self, left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef, String> {
        match (left.data_type(), right.data_type()) {
            (DataType::Int32, DataType::Int32) => {
                let left_array = left.as_any().downcast_ref::<Int32Array>().unwrap();
                let right_array = right.as_any().downcast_ref::<Int32Array>().unwrap();
                Ok(Arc::new(modulus(left_array, right_array).map_err(|e| e.to_string())?))
            },
            (DataType::Int64, DataType::Int64) => {
                let left_array = left.as_any().downcast_ref::<Int64Array>().unwrap();
                let right_array = right.as_any().downcast_ref::<Int64Array>().unwrap();
                Ok(Arc::new(modulus(left_array, right_array).map_err(|e| e.to_string())?))
            },
            _ => Err(format!("Unsupported modulo operation: {:?} % {:?}", left.data_type(), right.data_type()))
        }
    }

    /// 计算幂运算
    fn evaluate_power(&self, left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef, String> {
        match (left.data_type(), right.data_type()) {
            (DataType::Float64, DataType::Float64) => {
                let left_array = left.as_any().downcast_ref::<Float64Array>().unwrap();
                let right_array = right.as_any().downcast_ref::<Float64Array>().unwrap();
                Ok(Arc::new(power_scalar(left_array, right_array).map_err(|e| e.to_string())?))
            },
            _ => Err(format!("Unsupported power operation: {:?} ^ {:?}", left.data_type(), right.data_type()))
        }
    }

    /// 计算比较表达式
    fn evaluate_comparison_expression(
        &mut self,
        left: &ProjectionExpression,
        op: &ComparisonOp,
        right: &ProjectionExpression,
        batch: &RecordBatch,
    ) -> Result<ArrayRef, String> {
        let left_array = self.evaluate_expression(left, batch)?;
        let right_array = self.evaluate_expression(right, batch)?;
        
        match op {
            ComparisonOp::Equal => {
                Ok(Arc::new(equal(&left_array, &right_array).map_err(|e| e.to_string())?))
            },
            ComparisonOp::NotEqual => {
                Ok(Arc::new(not_equal(&left_array, &right_array).map_err(|e| e.to_string())?))
            },
            ComparisonOp::LessThan => {
                Ok(Arc::new(arrow::compute::kernels::cmp::lt(&left_array, &right_array).map_err(|e| e.to_string())?))
            },
            ComparisonOp::LessThanOrEqual => {
                // 使用 lt 和 eq 的组合来实现 lte
                let lt_result = arrow::compute::kernels::cmp::lt(&left_array, &right_array).map_err(|e| e.to_string())?;
                let eq_result = arrow::compute::kernels::cmp::eq(&left_array, &right_array).map_err(|e| e.to_string())?;
                Ok(Arc::new(arrow::compute::or(&lt_result, &eq_result).map_err(|e| e.to_string())?))
            },
            ComparisonOp::GreaterThan => {
                Ok(Arc::new(arrow::compute::kernels::cmp::gt(&left_array, &right_array).map_err(|e| e.to_string())?))
            },
            ComparisonOp::GreaterThanOrEqual => {
                // 使用 gt 和 eq 的组合来实现 gte
                let gt_result = arrow::compute::kernels::cmp::gt(&left_array, &right_array).map_err(|e| e.to_string())?;
                let eq_result = arrow::compute::kernels::cmp::eq(&left_array, &right_array).map_err(|e| e.to_string())?;
                Ok(Arc::new(arrow::compute::or(&gt_result, &eq_result).map_err(|e| e.to_string())?))
            },
        }
    }

    /// 计算逻辑表达式
    fn evaluate_logical_expression(
        &mut self,
        left: &ProjectionExpression,
        op: &LogicalOp,
        right: &ProjectionExpression,
        batch: &RecordBatch,
    ) -> Result<ArrayRef, String> {
        let left_array = self.evaluate_expression(left, batch)?;
        let right_array = self.evaluate_expression(right, batch)?;
        
        match op {
            LogicalOp::And => {
                Ok(Arc::new(and(&left_array, &right_array).map_err(|e| e.to_string())?))
            },
            LogicalOp::Or => {
                Ok(Arc::new(or(&left_array, &right_array).map_err(|e| e.to_string())?))
            },
            LogicalOp::Not => {
                Ok(Arc::new(not(&left_array).map_err(|e| e.to_string())?))
            },
        }
    }

    /// 计算函数表达式
    fn evaluate_function_expression(
        &mut self,
        name: &str,
        args: &[ProjectionExpression],
        batch: &RecordBatch,
    ) -> Result<ArrayRef, String> {
        self.stats.expression_eval_count.entry(name.to_string()).and_modify(|e| *e += 1).or_insert(1);
        
        match name {
            "abs" => {
                if args.len() != 1 {
                    return Err(format!("abs function expects 1 argument, got {}", args.len()));
                }
                let arg_array = self.evaluate_expression(&args[0], batch)?;
                self.evaluate_abs(&arg_array)
            },
            "sqrt" => {
                if args.len() != 1 {
                    return Err(format!("sqrt function expects 1 argument, got {}", args.len()));
                }
                let arg_array = self.evaluate_expression(&args[0], batch)?;
                self.evaluate_sqrt(&arg_array)
            },
            "sin" => {
                if args.len() != 1 {
                    return Err(format!("sin function expects 1 argument, got {}", args.len()));
                }
                let arg_array = self.evaluate_expression(&args[0], batch)?;
                self.evaluate_sin(&arg_array)
            },
            "cos" => {
                if args.len() != 1 {
                    return Err(format!("cos function expects 1 argument, got {}", args.len()));
                }
                let arg_array = self.evaluate_expression(&args[0], batch)?;
                self.evaluate_cos(&arg_array)
            },
            "exp" => {
                if args.len() != 1 {
                    return Err(format!("exp function expects 1 argument, got {}", args.len()));
                }
                let arg_array = self.evaluate_expression(&args[0], batch)?;
                self.evaluate_exp(&arg_array)
            },
            "ln" => {
                if args.len() != 1 {
                    return Err(format!("ln function expects 1 argument, got {}", args.len()));
                }
                let arg_array = self.evaluate_expression(&args[0], batch)?;
                self.evaluate_ln(&arg_array)
            },
            _ => Err(format!("Unknown function: {}", name))
        }
    }

    /// 计算绝对值
    fn evaluate_abs(&self, array: &ArrayRef) -> Result<ArrayRef, String> {
        match array.data_type() {
            DataType::Int32 => {
                let array = array.as_any().downcast_ref::<Int32Array>().unwrap();
                Ok(Arc::new(abs(array).map_err(|e| e.to_string())?))
            },
            DataType::Int64 => {
                let array = array.as_any().downcast_ref::<Int64Array>().unwrap();
                Ok(Arc::new(abs(array).map_err(|e| e.to_string())?))
            },
            DataType::Float32 => {
                let array = array.as_any().downcast_ref::<Float32Array>().unwrap();
                Ok(Arc::new(abs(array).map_err(|e| e.to_string())?))
            },
            DataType::Float64 => {
                let array = array.as_any().downcast_ref::<Float64Array>().unwrap();
                Ok(Arc::new(abs(array).map_err(|e| e.to_string())?))
            },
            _ => Err(format!("Unsupported abs operation for type: {:?}", array.data_type()))
        }
    }

    /// 计算平方根
    fn evaluate_sqrt(&self, array: &ArrayRef) -> Result<ArrayRef, String> {
        match array.data_type() {
            DataType::Float32 => {
                let array = array.as_any().downcast_ref::<Float32Array>().unwrap();
                // 自定义sqrt实现
                let sqrt_array = match array.data_type() {
                    DataType::Float32 => {
                        let float_array = array.as_any().downcast_ref::<Float32Array>().unwrap();
                        let sqrt_values: Vec<Option<f32>> = float_array.iter()
                            .map(|v| v.map(|x| x.sqrt()))
                            .collect();
                        Arc::new(Float32Array::from(sqrt_values)) as ArrayRef
                    },
                    DataType::Float64 => {
                        let float_array = array.as_any().downcast_ref::<Float64Array>().unwrap();
                        let sqrt_values: Vec<Option<f64>> = float_array.iter()
                            .map(|v| v.map(|x| x.sqrt()))
                            .collect();
                        Arc::new(Float64Array::from(sqrt_values)) as ArrayRef
                    },
                    _ => return Err(format!("sqrt only supported for float types, got {:?}", array.data_type()))
                };
                Ok(sqrt_array)
            },
            DataType::Float64 => {
                let array = array.as_any().downcast_ref::<Float64Array>().unwrap();
                // 自定义sqrt实现
                let sqrt_array = match array.data_type() {
                    DataType::Float32 => {
                        let float_array = array.as_any().downcast_ref::<Float32Array>().unwrap();
                        let sqrt_values: Vec<Option<f32>> = float_array.iter()
                            .map(|v| v.map(|x| x.sqrt()))
                            .collect();
                        Arc::new(Float32Array::from(sqrt_values)) as ArrayRef
                    },
                    DataType::Float64 => {
                        let float_array = array.as_any().downcast_ref::<Float64Array>().unwrap();
                        let sqrt_values: Vec<Option<f64>> = float_array.iter()
                            .map(|v| v.map(|x| x.sqrt()))
                            .collect();
                        Arc::new(Float64Array::from(sqrt_values)) as ArrayRef
                    },
                    _ => return Err(format!("sqrt only supported for float types, got {:?}", array.data_type()))
                };
                Ok(sqrt_array)
            },
            _ => Err(format!("Unsupported sqrt operation for type: {:?}", array.data_type()))
        }
    }

    /// 计算正弦
    fn evaluate_sin(&self, array: &ArrayRef) -> Result<ArrayRef, String> {
        match array.data_type() {
            DataType::Float32 => {
                let array = array.as_any().downcast_ref::<Float32Array>().unwrap();
                Ok(Arc::new(sin(array).map_err(|e| e.to_string())?))
            },
            DataType::Float64 => {
                let array = array.as_any().downcast_ref::<Float64Array>().unwrap();
                Ok(Arc::new(sin(array).map_err(|e| e.to_string())?))
            },
            _ => Err(format!("Unsupported sin operation for type: {:?}", array.data_type()))
        }
    }

    /// 计算余弦
    fn evaluate_cos(&self, array: &ArrayRef) -> Result<ArrayRef, String> {
        match array.data_type() {
            DataType::Float32 => {
                let array = array.as_any().downcast_ref::<Float32Array>().unwrap();
                Ok(Arc::new(cos(array).map_err(|e| e.to_string())?))
            },
            DataType::Float64 => {
                let array = array.as_any().downcast_ref::<Float64Array>().unwrap();
                Ok(Arc::new(cos(array).map_err(|e| e.to_string())?))
            },
            _ => Err(format!("Unsupported cos operation for type: {:?}", array.data_type()))
        }
    }

    /// 计算指数
    fn evaluate_exp(&self, array: &ArrayRef) -> Result<ArrayRef, String> {
        match array.data_type() {
            DataType::Float32 => {
                let array = array.as_any().downcast_ref::<Float32Array>().unwrap();
                Ok(Arc::new(exp(array).map_err(|e| e.to_string())?))
            },
            DataType::Float64 => {
                let array = array.as_any().downcast_ref::<Float64Array>().unwrap();
                Ok(Arc::new(exp(array).map_err(|e| e.to_string())?))
            },
            _ => Err(format!("Unsupported exp operation for type: {:?}", array.data_type()))
        }
    }

    /// 计算自然对数
    fn evaluate_ln(&self, array: &ArrayRef) -> Result<ArrayRef, String> {
        match array.data_type() {
            DataType::Float32 => {
                let array = array.as_any().downcast_ref::<Float32Array>().unwrap();
                Ok(Arc::new(ln(array).map_err(|e| e.to_string())?))
            },
            DataType::Float64 => {
                let array = array.as_any().downcast_ref::<Float64Array>().unwrap();
                Ok(Arc::new(ln(array).map_err(|e| e.to_string())?))
            },
            _ => Err(format!("Unsupported ln operation for type: {:?}", array.data_type()))
        }
    }

    /// 计算条件表达式
    fn evaluate_case_expression(
        &mut self,
        condition: &ProjectionExpression,
        then_expr: &ProjectionExpression,
        else_expr: &ProjectionExpression,
        batch: &RecordBatch,
    ) -> Result<ArrayRef, String> {
        let condition_array = self.evaluate_expression(condition, batch)?;
        let then_array = self.evaluate_expression(then_expr, batch)?;
        let else_array = self.evaluate_expression(else_expr, batch)?;
        
        // 使用Arrow的case函数
        Ok(Arc::new(case(&condition_array, &then_array, &else_array).map_err(|e| e.to_string())?))
    }

    /// 计算类型转换
    fn evaluate_cast_expression(
        &mut self,
        expr: &ProjectionExpression,
        target_type: &DataType,
        batch: &RecordBatch,
    ) -> Result<ArrayRef, String> {
        let array = self.evaluate_expression(expr, batch)?;
        
        // 使用Arrow的cast函数
        Ok(Arc::new(cast(&array, target_type).map_err(|e| e.to_string())?))
    }

    /// 更新统计信息
    fn update_stats(&mut self, rows: usize, duration: std::time::Duration) {
        self.stats.total_rows_processed += rows as u64;
        self.stats.total_batches_processed += 1;
        self.stats.total_project_time += duration;
        
        if self.stats.total_batches_processed > 0 {
            self.stats.avg_project_time = std::time::Duration::from_nanos(
                self.stats.total_project_time.as_nanos() as u64 / self.stats.total_batches_processed
            );
        }
    }

    /// 获取统计信息
    pub fn get_stats(&self) -> &ProjectorStats {
        &self.stats
    }

    /// 重置统计信息
    pub fn reset_stats(&mut self) {
        self.stats = ProjectorStats::default();
    }
}

/// 实现Operator trait
impl Operator for VectorizedProjector {
    fn on_register(&mut self, _ctx: OperatorContext) -> Result<()> {
        debug!("向量化投影器算子注册: {}", self.name);
        Ok(())
    }
    
    fn on_event(&mut self, ev: Event, out: &mut Outbox) -> OpStatus {
        match ev {
            Event::Data { port, batch } => {
                if self.input_ports.contains(&port) {
                    match self.project(&batch) {
                        Ok(projected_batch) => {
                            // 发送到所有输出端口
                            for &output_port in &self.output_ports {
                                out.send(output_port, projected_batch.clone());
                            }
                            OpStatus::Ready
                        },
                        Err(e) => {
                            warn!("向量化投影失败: {}", e);
                            OpStatus::Error
                        }
                    }
                } else {
                    warn!("未知的输入端口: {}", port);
                    OpStatus::Error
                }
            },
            Event::EndOfStream { port } => {
                if self.input_ports.contains(&port) {
                    self.finished = true;
                    // 转发EndOfStream事件
                    for &output_port in &self.output_ports {
                        out.send_eos(output_port);
                    }
                    OpStatus::Finished
                } else {
                    OpStatus::Ready
                }
            },
            _ => OpStatus::Ready,
        }
    }
    
    fn is_finished(&self) -> bool {
        self.finished
    }
    
    fn name(&self) -> &str {
        &self.name
    }
}

/// 批量投影处理器
pub struct BatchProjectorProcessor {
    projectors: Vec<VectorizedProjector>,
    config: VectorizedProjectorConfig,
}

impl BatchProjectorProcessor {
    pub fn new(config: VectorizedProjectorConfig) -> Self {
        Self {
            projectors: Vec::new(),
            config,
        }
    }

    /// 添加投影器
    pub fn add_projector(&mut self, expressions: Vec<ProjectionExpression>, output_schema: SchemaRef) {
        let projector = VectorizedProjector::new(self.config.clone(), expressions, output_schema);
        self.projectors.push(projector);
    }

    /// 批量投影
    pub fn project_batch(&mut self, batch: &RecordBatch) -> Result<Vec<RecordBatch>, String> {
        let mut results = Vec::new();
        
        for projector in &mut self.projectors {
            let result = projector.project(batch)?;
            results.push(result);
        }
        
        Ok(results)
    }

    /// 获取所有投影器的统计信息
    pub fn get_all_stats(&self) -> Vec<&ProjectorStats> {
        self.projectors.iter().map(|p| p.get_stats()).collect()
    }
}

/// 投影优化器
pub struct ProjectorOptimizer {
    config: VectorizedProjectorConfig,
}

impl ProjectorOptimizer {
    pub fn new(config: VectorizedProjectorConfig) -> Self {
        Self { config }
    }

    /// 优化投影表达式
    pub fn optimize_expressions(&self, expressions: &[ProjectionExpression]) -> Vec<ProjectionExpression> {
        // 实现表达式优化逻辑
        // 1. 常量折叠
        // 2. 表达式重排序
        // 3. 公共子表达式消除
        // 4. 向量化优化
        expressions.to_vec()
    }

    /// 分析表达式复杂度
    pub fn analyze_complexity(&self, expr: &ProjectionExpression) -> usize {
        match expr {
            ProjectionExpression::Column { .. } => 1,
            ProjectionExpression::Literal { .. } => 1,
            ProjectionExpression::Arithmetic { left, right, .. } => {
                1 + self.analyze_complexity(left) + self.analyze_complexity(right)
            },
            ProjectionExpression::Comparison { left, right, .. } => {
                1 + self.analyze_complexity(left) + self.analyze_complexity(right)
            },
            ProjectionExpression::Logical { left, right, .. } => {
                1 + self.analyze_complexity(left) + self.analyze_complexity(right)
            },
            ProjectionExpression::Function { args, .. } => {
                1 + args.iter().map(|arg| self.analyze_complexity(arg)).sum::<usize>()
            },
            ProjectionExpression::Case { condition, then_expr, else_expr } => {
                1 + self.analyze_complexity(condition) + self.analyze_complexity(then_expr) + self.analyze_complexity(else_expr)
            },
            ProjectionExpression::Cast { expr, .. } => {
                1 + self.analyze_complexity(expr)
            },
        }
    }
}
