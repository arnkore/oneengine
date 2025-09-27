//! Hash聚合算子实现
//! 
//! 实现两阶段Hash聚合：
//! 1. 局部聚合阶段：对输入数据进行分组和聚合
//! 2. 合并阶段：合并多个局部聚合结果

use crate::columnar::{
    batch::{Batch, BatchSchema, Field},
    column::{Column, AnyArray},
    types::{DataType, Bitmap},
};
use crate::execution::{
    context::ExecContext,
    operator::Operator,
    driver::DriverYield,
};
use anyhow::Result;
use hashbrown::HashMap;
use smallvec::SmallVec;
use std::sync::Arc;

/// 聚合函数类型
#[derive(Debug, Clone, PartialEq)]
pub enum AggFunction {
    /// 计数
    Count,
    /// 求和
    Sum,
    /// 平均值
    Avg,
    /// 最大值
    Max,
    /// 最小值
    Min,
    /// 去重计数
    CountDistinct,
}

/// 聚合表达式
#[derive(Debug, Clone)]
pub struct AggExpr {
    /// 聚合函数
    pub func: AggFunction,
    /// 输入列索引
    pub input_col: usize,
    /// 输出列名称
    pub output_name: String,
    /// 输出数据类型
    pub output_type: DataType,
}

/// Hash聚合算子配置
#[derive(Debug, Clone)]
pub struct HashAggConfig {
    /// 分组列索引
    pub group_by_cols: Vec<usize>,
    /// 聚合表达式
    pub agg_exprs: Vec<AggExpr>,
    /// 输入schema
    pub input_schema: BatchSchema,
    /// 是否启用两阶段聚合
    pub two_phase: bool,
    /// 哈希表初始容量
    pub initial_capacity: usize,
}

/// 聚合状态
#[derive(Debug, Clone)]
pub struct AggState {
    /// 计数
    pub count: u64,
    /// 求和
    pub sum: f64,
    /// 最小值
    pub min: f64,
    /// 最大值
    pub max: f64,
    /// 平方和（用于计算方差）
    pub sum_squares: f64,
    /// 去重值集合
    pub distinct_values: std::collections::HashSet<u64>,
}

impl Default for AggState {
    fn default() -> Self {
        Self {
            count: 0,
            sum: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            sum_squares: 0.0,
            distinct_values: std::collections::HashSet::new(),
        }
    }
}

impl AggState {
    /// 更新聚合状态
    pub fn update(&mut self, value: f64, func: &AggFunction) {
        match func {
            AggFunction::Count => {
                self.count += 1;
            }
            AggFunction::Sum => {
                self.count += 1;
                self.sum += value;
            }
            AggFunction::Avg => {
                self.count += 1;
                self.sum += value;
            }
            AggFunction::Max => {
                self.count += 1;
                if value > self.max {
                    self.max = value;
                }
            }
            AggFunction::Min => {
                self.count += 1;
                if value < self.min {
                    self.min = value;
                }
            }
            AggFunction::CountDistinct => {
                self.count += 1;
                self.distinct_values.insert(value.to_bits());
            }
        }
    }

    /// 合并另一个聚合状态
    pub fn merge(&mut self, other: &AggState) {
        self.count += other.count;
        self.sum += other.sum;
        self.sum_squares += other.sum_squares;
        
        if other.min < self.min {
            self.min = other.min;
        }
        if other.max > self.max {
            self.max = other.max;
        }
        
        for &value in &other.distinct_values {
            self.distinct_values.insert(value);
        }
    }

    /// 获取聚合结果
    pub fn get_result(&self, func: &AggFunction) -> f64 {
        match func {
            AggFunction::Count => self.count as f64,
            AggFunction::Sum => self.sum,
            AggFunction::Avg => {
                if self.count > 0 {
                    self.sum / self.count as f64
                } else {
                    0.0
                }
            }
            AggFunction::Max => self.max,
            AggFunction::Min => self.min,
            AggFunction::CountDistinct => self.distinct_values.len() as f64,
        }
    }
}

/// Hash聚合算子
pub struct HashAggOperator {
    config: HashAggConfig,
    /// 局部聚合结果
    local_agg: HashMap<SmallVec<[u64; 4]>, Vec<AggState>>,
    /// 是否已完成局部聚合
    local_phase_done: bool,
    /// 输出批次
    output_batches: Vec<Batch>,
    /// 当前输出批次索引
    current_batch_idx: usize,
}

impl HashAggOperator {
    /// 创建新的Hash聚合算子
    pub fn new(config: HashAggConfig) -> Self {
        Self {
            local_agg: HashMap::with_capacity(config.initial_capacity),
            local_phase_done: false,
            output_batches: Vec::new(),
            current_batch_idx: 0,
            config,
        }
    }

    /// 处理输入批次
    pub fn process_input(&mut self, batch: &Batch) -> Result<()> {
        if !self.local_phase_done {
            self.do_local_aggregation(batch)?;
        }
        Ok(())
    }

    /// 完成局部聚合阶段
    pub fn finish_local_phase(&mut self) -> Result<()> {
        if !self.local_phase_done {
            self.local_phase_done = true;
            self.generate_output_batches()?;
        }
        Ok(())
    }

    /// 计算分组键的哈希值
    fn compute_group_key(&self, row: usize, batch: &Batch) -> SmallVec<[u64; 4]> {
        let mut key = SmallVec::new();
        
        for &col_idx in &self.config.group_by_cols {
            if let Some(column) = batch.columns.get(col_idx) {
                let hash = match &column.data {
                    AnyArray::Int32(data) => {
                        if let Some(bitmap) = &column.nulls {
                            if !bitmap.get(row) {
                                0 // NULL值的哈希
                            } else {
                                data[row] as u64
                            }
                        } else {
                            data[row] as u64
                        }
                    }
                    AnyArray::Int64(data) => {
                        if let Some(bitmap) = &column.nulls {
                            if !bitmap.get(row) {
                                0
                            } else {
                                data[row] as u64
                            }
                        } else {
                            data[row] as u64
                        }
                    }
                    AnyArray::Float64(data) => {
                        if let Some(bitmap) = &column.nulls {
                            if !bitmap.get(row) {
                                0
                            } else {
                                data[row].to_bits()
                            }
                        } else {
                            data[row].to_bits()
                        }
                    }
                    AnyArray::Utf8(data) => {
                        if let Some(bitmap) = &column.nulls {
                            if !bitmap.get(row) {
                                0
                            } else {
                                // 简单的字符串哈希
                                data[row].as_bytes().iter().fold(0u64, |acc, &b| {
                                    acc.wrapping_mul(31).wrapping_add(b as u64)
                                })
                            }
                        } else {
                            data[row].as_bytes().iter().fold(0u64, |acc, &b| {
                                acc.wrapping_mul(31).wrapping_add(b as u64)
                            })
                        }
                    }
                    // 其他类型暂时返回0
                    _ => 0,
                };
                key.push(hash);
            }
        }
        
        key
    }

    /// 执行局部聚合
    fn do_local_aggregation(&mut self, batch: &Batch) -> Result<()> {
        for row in 0..batch.len {
            let group_key = self.compute_group_key(row, batch);
            
            // 获取或创建聚合状态
            let agg_states = self.local_agg.entry(group_key).or_insert_with(|| {
                vec![AggState::default(); self.config.agg_exprs.len()]
            });

            // 更新每个聚合表达式
            for (expr_idx, expr) in self.config.agg_exprs.iter().enumerate() {
                if let Some(column) = batch.columns.get(expr.input_col) {
                let value = match &column.data {
                    AnyArray::Int32(data) => {
                        if let Some(bitmap) = &column.nulls {
                            if !bitmap.get(row) {
                                continue; // 跳过NULL值
                            }
                        }
                        data[row] as f64
                    }
                    AnyArray::Int64(data) => {
                        if let Some(bitmap) = &column.nulls {
                            if !bitmap.get(row) {
                                continue;
                            }
                        }
                        data[row] as f64
                    }
                    AnyArray::Float64(data) => {
                        if let Some(bitmap) = &column.nulls {
                            if !bitmap.get(row) {
                                continue;
                            }
                        }
                        data[row]
                    }
                    AnyArray::Utf8(_) => {
                        // 字符串类型只支持COUNT
                        if matches!(expr.func, AggFunction::Count) {
                            1.0
                        } else {
                            continue;
                        }
                    }
                    // 其他类型暂时跳过
                    _ => continue,
                };
                    
                    agg_states[expr_idx].update(value, &expr.func);
                }
            }
        }
        
        Ok(())
    }

    /// 生成输出批次
    fn generate_output_batches(&mut self) -> Result<()> {
        if self.local_agg.is_empty() {
            return Ok(());
        }

        // 计算输出schema
        let mut output_schema = BatchSchema::new();
        
        // 添加分组列
        for &col_idx in &self.config.group_by_cols {
            if let Some(field) = self.config.input_schema.fields.get(col_idx) {
                output_schema.fields.push(field.clone());
            }
        }
        
        // 添加聚合列
        for expr in &self.config.agg_exprs {
            output_schema.fields.push(Field {
                name: expr.output_name.clone(),
                data_type: expr.output_type.clone(),
                nullable: true,
            });
        }

        // 准备输出数据
        let mut group_keys = Vec::new();
        let mut agg_results = Vec::new();
        
        for (group_key, states) in &self.local_agg {
            group_keys.push(group_key.clone());
            agg_results.push(states.clone());
        }

        // 创建输出批次
        let mut output_columns = Vec::new();
        
        // 添加分组列数据
        for &col_idx in &self.config.group_by_cols {
            let mut column_data = Vec::new();
            let mut null_bitmap = Bitmap::new(group_keys.len());
            
            for (row, group_key) in group_keys.iter().enumerate() {
                if let Some(key_part) = group_key.get(col_idx - self.config.group_by_cols[0]) {
                    // 这里简化处理，实际应该根据原始数据类型重建
                    column_data.push(*key_part as i32);
                    null_bitmap.set(row, true);
                } else {
                    null_bitmap.set(row, false);
                }
            }
            
            let column = Column::new(DataType::Int32, group_keys.len());
            // 这里需要设置实际的数据，暂时简化
            output_columns.push(column);
        }
        
        // 添加聚合列数据
        for (expr_idx, expr) in self.config.agg_exprs.iter().enumerate() {
            let mut column_data = Vec::new();
            let mut null_bitmap = Bitmap::new(group_keys.len());
            
            for (row, states) in agg_results.iter().enumerate() {
                let result = states[expr_idx].get_result(&expr.func);
                column_data.push(result);
                null_bitmap.set(row, true);
            }
            
            let column = Column::new(DataType::Float64, group_keys.len());
            // 这里需要设置实际的数据，暂时简化
            output_columns.push(column);
        }

        let output_batch = Batch::new(output_schema);
        self.output_batches.push(output_batch);
        
        Ok(())
    }
}

impl Operator for HashAggOperator {
    fn prepare(&mut self, _ctx: &ExecContext) -> Result<()> {
        Ok(())
    }

    fn poll_next(&mut self, _cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<Option<Batch>>> {
        if !self.local_phase_done {
            // 局部聚合阶段，等待输入
            std::task::Poll::Pending
        } else {
            // 返回下一个输出批次
            if self.current_batch_idx < self.output_batches.len() {
                let batch = self.output_batches[self.current_batch_idx].clone();
                self.current_batch_idx += 1;
                std::task::Poll::Ready(Ok(Some(batch)))
            } else {
                std::task::Poll::Ready(Ok(None)) // 所有输出已返回
            }
        }
    }

    fn close(&mut self) -> Result<()> {
        Ok(())
    }

    fn output_schema(&self) -> &BatchSchema {
        // 这里需要返回输出schema，暂时返回输入schema
        &self.config.input_schema
    }
}

// HashAggOperator不需要实现Driver trait，它只实现Operator trait

#[cfg(test)]
mod tests {
    use super::*;
    use crate::columnar::types::Field;

    #[test]
    fn test_hash_agg_basic() {
        // 创建测试数据
        let input_schema = BatchSchema::new(vec![
            ("group_col".to_string(), DataType::Int32),
            ("value_col".to_string(), DataType::Float64),
        ]);

        let config = HashAggConfig {
            group_by_cols: vec![0],
            agg_exprs: vec![
                AggExpr {
                    func: AggFunction::Count,
                    input_col: 1,
                    output_name: "count".to_string(),
                    output_type: DataType::Int64,
                },
                AggExpr {
                    func: AggFunction::Sum,
                    input_col: 1,
                    output_name: "sum".to_string(),
                    output_type: DataType::Float64,
                },
            ],
            input_schema,
            two_phase: false,
            initial_capacity: 1024,
        };

        let mut operator = HashAggOperator::new(config);
        
        // 创建测试批次
        let test_batch = Batch::new(
            BatchSchema::new(vec![
                ("group_col".to_string(), DataType::Int32),
                ("value_col".to_string(), DataType::Float64),
            ]),
            vec![
                Column::new(AnyArray::Int32(vec![1, 1, 2, 2, 3]), None),
                Column::new(AnyArray::Float64(vec![10.0, 20.0, 30.0, 40.0, 50.0]), None),
            ],
            5,
        ).unwrap();

        // 执行聚合
        let result = operator.execute(&ExecContext::default(), Some(test_batch));
        assert!(result.is_ok());
        assert!(result.unwrap().is_none()); // 局部聚合阶段无输出

        // 完成聚合
        let final_result = operator.execute(&ExecContext::default(), None);
        assert!(final_result.is_ok());
        assert!(final_result.unwrap().is_some()); // 应该有最终输出
    }
}
