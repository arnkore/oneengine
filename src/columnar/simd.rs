use crate::columnar::types::DataType;
use anyhow::Result;

/// SIMD operations for vectorized execution
pub struct SimdOps;

impl SimdOps {
    /// Check if SIMD is available for the given data type
    pub fn is_supported(data_type: &DataType) -> bool {
        matches!(
            data_type,
            DataType::Int32 | DataType::Int64 | DataType::Float32 | DataType::Float64
        )
    }

    /// Vectorized comparison for integers
    pub fn compare_int32_simd(left: &[i32], right: &[i32], op: CompareOp) -> Result<Vec<bool>> {
        // For now, fallback to scalar implementation
        // In production, this would use std::simd or platform-specific intrinsics
        Ok(match op {
            CompareOp::Equal => left.iter().zip(right.iter()).map(|(l, r)| l == r).collect(),
            CompareOp::NotEqual => left.iter().zip(right.iter()).map(|(l, r)| l != r).collect(),
            CompareOp::Less => left.iter().zip(right.iter()).map(|(l, r)| l < r).collect(),
            CompareOp::LessEqual => left.iter().zip(right.iter()).map(|(l, r)| l <= r).collect(),
            CompareOp::Greater => left.iter().zip(right.iter()).map(|(l, r)| l > r).collect(),
            CompareOp::GreaterEqual => left.iter().zip(right.iter()).map(|(l, r)| l >= r).collect(),
        })
    }

    /// Vectorized arithmetic for integers
    pub fn add_int32_simd(left: &[i32], right: &[i32]) -> Result<Vec<i32>> {
        Ok(left.iter().zip(right.iter()).map(|(l, r)| l + r).collect())
    }
}

#[derive(Debug, Clone, Copy)]
pub enum CompareOp {
    Equal,
    NotEqual,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
}
