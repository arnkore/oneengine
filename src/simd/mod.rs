//! SIMD优化模块
//! 
//! 提供SIMD优化、压缩计算等高性能优化功能

pub mod simd;
pub mod compressed_compute;

// 重新导出常用类型
pub use simd::{
    SimdConfig, SimdCapabilities, SimdStringComparator, 
    SimdArithmetic, SimdDateComparator, SimdBenchmark,
    comprehensive_simd_test
};
