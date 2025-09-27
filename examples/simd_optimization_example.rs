use oneengine::optimization::*;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt::init();
    
    println!("🚀 SIMD优化演示");
    println!("================================================");
    
    // 创建SIMD配置
    let config = SimdConfig {
        enable_simd: true,
        enable_avx2: true,
        enable_avx512: true,
        enable_sse42: true,
        min_vector_length: 16,
        enable_cpu_feature_detection: true,
    };
    
    println!("📊 SIMD配置：");
    println!("✅ SIMD启用: {}", config.enable_simd);
    println!("✅ AVX2启用: {}", config.enable_avx2);
    println!("✅ AVX512启用: {}", config.enable_avx512);
    println!("✅ SSE4.2启用: {}", config.enable_sse42);
    println!("✅ 最小向量长度: {}", config.min_vector_length);
    println!("✅ CPU特性检测: {}", config.enable_cpu_feature_detection);
    println!();
    
    // 检测CPU SIMD能力
    println!("🔄 测试场景1：CPU SIMD能力检测");
    println!("------------------------");
    let capabilities = SimdCapabilities::detect();
    println!("✅ CPU SIMD能力检测完成:");
    println!("  📈 AVX2: {}", capabilities.avx2);
    println!("  📈 AVX512: {}", capabilities.avx512);
    println!("  📈 SSE4.2: {}", capabilities.sse42);
    println!("  📈 FMA: {}", capabilities.fma);
    println!("  📈 BMI1: {}", capabilities.bmi1);
    println!("  📈 BMI2: {}", capabilities.bmi2);
    println!("  📈 POPCNT: {}", capabilities.popcnt);
    println!("  📈 LZCNT: {}", capabilities.lzcnt);
    println!("  📈 TZCNT: {}", capabilities.tzcnt);
    println!();
    
    // 字符串比较测试
    println!("🔄 测试场景2：SIMD字符串比较");
    println!("------------------------");
    let comparator = SimdStringComparator::new(config.clone());
    
    let left = b"Hello, World! This is a test string for SIMD comparison.";
    let right = b"Hello, World! This is a test string for SIMD comparison.";
    let right_different = b"Hello, World! This is a test string for SIMD different.";
    
    let start = Instant::now();
    let equal_result = comparator.compare_equal(left, right);
    let not_equal_result = comparator.compare_equal(left, right_different);
    let duration = start.elapsed();
    
    println!("✅ 字符串比较测试完成:");
    println!("  📈 相等比较: {} ({}ns)", equal_result, duration.as_nanos());
    println!("  📈 不等比较: {} ({}ns)", not_equal_result, duration.as_nanos());
    println!();
    
    // 算术运算测试
    println!("🔄 测试场景3：SIMD算术运算");
    println!("------------------------");
    let arithmetic = SimdArithmetic::new(config.clone());
    
    // 整数加法测试
    let left_i32 = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
    let right_i32 = vec![2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17];
    let mut result_i32 = vec![0; 16];
    
    let start = Instant::now();
    arithmetic.add_i32(&left_i32, &right_i32, &mut result_i32);
    let duration = start.elapsed();
    
    println!("✅ 整数加法测试完成:");
    println!("  📈 输入: {:?}", &left_i32[0..8]);
    println!("  📈 加数: {:?}", &right_i32[0..8]);
    println!("  📈 结果: {:?} ({}ns)", &result_i32[0..8], duration.as_nanos());
    
    // 浮点数乘法测试
    let left_f32 = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0];
    let right_f32 = vec![2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0];
    let mut result_f32 = vec![0.0; 16];
    
    let start = Instant::now();
    arithmetic.multiply_f32(&left_f32, &right_f32, &mut result_f32);
    let duration = start.elapsed();
    
    println!("✅ 浮点数乘法测试完成:");
    println!("  📈 输入: {:?}", &left_f32[0..8]);
    println!("  📈 乘数: {:?}", &right_f32[0..8]);
    println!("  📈 结果: {:?} ({}ns)", &result_f32[0..8], duration.as_nanos());
    println!();
    
    // 日期比较测试
    println!("🔄 测试场景4：SIMD日期比较");
    println!("------------------------");
    let date_comparator = SimdDateComparator::new(config.clone());
    
    // 准备测试数据：2021-01-01的几个时间点
    let dates = vec![
        1609459200, // 2021-01-01 00:00:00
        1609462800, // 2021-01-01 01:00:00
        1609466400, // 2021-01-01 02:00:00
        1609470000, // 2021-01-01 03:00:00
        1609473600, // 2021-01-01 04:00:00
        1609477200, // 2021-01-01 05:00:00
        1609480800, // 2021-01-01 06:00:00
        1609484400, // 2021-01-01 07:00:00
    ];
    let min_date = 1609459200; // 2021-01-01 00:00:00
    let max_date = 1609466400; // 2021-01-01 02:00:00
    let mut result_indices = Vec::new();
    
    let start = Instant::now();
    date_comparator.filter_date_range(&dates, min_date, max_date, &mut result_indices);
    let duration = start.elapsed();
    
    println!("✅ 日期范围过滤测试完成:");
    println!("  📈 日期范围: 2021-01-01 00:00:00 到 2021-01-01 02:00:00");
    println!("  📈 匹配索引: {:?} ({}ns)", result_indices, duration.as_nanos());
    println!();
    
    // 性能基准测试
    println!("🔄 测试场景5：SIMD性能基准测试");
    println!("------------------------");
    let benchmark = SimdBenchmark::new(config.clone());
    let iterations = 10000;
    
    // 字符串比较性能测试
    let start = Instant::now();
    let string_results = benchmark.benchmark_string_comparison(iterations);
    let string_duration = start.elapsed();
    
    let string_avg: u128 = string_results.iter().sum::<u128>() / string_results.len() as u128;
    let string_min = *string_results.iter().min().unwrap();
    let string_max = *string_results.iter().max().unwrap();
    
    println!("✅ 字符串比较性能测试 ({}次迭代):", iterations);
    println!("  📈 总耗时: {}μs", string_duration.as_micros());
    println!("  📈 平均耗时: {}ns", string_avg);
    println!("  📈 最小耗时: {}ns", string_min);
    println!("  📈 最大耗时: {}ns", string_max);
    
    // 算术运算性能测试
    let start = Instant::now();
    let arithmetic_results = benchmark.benchmark_arithmetic(iterations);
    let arithmetic_duration = start.elapsed();
    
    let arithmetic_avg: u128 = arithmetic_results.iter().sum::<u128>() / arithmetic_results.len() as u128;
    let arithmetic_min = *arithmetic_results.iter().min().unwrap();
    let arithmetic_max = *arithmetic_results.iter().max().unwrap();
    
    println!("✅ 算术运算性能测试 ({}次迭代):", iterations);
    println!("  📈 总耗时: {}μs", arithmetic_duration.as_micros());
    println!("  📈 平均耗时: {}ns", arithmetic_avg);
    println!("  📈 最小耗时: {}ns", arithmetic_min);
    println!("  📈 最大耗时: {}ns", arithmetic_max);
    
    // 日期比较性能测试
    let start = Instant::now();
    let date_results = benchmark.benchmark_date_comparison(iterations);
    let date_duration = start.elapsed();
    
    let date_avg: u128 = date_results.iter().sum::<u128>() / date_results.len() as u128;
    let date_min = *date_results.iter().min().unwrap();
    let date_max = *date_results.iter().max().unwrap();
    
    println!("✅ 日期比较性能测试 ({}次迭代):", iterations);
    println!("  📈 总耗时: {}μs", date_duration.as_micros());
    println!("  📈 平均耗时: {}ns", date_avg);
    println!("  📈 最小耗时: {}ns", date_min);
    println!("  📈 最大耗时: {}ns", date_max);
    println!();
    
    // 综合功能测试
    println!("🔄 测试场景6：综合SIMD功能测试");
    println!("------------------------");
    let start = Instant::now();
    comprehensive_simd_test(config)?;
    let duration = start.elapsed();
    
    println!("✅ 综合SIMD功能测试完成: {}μs", duration.as_micros());
    println!();
    
    // 性能对比测试
    println!("🔄 测试场景7：SIMD vs 标量性能对比");
    println!("------------------------");
    
    // 创建禁用SIMD的配置
    let scalar_config = SimdConfig {
        enable_simd: false,
        enable_avx2: false,
        enable_avx512: false,
        enable_sse42: false,
        min_vector_length: 16,
        enable_cpu_feature_detection: false,
    };
    
    let scalar_benchmark = SimdBenchmark::new(scalar_config);
    let simd_benchmark = SimdBenchmark::new(config.clone());
    
    let test_iterations = 1000;
    
    // 字符串比较对比
    let scalar_string_results = scalar_benchmark.benchmark_string_comparison(test_iterations);
    let simd_string_results = simd_benchmark.benchmark_string_comparison(test_iterations);
    
    let scalar_string_avg: u128 = scalar_string_results.iter().sum::<u128>() / scalar_string_results.len() as u128;
    let simd_string_avg: u128 = simd_string_results.iter().sum::<u128>() / simd_string_results.len() as u128;
    let string_speedup = scalar_string_avg as f64 / simd_string_avg as f64;
    
    println!("✅ 字符串比较性能对比:");
    println!("  📈 标量平均耗时: {}ns", scalar_string_avg);
    println!("  📈 SIMD平均耗时: {}ns", simd_string_avg);
    println!("  📈 加速比: {:.2}x", string_speedup);
    
    // 算术运算对比
    let scalar_arithmetic_results = scalar_benchmark.benchmark_arithmetic(test_iterations);
    let simd_arithmetic_results = simd_benchmark.benchmark_arithmetic(test_iterations);
    
    let scalar_arithmetic_avg: u128 = scalar_arithmetic_results.iter().sum::<u128>() / scalar_arithmetic_results.len() as u128;
    let simd_arithmetic_avg: u128 = simd_arithmetic_results.iter().sum::<u128>() / simd_arithmetic_results.len() as u128;
    let arithmetic_speedup = scalar_arithmetic_avg as f64 / simd_arithmetic_avg as f64;
    
    println!("✅ 算术运算性能对比:");
    println!("  📈 标量平均耗时: {}ns", scalar_arithmetic_avg);
    println!("  📈 SIMD平均耗时: {}ns", simd_arithmetic_avg);
    println!("  📈 加速比: {:.2}x", arithmetic_speedup);
    
    // 日期比较对比
    let scalar_date_results = scalar_benchmark.benchmark_date_comparison(test_iterations);
    let simd_date_results = simd_benchmark.benchmark_date_comparison(test_iterations);
    
    let scalar_date_avg: u128 = scalar_date_results.iter().sum::<u128>() / scalar_date_results.len() as u128;
    let simd_date_avg: u128 = simd_date_results.iter().sum::<u128>() / simd_date_results.len() as u128;
    let date_speedup = scalar_date_avg as f64 / simd_date_avg as f64;
    
    println!("✅ 日期比较性能对比:");
    println!("  📈 标量平均耗时: {}ns", scalar_date_avg);
    println!("  📈 SIMD平均耗时: {}ns", simd_date_avg);
    println!("  📈 加速比: {:.2}x", date_speedup);
    println!();
    
    println!("🎯 SIMD优化演示完成！");
    println!("✅ 支持AVX2、AVX512、SSE4.2等SIMD指令集");
    println!("✅ 支持字符串比较、算术运算、日期比较的SIMD优化");
    println!("✅ 支持平台特性检测和自动选择最优实现");
    println!("✅ 支持性能基准测试和对比分析");
    println!("✅ 为高性能计算提供了完整的SIMD优化能力");
    
    Ok(())
}
