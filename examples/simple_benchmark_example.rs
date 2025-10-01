//! Simple benchmark example
//! 
//! This example demonstrates how to run simple performance benchmarks
//! using the OneEngine benchmark runner.

use oneengine::performance::*;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    println!("🚀 Starting OneEngine Simple Benchmark Example");

    // Create benchmark configuration
    let config = BenchmarkConfig {
        warmup_iterations: 2,
        measurement_iterations: 5,
        timeout: Duration::from_secs(60),
        memory_limit: Some(1024 * 1024 * 1024), // 1GB
        cpu_limit: Some(0.8), // 80% CPU usage
    };

    // Create benchmark suite
    let mut suite = BenchmarkSuite::new(config);

    // Run simple benchmarks
    println!("\n📊 Running Simple Benchmarks...");

    // Simple computation benchmark
    let _ = suite.run_benchmark(
        "simple_computation".to_string(),
        || async {
            // Simulate some computation
            let mut sum = 0.0;
            for i in 0..1000000 {
                sum += (i as f64).sqrt();
            }
            Ok(sum)
        },
    ).await.map_err(|e| format!("Benchmark failed: {}", e))?;

    // Memory allocation benchmark
    let _ = suite.run_benchmark(
        "memory_allocation".to_string(),
        || async {
            // Simulate memory allocation
            let mut data = Vec::with_capacity(10000);
            for i in 0..10000 {
                data.push(i * i);
            }
            Ok(data.len())
        },
    ).await.map_err(|e| format!("Benchmark failed: {}", e))?;

    // Async operation benchmark
    let _ = suite.run_benchmark(
        "async_operation".to_string(),
        || async {
            // Simulate async operation
            tokio::time::sleep(Duration::from_millis(10)).await;
            Ok(())
        },
    ).await.map_err(|e| format!("Benchmark failed: {}", e))?;

    // Generate and display reports
    println!("\n📈 Benchmark Results:");
    println!("{}", suite.generate_report());

    // Export results
    println!("\n💾 Exporting benchmark results...");
    
    let json = suite.export_json()?;
    std::fs::write("simple_benchmark_results.json", json)?;
    println!("✅ Results exported to simple_benchmark_results.json");
    
    let csv = suite.export_csv();
    std::fs::write("simple_benchmark_results.csv", csv)?;
    println!("✅ Results exported to simple_benchmark_results.csv");

    println!("\n🎉 Simple benchmark example completed successfully!");

    Ok(())
}
