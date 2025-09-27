//! 端到端集成示例
//! 
//! 演示从Task调度、Driver调度、Pipeline执行、Pipeline调度框架、Operator算子到湖仓列式内容读取的完整打通

use oneengine::core::engine::OneEngine;
use oneengine::core::task::{Task, TaskType, Priority, ResourceRequirements};
use oneengine::core::pipeline::{Pipeline, PipelineEdge, EdgeType};
use oneengine::utils::config::Config;
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use uuid::Uuid;
use chrono::Utc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 端到端集成示例");
    println!("===============================================");
    println!("演示完整的执行链路：Task调度 -> Driver调度 -> Pipeline执行 -> Operator算子 -> 湖仓读取");
    println!();

    // 1. 创建OneEngine配置
    let config = create_engine_config();
    println!("✅ 创建OneEngine配置完成");

    // 2. 初始化OneEngine
    let mut engine = OneEngine::new(config).await?;
    println!("✅ OneEngine初始化完成");

    // 3. 启动OneEngine
    engine.start().await?;
    println!("✅ OneEngine启动完成");

    // 4. 测试1: 单个Task执行
    println!("\n📋 测试1: 单个Task执行");
    test_single_task_execution(&engine).await?;

    // 5. 测试2: Pipeline执行
    println!("\n📋 测试2: Pipeline执行");
    test_pipeline_execution(&engine).await?;

    // 6. 测试3: 复杂Pipeline执行
    println!("\n📋 测试3: 复杂Pipeline执行");
    test_complex_pipeline_execution(&engine).await?;

    // 7. 测试4: 湖仓集成测试
    println!("\n📋 测试4: 湖仓集成测试");
    test_data_lake_integration(&engine).await?;

    // 8. 测试5: 性能基准测试
    println!("\n📋 测试5: 性能基准测试");
    test_performance_benchmark(&engine).await?;

    // 9. 停止OneEngine
    engine.stop().await?;
    println!("✅ OneEngine停止完成");

    println!("\n🎉 端到端集成测试完成！");
    println!("✅ Task调度系统已打通");
    println!("✅ Driver调度系统已打通");
    println!("✅ Pipeline执行系统已打通");
    println!("✅ Pipeline调度框架已打通");
    println!("✅ Operator算子系统已打通");
    println!("✅ 湖仓列式读取系统已打通");

    Ok(())
}

/// 创建OneEngine配置
fn create_engine_config() -> Config {
    use oneengine::utils::config::*;
    
    Config {
        scheduler: SchedulerConfig {
            max_concurrent_tasks: 100,
            task_queue_size: 1000,
            worker_threads: 4,
            enable_priority_scheduling: true,
            enable_load_balancing: true,
        },
        executor: ExecutorConfig {
            max_workers: 4,
            memory_limit: 1024 * 1024 * 1024, // 1GB
            batch_size: 8192,
            enable_vectorization: true,
            enable_simd: true,
            enable_compression: true,
        },
        protocol: ProtocolConfig {
            port: 8080,
            max_connections: 100,
            enable_compression: true,
            timeout_seconds: 30,
        },
    }
}

/// 测试单个Task执行
async fn test_single_task_execution(engine: &OneEngine) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    
    // 创建数据源Task
    let data_source_task = Task {
        id: Uuid::new_v4(),
        name: "data_source_task".to_string(),
        task_type: TaskType::DataSource {
            source_type: "parquet://test.parquet".to_string(),
            connection_info: HashMap::new(),
        },
        priority: Priority::Normal,
        resource_requirements: ResourceRequirements {
            cpu_cores: 1,
            memory_mb: 100,
            disk_mb: 0,
            network_bandwidth_mbps: None,
            gpu_cores: None,
            custom_resources: HashMap::new(),
        },
        dependencies: vec![],
        created_at: Utc::now(),
        deadline: None,
        metadata: HashMap::new(),
    };
    
    println!("   执行数据源Task...");
    let result = engine.execute_task(data_source_task).await?;
    println!("   ✅ 数据源Task执行完成，结果行数: {}", result.num_rows());
    
    // 创建数据处理Task
    let processing_task = Task {
        id: Uuid::new_v4(),
        name: "processing_task".to_string(),
        task_type: TaskType::DataProcessing {
            operator: "filter".to_string(),
            input_schema: Some("id:Int32,value:Float64".to_string()),
            output_schema: Some("id:Int32".to_string()),
        },
        priority: Priority::High,
        resource_requirements: ResourceRequirements {
            cpu_cores: 2,
            memory_mb: 200,
            disk_mb: 0,
            network_bandwidth_mbps: None,
            gpu_cores: None,
            custom_resources: HashMap::new(),
        },
        dependencies: vec![],
        created_at: Utc::now(),
        deadline: None,
        metadata: HashMap::new(),
    };
    
    println!("   执行数据处理Task...");
    let result = engine.execute_task(processing_task).await?;
    println!("   ✅ 数据处理Task执行完成，结果行数: {}", result.num_rows());
    
    let duration = start.elapsed();
    println!("   ⏱️  单个Task执行耗时: {:?}", duration);
    
    Ok(())
}

/// 测试Pipeline执行
async fn test_pipeline_execution(engine: &OneEngine) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    
    // 创建Pipeline
    let pipeline = create_simple_pipeline();
    
    println!("   执行简单Pipeline...");
    let results = engine.execute_pipeline(pipeline).await?;
    println!("   ✅ Pipeline执行完成，结果批次数: {}", results.len());
    
    for (i, batch) in results.iter().enumerate() {
        println!("      批次 {}: {} 行, {} 列", i, batch.num_rows(), batch.num_columns());
    }
    
    let duration = start.elapsed();
    println!("   ⏱️  Pipeline执行耗时: {:?}", duration);
    
    Ok(())
}

/// 测试复杂Pipeline执行
async fn test_complex_pipeline_execution(engine: &OneEngine) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    
    // 创建复杂Pipeline
    let pipeline = create_complex_pipeline();
    
    println!("   执行复杂Pipeline...");
    let results = engine.execute_pipeline(pipeline).await?;
    println!("   ✅ 复杂Pipeline执行完成，结果批次数: {}", results.len());
    
    for (i, batch) in results.iter().enumerate() {
        println!("      批次 {}: {} 行, {} 列", i, batch.num_rows(), batch.num_columns());
    }
    
    let duration = start.elapsed();
    println!("   ⏱️  复杂Pipeline执行耗时: {:?}", duration);
    
    Ok(())
}

/// 测试湖仓集成
async fn test_data_lake_integration(engine: &OneEngine) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    
    // 创建湖仓数据源Task
    let data_lake_task = Task {
        id: Uuid::new_v4(),
        name: "data_lake_task".to_string(),
        task_type: TaskType::DataSource {
            source_type: "iceberg://test_table".to_string(),
            connection_info: HashMap::from([
                ("catalog".to_string(), "hive".to_string()),
                ("database".to_string(), "test_db".to_string()),
                ("table".to_string(), "test_table".to_string()),
            ]),
        },
        priority: Priority::High,
        resource_requirements: ResourceRequirements {
            cpu_cores: 4,
            memory_mb: 500,
            disk_mb: 0,
            network_bandwidth_mbps: Some(1000),
            gpu_cores: None,
            custom_resources: HashMap::new(),
        },
        dependencies: vec![],
        created_at: Utc::now(),
        deadline: None,
        metadata: HashMap::new(),
    };
    
    println!("   执行湖仓数据源Task...");
    let result = engine.execute_task(data_lake_task).await?;
    println!("   ✅ 湖仓数据源Task执行完成，结果行数: {}", result.num_rows());
    
    let duration = start.elapsed();
    println!("   ⏱️  湖仓集成测试耗时: {:?}", duration);
    
    Ok(())
}

/// 测试性能基准
async fn test_performance_benchmark(engine: &OneEngine) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    
    // 创建性能测试Pipeline
    let pipeline = create_performance_test_pipeline();
    
    println!("   执行性能测试Pipeline...");
    let results = engine.execute_pipeline(pipeline).await?;
    
    let total_rows: usize = results.iter().map(|batch| batch.num_rows()).sum();
    let total_columns = results.first().map(|batch| batch.num_columns()).unwrap_or(0);
    
    println!("   ✅ 性能测试完成:");
    println!("      总行数: {}", total_rows);
    println!("      总列数: {}", total_columns);
    println!("      批次数: {}", results.len());
    
    let duration = start.elapsed();
    let throughput = total_rows as f64 / duration.as_secs_f64();
    println!("   ⏱️  性能测试耗时: {:?}", duration);
    println!("   📊 吞吐量: {:.2} 行/秒", throughput);
    
    Ok(())
}

/// 创建简单Pipeline
fn create_simple_pipeline() -> Pipeline {
    let task1 = Task {
        id: Uuid::new_v4(),
        name: "scan_task".to_string(),
        task_type: TaskType::DataSource {
            source_type: "parquet://input.parquet".to_string(),
            connection_info: HashMap::new(),
        },
        priority: Priority::Normal,
        resource_requirements: ResourceRequirements {
            cpu_cores: 1,
            memory_mb: 100,
            disk_mb: 0,
            network_bandwidth_mbps: None,
            gpu_cores: None,
            custom_resources: HashMap::new(),
        },
        dependencies: vec![],
        created_at: Utc::now(),
        deadline: None,
        metadata: HashMap::new(),
    };
    
    let task2 = Task {
        id: Uuid::new_v4(),
        name: "filter_task".to_string(),
        task_type: TaskType::DataProcessing {
            operator: "filter".to_string(),
            input_schema: Some("id:Int32,value:Float64".to_string()),
            output_schema: Some("id:Int32,value:Float64".to_string()),
        },
        priority: Priority::Normal,
        resource_requirements: ResourceRequirements {
            cpu_cores: 2,
            memory_mb: 200,
            disk_mb: 0,
            network_bandwidth_mbps: None,
            gpu_cores: None,
            custom_resources: HashMap::new(),
        },
        dependencies: vec![task1.id],
        created_at: Utc::now(),
        deadline: None,
        metadata: HashMap::new(),
    };
    
    let edge = PipelineEdge {
        from_task: task1.id,
        to_task: task2.id,
        edge_type: EdgeType::DataFlow,
        data_schema: Some("id:Int32,value:Float64".to_string()),
    };
    
    Pipeline {
        id: Uuid::new_v4(),
        name: "simple_pipeline".to_string(),
        description: Some("简单的Scan -> Filter Pipeline".to_string()),
        tasks: vec![task1, task2],
        edges: vec![edge],
        created_at: Utc::now(),
        metadata: HashMap::new(),
    }
}

/// 创建复杂Pipeline
fn create_complex_pipeline() -> Pipeline {
    let scan_task = Task {
        id: Uuid::new_v4(),
        name: "scan_task".to_string(),
        task_type: TaskType::DataSource {
            source_type: "parquet://input.parquet".to_string(),
            connection_info: HashMap::new(),
        },
        priority: Priority::Normal,
        resource_requirements: ResourceRequirements {
            cpu_cores: 1,
            memory_mb: 100,
            disk_mb: 0,
            network_bandwidth_mbps: None,
            gpu_cores: None,
            custom_resources: HashMap::new(),
        },
        dependencies: vec![],
        created_at: Utc::now(),
        deadline: None,
        metadata: HashMap::new(),
    };
    
    let filter_task = Task {
        id: Uuid::new_v4(),
        name: "filter_task".to_string(),
        task_type: TaskType::DataProcessing {
            operator: "filter".to_string(),
            input_schema: Some("id:Int32,value:Float64".to_string()),
            output_schema: Some("id:Int32,value:Float64".to_string()),
        },
        priority: Priority::Normal,
        resource_requirements: ResourceRequirements {
            cpu_cores: 2,
            memory_mb: 200,
            disk_mb: 0,
            network_bandwidth_mbps: None,
            gpu_cores: None,
            custom_resources: HashMap::new(),
        },
        dependencies: vec![scan_task.id],
        created_at: Utc::now(),
        deadline: None,
        metadata: HashMap::new(),
    };
    
    let project_task = Task {
        id: Uuid::new_v4(),
        name: "project_task".to_string(),
        task_type: TaskType::DataProcessing {
            operator: "project".to_string(),
            input_schema: Some("id:Int32,value:Float64".to_string()),
            output_schema: Some("id:Int32".to_string()),
        },
        priority: Priority::Normal,
        resource_requirements: ResourceRequirements {
            cpu_cores: 1,
            memory_mb: 100,
            disk_mb: 0,
            network_bandwidth_mbps: None,
            gpu_cores: None,
            custom_resources: HashMap::new(),
        },
        dependencies: vec![filter_task.id],
        created_at: Utc::now(),
        deadline: None,
        metadata: HashMap::new(),
    };
    
    let aggregate_task = Task {
        id: Uuid::new_v4(),
        name: "aggregate_task".to_string(),
        task_type: TaskType::DataProcessing {
            operator: "aggregate".to_string(),
            input_schema: Some("id:Int32".to_string()),
            output_schema: Some("count:Int64".to_string()),
        },
        priority: Priority::High,
        resource_requirements: ResourceRequirements {
            cpu_cores: 4,
            memory_mb: 500,
            disk_mb: 0,
            network_bandwidth_mbps: None,
            gpu_cores: None,
            custom_resources: HashMap::new(),
        },
        dependencies: vec![project_task.id],
        created_at: Utc::now(),
        deadline: None,
        metadata: HashMap::new(),
    };
    
    let edges = vec![
        PipelineEdge {
            from_task: scan_task.id,
            to_task: filter_task.id,
            edge_type: EdgeType::DataFlow,
            data_schema: Some("id:Int32,value:Float64".to_string()),
        },
        PipelineEdge {
            from_task: filter_task.id,
            to_task: project_task.id,
            edge_type: EdgeType::DataFlow,
            data_schema: Some("id:Int32,value:Float64".to_string()),
        },
        PipelineEdge {
            from_task: project_task.id,
            to_task: aggregate_task.id,
            edge_type: EdgeType::DataFlow,
            data_schema: Some("id:Int32".to_string()),
        },
    ];
    
    Pipeline {
        id: Uuid::new_v4(),
        name: "complex_pipeline".to_string(),
        description: Some("复杂的Scan -> Filter -> Project -> Aggregate Pipeline".to_string()),
        tasks: vec![scan_task, filter_task, project_task, aggregate_task],
        edges,
        created_at: Utc::now(),
        metadata: HashMap::new(),
    }
}

/// 创建性能测试Pipeline
fn create_performance_test_pipeline() -> Pipeline {
    let scan_task = Task {
        id: Uuid::new_v4(),
        name: "performance_scan_task".to_string(),
        task_type: TaskType::DataSource {
            source_type: "parquet://large_input.parquet".to_string(),
            connection_info: HashMap::new(),
        },
        priority: Priority::High,
        resource_requirements: ResourceRequirements {
            cpu_cores: 8,
            memory_mb: 2000,
            disk_mb: 0,
            network_bandwidth_mbps: Some(10000),
            gpu_cores: None,
            custom_resources: HashMap::new(),
        },
        dependencies: vec![],
        created_at: Utc::now(),
        deadline: None,
        metadata: HashMap::new(),
    };
    
    let filter_task = Task {
        id: Uuid::new_v4(),
        name: "performance_filter_task".to_string(),
        task_type: TaskType::DataProcessing {
            operator: "filter".to_string(),
            input_schema: Some("id:Int32,value:Float64".to_string()),
            output_schema: Some("id:Int32,value:Float64".to_string()),
        },
        priority: Priority::High,
        resource_requirements: ResourceRequirements {
            cpu_cores: 8,
            memory_mb: 2000,
            disk_mb: 0,
            network_bandwidth_mbps: None,
            gpu_cores: None,
            custom_resources: HashMap::new(),
        },
        dependencies: vec![scan_task.id],
        created_at: Utc::now(),
        deadline: None,
        metadata: HashMap::new(),
    };
    
    let edges = vec![
        PipelineEdge {
            from_task: scan_task.id,
            to_task: filter_task.id,
            edge_type: EdgeType::DataFlow,
            data_schema: Some("id:Int32,value:Float64".to_string()),
        },
    ];
    
    Pipeline {
        id: Uuid::new_v4(),
        name: "performance_test_pipeline".to_string(),
        description: Some("性能测试Pipeline".to_string()),
        tasks: vec![scan_task, filter_task],
        edges,
        created_at: Utc::now(),
        metadata: HashMap::new(),
    }
}
