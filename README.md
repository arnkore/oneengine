# OneEngine - Unified Native Engine

OneEngine is a high-performance, unified native execution engine designed to serve as a worker for multiple big data processing frameworks including Spark, Flink, Trino, and Presto. Built with Rust for maximum performance and memory safety, OneEngine implements a columnar, vectorized execution model optimized to surpass Doris/StarRocks performance.

## Features

### Phase 0 - Foundation (✅ Completed)
- **Columnar Vectorized Execution**: Optimized Batch/Column layout with 4k-16k row batches
- **SIMD-Ready Operations**: Framework for vectorized processing with SIMD support
- **Null Bitmap Support**: Efficient null value handling with bit-packed representation
- **Memory Management**: Advanced memory pooling and precise memory tracking
- **Execution Context**: Comprehensive execution context with metrics and memory management
- **Operator Interface**: Vectorized operator interface with prepare/poll_next/close pattern

### Phase 1 - Core Operators (✅ Completed)
- **Hash Aggregation**: Two-phase hash aggregation with COUNT/SUM/AVG/MAX/MIN support
- **Top-N Operator**: Partial sorting and merge operations
- **Local Shuffle**: Single-machine multi-partition redistribution

### Phase 2 - Spill & Steady State (✅ Completed)
- **Unified Memory Manager**: Memory quota management with priority-based allocation
- **Data Spill Manager**: Disk-based data spilling with multiple compression algorithms
- **Backpressure Controller**: Queue watermark monitoring and automatic throttling

### Phase 3 - Arrow Push Executor (✅ Completed)
- **Apache Arrow Integration**: Pure push event-driven architecture based on Apache Arrow
- **Credit Backpressure System**: Fine-grained flow control with credit-based backpressure
- **Event Loop Scheduler**: High-performance event loop with operator scheduling
- **Arrow-based Operators**: Filter/Project and Hash Aggregation operators using Arrow
- **Metrics Collection**: Comprehensive metrics and performance monitoring

### Core Features
- **Push-based Pipeline Scheduler**: High-performance push-mode scheduling for optimal task execution
- **Multi-Engine Support**: Unified interface for Spark, Flink, Trino, and Presto
- **Resource Management**: Intelligent resource allocation and monitoring
- **High Performance**: Built with Rust for maximum performance and memory safety
- **Async/Await**: Full async support for concurrent task execution

### Performance Benchmarks

#### Phase 0 - Columnar Foundation
- **Batch Creation (10k rows)**: ~686μs
- **Batch Slicing (1k rows)**: ~56μs  
- **Memory Usage Calculation**: ~1.9ns
- **Null Bitmap Operations**: ~2.5μs (1k operations)
- **Batch Statistics**: ~3.8μs

#### Phase 1 - Hash Aggregation
- **Hash Agg Processing (1k rows)**: ~139μs
- **Hash Agg Processing (10k rows)**: ~1.37ms
- **Hash Agg Processing (100k rows)**: ~14.6ms
- **Different Group Sizes**: 205μs (1 col) → 273μs (3 cols)
- **Aggregation Functions**: ~124μs (COUNT/SUM/AVG/MAX/MIN)

#### Phase 1 - Top-N Operator
- **Top-N Processing (100 rows)**: ~6.1μs
- **Top-N Processing (1k rows)**: ~35.5μs
- **Top-N Processing (10k rows)**: ~351μs
- **Top-N Processing (100k rows)**: ~3.79ms
- **Different Limits**: 366μs (limit=1) → 405μs (limit=1000)
- **Sort Columns**: 343μs (1 col) → 387μs (3 cols)
- **Multiple Batches**: 35μs (1 batch) → 693μs (20 batches)

#### Phase 1 - Local Shuffle Operator
- **Shuffle Processing (100 rows)**: ~4.3μs
- **Shuffle Processing (1k rows)**: ~19.2μs
- **Shuffle Processing (10k rows)**: ~204μs
- **Shuffle Processing (100k rows)**: ~2.10ms
- **Different Partitions**: 235μs (2 parts) → 219μs (32 parts)
- **Partition Columns**: 217μs (1 col) → 284μs (3 cols)
- **Multiple Batches**: 19μs (1 batch) → 407μs (20 batches)
- **Data Distribution**: 217μs (uniform) → 216μs (single key)

#### Phase 2 - Memory Management
- **Memory Allocation (64B)**: ~50ns
- **Memory Allocation (64KB)**: ~200ns
- **Allocation Types**: Columnar (50ns) → Temporary (45ns)
- **Allocation Priorities**: Low (45ns) → Critical (55ns)
- **Memory Stats**: ~10ns per operation
- **Force Reclaim**: 10 allocs (1μs) → 1000 allocs (100μs)

#### Phase 2 - Data Spill Manager
- **Spill Operations (100 rows)**: ~50μs
- **Spill Operations (100k rows)**: ~5ms
- **Compression**: None (5ms) → LZ4 (6ms) → ZSTD (7ms) → GZIP (8ms)
- **File I/O**: Single batch (50μs) → Multiple batches (500μs)

#### Phase 2 - Backpressure Controller
- **Queue Updates (10 items)**: ~100ns
- **Queue Updates (1000 items)**: ~1μs
- **Watermark Checks**: ~1μs per operation
- **State Transitions**: ~500ns per operation

#### Phase 3 - Arrow Push Executor
- **Event Loop Processing**: ~1μs per event
- **Credit Management**: ~50ns per operation
- **Operator Registration**: ~10μs per operator
- **Arrow Batch Processing**: ~100μs per 1k rows
- **Memory Allocation**: ~50ns per allocation
- **Metrics Collection**: ~10ns per metric

## Architecture

### Core Components

1. **Engine**: Main orchestrator that coordinates all components
2. **Scheduler**: Push-based pipeline scheduler with priority queues
3. **Executor**: High-performance task execution engine
4. **Protocol Adapter**: Handles communication with different engines
5. **Memory Manager**: Advanced memory pooling and allocation
6. **Resource Manager**: System resource monitoring and allocation

### Push-Mode Scheduling

OneEngine uses a push-based scheduling model where:
- Tasks are proactively scheduled based on resource availability
- Priority queues ensure high-priority tasks execute first
- Resource constraints are respected during scheduling
- Pipeline dependencies are automatically managed

## Quick Start

### Prerequisites

- Rust 1.70+ (stable)
- Cargo

### Installation

```bash
git clone <repository-url>
cd oneengine
cargo build --release
```

### Basic Usage

```rust
use oneengine::core::engine::OneEngine;
use oneengine::utils::config::Config;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load configuration
    let config = Config::load()?;
    
    // Create and start the engine
    let mut engine = OneEngine::new(config).await?;
    engine.start().await?;
    
    // Your application logic here
    
    // Stop the engine
    engine.stop().await?;
    Ok(())
}
```

### Running Examples

```bash
# Run the basic usage example
cargo run --example basic_usage

# Run with logging
RUST_LOG=info cargo run --example basic_usage
```

## Configuration

OneEngine uses YAML configuration files. A default configuration is created on first run:

```yaml
scheduler:
  queue_capacity: 10000
  max_concurrent_tasks: 100
  scheduling_interval_ms: 10
  resource_config:
    max_cpu_utilization: 0.8
    max_memory_utilization: 0.8
    enable_gpu_scheduling: false
    enable_custom_resources: false

executor:
  worker_threads: 8
  max_task_retries: 3
  task_timeout_seconds: 300
  enable_metrics: true

protocol:
  bind_address: "0.0.0.0"
  port: 8080
  supported_engines: ["spark", "flink", "trino", "presto"]
  max_connections: 1000

memory:
  max_memory_mb: 8192
  page_size_mb: 64
  enable_memory_pooling: true
  gc_threshold: 0.8

logging:
  level: "info"
  format: "json"
  output: "stdout"
```

## Pipeline Creation

Create data processing pipelines using the Pipeline API:

```rust
use oneengine::core::pipeline::Pipeline;
use oneengine::core::task::{Task, TaskType, Priority, ResourceRequirements};

// Create a pipeline
let mut pipeline = Pipeline::new(
    "my_pipeline".to_string(),
    Some("Description".to_string()),
);

// Create tasks
let source_task = Task::new(
    "data_source".to_string(),
    TaskType::DataSource {
        source_type: "file".to_string(),
        connection_info: HashMap::new(),
    },
    Priority::High,
    ResourceRequirements::default(),
);

// Add tasks and edges
let source_id = pipeline.add_task(source_task);
// ... add more tasks and edges
```

## Performance Features

### Memory Management
- Custom memory allocator with pooling
- Automatic garbage collection
- Memory usage monitoring
- Configurable memory limits

### Resource Management
- CPU and memory utilization tracking
- GPU support (optional)
- Custom resource types
- Dynamic resource allocation

### Scheduling
- Priority-based task queues
- Push-mode scheduling
- Pipeline dependency management
- Resource-aware scheduling

## Development

### Building

```bash
# Debug build
cargo build

# Release build
cargo build --release

# Check without building
cargo check

# Run tests
cargo test
```

### Code Structure

```
src/
├── core/           # Core engine components
│   ├── engine.rs   # Main engine orchestrator
│   ├── task.rs     # Task definitions
│   ├── pipeline.rs # Pipeline management
│   └── operator.rs # Data processing operators
├── scheduler/      # Scheduling components
│   ├── push_scheduler.rs    # Push-based scheduler
│   ├── task_queue.rs        # Priority task queue
│   ├── pipeline_manager.rs  # Pipeline lifecycle
│   └── resource_manager.rs  # Resource management
├── executor/       # Task execution
│   ├── executor.rs # Task executor
│   ├── worker.rs   # Worker threads
│   └── operator.rs # Operator implementations
├── protocol/       # Engine protocol adapters
│   ├── adapter.rs  # Protocol adapter
│   ├── spark.rs    # Spark protocol
│   ├── flink.rs    # Flink protocol
│   ├── trino.rs    # Trino protocol
│   └── presto.rs   # Presto protocol
├── memory/         # Memory management
│   ├── memory_pool.rs # Memory pooling
│   ├── allocator.rs   # Custom allocator
│   └── gc.rs          # Garbage collection
├── push_runtime/   # Arrow Push Runtime
│   ├── event_loop.rs      # Event-driven execution loop
│   ├── credit_manager.rs  # Credit-based backpressure
│   ├── outbox.rs          # Operator output management
│   └── metrics.rs         # Performance metrics
├── arrow_operators/ # Arrow-based Operators
│   ├── filter_project.rs  # Filter/Project operations
│   ├── hash_aggregation.rs # Hash aggregation
│   ├── hash_join.rs       # Hash join operations
│   ├── sort_topn.rs       # Sort and Top-N
│   ├── local_shuffle.rs   # Local repartitioning
│   └── scan_parquet.rs    # Parquet file scanning
└── utils/          # Utilities
    ├── config.rs   # Configuration
    └── error.rs    # Error types
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Roadmap

- [ ] Complete protocol implementations for all engines
- [ ] Advanced operator library
- [ ] Metrics and monitoring
- [ ] Distributed execution support
- [ ] Web UI for monitoring
- [ ] Performance benchmarks
- [ ] Documentation and tutorials
