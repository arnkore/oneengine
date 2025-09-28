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


//! 向量化执行器驱动
//! 
//! 集成向量化算子、湖仓读取、pipeline调度、task调度的完整执行器

use crate::push_runtime::{event_loop::EventLoop, Event, PortId, OperatorId, SimpleMetricsCollector};
use crate::execution::operators::vectorized_filter::*;
use crate::execution::operators::vectorized_projector::*;
use crate::execution::operators::vectorized_aggregator::*;
use crate::io::vectorized_scan_operator::*;
use crate::execution::worker::Worker;
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, info, warn, error};
use anyhow::Result;

/// 向量化执行器配置
#[derive(Debug, Clone)]
pub struct VectorizedDriverConfig {
    /// 最大工作线程数
    pub max_workers: usize,
    /// 内存限制（字节）
    pub memory_limit: usize,
    /// 批次大小
    pub batch_size: usize,
    /// 是否启用向量化
    pub enable_vectorization: bool,
    /// 是否启用SIMD
    pub enable_simd: bool,
    /// 是否启用压缩
    pub enable_compression: bool,
    /// 是否启用预取
    pub enable_prefetch: bool,
    /// 是否启用NUMA感知
    pub enable_numa_aware: bool,
    // /// 是否启用自适应批量
    // pub enable_adaptive_batching: bool, // 暂时注释掉，只在example中使用
}

impl Default for VectorizedDriverConfig {
    fn default() -> Self {
        Self {
            max_workers: 4,
            memory_limit: 1024 * 1024 * 1024, // 1GB
            batch_size: 8192,
            enable_vectorization: true,
            enable_simd: true,
            enable_compression: true,
            enable_prefetch: true,
            enable_numa_aware: true,
            // enable_adaptive_batching: true, // 暂时注释掉，只在example中使用
        }
    }
}

/// 向量化执行器
pub struct VectorizedDriver {
    config: VectorizedDriverConfig,
    event_loop: EventLoop,
    workers: Vec<Worker>,
    query_plans: Vec<QueryPlan>,
    stats: DriverStats,
}

/// 查询计划
#[derive(Debug)]
pub struct QueryPlan {
    pub plan_id: u32,
    pub operators: Vec<OperatorNode>,
    pub connections: Vec<Connection>,
    pub input_files: Vec<String>,
    pub output_schema: SchemaRef,
}

/// 算子节点
#[derive(Debug)]
pub struct OperatorNode {
    pub operator_id: OperatorId,
    pub operator_type: OperatorType,
    pub input_ports: Vec<PortId>,
    pub output_ports: Vec<PortId>,
    pub config: OperatorConfig,
}

/// 算子类型
#[derive(Debug, Clone)]
pub enum OperatorType {
    Scan { file_path: String },
    Filter { predicate: FilterPredicate, column_index: usize },
    Project { expressions: Vec<ProjectionExpression>, output_schema: SchemaRef },
    Aggregate { group_columns: Vec<usize>, agg_functions: Vec<AggregationFunction> },
    Sort { sort_columns: Vec<SortColumn> },
    Join { join_type: JoinType, left_keys: Vec<usize>, right_keys: Vec<usize> },
}

/// 算子配置
#[derive(Debug, Clone)]
pub enum OperatorConfig {
    ScanConfig(VectorizedScanConfig),
    FilterConfig(VectorizedFilterConfig),
    ProjectorConfig(VectorizedProjectorConfig),
    AggregatorConfig(VectorizedAggregatorConfig),
}

/// 连接
#[derive(Debug)]
pub struct Connection {
    pub from_operator: OperatorId,
    pub from_port: PortId,
    pub to_operator: OperatorId,
    pub to_port: PortId,
}

/// 驱动统计信息
#[derive(Debug, Default)]
pub struct DriverStats {
    pub total_queries_executed: u64,
    pub total_execution_time: std::time::Duration,
    pub avg_execution_time: std::time::Duration,
    pub peak_memory_usage: usize,
    pub total_rows_processed: u64,
    pub vectorization_speedup: f64,
    pub simd_utilization: f64,
    pub cache_hit_rate: f64,
}

impl VectorizedDriver {
    pub fn new(config: VectorizedDriverConfig) -> Self {
        let metrics = Arc::new(SimpleMetricsCollector::default());
        let event_loop = EventLoop::new(metrics);
        
        Self {
            config,
            event_loop,
            workers: Vec::new(),
            query_plans: Vec::new(),
            stats: DriverStats::default(),
        }
    }

    /// 启动驱动
    pub async fn start(&mut self) -> Result<()> {
        info!("启动向量化执行器驱动");
        
        // 创建工作线程
        for i in 0..self.config.max_workers {
            let worker = Worker::new(i);
            self.workers.push(worker);
        }
        
        // 启动工作线程
        for worker in &self.workers {
            worker.start().await?;
        }
        
        info!("向量化执行器驱动启动完成，工作线程数: {}", self.config.max_workers);
        Ok(())
    }

    /// 停止驱动
    pub async fn stop(&mut self) -> Result<()> {
        info!("停止向量化执行器驱动");
        
        // 停止工作线程
        for worker in &self.workers {
            worker.stop().await?;
        }
        
        info!("向量化执行器驱动停止完成");
        Ok(())
    }

    /// 执行查询计划
    pub async fn execute_query(&mut self, query_plan: QueryPlan) -> Result<Vec<RecordBatch>, String> {
        let start = Instant::now();
        
        info!("开始执行查询计划: {}", query_plan.plan_id);
        
        // 注册所有算子
        self.register_operators(&query_plan)?;
        
        // 建立连接
        self.establish_connections(&query_plan)?;
        
        // 执行查询
        let result = self.execute_plan(&query_plan).await?;
        
        let duration = start.elapsed();
        self.update_stats(duration);
        
        info!("查询计划执行完成: {} rows ({}μs)", 
              result.len(), duration.as_micros());
        
        Ok(result)
    }

    /// 注册算子
    fn register_operators(&mut self, query_plan: &QueryPlan) -> Result<(), String> {
        for node in &query_plan.operators {
            match &node.operator_type {
                OperatorType::Scan { file_path } => {
                    let scan_config = match &node.config {
                        OperatorConfig::ScanConfig(config) => config.clone(),
                        _ => return Err("Invalid config for scan operator".to_string()),
                    };
                    
                    let mut scan_operator = VectorizedScanOperator::new(
                        scan_config,
                        node.operator_id,
                        node.input_ports.clone(),
                        node.output_ports.clone(),
                        format!("scan_{}", node.operator_id),
                    );
                    
                    scan_operator.set_file_path(file_path.clone());
                    self.event_loop.register_operator(
                        node.operator_id, 
                        Box::new(scan_operator),
                        vec![], // input ports
                        vec![0] // output ports
                    ).map_err(|e| e.to_string())?;
                },
                OperatorType::Filter { predicate, column_index } => {
                    let filter_config = match &node.config {
                        OperatorConfig::FilterConfig(config) => config.clone(),
                        _ => return Err("Invalid config for filter operator".to_string()),
                    };
                    
                    let mut filter_operator = VectorizedFilter::new(
                        filter_config,
                        predicate.clone(),
                        node.operator_id,
                        node.input_ports.clone(),
                        node.output_ports.clone(),
                        format!("filter_{}", node.operator_id),
                    );
                    
                    filter_operator.set_column_index(*column_index);
                    self.event_loop.register_operator(
                        node.operator_id, 
                        Box::new(filter_operator),
                        vec![0], // input ports
                        vec![1] // output ports
                    ).map_err(|e| e.to_string())?;
                },
                OperatorType::Project { expressions, output_schema } => {
                    let projector_config = match &node.config {
                        OperatorConfig::ProjectorConfig(config) => config.clone(),
                        _ => return Err("Invalid config for projector operator".to_string()),
                    };
                    
                    let projector_operator = VectorizedProjector::new(
                        projector_config,
                        expressions.clone(),
                        output_schema.clone(),
                        node.operator_id,
                        node.input_ports.clone(),
                        node.output_ports.clone(),
                        format!("projector_{}", node.operator_id),
                    );
                    
                    self.event_loop.register_operator(
                        node.operator_id, 
                        Box::new(projector_operator),
                        vec![1], // input ports
                        vec![2] // output ports
                    ).map_err(|e| e.to_string())?;
                },
                OperatorType::Aggregate { group_columns, agg_functions } => {
                    let aggregator_config = match &node.config {
                        OperatorConfig::AggregatorConfig(config) => config.clone(),
                        _ => return Err("Invalid config for aggregator operator".to_string()),
                    };
                    
                    let aggregator_operator = VectorizedAggregator::new(
                        aggregator_config,
                        group_columns.clone(),
                        agg_functions.clone(),
                        node.operator_id,
                        node.input_ports.clone(),
                        node.output_ports.clone(),
                        format!("aggregator_{}", node.operator_id),
                    );
                    
                    self.event_loop.register_operator(
                        node.operator_id, 
                        Box::new(aggregator_operator),
                        vec![2], // input ports
                        vec![3] // output ports
                    ).map_err(|e| e.to_string())?;
                },
                _ => {
                    return Err(format!("Unsupported operator type: {:?}", node.operator_type));
                }
            }
        }
        
        Ok(())
    }

    /// 建立连接
    fn establish_connections(&mut self, query_plan: &QueryPlan) -> Result<(), String> {
        for connection in &query_plan.connections {
            self.event_loop.add_port_mapping(connection.from_port, connection.to_operator);
        }
        Ok(())
    }

    /// 执行计划
    async fn execute_plan(&mut self, query_plan: &QueryPlan) -> Result<Vec<RecordBatch>, String> {
        let mut results = Vec::new();
        
        // 找到输入算子（通常是Scan算子）
        let input_operators: Vec<&OperatorNode> = query_plan.operators
            .iter()
            .filter(|node| matches!(node.operator_type, OperatorType::Scan { .. }))
            .collect();
        
        if input_operators.is_empty() {
            return Err("No input operators found".to_string());
        }
        
        // 启动输入算子
        for input_operator in input_operators {
            if let OperatorType::Scan { file_path } = &input_operator.operator_type {
                let start_scan_event = Event::StartScan { file_path: file_path.clone() };
                self.event_loop.process_event(start_scan_event)
                    .map_err(|e| format!("Event processing failed: {}", e))?;
            }
        }
        
        // 运行事件循环直到完成
        while !self.event_loop.is_finished() {
            // 处理所有待处理的事件
            while let Some(event) = self.event_loop.get_next_event() {
                match self.event_loop.process_event(event) {
                    Ok(_has_more) => {
                        // 继续处理下一个事件
                    },
                    Err(e) => {
                        error!("事件处理失败: {}", e);
                        return Err(format!("事件处理失败: {}", e));
                    }
                }
            }
            
            // 检查是否有算子需要处理
            let mut has_work = false;
            for worker in &self.workers {
                if worker.has_pending_tasks().await {
                    has_work = true;
                    break;
                }
            }
            
            if !has_work {
                // 等待一小段时间再检查
                tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
            }
            
            // 收集结果
            for worker in &self.workers {
                if let Some(batch) = worker.get_result().await {
                    results.push(batch);
                }
            }
        }
        
        Ok(results)
    }

    /// 创建简单的查询计划
    pub fn create_simple_query_plan(
        &self,
        file_path: String,
        filter_predicate: Option<FilterPredicate>,
        projection_expressions: Option<Vec<ProjectionExpression>>,
        aggregation: Option<(Vec<usize>, Vec<AggregationFunction>)>,
    ) -> QueryPlan {
        let mut operators = Vec::new();
        let mut connections = Vec::new();
        let mut port_counter = 0;
        
        // 创建Scan算子
        let scan_operator = OperatorNode {
            operator_id: 1,
            operator_type: OperatorType::Scan { file_path },
            input_ports: vec![],
            output_ports: vec![port_counter],
            config: OperatorConfig::ScanConfig(VectorizedScanConfig::default()),
        };
        operators.push(scan_operator);
        port_counter += 1;
        
        let mut current_operator_id = 1;
        let mut current_output_port = 0;
        
        // 添加Filter算子
        if let Some(predicate) = filter_predicate {
            current_operator_id += 1;
            let filter_operator = OperatorNode {
                operator_id: current_operator_id,
                operator_type: OperatorType::Filter { 
                    predicate, 
                    column_index: 2 // age列
                },
                input_ports: vec![current_output_port],
                output_ports: vec![port_counter],
                config: OperatorConfig::FilterConfig(VectorizedFilterConfig::default()),
            };
            operators.push(filter_operator);
            
            connections.push(Connection {
                from_operator: current_operator_id - 1,
                from_port: current_output_port,
                to_operator: current_operator_id,
                to_port: current_output_port,
            });
            
            current_output_port = port_counter;
            port_counter += 1;
        }
        
        // 添加Project算子
        if let Some(expressions) = projection_expressions {
            current_operator_id += 1;
            let projector_operator = OperatorNode {
                operator_id: current_operator_id,
                operator_type: OperatorType::Project { 
                    expressions, 
                    output_schema: Arc::new(Schema::new(vec![
                        Field::new("id", DataType::Int32, false),
                        Field::new("name", DataType::Utf8, false),
                        Field::new("age", DataType::Int32, false),
                    ]))
                },
                input_ports: vec![current_output_port],
                output_ports: vec![port_counter],
                config: OperatorConfig::ProjectorConfig(VectorizedProjectorConfig::default()),
            };
            operators.push(projector_operator);
            
            connections.push(Connection {
                from_operator: current_operator_id - 1,
                from_port: current_output_port,
                to_operator: current_operator_id,
                to_port: current_output_port,
            });
            
            current_output_port = port_counter;
            port_counter += 1;
        }
        
        // 添加Aggregate算子
        if let Some((group_columns, agg_functions)) = aggregation {
            current_operator_id += 1;
            let aggregator_operator = OperatorNode {
                operator_id: current_operator_id,
                operator_type: OperatorType::Aggregate { 
                    group_columns, 
                    agg_functions 
                },
                input_ports: vec![current_output_port],
                output_ports: vec![port_counter],
                config: OperatorConfig::AggregatorConfig(VectorizedAggregatorConfig::default()),
            };
            operators.push(aggregator_operator);
            
            connections.push(Connection {
                from_operator: current_operator_id - 1,
                from_port: current_output_port,
                to_operator: current_operator_id,
                to_port: current_output_port,
            });
        }
        
        QueryPlan {
            plan_id: 1,
            operators,
            connections,
            input_files: vec![],
            output_schema: Arc::new(Schema::new(vec![
                Field::new("result", DataType::Utf8, false),
            ])),
        }
    }

    /// 更新统计信息
    fn update_stats(&mut self, duration: std::time::Duration) {
        self.stats.total_queries_executed += 1;
        self.stats.total_execution_time += duration;
        
        if self.stats.total_queries_executed > 0 {
            self.stats.avg_execution_time = std::time::Duration::from_nanos(
                self.stats.total_execution_time.as_nanos() as u64 / self.stats.total_queries_executed
            );
        }
    }

    /// 获取统计信息
    pub fn get_stats(&self) -> &DriverStats {
        &self.stats
    }

    /// 重置统计信息
    pub fn reset_stats(&mut self) {
        self.stats = DriverStats::default();
    }
}

/// 排序列
#[derive(Debug, Clone)]
pub struct SortColumn {
    pub column_index: usize,
    pub ascending: bool,
    pub nulls_first: bool,
}

/// 连接类型
#[derive(Debug, Clone)]
pub enum JoinType {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
    LeftSemi,
    LeftAnti,
}
