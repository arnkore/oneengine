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

//! 工作窃取调度器
//! 
//! 基于crossbeam-deque的高性能工作窃取调度器实现

use crate::execution::task::{Task, TaskType, Priority, ResourceRequirements};
use crate::execution::pipeline::Pipeline;
use uuid::Uuid;
use crate::execution::vectorized_driver::VectorizedDriver;
use anyhow::Result;
use crossbeam::deque::{Injector, Steal, Stealer};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::RwLock;
use tracing::{debug, info, error};

/// 工作窃取调度器
pub struct WorkStealingScheduler {
    /// 全局任务注入器
    injector: Arc<Injector<Task>>,
    /// 工作线程数量
    num_workers: usize,
    /// 统计信息
    stats: Arc<WorkStealingStats>,
    /// 向量化驱动
    vectorized_driver: Arc<RwLock<Option<VectorizedDriver>>>,
}

/// 工作窃取统计信息
#[derive(Debug, Default)]
pub struct WorkStealingStats {
    /// 总任务数
    pub total_tasks: AtomicU64,
    /// 已完成任务数
    pub completed_tasks: AtomicU64,
    /// 窃取次数
    pub steal_count: AtomicU64,
    /// 平均执行时间
    pub avg_execution_time: AtomicU64,
    /// 空闲时间
    pub idle_time: AtomicU64,
}

impl Clone for WorkStealingStats {
    fn clone(&self) -> Self {
        Self {
            total_tasks: AtomicU64::new(self.total_tasks.load(Ordering::Relaxed)),
            completed_tasks: AtomicU64::new(self.completed_tasks.load(Ordering::Relaxed)),
            steal_count: AtomicU64::new(self.steal_count.load(Ordering::Relaxed)),
            avg_execution_time: AtomicU64::new(self.avg_execution_time.load(Ordering::Relaxed)),
            idle_time: AtomicU64::new(self.idle_time.load(Ordering::Relaxed)),
        }
    }
}

impl WorkStealingScheduler {
    /// 创建新的工作窃取调度器
    pub fn new(num_workers: usize) -> Self {
        Self {
            injector: Arc::new(Injector::new()),
            num_workers,
            stats: Arc::new(WorkStealingStats::default()),
            vectorized_driver: Arc::new(RwLock::new(None)),
        }
    }

    /// 设置向量化驱动
    pub async fn set_vectorized_driver(&self, driver: VectorizedDriver) {
        let mut driver_guard = self.vectorized_driver.write().await;
        *driver_guard = Some(driver);
    }

    /// 启动工作窃取调度器
    pub fn start(&self) {
        let injector = Arc::clone(&self.injector);
        let stats = Arc::clone(&self.stats);
        let vectorized_driver = Arc::clone(&self.vectorized_driver);

        // 启动工作线程
        for worker_id in 0..self.num_workers {
            let injector = Arc::clone(&injector);
            let stats = Arc::clone(&stats);
            let vectorized_driver = Arc::clone(&vectorized_driver);

            thread::spawn(move || {
                Self::worker_loop(worker_id, injector, stats, vectorized_driver);
            });
        }

        // 启动监控线程
        let stats = Arc::clone(&self.stats);
        thread::spawn(move || {
            Self::monitor_loop(stats);
        });
    }

    /// 提交任务
    pub fn submit_task(&self, task: Task) -> Result<()> {
        self.injector.push(task);
        self.stats.total_tasks.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// 提交管道
    pub async fn submit_pipeline(&self, pipeline: Pipeline) -> Result<()> {
        let tasks = self.pipeline_to_tasks(pipeline).await?;
        
        let task_count = tasks.len();
        for task in tasks {
            self.submit_task(task)?;
        }
        
        info!("Pipeline submitted with {} tasks", task_count);
        Ok(())
    }

    /// 将管道转换为任务
    async fn pipeline_to_tasks(&self, pipeline: Pipeline) -> Result<Vec<Task>> {
        // 这里应该根据管道的结构生成任务
        // 简化实现：为每个算子创建一个任务
        let mut tasks = Vec::new();
        
        for (i, _operator) in pipeline.tasks.iter().enumerate() {
            let task = Task {
                id: Uuid::new_v4(),
                name: format!("task_{}", i),
                task_type: TaskType::DataProcessing {
                    operator: "vectorized_operator".to_string(),
                    input_schema: None,
                    output_schema: None,
                },
                priority: Priority::Normal,
                resource_requirements: ResourceRequirements::default(),
                dependencies: Vec::new(),
                created_at: chrono::Utc::now(),
                deadline: None,
                metadata: std::collections::HashMap::new(),
            };
            tasks.push(task);
        }
        
        Ok(tasks)
    }

    /// 工作线程主循环
    fn worker_loop(
        worker_id: usize,
        injector: Arc<Injector<Task>>,
        stats: Arc<WorkStealingStats>,
        vectorized_driver: Arc<RwLock<Option<VectorizedDriver>>>,
    ) {
        let mut idle_start = Instant::now();
        let mut execution_times = Vec::new();

        loop {
            // 尝试从全局队列获取任务
            if let Steal::Success(task) = injector.steal() {
                let execution_start = Instant::now();
                
                if let Err(e) = Self::execute_task(task, &vectorized_driver) {
                    error!("Worker {} failed to execute task: {}", worker_id, e);
                }
                
                let execution_time = execution_start.elapsed();
                execution_times.push(execution_time.as_micros() as u64);
                
                stats.completed_tasks.fetch_add(1, Ordering::Relaxed);
                Self::update_avg_execution_time(&stats, &execution_times);
                
                continue;
            }

            // 没有任务可执行，记录空闲时间
            let idle_duration = idle_start.elapsed();
            stats.idle_time.fetch_add(idle_duration.as_micros() as u64, Ordering::Relaxed);
            
            // 短暂休眠避免忙等待
            thread::sleep(Duration::from_millis(1));
            idle_start = Instant::now();
        }
    }

    /// 执行任务
    fn execute_task(_task: Task, _vectorized_driver: &Arc<RwLock<Option<VectorizedDriver>>>) -> Result<()> {
        // 这里应该根据任务类型执行相应的操作
        // 简化实现：模拟任务执行
        debug!("Executing task {}", _task.id);
        
        // 在实际实现中，这里应该：
        // 1. 根据任务类型调用相应的算子
        // 2. 处理输入数据
        // 3. 生成输出数据
        
        Ok(())
    }

    /// 更新平均执行时间
    fn update_avg_execution_time(stats: &WorkStealingStats, execution_times: &[u64]) {
        if !execution_times.is_empty() {
            let sum: u64 = execution_times.iter().sum();
            let avg = sum / execution_times.len() as u64;
            stats.avg_execution_time.store(avg, Ordering::Relaxed);
        }
    }

    /// 监控循环
    fn monitor_loop(stats: Arc<WorkStealingStats>) {
        loop {
            thread::sleep(Duration::from_secs(10));
            
            let total = stats.total_tasks.load(Ordering::Relaxed);
            let completed = stats.completed_tasks.load(Ordering::Relaxed);
            let steals = stats.steal_count.load(Ordering::Relaxed);
            let avg_time = stats.avg_execution_time.load(Ordering::Relaxed);
            let idle_time = stats.idle_time.load(Ordering::Relaxed);
            
            info!(
                "WorkStealingScheduler stats: total={}, completed={}, steals={}, avg_time={}μs, idle_time={}μs",
                total, completed, steals, avg_time, idle_time
            );
        }
    }

    /// 获取统计信息
    pub fn get_stats(&self) -> WorkStealingStats {
        (*self.stats).clone()
    }
}