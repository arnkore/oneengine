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


use criterion::{black_box, criterion_group, criterion_main, Criterion};
use oneengine::core::task::{Task, TaskType, Priority, ResourceRequirements};
use oneengine::core::pipeline::Pipeline;
use oneengine::scheduler::task_queue::TaskQueue;
use oneengine::scheduler::push_scheduler::PushScheduler;
use oneengine::utils::config::SchedulerConfig;
use std::collections::HashMap;

fn create_test_task(id: usize) -> Task {
    Task::new(
        format!("task_{}", id),
        TaskType::DataProcessing {
            operator: "map".to_string(),
            input_schema: None,
            output_schema: None,
        },
        Priority::Normal,
        ResourceRequirements::default(),
    )
}

fn create_test_pipeline(task_count: usize) -> Pipeline {
    let mut pipeline = Pipeline::new(
        "test_pipeline".to_string(),
        Some("Benchmark pipeline".to_string()),
    );

    // Create tasks
    let mut task_ids = Vec::new();
    for i in 0..task_count {
        let task = create_test_task(i);
        let task_id = pipeline.add_task(task);
        task_ids.push(task_id);
    }

    // Create linear pipeline
    for i in 0..task_count - 1 {
        pipeline.add_edge(task_ids[i], task_ids[i + 1], oneengine::core::pipeline::EdgeType::DataFlow)
            .unwrap();
    }

    pipeline
}

fn bench_task_queue_enqueue(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("task_queue_enqueue", |b| {
        b.iter(|| {
            let queue = TaskQueue::new(10000);
            let task = create_test_task(black_box(0));
            rt.block_on(queue.enqueue(task)).unwrap();
        })
    });
}

fn bench_task_queue_dequeue(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("task_queue_dequeue", |b| {
        b.iter(|| {
            let queue = TaskQueue::new(10000);
            
            // Pre-populate queue
            for i in 0..100 {
                let task = create_test_task(i);
                rt.block_on(queue.enqueue(task)).unwrap();
            }
            
            // Dequeue one task
            rt.block_on(queue.dequeue()).unwrap();
        })
    });
}

fn bench_pipeline_creation(c: &mut Criterion) {
    c.bench_function("pipeline_creation_100_tasks", |b| {
        b.iter(|| {
            create_test_pipeline(black_box(100))
        })
    });
}

fn bench_pipeline_validation(c: &mut Criterion) {
    let pipeline = create_test_pipeline(100);
    
    c.bench_function("pipeline_validation", |b| {
        b.iter(|| {
            pipeline.validate().unwrap();
        })
    });
}

fn bench_scheduler_creation(c: &mut Criterion) {
    let config = SchedulerConfig::default();
    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("scheduler_creation", |b| {
        b.iter(|| {
            rt.block_on(PushScheduler::new(black_box(config.clone()))).unwrap();
        })
    });
}

criterion_group!(
    benches,
    bench_task_queue_enqueue,
    bench_task_queue_dequeue,
    bench_pipeline_creation,
    bench_pipeline_validation,
    bench_scheduler_creation
);
criterion_main!(benches);
