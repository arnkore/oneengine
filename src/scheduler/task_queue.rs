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


use crate::execution::task::{Task, Priority};
use anyhow::Result;
use std::collections::BinaryHeap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, warn};

/// A priority queue for tasks
pub struct TaskQueue {
    capacity: usize,
    queue: Arc<RwLock<BinaryHeap<QueuedTask>>>,
    size: Arc<RwLock<usize>>,
}

/// A task wrapped for priority queue ordering
#[derive(Debug, Clone)]
struct QueuedTask {
    task: Task,
    enqueued_at: std::time::Instant,
}

impl PartialEq for QueuedTask {
    fn eq(&self, other: &Self) -> bool {
        self.task.priority == other.task.priority
    }
}

impl Eq for QueuedTask {}

impl PartialOrd for QueuedTask {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        // Higher priority tasks come first
        // If priorities are equal, FIFO (earlier enqueued first)
        match other.task.priority.cmp(&self.task.priority) {
            std::cmp::Ordering::Equal => {
                // FIFO for same priority
                Some(other.enqueued_at.cmp(&self.enqueued_at))
            }
            other => Some(other),
        }
    }
}

impl Ord for QueuedTask {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl TaskQueue {
    /// Create a new task queue with the specified capacity
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            queue: Arc::new(RwLock::new(BinaryHeap::new())),
            size: Arc::new(RwLock::new(0)),
        }
    }

    /// Enqueue a task
    pub async fn enqueue(&self, task: Task) -> Result<()> {
        let current_size = *self.size.read().await;
        
        if current_size >= self.capacity {
            warn!("Task queue is full, dropping task: {}", task.id);
            return Err(anyhow::anyhow!("Task queue is full"));
        }

        let queued_task = QueuedTask {
            task,
            enqueued_at: std::time::Instant::now(),
        };

        {
            let mut queue = self.queue.write().await;
            queue.push(queued_task);
        }

        {
            let mut size = self.size.write().await;
            *size += 1;
        }

        debug!("Task enqueued, queue size: {}", current_size + 1);
        Ok(())
    }

    /// Dequeue the highest priority task
    pub async fn dequeue(&self) -> Result<Option<Task>> {
        let mut queue = self.queue.write().await;
        let mut size = self.size.write().await;

        match queue.pop() {
            Some(queued_task) => {
                *size -= 1;
                debug!("Task dequeued, queue size: {}", *size);
                Ok(Some(queued_task.task))
            }
            None => Ok(None),
        }
    }

    /// Peek at the highest priority task without removing it
    pub async fn peek(&self) -> Result<Option<Task>> {
        let queue = self.queue.read().await;
        Ok(queue.peek().map(|qt| qt.task.clone()))
    }

    /// Get the current queue size
    pub async fn size(&self) -> usize {
        *self.size.read().await
    }

    /// Check if the queue is empty
    pub async fn is_empty(&self) -> bool {
        *self.size.read().await == 0
    }

    /// Check if the queue is full
    pub async fn is_full(&self) -> bool {
        *self.size.read().await >= self.capacity
    }

    /// Clear all tasks from the queue
    pub async fn clear(&self) {
        let mut queue = self.queue.write().await;
        let mut size = self.size.write().await;
        
        queue.clear();
        *size = 0;
        
        debug!("Task queue cleared");
    }

    /// Get statistics about the queue
    pub async fn stats(&self) -> QueueStats {
        let queue = self.queue.read().await;
        let size = *self.size.read().await;
        
        let priority_counts = queue.iter().fold(
            std::collections::HashMap::new(),
            |mut acc, qt| {
                *acc.entry(qt.task.priority).or_insert(0) += 1;
                acc
            },
        );

        QueueStats {
            size,
            capacity: self.capacity,
            priority_counts,
        }
    }
}

/// Statistics about the task queue
#[derive(Debug, Clone)]
pub struct QueueStats {
    pub size: usize,
    pub capacity: usize,
    pub priority_counts: std::collections::HashMap<Priority, usize>,
}
