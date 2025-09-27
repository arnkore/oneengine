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


use crate::memory::memory_pool::MemoryPool;
use anyhow::Result;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, debug};

/// Garbage collector for memory management
pub struct GarbageCollector {
    memory_pool: Arc<MemoryPool>,
    running: Arc<RwLock<bool>>,
    gc_interval_ms: u64,
}

impl GarbageCollector {
    pub fn new(memory_pool: Arc<MemoryPool>, gc_interval_ms: u64) -> Self {
        Self {
            memory_pool,
            running: Arc::new(RwLock::new(false)),
            gc_interval_ms,
        }
    }

    pub async fn start(&self) -> Result<()> {
        *self.running.write().await = true;
        
        let gc = self.clone();
        tokio::spawn(async move {
            if let Err(e) = gc.gc_loop().await {
                tracing::error!("Garbage collector error: {}", e);
            }
        });

        info!("Garbage collector started");
        Ok(())
    }

    pub async fn stop(&self) -> Result<()> {
        *self.running.write().await = false;
        info!("Garbage collector stopped");
        Ok(())
    }

    async fn gc_loop(&self) -> Result<()> {
        while *self.running.read().await {
            // Check if GC is needed
            if self.memory_pool.needs_gc().await {
                debug!("Memory threshold exceeded, running garbage collection");
                let cleaned_blocks = self.memory_pool.garbage_collect().await?;
                debug!("Garbage collection completed, cleaned {} blocks", cleaned_blocks);
            }

            // Wait for next GC cycle
            tokio::time::sleep(tokio::time::Duration::from_millis(self.gc_interval_ms)).await;
        }

        Ok(())
    }

    pub async fn force_gc(&self) -> Result<usize> {
        debug!("Forcing garbage collection");
        self.memory_pool.garbage_collect().await
    }
}

impl Clone for GarbageCollector {
    fn clone(&self) -> Self {
        Self {
            memory_pool: self.memory_pool.clone(),
            running: self.running.clone(),
            gc_interval_ms: self.gc_interval_ms,
        }
    }
}
