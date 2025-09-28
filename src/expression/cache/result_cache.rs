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

//! 结果缓存实现
//! 
//! 提供表达式计算结果缓存

use arrow::array::ArrayRef;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// 结果缓存键
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultCacheKey {
    expression_id: String,
    input_hash: u64,
}

impl Hash for ResultCacheKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.expression_id.hash(state);
        self.input_hash.hash(state);
    }
}

/// 结果缓存条目
#[derive(Debug, Clone)]
pub struct ResultCacheEntry {
    pub result: ArrayRef,
    pub access_count: u64,
    pub last_access: std::time::Instant,
    pub memory_usage: usize,
}

/// 结果缓存
pub struct ResultCache {
    cache: HashMap<ResultCacheKey, ResultCacheEntry>,
    max_memory: usize,
    current_memory: usize,
    hit_count: u64,
    miss_count: u64,
}

impl ResultCache {
    /// 创建新的结果缓存
    pub fn new(max_memory: usize) -> Self {
        Self {
            cache: HashMap::new(),
            max_memory,
            current_memory: 0,
            hit_count: 0,
            miss_count: 0,
        }
    }

    /// 获取缓存结果
    pub fn get(&mut self, expression_id: &str, input_hash: u64) -> Option<ArrayRef> {
        let key = ResultCacheKey {
            expression_id: expression_id.to_string(),
            input_hash,
        };
        
        if let Some(entry) = self.cache.get_mut(&key) {
            entry.access_count += 1;
            entry.last_access = std::time::Instant::now();
            self.hit_count += 1;
            Some(entry.result.clone())
        } else {
            self.miss_count += 1;
            None
        }
    }

    /// 存储缓存结果
    pub fn put(&mut self, expression_id: &str, input_hash: u64, result: ArrayRef) {
        let key = ResultCacheKey {
            expression_id: expression_id.to_string(),
            input_hash,
        };
        
        let memory_usage = self.estimate_memory_usage(&result);
        
        // 如果内存超限，清理缓存
        while self.current_memory + memory_usage > self.max_memory && !self.cache.is_empty() {
            self.evict_least_used();
        }
        
        let entry = ResultCacheEntry {
            result,
            access_count: 1,
            last_access: std::time::Instant::now(),
            memory_usage,
        };
        
        self.cache.insert(key, entry);
        self.current_memory += memory_usage;
    }

    /// 估算内存使用量
    fn estimate_memory_usage(&self, array: &ArrayRef) -> usize {
        // 简单的估算：行数 * 数据类型大小
        let base_size = array.len() * 8; // 假设平均每行8字节
        base_size + std::mem::size_of::<ArrayRef>() // 加上ArrayRef本身的大小
    }

    /// 删除最少使用的条目
    fn evict_least_used(&mut self) {
        if let Some((key_to_remove, entry)) = self.cache.iter()
            .min_by_key(|(_, entry)| (entry.access_count, entry.last_access))
            .map(|(key, entry)| (key.clone(), entry.clone()))
        {
            self.current_memory -= entry.memory_usage;
            self.cache.remove(&key_to_remove);
        }
    }

    /// 获取缓存命中次数
    pub fn get_hit_count(&self) -> u64 {
        self.hit_count
    }

    /// 获取缓存未命中次数
    pub fn get_miss_count(&self) -> u64 {
        self.miss_count
    }

    /// 获取当前内存使用量
    pub fn get_memory_usage(&self) -> usize {
        self.current_memory
    }

    /// 获取缓存大小
    pub fn size(&self) -> usize {
        self.cache.len()
    }

    /// 清空缓存
    pub fn clear(&mut self) {
        self.cache.clear();
        self.current_memory = 0;
    }

    /// 获取缓存统计信息
    pub fn get_stats(&self) -> ResultCacheStats {
        ResultCacheStats {
            size: self.cache.len(),
            memory_usage: self.current_memory,
            hit_count: self.hit_count,
            miss_count: self.miss_count,
            hit_rate: if self.hit_count + self.miss_count > 0 {
                self.hit_count as f64 / (self.hit_count + self.miss_count) as f64
            } else {
                0.0
            },
        }
    }
}

/// 结果缓存统计信息
#[derive(Debug, Clone)]
pub struct ResultCacheStats {
    pub size: usize,
    pub memory_usage: usize,
    pub hit_count: u64,
    pub miss_count: u64,
    pub hit_rate: f64,
}

impl Default for ResultCache {
    fn default() -> Self {
        Self::new(1024 * 1024 * 1024) // 默认1GB内存限制
    }
}
