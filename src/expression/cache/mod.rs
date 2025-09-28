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

//! 表达式缓存模块
//! 
//! 提供表达式结果缓存功能，避免重复计算

pub mod result_cache;

use crate::expression::ast::Expression;
use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

/// 表达式缓存键
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheKey {
    expression_hash: u64,
    batch_hash: u64,
}

impl Hash for CacheKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.expression_hash.hash(state);
        self.batch_hash.hash(state);
    }
}

/// 表达式缓存条目
#[derive(Debug, Clone)]
pub struct CacheEntry {
    pub result: ArrayRef,
    pub access_count: u64,
    pub last_access: std::time::Instant,
}

/// 简化的表达式缓存
pub struct ExpressionCache {
    cache: HashMap<CacheKey, CacheEntry>,
    max_size: usize,
    hit_count: u64,
    miss_count: u64,
}

impl ExpressionCache {
    /// 创建新的表达式缓存
    pub fn new(max_size: usize) -> Self {
        Self {
            cache: HashMap::new(),
            max_size,
            hit_count: 0,
            miss_count: 0,
        }
    }

    /// 获取缓存结果
    pub fn get(&mut self, expression: &Expression, batch: &RecordBatch) -> Option<ArrayRef> {
        let key = self.create_cache_key(expression, batch);
        
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
    pub fn put(&mut self, expression: &Expression, batch: &RecordBatch, result: ArrayRef) {
        let key = self.create_cache_key(expression, batch);
        
        // 如果缓存已满，删除最少使用的条目
        if self.cache.len() >= self.max_size {
            self.evict_least_used();
        }
        
        let entry = CacheEntry {
            result,
            access_count: 1,
            last_access: std::time::Instant::now(),
        };
        
        self.cache.insert(key, entry);
    }

    /// 创建缓存键
    fn create_cache_key(&self, expression: &Expression, batch: &RecordBatch) -> CacheKey {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        
        // 计算表达式哈希
        expression.hash(&mut hasher);
        let expression_hash = hasher.finish();
        
        // 计算批次哈希（基于行数和列数）
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        batch.num_rows().hash(&mut hasher);
        batch.num_columns().hash(&mut hasher);
        let batch_hash = hasher.finish();
        
        CacheKey {
            expression_hash,
            batch_hash,
        }
    }

    /// 删除最少使用的条目
    fn evict_least_used(&mut self) {
        if let Some((key_to_remove, _)) = self.cache.iter()
            .min_by_key(|(_, entry)| (entry.access_count, entry.last_access))
            .map(|(key, _)| (key.clone(), ()))
        {
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

    /// 获取缓存大小
    pub fn size(&self) -> usize {
        self.cache.len()
    }

    /// 清空缓存
    pub fn clear(&mut self) {
        self.cache.clear();
    }

    /// 获取缓存统计信息
    pub fn get_stats(&self) -> CacheStats {
        CacheStats {
            size: self.cache.len(),
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

/// 缓存统计信息
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub size: usize,
    pub hit_count: u64,
    pub miss_count: u64,
    pub hit_rate: f64,
}

impl Default for ExpressionCache {
    fn default() -> Self {
        Self::new(1000) // 默认缓存1000个条目
    }
}
