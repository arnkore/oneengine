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

//! 表达式缓存实现
//! 
//! 提供基于LRU的表达式结果缓存

use crate::expression::ast::Expression;
use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// 表达式缓存实现
pub struct ExpressionCacheImpl {
    cache: HashMap<CacheKey, CacheEntry>,
    max_size: usize,
    hit_count: u64,
    miss_count: u64,
}

/// 缓存键
#[derive(Debug, Clone, PartialEq, Eq)]
struct CacheKey {
    expression_hash: u64,
    batch_signature: BatchSignature,
}

/// 批次签名
#[derive(Debug, Clone, PartialEq, Eq)]
struct BatchSignature {
    num_rows: usize,
    num_columns: usize,
    schema_hash: u64,
}

impl Hash for CacheKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.expression_hash.hash(state);
        self.batch_signature.hash(state);
    }
}

/// 缓存条目
#[derive(Debug, Clone)]
struct CacheEntry {
    result: ArrayRef,
    access_count: u64,
    last_access: std::time::Instant,
    size_bytes: usize,
}

impl ExpressionCacheImpl {
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
        let size_bytes = self.estimate_array_size(&result);
        
        // 如果缓存已满，删除最少使用的条目
        if self.cache.len() >= self.max_size {
            self.evict_least_used();
        }
        
        let entry = CacheEntry {
            result,
            access_count: 1,
            last_access: std::time::Instant::now(),
            size_bytes,
        };
        
        self.cache.insert(key, entry);
    }

    /// 创建缓存键
    fn create_cache_key(&self, expression: &Expression, batch: &RecordBatch) -> CacheKey {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        
        // 计算表达式哈希
        expression.hash(&mut hasher);
        let expression_hash = hasher.finish();
        
        // 计算批次签名
        let batch_signature = BatchSignature {
            num_rows: batch.num_rows(),
            num_columns: batch.num_columns(),
            schema_hash: self.hash_schema(batch.schema()),
        };
        
        CacheKey {
            expression_hash,
            batch_signature,
        }
    }

    /// 计算Schema哈希
    fn hash_schema(&self, schema: &arrow::datatypes::Schema) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        for field in schema.fields() {
            field.name().hash(&mut hasher);
            field.data_type().hash(&mut hasher);
        }
        hasher.finish()
    }

    /// 估算数组大小
    fn estimate_array_size(&self, array: &ArrayRef) -> usize {
        // 简单的估算：行数 * 8字节（假设平均每行8字节）
        array.len() * 8
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
}
